from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import requests

from batch_enrichment.models import EnrichmentResult, FieldConfig, RetrieveConfig
from batch_enrichment.response_parser import parse_batch_lines

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_S = 30
_STREAM_CHUNK_SIZE = 65536  # 64 KB chunks for streaming large JSONL downloads


def _parse_array(value: Any) -> list[Any] | None:
    """Snowflake ARRAY columns come back as JSON-encoded strings via Snowpark.
    Parse to a Python list. Already-a-list inputs (fakes/tests) pass through.
    """
    if value is None:
        return None
    if isinstance(value, list):
        return value
    return json.loads(value)


@dataclass
class RetrieveResult:
    batches_checked: int
    batches_completed: int
    rows_written: int
    parse_errors: int
    guardrail_failures: int = 0


def _azure_headers(api_key: str) -> dict[str, str]:
    return {"api-key": api_key, "Content-Type": "application/json"}


def _fetch_active_batches(session: Any) -> list[dict[str, Any]]:
    rows = session.sql(
        "SELECT batch_tracking_id, azure_batch_id, azure_input_file_id, prompt_version "
        "FROM datalake.llm_enrichments.batch_tracking "
        "WHERE status IN ('SUBMITTED', 'IN_PROGRESS')"
    ).collect()
    return [
        {
            "batch_tracking_id": row["BATCH_TRACKING_ID"],
            "azure_batch_id": row["AZURE_BATCH_ID"],
            "azure_input_file_id": row["AZURE_INPUT_FILE_ID"],
            "prompt_version": row["PROMPT_VERSION"],
        }
        for row in rows
    ]


def _fetch_field_configs(session: Any, prompt_version: str) -> list[FieldConfig]:
    """Load enrichment field definitions for a prompt version from Snowflake."""
    rows = session.sql(
        "SELECT field_name, field_type, allowed_values, min_value, max_value, "
        "field_description, is_nullable, display_order "
        "FROM datalake.llm_enrichments.enrichment_field_config "
        f"WHERE config_version = '{prompt_version}' "
        "ORDER BY display_order"
    ).collect()
    return [
        FieldConfig(
            field_name=row["FIELD_NAME"],
            field_type=row["FIELD_TYPE"],
            allowed_values=_parse_array(row["ALLOWED_VALUES"]),
            min_value=row["MIN_VALUE"],
            max_value=row["MAX_VALUE"],
            field_description=row["FIELD_DESCRIPTION"] or "",
            is_nullable=bool(row["IS_NULLABLE"]),
            display_order=int(row["DISPLAY_ORDER"]),
        )
        for row in rows
    ]


def _check_azure_batch_status(
    azure_endpoint: str, api_key: str, azure_batch_id: str
) -> dict[str, Any]:
    url = f"{azure_endpoint}/openai/batches/{azure_batch_id}?api-version=2024-07-01-preview"
    response = requests.get(url, headers=_azure_headers(api_key), timeout=_DEFAULT_TIMEOUT_S)
    response.raise_for_status()
    return response.json()


def _download_output_file(azure_endpoint: str, api_key: str, output_file_id: str) -> str:
    """Stream output JSONL from Azure to avoid loading the full file into memory at once."""
    url = f"{azure_endpoint}/openai/files/{output_file_id}/content?api-version=2024-07-01-preview"
    chunks: list[bytes] = []
    with requests.get(
        url, headers=_azure_headers(api_key), stream=True, timeout=_DEFAULT_TIMEOUT_S
    ) as response:
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=_STREAM_CHUNK_SIZE):
            if chunk:
                chunks.append(chunk)
    return b"".join(chunks).decode("utf-8")


def _merge_enrichment_results(session: Any, results: list[EnrichmentResult]) -> int:
    """MERGE into enrichment_results on (conversation_id, prompt_version).

    parsed_fields is stored as a VARIANT (JSON object) so the schema can evolve
    without DDL changes.  Prevents silent duplicate rows when retrieve is called
    twice for the same completed batch.
    Returns the number of rows merged.
    """
    if not results:
        return 0

    # PARSE_JSON cannot appear inside FROM VALUES — Snowflake rejects function
    # expressions in VALUES clauses. Keep the VALUES rows as plain string/bool
    # literals and apply PARSE_JSON in the outer SELECT.
    values = ", ".join(
        "({conv_id}, {pv}, {parsed}, {raw}, "
        "{parse_error}, {parse_msg}, {failure_reason}, {tracking_id})".format(
            conv_id=_sql_str(r.conversation_id),
            pv=_sql_str(r.prompt_version),
            parsed=_sql_str(json.dumps(r.parsed_fields)),
            raw=_sql_str(_json_or_null(r.raw_response)),
            parse_error="TRUE" if r.parse_error else "FALSE",
            parse_msg=_sql_str(r.parse_error_message),
            failure_reason=_sql_str(r.failure_reason),
            tracking_id=_sql_str(r.batch_tracking_id),
        )
        for r in results
    )

    session.sql(f"""
        MERGE INTO datalake.llm_enrichments.enrichment_results AS target
        USING (
            SELECT
                column1 AS conversation_id,
                column2 AS prompt_version,
                PARSE_JSON(column3) AS parsed_fields,
                PARSE_JSON(column4) AS raw_response,
                column5 AS parse_error,
                column6 AS parse_error_message,
                column7 AS failure_reason,
                column8 AS batch_tracking_id
            FROM VALUES {values}
        ) AS source
        ON target.conversation_id = source.conversation_id
           AND target.prompt_version = source.prompt_version
        WHEN NOT MATCHED THEN INSERT (
            conversation_id, prompt_version, parsed_fields,
            raw_response, parse_error, parse_error_message,
            failure_reason, batch_tracking_id
        ) VALUES (
            source.conversation_id, source.prompt_version, source.parsed_fields,
            source.raw_response, source.parse_error, source.parse_error_message,
            source.failure_reason, source.batch_tracking_id
        )
    """).collect()
    return len(results)


def _update_tracking_completed(session: Any, tracking_id: str, output_file_id: str) -> None:
    session.sql(
        f"UPDATE datalake.llm_enrichments.batch_tracking "
        f"SET status = 'COMPLETED', azure_output_file_id = '{output_file_id}', "
        f"completed_at = CURRENT_TIMESTAMP(), updated_at = CURRENT_TIMESTAMP() "
        f"WHERE batch_tracking_id = '{tracking_id}'"
    ).collect()


_MAX_ERROR_MESSAGE_LEN = 4000


def _sql_escape(value: str) -> str:
    """Escape backslashes and single quotes for safe interpolation into a
    Snowflake string literal. Backslash first so the '' we add isn't doubled.
    """
    return value.replace("\\", "\\\\").replace("'", "''")


def _update_tracking_status(
    session: Any, tracking_id: str, status: str, error_message: str | None = None
) -> None:
    if error_message:
        safe_msg = _sql_escape(error_message[:_MAX_ERROR_MESSAGE_LEN])
        error_clause = f", error_message = '{safe_msg}'"
    else:
        error_clause = ""
    ts_clause = ", failed_at = CURRENT_TIMESTAMP()" if status == "FAILED" else ""
    session.sql(
        f"UPDATE datalake.llm_enrichments.batch_tracking "
        f"SET status = '{status}', updated_at = CURRENT_TIMESTAMP() {error_clause}{ts_clause} "
        f"WHERE batch_tracking_id = '{tracking_id}'"
    ).collect()


def _update_mapping_completed(session: Any, tracking_id: str) -> None:
    session.sql(
        f"UPDATE datalake.llm_enrichments.batch_row_mapping "
        f"SET batch_status = 'COMPLETED' "
        f"WHERE batch_tracking_id = '{tracking_id}'"
    ).collect()


def _delete_azure_files(azure_endpoint: str, api_key: str, file_ids: list[str]) -> None:
    for file_id in file_ids:
        if not file_id:
            continue
        try:
            url = f"{azure_endpoint}/openai/files/{file_id}?api-version=2024-07-01-preview"
            requests.delete(url, headers=_azure_headers(api_key), timeout=_DEFAULT_TIMEOUT_S)
            logger.info("Deleted Azure file: %s", file_id)
        except Exception as exc:
            logger.warning("Failed to delete Azure file %s: %s", file_id, exc)


def _sql_str(value: Any) -> str:
    if value is None:
        return "NULL"
    # Snowflake string literals interpret backslash escapes (\n → newline, etc.)
    # by default. Without this, JSON content with `\n` is parsed as a real
    # newline before PARSE_JSON runs, breaking strings across multiple lines.
    # Escape backslashes FIRST, then single quotes, so the '' we add isn't
    # re-doubled by the backslash pass.
    escaped = str(value).replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def _json_or_null(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value)


def retrieve_batch(session: Any, config: RetrieveConfig) -> RetrieveResult:
    """Poll Azure for completed batches and write results to enrichment_results.

    Follows the 9-step procedure defined in the plan (Section 6):
    1. Query batch_tracking for SUBMITTED/IN_PROGRESS rows
    2. For each: load field configs, GET Azure batch status
    3. Update batch_tracking status
    4. For completed: download output JSONL (streamed)
    5. Parse output line-by-line (validated against field configs)
    6. MERGE into enrichment_results as parsed_fields VARIANT (idempotent)
    7. Update batch_tracking to COMPLETED
    8. Update batch_row_mapping to COMPLETED
    9. Delete input + output files from Azure
    """
    active_batches = _fetch_active_batches(session)
    batches_completed = 0
    rows_written = 0
    parse_errors = 0
    guardrail_failures = 0

    for batch in active_batches:
        tracking_id = batch["batch_tracking_id"]
        azure_batch_id = batch["azure_batch_id"]
        input_file_id = batch["azure_input_file_id"]
        prompt_version = batch["prompt_version"]

        try:
            # 2 — load field configs and check Azure status
            field_configs = _fetch_field_configs(session, prompt_version)
            azure_status = _check_azure_batch_status(
                config.azure_endpoint, config.azure_api_key, azure_batch_id
            )
            status_str: str = azure_status.get("status", "").lower()

            # 3 — update tracking status
            if status_str == "validating":
                _update_tracking_status(session, tracking_id, "SUBMITTED")
                continue
            elif status_str == "in_progress":
                _update_tracking_status(session, tracking_id, "IN_PROGRESS")
                continue
            elif status_str == "failed":
                error_msg = azure_status.get("errors", {}).get("data", [{}])[0].get("message", "")
                _update_tracking_status(session, tracking_id, "FAILED", error_msg)
                continue
            elif status_str != "completed":
                logger.info("Batch %s has status %s; skipping.", azure_batch_id, status_str)
                continue

            # 4 — download output file (streamed)
            output_file_id: str = azure_status.get("output_file_id", "")
            output_jsonl = _download_output_file(
                config.azure_endpoint, config.azure_api_key, output_file_id
            )

            # 5 — parse output line-by-line, validated against field configs
            results = parse_batch_lines(output_jsonl, tracking_id, field_configs)
            for result in results:
                result.prompt_version = prompt_version

            # 6 — MERGE into enrichment_results (idempotent, parsed_fields as VARIANT)
            rows_written += _merge_enrichment_results(session, results)
            parse_errors += sum(1 for r in results if r.parse_error)
            guardrail_failures += sum(1 for r in results if r.failure_reason == "guardrail")

            # 7 — update batch_tracking to COMPLETED
            _update_tracking_completed(session, tracking_id, output_file_id)

            # 8 — update batch_row_mapping to COMPLETED
            _update_mapping_completed(session, tracking_id)

            # 9 — delete Azure files to avoid storage charges
            _delete_azure_files(
                config.azure_endpoint, config.azure_api_key, [input_file_id, output_file_id]
            )

            batches_completed += 1
            logger.info(
                "Completed batch %s: %d rows written, %d parse errors.",
                azure_batch_id,
                len(results),
                parse_errors,
            )

        except Exception as exc:
            logger.error("Error processing batch %s: %s", azure_batch_id, exc)
            _update_tracking_status(session, tracking_id, "FAILED", str(exc))

    return RetrieveResult(
        batches_checked=len(active_batches),
        batches_completed=batches_completed,
        rows_written=rows_written,
        parse_errors=parse_errors,
        guardrail_failures=guardrail_failures,
    )


def retrieve_batch_handler(
    session: Any, azure_endpoint: str, prompt_version: str
) -> dict[str, Any]:
    """Snowpark stored procedure entry point.

    The Azure API key is read from the bound Snowflake secret `azure_api_key`
    (declared in the CREATE PROCEDURE `SECRETS` clause), not passed as an
    argument. This keeps the key out of task DDL, query history, and logs.
    """
    import _snowflake  # available only inside the Snowpark runtime

    azure_api_key = _snowflake.get_generic_secret_string("azure_api_key")
    config = RetrieveConfig(
        azure_endpoint=azure_endpoint,
        azure_api_key=azure_api_key,
        prompt_version=prompt_version,
    )
    result = retrieve_batch(session, config)
    return {
        "batches_checked": result.batches_checked,
        "batches_completed": result.batches_completed,
        "rows_written": result.rows_written,
        "parse_errors": result.parse_errors,
        "guardrail_failures": result.guardrail_failures,
    }
