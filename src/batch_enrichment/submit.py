from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import requests

from batch_enrichment.jsonl_builder import build_batch_lines
from batch_enrichment.models import (
    BatchStatus,
    ContextConfig,
    ConversationTranscript,
    FieldConfig,
    SubmitConfig,
)

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_S = 30


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
class SubmitResult:
    batch_tracking_id: str | None
    azure_batch_id: str | None
    row_count: int
    status: BatchStatus
    error_message: str | None = None


def _azure_headers(api_key: str) -> dict[str, str]:
    return {"api-key": api_key, "Content-Type": "application/json"}


def _count_active_batches(session: Any) -> int:
    rows = session.sql(
        "SELECT COUNT(*) AS n FROM datalake.llm_enrichments.batch_tracking "
        "WHERE status IN ('SUBMITTING','SUBMITTED','IN_PROGRESS')"
    ).collect()
    return int(rows[0]["N"])


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


def _fetch_context_configs(session: Any, prompt_version: str) -> list[ContextConfig]:
    """Load context column definitions for a prompt version from Snowflake."""
    rows = session.sql(
        "SELECT column_name, display_label, value_description, display_order "
        "FROM datalake.llm_enrichments.enrichment_context_config "
        f"WHERE config_version = '{prompt_version}' "
        "ORDER BY display_order"
    ).collect()
    return [
        ContextConfig(
            column_name=row["COLUMN_NAME"],
            display_label=row["DISPLAY_LABEL"],
            value_description=row["VALUE_DESCRIPTION"],
            display_order=int(row["DISPLAY_ORDER"]),
        )
        for row in rows
    ]


def _fetch_queue(
    session: Any,
    chunk_size: int,
    context_configs: list[ContextConfig],
) -> list[ConversationTranscript]:
    """Fetch unenriched conversations and any configured context columns."""
    context_col_sql = ""
    if context_configs:
        sorted_ctx = sorted(context_configs, key=lambda c: c.display_order)
        # Unquoted — Snowflake folds these to uppercase and matches the view's
        # column names case-insensitively. Double-quoting would make the lookup
        # case-sensitive against the lowercased config value and fail to resolve.
        context_col_sql = ", " + ", ".join(c.column_name for c in sorted_ctx)

    # ORDER BY in the view is not guaranteed to propagate through a LIMIT in
    # the consumer — must be explicit here for FIFO eligibility under backlog.
    rows = session.sql(
        f"SELECT conversation_id, transcript_text{context_col_sql} "
        f"FROM datalake.llm_enrichments.enrichment_queue "
        f"ORDER BY conversation_started_at ASC "
        f"LIMIT {chunk_size}"
    ).collect()

    return [
        ConversationTranscript(
            conversation_id=row["CONVERSATION_ID"],
            transcript_text=row["TRANSCRIPT_TEXT"] if "TRANSCRIPT_TEXT" in str(row) else "",
            context_fields={c.column_name: row[c.column_name.upper()] for c in context_configs},
        )
        for row in rows
    ]


def _insert_mapping_rows(session: Any, conversation_ids: list[str], tracking_id: str) -> None:
    placeholders = ", ".join(f"('{cid}', '{tracking_id}', 'PENDING')" for cid in conversation_ids)
    session.sql(
        f"INSERT INTO datalake.llm_enrichments.batch_row_mapping "
        f"(conversation_id, batch_tracking_id, batch_status) VALUES {placeholders}"
    ).collect()


def _update_mapping_status(
    session: Any, conversation_ids: list[str], tracking_id: str, status: str
) -> None:
    ids_csv = ", ".join(f"'{cid}'" for cid in conversation_ids)
    session.sql(
        f"UPDATE datalake.llm_enrichments.batch_row_mapping "
        f"SET batch_status = '{status}' "
        f"WHERE batch_tracking_id = '{tracking_id}' "
        f"AND conversation_id IN ({ids_csv})"
    ).collect()


def _insert_tracking_row(
    session: Any,
    tracking_id: str,
    azure_batch_id: str,
    input_file_id: str,
    row_count: int,
    config: SubmitConfig,
) -> None:
    session.sql(
        "INSERT INTO datalake.llm_enrichments.batch_tracking "
        "(batch_tracking_id, azure_batch_id, azure_input_file_id, status, row_count, "
        " model_deployment, prompt_version, submitted_at) "
        f"VALUES ('{tracking_id}', '{azure_batch_id}', '{input_file_id}', 'SUBMITTED', "
        f"{row_count}, '{config.model_deployment}', '{config.prompt_version}', "
        "CURRENT_TIMESTAMP())"
    ).collect()


def _upload_jsonl_file(azure_endpoint: str, api_key: str, jsonl_content: str) -> str:
    url = f"{azure_endpoint}/openai/files?api-version=2024-07-01-preview"
    response = requests.post(
        url,
        headers={"api-key": api_key},
        files={"file": ("batch_input.jsonl", jsonl_content.encode(), "application/jsonl")},
        data={"purpose": "batch"},
        timeout=_DEFAULT_TIMEOUT_S,
    )
    response.raise_for_status()
    return response.json()["id"]


def _submit_batch_job(
    azure_endpoint: str, api_key: str, input_file_id: str, model_deployment: str
) -> str:
    url = f"{azure_endpoint}/openai/batches?api-version=2024-07-01-preview"
    payload = {
        "input_file_id": input_file_id,
        "endpoint": "/chat/completions",
        "completion_window": "24h",
        "metadata": {"model": model_deployment},
    }
    response = requests.post(
        url,
        headers=_azure_headers(api_key),
        json=payload,
        timeout=_DEFAULT_TIMEOUT_S,
    )
    response.raise_for_status()
    return response.json()["id"]


def _delete_azure_file(azure_endpoint: str, api_key: str, file_id: str) -> None:
    try:
        url = f"{azure_endpoint}/openai/files/{file_id}?api-version=2024-07-01-preview"
        requests.delete(url, headers=_azure_headers(api_key), timeout=_DEFAULT_TIMEOUT_S)
    except Exception as exc:
        logger.warning("Failed to delete Azure file %s: %s", file_id, exc)


def _generate_tracking_id(session: Any) -> str:
    rows = session.sql("SELECT UUID_STRING() AS id").collect()
    return str(rows[0]["ID"])


def submit_batch_handler(
    session: Any,
    chunk_size: int,
    max_active_batches: int,
    model_deployment: str,
    prompt_version: str,
    analytics_db: str,
    azure_endpoint: str,
) -> dict[str, Any]:
    """Snowpark stored procedure entry point. Returns a JSON-serialisable dict.

    The Azure API key is read from the bound Snowflake secret `azure_api_key`
    (declared in the CREATE PROCEDURE `SECRETS` clause), not passed as an
    argument. This keeps the key out of task DDL, query history, and logs.
    """
    import _snowflake  # available only inside the Snowpark runtime

    azure_api_key = _snowflake.get_generic_secret_string("azure_api_key")
    config = SubmitConfig(
        chunk_size=chunk_size,
        max_active_batches=max_active_batches,
        model_deployment=model_deployment,
        prompt_version=prompt_version,
        analytics_db=analytics_db,
        azure_endpoint=azure_endpoint,
        azure_api_key=azure_api_key,
    )
    result = submit_batch(session, config)
    return {
        "batch_tracking_id": result.batch_tracking_id,
        "azure_batch_id": result.azure_batch_id,
        "row_count": result.row_count,
        "status": result.status.value,
        "error_message": result.error_message,
    }


def submit_batch(session: Any, config: SubmitConfig) -> SubmitResult:
    """Submit one batch of conversations to Azure OpenAI for enrichment.

    Follows the 10-step procedure defined in the plan (Section 6):
    1. Guard against too many active batches
    2. Load field and context configs for this prompt version
    3–4. Fetch queue; exit if empty
    5. Build JSONL payload
    6. Write mapping rows (idempotency guard — BEFORE the Azure API call)
    7. Upload JSONL to Azure Files API
    8. Submit batch job
    9. Record batch_tracking row
    10. Update mapping rows to SUBMITTED
    On failure: mark mapping rows FAILED, attempt file cleanup
    """
    # 1 — guard: exit if max active batches reached
    active_count = _count_active_batches(session)
    if active_count >= config.max_active_batches:
        logger.info(
            "Active batch count %d >= max %d; skipping.", active_count, config.max_active_batches
        )
        return SubmitResult(
            batch_tracking_id=None,
            azure_batch_id=None,
            row_count=0,
            status=BatchStatus.PENDING,
            error_message="max_active_batches reached",
        )

    # 2 — load field and context configs for this prompt version
    field_configs = _fetch_field_configs(session, config.prompt_version)
    context_configs = _fetch_context_configs(session, config.prompt_version)

    # 3–4 — fetch queue; exit if empty
    transcripts = _fetch_queue(session, config.chunk_size, context_configs)
    if not transcripts:
        logger.info("Enrichment queue is empty; nothing to submit.")
        return SubmitResult(
            batch_tracking_id=None,
            azure_batch_id=None,
            row_count=0,
            status=BatchStatus.PENDING,
        )

    tracking_id = _generate_tracking_id(session)
    conversation_ids = [t.conversation_id for t in transcripts]
    input_file_id: str | None = None

    # 6 — write mapping rows BEFORE the API call (idempotency guard)
    _insert_mapping_rows(session, conversation_ids, tracking_id)

    try:
        # 5 — build JSONL payload (done here to keep the mapping insert before API calls)
        jsonl_content = build_batch_lines(transcripts, config, field_configs, context_configs)

        # 7 — upload to Azure Files API
        input_file_id = _upload_jsonl_file(
            config.azure_endpoint, config.azure_api_key, jsonl_content
        )
        logger.info("Uploaded input file: %s", input_file_id)

        # 8 — submit batch job
        azure_batch_id = _submit_batch_job(
            config.azure_endpoint, config.azure_api_key, input_file_id, config.model_deployment
        )
        logger.info("Submitted batch job: %s", azure_batch_id)

        # 9 — record tracking row
        _insert_tracking_row(
            session, tracking_id, azure_batch_id, input_file_id, len(transcripts), config
        )

        # 10 — update mapping rows to SUBMITTED
        _update_mapping_status(session, conversation_ids, tracking_id, "SUBMITTED")

        return SubmitResult(
            batch_tracking_id=tracking_id,
            azure_batch_id=azure_batch_id,
            row_count=len(transcripts),
            status=BatchStatus.SUBMITTED,
        )

    except Exception as exc:
        # Partial failure: mark mapping rows FAILED; attempt file cleanup
        logger.error("Submit failed for tracking_id=%s: %s", tracking_id, exc)
        _update_mapping_status(session, conversation_ids, tracking_id, "FAILED")
        if input_file_id:
            _delete_azure_file(config.azure_endpoint, config.azure_api_key, input_file_id)
        return SubmitResult(
            batch_tracking_id=tracking_id,
            azure_batch_id=None,
            row_count=len(transcripts),
            status=BatchStatus.FAILED,
            error_message=str(exc),
        )
