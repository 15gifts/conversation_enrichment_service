"""Integration tests for the retrieve procedure.

Azure HTTP calls are stubbed with `responses`. Snowflake session is a fake.
No live Snowflake or Azure connection required.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import responses as responses_lib

from batch_enrichment.models import RetrieveConfig
from batch_enrichment.retrieve import retrieve_batch

# ---------------------------------------------------------------------------
# Shared test field config rows — returned by the fake session for
# ENRICHMENT_FIELD_CONFIG queries. Must match _make_output_jsonl below.
# ---------------------------------------------------------------------------

_FIELD_CONFIG_ROWS = [
    {
        "FIELD_NAME": "sentiment",
        "FIELD_TYPE": "string_enum",
        "ALLOWED_VALUES": ["positive", "neutral", "negative"],
        "MIN_VALUE": None,
        "MAX_VALUE": None,
        "FIELD_DESCRIPTION": "Sentiment.",
        "IS_NULLABLE": False,
        "DISPLAY_ORDER": 1,
    },
    {
        "FIELD_NAME": "summary",
        "FIELD_TYPE": "string",
        "ALLOWED_VALUES": None,
        "MIN_VALUE": None,
        "MAX_VALUE": None,
        "FIELD_DESCRIPTION": "Summary.",
        "IS_NULLABLE": False,
        "DISPLAY_ORDER": 2,
    },
]


# ---------------------------------------------------------------------------
# Fake Snowflake session
# ---------------------------------------------------------------------------


@dataclass
class _FakeRow:
    data: dict[str, object]

    def __getitem__(self, key: str) -> object:
        return self.data[key.upper()]


@dataclass
class _FakeSession:
    active_batches: list[dict[str, str]] = field(default_factory=list)
    sql_log: list[str] = field(default_factory=list)

    def sql(self, query: str) -> _FakeSession:
        self.sql_log.append(query)
        self._last_query = query
        return self

    def collect(self) -> list[_FakeRow]:
        q = self._last_query.upper()
        if "SAT_GOOGLE_SHEETS__ENRICHMENT_FIELD_CONFIG_ACT" in q:
            return [_FakeRow(row) for row in _FIELD_CONFIG_ROWS]
        if "BATCH_TRACKING" in q and "SELECT" in q and "SUBMITTED" in q:
            return [
                _FakeRow(
                    {
                        "BATCH_TRACKING_ID": b["batch_tracking_id"],
                        "AZURE_BATCH_ID": b["azure_batch_id"],
                        "AZURE_INPUT_FILE_ID": b.get("azure_input_file_id", "file-in"),
                        "PROMPT_VERSION": b.get("prompt_version", "v1.0"),
                        "CONFIG_LOADED_AT": b.get("config_loaded_at", "2026-05-18 12:34:56.000"),
                    }
                )
                for b in self.active_batches
            ]
        return []


def make_config() -> RetrieveConfig:
    return RetrieveConfig(
        azure_endpoint="https://test.openai.azure.com",
        azure_api_key="test-key",
        prompt_version="v1.0",
    )


def _azure_batch_status(status: str, output_file_id: str = "file-out") -> dict:
    return {"status": status, "output_file_id": output_file_id, "errors": {"data": []}}


def _make_output_jsonl(n_ok: int = 3, n_error: int = 0, n_guardrail: int = 0) -> str:
    """Generate Azure Batch API output JSONL. Fields match _FIELD_CONFIG_ROWS."""
    valid = {
        "sentiment": "positive",
        "summary": "Issue resolved quickly.",
    }
    lines = []
    for i in range(n_ok):
        lines.append(
            json.dumps(
                {
                    "custom_id": f"conv-{i:03d}",
                    "response": {
                        "status_code": 200,
                        "body": {
                            "choices": [
                                {"message": {"role": "assistant", "content": json.dumps(valid)}}
                            ]
                        },
                    },
                }
            )
        )
    for i in range(n_error):
        lines.append(
            json.dumps(
                {
                    "custom_id": f"conv-err-{i}",
                    "response": {
                        "status_code": 200,
                        "body": {
                            "choices": [{"message": {"role": "assistant", "content": "not json"}}]
                        },
                    },
                }
            )
        )
    for i in range(n_guardrail):
        # Realistic Azure guardrail rejection at the per-line level.
        lines.append(
            json.dumps(
                {
                    "custom_id": f"conv-guard-{i}",
                    "response": {
                        "status_code": 400,
                        "body": {
                            "error": {
                                "code": "content_filter",
                                "message": (
                                    "The response was filtered due to the prompt "
                                    "triggering Azure OpenAI's content management policy."
                                ),
                            }
                        },
                    },
                }
            )
        )
    return "\n".join(lines)


class TestRetrieveHappyPath:
    @responses_lib.activate
    def test_completed_batch_writes_results_to_enrichment_results(self) -> None:
        # T8: batch_tracking IN_PROGRESS, Azure returns completed → rows in enrichment_results
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/batches/batch-xyz",
            json=_azure_batch_status("completed"),
            status=200,
        )
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/files/file-out/content",
            body=_make_output_jsonl(3).encode(),
            status=200,
        )
        responses_lib.add(
            responses_lib.DELETE,
            "https://test.openai.azure.com/openai/files/",
            status=200,
        )

        session = _FakeSession(
            active_batches=[
                {
                    "batch_tracking_id": "tracking-001",
                    "azure_batch_id": "batch-xyz",
                }
            ]
        )
        result = retrieve_batch(session, make_config())

        assert result.batches_completed == 1
        assert result.rows_written == 3
        assert result.parse_errors == 0

        executed = " ".join(session.sql_log).upper()
        assert "MERGE INTO" in executed
        assert "ENRICHMENT_RESULTS" in executed

    @responses_lib.activate
    def test_completed_batch_writes_parsed_fields_variant(self) -> None:
        # MERGE statement must use parsed_fields VARIANT (not individual columns)
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/batches/batch-xyz",
            json=_azure_batch_status("completed"),
            status=200,
        )
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/files/file-out/content",
            body=_make_output_jsonl(2).encode(),
            status=200,
        )
        responses_lib.add(
            responses_lib.DELETE,
            "https://test.openai.azure.com/openai/files/",
            status=200,
        )

        session = _FakeSession(
            active_batches=[
                {
                    "batch_tracking_id": "tracking-001",
                    "azure_batch_id": "batch-xyz",
                }
            ]
        )
        retrieve_batch(session, make_config())

        merge_queries = [q for q in session.sql_log if "MERGE INTO" in q.upper()]
        assert len(merge_queries) >= 1
        merge_sql = merge_queries[0].upper()
        # VARIANT column and PARSE_JSON cast must be present
        assert "PARSED_FIELDS" in merge_sql
        assert "PARSE_JSON" in merge_sql
        # INSERT column list must use parsed_fields, not the old individual columns
        # (find the INSERT target column list between "INSERT (" and the next ")")
        insert_start = merge_sql.index("INSERT (") + len("INSERT (")
        insert_cols = merge_sql[insert_start : merge_sql.index(")", insert_start)]
        assert "PARSED_FIELDS" in insert_cols
        assert "PRIMARY_INTENT" not in insert_cols
        assert "RESOLUTION_STATUS" not in insert_cols

    @responses_lib.activate
    def test_completed_batch_updates_tracking_to_completed(self) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/batches/batch-xyz",
            json=_azure_batch_status("completed"),
            status=200,
        )
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/files/file-out/content",
            body=_make_output_jsonl(2).encode(),
            status=200,
        )
        responses_lib.add(
            responses_lib.DELETE,
            "https://test.openai.azure.com/openai/files/",
            status=200,
        )

        session = _FakeSession(
            active_batches=[
                {
                    "batch_tracking_id": "tracking-001",
                    "azure_batch_id": "batch-xyz",
                }
            ]
        )
        retrieve_batch(session, make_config())

        completed_updates = [
            q
            for q in session.sql_log
            if "COMPLETED" in q.upper() and "BATCH_TRACKING" in q.upper() and "UPDATE" in q.upper()
        ]
        assert len(completed_updates) >= 1


class TestRetrieveErrorPaths:
    @responses_lib.activate
    def test_azure_batch_failed_marks_tracking_failed(self) -> None:
        # T16: Azure batch status 'failed' → batch_tracking FAILED, error_message populated
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/batches/batch-xyz",
            json={
                "status": "failed",
                "errors": {"data": [{"message": "Content policy violation"}]},
            },
            status=200,
        )

        session = _FakeSession(
            active_batches=[
                {
                    "batch_tracking_id": "tracking-001",
                    "azure_batch_id": "batch-xyz",
                }
            ]
        )
        result = retrieve_batch(session, make_config())

        assert result.batches_completed == 0
        failed_updates = [
            q for q in session.sql_log if "FAILED" in q.upper() and "UPDATE" in q.upper()
        ]
        assert len(failed_updates) >= 1

    @responses_lib.activate
    def test_partial_row_failures_writes_all_results(self) -> None:
        # T17: 8 OK + 2 error lines → 10 results total, 2 with parse_error=True
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/batches/batch-xyz",
            json=_azure_batch_status("completed"),
            status=200,
        )
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/files/file-out/content",
            body=_make_output_jsonl(n_ok=8, n_error=2).encode(),
            status=200,
        )
        responses_lib.add(
            responses_lib.DELETE,
            "https://test.openai.azure.com/openai/files/",
            status=200,
        )

        session = _FakeSession(
            active_batches=[
                {
                    "batch_tracking_id": "tracking-001",
                    "azure_batch_id": "batch-xyz",
                }
            ]
        )
        result = retrieve_batch(session, make_config())

        assert result.rows_written == 10
        assert result.parse_errors == 2

    @responses_lib.activate
    def test_idempotent_retrieve_does_not_duplicate_rows(self) -> None:
        # T8 extension: calling retrieve twice for same completed batch → MERGE prevents duplicates
        for _ in range(2):
            responses_lib.add(
                responses_lib.GET,
                "https://test.openai.azure.com/openai/batches/batch-xyz",
                json=_azure_batch_status("completed"),
                status=200,
            )
            responses_lib.add(
                responses_lib.GET,
                "https://test.openai.azure.com/openai/files/file-out/content",
                body=_make_output_jsonl(3).encode(),
                status=200,
            )
        responses_lib.add(
            responses_lib.DELETE,
            "https://test.openai.azure.com/openai/files/",
            status=200,
        )

        # First call: batch is active
        session_first = _FakeSession(
            active_batches=[
                {
                    "batch_tracking_id": "tracking-001",
                    "azure_batch_id": "batch-xyz",
                }
            ]
        )
        retrieve_batch(session_first, make_config())

        # Second call: batch is still active (e.g. status update failed before completing)
        session_second = _FakeSession(
            active_batches=[
                {
                    "batch_tracking_id": "tracking-001",
                    "azure_batch_id": "batch-xyz",
                }
            ]
        )
        retrieve_batch(session_second, make_config())

        # Both calls use MERGE — no INSERT that could duplicate rows
        for session in [session_first, session_second]:
            inserts = [
                q
                for q in session.sql_log
                if "INSERT INTO" in q.upper() and "ENRICHMENT_RESULTS" in q.upper()
            ]
            assert len(inserts) == 0, "retrieve must use MERGE, not INSERT, for enrichment_results"


class TestRetrieveGuardrailHandling:
    @responses_lib.activate
    def test_guardrail_failures_counted_and_persisted(self) -> None:
        # Realistic mix: 7 OK + 3 Azure guardrail rejections.
        # Guardrail rows must be written to enrichment_results (so the queue view
        # excludes them on the next run — they are NOT retried) and surfaced as
        # `guardrail_failures` in the procedure return for observability.
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/batches/batch-xyz",
            json=_azure_batch_status("completed"),
            status=200,
        )
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/files/file-out/content",
            body=_make_output_jsonl(n_ok=7, n_guardrail=3).encode(),
            status=200,
        )
        responses_lib.add(
            responses_lib.DELETE,
            "https://test.openai.azure.com/openai/files/",
            status=200,
        )

        session = _FakeSession(
            active_batches=[{"batch_tracking_id": "tracking-001", "azure_batch_id": "batch-xyz"}]
        )
        result = retrieve_batch(session, make_config())

        # All 10 rows persist (so the queue excludes them); 3 are guardrail hits.
        assert result.rows_written == 10
        assert result.guardrail_failures == 3
        # Guardrail rows are also parse_errors (the LLM gave us no parseable output).
        assert result.parse_errors == 3

        # The MERGE must carry failure_reason and must populate 'guardrail'
        # for the rejected rows — this is what enables non-retry classification.
        merge_queries = [q for q in session.sql_log if "MERGE INTO" in q.upper()]
        assert merge_queries, "expected a MERGE into enrichment_results"
        merge_sql = merge_queries[0]
        assert "failure_reason" in merge_sql
        assert "'guardrail'" in merge_sql

    @responses_lib.activate
    def test_guardrail_batch_still_marks_tracking_completed(self) -> None:
        # A batch composed entirely of guardrail rejections is still a COMPLETED
        # batch from the pipeline's point of view — Azure successfully processed
        # every row, even if every row was refused. batch_tracking must reach
        # COMPLETED so the active-batch slot frees up; mapping rows go COMPLETED
        # so they aren't seen as in-flight by the queue view.
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/batches/batch-xyz",
            json=_azure_batch_status("completed"),
            status=200,
        )
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/files/file-out/content",
            body=_make_output_jsonl(n_ok=0, n_guardrail=2).encode(),
            status=200,
        )
        responses_lib.add(
            responses_lib.DELETE,
            "https://test.openai.azure.com/openai/files/",
            status=200,
        )

        session = _FakeSession(
            active_batches=[{"batch_tracking_id": "tracking-001", "azure_batch_id": "batch-xyz"}]
        )
        result = retrieve_batch(session, make_config())

        assert result.batches_completed == 1
        assert result.guardrail_failures == 2

        executed = " ".join(session.sql_log).upper()
        # batch_tracking → COMPLETED  AND  batch_row_mapping → COMPLETED
        assert any(
            "UPDATE" in q.upper() and "BATCH_TRACKING" in q.upper() and "COMPLETED" in q.upper()
            for q in session.sql_log
        )
        assert any(
            "UPDATE" in q.upper() and "BATCH_ROW_MAPPING" in q.upper() and "COMPLETED" in q.upper()
            for q in session.sql_log
        )
        assert "ENRICHMENT_RESULTS" in executed


class TestRetrieveNoBatches:
    def test_no_active_batches_returns_zero_counts(self) -> None:
        session = _FakeSession(active_batches=[])
        result = retrieve_batch(session, make_config())
        assert result.batches_checked == 0
        assert result.batches_completed == 0
        assert result.rows_written == 0


class TestConfigSourceMigration:
    """Verifies retrieve.py reads field configs from the datawarehouse _act
    satellite and propagates config_loaded_at to enrichment_results
    (ticket configure_config_sheet)."""

    @responses_lib.activate
    def test_field_config_query_targets_act_satellite(self) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/batches/batch-xyz",
            json=_azure_batch_status("completed"),
            status=200,
        )
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/files/file-out/content",
            body=_make_output_jsonl(1).encode(),
            status=200,
        )
        responses_lib.add(
            responses_lib.DELETE,
            "https://test.openai.azure.com/openai/files/",
            status=200,
        )

        session = _FakeSession(
            active_batches=[{"batch_tracking_id": "tracking-001", "azure_batch_id": "batch-xyz"}]
        )
        retrieve_batch(session, make_config())

        field_queries = [
            q
            for q in session.sql_log
            if "SAT_GOOGLE_SHEETS__ENRICHMENT_FIELD_CONFIG_ACT" in q.upper()
        ]
        assert len(field_queries) >= 1, "retrieve did not query the _act satellite"
        assert "CONFIG_VERSION" not in field_queries[0].upper()

    def test_active_batches_query_selects_config_loaded_at(self) -> None:
        session = _FakeSession(
            active_batches=[{"batch_tracking_id": "tracking-001", "azure_batch_id": "batch-xyz"}]
        )
        # Stub Azure to a non-completed status so we exit the per-batch loop
        # before any other queries — this isolates _fetch_active_batches.
        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                "https://test.openai.azure.com/openai/batches/batch-xyz",
                json=_azure_batch_status("in_progress"),
                status=200,
            )
            retrieve_batch(session, make_config())

        select_queries = [
            q
            for q in session.sql_log
            if "BATCH_TRACKING" in q.upper() and q.upper().lstrip().startswith("SELECT")
        ]
        assert len(select_queries) >= 1
        assert "CONFIG_LOADED_AT" in select_queries[0].upper(), (
            "_fetch_active_batches must select config_loaded_at so it can be "
            "propagated to enrichment_results"
        )

    @responses_lib.activate
    def test_merge_into_enrichment_results_includes_config_loaded_at(self) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/batches/batch-xyz",
            json=_azure_batch_status("completed"),
            status=200,
        )
        responses_lib.add(
            responses_lib.GET,
            "https://test.openai.azure.com/openai/files/file-out/content",
            body=_make_output_jsonl(1).encode(),
            status=200,
        )
        responses_lib.add(
            responses_lib.DELETE,
            "https://test.openai.azure.com/openai/files/",
            status=200,
        )

        session = _FakeSession(
            active_batches=[
                {
                    "batch_tracking_id": "tracking-001",
                    "azure_batch_id": "batch-xyz",
                    "config_loaded_at": "2026-05-18 12:34:56.000",
                }
            ]
        )
        retrieve_batch(session, make_config())

        merge_queries = [q for q in session.sql_log if "MERGE INTO" in q.upper()]
        assert len(merge_queries) >= 1
        merge_sql = merge_queries[0]
        assert "config_loaded_at" in merge_sql.lower(), (
            "MERGE must carry config_loaded_at into enrichment_results"
        )
        assert "2026-05-18 12:34:56.000" in merge_sql
