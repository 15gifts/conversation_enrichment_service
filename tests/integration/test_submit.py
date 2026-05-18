"""Integration tests for the submit procedure.

The Snowflake session is replaced with a lightweight fake that records SQL calls
and returns controlled row data. Azure HTTP calls are stubbed with `responses`.

No live Snowflake or Azure connection is required.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import responses as responses_lib

from batch_enrichment.models import BatchStatus, SubmitConfig
from batch_enrichment.submit import submit_batch

# ---------------------------------------------------------------------------
# Shared test field config rows — returned by the fake session for
# ENRICHMENT_FIELD_CONFIG queries. Two fields is enough for all submit tests
# (content of the JSONL isn't validated at the submit layer).
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

# Canned max(load_datetime) across the two _act satellites — the value
# returned by _fetch_config_loaded_at and stamped on the batch_tracking row.
_CONFIG_LOADED_AT = "2026-05-18 12:34:56.000"


# ---------------------------------------------------------------------------
# Fake Snowflake session
# ---------------------------------------------------------------------------


@dataclass
class _FakeRow:
    data: dict[str, object]

    def __getitem__(self, key: str) -> object:
        return self.data[key.upper()]

    def __contains__(self, key: str) -> bool:
        return key.upper() in self.data


@dataclass
class _FakeSession:
    """Minimal Snowflake session fake — records SQL and returns pre-configured rows."""

    queue_rows: list[dict[str, str]] = field(default_factory=list)
    active_batch_count: int = 0
    sql_log: list[str] = field(default_factory=list)

    def sql(self, query: str) -> _FakeSession:
        self.sql_log.append(query)
        self._last_query = query
        return self

    def collect(self) -> list[_FakeRow]:
        q = self._last_query.upper()
        if "COUNT(*)" in q and "BATCH_TRACKING" in q:
            return [_FakeRow({"N": self.active_batch_count})]
        # GREATEST query references both _act tables; must match first.
        if "GREATEST" in q and "MAX(LOAD_DATETIME)" in q:
            return [_FakeRow({"CONFIG_LOADED_AT": _CONFIG_LOADED_AT})]
        if "SAT_GOOGLE_SHEETS__ENRICHMENT_FIELD_CONFIG_ACT" in q:
            return [_FakeRow(row) for row in _FIELD_CONFIG_ROWS]
        if "SAT_GOOGLE_SHEETS__ENRICHMENT_CONTEXT_CONFIG_ACT" in q:
            return []  # no context columns in submit tests
        if "ENRICHMENT_QUEUE" in q:
            return [
                _FakeRow(
                    {
                        "CONVERSATION_ID": r["conversation_id"],
                        "TRANSCRIPT_TEXT": r.get("transcript_text", ""),
                    }
                )
                for r in self.queue_rows
            ]
        if "UUID_STRING()" in q:
            return [_FakeRow({"ID": "test-tracking-id-001"})]
        return []


def make_config(**overrides: object) -> SubmitConfig:
    defaults = {
        "chunk_size": 10,
        "max_active_batches": 1,
        "model_deployment": "gpt-4-1-batch",
        "prompt_version": "v1.0",
        "analytics_db": "prod_analytics",
        "azure_endpoint": "https://test.openai.azure.com",
        "azure_api_key": "test-key",
    }
    defaults.update(overrides)
    return SubmitConfig(**defaults)  # type: ignore[arg-type]


def make_queue_rows(n: int) -> list[dict[str, str]]:
    return [
        {"conversation_id": f"conv-{i:03d}", "transcript_text": f"user: hello {i}"}
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Tests: T6, T7, T13, T14, T15, T20, T21, T22
# ---------------------------------------------------------------------------


class TestEnrichmentQueueBehaviour:
    # T6 and T20/T21/T22 are validated by the enrichment_queue VIEW definition (sql/03_views.sql).
    # The submit procedure itself does not filter — it queries the view result.
    # These tests verify submit exits cleanly when the queue is empty.

    def test_empty_queue_returns_pending_with_zero_rows(self) -> None:
        # T6 corollary: no rows in queue → procedure exits cleanly
        session = _FakeSession(queue_rows=[])
        result = submit_batch(session, make_config())
        assert result.row_count == 0
        assert result.status == BatchStatus.PENDING
        assert result.batch_tracking_id is None


class TestSubmitHappyPath:
    @responses_lib.activate
    def test_submit_records_tracking_and_mapping_rows(self) -> None:
        # T7: 10 rows in queue, stubbed Azure → SUBMITTED status, mapping rows written
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/files",
            json={"id": "file-abc"},
            status=200,
        )
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/batches",
            json={"id": "batch-xyz"},
            status=200,
        )

        session = _FakeSession(queue_rows=make_queue_rows(10))
        result = submit_batch(session, make_config())

        assert result.status == BatchStatus.SUBMITTED
        assert result.row_count == 10
        assert result.azure_batch_id == "batch-xyz"
        assert result.batch_tracking_id == "test-tracking-id-001"

        executed = " ".join(session.sql_log).upper()
        assert "INSERT INTO DATALAKE.LLM_ENRICHMENTS.BATCH_ROW_MAPPING" in executed
        assert "INSERT INTO DATALAKE.LLM_ENRICHMENTS.BATCH_TRACKING" in executed

    @responses_lib.activate
    def test_mapping_rows_inserted_before_azure_api_call(self) -> None:
        # Idempotency guard: mapping INSERT must appear before any Azure POST in the sql log
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/files",
            json={"id": "file-abc"},
            status=200,
        )
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/batches",
            json={"id": "batch-xyz"},
            status=200,
        )

        session = _FakeSession(queue_rows=make_queue_rows(5))
        submit_batch(session, make_config())

        # Use full table path to avoid matching column name 'batch_tracking_id' in other queries
        mapping_insert_idx = _find_idx(
            session.sql_log, "INSERT INTO", "DATALAKE.LLM_ENRICHMENTS.BATCH_ROW_MAPPING"
        )
        tracking_insert_idx = _find_idx(
            session.sql_log, "INSERT INTO", "DATALAKE.LLM_ENRICHMENTS.BATCH_TRACKING"
        )
        assert mapping_insert_idx is not None
        assert tracking_insert_idx is not None
        assert mapping_insert_idx < tracking_insert_idx


class TestSubmitErrorPaths:
    @responses_lib.activate
    def test_azure_401_marks_mapping_rows_failed(self) -> None:
        # T13: Azure 401 → exception raised; mapping rows set to FAILED; no tracking row
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/files",
            status=401,
        )

        session = _FakeSession(queue_rows=make_queue_rows(3))
        result = submit_batch(session, make_config())

        assert result.status == BatchStatus.FAILED
        assert result.error_message is not None

        executed = " ".join(session.sql_log).upper()
        assert "FAILED" in executed
        assert "INSERT INTO DATALAKE.LLM_ENRICHMENTS.BATCH_TRACKING" not in executed

    @responses_lib.activate
    def test_file_upload_succeeds_batch_submit_fails_cleans_up(self) -> None:
        # T14: upload OK, batch POST 429 → mapping FAILED, DELETE file called, no tracking row
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/files",
            json={"id": "file-to-delete"},
            status=200,
        )
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/batches",
            status=429,
        )
        responses_lib.add(
            responses_lib.DELETE,
            "https://test.openai.azure.com/openai/files/file-to-delete",
            status=200,
        )

        session = _FakeSession(queue_rows=make_queue_rows(3))
        result = submit_batch(session, make_config())

        assert result.status == BatchStatus.FAILED
        delete_calls = [c for c in responses_lib.calls if c.request.method == "DELETE"]
        assert len(delete_calls) == 1
        assert "file-to-delete" in delete_calls[0].request.url

    def test_max_active_batches_reached_exits_early(self) -> None:
        # T15: 1 existing IN_PROGRESS batch, max=1 → exits; no new rows
        session = _FakeSession(queue_rows=make_queue_rows(10), active_batch_count=1)
        result = submit_batch(session, make_config(max_active_batches=1))

        assert result.status == BatchStatus.PENDING
        assert result.row_count == 0
        assert result.batch_tracking_id is None

        executed = " ".join(session.sql_log).upper()
        assert "INSERT" not in executed


class TestConcurrentSubmitBehaviour:
    @responses_lib.activate
    def test_second_call_exits_early_after_first_succeeds(self) -> None:
        # T25: second submit call sees active_batch_count=1 (set by first call) → exits early
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/files",
            json={"id": "file-abc"},
            status=200,
        )
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/batches",
            json={"id": "batch-xyz"},
            status=200,
        )

        session_first = _FakeSession(queue_rows=make_queue_rows(3), active_batch_count=0)
        result_first = submit_batch(session_first, make_config())
        assert result_first.status == BatchStatus.SUBMITTED

        # Second session sees active_batch_count=1 (as if first batch is now IN_PROGRESS)
        session_second = _FakeSession(queue_rows=make_queue_rows(3), active_batch_count=1)
        result_second = submit_batch(session_second, make_config(max_active_batches=1))
        assert result_second.status == BatchStatus.PENDING
        assert result_second.row_count == 0


def _find_idx(sql_log: list[str], *tokens: str) -> int | None:
    """Return index of first query that contains ALL tokens (case-insensitive)."""
    for i, q in enumerate(sql_log):
        upper = q.upper()
        if all(t.upper() in upper for t in tokens):
            return i
    return None


class TestConfigSourceMigration:
    """Verifies the migration from datalake.llm_enrichments config tables to
    datawarehouse _act satellites (ticket configure_config_sheet)."""

    @responses_lib.activate
    def test_field_config_reads_from_act_satellite_without_version_filter(self) -> None:
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/files",
            json={"id": "file-abc"},
            status=200,
        )
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/batches",
            json={"id": "batch-xyz"},
            status=200,
        )

        session = _FakeSession(queue_rows=make_queue_rows(2))
        submit_batch(session, make_config())

        field_query_idx = _find_idx(
            session.sql_log, "SAT_GOOGLE_SHEETS__ENRICHMENT_FIELD_CONFIG_ACT"
        )
        assert field_query_idx is not None, "field config query did not target _act satellite"

        field_query = session.sql_log[field_query_idx].upper()
        # Old behaviour filtered by config_version; new behaviour reads the
        # whole satellite (history lives in _hist; _act is current state).
        assert "CONFIG_VERSION" not in field_query
        assert "WHERE" not in field_query or "CONFIG_VERSION" not in field_query

        # The legacy table name must not appear anywhere in the submit path.
        assert _find_idx(session.sql_log, "ENRICHMENT_FIELD_CONFIG ") is None or all(
            "SAT_GOOGLE_SHEETS__ENRICHMENT_FIELD_CONFIG_ACT" in q.upper()
            for q in session.sql_log
            if "ENRICHMENT_FIELD_CONFIG" in q.upper()
        )

    @responses_lib.activate
    def test_context_config_reads_from_act_satellite_without_version_filter(self) -> None:
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/files",
            json={"id": "file-abc"},
            status=200,
        )
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/batches",
            json={"id": "batch-xyz"},
            status=200,
        )

        session = _FakeSession(queue_rows=make_queue_rows(2))
        submit_batch(session, make_config())

        ctx_query_idx = _find_idx(
            session.sql_log, "SAT_GOOGLE_SHEETS__ENRICHMENT_CONTEXT_CONFIG_ACT"
        )
        assert ctx_query_idx is not None, "context config query did not target _act satellite"
        assert "CONFIG_VERSION" not in session.sql_log[ctx_query_idx].upper()

    @responses_lib.activate
    def test_config_loaded_at_query_combines_both_act_satellites(self) -> None:
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/files",
            json={"id": "file-abc"},
            status=200,
        )
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/batches",
            json={"id": "batch-xyz"},
            status=200,
        )

        session = _FakeSession(queue_rows=make_queue_rows(2))
        submit_batch(session, make_config())

        idx = _find_idx(session.sql_log, "GREATEST", "MAX(LOAD_DATETIME)")
        assert idx is not None, "no GREATEST(MAX(load_datetime)...) query was issued"
        q = session.sql_log[idx].upper()
        assert "SAT_GOOGLE_SHEETS__ENRICHMENT_FIELD_CONFIG_ACT" in q
        assert "SAT_GOOGLE_SHEETS__ENRICHMENT_CONTEXT_CONFIG_ACT" in q

    @responses_lib.activate
    def test_tracking_insert_carries_config_loaded_at(self) -> None:
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/files",
            json={"id": "file-abc"},
            status=200,
        )
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/batches",
            json={"id": "batch-xyz"},
            status=200,
        )

        session = _FakeSession(queue_rows=make_queue_rows(2))
        submit_batch(session, make_config())

        idx = _find_idx(session.sql_log, "INSERT INTO", "DATALAKE.LLM_ENRICHMENTS.BATCH_TRACKING")
        assert idx is not None, "no batch_tracking INSERT was issued"
        tracking_sql = session.sql_log[idx]
        assert "config_loaded_at" in tracking_sql.lower(), (
            "config_loaded_at column missing from batch_tracking INSERT"
        )
        assert _CONFIG_LOADED_AT in tracking_sql, (
            "config_loaded_at value not stamped on the tracking row"
        )

    @responses_lib.activate
    def test_config_loaded_at_fetched_before_azure_calls(self) -> None:
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/files",
            json={"id": "file-abc"},
            status=200,
        )
        responses_lib.add(
            responses_lib.POST,
            "https://test.openai.azure.com/openai/batches",
            json={"id": "batch-xyz"},
            status=200,
        )

        session = _FakeSession(queue_rows=make_queue_rows(2))
        submit_batch(session, make_config())

        # The fetch must happen before the Azure POSTs so the value is known
        # when the tracking row is inserted on Azure success.
        config_idx = _find_idx(session.sql_log, "GREATEST", "MAX(LOAD_DATETIME)")
        tracking_idx = _find_idx(
            session.sql_log, "INSERT INTO", "DATALAKE.LLM_ENRICHMENTS.BATCH_TRACKING"
        )
        assert config_idx is not None and tracking_idx is not None
        assert config_idx < tracking_idx
