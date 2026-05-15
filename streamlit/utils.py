"""Shared helpers for the batch enrichment monitoring dashboard.

All Snowflake queries run via `get_active_session()` (Streamlit in Snowflake
binds this automatically). Every query in this module is wrapped in
`@st.cache_data` with a TTL appropriate to the data's volatility — see
monitoring.md "Streamlit Dashboard Layout" for the refresh cadence per panel.

Queries are tagged at session start so dashboard cost is separable from
ad-hoc Snowflake spend on ENRICHMENT_WH (see Cost panel).
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

QUERY_TAG = "streamlit_enrichment_dashboard"

# Refresh cadences (seconds). Aligned to monitoring.md panel cadences.
TTL_HEARTBEAT = 60       # Overview — must be near-real-time
TTL_OPERATIONAL = 300    # Latency / failures / quality detail
TTL_THROUGHPUT = 3600    # Daily aggregates — hourly refresh is plenty
TTL_AUDIT = 86400        # Security/audit — daily refresh


@st.cache_resource
def get_session():
    """Return the active Snowpark session, tagging it for cost attribution.

    Streamlit-in-Snowflake sandboxes the session and rejects `ALTER SESSION`
    statements ("Unsupported statement type 'ALTER_SESSION'"). Use the
    Snowpark `query_tag` property instead — it sets the same tag without
    running DDL. `@st.cache_resource` ensures this only happens once per
    app instance.
    """
    session = get_active_session()
    try:
        session.query_tag = QUERY_TAG
    except Exception:
        # Older Snowpark builds may not expose `query_tag` as a settable
        # property. Failing silently is fine — the tag is for cost
        # attribution only and doesn't affect dashboard correctness.
        pass
    return session


def run_query(sql: str) -> pd.DataFrame:
    """Execute SQL and return a pandas DataFrame.

    Caching is applied per-query at the call site (not here), because TTL
    varies by panel.
    """
    return get_session().sql(sql).to_pandas()


# ---------------------------------------------------------------------------
# Panel 1 — Heartbeat
# ---------------------------------------------------------------------------
@st.cache_data(ttl=TTL_HEARTBEAT)
def task_liveness() -> pd.DataFrame:
    """Minutes since the most recent SUCCEEDED run of each task.

    Used by alert rules: SUBMIT >180 min stale = P1; RETRIEVE >60 min = P1.
    """
    return run_query(
        """
        SELECT
            name                                         AS task_name,
            MAX(query_start_time)                        AS last_run_at,
            DATEDIFF('minute', MAX(query_start_time), CURRENT_TIMESTAMP())
                                                         AS minutes_since_last_run
        FROM TABLE(
            snowflake.information_schema.task_history(
                scheduled_time_range_start =>
                    DATEADD('day', -2, CURRENT_TIMESTAMP())
            )
        )
        WHERE state = 'SUCCEEDED'
          AND name IN ('SUBMIT_BATCH_TASK', 'RETRIEVE_BATCH_TASK')
        GROUP BY name
        """
    )


@st.cache_data(ttl=TTL_HEARTBEAT)
def recent_task_runs(days: int = 7) -> pd.DataFrame:
    """Per-run timeline for both tasks — green/red dots in the UI."""
    return run_query(
        f"""
        SELECT
            name AS task_name, state AS run_state,
            scheduled_time, query_start_time, completed_time,
            DATEDIFF('second', query_start_time, completed_time) AS duration_s,
            error_code, error_message
        FROM TABLE(
            snowflake.information_schema.task_history(
                scheduled_time_range_start =>
                    DATEADD('day', -{days}, CURRENT_TIMESTAMP()),
                task_name => 'SUBMIT_BATCH_TASK'
            )
        )
        UNION ALL
        SELECT
            name, state, scheduled_time, query_start_time, completed_time,
            DATEDIFF('second', query_start_time, completed_time),
            error_code, error_message
        FROM TABLE(
            snowflake.information_schema.task_history(
                scheduled_time_range_start =>
                    DATEADD('day', -{days}, CURRENT_TIMESTAMP()),
                task_name => 'RETRIEVE_BATCH_TASK'
            )
        )
        ORDER BY scheduled_time DESC
        """
    )


# ---------------------------------------------------------------------------
# Panel 2 — Queue & throughput
# ---------------------------------------------------------------------------
@st.cache_data(ttl=TTL_OPERATIONAL)
def queue_age_buckets() -> pd.DataFrame:
    """Queue depth bucketed by conversation age.

    Preferred over `SELECT COUNT(*)` on the view because the view is a
    multi-fact join and is expensive to scan repeatedly.
    """
    return run_query(
        """
        SELECT
            CASE
                WHEN DATEDIFF('hour', conversation_started_at, CURRENT_TIMESTAMP()) < 4
                    THEN '0-4h'
                WHEN DATEDIFF('hour', conversation_started_at, CURRENT_TIMESTAMP()) < 12
                    THEN '4-12h'
                WHEN DATEDIFF('hour', conversation_started_at, CURRENT_TIMESTAMP()) < 24
                    THEN '12-24h'
                WHEN DATEDIFF('hour', conversation_started_at, CURRENT_TIMESTAMP()) < 72
                    THEN '1-3d'
                ELSE '3d+'
            END AS age_bucket,
            COUNT(*) AS rows_in_queue,
            MIN(DATEDIFF('hour', conversation_started_at, CURRENT_TIMESTAMP()))
                AS bucket_sort
        FROM datalake.llm_enrichments.enrichment_queue
        GROUP BY age_bucket
        ORDER BY bucket_sort
        """
    )


@st.cache_data(ttl=TTL_OPERATIONAL)
def active_batches() -> pd.DataFrame:
    """Snapshot of in-flight batches by status."""
    return run_query(
        """
        -- `total_rows` (not `rows`) — ROWS is reserved in Snowflake.
        SELECT status, COUNT(*) AS batches, SUM(row_count) AS total_rows
        FROM datalake.llm_enrichments.batch_tracking
        WHERE status IN ('SUBMITTING', 'SUBMITTED', 'IN_PROGRESS')
        GROUP BY status
        """
    )


@st.cache_data(ttl=TTL_THROUGHPUT)
def daily_submissions(days: int = 30) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            DATE_TRUNC('day', submitted_at) AS submit_day,
            COUNT(*)                        AS batches_submitted,
            SUM(row_count)                  AS rows_submitted
        FROM datalake.llm_enrichments.batch_tracking
        WHERE submitted_at >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        GROUP BY submit_day
        ORDER BY submit_day
        """
    )


@st.cache_data(ttl=TTL_THROUGHPUT)
def daily_completions(days: int = 30) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            DATE_TRUNC('day', completed_at) AS day,
            SUM(row_count)                  AS rows_completed,
            COUNT(*)                        AS batches_completed
        FROM datalake.llm_enrichments.batch_tracking
        WHERE status = 'COMPLETED'
          AND completed_at >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        GROUP BY day
        ORDER BY day
        """
    )


# ---------------------------------------------------------------------------
# Panel 3 — Latency
# ---------------------------------------------------------------------------
@st.cache_data(ttl=TTL_OPERATIONAL)
def latency_percentiles(days: int = 14) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            PERCENTILE_CONT(0.50) WITHIN GROUP (
                ORDER BY DATEDIFF('minute', submitted_at, completed_at)
            ) AS p50_minutes,
            PERCENTILE_CONT(0.90) WITHIN GROUP (
                ORDER BY DATEDIFF('minute', submitted_at, completed_at)
            ) AS p90_minutes,
            PERCENTILE_CONT(0.99) WITHIN GROUP (
                ORDER BY DATEDIFF('minute', submitted_at, completed_at)
            ) AS p99_minutes,
            MAX(DATEDIFF('minute', submitted_at, completed_at)) AS max_minutes,
            COUNT(*) AS batches
        FROM datalake.llm_enrichments.batch_tracking
        WHERE status = 'COMPLETED'
          AND completed_at >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        """
    )


@st.cache_data(ttl=TTL_OPERATIONAL)
def batch_latencies(days: int = 14) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            batch_tracking_id, azure_batch_id, row_count,
            submitted_at, completed_at,
            DATEDIFF('minute', submitted_at, completed_at) AS minutes_e2e
        FROM datalake.llm_enrichments.batch_tracking
        WHERE status = 'COMPLETED'
          AND completed_at >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        ORDER BY submitted_at DESC
        """
    )


@st.cache_data(ttl=TTL_OPERATIONAL)
def stuck_batches() -> pd.DataFrame:
    """In-flight beyond Azure's 24h SLA + retrieval cadence buffer.

    Any row returned is an incident.
    """
    return run_query(
        """
        SELECT
            batch_tracking_id, azure_batch_id, status, submitted_at,
            DATEDIFF('hour', submitted_at, CURRENT_TIMESTAMP()) AS hours_in_flight,
            row_count
        FROM datalake.llm_enrichments.batch_tracking
        WHERE status IN ('SUBMITTING', 'SUBMITTED', 'IN_PROGRESS')
          AND submitted_at < DATEADD('hour', -26, CURRENT_TIMESTAMP())
        ORDER BY submitted_at
        """
    )


# ---------------------------------------------------------------------------
# Panel 4 — Quality
# ---------------------------------------------------------------------------
@st.cache_data(ttl=TTL_OPERATIONAL)
def outcome_breakdown(days: int = 7) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            CASE
                WHEN parse_error = TRUE             THEN 'parse_error'
                WHEN failure_reason = 'guardrail'   THEN 'guardrail_block'
                WHEN failure_reason = 'azure_error' THEN 'azure_error'
                WHEN parsed_fields IS NOT NULL      THEN 'success'
                ELSE 'other'
            END                AS outcome,
            COUNT(*)           AS total_rows,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM datalake.llm_enrichments.enrichment_results
        WHERE enriched_at >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        GROUP BY outcome
        ORDER BY total_rows DESC
        """
    )


@st.cache_data(ttl=TTL_OPERATIONAL)
def recent_parse_errors(days: int = 7, limit: int = 50) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            conversation_id, prompt_version, parse_error_message,
            LEFT(raw_response::STRING, 500) AS raw_preview,
            enriched_at
        FROM datalake.llm_enrichments.enrichment_results
        WHERE parse_error = TRUE
          AND enriched_at >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        ORDER BY enriched_at DESC
        LIMIT {limit}
        """
    )


@st.cache_data(ttl=TTL_OPERATIONAL)
def failing_fields(days: int = 30) -> pd.DataFrame:
    return run_query(
        f"""
        WITH errors AS (
            SELECT
                REGEXP_SUBSTR(
                    parse_error_message,
                    'Invalid value for ''([^'']+)''',
                    1, 1, 'e', 1
                ) AS failing_field,
                enriched_at
            FROM datalake.llm_enrichments.enrichment_results
            WHERE parse_error = TRUE
              AND enriched_at >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        )
        SELECT
            failing_field,
            COUNT(*) AS failures,
            MIN(enriched_at) AS first_seen,
            MAX(enriched_at) AS last_seen
        FROM errors
        WHERE failing_field IS NOT NULL
        GROUP BY failing_field
        ORDER BY failures DESC
        """
    )


# ---------------------------------------------------------------------------
# Panel 5 — Failures
# ---------------------------------------------------------------------------
@st.cache_data(ttl=TTL_OPERATIONAL)
def failed_batches(days: int = 30) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            batch_tracking_id, azure_batch_id, status,
            submitted_at, failed_at,
            DATEDIFF('minute', submitted_at, failed_at) AS minutes_to_failure,
            row_count,
            LEFT(error_message, 200) AS error_preview
        FROM datalake.llm_enrichments.batch_tracking
        WHERE status = 'FAILED'
          AND submitted_at >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        ORDER BY submitted_at DESC
        """
    )


@st.cache_data(ttl=TTL_OPERATIONAL)
def failure_categories(days: int = 30) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            CASE
                WHEN error_message ILIKE '%timeout%'      THEN 'timeout'
                WHEN error_message ILIKE '%rate limit%'   THEN 'rate_limit'
                WHEN error_message ILIKE '%401%'
                  OR error_message ILIKE '%unauthorized%' THEN 'auth'
                WHEN error_message ILIKE '%PARSE_JSON%'   THEN 'json_escape'
                WHEN error_message ILIKE '%network%'      THEN 'network'
                ELSE 'other'
            END             AS failure_category,
            COUNT(*)        AS batches,
            SUM(row_count)  AS rows_affected
        FROM datalake.llm_enrichments.batch_tracking
        WHERE status = 'FAILED'
          AND submitted_at >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        GROUP BY failure_category
        ORDER BY batches DESC
        """
    )


@st.cache_data(ttl=TTL_OPERATIONAL)
def orphan_rows() -> pd.DataFrame:
    """Mapping rows still PENDING/SUBMITTING after their batch is past SLA.

    Candidates for the manual reset path in backlog_management_runbook.md.
    """
    return run_query(
        """
        SELECT
            brm.batch_status,
            COUNT(*) AS total_rows
        FROM datalake.llm_enrichments.batch_row_mapping brm
        JOIN datalake.llm_enrichments.batch_tracking bt
          ON brm.batch_tracking_id = bt.batch_tracking_id
        WHERE brm.batch_status NOT IN ('COMPLETED', 'FAILED')
          AND bt.submitted_at < DATEADD('hour', -26, CURRENT_TIMESTAMP())
        GROUP BY brm.batch_status
        """
    )


# ---------------------------------------------------------------------------
# Panel 6 — Cost
# ---------------------------------------------------------------------------
@st.cache_data(ttl=TTL_AUDIT)
def warehouse_credits(days: int = 30) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            DATE_TRUNC('day', start_time) AS day,
            SUM(credits_used)             AS credits,
            SUM(credits_used) * 3.0       AS approx_usd_at_3_per_credit
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE warehouse_name = 'ENRICHMENT_WH'
          AND start_time >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        GROUP BY day
        ORDER BY day
        """
    )


# ---------------------------------------------------------------------------
# Panel 7 — Audit
# ---------------------------------------------------------------------------
_EXPECTED_TASKS = {"SUBMIT_BATCH_TASK", "RETRIEVE_BATCH_TASK"}


@st.cache_data(ttl=TTL_AUDIT)
def unexpected_tasks() -> pd.DataFrame:
    """Allowlist check — any task in the schema not in the expected set.

    Uses `SHOW TASKS` rather than `account_usage.tasks` or
    `information_schema.tasks` because:
      - INFORMATION_SCHEMA has no TASKS view (only TASK_HISTORY).
      - ACCOUNT_USAGE.TASKS column naming varies by account version
        (e.g. NAME vs other casing/quoting), making it brittle.
      - SHOW TASKS is universal, real-time, and the Snowpark row keys
        are stable (lowercase identifiers).
    """
    session = get_session()
    rows = session.sql(
        "SHOW TASKS IN SCHEMA datalake.llm_enrichments"
    ).collect()
    actual = {row["name"].upper() for row in rows}
    unexpected = sorted(actual - _EXPECTED_TASKS)
    return pd.DataFrame({"unexpected_task": unexpected})


@st.cache_data(ttl=TTL_AUDIT)
def unexpected_procedures() -> pd.DataFrame:
    return run_query(
        """
        WITH expected AS (
            SELECT 'SUBMIT_BATCH_SP'   AS proc_name UNION ALL
            SELECT 'RETRIEVE_BATCH_SP'
        ),
        actual AS (
            SELECT UPPER(procedure_name) AS proc_name
            FROM datalake.information_schema.procedures
            WHERE procedure_schema = 'LLM_ENRICHMENTS'
        )
        SELECT a.proc_name AS unexpected_procedure
        FROM actual a
        LEFT JOIN expected e ON a.proc_name = e.proc_name
        WHERE e.proc_name IS NULL
        """
    )


@st.cache_data(ttl=TTL_AUDIT)
def recent_eai_grants(days: int = 30) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            created_on, privilege, granted_on,
            name AS object_name, granted_to, grantee_name, granted_by
        FROM snowflake.account_usage.grants_to_roles
        WHERE granted_on IN ('INTEGRATION', 'SECRET')
          AND name IN ('AZURE_OPENAI_EAI', 'AZURE_OPENAI_KEY')
          AND created_on >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        ORDER BY created_on DESC
        """
    )


@st.cache_data(ttl=TTL_AUDIT)
def recent_procedure_calls(days: int = 7) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            start_time, user_name, role_name,
            LEFT(query_text, 200) AS query_preview,
            execution_status
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
          AND (
              query_text ILIKE '%submit_batch_sp%'
              OR query_text ILIKE '%retrieve_batch_sp%'
              OR query_text ILIKE '%execute task%batch%'
          )
        ORDER BY start_time DESC
        """
    )


# ---------------------------------------------------------------------------
# Alert thresholds (from monitoring.md "Alert Rules")
# ---------------------------------------------------------------------------
ALERT_THRESHOLDS = {
    "SUBMIT_BATCH_TASK_MAX_AGE_MIN": 180,    # cron = 2h, 50% headroom
    "RETRIEVE_BATCH_TASK_MAX_AGE_MIN": 60,   # cron = 30m, 100% headroom
    "PARSE_ERROR_PCT_WARN": 2.0,
    "PARSE_ERROR_PCT_CRIT": 5.0,
    "STUCK_BATCH_HOURS": 26,
}
