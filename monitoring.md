# Monitoring Runbook — Batch LLM Enrichment Pipeline (AE-1540)

This document defines what to observe, why it matters, and the SQL queries that
power it. Each section maps to a panel on the planned Streamlit dashboard.

All queries assume:
- `USE ROLE llm_enrichment_role;` (or any role with SELECT on the LLM tables)
- `USE WAREHOUSE enrichment_wh;`
- `USE SCHEMA datalake.llm_enrichments;`

---

## Observability Goals

The pipeline is operationally healthy when **all six** of the following hold:

1. **Both Snowflake tasks are RESUMED and firing on schedule** — submissions
   and retrievals are happening.
2. **The queue is not growing unboundedly** — submission rate ≥ ingestion rate.
3. **End-to-end latency is within SLA** — most conversations get a result
   within 24h (Azure's batch SLA) plus retrieval lag.
4. **Per-row success rate is high** — `parse_error = FALSE` and
   `failure_reason IS NULL` for the vast majority of rows.
5. **No batches are stuck** — nothing in `SUBMITTED` or `IN_PROGRESS` for
   longer than the Azure 24h completion window plus retrieval cadence.
6. **No unexpected tasks or procedures exist** — the only objects in
   `datalake.llm_enrichments` are the ones we deployed.

Each panel below maps to one or more of these goals.

---

## Panel 1 — Pipeline Heartbeat

**Goal 1 — are the tasks firing?**

A pipeline that has silently stopped is the worst failure mode: no alerts
fire, the queue grows, and the first signal is an analyst complaining.

### 1a. Current task state

```sql
SHOW TASKS IN SCHEMA datalake.llm_enrichments;
```

Look at the `state` column. `started` means scheduled execution is active;
`suspended` means it will not fire even when its cron triggers.

### 1b. Recent task runs (last 7 days)

```sql
SELECT
    name                       AS task_name,
    state                      AS run_state,         -- SUCCEEDED | FAILED | SKIPPED
    scheduled_time,
    query_start_time,
    completed_time,
    DATEDIFF('second', query_start_time, completed_time) AS duration_s,
    error_code,
    error_message
FROM TABLE(
    snowflake.information_schema.task_history(
        scheduled_time_range_start => DATEADD('day', -7, CURRENT_TIMESTAMP()),
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
        scheduled_time_range_start => DATEADD('day', -7, CURRENT_TIMESTAMP()),
        task_name => 'RETRIEVE_BATCH_TASK'
    )
)
ORDER BY scheduled_time DESC;
```

**Dashboard treatment:** two timelines (one per task) with green/red dots
per scheduled run. Any `FAILED` or `SKIPPED` run is an immediate red flag.

### 1c. Liveness check — time since last successful run

```sql
SELECT
    name                                         AS task_name,
    MAX(query_start_time)                        AS last_run_at,
    DATEDIFF('minute', MAX(query_start_time), CURRENT_TIMESTAMP())
                                                 AS minutes_since_last_run
FROM TABLE(
    snowflake.information_schema.task_history(
        scheduled_time_range_start => DATEADD('day', -2, CURRENT_TIMESTAMP())
    )
)
WHERE state = 'SUCCEEDED'
  AND name IN ('SUBMIT_BATCH_TASK', 'RETRIEVE_BATCH_TASK')
GROUP BY name;
```

**Alert thresholds:**
- `SUBMIT_BATCH_TASK` — alert if `minutes_since_last_run > 180` (cron is every
  2h; 50% headroom).
- `RETRIEVE_BATCH_TASK` — alert if `minutes_since_last_run > 60` (cron is
  every 30m; 100% headroom).

---

## Panel 2 — Queue Depth & Throughput

**Goal 2 — is submission keeping up with ingestion?**

### 2a. Current queue depth

```sql
SELECT COUNT(*) AS queue_depth
FROM datalake.llm_enrichments.enrichment_queue;
```

**Note on cost:** `enrichment_queue` is a view that joins
`fact_conversation_messages`, `fact_events`, `fact_sales`,
`enrichment_results`, and `batch_row_mapping`. Counting it is not free on a
busy cluster. For the dashboard, prefer the cached approximation in 2b
unless the user explicitly clicks "refresh".

### 2b. Backlog age distribution

```sql
SELECT
    CASE
        WHEN DATEDIFF('hour', conversation_started_at, CURRENT_TIMESTAMP()) < 4 THEN '0-4h'
        WHEN DATEDIFF('hour', conversation_started_at, CURRENT_TIMESTAMP()) < 12 THEN '4-12h'
        WHEN DATEDIFF('hour', conversation_started_at, CURRENT_TIMESTAMP()) < 24 THEN '12-24h'
        WHEN DATEDIFF('hour', conversation_started_at, CURRENT_TIMESTAMP()) < 72 THEN '1-3d'
        ELSE '3d+'
    END AS age_bucket,
    COUNT(*) AS rows_in_queue
FROM datalake.llm_enrichments.enrichment_queue
GROUP BY age_bucket
ORDER BY MIN(DATEDIFF('hour', conversation_started_at, CURRENT_TIMESTAMP()));
```

Anything in `3d+` indicates the pipeline is not keeping up — see the
backlog runbook for triage.

### 2c. Submission throughput (rows submitted per day)

```sql
SELECT
    DATE_TRUNC('day', submitted_at)  AS submit_day,
    COUNT(*)                          AS batches_submitted,
    SUM(row_count)                    AS rows_submitted
FROM datalake.llm_enrichments.batch_tracking
WHERE submitted_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY submit_day
ORDER BY submit_day DESC;
```

### 2d. Active batches (concurrency snapshot)

```sql
SELECT status, COUNT(*) AS batches, SUM(row_count) AS rows
FROM datalake.llm_enrichments.batch_tracking
WHERE status IN ('SUBMITTING', 'SUBMITTED', 'IN_PROGRESS')
GROUP BY status;
```

The `max_active_batches` guard in `submit.py` prevents this from exceeding
the configured ceiling. If `batches` is consistently equal to that ceiling,
either raise `MAX_ACTIVE_BATCHES` or increase `CHUNK_SIZE`.

---

## Panel 3 — End-to-End Latency

**Goal 3 — how long does a conversation take from queue to result?**

### 3a. Per-batch latency (submitted → completed)

```sql
SELECT
    batch_tracking_id,
    azure_batch_id,
    row_count,
    submitted_at,
    completed_at,
    DATEDIFF('minute', submitted_at, completed_at) AS minutes_e2e
FROM datalake.llm_enrichments.batch_tracking
WHERE status = 'COMPLETED'
  AND completed_at >= DATEADD('day', -14, CURRENT_TIMESTAMP())
ORDER BY submitted_at DESC;
```

### 3b. Latency distribution (last 14 days)

```sql
SELECT
    PERCENTILE_CONT(0.50) WITHIN GROUP (
        ORDER BY DATEDIFF('minute', submitted_at, completed_at)
    )                       AS p50_minutes,
    PERCENTILE_CONT(0.90) WITHIN GROUP (
        ORDER BY DATEDIFF('minute', submitted_at, completed_at)
    )                       AS p90_minutes,
    PERCENTILE_CONT(0.99) WITHIN GROUP (
        ORDER BY DATEDIFF('minute', submitted_at, completed_at)
    )                       AS p99_minutes,
    MAX(DATEDIFF('minute', submitted_at, completed_at))
                            AS max_minutes,
    COUNT(*)                AS batches
FROM datalake.llm_enrichments.batch_tracking
WHERE status = 'COMPLETED'
  AND completed_at >= DATEADD('day', -14, CURRENT_TIMESTAMP());
```

Azure's batch SLA is 24h (1440 minutes). p99 above ~1500 minutes means
some batches are timing out — check Azure status for those specific
`azure_batch_id`s.

### 3c. Stuck batches (in flight beyond the SLA window)

```sql
SELECT
    batch_tracking_id,
    azure_batch_id,
    status,
    submitted_at,
    DATEDIFF('hour', submitted_at, CURRENT_TIMESTAMP()) AS hours_in_flight,
    row_count
FROM datalake.llm_enrichments.batch_tracking
WHERE status IN ('SUBMITTING', 'SUBMITTED', 'IN_PROGRESS')
  AND submitted_at < DATEADD('hour', -26, CURRENT_TIMESTAMP())
ORDER BY submitted_at ASC;
```

**Any row returned here is an incident.** Likely causes: Azure-side stall,
retrieval task suspended, or the API returned a status the polling code
doesn't recognise.

---

## Panel 4 — Per-Row Outcome Quality

**Goal 4 — what proportion of rows succeed cleanly?**

### 4a. Outcome breakdown (last 7 days)

```sql
SELECT
    CASE
        WHEN parse_error = TRUE                       THEN 'parse_error'
        WHEN failure_reason = 'guardrail'             THEN 'guardrail_block'
        WHEN failure_reason = 'azure_error'           THEN 'azure_error'
        WHEN parsed_fields IS NOT NULL                THEN 'success'
        ELSE 'other'
    END                       AS outcome,
    COUNT(*)                  AS rows,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM datalake.llm_enrichments.enrichment_results
WHERE enriched_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY outcome
ORDER BY rows DESC;
```

**Healthy targets** (rule of thumb — refine once you have baseline data):
- `success` ≥ 95%
- `parse_error` ≤ 2% (sustained higher indicates the prompt or validation
  rules need tuning)
- `guardrail_block` < 1% (a small steady rate is normal — content policy)
- `azure_error` ≈ 0% (anything else is an infrastructure problem)

### 4b. Parse error samples (for prompt/validation tuning)

```sql
SELECT
    conversation_id,
    prompt_version,
    parse_error_message,
    LEFT(raw_response::STRING, 500) AS raw_preview,
    enriched_at
FROM datalake.llm_enrichments.enrichment_results
WHERE parse_error = TRUE
  AND enriched_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY enriched_at DESC
LIMIT 50;
```

### 4c. Per-field validation failures

For diagnosing which specific output fields cause the most rejections:

```sql
WITH errors AS (
    SELECT
        REGEXP_SUBSTR(parse_error_message, 'Invalid value for ''([^'']+)''', 1, 1, 'e', 1)
            AS failing_field,
        parse_error_message,
        enriched_at
    FROM datalake.llm_enrichments.enrichment_results
    WHERE parse_error = TRUE
      AND enriched_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
)
SELECT
    failing_field,
    COUNT(*) AS failures,
    MIN(enriched_at) AS first_seen,
    MAX(enriched_at) AS last_seen
FROM errors
WHERE failing_field IS NOT NULL
GROUP BY failing_field
ORDER BY failures DESC;
```

---

## Panel 5 — Failure & Retry Visibility

**Goal 5 — surface batch-level failures, not just row-level.**

### 5a. Failed batches (last 30 days)

```sql
SELECT
    batch_tracking_id,
    azure_batch_id,
    status,
    submitted_at,
    failed_at,
    DATEDIFF('minute', submitted_at, failed_at) AS minutes_to_failure,
    row_count,
    LEFT(error_message, 200) AS error_preview
FROM datalake.llm_enrichments.batch_tracking
WHERE status = 'FAILED'
  AND submitted_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY submitted_at DESC;
```

### 5b. Failure categorisation

A small regex on `error_message` to bucket common failure modes — extend
as new categories appear:

```sql
SELECT
    CASE
        WHEN error_message ILIKE '%timeout%'         THEN 'timeout'
        WHEN error_message ILIKE '%rate limit%'      THEN 'rate_limit'
        WHEN error_message ILIKE '%401%'
          OR error_message ILIKE '%unauthorized%'    THEN 'auth'
        WHEN error_message ILIKE '%PARSE_JSON%'      THEN 'json_escape'
        WHEN error_message ILIKE '%network%'         THEN 'network'
        ELSE 'other'
    END                  AS failure_category,
    COUNT(*)             AS batches,
    SUM(row_count)       AS rows_affected
FROM datalake.llm_enrichments.batch_tracking
WHERE status = 'FAILED'
  AND submitted_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY failure_category
ORDER BY batches DESC;
```

### 5c. Rows still pending (mapping not closed out)

```sql
SELECT
    brm.batch_status,
    COUNT(*) AS rows
FROM datalake.llm_enrichments.batch_row_mapping brm
JOIN datalake.llm_enrichments.batch_tracking bt
  ON brm.batch_tracking_id = bt.batch_tracking_id
WHERE brm.batch_status NOT IN ('COMPLETED', 'FAILED')
  AND bt.submitted_at < DATEADD('hour', -26, CURRENT_TIMESTAMP())
GROUP BY brm.batch_status;
```

Rows here are candidates for the manual reset path in the backlog runbook.

---

## Panel 6 — Cost & Compute

**Goal — track spend before it surprises us.**

Two cost surfaces: Snowflake compute (warehouse credits) and Azure OpenAI
token spend. Azure spend is not in Snowflake — pull it from the Azure
portal or surface it via `BATCH_TRACKING.cost_estimate_usd` if you start
populating that column (currently unused).

### 6a. Snowflake compute attributable to enrichment

```sql
SELECT
    DATE_TRUNC('day', start_time)  AS day,
    SUM(credits_used)               AS credits,
    SUM(credits_used) * 3.0         AS approx_usd_at_3_per_credit
FROM snowflake.account_usage.warehouse_metering_history
WHERE warehouse_name = 'ENRICHMENT_WH'
  AND start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY day
ORDER BY day DESC;
```

### 6b. Token/row throughput proxy

Until you wire Azure spend in directly, rows-completed-per-day is the best
proxy for Azure cost (cost scales near-linearly with token volume, which
scales with row count at a fixed prompt/transcript length):

```sql
SELECT
    DATE_TRUNC('day', completed_at) AS day,
    SUM(row_count)                   AS rows_completed,
    COUNT(*)                         AS batches_completed
FROM datalake.llm_enrichments.batch_tracking
WHERE status = 'COMPLETED'
  AND completed_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY day
ORDER BY day DESC;
```

---

## Panel 7 — Security & Drift Audit

**Goal 6 — detect unexpected tasks, procedures, or grants.**

This is the Option A monitoring from the earlier conversation about
`CREATE TASK` privilege.

### 7a. Allowlist check — tasks

```sql
WITH expected AS (
    SELECT 'SUBMIT_BATCH_TASK'   AS task_name UNION ALL
    SELECT 'RETRIEVE_BATCH_TASK'
),
actual AS (
    SELECT UPPER(name) AS task_name
    FROM TABLE(snowflake.information_schema.tasks(IN_SCHEMA => 'datalake.llm_enrichments'))
)
SELECT a.task_name AS unexpected_task
FROM actual a
LEFT JOIN expected e ON a.task_name = e.task_name
WHERE e.task_name IS NULL;
```

**Any row returned is a security event** — an unexpected task has been
created in the schema. Trigger an alert.

### 7b. Allowlist check — stored procedures

```sql
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
WHERE e.proc_name IS NULL;
```

### 7c. Recent privilege grants on EAI / secret

```sql
SELECT
    created_on,
    privilege,
    granted_on,
    name           AS object_name,
    granted_to,
    grantee_name,
    granted_by
FROM snowflake.account_usage.grants_to_roles
WHERE granted_on IN ('INTEGRATION', 'SECRET')
  AND name IN ('AZURE_OPENAI_EAI', 'AZURE_OPENAI_KEY')
  AND created_on >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY created_on DESC;
```

`account_usage.grants_to_roles` has up to 2h latency — fine for daily
audit, not for real-time alerting.

### 7d. Who has executed the enrichment procedures recently

```sql
SELECT
    start_time,
    user_name,
    role_name,
    LEFT(query_text, 200) AS query_preview,
    execution_status
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND (
      query_text ILIKE '%submit_batch_sp%'
      OR query_text ILIKE '%retrieve_batch_sp%'
      OR query_text ILIKE '%execute task%batch%'
  )
ORDER BY start_time DESC;
```

Useful for incident forensics and for confirming smoke tests ran under
the expected role.

---

## Streamlit Dashboard Layout (Suggested)

| Page | Panels | Refresh cadence |
|---|---|---|
| **Overview** | 1c (heartbeat), 2a (queue depth), 2d (in flight), 4a (outcome donut) | 5 min |
| **Throughput** | 2b (age buckets), 2c (daily submissions), 6b (daily completions) | 1 hour |
| **Latency** | 3b (p50/p90/p99 cards), 3a (scatter plot), 3c (stuck list) | 15 min |
| **Quality** | 4a, 4b (recent parse errors), 4c (failing-field breakdown) | 1 hour |
| **Failures** | 5a (failed batch table), 5b (failure category bar), 5c (orphan rows) | 15 min |
| **Cost** | 6a (Snowflake credits), 6b (rows completed) | 1 day |
| **Audit** | 7a, 7b (allowlist checks), 7c, 7d (recent privileged activity) | 1 day |

### Practical caching notes

- `enrichment_queue` is the most expensive object on the page (multi-fact
  join). Cache `COUNT(*)` results in Streamlit for ≥60s. Prefer the age
  bucket query (2b) to a naked `SELECT *`.
- `task_history` and `query_history` from `information_schema` are cheap;
  the `account_usage` versions are slower but cover more history (and have
  the documented 45min–3h latency).
- Tag every Streamlit query with `ALTER SESSION SET QUERY_TAG =
  'streamlit_enrichment_dashboard'` so dashboard cost can be carved out of
  6a if needed.

---

## Alert Rules (Recommended)

Implement as Snowflake alerts (`CREATE ALERT`) or via the Streamlit job
itself if you want everything in one place.

| Rule | Condition | Severity | Source query |
|---|---|---|---|
| Submit task silent | minutes since last SUCCEEDED > 180 | P1 | 1c |
| Retrieve task silent | minutes since last SUCCEEDED > 60 | P1 | 1c |
| Stuck batch | any row in 3c | P1 | 3c |
| Queue growth | `3d+` bucket in 2b > 0 | P2 | 2b |
| Parse error rate spike | `parse_error` pct in 4a > 5% over 24h | P2 | 4a |
| Unexpected task created | any row in 7a | P1 (security) | 7a |
| Unexpected procedure created | any row in 7b | P1 (security) | 7b |
| New EAI/secret grant | any row in 7c in last day | P2 (security) | 7c |

---

## Known Gaps

These are deliberately out of scope for v1 monitoring; flag them if they
become operationally relevant:

- **No Azure-side spend integration.** Token-level cost lives in the Azure
  portal. Long-term, surface it via an Azure metrics export → Snowflake.
- **No per-field drift detection.** If the LLM starts returning subtly
  different values for `engagement_trajectory` (e.g. casing changes), the
  validation will catch it but trend analysis won't. Add value-distribution
  panels per field once there's enough volume.
- **No upstream queue source-of-truth monitoring.** We watch the
  `enrichment_queue` view, not `fact_conversation_messages`. If the
  upstream stops producing data we'd see the queue drain to zero, which
  could be misread as "everything is processed".
