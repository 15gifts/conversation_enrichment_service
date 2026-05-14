# Backlog Management Runbook

How `enrichment_queue` behaves when conversations arrive faster than the pipeline can enrich them, and what to do about it.

---

## Mental model

`enrichment_queue` is a **view**, not a table. It is recomputed on every query as:

> all conversations in `{analytics_db}.info_general.fact_conversation_messages`
> **minus** those already in `enrichment_results`
> **minus** those in-flight in `batch_row_mapping` (status `PENDING`/`SUBMITTING`/`SUBMITTED`/`IN_PROGRESS`)

Consequences:

- **No persistent backlog storage.** Backlog growth costs nothing in disk.
- **No TTL / expiry.** A conversation eligible today is still eligible in six months.
- **No data loss.** Eligibility is sticky until the conversation lands in `enrichment_results`.
- **No "queued" status.** A row is either in the view or it has moved on. There is no intermediate state to monitor.

Throughput is gated by two procedure arguments, set per-task in [`sql/07_tasks.sql`](sql/07_tasks.sql):

| Arg | Pilot value | Effect |
|---|---|---|
| `CHUNK_SIZE` | 10 | Max conversations per Azure batch |
| `MAX_ACTIVE_BATCHES` | 1 | Max concurrent batches in flight |

Real throughput ≈ `CHUNK_SIZE × MAX_ACTIVE_BATCHES` per Azure SLA window (up to 24h per batch).

---

## Known sharp edges

### 1. FIFO is enforced by the submit query

The view declares `ORDER BY conversation_started_at ASC`, but Snowflake does **not** guarantee that order propagates through a `LIMIT` in the consumer. To guarantee oldest-first selection under backlog, the submit handler issues its own explicit `ORDER BY conversation_started_at ASC` before the `LIMIT`. See [`submit.py:_fetch_queue`](src/batch_enrichment/submit.py).

If you ever change the view shape or the consumer query, re-verify FIFO. Without it, under heavy backlog you can end up enriching new conversations while old ones languish indefinitely.

### 2. View cost grows linearly with backlog

Every submit-task run does a full scan + LISTAGG + JOIN over `fact_conversation_messages`. With small backlogs this is milliseconds. With hundreds of thousands of pending conversations the submit task's own SQL becomes the slow part of the run.

If submit-task duration starts climbing past ~30s, that's the signal to scale up — not a sign of a bug.

### 3. `LISTAGG` 16MB cap is silent

A pathologically long transcript (large support thread, etc.) gets truncated to 16MB inside `LISTAGG(...) WITHIN GROUP` with no error raised. Documented in [`sql/03_views.sql`](sql/03_views.sql). Phase 2 work: add a `SUBSTR` guard and a `transcript_truncated` boolean signal on the view.

Backlog doesn't cause truncation, but pathological transcripts will keep being selected on every run and keep being truncated identically.

### 4. No upstream backpressure

Whatever is feeding `fact_conversation_messages` (Fivetran/Kafka/COPY INTO) has no signal that enrichment is falling behind. That's the correct architectural choice — enrichment is asynchronous — but it means the **only** feedback loop is monitoring queue depth here. Don't expect ingestion to slow down by itself.

---

## Monitoring

Add this query to whatever monitoring surface you use (Omni dashboard, scheduled task with Slack webhook, etc.):

```sql
SELECT
    COUNT(*)                          AS queue_depth,
    MIN(conversation_started_at)      AS oldest_pending,
    DATEDIFF('hour', MIN(conversation_started_at), CURRENT_TIMESTAMP())
                                       AS oldest_pending_hours,
    AVG(DATEDIFF('hour', conversation_started_at, CURRENT_TIMESTAMP()))
                                       AS avg_age_hours
FROM datalake.llm_enrichments.enrichment_queue;
```

Suggested alert thresholds for the pilot:

| Signal | Warn | Page |
|---|---|---|
| `queue_depth` | > 1,000 | > 10,000 |
| `oldest_pending_hours` | > 48 | > 168 (7 days) |

These thresholds assume pilot settings (`CHUNK_SIZE=10`, `MAX_ACTIVE_BATCHES=1`). Re-tune after scaling.

Also worth tracking in the same dashboard:

```sql
-- in-flight batches
SELECT status, COUNT(*) FROM datalake.llm_enrichments.batch_tracking
WHERE status IN ('SUBMITTING','SUBMITTED','IN_PROGRESS','FAILED','RETRYING')
GROUP BY status;

-- recent enrichment throughput (rows/hour for last 24h)
SELECT
    DATE_TRUNC('hour', enriched_at) AS hour,
    COUNT(*)                         AS rows_enriched,
    SUM(CASE WHEN parse_error THEN 1 ELSE 0 END) AS rows_failed
FROM datalake.llm_enrichments.enrichment_results
WHERE enriched_at > DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY 1 DESC;
```

---

## Scaling playbook

When `queue_depth` is growing and you need more throughput, scale **in this order**. After each step, wait at least 24h (one full Azure SLA window) before deciding whether the next step is needed.

### Step 1 — Raise `CHUNK_SIZE` first

Azure Batch API is priced per-token, not per-batch. A batch of 500 conversations costs the same as 50 batches of 10 conversations. Always saturate batch size before adding concurrency.

Target values:

| Phase | `CHUNK_SIZE` |
|---|---|
| Pilot | 10 |
| Steady state | 500–2,000 |
| High volume | 5,000+ |

Practical ceiling: Azure's batch input file limit is 200MB. A typical conversation transcript at ~500 tokens × 4 bytes ≈ 2KB serialised, so ~100,000 rows per file is the hard limit. Stay well below — large input files also mean slower upload and validation.

Update the task DDL:

```sql
ALTER TASK submit_batch_task SUSPEND;
CREATE OR REPLACE TASK submit_batch_task
    WAREHOUSE = enrichment_wh
    SCHEDULE = 'USING CRON 0 */2 * * * UTC'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
AS
CALL submit_batch_sp(
    500,                                                           -- CHUNK_SIZE (was 10)
    1,                                                             -- MAX_ACTIVE_BATCHES
    'gpt-4.1-mini-2025-04-14',
    'v1.0',
    'prd_analytics',
    'https://humara-oai-transcript-insights-ncus.openai.azure.com'
);
ALTER TASK submit_batch_task RESUME;
```

### Step 2 — Raise `MAX_ACTIVE_BATCHES`

Once `CHUNK_SIZE` is at a reasonable steady-state value and you still have backlog, allow concurrent batches.

| Phase | `MAX_ACTIVE_BATCHES` |
|---|---|
| Pilot | 1 |
| Steady state | 3–5 |
| High volume | 10+ |

Concurrent batches mean concurrent retrieves — confirm the retrieve task's MERGE can handle multiple completing batches in one run before going past ~5. (It can today; the loop in `retrieve_batch` iterates active batches sequentially.)

### Step 3 — Tighten the cron

Only useful if `MAX_ACTIVE_BATCHES > 1`. With a single active batch slot, running the task every 30 minutes vs every 2 hours makes no difference — most runs will be skipped with "max_active_batches reached".

If `MAX_ACTIVE_BATCHES = 5` and `CHUNK_SIZE = 500`, a 30-minute cron lets you fill all 5 slots within ~2.5 hours from a cold start.

```sql
ALTER TASK submit_batch_task SET SCHEDULE = 'USING CRON */30 * * * * UTC';
```

### Step 4 — Accept the floor

After all the above, sustained throughput is bounded by **Azure batch SLA × concurrent jobs × chunk size**. With `MAX_ACTIVE_BATCHES=10` and `CHUNK_SIZE=2000`, peak is ~20,000 conversations per Azure SLA window (up to 24h). If ingestion is faster than that, either:

- Raise Azure quota with Microsoft (the only real option)
- Accept that enrichment lags real-time by days
- Sample / prioritise — change the queue view to enrich only a subset (e.g. recent conversations, or only those matching certain criteria), and accept that older / lower-priority rows never get enriched

---

## Triage flowchart

```
queue_depth alert fires
    │
    ├── Is anything stuck in batch_tracking?
    │     • status='FAILED' rows with old failed_at?  →  Investigate root cause via error_message,
    │                                                     then either resubmit (reset to SUBMITTED)
    │                                                     or mark PERMANENTLY_FAILED.
    │     • status='SUBMITTED'/'IN_PROGRESS' older than 36h?
    │                                                  →  Check Azure portal; likely an Azure-side
    │                                                     stuck batch. May need manual status update.
    │
    ├── Is the submit task running?
    │     SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    │         TASK_NAME => 'SUBMIT_BATCH_TASK')) ORDER BY scheduled_time DESC LIMIT 10;
    │     • SUSPENDED? Resume it.
    │     • Failing repeatedly? Check error column; commonly the Azure key, network rule,
    │       or model deployment name has drifted.
    │
    └── Pipeline healthy, just under-provisioned?
          →  Follow the scaling playbook above. Always Step 1 (raise CHUNK_SIZE) first.
```

---

## Resetting a stuck batch (operational)

If a batch is stuck in `SUBMITTED`/`IN_PROGRESS` and Azure shows it as failed/expired:

```sql
-- Mark the tracking row failed so retrieve stops polling it
UPDATE datalake.llm_enrichments.batch_tracking
SET status = 'FAILED',
    error_message = 'Manually marked failed — Azure batch <azure_batch_id> expired/cancelled',
    failed_at = CURRENT_TIMESTAMP()
WHERE batch_tracking_id = '<uuid>';

-- Free the conversations so they re-queue
UPDATE datalake.llm_enrichments.batch_row_mapping
SET batch_status = 'FAILED'
WHERE batch_tracking_id = '<uuid>'
  AND batch_status IN ('PENDING','SUBMITTING','SUBMITTED','IN_PROGRESS');

-- Verify those conversations now appear back in the queue
SELECT COUNT(*) FROM datalake.llm_enrichments.enrichment_queue
WHERE conversation_id IN (
    SELECT conversation_id FROM datalake.llm_enrichments.batch_row_mapping
    WHERE batch_tracking_id = '<uuid>'
);
```

Note: `enrichment_queue` excludes mappings only when status is `PENDING/SUBMITTING/SUBMITTED/IN_PROGRESS`. Marking them `FAILED` reopens them for enrichment.

---

## Known issues / follow-ups

- **String interpolation in MERGE/INSERT statements.** Today's SQL builders escape single quotes and backslashes but parameterised binds would be cleaner. Worth its own ticket. Not a backlog concern but related to general pipeline robustness.
- **`LISTAGG` 16MB silent truncation.** Phase 2 — add explicit length guard + signal column.
- **No per-conversation prioritisation.** All eligible conversations are FIFO by start time. If business priority deviates from chronology (e.g. enrich VIP customer conversations first), the queue view needs a priority dimension.
- **No alerting infrastructure.** This runbook describes the queries; nothing automatically runs them yet. Wire up via Snowflake Task → external function → Slack, or via the dbt mart layer and Omni alerts.
