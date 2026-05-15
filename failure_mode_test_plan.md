# Failure Mode Test Plan — Batch LLM Enrichment Pipeline (AE-1540)

A structured way to find out how this pipeline breaks **before** it breaks
in production. Each test below has:

- **Trigger** — how to simulate the failure (concrete SQL / actions)
- **Expect** — what the system should do
- **Verify** — what to check on the dashboard, in Snowflake, and in
  `BATCH_TRACKING` / `ENRICHMENT_RESULTS`
- **Recover** — how to undo the test cleanly

Run these in a **non-production** schema if possible, or during a quiet
window. Several tests deliberately produce `FAILED` rows in
`BATCH_TRACKING` — keep that in mind for downstream analytics.

Group rows below by category. Priority ranks how often this failure mode
shows up in production: **P1** = inevitable, **P2** = common, **P3** =
edge case.

---

## 1. Task / scheduler failures

### 1.1 — Submit task stopped firing (P1)

**Trigger:**
```sql
USE ROLE llm_enrichment_role;
ALTER TASK datalake.llm_enrichments.submit_batch_task SUSPEND;
-- Leave it suspended for the duration of one cron window (>2 h),
-- or fast-forward by inserting a synthetic delay row.
```

**Expect:**
- Queue depth grows.
- Active batch count stays flat (no new submissions).
- Existing in-flight batches still complete via the retrieve task.

**Verify:**
- Dashboard **Overview** — "SUBMIT_BATCH_TASK" tile flips to `stale (>180 min)`.
- **Throughput** page — daily submissions drops to zero.
- **Throughput** page — queue age buckets shift right (`12-24h` and `1-3d` grow).
- SQL:
  ```sql
  SELECT minutes_since_last_run FROM TABLE(...)  -- per monitoring.md 1c
  ```

**Recover:**
```sql
ALTER TASK datalake.llm_enrichments.submit_batch_task RESUME;
-- Next cron firing will drain the backlog.
```

### 1.2 — Retrieve task stopped firing (P1)

**Trigger:**
```sql
ALTER TASK datalake.llm_enrichments.retrieve_batch_task SUSPEND;
```

**Expect:**
- Submit continues firing → active batch count climbs to `MAX_ACTIVE_BATCHES` ceiling, then plateaus.
- No new rows in `ENRICHMENT_RESULTS`.
- `BATCH_TRACKING.status` for in-flight batches stays at `SUBMITTED` / `IN_PROGRESS` past Azure's 24h SLA.

**Verify:**
- Dashboard **Overview** — `RETRIEVE_BATCH_TASK` tile goes stale (`>60 min`).
- **Latency** page — stuck-batch banner appears (rows past 26h SLA).
- **Throughput** page — submissions per day still healthy, completions drop to zero.

**Recover:**
```sql
ALTER TASK datalake.llm_enrichments.retrieve_batch_task RESUME;
-- Backlogged completed batches will be drained over subsequent firings.
```

### 1.3 — Task throws unhandled exception (P2)

**Trigger:** temporarily revoke USAGE on the EAI from the stored procedure's owner role:
```sql
USE ROLE accountadmin;
REVOKE USAGE ON INTEGRATION azure_openai_eai FROM ROLE llm_enrichment_role;
```

**Expect:** the next task firing fails with a permission error. The task itself stays `started` (Snowflake doesn't auto-suspend on failure).

**Verify:**
- Dashboard **Overview** — heartbeat tile may still look OK (Snowflake records FAILED runs in `task_history`).
- Manual SQL on `task_history` (monitoring.md 1b) — `state = 'FAILED'` for recent runs.

**Recover:**
```sql
GRANT USAGE ON INTEGRATION azure_openai_eai TO ROLE llm_enrichment_role;
```

> **Gap identified:** dashboard doesn't currently surface FAILED task runs
> as prominently as silent runs — only the "minutes since last SUCCEEDED"
> tile catches it, and only once enough time passes. Consider adding a
> "recent task failures" tile to the Overview page.

---

## 2. Network / API failures

### 2.1 — Azure auth failure (P1)

**Trigger:** corrupt the secret so the next submission's API call returns 401.
```sql
USE ROLE accountadmin;
ALTER SECRET datalake.llm_enrichments.azure_openai_key
    SET SECRET_STRING = 'INVALID_KEY_FOR_CHAOS_TEST';
```

**Expect:** the submit procedure catches the HTTPError, marks the batch `FAILED`, attempts to clean up the uploaded input file, and inserts a `FAILED` row in `BATCH_TRACKING` with `error_message` containing `401` / `unauthorized`.

**Verify:**
- Dashboard **Failures** page — failure category bar shows `auth`.
- **Failures** page — failed-batches table shows the new row with the auth error.

**Recover:**
```sql
ALTER SECRET datalake.llm_enrichments.azure_openai_key
    SET SECRET_STRING = '<real key>';
```

### 2.2 — Azure rate limit / 429 (P2)

**Trigger:** hard to simulate without coordinating with the Azure team. Easiest proxy: set `MAX_ACTIVE_BATCHES = 100` on the submit task and let the pipeline burst against the rate limit naturally.

**Expect:** Azure returns 429 on submit → procedure marks the batch `FAILED` with rate-limit error → next cron firing tries again (the queue rows are still eligible because failed batches don't poison the queue).

**Verify:** **Failures** page → `rate_limit` category.

**Recover:** restore `MAX_ACTIVE_BATCHES` to its production value.

### 2.3 — Network rule blocks egress (P3)

**Trigger:** point the network rule at a hostname that doesn't exist:
```sql
USE ROLE accountadmin;
ALTER NETWORK RULE datalake.llm_enrichments.azure_openai_network_rule
    SET VALUE_LIST = ('does-not-exist.openai.azure.com');
```

**Expect:** next submission's HTTP call fails fast. Procedure marks batch `FAILED` with network/connection error.

**Verify:** **Failures** page → `network` category.

**Recover:** restore the original hostname.

### 2.4 — Network timeout mid-upload (P3)

Hard to inject without code changes. The current 30s timeout is the
safety net. If you want to verify the timeout path, temporarily set
`_DEFAULT_TIMEOUT_S = 0.001` in `submit.py`, rebuild the zip, redeploy,
and submit one batch — every call times out, batch is marked `FAILED`
with timeout error.

---

## 3. Scaling / volume failures

### 3.1 — Queue grows faster than throughput (P1)

**Trigger:** synthetically bulk-insert eligible queue rows. Easiest:
   suspend the submit task, let real data accumulate over a few cron
   windows, then resume.

Alternative (synthetic): insert N test conversations directly into
`fact_conversation_messages` with unique `conversation_id`s in a date
range that puts them in the queue. **Only do this in a non-production
analytics DB.**

**Expect:** submit task can only drain at `CHUNK_SIZE × cron_frequency`
rows per period. If ingest exceeds that, queue grows.

**Verify:**
- Dashboard **Throughput** — age-bucket bars shift right. `3d+` bucket appearing is the canary.
- **Overview** — queue-depth tile shows non-zero "> 3 days old" delta.
- Cross-reference with `backlog_management_runbook.md` "Triage flowchart".

**Recover:** scale per the runbook:
1. Bump `CHUNK_SIZE` (first lever — more rows per batch, same number of batches)
2. Bump `MAX_ACTIVE_BATCHES` (more batches in flight concurrently)
3. Tighten submit cron (last resort — increases Snowflake task cost)

### 3.2 — CHUNK_SIZE too high (payload too large) (P2)

**Trigger:** set CHUNK_SIZE to a value large enough that the JSONL payload exceeds Azure's per-file limit (currently 200 MB).
```sql
CALL submit_batch_sp(
    chunk_size => 50000,
    max_active_batches => 1,
    model_deployment => 'gpt-4.1-mini-2025-04-14',
    prompt_version => 'v1.0',
    analytics_db => 'prd_analytics',
    azure_endpoint => 'https://humara-oai-transcript-insights-ncus.openai.azure.com'
);
```

**Expect:** Azure rejects the upload with a size error. Procedure catches, marks `FAILED`, cleans up.

**Verify:** **Failures** page — `other` category with the size error visible in `error_preview`.

**Recover:** none needed — just don't set `CHUNK_SIZE` that high.

### 3.3 — LISTAGG silent truncation in `enrichment_queue` (P3)

**Trigger:** a single conversation with > 16 MB of concatenated message text. Realistically only happens with abusive / spam conversations.

**Expect:** Snowflake silently truncates the transcript. **No error.** The LLM gets a partial transcript and likely produces lower-quality output.

**Verify:** there's no automatic signal — this is a known monitoring gap noted in `sql/03_views.sql`. Manual check:
```sql
SELECT conversation_id, LENGTH(transcript_text) AS chars
FROM datalake.llm_enrichments.enrichment_queue
WHERE LENGTH(transcript_text) >= 16 * 1024 * 1024 - 1024
ORDER BY chars DESC
LIMIT 20;
```

**Recover:** Phase 2 work — add explicit `SUBSTR(...)` guard and a `truncated` flag column in the view.

---

## 4. Per-row data quality failures

These are mostly testable against real LLM behaviour; the pipeline already
handles them in production. Worth running to confirm the handling is sound.

### 4.1 — LLM returns malformed JSON (P1)

**Trigger:** organic — happens every few hundred rows. To force it, temporarily set the prompt's `response_format` to text. Or wait — you'll see them naturally.

**Expect:** `enrichment_results.parse_error = TRUE`, `failure_reason = 'parse_error'`, raw response stored in `raw_response` VARIANT for forensic analysis. Row is NOT retried (per the comment in `sql/03_views.sql`).

**Verify:** **Quality** page → outcome distribution shows `parse_error` count. Recent parse errors table shows the bad raw response.

### 4.2 — LLM hits content policy / guardrail (P1)

**Trigger:** organic — a small steady rate.

**Expect:** `enrichment_results.failure_reason = 'guardrail'`. Row stored as a "failed enrichment" so it doesn't get re-queued.

**Verify:** **Quality** page → `guardrail_block` slice in the outcome donut.

### 4.3 — All rows in a batch fail validation (P2)

**Trigger:** introduce a prompt change that violates the schema (e.g.
change the system prompt to instruct the LLM to return `"none"` for
`engagement_trajectory` when the schema requires `increasing` / `stable` /
`decreasing`). Bump `prompt_version` first so you can roll back cleanly.

**Expect:** the whole batch completes from Azure's perspective (status=`completed`), but every row hits validation failure in `response_parser.py`. `BATCH_TRACKING` shows `COMPLETED`, but `enrichment_results` has 100% `parse_error = TRUE` for that `prompt_version`.

**Verify:** **Quality** page → parse error rate alert fires (>= 5%).

### 4.4 — Empty transcript (P3)

**Trigger:** a conversation row with `message_text_combined` IS NULL or empty for every message.

**Expect:** `enrichment_queue` still produces a row (LISTAGG collapses to empty string). LLM either refuses or hallucinates. Either way the parse layer should catch the inconsistency.

**Verify:** find the conversation in `enrichment_results` and look at `parsed_fields`. If the LLM produced plausible-looking fields from no input, that's a model-side concern worth flagging.

---

## 5. State machine / idempotency failures

### 5.1 — Retrieve runs twice on the same completed batch (P1)

**Trigger:** while a batch is `COMPLETED`, manually invoke retrieve again:
```sql
EXECUTE TASK datalake.llm_enrichments.retrieve_batch_task;
-- Wait for it to finish, then run again immediately:
EXECUTE TASK datalake.llm_enrichments.retrieve_batch_task;
```

**Expect:** the `MERGE` into `enrichment_results` (per `retrieve.py`
`_merge_enrichment_results`) is a no-op the second time because the row
already exists for `(conversation_id, prompt_version)`. No duplicates.

**Verify:**
```sql
SELECT conversation_id, prompt_version, COUNT(*) AS dup_count
FROM datalake.llm_enrichments.enrichment_results
GROUP BY conversation_id, prompt_version
HAVING dup_count > 1;
-- Should return 0 rows.
```

### 5.2 — Submit crashes between API call and tracking insert (P2)

**Trigger:** simulate by manually inserting a `batch_row_mapping` row
without ever submitting to Azure:
```sql
INSERT INTO datalake.llm_enrichments.batch_row_mapping
    (conversation_id, batch_tracking_id, batch_status)
VALUES ('test-conv-1', 'ghost-batch-123', 'PENDING');
```

**Expect:** the conversation is now excluded from `enrichment_queue`
(because mapping has an active status). It's effectively orphaned — no
batch will ever process it.

**Verify:**
- The conversation does not appear in `enrichment_queue`.
- `BATCH_TRACKING` has no row for `ghost-batch-123` (no parent).

**Recover:** this is the failure mode the **Orphan rows** panel on the
Failures page catches:
```sql
-- Reset the orphan so it's retry-eligible:
UPDATE datalake.llm_enrichments.batch_row_mapping
SET batch_status = 'FAILED'
WHERE batch_tracking_id = 'ghost-batch-123';
```

> **Production gap:** the current submit procedure inserts mapping rows
> *before* the Azure API call. If submit dies after that insert but
> before completing the API call, this is exactly the orphan state. The
> recovery is the manual UPDATE above. Worth a Phase 2 reconciler job.

### 5.3 — Same conversation reaches submit twice (P3)

**Trigger:** insert two conversations with the same `conversation_id` into the upstream fact table. The queue should deduplicate, but the underlying view doesn't currently enforce uniqueness.

**Expect:** depending on race conditions, the conversation could be sent to two batches. The MERGE in retrieve will deduplicate at the `(conversation_id, prompt_version)` level — last write wins.

**Verify:** check the conversation in `enrichment_results`. Should be exactly one row per `(conversation_id, prompt_version)`.

---

## 6. Resource / privilege failures

### 6.1 — Warehouse suspended (P3)

**Trigger:**
```sql
ALTER WAREHOUSE enrichment_wh SUSPEND;
```

**Expect:** the task auto-resumes the warehouse on next firing (Snowflake default). Slight latency hit (~3–5s cold start). No batch should fail.

**Verify:** task_history shows normal SUCCEEDED. Latency on Latency page may show a small bump.

**Recover:** automatic.

### 6.2 — Secret deleted (P3)

**Trigger:**
```sql
USE ROLE accountadmin;
DROP SECRET IF EXISTS datalake.llm_enrichments.azure_openai_key;
```

**Expect:** procedure fails at `_snowflake.get_generic_secret_string(...)` with secret-not-found error. Batch is never created (because the failure happens before any tracking row is inserted).

**Verify:** task_history shows FAILED runs. No new rows in `BATCH_TRACKING`.

**Recover:**
```sql
CREATE SECRET IF NOT EXISTS azure_openai_key
    TYPE = GENERIC_STRING
    SECRET_STRING = '<real key>';
```

### 6.3 — Schema USAGE revoked from analytics_engineer (P3)

**Trigger:** revoke USAGE so the dashboard can no longer read the tables.

**Expect:** dashboard pages error out with "schema does not exist" on every query.

**Verify:** dashboard renders error tiles on every page.

**Recover:** restore the grant.

---

## 7. Adversarial / security tests

### 7.1 — Unexpected task created (P3 but high severity)

**Trigger:** create a noise task in the enrichment schema, as a role that has CREATE TASK there (i.e. `llm_enrichment_role`):
```sql
USE ROLE llm_enrichment_role;
CREATE TASK datalake.llm_enrichments.chaos_test_task
    WAREHOUSE = enrichment_wh
    SCHEDULE = '60 minute'
AS
    SELECT 1;
```

**Expect:** Audit page **Object allowlist → Tasks** tile flips to red with `chaos_test_task` listed.

**Verify:** Audit panel.

**Recover:**
```sql
DROP TASK datalake.llm_enrichments.chaos_test_task;
```

### 7.2 — Unexpected procedure created (P3)

**Trigger:**
```sql
USE ROLE llm_enrichment_role;
CREATE PROCEDURE datalake.llm_enrichments.chaos_test_sp() RETURNS INT
    LANGUAGE SQL AS $$ BEGIN RETURN 1; END; $$;
```

**Expect:** Audit page **Stored procedures** tile flips to red.

**Recover:**
```sql
DROP PROCEDURE datalake.llm_enrichments.chaos_test_sp();
```

### 7.3 — Unauthorised grant on EAI / secret (P3)

**Trigger:**
```sql
USE ROLE accountadmin;
GRANT USAGE ON INTEGRATION azure_openai_eai TO ROLE public;  -- never do this for real
```

**Expect:** Audit page **Grants on EAI / secret** panel shows the new grant (with up to 3h latency from `account_usage.grants_to_roles`).

**Recover:**
```sql
REVOKE USAGE ON INTEGRATION azure_openai_eai FROM ROLE public;
```

---

## Suggested run order for a first chaos session

If you've never run a chaos test on this pipeline before, work through in
this order. Each one tests something fundamentally different.

1. **1.1** — Suspend submit task. (Trivial; the most common production failure.)
2. **1.2** — Suspend retrieve task. (Inverse of 1.1; tests the second half.)
3. **2.1** — Corrupt the API key. (Tests the unhappy path of the Azure integration.)
4. **5.1** — Run retrieve twice. (Confirms the MERGE idempotency claim.)
5. **5.2** — Insert an orphan mapping row. (Confirms the Orphan-rows panel works.)
6. **7.1 + 7.2** — Create a chaos task and procedure. (Confirms the audit detection works.)
7. **3.1** — Queue scaling (optional, takes the longest because it's time-based).

Allow 5–10 minutes between tests for the dashboard to refresh past its
cache TTLs.

---

## What this plan does NOT cover

Out of scope for this iteration — flag if they become operationally relevant:

- **Long-running batch concurrency between submit and retrieve.** With
  `ALLOW_OVERLAPPING_EXECUTION = FALSE`, Snowflake guarantees no
  overlapping firings of the same task, but the **two different** tasks
  can run concurrently and operate on the same `BATCH_TRACKING` rows.
  The current code is safe under this race (status checks are scoped),
  but a thorough load test with parallel manual triggers would confirm.
- **Snowpark memory pressure on very large output JSONL** (>500 MB). The
  current implementation streams chunks (per `retrieve.py
  _download_output_file`), so this should be fine, but not load-tested.
- **dbt model failures.** Out of scope for the enrichment pipeline
  itself; dbt has its own observability stack.
- **Cost runaway.** Tracked on the dashboard's Cost page, but no
  automatic kill-switch exists. Phase 2 should consider a budget alert.
