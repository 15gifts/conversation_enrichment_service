Here's the end-to-end smoke test for one conversation. Run all of this as `LLM_ENRICHMENT_ROLE` from the `DATALAKE.LLM_ENRICHMENTS` context.

## Step 1 — Set the real API key (one-time, if not done)

The secret in `sql/04_security.sql` ships with `SECRET_STRING = 'PLACEHOLDER'`. Set the real key now:

```sql
ALTER SECRET azure_openai_key SET SECRET_STRING = '<real-azure-api-key>';
```

Verify by running `DESC SECRET azure_openai_key;` — you'll see metadata but not the value.

## Step 2 — Verify the queue has rows

The `enrichment_queue` view references `$analytics_db.info_general.fact_conversation_messages`. Set the session variable, then check:

```sql
SET analytics_db = 'prd_analytics';
SELECT COUNT(*) AS queue_depth FROM enrichment_queue;
SELECT * FROM enrichment_queue LIMIT 1;
```

If `queue_depth = 0` and you know unenriched conversations exist, something's wrong with the join — stop and debug before going further.

## Step 3 — Submit ONE conversation

`CHUNK_SIZE = 1` makes the submit procedure pull exactly one conversation off the queue (the oldest by `conversation_started_at`).

```sql
CALL submit_batch_sp(
    1,                                                                 -- CHUNK_SIZE = 1
    1,                                                                 -- MAX_ACTIVE_BATCHES
    'gpt-4.1-mini-2025-04-14',                                         -- MODEL_DEPLOYMENT
    'v1.0',                                                            -- PROMPT_VERSION
    'prd_analytics',                                                   -- ANALYTICS_DB
    'https://humara-oai-transcript-insights-ncus.openai.azure.com'     -- AZURE_ENDPOINT
);
```

Expected output (a VARIANT):
```json
{
  "batch_tracking_id": "<uuid>",
  "azure_batch_id": "batch_abc123...",
  "row_count": 1,
  "status": "SUBMITTED",
  "error_message": null
}
```

Record the `batch_tracking_id` — you'll use it for verification.

## Step 4 — Verify state was written correctly

```sql
-- Tracking row should be SUBMITTED
SELECT batch_tracking_id, azure_batch_id, status, row_count, prompt_version, submitted_at
FROM batch_tracking
ORDER BY submitted_at DESC LIMIT 1;

-- Row mapping should be SUBMITTED for the one conversation
SELECT conversation_id, batch_status, batch_tracking_id
FROM batch_row_mapping
ORDER BY created_at DESC LIMIT 5;

-- That conversation should no longer appear in the queue
SELECT COUNT(*) FROM enrichment_queue;  -- should be queue_depth - 1
```

## Step 5 — Wait for Azure, then retrieve

Azure's Batch API SLA is **up to 24h** but small one-row batches usually finish in 5–30 minutes. Poll Azure status before retrieving:

```sql
-- Optional: just retrieve directly — it polls Azure for each active batch
CALL retrieve_batch_sp(
    'https://humara-oai-transcript-insights-ncus.openai.azure.com',
    'v1.0'
);
```

Run this every ~5 minutes until the response shows `batches_completed: 1`:

```json
{
  "batches_checked": 1,
  "batches_completed": 1,
  "rows_written": 1,
  "parse_errors": 0,
  "guardrail_failures": 0
}
```

## Step 6 — Inspect the enriched result

```sql
SELECT
    conversation_id,
    prompt_version,
    parsed_fields,                          -- VARIANT with all 9 fields
    parsed_fields:conversation_summary::STRING       AS summary,
    parsed_fields:resolution_state::STRING           AS resolution,
    parsed_fields:conversation_sentiment::STRING     AS sentiment,
    parsed_fields:friction_types                      AS friction_types,
    parse_error,
    failure_reason,
    enriched_at
FROM enrichment_results
ORDER BY enriched_at DESC
LIMIT 1;

-- And the tracking row should now be COMPLETED
SELECT status, completed_at FROM batch_tracking
WHERE batch_tracking_id = '<the uuid from step 3>';

-- The mapping row should be COMPLETED
SELECT batch_status FROM batch_row_mapping
WHERE batch_tracking_id = '<the uuid from step 3>';
```

## If something goes wrong

| Symptom | Likely cause | Fix |
|---|---|---|
| `submit_batch_sp` returns `status="FAILED"` with `403`/`401` | API key not set | Re-run Step 1 |
| `submit_batch_sp` returns `status="FAILED"` with connection error | Network rule hostname mismatch | `DESC NETWORK RULE azure_openai_network_rule;` — confirm `humara-oai-transcript-insights-ncus.openai.azure.com` (no scheme, no path) |
| `submit_batch_sp` returns `status="PENDING"` with `"max_active_batches reached"` | A prior batch is still IN_PROGRESS | `SELECT * FROM batch_tracking WHERE status IN ('SUBMITTED','IN_PROGRESS');` — let it finish or manually mark stuck rows |
| `retrieve` shows `batches_completed: 0` but `batches_checked: 1` | Azure batch still processing | Wait and retry |
| `parse_error = TRUE` on the result row | LLM didn't return valid JSON or failed validation | Inspect `raw_response` and `parse_error_message` columns; `failure_reason` will say `parse_error` or `guardrail` |

## To clean up and re-test the same conversation

Because `enrichment_queue` excludes anything already in `enrichment_results`, you'd need to delete the result row to re-enrich:

```sql
DELETE FROM enrichment_results WHERE conversation_id = '<id>';
DELETE FROM batch_row_mapping WHERE conversation_id = '<id>';
-- the batch_tracking row can stay — it's history
```

Then the conversation reappears in the queue.