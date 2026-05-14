USE SCHEMA datalake.llm_enrichments;

-- ---------------------------------------------------------------------------
-- Tasks are created SUSPENDED for Phase 1 (pilot manual execution only).
-- Enable for Phase 2 automated scheduling:
--   ALTER TASK submit_batch_task RESUME;
--   ALTER TASK retrieve_batch_task RESUME;
--
-- Before resuming: confirm ALLOW_OVERLAPPING_EXECUTION = FALSE is set and
-- the runbook rule "no concurrent manual CALL submit_batch_sp()" is enforced.
-- ---------------------------------------------------------------------------

-- Submit task: runs every 2 hours
-- AZURE_ENDPOINT host must match the hostname allowed by azure_openai_network_rule.
-- AZURE_API_KEY is NOT passed here — the procedure reads it from the bound
-- `azure_api_key` secret (see SECRETS clause on submit_batch_sp).
CREATE OR REPLACE TASK submit_batch_task
    WAREHOUSE = enrichment_wh
    SCHEDULE = 'USING CRON 0 */2 * * * UTC'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    COMMENT = 'Submits enrichment batches to Azure OpenAI every 2 hours (AE-1540)'
AS
CALL submit_batch_sp(
    10,                                                                -- CHUNK_SIZE (Phase 1 pilot; increase for Phase 2)
    1,                                                                 -- MAX_ACTIVE_BATCHES
    'gpt-4.1-mini-2025-04-14',                                         -- MODEL_DEPLOYMENT
    'v1.0',                                                            -- PROMPT_VERSION
    'prd_analytics',                                                  -- ANALYTICS_DB (update to match target environment)
    'https://humara-oai-transcript-insights-ncus.openai.azure.com'     -- AZURE_ENDPOINT
);

-- Suspend immediately after creation (Phase 1 — manual execution only)
ALTER TASK submit_batch_task SUSPEND;

---

-- Retrieve task: polls every 30 minutes
CREATE OR REPLACE TASK retrieve_batch_task
    WAREHOUSE = enrichment_wh
    SCHEDULE = 'USING CRON */30 * * * * UTC'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    COMMENT = 'Retrieves completed enrichment batches from Azure OpenAI every 30 minutes (AE-1540)'
AS
-- AZURE_ENDPOINT host must match the hostname allowed by azure_openai_network_rule.
-- AZURE_API_KEY is NOT passed here — the procedure reads it from the bound
-- `azure_api_key` secret (see SECRETS clause on retrieve_batch_sp).
CALL retrieve_batch_sp(
    'https://humara-oai-transcript-insights-ncus.openai.azure.com',    -- AZURE_ENDPOINT
    'v1.0'                                                             -- PROMPT_VERSION
);

-- Suspend immediately after creation (Phase 1 — manual execution only)
ALTER TASK retrieve_batch_task SUSPEND;
