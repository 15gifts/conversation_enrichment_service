-- Schema DATALAKE.LLM_ENRICHMENTS is managed by Terraform in the datawarehouse repo:
--   terraform/schemas.tf  →  resource "snowflake_schema" "llm_enrichments"
-- Run `terraform apply` in the datawarehouse repo before running this script.

USE ROLE llm_enrichment_role;
USE WAREHOUSE enrichment_wh;
USE SCHEMA datalake.llm_enrichments;

-- ---------------------------------------------------------------------------
-- batch_tracking: central state machine for every Azure batch job
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_tracking (
    batch_tracking_id    VARCHAR         DEFAULT UUID_STRING()  NOT NULL,
    azure_batch_id       VARCHAR,
    azure_input_file_id  VARCHAR,
    azure_output_file_id VARCHAR,
    status               VARCHAR         DEFAULT 'PENDING'      NOT NULL,
    -- valid (Phase 1): PENDING | SUBMITTING | SUBMITTED | IN_PROGRESS | COMPLETED | FAILED
    -- Phase 2 additions: RETRYING | PERMANENTLY_FAILED
    row_count            INTEGER,
    chunk_index          INTEGER,
    total_chunks         INTEGER,
    model_deployment     VARCHAR,
    prompt_version       VARCHAR,
    -- config_loaded_at: SCD2 timestamp captured from the datawarehouse _act
    -- satellites at submit time (max load_datetime across
    -- sat_google_sheets__enrichment_field_config_act and
    -- sat_google_sheets__enrichment_context_config_act). Identifies the exact
    -- config snapshot used for this batch; reproduce via the `_hist AS OF`
    -- query in docs/maintenance.md.
    config_loaded_at     TIMESTAMP_NTZ,
    submitted_at         TIMESTAMP_NTZ,
    completed_at         TIMESTAMP_NTZ,
    failed_at            TIMESTAMP_NTZ,
    retry_count          INTEGER         DEFAULT 0,
    max_retries          INTEGER         DEFAULT 3,
    error_message        VARCHAR,
    error_code           VARCHAR,
    cost_estimate_usd    FLOAT,
    created_at           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()  NOT NULL,
    updated_at           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()  NOT NULL,
    CONSTRAINT pk_batch_tracking PRIMARY KEY (batch_tracking_id)
);

-- ---------------------------------------------------------------------------
-- batch_row_mapping: prevents double-processing of conversations
-- Written BEFORE the Azure API call — this is the idempotency guard.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_row_mapping (
    conversation_id    VARCHAR         NOT NULL,
    batch_tracking_id  VARCHAR         NOT NULL,
    batch_status       VARCHAR         DEFAULT 'PENDING'  NOT NULL,
    created_at         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()  NOT NULL,
    CONSTRAINT pk_batch_row_mapping PRIMARY KEY (conversation_id, batch_tracking_id)
);

-- Config tables (enrichment_field_config, enrichment_context_config) are no
-- longer defined here. They are now sourced from Google Sheets via Fivetran,
-- routed through the datawarehouse Data Vault, and exposed as:
--   datawarehouse.<schema>.sat_google_sheets__enrichment_field_config_act
--   datawarehouse.<schema>.sat_google_sheets__enrichment_context_config_act
-- The stored procedures read these _act satellites directly. See
-- docs/maintenance.md for the historical-config reproducibility query.

-- ---------------------------------------------------------------------------
-- enrichment_results: parsed LLM output per conversation.
-- parsed_fields (VARIANT) stores all dynamic output fields as JSON — schema
-- is defined by enrichment_field_config so DDL never needs to change when
-- fields are added or removed.
-- UNIQUE on (conversation_id, prompt_version) enforced at application layer
-- via MERGE in retrieve procedure — prevents silent duplicates on double-retrieval.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_results (
    enrichment_id       VARCHAR         DEFAULT UUID_STRING()  NOT NULL,
    conversation_id     VARCHAR                                NOT NULL,
    prompt_version      VARCHAR                                NOT NULL,
    parsed_fields       VARIANT,        -- all dynamic LLM output fields as JSON object
    raw_response        VARIANT,        -- full Azure response object; access restricted to LLM_ENRICHMENT_ADMIN
    parse_error         BOOLEAN         DEFAULT FALSE  NOT NULL,
    parse_error_message VARCHAR,
    -- failure_reason classifies WHY a row failed enrichment, so analytics can
    -- distinguish guardrail blocks (non-retryable, model refused on policy grounds)
    -- from transient Azure errors and from downstream JSON / validation failures.
    -- Values: 'guardrail' | 'azure_error' | 'parse_error' | NULL (success)
    failure_reason      VARCHAR,
    batch_tracking_id   VARCHAR                        NOT NULL,
    -- config_loaded_at: propagated from batch_tracking; identifies the config
    -- snapshot used to produce this row. Reproduce the exact config via
    -- docs/maintenance.md.
    config_loaded_at    TIMESTAMP_NTZ,
    enriched_at         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()  NOT NULL,
    CONSTRAINT pk_enrichment_results PRIMARY KEY (enrichment_id)
);
