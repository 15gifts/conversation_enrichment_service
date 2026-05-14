USE SCHEMA datalake.llm_enrichments;

-- Upload the package as a zip before running this.
--
-- Snowflake adds the *directory containing each IMPORTS file* to sys.path,
-- not its parent. Individual .py files therefore cannot satisfy package-style
-- imports (`from batch_enrichment.models import ...`). The correct approach
-- is to zip the whole package and import the zip — Snowflake extracts it and
-- adds its root to sys.path so `batch_enrichment` becomes importable.
--
-- Build the zip (from repo root):
--   cd src && zip -r ../batch_enrichment.zip batch_enrichment/ && cd ..
--
-- Upload batch_enrichment.zip to the stage root (Path field blank in Snowsight).
-- Via SnowSQL:
--   PUT file://batch_enrichment.zip @batch_enrichment_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--
-- Verify: LIST @batch_enrichment_stage;

-- AZURE_API_KEY is NOT a parameter — the handler reads it from the bound
-- `azure_api_key` secret (see SECRETS clause). This keeps the key out of
-- task DDL, query history, and logs.
CREATE OR REPLACE PROCEDURE submit_batch_sp(
    CHUNK_SIZE        INTEGER,
    MAX_ACTIVE_BATCHES INTEGER,
    MODEL_DEPLOYMENT  VARCHAR,
    PROMPT_VERSION    VARCHAR,
    ANALYTICS_DB      VARCHAR,
    AZURE_ENDPOINT    VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pydantic')
IMPORTS = ('@batch_enrichment_stage/batch_enrichment.zip')
HANDLER = 'batch_enrichment.submit.submit_batch_handler'
EXTERNAL_ACCESS_INTEGRATIONS = (azure_openai_eai)
SECRETS = ('azure_api_key' = azure_openai_key)
COMMENT = 'Submits a batch of conversations to Azure OpenAI for enrichment (AE-1540)'
EXECUTE AS OWNER;
