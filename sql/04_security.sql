-- ---------------------------------------------------------------------------
-- ROLE CONTEXT NOTE
-- CREATE NETWORK RULE, CREATE SECRET, and CREATE EXTERNAL ACCESS INTEGRATION
-- require ACCOUNTADMIN (or a role granted CREATE INTEGRATION privilege at the
-- account level). Switch to ACCOUNTADMIN for the DDL blocks below, then switch
-- back to llm_enrichment_role for the GRANT statements at the bottom.
--
--   Step 1: run as ACCOUNTADMIN
USE ROLE accountadmin;
USE WAREHOUSE enrichment_wh;
USE SCHEMA datalake.llm_enrichments;
--
--   Step 2: after the GRANTs, optionally switch back:
--   USE ROLE llm_enrichment_role;
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Azure OpenAI API key secret (placeholder)
-- Update with real key before Step 22:
--   ALTER SECRET azure_openai_key SET SECRET_STRING = '<real-api-key>';
-- ---------------------------------------------------------------------------
CREATE SECRET IF NOT EXISTS azure_openai_key
    TYPE = GENERIC_STRING
    SECRET_STRING = 'PLACEHOLDER'
    COMMENT = 'Azure OpenAI API key for batch enrichment pipeline (AE-1540). Set via ALTER SECRET before use.';

-- ---------------------------------------------------------------------------
-- Network rule: restricts egress to the Azure OpenAI endpoint only.
-- TYPE = HOST_PORT requires host (or host:port) — NOT a full URL with scheme,
-- path, or query string. The path/query belongs in the HTTP request issued by
-- the stored procedure, not here.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE NETWORK RULE azure_openai_network_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('humara-oai-transcript-insights-ncus.openai.azure.com')
    COMMENT = 'Restricts Snowpark egress to the Azure OpenAI endpoint (AE-1540).';

-- ---------------------------------------------------------------------------
-- External Access Integration: binds the network rule + secret
-- Grants stored procedures permission to make outbound HTTP calls to Azure.
-- Requires ACCOUNTADMIN or a role with CREATE INTEGRATION privilege.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION azure_openai_eai
    ALLOWED_NETWORK_RULES = (azure_openai_network_rule)
    ALLOWED_AUTHENTICATION_SECRETS = (azure_openai_key)
    ENABLED = TRUE
    COMMENT = 'External Access Integration for Azure OpenAI batch enrichment (AE-1540)';

-- ---------------------------------------------------------------------------
-- Role and grants
--
-- LLM_ENRICHMENT_ADMIN is managed by Terraform in the datawarehouse repo:
--   terraform/_locals.tf  →  local.roles.llm_enrichment_role
--   terraform/roles.tf
--
-- It inherits DATALAKE_RW_ROLE (via roles_to_grant), which provides:
--   • USAGE + CREATE SCHEMA on DATALAKE
--   • future_schema_grants on DATALAKE covering CREATE TABLE / VIEW /
--     PROCEDURE / STAGE / SECRET / NETWORK RULE / TASK / USAGE on any new
--     schema — automatically applied to LLM_ENRICHMENTS once it's created.
--
-- Tables and views created by this role are owned by it, so DML privileges
-- follow from ownership and don't need to be granted explicitly.
--
-- The grants below cover objects with no Terraform resource type — they
-- must remain here:
--   • External Access Integration
--   • Secret
-- ---------------------------------------------------------------------------
GRANT USAGE ON INTEGRATION azure_openai_eai TO ROLE llm_enrichment_role;
GRANT READ ON SECRET azure_openai_key TO ROLE llm_enrichment_role;
