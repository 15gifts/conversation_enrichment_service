USE ROLE llm_enrichment_role;
USE WAREHOUSE enrichment_wh;
USE SCHEMA datalake.llm_enrichments;

-- Internal stage for Python source files used by stored procedures.
-- Upload files with: PUT file://src/batch_enrichment/*.py @batch_enrichment_stage AUTO_COMPRESS=FALSE;
CREATE STAGE IF NOT EXISTS batch_enrichment_stage
    COMMENT = 'Python source files for batch enrichment stored procedures (AE-1540)';
