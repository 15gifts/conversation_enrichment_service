USE ROLE llm_enrichment_role;
USE WAREHOUSE enrichment_wh;
USE SCHEMA datalake.llm_enrichments;

-- ---------------------------------------------------------------------------
-- enrichment_queue: conversations eligible for enrichment
--
-- Excludes:
--   1. Conversations already in enrichment_results — this covers BOTH successes
--      AND terminal per-row failures (parse_error=TRUE with failure_reason set).
--      Guardrail-blocked rows (failure_reason='guardrail') and malformed-output
--      rows (failure_reason='parse_error') are intentionally NOT retried —
--      re-running the same input through the same prompt yields the same
--      refusal or the same bad output. Analysts can query
--      enrichment_results WHERE failure_reason IS NOT NULL to audit rejects.
--   2. Conversations in batch_row_mapping with an active status (in-flight)
--      Active statuses: PENDING, SUBMITTING, SUBMITTED, IN_PROGRESS
--      FAILED rows are included — they are retry-eligible (whole-batch failure,
--      e.g. submit-side network error, not a per-row Azure rejection).
--
-- Cross-database reference: set the Snowflake session variable analytics_db
-- to the target analytics database before running this DDL, e.g.:
--   SET analytics_db = 'prod_analytics';
--   CREATE OR REPLACE VIEW enrichment_queue AS ...
-- Or replace {analytics_db} with the literal database name for your environment.
--
-- LISTAGG limit: Snowflake LISTAGG has a 16MB per-group result size limit.
-- Transcripts approaching this limit will be silently truncated — no error is raised.
-- Risk is LOW for the pilot (10 rows). Phase 2 must add a SUBSTR guard and length signal.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW enrichment_queue AS
SELECT
    c.conversation_id,
    LISTAGG(c.message_sent_by || ': ' || COALESCE(c.message_text_combined, ''), '\n')
        WITHIN GROUP (ORDER BY c.conversation_message_num) AS transcript_text,
    MIN(c.event_at) AS conversation_started_at,
    max(case when fact_events.event_hkey is not null then 1 else 0 end) as exit_to_purchase,
    max(case when fact_sales.sale_hkey is not null then 1 else 0 end) as sale
FROM IDENTIFIER('prd_analytics.info_general.fact_conversation_messages') AS c
left join IDENTIFIER('prd_analytics.info_general.fact_events') AS fact_events
    on
      c.conversation_hkey = fact_events.conversation_hkey
      and fact_events.event_name = 'exit_to_purchase-completed'
left join IDENTIFIER('prd_analytics.info_general.fact_sales') AS fact_sales
    on
      fact_sales.customer_hkey = c.customer_hkey
      and fact_sales.user_hkey = c.user_hkey
      and fact_sales.event_at > c.event_at
      and datediff(day, c.event_at, fact_sales.event_at) <= 30
LEFT JOIN enrichment_results er
    ON c.conversation_id = er.conversation_id
LEFT JOIN batch_row_mapping brm
    ON c.conversation_id = brm.conversation_id
    AND brm.batch_status IN ('PENDING', 'SUBMITTING', 'SUBMITTED', 'IN_PROGRESS')
WHERE er.conversation_id IS NULL
  AND brm.conversation_id IS NULL
GROUP BY c.conversation_id
ORDER BY conversation_started_at ASC;
