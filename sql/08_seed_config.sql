-- Seed enrichment_field_config and enrichment_context_config for config_version 'v1.0'.
-- This reproduces the Humara conversation analysis schema originally defined in
-- team_data/Humara/Insights/Conversation Summary/Create_file.py.
--
-- To introduce a new schema: INSERT rows with a new config_version and update
-- SUBMIT_BATCH_TASK / RETRIEVE_BATCH_TASK to pass the new version string.
-- Old rows are preserved for historical enrichment traceability.

USE ROLE llm_enrichment_role;
USE WAREHOUSE enrichment_wh;
USE SCHEMA datalake.llm_enrichments;

-- ---------------------------------------------------------------------------
-- Output fields — v1.0
-- ---------------------------------------------------------------------------
-- INSERT ... SELECT (not VALUES) — Snowflake VALUES clauses don't permit
-- function expressions like ARRAY_CONSTRUCT in the value list.
INSERT INTO enrichment_field_config
    (config_version, field_name, field_type, allowed_values, min_value, max_value,
     field_description, is_nullable, display_order)
SELECT 'v1.0', 'conversation_summary', 'string', NULL,
       NULL, NULL,
       'A brief summary of the conversation.',
       FALSE, 1
UNION ALL SELECT 'v1.0', 'engagement_trajectory', 'string_enum',
       ARRAY_CONSTRUCT('increasing', 'stable', 'decreasing'),
       NULL, NULL,
       'Overall direction of user engagement across the conversation.',
       FALSE, 2
UNION ALL SELECT 'v1.0', 'confusion_signals_detected', 'boolean', NULL,
       NULL, NULL,
       'TRUE if the user showed clear signs of confusion (repeated questions, contradictions, misunderstandings).',
       FALSE, 3
UNION ALL SELECT 'v1.0', 'friction_types', 'enum_array',
       ARRAY_CONSTRUCT('price', 'confidence', 'commitment', 'trust'),
       NULL, NULL,
       'Friction types observed. Return an empty array if none detected.',
       FALSE, 4
UNION ALL SELECT 'v1.0', 'purchase_readiness_reached', 'boolean', NULL,
       NULL, NULL,
       'TRUE if the user reached a point of genuine purchase readiness during the conversation.',
       FALSE, 5
UNION ALL SELECT 'v1.0', 'resolution_state', 'string_enum',
       ARRAY_CONSTRUCT('converted', 'exit_to_purchase', 'warm_abandon',
                       'frustrated_abandon', 'neutral_abandon'),
       NULL, NULL,
       'Outcome of the conversation. If resolution_state is already determined by context signals (sale or exit_to_purchase), use that value exactly.',
       FALSE, 6
UNION ALL SELECT 'v1.0', 'key_moment', 'string', NULL,
       NULL, NULL,
       'A short description of the message or exchange that most influenced the outcome.',
       FALSE, 7
UNION ALL SELECT 'v1.0', 'frustration_summary', 'string', NULL,
       NULL, NULL,
       'A brief summary of what the user was frustrated or angry about. Use null if no frustration was detected.',
       TRUE, 8
UNION ALL SELECT 'v1.0', 'conversation_sentiment', 'string_enum',
       ARRAY_CONSTRUCT('very positive', 'positive', 'neutral', 'negative', 'very negative'),
       NULL, NULL,
       'Overall sentiment of the conversation.',
       FALSE, 9;

-- ---------------------------------------------------------------------------
-- Context columns — v1.0
-- sale and exit_to_purchase are outcome signals from the enrichment_queue view.
-- They are passed to the LLM so it can deterministically resolve resolution_state.
-- ---------------------------------------------------------------------------
INSERT INTO enrichment_context_config
    (config_version, column_name, display_label, value_description, display_order)
VALUES
    ('v1.0', 'sale',
     'sale',
     '1 if the user made a purchase after this conversation, 0 if not.',
     1),

    ('v1.0', 'exit_to_purchase',
     'exit_to_purchase',
     '1 if the user left to purchase elsewhere after this conversation, 0 if not.',
     2);
