# Maintenance — batch_api_enrichment_service

## Reproducing the config for a historical batch

Every batch stamps `batch_tracking.config_loaded_at` at submit time. This is
the `max(load_datetime)` across the two `_act` satellites at the moment the
batch was sent to Azure, and it is the only authoritative pointer to the
exact config that produced the batch's enrichments.

To reconstruct the configs used for a specific `batch_tracking_id`, query the
SCD2 `_hist` tables in the datawarehouse intermediate database, filtering for
rows valid at `config_loaded_at`:

```sql
WITH b AS (
    SELECT batch_tracking_id, config_loaded_at
    FROM datalake.llm_enrichments.batch_tracking
    WHERE batch_tracking_id = :batch_tracking_id
)
SELECT
    'field' AS kind,
    f.*
FROM b
JOIN datawarehouse_intermediate.vault.sat_google_sheets__enrichment_field_config_hist AS f
    ON f.load_datetime <= b.config_loaded_at
   AND (f.valid_to IS NULL OR f.valid_to > b.config_loaded_at)
UNION ALL
SELECT
    'context' AS kind,
    c.*
FROM b
JOIN datawarehouse_intermediate.vault.sat_google_sheets__enrichment_context_config_hist AS c
    ON c.load_datetime <= b.config_loaded_at
   AND (c.valid_to IS NULL OR c.valid_to > b.config_loaded_at);
```

Notes:

- The `valid_to` predicate assumes the standard `automate_dv` historical
  satellite convention (`NULL` on the open record, an end-timestamp on closed
  ones). Adjust if the satellite definition in the datawarehouse repo
  diverges from that convention.
- The intermediate database name is environment-dependent
  (`prd_intermediate`, `dev_<user>_intermediate`, etc.) — substitute as
  appropriate.
- If `config_loaded_at` is `NULL` on the tracking row, the batch was
  submitted before this migration landed. Those batches used the legacy
  `datalake.llm_enrichments.enrichment_field_config` /
  `enrichment_context_config` tables, which were dropped in this ticket.
  Recover their state from the corresponding seed-file commit history.

## Versioning model

Two independent pointers identify what produced a given batch's results:

| Pointer                                  | What it versions                                              | Who sets it                                                        |
| ---------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------ |
| `batch_tracking.prompt_version`          | The prompt template string defined in this repo's Python code | Bumped manually in code when the template body changes             |
| `batch_tracking.config_loaded_at`        | The Google-Sheet config snapshot (field + context)            | Set automatically at submit time from `max(load_datetime)`         |

To fully reproduce a historical batch, both values are required: check out
the repo at the commit pinning the matching `prompt_version`, and use the
query above against the satellite `_hist` tables at `config_loaded_at`.
