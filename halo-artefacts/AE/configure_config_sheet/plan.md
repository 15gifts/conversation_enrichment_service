# Plan — Migrate enrichment configs to Fivetran-ingested Google Sheets

**Ticket:** `configure_config_sheet`
**Status:** §5 IN PROGRESS — repo changes implemented and verified by tests; §4 (datawarehouse repo) still to do; second opinion not yet run.
**Author:** robert.bramwell@humara.com
**Date:** 2026-05-18

## Implementation log (2026-05-18)

- §5.1 `sql/08_seed_config.sql` deleted; `enrichment_field_config` / `enrichment_context_config` `CREATE TABLE` blocks removed from `sql/02_schema_and_tables.sql`.
- §5.2 `config_loaded_at TIMESTAMP_NTZ` added to `batch_tracking` and `enrichment_results` in `sql/02_schema_and_tables.sql`.
- §5.3 `submit.py` rewritten: `_fetch_field_configs` / `_fetch_context_configs` query the `_act` satellites with no `config_version` filter; new `_fetch_config_loaded_at` helper computes `GREATEST(MAX(load_datetime), …)` and is called before the Azure POSTs; `_insert_tracking_row` stamps `config_loaded_at` on the tracking row. TDD: 5 new failing tests → all green; existing 7 tests still pass.
- §5.4 `retrieve.py` rewritten: `_fetch_field_configs` targets the `_act` satellite; `_fetch_active_batches` selects `config_loaded_at`; `_merge_enrichment_results` propagates it into the `MERGE` (new `config_loaded_at` column in source/target column lists). TDD: 3 new failing tests → all green; existing 9 tests still pass.
- §5.5 `docs/maintenance.md` created with the `_hist AS OF` reproducibility query and the dual-pointer versioning table.
- §5.6 `CLAUDE.md` Object Inventory adds the two `_act` satellites and notes `config_loaded_at` on `ENRICHMENT_RESULTS`; Design Rule §4 rewritten for the split; Maintained Documents table now lists `docs/maintenance.md`.
- Lint: `ruff check` and `ruff format` clean on all four touched source/test files. Pre-existing format issues in `streamlit/*` are out of scope and left as-is.
- Tests: `pytest` 85/85 passing, coverage 92%.

### Deviations

- **No `README.md` in the repo.** §5.5 specified linking `docs/maintenance.md` from `README.md`. Since none exists, the link was added to `CLAUDE.md` (Maintained Documents table + Design Rule §4) instead. If a `README.md` is created later, it should also link the maintenance doc.
- **Schema name placeholder.** `submit.py` / `retrieve.py` reference `datawarehouse_intermediate.vault.sat_google_sheets__enrichment_*_config_act`. The actual database name is environment-dependent (`prd_intermediate`, `dev_<user>_intermediate`, …) per the datawarehouse repo's dbt profile. Constants are flagged with a comment; resolution depends on §4 landing in the datawarehouse repo. **Tracking as open question §9.**
- **§4 (datawarehouse repo) not implemented in this session.** The plan's §4 — source, dq, raw, stage, hub, sat, hist, act, DQ tests, grants — lives in `/Users/robertbramwell/Documents/GitHub/datawarehouse` and was deliberately scoped out of this worktree. **§5 cannot deploy in production until §4 has merged in datawarehouse and the `_act` satellites materialise.**

---

## 1. Problem & goal

Today, the two enrichment configuration tables live in `datalake.llm_enrichments` and are populated by hand-written SQL inserts checked into this repo:

- `enrichment_field_config` — defines LLM output fields per `config_version` (10 cols, business key `field_name`)
- `enrichment_context_config` — defines extra context columns sent to the LLM (6 cols, business key `column_name`)

DDL: [sql/02_schema_and_tables.sql:51-83](sql/02_schema_and_tables.sql:51)
Seed: [sql/08_seed_config.sql](sql/08_seed_config.sql)
Runtime reads: [src/batch_enrichment/submit.py:56-96](src/batch_enrichment/submit.py:56)

**Goal:** Move the source of truth to two Google Sheets ingested by Fivetran into `pc_fivetran_db.google_sheets`, route them through the `datawarehouse` Data Vault (hub → sat → hist → act) following the `fact_conversation_questions` pattern, and have the enrichment service read the `_act` satellites instead. Versioning becomes implicit (SCD2 `load_datetime`) rather than a user-supplied `config_version` string.

## 2. Non-goals

- Changing prompt content or LLM output schema.
- Changing `BATCH_TRACKING` state machine semantics.
- Adding any new orchestration system — submission still runs from `SUBMIT_BATCH_TASK` on the existing cadence.
- Migrating other tables in `datalake.llm_enrichments`.

## 3. Target architecture

```
Google Sheet (field_config)   Google Sheet (context_config)
        │                              │
        ▼ Fivetran                     ▼ Fivetran
pc_fivetran_db.google_sheets.enrichment_field_config
pc_fivetran_db.google_sheets.enrichment_context_config
        │                              │
        ▼ dbt (datawarehouse repo)     ▼
stg → hub → sat → sat_hist → sat_act   (both)
        │                              │
        ▼ DQ tests gate _act build     ▼
SAT_GOOGLE_SHEETS__ENRICHMENT_FIELD_CONFIG_ACT
SAT_GOOGLE_SHEETS__ENRICHMENT_CONTEXT_CONFIG_ACT
        │                              │
        └──────────────┬───────────────┘
                       ▼
       SUBMIT_BATCH_SP reads _act satellites
       Stamps BATCH_TRACKING.config_loaded_at = max(load_datetime)
                       ▼
       ENRICHMENT_RESULTS.config_loaded_at carries forward
```

Business keys (hub natural key) — chosen because each is the unique row identifier in the sheet and what change-tracking should hang off:

- `enrichment_field_config` → `field_name`
- `enrichment_context_config` → `column_name`

`config_version` disappears from the source sheet. Reproducibility comes from SCD2: any historical batch's config is recoverable by querying `_hist AS OF` the `config_loaded_at` stamped on the batch.

## 4. Changes — `datawarehouse` repo

Pattern mirrors `fact_conversation_questions` exactly. The full lineage is **source → dq → raw → stage → hub/sat → hist → act**. Reference files for shape (do not modify):
- Source: [models/_sources.yml:102-139](../../../../../datawarehouse/models/_sources.yml)
- DQ: [models/intermediate/dq/google_sheets/dq_google_sheets__message_tags.sql](../../../../../datawarehouse/models/intermediate/dq/google_sheets/dq_google_sheets__message_tags.sql)
- DQ macro: [macros/models/dq_template.sql](../../../../../datawarehouse/macros/models/dq_template.sql)
- Raw: [models/intermediate/raw/raw_google_sheets__message_tags.sql](../../../../../datawarehouse/models/intermediate/raw/raw_google_sheets__message_tags.sql)
- Stage: [models/intermediate/stage/stg_google_sheets__message_tags.sql](../../../../../datawarehouse/models/intermediate/stage/stg_google_sheets__message_tags.sql)
- Hub / Sat / Hist / Act: corresponding files under `models/intermediate/vault/`

### 4.1 Source entries

Add two tables to the `google_sheets` source in `models/_sources.yml`:
- `enrichment_field_config`
- `enrichment_context_config`

### 4.2 DQ models

Two new files, following the exact shape of `dq_google_sheets__message_tags.sql`:

- `models/intermediate/dq/google_sheets/dq_google_sheets__enrichment_field_config.sql`
  ```sql
  with raw_data as (
    {{ dq_template('google_sheets','enrichment_field_config',ingestion_method='fivetran') }}
  )

  select
    * exclude (field_type, allowed_values, field_description),
    {{ clean_string('field_type', 'lower') }} as field_type,
    {{ clean_string('allowed_values', 'lower') }} as allowed_values,
    {{ clean_string('field_description', 'lower') }} as field_description
  from raw_data
  ```
  *(Exact `clean_string` set to be finalised against the agreed sheet column list. `field_name` is **not** cleaned — it must match exactly between sheet and `submit.py` consumer.)*

- `models/intermediate/dq/google_sheets/dq_google_sheets__enrichment_context_config.sql`
  ```sql
  with raw_data as (
    {{ dq_template('google_sheets','enrichment_context_config',ingestion_method='fivetran') }}
  )

  select
    * exclude (display_label, value_description),
    {{ clean_string('display_label', 'lower') }} as display_label,
    {{ clean_string('value_description', 'lower') }} as value_description
  from raw_data
  ```

Register both in `models/intermediate/dq/google_sheets/_intermediate__dq__google_sheets__models.yml` (or the equivalent yml used by neighbouring dq models — confirm during implementation) with the data-quality `dbt test`s listed in §4.7 attached at this layer.

### 4.3 `dbt_project.yml` vars

Add SKEY column lists alongside the existing `skey_columns_google_sheets_*` entries (around line 45-48):
```yaml
skey_columns_google_sheets_enrichment_field_config: ['_ROW']
skey_columns_google_sheets_enrichment_context_config: ['_ROW']
```
`_ROW` is the Fivetran-supplied per-row identifier — same choice as `message_tags`. Business-key hashing happens at the stage layer, not here.

### 4.4 Raw models

- `models/intermediate/raw/raw_google_sheets__enrichment_field_config.sql`
  - Source: `ref('dq_google_sheets__enrichment_field_config')`
  - Project / rename to DWH conventions
  - Emit `field_name` as the natural key (no `concat` needed — it's already unique in the sheet)
  - Add `is_deleted = false`, `loaddate = clean_timestamp('current_timestamp')`, `effective_from = clean_timestamp('_fivetran_synced')`
- `models/intermediate/raw/raw_google_sheets__enrichment_context_config.sql`
  - Analogous, natural key `column_name`

Register both in `models/intermediate/raw/_intermediate__raw__models.yml`.

### 4.5 Stage models

- `models/intermediate/stage/stg_google_sheets__enrichment_field_config.sql`
  - Hash `field_name` → `ENRICHMENT_FIELD_HKEY`
  - `HASHDIFF` over: `FIELD_TYPE, ALLOWED_VALUES, MIN_VALUE, MAX_VALUE, FIELD_DESCRIPTION, IS_NULLABLE, DISPLAY_ORDER, IS_DELETED`
- `models/intermediate/stage/stg_google_sheets__enrichment_context_config.sql`
  - Hash `column_name` → `ENRICHMENT_CONTEXT_COLUMN_HKEY`
  - `HASHDIFF` over: `DISPLAY_LABEL, VALUE_DESCRIPTION, DISPLAY_ORDER, IS_DELETED`

### 4.6 Hub / Sat / Hist / Act

- Hubs:
  - `models/intermediate/vault/hubs/hub_enrichment_field.sql` (`automate_dv.hub`, nk=`field_name`)
  - `models/intermediate/vault/hubs/hub_enrichment_context_column.sql` (`automate_dv.hub`, nk=`column_name`)
- Sats:
  - `models/intermediate/vault/sats/sat_google_sheets__enrichment_field_config.sql`
  - `models/intermediate/vault/sats/sat_google_sheets__enrichment_context_config.sql`
- Hist:
  - `hists/sat_google_sheets__enrichment_field_config_hist.sql`
  - `hists/sat_google_sheets__enrichment_context_config_hist.sql`
- Act:
  - `acts/sat_google_sheets__enrichment_field_config_act.sql`
  - `acts/sat_google_sheets__enrichment_context_config_act.sql`

The `_act` satellites are the contract surface for the enrichment service.

### 4.7 DQ tests (schema yml)

Attached to the dq models in §4.2 so they run before the vault layer builds — a failing test fails `dbt build` and prevents stale/invalid data flowing downstream into `_act`.

**`dq_google_sheets__enrichment_field_config`:**
- `field_name` — `not_null`, `unique`
- `field_type` — `accepted_values` in (`string`, `number`, `integer`, `boolean`, `array`, `enum`) *(confirm against `submit.py` usage)*
- `allowed_values` — parses as JSON when `field_type='enum'` (custom singular test)
- `min_value < max_value` when both populated (custom singular test)
- `display_order` — `not_null`, `unique`
- `is_nullable` — castable to boolean

**`dq_google_sheets__enrichment_context_config`:**
- `column_name` — `not_null`, `unique`
- `display_label` — `not_null`
- `display_order` — `not_null`, `unique`

### 4.8 Grants

`LLM_ENRICHMENT_ROLE` needs `SELECT` on both `_act` satellites. Add to the existing dbt post-hook / grants config in the same place as `sat_google_sheets__message_tags_act`.

## 5. Changes — `batch_api_enrichment_service` repo

### 5.1 Remove

- Delete [sql/08_seed_config.sql](sql/08_seed_config.sql).
- Delete the two `CREATE TABLE` blocks at [sql/02_schema_and_tables.sql:51-83](sql/02_schema_and_tables.sql:51) and any associated grants in [sql/01_setup.sql](sql/01_setup.sql) (verify in implementation).
- Remove deploy ordering references to `08_seed_config.sql`.

### 5.2 Schema additions

Add `config_loaded_at TIMESTAMP_NTZ` to:
- `BATCH_TRACKING` (the value used for the batch at submit time)
- `ENRICHMENT_RESULTS` (propagated from the parent batch)

`prompt_version` is retained, but its meaning narrows. It is the version identifier of the **prompt template string defined in this repo's Python code** — the scaffold into which the config rows from the `_act` satellites are interpolated at submit time. It versions the template itself (instructions, system message, JSON-schema scaffolding, the static parts of the user message) and is bumped manually in code whenever that template body changes. It is **not** tied to the contents of the config tables anymore; those are versioned independently via `config_loaded_at` (SCD2 `load_datetime` on the satellites).

Every batch therefore carries two independent version pointers on `BATCH_TRACKING` (propagated to `ENRICHMENT_RESULTS`):
- `prompt_version` — code-managed label for the template the config is inserted into
- `config_loaded_at` — auto-stamped timestamp identifying the config snapshot

CLAUDE.md Design Rule §4 must be reworded to make this split explicit (see §5.6).

### 5.3 `submit.py` rewrite

In [src/batch_enrichment/submit.py:56-96](src/batch_enrichment/submit.py:56):

- `_fetch_field_configs(session)` — drop the `config_version` parameter. Query:
  ```sql
  SELECT field_name, field_type, allowed_values, min_value, max_value,
         field_description, is_nullable, display_order
  FROM datawarehouse.<schema>.sat_google_sheets__enrichment_field_config_act
  ORDER BY display_order
  ```
  Lowercase identifiers are fine — Snowflake resolves case-insensitively against DV satellites.

- `_fetch_context_configs(session)` — analogous, against `…_context_config_act`.

- New helper `_fetch_config_loaded_at(session)`:
  ```sql
  SELECT GREATEST(
    (SELECT MAX(load_datetime) FROM …field_config_act),
    (SELECT MAX(load_datetime) FROM …context_config_act)
  )
  ```
  Called once per submit invocation; result is written to the `BATCH_TRACKING` row before the Azure Files / Batches API call (preserving the "write tracking before API call" idempotency rule).

### 5.4 SQL deploy artefacts

- Update [sql/05_submit_procedure.sql](sql/05_submit_procedure.sql) signature / body if config_version is currently a parameter.
- Update any view in [sql/03_views.sql](sql/03_views.sql) that joins on `config_version` (audit during implementation).

### 5.5 Maintenance doc + README

Create `docs/maintenance.md` with a "Reproducing a historical batch's config" section containing a parameterised query (`:batch_id`) like:

```sql
WITH b AS (
  SELECT batch_id, config_loaded_at
  FROM batch_tracking
  WHERE batch_id = :batch_id
)
SELECT 'field' AS kind, f.*
FROM b
JOIN datawarehouse.<schema>.sat_google_sheets__enrichment_field_config_hist f
  ON f.load_datetime <= b.config_loaded_at
 AND (f.valid_to IS NULL OR f.valid_to > b.config_loaded_at)
UNION ALL
SELECT 'context' AS kind, c.*
FROM b
JOIN datawarehouse.<schema>.sat_google_sheets__enrichment_context_config_hist c
  ON c.load_datetime <= b.config_loaded_at
 AND (c.valid_to IS NULL OR c.valid_to > b.config_loaded_at);
```

(Exact projection / hist column names finalised during implementation against the real `_hist` shape.)

Add a "Troubleshooting / Reproducing a batch" section to `README.md` linking to `docs/maintenance.md`.

### 5.6 CLAUDE.md updates

- Snowflake Object Inventory: replace `enrichment_field_config` / `enrichment_context_config` rows with references to the `_act` satellites; note they live in the datawarehouse repo.
- Design Rule §4 ("Prompt versioning is mandatory"): rewrite to describe the split. `prompt_version` now versions only the prompt template defined in code (the scaffold the config is inserted into) and is bumped manually when that template changes. Config history is timestamp-based via SCD2 on the `_act` satellites, captured by `BATCH_TRACKING.config_loaded_at`. Both must be stamped on every batch; reproducing a historical result requires both values.

## 6. Tests (TDD)

Defined before implementation, per HALO workflow.

### 6.1 datawarehouse repo

- **Unit / dbt tests** (added in §4.6) — every DQ rule listed must have an executable `dbt test` and must fail when fed a synthetic violating row.
- **Hashdiff regression** — seed two snapshots of the source where one field changes; assert exactly one new row in `_hist` and that `_act` reflects the new value.
- **Build-blocking** — assert that a failing DQ test prevents `_act` from refreshing (verified via `dbt build` exit code in CI).

### 6.2 batch_api_enrichment_service repo

- **`_fetch_field_configs` returns satellite rows** — fixture: insert rows into a mock `_act`; assert dict shape matches what the prompt builder expects.
- **`_fetch_context_configs`** — analogous.
- **`_fetch_config_loaded_at` returns greater of the two satellites** — fixture: stamp different `load_datetime` values; assert correct max.
- **Submit writes `config_loaded_at` before Azure call** — mock the Azure client; assert the `BATCH_TRACKING` UPDATE happens first and contains the stamped timestamp.
- **`ENRICHMENT_RESULTS.config_loaded_at` is propagated** — end-to-end test: stamp on batch, retrieve, assert column populated on result rows.
- **Maintenance query reproducibility** — given a known `batch_id` and seeded `_hist` rows, the documented query returns exactly the config that was current at `config_loaded_at`.

### 6.3 Integration

- Run a full submit → retrieve cycle in dev against the new `_act` satellites; assert success on a single conversation row end-to-end.

## 7. Rollout

Single deploy, no dual-write needed (configs are read-only at runtime, and the old tables are immediately replaced):

1. Merge datawarehouse PR; verify `_act` satellites populate from existing Google Sheet (sheet must be pre-populated with current v1.0 config before merge).
2. Verify DQ tests pass.
3. Grant `LLM_ENRICHMENT_ROLE` SELECT on both `_act` satellites.
4. Merge this repo's PR; run schema migration (`config_loaded_at` columns + drop of old tables).
5. Resume `SUBMIT_BATCH_TASK`.

Rollback: keep the old tables in a `_deprecated` schema for one cycle (drop in a follow-up). Revert path: re-grant `submit.py` against the old tables and resume task.

## 8. Risks

- **Sheet edit cadence vs. submit cadence.** dbt-orchestrated `_act` build prevents partial reads, but rapid edits can still mean a batch lands on a config the editor didn't intend to publish. Mitigation: document an editor convention; consider a `published` boolean column in the sheet (out of scope for v1, captured as follow-up).
- **Cross-repo dependency.** Enrichment service now depends on the datawarehouse build. Add a freshness check (`config_loaded_at` within N hours of `current_timestamp`) — fail the submit task loudly if stale.
- **Lost code-review on config changes.** Editing a sheet bypasses PR review. DQ tests catch shape violations; semantic correctness (does this prompt change make sense?) is uncovered. Out of scope for v1; raise as a process item.

## 9. Open questions

- Exact `datawarehouse` schema name for the `_act` satellites (confirm before writing the FROM clauses).
- Are there any existing consumers of `datalake.llm_enrichments.enrichment_*_config` outside this repo? (`grep` across the org before drop.)
- Should `prompt_version` be removed entirely from `BATCH_TRACKING`, or retained as the code-managed label? Recommendation: retain.

## 10. Out of scope / follow-ups

- `published` flag column on the sheet for editor-controlled atomicity.
- Lightweight approval workflow for sheet changes (e.g. PR on a CSV mirror).
- Deletion of `_deprecated` schema once rollout is stable.
