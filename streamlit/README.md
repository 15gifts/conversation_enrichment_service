# Enrichment Pipeline Monitoring Dashboard

Streamlit-in-Snowflake (SiS) app for AE-1540. Implements the seven panels
defined in `../monitoring.md`.

## Layout

```
streamlit/
├── streamlit_app.py        # Overview page (heartbeat + at-a-glance health)
├── utils.py                # Shared session + cached query helpers
├── environment.yml         # SiS dependencies (streamlit, pandas, plotly)
├── pages/
│   ├── 01_Throughput.py    # Queue, submissions, completions
│   ├── 02_Latency.py       # p50/p90/p99 + stuck-batch alarm
│   ├── 03_Quality.py       # Outcome donut + failing-field tally
│   ├── 04_Failures.py      # Batch failures + orphan mapping rows
│   ├── 05_Cost.py          # ENRICHMENT_WH credits
│   └── 06_Audit.py         # Object allowlist + grant audit
└── README.md
```

## Ownership model

The Streamlit object is **owned by `ANALYTICS_ENGINEER`**, not
`llm_enrichment_role`. This is deliberate:

- Streamlit in Snowflake uses **owner's rights** — every query inside the
  app runs as the role that owns the object, regardless of who is viewing.
- `ANALYTICS_ENGINEER` already has everything needed to render the
  operational pages: SELECT on the enrichment tables (via inherited
  `LLM_ENRICHMENT_ROLE`) and USAGE on `ENRICHMENT_WH`.
- It only needs one extra grant — `IMPORTED PRIVILEGES ON DATABASE snowflake`
  — for the Cost and Audit pages, which read from `account_usage`.
- Keeping `llm_enrichment_role` free of the broad `IMPORTED PRIVILEGES`
  grant preserves its narrow operator-only role definition.

Audience: anyone with the `ANALYTICS_ENGINEER` role (and any admin) can
view the dashboard. There's no need to grant `USAGE` on the Streamlit
object to a wider group.

## Required grants

Two one-time grants are needed. Both must be **direct** grants to
`analytics_engineer` — inheritance from `llm_enrichment_role` (which owns
the stage) is **not** sufficient. The `CREATE STREAMLIT` privilege check
walks explicit grants on the stage, not the effective-privilege graph
through ownership. Without these you'll hit:

> *"The specified stage BATCH_ENRICHMENT_STAGE does not exist or the
> current role does not have access. Owner of the Streamlit must have at
> least READ on the specified stage."*

```sql
-- 1. Account-level metadata access for the Cost and Audit pages.
USE ROLE accountadmin;
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE analytics_engineer;

-- 2. Direct READ/WRITE on the stage (must come from the stage owner,
--    not via role inheritance — see note above).
USE ROLE llm_enrichment_role;
GRANT READ, WRITE ON STAGE datalake.llm_enrichments.batch_enrichment_stage
    TO ROLE analytics_engineer;
```

Everything else `analytics_engineer` already has via existing role
inheritance (SELECT on the enrichment tables, USAGE on `ENRICHMENT_WH`,
USAGE on the schema).

## Deployment

The Streamlit app reuses the existing `batch_enrichment_stage` (created in
`sql/05a_stage.sql` for the stored-procedure zip). All dashboard files
live under the `dashboard/` subpath to keep them separated from
`batch_enrichment.zip` at the stage root.

### Option 1 — via Snowflake CLI / Snowsight (recommended)

1. Confirm the grants from the **Required grants** section above have been
   run. Without the direct `READ, WRITE` grant on the stage, the deploy
   command will fail with a stage-not-found error.
2. Deploy as `ANALYTICS_ENGINEER` so the Streamlit object is owned by it:
   ```bash
   # In Snowflake CLI config, set role = analytics_engineer for this connection.
   cd streamlit/
   snow streamlit deploy \
       --name enrichment_dashboard \
       --warehouse enrichment_wh \
       --stage batch_enrichment_stage
   ```
   `snow streamlit deploy` will upload files under a subpath named after
   the app (`enrichment_dashboard/`) by default — confirm with
   `LIST @batch_enrichment_stage` after deploy.

### Option 2 — manual CREATE STREAMLIT

1. PUT files to the stage under `dashboard/` (any role with WRITE on the
   stage will do):
   ```sql
   PUT file://streamlit/streamlit_app.py        @batch_enrichment_stage/dashboard/        AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   PUT file://streamlit/utils.py                @batch_enrichment_stage/dashboard/        AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   PUT file://streamlit/environment.yml         @batch_enrichment_stage/dashboard/        AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   PUT file://streamlit/pages/01_Throughput.py  @batch_enrichment_stage/dashboard/pages/  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   PUT file://streamlit/pages/02_Latency.py     @batch_enrichment_stage/dashboard/pages/  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   PUT file://streamlit/pages/03_Quality.py     @batch_enrichment_stage/dashboard/pages/  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   PUT file://streamlit/pages/04_Failures.py    @batch_enrichment_stage/dashboard/pages/  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   PUT file://streamlit/pages/05_Cost.py        @batch_enrichment_stage/dashboard/pages/  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   PUT file://streamlit/pages/06_Audit.py       @batch_enrichment_stage/dashboard/pages/  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   ```
2. Create the Streamlit object **as `ANALYTICS_ENGINEER`** so it owns the
   object and queries execute with its privileges. `ROOT_LOCATION` points
   at the `dashboard/` subpath so Snowflake doesn't mistakenly pick up
   `batch_enrichment.zip` at the stage root:
   ```sql
   USE ROLE analytics_engineer;
   USE WAREHOUSE enrichment_wh;
   USE SCHEMA datalake.llm_enrichments;

   CREATE OR REPLACE STREAMLIT enrichment_dashboard
       ROOT_LOCATION = '@datalake.llm_enrichments.batch_enrichment_stage/dashboard'
       MAIN_FILE = 'streamlit_app.py'
       QUERY_WAREHOUSE = enrichment_wh
       COMMENT = 'Monitoring dashboard for the batch enrichment pipeline (AE-1540).';
   ```
3. Open it from Snowsight → Streamlit → `enrichment_dashboard`. Anyone
   with the `ANALYTICS_ENGINEER` role (or admin) can view it.

## Caching

Every query in `utils.py` is wrapped in `@st.cache_data` with a TTL aligned
to the panel's refresh cadence (see `monitoring.md`):

| Cadence | Used for |
|---|---|
| 60 s | Task heartbeat |
| 5 min | Queue, latency, outcomes, failures |
| 1 h | Daily throughput aggregates |
| 24 h | Cost and audit |

The Snowpark session itself is cached with `@st.cache_resource` so the
`QUERY_TAG = streamlit_enrichment_dashboard` is only set once per app
instance — useful for carving dashboard cost out of `ENRICHMENT_WH` spend:

```sql
SELECT SUM(credits_used_cloud_services) AS dashboard_credits
FROM snowflake.account_usage.query_history
WHERE query_tag = 'streamlit_enrichment_dashboard'
  AND start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP());
```

## Modifying queries

Every query lives in `utils.py`. Pages only render — they don't construct
SQL. To add a panel:

1. Add a new `@st.cache_data` function to `utils.py` returning a DataFrame.
2. Add a new page under `pages/` (numeric prefix sets sidebar order).
3. Re-deploy (`snow streamlit deploy` or re-PUT changed files).

## Known limitations

- `account_usage` views have 45 min – 3 h latency. The Audit and Cost
  pages will lag real time by that much.
- `enrichment_queue` is a view over a multi-fact join. The bucket query
  in `queue_age_buckets()` is cheaper than a naked `COUNT(*)`, but it's
  still the most expensive query on the dashboard. Caching is set to 5
  minutes specifically to limit hits to this view.
- No write actions yet. Resume / suspend / reset operations are documented
  in `backlog_management_runbook.md` and must be executed via a worksheet.
  Adding action buttons here would require a writer role and form-level
  confirmation — defer to Phase 2.
