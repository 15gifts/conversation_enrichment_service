# Batch API Enrichment Service

Snowflake-native pipeline that classifies conversational transcripts at scale using the Azure OpenAI Batch API. All orchestration runs inside Snowflake — no external infrastructure required.

**Product:** Humara  
**Ticket:** [AE-1540](halo-artefacts/AE/AE-1540/plan.md)

---

## Contents

- [How it works](#how-it-works)
- [Folder structure](#folder-structure)
- [Key files](#key-files)
- [Snowflake object inventory](#snowflake-object-inventory)
- [Deployment](#deployment)
- [Running locally](#running-locally)
- [Monitoring](#monitoring)
- [Operational runbooks](#operational-runbooks)
- [Design decisions](#design-decisions)
- [Related systems](#related-systems)

---

## How it works

```
Raw transcripts (FACT_CONVERSATIONS)
        │
        ▼
ENRICHMENT_QUEUE view  ←── excludes already-enriched or in-flight rows
        │
        ▼
SUBMIT_BATCH_TASK (every N hours)
   └── SUBMIT_BATCH_SP
       ├── Reads queue, serialises as JSONL
       ├── Writes to BATCH_ROW_MAPPING
       ├── Uploads to Azure OpenAI Files API
       ├── Submits batch job
       └── Writes to BATCH_TRACKING (after Azure returns batch ID)
        │
        ▼  (Azure processes — up to 24 h)
        │
        ▼
RETRIEVE_BATCH_TASK (every N mins)
   └── RETRIEVE_BATCH_SP
       ├── Polls Azure for completed batches
       ├── Downloads result JSONL in 64 KB chunks
       ├── Parses per-row LLM output
       └── Writes to ENRICHMENT_RESULTS
        │
        ▼
dbt (datawarehouse repo)
   ├── TBD
   └── TBD → Omni/Looker
```

State transitions: `PENDING → SUBMITTING → SUBMITTED → IN_PROGRESS → COMPLETED`  
Failure path: `→ FAILED`  
Note: automatic transitions to `RETRYING` or `PERMANENTLY_FAILED` are not currently implemented in this service; failed batches require manual or external handling if they need to be retried or marked terminal.

---

## Folder structure

```
batch_api_enrichment_service/
│
├── src/batch_enrichment/        # Python business logic (deployed into Snowpark stored procs)
│   ├── models.py
│   ├── submit.py
│   ├── retrieve.py
│   ├── jsonl_builder.py
│   └── response_parser.py
│
├── sql/                         # Snowflake DDL and DML — run in numbered order
│   ├── 01_warehouse.sql
│   ├── 02_schema_and_tables.sql
│   ├── 03_views.sql
│   ├── 04_security.sql
│   ├── 05_submit_procedure.sql
│   ├── 05a_stage.sql
│   ├── 06_retrieve_procedure.sql
│   ├── 07_tasks.sql
│   └── 08_seed_config.sql
│
├── streamlit/                   # Snowflake-hosted monitoring dashboard
│   ├── streamlit_app.py         # Landing page (heartbeat + queue overview)
│   ├── utils.py                 # Cached query helpers, alert thresholds
│   ├── environment.yml
│   ├── README.md                # Dashboard deployment instructions
│   └── pages/
│       ├── 01_Throughput.py
│       ├── 02_Latency.py
│       ├── 03_Quality.py
│       ├── 04_Failures.py
│       ├── 05_Cost.py
│       └── 06_Audit.py
│
├── tests/
│   ├── unit/
│   │   ├── test_models.py
│   │   ├── test_jsonl_builder.py
│   │   └── test_response_parser.py
│   └── integration/
│       ├── test_submit.py       # Full submit flow — mocked Azure API
│       └── test_retrieve.py     # Full retrieve flow — mocked Azure API
│
├── halo-artefacts/AE/AE-1540/   # Planning artefacts for this ticket
│   ├── plan.md
│   ├── plan_second-opinion.md
│   └── adr_draft.md
│
├── CLAUDE.md                    # AI assistant context and coding standards
├── monitoring.md                # Observability goals + Streamlit panel SQL
├── backlog_management_runbook.md # runbook explaining how to manage batch size & frequency in case of insufficient throughput
├── failure_mode_test_plan.md    # a set of tests to diagnose potential problems
├── smoke_test_instructions.md   # instructions for running a single conversation smoke test
├── implementation_design_1.docx # Architecture reference document
├── decision_rationale_1.docx    # Key design decision rationale
├── pyproject.toml
├── ruff.toml
└── uv.lock
```

---

## Key files

### Python source — `src/batch_enrichment/`

| File | Purpose |
|---|---|
| [models.py](src/batch_enrichment/models.py) | Core data classes: `BatchStatus` enum, `FieldConfig` (LLM output field definitions), `ContextConfig`, `ConversationTranscript`, `EnrichmentResult`, `SubmitConfig`, `RetrieveConfig` |
| [submit.py](src/batch_enrichment/submit.py) | `submit_batch_handler()` — orchestrates queue fetch, JSONL build, Azure Files upload, batch submission, and tracking/mapping writes |
| [retrieve.py](src/batch_enrichment/retrieve.py) | `retrieve_batch_handler()` — polls Azure batch status, streams result JSONL, parses per-row output, writes to `ENRICHMENT_RESULTS` |
| [jsonl_builder.py](src/batch_enrichment/jsonl_builder.py) | Builds system prompt dynamically from `FieldConfig` list (no hardcoded schema); constructs JSONL lines |
| [response_parser.py](src/batch_enrichment/response_parser.py) | Parses Azure per-row responses; detects guardrail blocks (non-retryable); validates JSON; logs parse errors |

### SQL scripts — `sql/`

Run these in numbered order on first deployment and after schema changes.

| File | Purpose |
|---|---|
| [01_warehouse.sql](sql/01_warehouse.sql) | Warehouse definition (managed by Terraform — do not edit directly) |
| [02_schema_and_tables.sql](sql/02_schema_and_tables.sql) | Creates `BATCH_TRACKING`, `BATCH_ROW_MAPPING`, `ENRICHMENT_RESULTS`, `ENRICHMENT_FIELD_CONFIG`, `ENRICHMENT_CONTEXT_CONFIG` |
| [03_views.sql](sql/03_views.sql) | `ENRICHMENT_QUEUE` (unenriched, non-in-flight rows) and state machine views |
| [04_security.sql](sql/04_security.sql) | Role, Secret, Network Rule, External Access Integration |
| [05_submit_procedure.sql](sql/05_submit_procedure.sql) | `SUBMIT_BATCH_SP` — Snowpark Python stored procedure for submission |
| [05a_stage.sql](sql/05a_stage.sql) | Internal stage for Python zips and Streamlit dashboard files |
| [06_retrieve_procedure.sql](sql/06_retrieve_procedure.sql) | `RETRIEVE_BATCH_SP` — Snowpark Python stored procedure for retrieval |
| [07_tasks.sql](sql/07_tasks.sql) | `SUBMIT_BATCH_TASK` (every 2 h) and `RETRIEVE_BATCH_TASK` (every 30 m) |
| [08_seed_config.sql](sql/08_seed_config.sql) | Seeds `ENRICHMENT_FIELD_CONFIG` and `ENRICHMENT_CONTEXT_CONFIG` for prompt v1.0 (9 output fields) |

### Documentation

| File | Purpose |
|---|---|
| [implementation_design_1.docx](implementation_design_1.docx) | Architecture reference — update if the pipeline design changes significantly |
| [decision_rationale_1.docx](decision_rationale_1.docx) | Rationale behind key design choices |
| [monitoring.md](monitoring.md) | Observability definitions and the SQL queries backing each Streamlit panel |
| [backlog_management_runbook.md](backlog_management_runbook.md) | How the queue view works, FIFO enforcement, throughput gating via `CHUNK_SIZE` and `MAX_ACTIVE_BATCHES` |
| [failure_mode_test_plan.md](failure_mode_test_plan.md) | Structured test matrix (15+ cases): task suspension, API errors, malformed responses, stuck batches |
| [smoke_test_instructions.md](smoke_test_instructions.md) | Step-by-step manual test for a single conversation end-to-end |
| [halo-artefacts/AE/AE-1540/plan.md](halo-artefacts/AE/AE-1540/plan.md) | Technical implementation plan |
| [halo-artefacts/AE/AE-1540/adr_draft.md](halo-artefacts/AE/AE-1540/adr_draft.md) | Architecture Decision Record draft |

---

## Snowflake object inventory

| Object | Type | Purpose |
|---|---|---|
| `prd_analytics.info_general.fact_conversation_messages` | Table | Source conversation-message fact table read directly by the queue DDL |
| `ENRICHMENT_QUEUE` | View | Unenriched rows eligible for submission |
| `BATCH_TRACKING` | Table | Central state machine — one row per Azure batch job |
| `BATCH_ROW_MAPPING` | Table | Maps `conversation_id` → batch; idempotency guard |
| `ENRICHMENT_RESULTS` | Table | Parsed LLM output in `parsed_fields` (for example `conversation_summary`, `engagement_trajectory`, `friction_types`, `purchase_readiness_reached`) |
| `ENRICHMENT_FIELD_CONFIG` | Table | Defines LLM output schema per `prompt_version` |
| `SUBMIT_BATCH_TASK` | Task | Fires every 2 h (`CRON 0 */2 * * * UTC`) |
| `RETRIEVE_BATCH_TASK` | Task | Fires every 30 m (`CRON */30 * * * * UTC`) |
| `SUBMIT_BATCH_SP` | Stored Procedure | Submission logic (Snowpark Python) |
| `RETRIEVE_BATCH_SP` | Stored Procedure | Retrieval and parsing logic (Snowpark Python) |
| `azure_openai_key` | Secret | Azure OpenAI API key |
| `AZURE_OPENAI_NETWORK_RULE` | Network Rule | Restricts egress to the configured Azure endpoint |
| `AZURE_OPENAI_EAI` | External Access Integration | Binds Network Rule + Secret to stored procedures |
| `INT_CONVERSATIONS_ENRICHED` | dbt model (downstream) | Joins enrichment results to conversations |
| `MART_CONVERSATION_LABELS` | dbt model (downstream) | Final dimensions queryable by Omni |

---

## Deployment

### Prerequisites

- Access to Snowflake with `ACCOUNTADMIN` (or equivalent) and `SECURITYADMIN` for the security objects
- Azure OpenAI API key with Batch API access in the correct region, this is shared via 1password
- `uv` installed (`pip install uv`)

### First-time setup

```bash
# 1. Install dependencies (all packages resolve from PyPI)
uv sync

# 3. Run SQL scripts in order
#    Connect to Snowflake (SnowSQL, Snowsight worksheet, or VS Code extension)
#    and execute each file in sql/ from 01 through 08.
#
#    NOTE: 01_warehouse.sql is Terraform-managed — skip if the warehouse already exists.
#    NOTE: 04_security.sql requires ACCOUNTADMIN role.
#    NOTE: Check the Azure endpoint hostname in 04_security.sql is correct before running.

# 4. Deploy the Streamlit dashboard
#    See streamlit/README.md for full instructions.
```

### Updating stored procedures

The Snowpark procedures in `05_submit_procedure.sql` and `06_retrieve_procedure.sql` embed the Python source inline. After changing Python code in `src/batch_enrichment/`:

1. Re-run the relevant SQL file to replace the procedure definition.
2. There is no separate build step — the SQL file is the deployment artefact.

### Enabling tasks

Tasks are created in `SUSPENDED` state. After verifying the smoke test passes:

```sql
ALTER TASK SUBMIT_BATCH_TASK RESUME;
ALTER TASK RETRIEVE_BATCH_TASK RESUME;
```

---

## Running locally

```bash
# Install dependencies
uv sync

# Run unit tests
uv run pytest tests/unit/

# Run integration tests (mocked Azure — no live credentials needed)
uv run pytest tests/integration/

# Run all tests
uv run pytest

# Lint
uv run ruff check .

# Format
uv run ruff format .
```

---

## Monitoring

The Streamlit dashboard is hosted inside Snowflake (owner's rights execution under `ANALYTICS_ENGINEER` role). See [streamlit/README.md](streamlit/README.md) for deployment.

| Page | What it shows |
|---|---|
| Overview | Pipeline heartbeat: task state, queue depth, active batches, stuck-batch alarm |
| Throughput | Queue volume, submissions, and completions by date |
| Latency | p50 / p90 / p99 batch latency; stuck-batch detection |
| Quality | Per-row outcome distribution (success, parse error, guardrail, Azure error); per-field failure rates |
| Failures | Batch failure reasons; orphaned mapping rows |
| Cost | `ENRICHMENT_WH` credit consumption over time |
| Audit | Object allowlist and grant verification via `ACCOUNT_USAGE` |

The SQL queries backing each panel are documented in [monitoring.md](monitoring.md).

---

## Operational runbooks

| Situation | Reference |
|---|---|
| Queue is growing faster than it drains | [backlog_management_runbook.md](backlog_management_runbook.md) — adjust `CHUNK_SIZE` or `MAX_ACTIVE_BATCHES` |
| A batch is stuck in `IN_PROGRESS` for >26 h | [failure_mode_test_plan.md](failure_mode_test_plan.md) — stuck batch recovery section |
| Tasks are suspended unexpectedly | Inspect `TASK_HISTORY` in Snowflake; check `RETRIEVE_BATCH_TASK` predecessor dependency |
| Azure API returns 429 / rate limit | Check `next_retry_after` in `BATCH_TRACKING`; procedure uses exponential backoff automatically |
| Malformed LLM responses | Check `ENRICHMENT_RESULTS` for `outcome = 'PARSE_ERROR'`; raw output is logged for diagnosis |
| Want to run a manual end-to-end test | [smoke_test_instructions.md](smoke_test_instructions.md) |

---

## Design decisions

Eight constraints govern all changes to this codebase. Violating any of them risks data loss, double-processing, or corrupted analytics.

1. **Idempotency** — Check `BATCH_TRACKING` for `IN_PROGRESS` batches before submitting. Write `BATCH_ROW_MAPPING` **before** calling the Azure API, not after.
2. **No orphaned batches** — If the procedure crashes post-API-call but pre-tracking-write, the mapping table prevents resubmission of the same rows.
3. **Per-row failure handling** — `status=completed` from Azure does not mean every row succeeded. Parse the `error` field per row.
4. **Prompt versioning** — `prompt_version` is stored in `BATCH_TRACKING` and propagated to `ENRICHMENT_RESULTS`. Changing the prompt schema requires a version bump and a new seed in `08_seed_config.sql`.
5. **Stream, don't load** — Large output JSONL files are streamed in 64 KB chunks to avoid Snowpark memory limits.
6. **Exponential backoff** — Use `next_retry_after` on `BATCH_TRACKING` rows. Never retry aggressively.
7. **Temperature = 0, `response_format = json_object`** — Handle `PARSE_ERROR` gracefully regardless; log raw output and don't block the batch.
8. **No LLM calls from dbt** — dbt owns transformation only. All Azure API calls live in stored procedures.

Full rationale: [decision_rationale_1.docx](decision_rationale_1.docx) and [halo-artefacts/AE/AE-1540/adr_draft.md](halo-artefacts/AE/AE-1540/adr_draft.md).

---

## Related systems

| System | Relationship |
|---|---|
| `datawarehouse` repo | Contains the downstream dbt models (`INT_CONVERSATIONS_ENRICHED`, `MART_CONVERSATION_LABELS`) that consume `ENRICHMENT_RESULTS` |
| Omni | BI layer — queries the dbt mart models for funnel and cohort analysis |
| Azure OpenAI | External LLM provider — Batch API only (contractual requirement) |
| Fivetran / Kafka | Upstream data sources that populate `RAW_TRANSCRIPTS` |
