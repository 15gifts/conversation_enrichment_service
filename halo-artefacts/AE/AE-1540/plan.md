# Technical Plan: Batch LLM Enrichment Pipeline — Phase 1 (Pilot)

**Ticket / Issue:** AE-1540
**Author:** Robert Bramwell
**Date:** 2026-05-05
**Branch:** feat/AE-1540-batch-llm-enrichment
**Status:** READY FOR REVIEW

---

## 1. Problem Statement

We need to classify millions of conversational transcripts with LLM-generated labels (sentiment, intent, resolution status, topics) to power analytics queries such as "show the conversion funnel for users who expressed pricing frustration." No classification capability exists today. The solution must use Azure OpenAI (contractual requirement), integrate with the existing Snowflake + dbt + Omni stack, and introduce no new orchestration infrastructure.

---

## 2. Goals

- Pipeline is fully built end-to-end: infrastructure, stored procedures, tasks, and dbt models
- Phase 1 scoped to pilot configuration: chunk_size=10, tasks suspended (manual execution only)
- A pilot run of 10 conversations produces enrichment labels in `datalake.llm_enrichments.enrichment_results`
- dbt models in `datawarehouse` join enrichment results to conversations and expose labels for Omni
- Azure credentials are placeholder — a single `ALTER SECRET` enables live operation once access is granted
- All components are independently testable without a live Azure API connection

---

## 3. Non-Goals (Out of Scope)

- Backfill of historical transcripts (Phase 2)
- Production task scheduling / automated execution (Phase 2)
- PII redaction pre-processing (flagged as a risk; deferred to ADR resolution)
- Private Link / Azure PrivateLink configuration (production hardening, Phase 2)
- Snowflake Alerts and monitoring dashboards (Phase 2)
- Omni dashboard configuration (consumer work, separate ticket)
- Incremental re-enrichment with updated prompts (Phase 2)
- Message-level enrichment (per-message labels, one API call per `message_hkey`) — separate future ticket; this ticket enriches at conversation level only

---

## 4. Background and Context

The `datawarehouse` repo (sister directory) contains the existing dbt project. Relevant models:

- `{target}_analytics.info_general.fact_conversation_messages` — individual messages per conversation; columns used: `conversation_id`, `conversation_hkey`, `message_text_combined`, `message_sent_by`, `conversation_message_num`, `event_at`
- `{target}_analytics.biz_general.fact_conversations` — conversation-level aggregation (no transcript text)

The `datalake` database is the raw/operational Snowflake database. A new schema `llm_enrichments` will be created within it to host all pipeline objects.

The design document (`implementation_design_1.docx`) in this repo contains the full reference architecture. This plan implements Phase 1 as described there.

**ADR required before implementation** — see Section 5 and Step 0. Review level: **Significant** — requires Architecture Forum + engineering lead sign-off (new external LLM integration, PII data flow, cost commitment, exemption from "all LLM calls via axiom-conversation-api" rule).

---

## 5. Proposed Solution

Two Snowpark Python stored procedures manage the asynchronous Azure OpenAI Batch API lifecycle, orchestrated by two Snowflake Tasks (suspended for pilot):

1. **Submit procedure** (`submit_batch_sp`): selects up to `chunk_size` unenriched conversations from the `enrichment_queue` view, builds a JSONL transcript payload, uploads to the Azure OpenAI Files API, submits a batch job, and records metadata in `batch_tracking` / `batch_row_mapping`.

2. **Retrieve procedure** (`retrieve_batch_sp`): polls `batch_tracking` for `SUBMITTED`/`IN_PROGRESS` batches, checks Azure status, downloads completed results JSONL, parses structured LLM output per row, and writes to `enrichment_results`.

The `enrichment_queue` view performs a cross-database aggregation of `fact_conversation_messages` into per-conversation transcript strings, excluding conversations already enriched or in-flight.

dbt models in `datawarehouse` consume `enrichment_results` as a source and produce `int_conversations_enriched` and `fact_conversation_labels` for Omni.

### Alternatives Considered

| Alternative | Reason Rejected |
|---|---|
| Route calls through `axiom-conversation-api` | That service is real-time; batch analytics enrichment is a separate concern. Adding batch-processing to a conversational API would couple unrelated domains. Requires ADR to formalise the exemption from the "LLM calls via axiom-conversation-api only" rule. |
| External orchestration (Airflow, Lambda) | Contractual preference for no new infrastructure; Snowflake-native solution meets the requirement |
| Real-time enrichment at event ingestion | Cost-prohibitive at scale; batch pricing is significantly cheaper; latency acceptable for analytics |
| Snowflake Cortex (native LLM) | Contractual requirement specifies Azure OpenAI GPT-4.1+ |

---

## 6. Architecture and Design

### Data Flow

```
datalake.events.raw_events
        │
        ▼ (existing dbt pipeline)
{target}_analytics.info_general.fact_conversation_messages
        │
        ▼ (enrichment_queue view — cross-database)
datalake.llm_enrichments.enrichment_queue
        │
        ▼ (submit_batch_sp — Snowpark Python)
Azure OpenAI Files API → Batch API
        │
        ▼ (retrieve_batch_sp — Snowpark Python)
datalake.llm_enrichments.enrichment_results
        │
        ▼ (dbt source → models in datawarehouse repo)
{target}_analytics.info_general.fact_conversation_labels
        │
        ▼
Omni
```

### Data Model — New Objects in `datalake.llm_enrichments`

**`batch_tracking`** — central state machine for every Azure batch job:
```sql
batch_tracking_id    VARCHAR DEFAULT UUID_STRING()
azure_batch_id       VARCHAR
azure_input_file_id  VARCHAR
azure_output_file_id VARCHAR
status               VARCHAR DEFAULT 'PENDING'
  -- valid (Phase 1): PENDING | SUBMITTING | SUBMITTED | IN_PROGRESS | COMPLETED | FAILED
  -- Phase 2 additions: RETRYING | PERMANENTLY_FAILED
row_count            INTEGER
chunk_index          INTEGER
total_chunks         INTEGER
model_deployment     VARCHAR
prompt_version       VARCHAR
submitted_at         TIMESTAMP_NTZ
completed_at         TIMESTAMP_NTZ
failed_at            TIMESTAMP_NTZ
retry_count          INTEGER DEFAULT 0
max_retries          INTEGER DEFAULT 3
error_message        VARCHAR
error_code           VARCHAR
cost_estimate_usd    FLOAT
created_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
updated_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
```

**`batch_row_mapping`** — prevents double-processing:
```sql
conversation_id    VARCHAR
batch_tracking_id  VARCHAR
batch_status       VARCHAR DEFAULT 'PENDING'
created_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
```

**`enrichment_results`** — parsed LLM output per conversation:
```sql
enrichment_id           VARCHAR DEFAULT UUID_STRING()
conversation_id         VARCHAR
prompt_version          VARCHAR
sentiment               VARCHAR   -- positive | neutral | negative | mixed
primary_intent          VARCHAR   -- support | billing | sales | onboarding | churn_risk | feedback | other
topics                  ARRAY
resolution_status       VARCHAR   -- resolved | unresolved | escalated | unknown
customer_effort_score   INTEGER   -- 1-5
summary                 VARCHAR
raw_response            VARIANT   -- full Azure response object
parse_error             BOOLEAN   DEFAULT FALSE
parse_error_message     VARCHAR
batch_tracking_id       VARCHAR
enriched_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
-- UNIQUE constraint on (conversation_id, prompt_version) enforced at application layer;
-- retrieve procedure uses MERGE to prevent silent duplicates on double-retrieval
```

**`enrichment_queue`** — view, excludes in-flight and completed:
```sql
-- Aggregates fact_conversation_messages into per-conversation transcripts
-- Excludes: conversations in enrichment_results (done)
--           conversations in batch_row_mapping with active status (in-flight)
-- Note: LISTAGG has a 16MB per-group limit. Transcripts approaching this limit will be
-- silently truncated. Risk is LOW for pilot (10 rows). Phase 2 must add SUBSTR guard.
SELECT
  c.conversation_id,
  LISTAGG(c.message_sent_by || ': ' || COALESCE(c.message_text_combined, ''), '\n')
    WITHIN GROUP (ORDER BY c.conversation_message_num) AS transcript_text,
  MIN(c.event_at) AS conversation_started_at
FROM {analytics_db}.info_general.fact_conversation_messages c
LEFT JOIN enrichment_results er ON c.conversation_id = er.conversation_id
LEFT JOIN batch_row_mapping brm
  ON c.conversation_id = brm.conversation_id
  AND brm.batch_status IN ('PENDING','SUBMITTING','SUBMITTED','IN_PROGRESS')
WHERE er.conversation_id IS NULL
  AND brm.conversation_id IS NULL
GROUP BY c.conversation_id
ORDER BY conversation_started_at ASC
```

Note: `{analytics_db}` is a Snowflake variable set at deploy time (e.g. `prod_analytics`).

### Security Objects

```
datalake.llm_enrichments.azure_openai_key          ← Snowflake Secret (placeholder on deploy)
datalake.llm_enrichments.azure_openai_network_rule ← EGRESS rule to Azure endpoint
azure_openai_eai                                    ← External Access Integration
enrichment_wh                                       ← XSMALL warehouse, auto-suspend 60s
```

### Stored Procedure: Submit

Entry point: `submit_batch(session, config: SubmitConfig) -> SubmitResult`

Steps (in order, all within one procedure call):
1. Check active batch count — exit early if `>= config.max_active_batches`
2. Query `enrichment_queue` with `LIMIT config.chunk_size`
3. Exit early if no rows returned
4. Build JSONL payload using `jsonl_builder.build_batch_lines(conversations, config)`
5. INSERT mapping rows into `batch_row_mapping` with status `PENDING`
6. Upload JSONL to Azure Files API — store `input_file_id`
7. Submit batch to Azure Batches API — store `azure_batch_id`
8. INSERT row into `batch_tracking` with status `SUBMITTED`
9. UPDATE `batch_row_mapping` rows to `SUBMITTED`
10. On any exception between steps 6–9: set mapping rows to `FAILED`, attempt file cleanup

Pilot config: `chunk_size=10`, `max_active_batches=1`

**Concurrent submit race condition:** Two simultaneous `CALL submit_batch_sp()` invocations could both read the same rows from `enrichment_queue` before either inserts into `batch_row_mapping`. Mitigation for Phase 1: tasks are SUSPENDED and manual calls are sequential; `max_active_batches=1` limits the window but does not eliminate it. Phase 2 must address this with `ALLOW_OVERLAPPING_EXECUTION = FALSE` on tasks and a documented "no concurrent manual calls" runbook rule. See T25.

### Stored Procedure: Retrieve

Entry point: `retrieve_batch(session, config: RetrieveConfig) -> RetrieveResult`

Steps:
1. Query `batch_tracking` for rows with status `IN ('SUBMITTED', 'IN_PROGRESS')`
2. For each: GET `/openai/batches/{azure_batch_id}` — check status
3. Update `batch_tracking` status to match Azure status
4. For completed batches: GET `/openai/files/{output_file_id}/content`
5. Parse output JSONL line-by-line using `response_parser.parse_batch_lines()`
6. MERGE all results into `enrichment_results` on `(conversation_id, prompt_version)` — prevents silent duplicates if the retrieve procedure is called twice for the same completed batch
7. UPDATE `batch_tracking` to `COMPLETED`, set `completed_at`
8. UPDATE `batch_row_mapping` rows to `COMPLETED`
9. DELETE input and output files from Azure to avoid storage charges

### JSONL Line Format

```json
{
  "custom_id": "<conversation_id>",
  "method": "POST",
  "url": "/chat/completions",
  "body": {
    "model": "<deployment_name>",
    "messages": [
      {"role": "system", "content": "<system_prompt>"},
      {"role": "user", "content": "<transcript_text>"}
    ],
    "temperature": 0,
    "response_format": {"type": "json_object"}
  }
}
```

### Classification Schema (System Prompt Response)

```json
{
  "sentiment": "positive | neutral | negative | mixed",
  "primary_intent": "support | billing | sales | onboarding | churn_risk | feedback | other",
  "topics": ["topic1", "topic2"],
  "resolution_status": "resolved | unresolved | escalated | unknown",
  "customer_effort_score": 3,
  "summary": "2-3 sentence summary"
}
```

Prompt version: `v1.0` — stored in `batch_tracking.prompt_version` and `enrichment_results.prompt_version`.

### dbt Models (in `datawarehouse` repo)

**New source** in `_sources.yml`:
```yaml
- name: llm_enrichments
  database: datalake
  schema: llm_enrichments
  tables:
    - name: enrichment_results
```

**`int_conversations_enriched`** — `intermediate/enrichment/`:
- Joins `fact_conversation_messages` (ref) to `enrichment_results` (source) on `conversation_id`
- Filters to `parse_error = FALSE`
- Groups to one row per conversation: latest enrichment labels + conversation metadata

**`fact_conversation_labels`** — `information_mart/`:
- Final Omni-queryable model
- One row per conversation with: all enrichment labels, `customer_id`, `conversation_start_at`, `environment`, `is_supported`, `is_assisted`
- Materialised as table, incremental on `enriched_at`

### Azure Trust Boundary Threat Model

This pipeline introduces a new external trust boundary: Snowflake → Azure OpenAI. Threat summary:

| Threat | Mitigation |
|---|---|
| Transcript PII exfiltrated to Azure | ADR must confirm legal sign-off; Azure OpenAI data is not used for training (MS standard terms); data residency confirmed by Azure region selection |
| API key compromise | Snowflake Secret (not visible post-write); key rotation triggers `ALTER SECRET`; Snowflake audit trail on secret access |
| `raw_response VARIANT` echoes PII back into Snowflake | Azure response includes the full chat completion object which may echo user content in logprobs/system fields. **Decision required in ADR: store raw_response for debugging vs. strip it before insert.** For pilot: `raw_response` stored but access restricted to `LLM_ENRICHMENT_ADMIN` role only. |
| MITM / network interception | Snowflake EAI restricts egress to named endpoint; TLS enforced by Azure |
| Prompt injection via transcript content | Transcripts passed as user-role content only; system prompt defines schema; `temperature=0`; `response_format=json_object` limits injection surface |

### External Dependencies

| Dependency | Justification |
|---|---|
| Azure OpenAI Batch API | Contractual requirement |
| `requests` (Python) | HTTP calls to Azure API from Snowpark |
| `pydantic` v2 | Validated models at Azure response boundary |
| `snowflake-snowpark-python` | Snowflake stored procedure runtime |

---

## 7. Test Cases

### Happy Path Tests

| # | Test Name | Inputs | Expected Output | Type |
|---|---|---|---|---|
| T1 | `jsonl_builder` single-turn conversation produces valid line | 1 conversation, 2 messages (user + assistant) | 1 valid JSONL line; `custom_id` = conversation_id; messages in order | UNIT |
| T2 | `jsonl_builder` multi-turn conversation preserves order | 1 conversation, 6 messages in mixed order | Messages ordered by `conversation_message_num` ascending | UNIT |
| T3 | `jsonl_builder` batch of 10 produces 10 lines | 10 conversations | 10 JSONL lines, each valid, each unique `custom_id` | UNIT |
| T4 | `response_parser` valid Azure output parses all fields | Azure output JSONL line with all 6 fields | `EnrichmentResult` with correct typed values, `parse_error=False` | UNIT |
| T5 | `response_parser` batch of 10 responses | 10 Azure output lines | 10 `EnrichmentResult` objects | UNIT |
| T6 | `enrichment_queue` unenriched conversation appears | conversation not in results or mapping | Conversation in queue | INTEGRATION |
| T7 | Submit procedure records tracking and mapping rows | 10 rows in enrichment_queue, stubbed Azure | `batch_tracking` row with `SUBMITTED`, 10 `batch_row_mapping` rows with `SUBMITTED` | INTEGRATION |
| T8 | Retrieve procedure writes results for completed batch | `batch_tracking` row `IN_PROGRESS`, stubbed Azure returning completed results | 10 rows in `enrichment_results`, `batch_tracking` `COMPLETED` | INTEGRATION |
| T9 | `int_conversations_enriched` joins correctly | enrichment_results source row + fact_conversation_messages ref | Output row has enrichment labels + conversation metadata | UNIT (dbt) |
| T10 | `fact_conversation_labels` has correct schema | populated `int_conversations_enriched` | All expected columns present, correct types, no nulls on required fields | UNIT (dbt) |

### Error Path Tests

| # | Test Name | Inputs | Expected Output | Type |
|---|---|---|---|---|
| T11 | `response_parser` malformed JSON → parse error | Azure line with non-JSON body | `EnrichmentResult` with `parse_error=True`, raw_response stored, no exception raised | UNIT |
| T12 | `response_parser` missing required field → parse error | Azure line missing `sentiment` | `EnrichmentResult` with `parse_error=True` | UNIT |
| T13 | Submit procedure: Azure 401 → mapping rows set to FAILED | Stubbed Azure returning 401 | Exception raised; `batch_row_mapping` rows `FAILED`; no `batch_tracking` row inserted | INTEGRATION |
| T14 | Submit procedure: file upload succeeds, batch submit fails → cleanup | Stubbed: upload OK, batch POST 429 | Mapping rows `FAILED`; DELETE file called; no `batch_tracking` row | INTEGRATION |
| T15 | Submit procedure: max_active_batches reached → exits early | 1 existing `IN_PROGRESS` batch, max=1 | Procedure exits; no new rows in mapping or tracking | INTEGRATION |
| T16 | Retrieve procedure: Azure batch failed → tracking marked FAILED | Azure batch status `failed` | `batch_tracking` `FAILED`, `error_message` populated | INTEGRATION |
| T17 | Retrieve procedure: partial row failures in completed batch | Azure output with 8 OK + 2 error lines | 8 rows in `enrichment_results`; 2 rows with `parse_error=True` | INTEGRATION |

### Boundary and Edge Case Tests

| # | Test Name | Inputs | Expected Output | Type |
|---|---|---|---|---|
| T18 | `jsonl_builder` null `message_text_combined` handled | Message row with `message_text_combined = NULL` | Empty string used; no exception | UNIT |
| T19 | `jsonl_builder` single-message conversation (no assistant turn) | 1 user message only | Valid JSONL line with single message | UNIT |
| T20 | `enrichment_queue` excludes already-enriched conversation | conversation_id in `enrichment_results` | Not in queue | INTEGRATION |
| T21 | `enrichment_queue` excludes in-flight conversation | conversation_id in `batch_row_mapping` with `IN_PROGRESS` | Not in queue | INTEGRATION |
| T22 | `enrichment_queue` includes FAILED mapping rows (retry eligible) | conversation_id in `batch_row_mapping` with `FAILED` | Appears in queue | INTEGRATION |

### Boundary and Edge Case Tests (continued)

| # | Test Name | Inputs | Expected Output | Type |
|---|---|---|---|---|
| T25 | Concurrent submit calls do not double-enqueue same conversations | Two simultaneous submit calls against same queue (simulated via two sequential calls with no mapping rows inserted between them) | Second call exits early (max_active_batches reached after first succeeds); no duplicate mapping rows | INTEGRATION |
| T26 | LISTAGG truncation: very long transcript noted in risk register | N/A — monitoring concern, not a testable unit | N/A — document as Phase 2 risk; Phase 1 pilot transcripts expected well under 16MB | NOTE |

### Integration Tests

| # | Test Name | Systems involved | What is verified | Type |
|---|---|---|---|---|
| T23 | End-to-end pilot: 10 conversations enriched | Snowflake + live Azure OpenAI | 10 rows in `enrichment_results`; `batch_tracking` `COMPLETED`; dbt models run clean | E2E |
| T24 | dbt source freshness test passes after enrichment cycle | dbt + `datalake.llm_enrichments` | No freshness failures reported by `dbt source freshness` | INTEGRATION |

---

## 8. Implementation Steps

### Step 0 — Pre-implementation: ADR (BLOCKING GATE)

> ⛔ HOLD: Do not begin Step 1 until the ADR is merged.

- [ ] **Step 0a:** Run `/halo-adr` to produce Jira design brief content
  - Address: new Azure OpenAI integration; PII in transcripts sent to external AI provider; direct LLM calls outside `axiom-conversation-api` pattern; cost commitment; data residency
- [ ] **Step 0b:** Create Jira ADR ticket — get approval to explore
  - Review level: **Significant** — Architecture Forum + engineering lead required
  - Involve: Jack Barker-Davy (LLM governance), Tracking domain lead, Architecture Forum
  - Key questions the ADR must answer: (1) PII legal sign-off for transcripts → Azure OpenAI; (2) `raw_response` storage approved/stripped; (3) formal exemption from "LLM calls via axiom-conversation-api only" for analytics batch workloads
- [ ] **Step 0c:** Confirm Azure OpenAI data residency satisfies requirements for customer PII; get legal sign-off if required
- [ ] **Step 0d:** Get Jira sign-off from required reviewers
- [ ] **Step 0e:** Open PR to `15gifts/adr` with completed `adr.md`
- [ ] **Step 0f:** Merge ADR PR
  - Done when: ADR PR merged; PII handling approach confirmed

---

### Step 1 — [CONFIG] Python project setup
- [ ] Create `pyproject.toml` with `uv`, `pydantic>=2.0`, `requests`, `snowflake-snowpark-python`, `pytest`, `ruff`
- [ ] Create `ruff.toml` (line-length 100, Python 3.12 target)
- [ ] Create `.python-version` (3.12)
- [ ] Create `src/batch_enrichment/__init__.py`
- Files: `pyproject.toml`, `ruff.toml`, `.python-version`, `src/batch_enrichment/__init__.py`
- Done when: `uv sync` succeeds; `ruff check .` passes

---

### Step 2 — [TEST] Unit tests: Pydantic models and JSONL builder (RED)
- [ ] Write failing tests for `models.py` and `jsonl_builder.py`
  - Test cases: T1, T2, T3, T18, T19
- Files: `tests/unit/test_models.py`, `tests/unit/test_jsonl_builder.py`
- Done when: tests exist and fail (`ModuleNotFoundError` or `AttributeError`)

### Step 3 — [CODE] Implement Pydantic models
- [ ] `BatchStatus` enum (all valid transitions)
- [ ] `ConversationMessage` dataclass (message_sent_by, message_text_combined, conversation_message_num)
- [ ] `ConversationTranscript` dataclass (conversation_id, messages list)
- [ ] `SubmitConfig` dataclass (chunk_size, max_active_batches, model_deployment, prompt_version, analytics_db)
- [ ] `RetrieveConfig` dataclass
- [ ] `EnrichmentResult` Pydantic model (all enrichment fields + parse_error)
- Files: `src/batch_enrichment/models.py`

### Step 4 — [CODE] Implement JSONL builder
- [ ] `build_jsonl_line(transcript: ConversationTranscript, config: SubmitConfig) -> str`
- [ ] `build_batch_lines(transcripts: list[ConversationTranscript], config: SubmitConfig) -> str`
- [ ] System prompt as a module-level constant (`SYSTEM_PROMPT_V1`)
- Files: `src/batch_enrichment/jsonl_builder.py`
- Done when: T1, T2, T3, T18, T19 pass

---

### Step 5 — [TEST] Unit tests: response parser (RED)
- [ ] Write failing tests for `response_parser.py`
  - Test cases: T4, T5, T11, T12
- Files: `tests/unit/test_response_parser.py`
- Done when: tests exist and fail

### Step 6 — [CODE] Implement response parser
- [ ] `parse_batch_line(line: str, batch_tracking_id: str) -> EnrichmentResult`
  - Handles malformed JSON, missing fields — returns `parse_error=True`, never raises
- [ ] `parse_batch_lines(output_jsonl: str, batch_tracking_id: str) -> list[EnrichmentResult]`
- Files: `src/batch_enrichment/response_parser.py`
- Done when: T4, T5, T11, T12 pass

---

### Step 7 — [CODE] Snowflake DDL — warehouse, schema, tables

- [ ] `sql/01_warehouse.sql` — `enrichment_wh` (XSMALL, auto-suspend 60s)
- [ ] `sql/02_schema_and_tables.sql` — `datalake.llm_enrichments` schema + `batch_tracking`, `batch_row_mapping`, `enrichment_results` tables (full DDL per Section 6)
- Files: `sql/01_warehouse.sql`, `sql/02_schema_and_tables.sql`

### Step 8 — [CODE] Snowflake DDL — enrichment_queue view

- [ ] `sql/03_views.sql` — `enrichment_queue` view
  - Cross-database ref: `{analytics_db}.info_general.fact_conversation_messages`
  - Include `analytics_db` as a parameterised reference (Snowflake variable or hardcoded with comment)
- Files: `sql/03_views.sql`

### Step 9 — [CODE] Snowflake DDL — security objects (placeholder credentials)

- [ ] `sql/04_security.sql`:
  - `GENERIC_STRING` Secret with `SECRET_STRING = 'PLACEHOLDER'`
  - Network Rule with `VALUE_LIST = ('PLACEHOLDER.openai.azure.com')` — update with real endpoint before Step 22
  - External Access Integration binding both
- Files: `sql/04_security.sql`

---

> ⛔ HOLD — HUMAN APPROVAL REQUIRED
>
> **Action:** Before deploying, confirm the `raw_response VARIANT` storage decision: the `enrichment_results` table will store the full Azure response object, which may echo transcript content (and therefore PII). For pilot this is restricted to `LLM_ENRICHMENT_ADMIN` role access only.
>
> **Risk:** PII stored in `raw_response` field accessible to any role granted `LLM_ENRICHMENT_ADMIN`. This decision must be consistent with the ADR outcome.
>
> **Area:** Data / privacy architecture — confirm ADR sign-off covers this before proceeding.
>
> Reply **CONFIRM** to proceed to the infrastructure deploy step.

> ⛔ HOLD — HUMAN APPROVAL REQUIRED
>
> **Action:** Execute `sql/01` through `sql/04` against the Snowflake environment, creating `enrichment_wh`, schema `datalake.llm_enrichments`, all tables, the enrichment queue view, and security objects with placeholder credentials.
>
> **Risk:** Creates a new schema in the live `datalake` database. DDL is reversible via DROP. No data affected.
>
> **Area:** Production infrastructure change
>
> Reply **CONFIRM** to proceed to Step 10.

### Step 10 — [CONFIG] Deploy infrastructure DDL to Snowflake
- [ ] Execute `sql/01` through `sql/04` against target Snowflake environment
- [ ] Verify: `enrichment_wh` exists; `datalake.llm_enrichments` schema exists; all tables created; `enrichment_queue` view returns rows
- Done when: `SELECT COUNT(*) FROM datalake.llm_enrichments.enrichment_queue` returns a row count

---

### Step 11 — [CODE] Submit stored procedure

- [ ] `src/batch_enrichment/submit.py` — `submit_batch(session, config: SubmitConfig) -> dict`
  - All 10 steps from Section 6 Submit design
  - Uses `jsonl_builder` for payload construction
  - Azure HTTP calls via `requests` (injected or imported — testable)
  - Exception handling for partial failures (steps 6–9)
- [ ] `sql/05_submit_procedure.sql` — `CREATE OR REPLACE PROCEDURE submit_batch_sp(...)` referencing handler in `submit.py` staged on Snowflake stage
- Files: `src/batch_enrichment/submit.py`, `sql/05_submit_procedure.sql`, `sql/05a_stage.sql`

### Step 12 — [CODE] Retrieve stored procedure

- [ ] `src/batch_enrichment/retrieve.py` — `retrieve_batch(session, config: RetrieveConfig) -> dict`
  - All 9 steps from Section 6 Retrieve design
  - Uses `response_parser` for output parsing
  - Per-row error handling (T17)
- [ ] `sql/06_retrieve_procedure.sql`
- Files: `src/batch_enrichment/retrieve.py`, `sql/06_retrieve_procedure.sql`

### Step 13 — [CODE] Snowflake Tasks (suspended)

- [ ] `sql/07_tasks.sql`:
  - `submit_batch_task`: `SCHEDULE = 'USING CRON 0 */2 * * * UTC'`, `ALLOW_OVERLAPPING_EXECUTION = FALSE`, `SUSPEND` immediately after creation
  - `retrieve_batch_task`: `SCHEDULE = 'USING CRON */30 * * * * UTC'`, suspended
- Files: `sql/07_tasks.sql`
- Note: Tasks remain suspended until Phase 2. Pilot uses manual `CALL` statements.

---

### Step 14 — [TEST] Integration tests: submit procedure (RED)

- [ ] Write failing integration tests against dev Snowflake (mock Azure HTTP layer)
  - Test cases: T6, T7, T13, T14, T15, T20, T21, T22
- Files: `tests/integration/test_submit.py`
- Note: Azure HTTP calls stubbed using `responses` or `httpretty` library
- Done when: tests exist and fail for the right reason

### Step 15 — [TEST] Integration tests: retrieve procedure (RED)

- [ ] Write failing integration tests
  - Test cases: T8, T16, T17
- Files: `tests/integration/test_retrieve.py`
- Done when: tests exist and fail

### Step 16 — [CODE] Deploy stored procedures and tasks to dev Snowflake

- [ ] Execute `sql/05a_stage.sql` to create `batch_enrichment_stage`
- [ ] Upload Python handler files to the stage (see **Deploying Python to the stage** below)
- [ ] Execute `sql/05_submit_procedure.sql`, `sql/06_retrieve_procedure.sql`, `sql/07_tasks.sql`
- [ ] Run integration test suite: `uv run pytest tests/integration/`
- Done when: T6–T8, T13–T17, T20–T22 all pass

#### Deploying Python to the stage

The stored procedures reference Python modules via `IMPORTS = ('@batch_enrichment_stage/<file>.py', ...)`. Those files must be present in the stage **before** `CREATE PROCEDURE` runs, and **re-uploaded after every change** — the stage is a snapshot, not a watch on the repo.

**Manual path (pilot — current approach):** Upload via the Snowsight stage browser.

1. Snowsight → Data → Databases → DATALAKE → LLM_ENRICHMENTS → Stages → BATCH_ENRICHMENT_STAGE
2. Click **+ Files** and select every `.py` file under `src/batch_enrichment/`
3. Files land flat in the stage root, which is what the `IMPORTS` clause expects
4. **On re-deploy:** delete the old version of each changed file via the ⋮ menu before re-uploading. The Snowsight uploader does not overwrite by default, and Snowflake will pick a version unpredictably if both exist.

**Caveats with the manual path:**

- No history, no diff — you cannot tell from Snowsight which git SHA is currently deployed
- No atomic deploy — between deleting the old file and uploading the new one, the procedure is broken
- Easy to forget a file when several modules changed together
- Cannot be automated from CI

**Scripted path (required before this leaves pilot):** Install SnowSQL (or use the Snowflake Python connector) and run:

```bash
snowsql -a <account> -u <user> -r LLM_ENRICHMENT_ROLE -w ENRICHMENT_WH \
        -d DATALAKE -s LLM_ENRICHMENTS \
        -q "PUT file://src/batch_enrichment/*.py @batch_enrichment_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
```

`OVERWRITE=TRUE` is the key flag — it makes redeploys idempotent. This is the path that must be used from CI/CD (GitHub Actions) and is a Phase 2 prerequisite for production. A follow-up ticket should add a `deploy.sh` wrapper and a GitHub Actions job that runs it on merge to `main`.

**Pilot deployment status (record):** Files have been uploaded to `@batch_enrichment_stage` via the Snowsight UI. Source of truth for the deployed code is the git SHA noted in the deploy ticket comment — confirm before any production cutover.

---

### Step 17 — [CODE] dbt source: add `llm_enrichments`

- [ ] Add source definition to `datawarehouse/models/_sources.yml`:
  ```yaml
  - name: llm_enrichments
    database: datalake
    schema: llm_enrichments
    tables:
      - name: enrichment_results
        columns:
          - name: enrichment_id
          - name: conversation_id
          - name: prompt_version
          - name: sentiment
          - name: primary_intent
          - name: topics
          - name: resolution_status
          - name: customer_effort_score
          - name: summary
          - name: parse_error
          - name: batch_tracking_id
          - name: enriched_at
  ```
- Files: `datawarehouse/models/_sources.yml`

### Step 18 — [TEST] dbt tests: enrichment models (RED)

- [ ] Write schema YML tests for `int_conversations_enriched` and `fact_conversation_labels`:
  - `not_null` on key columns (conversation_id, sentiment, primary_intent, resolution_status)
  - `accepted_values` for sentiment, primary_intent, resolution_status
  - `unique` on conversation_id in `fact_conversation_labels` (one label set per conversation)
  - Custom test: `parse_error_rate` < 5% of rows (warn threshold)
- [ ] Stub `enrichment_results` source with seed data for CI
- Files: `datawarehouse/models/intermediate/enrichment/_intermediate__enrichment__models.yml`, `datawarehouse/models/information_mart/_information_mart__models.yml`
- Done when: `dbt test --select int_conversations_enriched fact_conversation_labels` fails with `relation does not exist`

### Step 19 — [CODE] `int_conversations_enriched` dbt model

- [ ] `datawarehouse/models/intermediate/enrichment/int_conversations_enriched.sql`
  - Joins `{{ ref('fact_conversation_messages') }}` to `{{ source('llm_enrichments', 'enrichment_results') }}` on `conversation_id`
  - Filters `parse_error = FALSE`
  - Groups to one row per conversation (latest `enriched_at`)
  - Includes: `conversation_hkey`, `conversation_id`, `customer_id`, `environment`, all label columns, `enriched_at`
- [ ] Update `datawarehouse/dbt_project.yml` — add `intermediate/enrichment` subdirectory config (schema: `enrichment`, database: `{target}_intermediate`)
- Files: `datawarehouse/models/intermediate/enrichment/int_conversations_enriched.sql`, `datawarehouse/dbt_project.yml`
- Done when: relevant dbt tests pass

### Step 20 — [CODE] `fact_conversation_labels` dbt model

- [ ] `datawarehouse/models/information_mart/fact_conversation_labels.sql`
  - References `{{ ref('int_conversations_enriched') }}` and `{{ ref('fact_conversations') }}`
  - Joins on `conversation_hkey`
  - Final columns: all enrichment labels + `customer_id`, `customer_hkey`, `conversation_start_at`, `conversation_end_at`, `environment`, `is_supported`, `is_assisted`, `prompt_version`, `enriched_at`
  - Materialised: incremental, `unique_key: ['conversation_id', 'prompt_version']` (dbt compound key syntax)
- [ ] Update `datawarehouse/models/information_mart/_information_mart__models.yml` with column docs and tests
- Files: `datawarehouse/models/information_mart/fact_conversation_labels.sql`, `datawarehouse/models/information_mart/_information_mart__models.yml`
- Done when: T9, T10, T24 pass

---

### Step 21 — [DOCS] Update documentation

- [ ] Update `CLAUDE.md` in this repo with confirmed warehouse name, Snowflake database structure, and dbt deployment notes
- [ ] Create `README.md` with: purpose, architecture diagram (text), prerequisites, deployment order, how to run a pilot batch manually
- Files: `CLAUDE.md`, `README.md`

---

> ⛔ HOLD — HUMAN APPROVAL REQUIRED
>
> **Action:** Update the Snowflake Secret `datalake.llm_enrichments.azure_openai_key` with the real Azure OpenAI API key, and update the Network Rule with the real Azure endpoint hostname.
>
> **Risk:** Writes the API key into Snowflake secret storage. Key grants access to Azure OpenAI — charges will accrue on first live API call. Secret is managed by Snowflake; not visible after write.
>
> **Area:** Configuration change — secrets management
>
> Reply **CONFIRM** to proceed to Step 22.
> Note: Step 22 is blocked until Azure access credentials are obtained.

### Step 22 — [CONFIG] Configure Azure credentials (blocked on access)

- [ ] `ALTER SECRET datalake.llm_enrichments.azure_openai_key SET SECRET_STRING = '<api-key>';`
- [ ] `ALTER NETWORK RULE datalake.llm_enrichments.azure_openai_network_rule SET VALUE_LIST = ('<resource>.openai.azure.com');`
- [ ] Verify connectivity: call a minimal test procedure that makes a GET to the Azure endpoint and returns HTTP status
- Done when: test procedure returns 200 from Azure

---

> ⛔ HOLD — HUMAN APPROVAL REQUIRED
>
> **Action:** Manually call `submit_batch_sp` and then `retrieve_batch_sp` against real data (10 conversations). This sends real transcript data to Azure OpenAI and incurs API charges.
>
> **Risk:** PII in transcripts sent to Azure. Cost incurred (10 conversations × ~2,500 tokens = ~25,000 tokens at batch pricing ≈ $0.01). Results written to `datalake.llm_enrichments.enrichment_results`.
>
> **Area:** Production deployment — first live API call with real data
>
> Reply **CONFIRM** to proceed to Step 23.

### Step 23 — [TEST] Pilot end-to-end run (T23, T24)

- [ ] `CALL datalake.llm_enrichments.submit_batch_sp();`
- [ ] Verify `batch_tracking` row with `SUBMITTED` status; check `batch_row_mapping` has 10 rows
- [ ] Wait for Azure batch completion (up to 24h; typical ~2h for small batches)
- [ ] `CALL datalake.llm_enrichments.retrieve_batch_sp();`
- [ ] Verify 10 rows in `enrichment_results` with `parse_error = FALSE`
- [ ] `cd datawarehouse && dbt run --select int_conversations_enriched fact_conversation_labels && dbt test --select int_conversations_enriched fact_conversation_labels`
- [ ] Verify `fact_conversation_labels` rows queryable from Omni

---

## 9. Files to be Modified

### `batch_api_enrichment_service` (this repo)

| File | Change Type | Reason |
|---|---|---|
| `pyproject.toml` | CREATE | Python project configuration |
| `ruff.toml` | CREATE | Linter configuration |
| `.python-version` | CREATE | Pin Python 3.12 |
| `src/batch_enrichment/__init__.py` | CREATE | Package marker |
| `src/batch_enrichment/models.py` | CREATE | Pydantic v2 models and enums |
| `src/batch_enrichment/jsonl_builder.py` | CREATE | JSONL payload builder |
| `src/batch_enrichment/response_parser.py` | CREATE | Azure response parser |
| `src/batch_enrichment/submit.py` | CREATE | Submit procedure business logic |
| `src/batch_enrichment/retrieve.py` | CREATE | Retrieve procedure business logic |
| `sql/01_warehouse.sql` | CREATE | Warehouse DDL |
| `sql/02_schema_and_tables.sql` | CREATE | Schema + table DDL |
| `sql/03_views.sql` | CREATE | enrichment_queue view |
| `sql/04_security.sql` | CREATE | Secret, network rule, EAI (placeholder) |
| `sql/05a_stage.sql` | CREATE | Snowflake stage for Python files |
| `sql/05_submit_procedure.sql` | CREATE | Submit stored procedure |
| `sql/06_retrieve_procedure.sql` | CREATE | Retrieve stored procedure |
| `sql/07_tasks.sql` | CREATE | Tasks (suspended) |
| `tests/unit/test_models.py` | CREATE | Unit tests for models |
| `tests/unit/test_jsonl_builder.py` | CREATE | Unit tests for JSONL builder |
| `tests/unit/test_response_parser.py` | CREATE | Unit tests for response parser |
| `tests/integration/test_submit.py` | CREATE | Integration tests for submit procedure |
| `tests/integration/test_retrieve.py` | CREATE | Integration tests for retrieve procedure |
| `CLAUDE.md` | MODIFY | Update with confirmed environment details |
| `README.md` | CREATE | Deployment and operations guide |

### `datawarehouse` (sister repo — separate PR)

| File | Change Type | Reason |
|---|---|---|
| `models/_sources.yml` | MODIFY | Add `llm_enrichments` source |
| `models/intermediate/enrichment/int_conversations_enriched.sql` | CREATE | Intermediate enrichment model |
| `models/intermediate/enrichment/_intermediate__enrichment__models.yml` | CREATE | Schema tests and docs |
| `models/information_mart/fact_conversation_labels.sql` | CREATE | Mart model for Omni |
| `models/information_mart/_information_mart__models.yml` | MODIFY | Add `fact_conversation_labels` docs and tests |
| `dbt_project.yml` | MODIFY | Add `intermediate/enrichment` subdirectory config |

---

## 10. Manual Verification

1. After Step 10: `SELECT COUNT(*) FROM datalake.llm_enrichments.enrichment_queue` returns rows
2. After Step 16: all integration tests pass; `SHOW PROCEDURES` lists both stored procedures
3. After Step 22: test connectivity procedure returns HTTP 200 from Azure
4. After Step 23: `SELECT * FROM datalake.llm_enrichments.enrichment_results LIMIT 10` shows 10 enriched conversations with non-null `sentiment`, `primary_intent`, `resolution_status`
5. After Step 23: `dbt test --select fact_conversation_labels` passes all tests

### Regression Risk

- `fact_conversation_messages` and `fact_conversations` in `datawarehouse` are unmodified — no regression risk to existing models
- The new `llm_enrichments` source is additive — no existing dbt tests are affected
- Enrichment queue view is read-only against `fact_conversation_messages` — no write risk

---

## 11. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| ADR not approved before team wants to start implementing | MED | HIGH | Flag immediately; do not begin Step 1 without ADR merge |
| Azure endpoint not on public internet (Private Link required) | MED | HIGH | Test connectivity in Step 22 before pilot; note Private Link path as Phase 2 |
| Snowflake role lacks CREATESCHEMA on `datalake` | LOW | HIGH | Confirm with Snowflake admin before Step 10 |
| `fact_conversation_messages` analytics DB name differs from `prod_analytics` | LOW | MED | Parameterise `analytics_db` in view DDL; confirm at deploy time |
| LLM produces `parse_error` for many pilot rows | MED | MED | 10-row pilot exposes prompt issues cheaply; fix prompt before scale-up |
| Transcript text contains PII sent to Azure | HIGH | HIGH | ADR must confirm legal sign-off; PII redaction deferred to ADR outcome |
| Azure batch API quota insufficient for concurrent batches | LOW | LOW | Pilot is 1 batch of 10 rows — well within any quota |
| LISTAGG 16MB per-group limit silently truncates long transcripts | LOW | MED | Phase 1 pilot transcripts expected well under limit; Phase 2 must add SUBSTR guard and error signal |
| Concurrent manual `CALL submit_batch_sp()` double-enqueues conversations | LOW | MED | Phase 1 mitigation: calls are sequential; `max_active_batches=1` limits window; Phase 2 runbook must enforce no concurrent calls |
| JSONL payload exceeds 200MB | LOW | LOW | 10 rows × ~5KB each = ~50KB; not relevant for pilot |
| Task overlap on production schedule | LOW | MED | Tasks are SUSPENDED for Phase 1; `ALLOW_OVERLAPPING_EXECUTION = FALSE` set for Phase 2 readiness |

---

## 12. Documentation Updates

| Document | Update Required? | What needs to change |
|---|---|---|
| `CLAUDE.md` (this repo) | YES | Confirm warehouse name, Snowflake DB, analytics_db variable, dbt deployment path |
| `README.md` (this repo) | YES (CREATE) | Purpose, architecture, prerequisites, deployment order, pilot run instructions |
| `datawarehouse` `_sources.yml` | YES | Add `llm_enrichments` source definition |
| `datawarehouse` model YML files | YES | Add column docs and tests for new models |
| `halo-artefacts/AE/AE-1540/plan.md` | — | This document |

---

## 13. Rollback Plan

### Rollback Steps

1. Suspend tasks (already suspended in Phase 1 — no action needed)
2. DROP VIEW `datalake.llm_enrichments.enrichment_queue`
3. DROP PROCEDURE `submit_batch_sp`; DROP PROCEDURE `retrieve_batch_sp`
4. DROP TABLE `datalake.llm_enrichments.enrichment_results`
5. DROP TABLE `datalake.llm_enrichments.batch_tracking`
6. DROP TABLE `datalake.llm_enrichments.batch_row_mapping`
7. DROP SCHEMA `datalake.llm_enrichments`
8. Revert `datawarehouse` PR — removes `fact_conversation_labels`, `int_conversations_enriched`, source definition

### Rollback Risks

- Enrichment results already consumed by Omni dashboards (Phase 2 risk, not Phase 1) — no dashboards exist yet
- No data in `fact_conversation_messages` or existing models is modified — rollback has zero impact on upstream

---

## 14. Open Questions

| # | Question | Blocking? | Owner | Resolution |
|---|---|---|---|---|
| 1 | ADR approved? PII → Azure legally cleared? `raw_response` storage approved? | YES | Robert Bramwell + Jack Barker-Davy + Architecture Forum | Resolve via ADR process (Step 0, Significant level) before any implementation |
| 2 | What is the exact Snowflake target name? (e.g. `prod-analytics` → `prod_analytics` DB) | YES (Step 8) | Robert Bramwell | Confirm with Snowflake admin; parameterise `analytics_db` in view DDL |
| 3 | What Snowflake role has CREATESCHEMA on `datalake`? | YES (Step 10) | Robert Bramwell | Confirm with Snowflake admin before running DDL |
| 4 | Is Azure OpenAI endpoint on public internet or requires Private Link? | YES (Step 22) | Robert Bramwell | Test with connectivity procedure; Private Link = Phase 2 if needed |
| 5 | What is the Azure OpenAI deployment name for GPT-4.1? | NO (placeholder until Step 22) | Robert Bramwell | Obtain with API key; fill into `SubmitConfig.model_deployment` |

---

## 15. Agent Confidence Assessment

**Confidence Rating:** HIGH

**Justification:**
The design document provides exhaustive architecture detail. The source data schema is fully known from `fact_conversation_messages`. The dbt project structure is understood. The Python helper modules (JSONL builder, response parser) are pure functions with well-defined inputs and outputs — high confidence in testability. Snowpark stored procedure patterns are well-established. All unknowns are environment-configuration items (database name, roles, Azure endpoint) that block deployment steps but not code authoring steps.

**Blocking uncertainties:**
- ADR must be approved before implementation begins (Step 0 is a hard gate)
- Snowflake target name and `CREATESCHEMA` role needed before Step 10 — does not block Steps 1–9 (Python + SQL authoring)
- Azure credentials needed before Step 22 — does not block Steps 1–21
