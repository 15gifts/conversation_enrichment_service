# CLAUDE.md — batch_api_enrichment_service

Product: Humara

## Shell Environment

Before running any shell commands, source the shell profile and (where required) the CodeArtifact authentication script:

```bash
source ~/.zshrc && source "<authenticate_script_path>"
```

Replace `<authenticate_script_path>` with the value in `CLAUDE.local.md`. This must be done before any command that installs Python packages from CodeArtifact.

---

## What This Repo Does

Batch LLM enrichment pipeline that classifies conversational transcripts at scale using Azure OpenAI GPT-4.1. Built entirely on Snowflake and dbt — no external orchestration infrastructure.

**Architecture:**
1. **Ingestion** — raw transcripts land in Snowflake via existing mechanism (Fivetran, Kafka, COPY INTO)
2. **Submission** — a Snowflake Task fires on schedule, selects unenriched rows, serialises as JSONL, uploads to Azure OpenAI Files API, submits batch job
3. **Retrieval** — a second Snowflake Task polls for completed jobs, downloads results JSONL, parses LLM output, writes to output table
4. **Transformation** — dbt models join enrichment output to user/event/product data for Omni funnel and cohort analysis

**Key constraint:** Must use Azure OpenAI (contractual). All orchestration stays inside Snowflake — no Airflow, Dagster, or Lambda.

---

## Tech Stack

| Component | Technology | Notes |
|---|---|---|
| Orchestration | Snowflake Tasks | Two tasks: submit (every 2h) and retrieve (every 15–30m) |
| Business logic | Snowpark Python stored procedures | With External Access Integration for Azure calls |
| Transformation | dbt | Staging → intermediate → mart models |
| LLM | Azure OpenAI GPT-4.1 (Batch API) | 24h completion window |
| Visualisation | Omni | Queries dbt mart models |
| Secrets | Snowflake Secrets | Azure OpenAI API key |
| Network | Snowflake External Access Integration | Restricts egress to specific Azure endpoint |
| Package manager | `uv` (expected) | Not yet configured |
| Language | Python 3.12+ | Snowpark stored procedures |
| Linter | Ruff | Not yet configured |

---

## Snowflake Object Inventory

| Object | Type | Purpose |
|---|---|---|
| `RAW_TRANSCRIPTS` | Table | Landing zone for raw conversational data |
| `STG_CONVERSATIONS` | dbt staging model / view | Cleaned, deduplicated transcripts |
| `ENRICHMENT_QUEUE` | View | Unenriched rows eligible for submission |
| `BATCH_TRACKING` | Table | Central state machine — every batch job |
| `BATCH_ROW_MAPPING` | Table | Maps conversation_id → batch, prevents double-processing |
| `ENRICHMENT_RESULTS` | Table | Parsed LLM output (sentiment, intent, topics, resolution, summary) |
| `SUBMIT_BATCH_TASK` | Snowflake Task | Submits on schedule (CRON `0 */2 * * * UTC`) |
| `RETRIEVE_BATCH_TASK` | Snowflake Task | Polls and retrieves (CRON `*/30 * * * * UTC`) |
| `SUBMIT_BATCH_SP` | Stored Procedure (Python) | Submission: chunking, JSONL, Azure Files + Batches API |
| `RETRIEVE_BATCH_SP` | Stored Procedure (Python) | Retrieval: poll status, parse, write results |
| `AZURE_OPENAI_SECRET` | Secret | Azure OpenAI API key |
| `AZURE_OPENAI_NETWORK_RULE` | Network Rule | Restricts egress to Azure endpoint |
| `AZURE_OPENAI_EAI` | External Access Integration | Binds network rule + secret |
| `INT_CONVERSATIONS_ENRICHED` | dbt intermediate model | Joins enrichment results to conversations |
| `MART_CONVERSATION_LABELS` | dbt mart model | Final dimensions queryable by Omni |

---

## Batch Status Machine

Valid transitions: `PENDING → SUBMITTING → SUBMITTED → IN_PROGRESS → COMPLETED`
Failure path: `→ FAILED → RETRYING → SUBMITTED`
Terminal failure: `→ PERMANENTLY_FAILED`

---

## Key Design Rules

1. **Idempotency is non-negotiable.** Check `BATCH_TRACKING` for `IN_PROGRESS` batches before submitting new rows. Mark rows in `BATCH_ROW_MAPPING` before calling the Azure API — not after.
2. **Write row mappings before the API call.** If the procedure crashes after the API call but before updating the tracking table, you get an orphaned Azure batch. The mapping table is the guard.
3. **Handle per-row failures in completed batches.** `status=completed` in Azure does not mean every row succeeded. Parse `error` field per row; write successes and log failures separately.
4. **Prompt versioning is mandatory.** Store `prompt_version` in `BATCH_TRACKING` and propagate to `ENRICHMENT_RESULTS`. Changing the prompt without versioning corrupts analytics.
5. **Stream, don't load.** Large output JSONL files (10k rows × 500 tokens) can exceed Snowpark memory. Stream in chunks rather than loading the full response into memory.
6. **Exponential backoff for retries.** Use `next_retry_after` timestamp on `BATCH_TRACKING` rows. Never retry aggressively against a rate-limited endpoint.
7. **Temperature=0, `response_format=json_object`.** Even then, LLMs can produce malformed JSON. Handle `PARSE_ERROR` gracefully — log raw output, don't block the batch.
8. **No direct LLM calls from dbt.** dbt owns transformation only — all Azure API calls are in stored procedures.

---

## Commands

```bash
# Run dbt models (from dbt project root)
dbt run

# Run dbt tests
dbt test

# Check source freshness
dbt source freshness

# Deploy stored procedures to Snowflake (TBD — add once tooling is configured)

# Lint Python
ruff check .

# Format Python
ruff format .
```

---

## Coding Standards

This repo follows HALO coding standards. **You must read the applicable standards before writing or reviewing code.**

Standards are maintained in the [HALO repository](https://github.com/15gifts/halo) and accessible locally via skill symlinks.

| Standard | Path |
|---|---|
| Developer standards | `~/.claude/skills/halo-init/halo/standards/developer.md` |
| Best practices | `~/.claude/skills/halo-init/halo/standards/best-practices.md` |
| Python standards | `~/.claude/skills/halo-init/halo/standards/python.md` |
| Humara product standards | `~/.claude/skills/halo-init/halo/products/humara/standards.md` |

---

## HALO Workflow

This repo uses [HALO](https://github.com/15gifts/halo) skills for AI-assisted development. Skills are installed globally via `~/.claude/skills/` — there is nothing to install per-repo.

### Workflow

Every piece of work follows: **Plan → Second Opinion → Implement → Code Review**

| Phase | Skill | What happens |
|---|---|---|
| Plan | `/halo-plan` | Iterative planning session — defines tests before implementation |
| Second opinion | `/halo-second-opinion` | Independent adversarial review of the plan |
| Implement | `/halo-implement` | TDD red-green-refactor guided by the plan |
| Code review | `/halo-review-code` | Independent review: quality, standards, tests, security |

Additional skills: `/halo-security-review`, `/halo-check-standards`, `/halo-test-review`

### Artefact Directory

All HALO artefacts for a ticket are stored in:

```
halo-artefacts/{{namespace}}/{{ticket-id}}/
├── plan.md              ← Technical plan
├── second-opinion.md    ← Adversarial review
├── review.md            ← Code review
├── security-review.md   ← Security audit
└── adr.md               ← ADR draft (if required)
```

- Each ticket or branch gets its own directory under `halo-artefacts/`
- Skills create and look up artefacts in this directory automatically
- These artefacts are committed to the repo and referenced in PRs
- The plan must have a second opinion review before implementation begins

---

## Maintained Documents

| Document | When to update |
|---|---|
| `implementation_design_1.docx` | Architecture reference — update if pipeline design changes significantly |
| `CLAUDE.md` (this file) | Run `/halo-init` when stack or key patterns change |

---

## Overrides

No TDD exemptions apply to this repo.

---

## Security Notes

- Transcripts likely contain PII — consider Snowflake Cortex `AI_REDACT` preprocessing before sending to Azure OpenAI
- Azure OpenAI resource must be deployed in a region satisfying data residency requirements
- Secrets via Snowflake Secrets only — never hardcoded in stored procedure code
- External Access Integration restricts egress to the configured Azure endpoint only
- `LLM_ENRICHMENT_ROLE` role owns EAI, Secrets, and stored procedures; dbt role gets read-only on `ENRICHMENT_RESULTS`
