## Second Opinion Review

**Reviewer:** AI Second Opinion (independent)
**Subject:** Technical Plan — Batch LLM Enrichment Pipeline (AE-1540), Phase 1 Pilot
**Date:** 2026-05-05

---

### Summary Verdict

AGREE WITH RESERVATIONS

The plan is well-structured and the core architecture is sound. The ADR gate, HITL hold steps, and test coverage are unusually thorough for a pilot plan. However, there are two issues the primary review did not surface: a material race condition in the submit procedure that the test suite does not cover, and a prompt injection risk that is unacknowledged despite the security standards requiring a threat model for external data boundaries. Neither is fatal for a 10-row pilot, but both become blocking concerns before any scale-up, and the race condition in particular should be addressed in the code before Phase 1 is signed off as a safe pattern for Phase 2.

---

### Issues Not Raised by Primary Review

#### BLOCKING

- **Missing threat model for the Azure trust boundary.** The security standards (`halo/standards/security.md`) require a threat model for every plan that introduces an external communication boundary. The plan's risk register notes PII risk but does not answer the four required questions: what assets are being protected, who are the adversaries, what are the attack vectors, and what are the mitigations beyond "ADR required." The `raw_response VARIANT` column stores the full Azure response object, which may contain model completions that echo back transcript fragments containing PII. There is no mention of what happens to this column if the ADR outcome restricts transcript egress — the data is already stored. The risk is classified HIGH/HIGH in the register but the mitigation is entirely deferred. Per the HITL standard (`halo/standards/hitl.md`), a HIGH/HIGH risk without an in-scope control requires an explicit hold step, not just a risk register entry. The existing hold before Step 23 covers first live execution, but there is no hold guarding the storage design decision (storing `raw_response` containing echoed PII). This is a BLOCKING issue because the HITL standard says: "a missing hold step for a high-risk area is a BLOCKING issue."

- **ADR review level is not assessed.** The ADR process (`halo/standards/adr-guidance.md`) requires the plan to identify the review level. This change involves a new external LLM integration, a new PII data flow to a third-party provider, a new cost commitment, and a deliberate exemption from the platform rule that "all LLM calls go through axiom-conversation-api." That combination of triggers maps to at minimum **Significant** level (multi-service impact, new integration, PII data architecture change) and potentially **Strategic** (new vendor spend commitment, security architecture change). The plan identifies Jack Barker-Davy as a stakeholder but does not call out the Architecture Forum as a required reviewer, which the Significant/Strategic level mandates. If the ADR is submitted at Standard level when Significant/Strategic is correct, it will be sent back, blocking implementation. The plan should state the expected review level explicitly so the ADR process is not surprised.

#### WARNING

- **Race condition: `enrichment_queue` exclusion logic is not atomic with the INSERT into `batch_row_mapping`.** Steps 2 and 5 of the submit procedure are: (2) query `enrichment_queue` with `LIMIT chunk_size`, then (5) INSERT mapping rows. The `enrichment_queue` view excludes conversations where `brm.batch_status IN ('PENDING','SUBMITTING','SUBMITTED','IN_PROGRESS')`. However, between steps 2 and 5, a second concurrent `submit_batch_sp` call (possible even in Phase 1 if called manually twice quickly) can read the same conversations from the view before either has written to `batch_row_mapping`. Both calls then insert the same conversation IDs into `batch_row_mapping`, submit duplicate JSONL lines to Azure, and produce duplicate rows in `enrichment_results`. The `max_active_batches` guard (Step 1) only checks `batch_tracking` count, not in-progress concurrent writes — it does not protect against this. Test T15 covers `max_active_batches` reached but no test covers two concurrent submit calls racing on the same queue. This is flagged as WARNING rather than BLOCKING because the pilot is explicitly `max_active_batches=1` and manual execution, making the race unlikely in Phase 1 — but the code will be promoted to Phase 2 as written, and the pattern is unsafe at scale.

- **`enrichment_results` has no uniqueness constraint on `conversation_id + prompt_version`.** The retrieve procedure inserts rows into `enrichment_results` without any deduplication guard. If `retrieve_batch_sp` is called twice for the same completed batch (e.g. network timeout on the first call after INSERT but before the status UPDATE to COMPLETED), duplicate enrichment rows will be written. The dbt model `int_conversations_enriched` handles this by selecting `latest enriched_at`, which masks duplicates rather than preventing them. Test T8 does not test what happens when retrieve is called twice on the same completed batch. This should be a `UNIQUE` constraint or a `MERGE` insert on `(conversation_id, prompt_version)`.

- **`fact_conversation_labels` incremental unique key is composite `conversation_id + prompt_version` but no concatenation or surrogate key is defined.** The plan specifies `unique_key: conversation_id + prompt_version` but `+` is not valid dbt incremental syntax — this needs to be either a list `[conversation_id, prompt_version]` (dbt merge strategy) or a single surrogate key column. If interpreted literally as string concatenation, dbt will fail to deploy. This is implementation detail but it is stated in the plan and will cause the Step 20 implementation to fail or produce silent bugs.

- **RETRYING status is defined in the BatchStatus enum and referenced in the CLAUDE.md state machine, but the retrieve procedure design has no retry logic.** The plan defines `RETRYING` as a valid batch status and the `retry_count` / `max_retries` / `next_retry_after` fields exist in `batch_tracking`, but the retrieve procedure steps (Section 6) contain no step for handling `FAILED` batches by re-queuing them. The `enrichment_queue` view correctly re-exposes conversations with `FAILED` batch mapping rows, but there is no mechanism to reset `batch_tracking` status from `FAILED` to `RETRYING` and resubmit the Azure batch. The state machine and the procedure design are inconsistent. Test T16 verifies that a failed Azure batch marks `batch_tracking` as `FAILED` but no test verifies retry promotion. Either the retry path should be removed from the state machine (simplifying the design) or a procedure step and tests should be added. Leaving it as a partial design risks Phase 2 implementers assuming retry is implemented.

- **`LISTAGG` in `enrichment_queue` will silently truncate transcripts over 16MB.** Snowflake's `LISTAGG` has a 16MB result size limit per group. Long conversations (e.g. enterprise multi-hour support sessions) will be silently truncated without error. The plan notes JSONL payload size limits (`10 rows × ~5KB each = ~50KB`) but this assumes average conversations. Truncated transcripts will produce semantically wrong classifications without any `parse_error` flag — they will appear as clean results. This risk is not in the risk register. For the 10-row pilot the exposure is low, but the risk register should acknowledge it.

- **The Humara standard states "All LLM calls go through axiom-conversation-api." The plan cites an ADR exemption requirement but does not flag the domain custodian for the Global Guardrails domain (Jack Barker-Davy, James Pearce, Max Yousif).** The architecture document notes these custodians govern guardrail logic and LLM call patterns. This pipeline bypasses all Humara guardrails — no competitor check, no jailbreak defence, no PII detection — against customer transcripts. The ADR step names Jack Barker-Davy as a LLM governance stakeholder but does not identify the `#ask-global-guardrails` channel or the full custodian list as required reviewers. Since the Humara standards require proactive engagement with domain custodians before raising the PR, this should be explicit in Step 0.

---

### Alternative Approach Considered

The `raw_response VARIANT` column storing the full Azure response object is worth reconsidering. The plan stores the complete Azure API response "for debugging" — but this response will contain the model's generated text, which may echo back fragments of the input transcript (a common LLM behaviour). If the ADR outcome restricts PII egress, this column will need to be purged or redacted, requiring a migration. A safer default for Phase 1 would be to store only the parsed JSON object (the structured output fields) in `raw_response`, not the full Azure response envelope with usage stats and model metadata. The full envelope adds no analytics value and the usage/cost data is already captured elsewhere. This is a low-cost change at design time that avoids a potentially awkward migration later.

---

### What Was Missed

1. **Concurrent submit race condition** — two manual `CALL submit_batch_sp()` calls in quick succession can process the same conversations twice. No test covers this. See WARNING above.

2. **Duplicate retrieve** — calling `retrieve_batch_sp()` twice for a completed batch produces duplicate `enrichment_results` rows. No unique constraint and no test for this path.

3. **`LISTAGG` 16MB truncation** — long transcripts silently truncated, producing wrong classifications without any error signal.

4. **Partial RETRYING state machine** — `RETRYING` and `next_retry_after` are modelled but the retrieve procedure has no steps that use them. The code as designed will never transition a batch out of `FAILED` via retry.

5. **`unique_key` syntax error in dbt incremental model** — `conversation_id + prompt_version` is not valid dbt syntax and will fail at deploy time.

6. **No streaming for large JSONL download in retrieve.** The CLAUDE.md explicitly notes "Stream, don't load" as key design rule #5, but the retrieve procedure design (step 4: `GET /openai/files/{output_file_id}/content`) loads the full response body with no mention of streaming. For the 10-row pilot this is irrelevant; at scale (Phase 2, thousands of rows) this will exceed Snowpark memory. The plan should at minimum carry a TODO or note about the streaming requirement so Phase 2 implementers don't miss it.

7. **No network timeout or connection error handling in procedure designs.** The submit and retrieve steps describe happy-path and partial-failure handling (Azure 401, 429) but do not address network-level failures: TCP timeout, DNS failure, hung connection. A Snowpark procedure that hangs waiting on a network call will consume the warehouse until the task timeout fires. The security standards require explicit timeouts on all outbound calls. This should be in the test cases and the procedure design.

8. **The `enrichment_queue` view ordering (`ORDER BY conversation_started_at ASC`) is not stable** — if two conversations have the same `conversation_started_at`, the LIMIT clause will select a non-deterministic subset across calls. For the pilot this is inconsequential but for a production queue with high-resolution timestamps this is predictable (FIFO) enough. Worth noting for Phase 2 that a stable tiebreaker column (e.g. `conversation_id`) should be added to the ORDER BY.

---

### Confidence Assessment

The primary agent's HIGH confidence rating is partially justified — the Python helper modules, data model, and HITL structure are solid. However the rating is overconfident given: the unresolved PII/storage design question (which could require a `raw_response` migration), the absent threat model (required by security standards), and the partial retry state machine. A more honest rating for this plan in its current form is MEDIUM-HIGH: high confidence in the code-authoring steps (Steps 1–16), medium confidence in the infrastructure and data design decisions (specifically `raw_response` storage), and low confidence that the retry logic as partially designed will be correctly implemented without an additional planning pass.

The plan correctly identifies that Steps 1–21 can proceed before Azure credentials are obtained, which is a sound observation. The ADR gate at Step 0 is correctly placed and non-optional.

---

### Recommendation

PROCEED WITH CHANGES

The following must be resolved before the plan is considered implementation-ready:

1. **Add a formal threat model section** addressing the four required questions from the security standards. At minimum, explicitly address the `raw_response` storage decision in light of the PII risk — either accept and document it, restrict what is stored, or add an explicit hold step before the schema is deployed.

2. **Assess and state the ADR review level** (expected: Significant or Strategic). Add the Architecture Forum and full Global Guardrails custodian list (`#ask-global-guardrails`: Jack Barker-Davy, James Pearce, Max Yousif) as required stakeholders in Step 0.

3. **Add a test case for duplicate retrieve** (call retrieve twice on a completed batch — expect idempotent outcome). Add a `UNIQUE` constraint or `MERGE` insert to prevent duplicate `enrichment_results` rows.

4. **Fix the dbt `unique_key` syntax** — change `unique_key: conversation_id + prompt_version` to a valid dbt form.

5. **Resolve the RETRYING state machine gap** — either remove `RETRYING`/`next_retry_after` from the design (simplifying to `FAILED` as terminal), or add explicit procedure steps and tests for the retry path.

The race condition (WARNING #1) and streaming gap (Warning #6/What Was Missed #6) should be acknowledged in the risk register with a note that they are acceptable for the Phase 1 manual pilot but must be resolved before Phase 2 task scheduling is enabled.
