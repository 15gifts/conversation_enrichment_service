# ADR-[NUMBER]: Batch LLM Enrichment of Conversation Transcripts via Azure OpenAI Batch API

**Date:** 2026-05-05
**Status:** Proposed
**Deciders:** Robert Bramwell, [Engineering Lead], Architecture Forum,
  Jack Barker-Davy, James Pearce, Max Yousif (Global Guardrails),
  Ed Mitchell, Carlos Olivet, Tom Levy (Tracking)
**Jira brief:** [ADR ticket ID]

---

## Context

Humara produces millions of conversational transcripts. We have an existing tagging solution
that classifies these conversations, but it has two material problems:

1. **Cost**: the current approach is more expensive than using the Azure OpenAI Batch API,
   which offers asynchronous processing at approximately 50% lower cost than real-time API calls.

2. **Model compliance**: the current solution does not use the OpenAI models we are
   contractually obliged to use. Our contract specifies Azure OpenAI GPT-4.1, and the
   existing tagging approach does not satisfy this requirement.

Migrating to the Azure OpenAI Batch API for conversation enrichment reduces cost and brings
us into compliance with our contractual model requirement. However, this introduces three
architectural decisions that require formal governance:

1. **A new PII data flow**: conversation transcripts (which contain customer PII) would be sent
   to Azure OpenAI for processing. The existing tagging solution's data handling posture must
   be compared to the proposed approach and any delta assessed for legal sign-off.

2. **A new LLM call pattern**: Humara's established rule is that all LLM calls route through
   `axiom-conversation-api`. This batch analytics pipeline would make direct Azure OpenAI calls
   from a Snowflake stored procedure, bypassing that service entirely.

3. **`raw_response` storage**: the Azure API response object may echo transcript fragments
   in model completion metadata, potentially introducing PII into Snowflake storage under
   a new field.

---

## Decision Drivers

- Contractual requirement to use Azure OpenAI GPT-4.1 (existing tagging solution does not comply)
- Cost reduction: Azure OpenAI Batch API is ~50% cheaper than the current approach at scale
- Batch analytics enrichment is a batch/async use case; routing through a real-time conversation
  API is architecturally inappropriate
- Zero new infrastructure constraint: Snowflake-native orchestration preferred
- Data residency and PII handling must satisfy legal requirements for all affected markets
- The "LLM calls via axiom-conversation-api" rule was designed for real-time guardrails, not
  batch analytics — a formal exemption for analytics workloads avoids future ambiguity

---

## Options Considered

### Option A: Standalone Snowflake-native batch pipeline (proposed)

New `batch_api_enrichment_service` using Snowpark Python stored procedures, Snowflake Tasks,
and Azure OpenAI Batch API via Snowflake External Access Integration.

**Pros:**
- No new infrastructure (Snowflake-native, same toolchain as datawarehouse)
- Azure OpenAI Batch API pricing (~50% cheaper than current approach)
- Satisfies contractual GPT-4.1 model requirement
- Completely decoupled from real-time conversation path
- Post-hoc enrichment does not require guardrails (transcripts already stored)

**Cons:**
- New external egress boundary from Snowflake (new security objects required)
- Direct Azure calls bypass all Humara guardrails — requires formal exemption
- Snowpark Python is less mature toolchain than existing Python services

---

### Option B: Route through `axiom-conversation-api`

Add batch analytics enrichment capability to the existing conversation API service.

**Pros:**
- Consistent with "LLM calls via axiom-conversation-api" rule
- Guardrails available if required

**Cons:**
- Wrong separation of concerns — batch analytics in a real-time conversation service
- Requires `axiom-conversation-api` to read directly from the data warehouse
- Async batch job pattern does not fit a synchronous REST service
- Increases operational risk on the most critical Humara service

---

### Option C: Snowflake Cortex (native LLM)

Use Snowflake's built-in `COMPLETE()` function — no external API.

**Pros:**
- No new external trust boundary; no PII egress outside Snowflake

**Cons:**
- Does not satisfy the contractual Azure OpenAI GPT-4.1 requirement
- **Not viable**

---

## Decision Outcome

**Chosen option:** Option A — standalone Snowflake-native batch pipeline, because:

1. It is the only option that satisfies the Azure OpenAI contractual requirement.
2. It delivers the cost reduction goal (~50% vs. current approach).
3. Batch analytics enrichment of stored transcripts is architecturally distinct from
   real-time conversation handling — coupling them through `axiom-conversation-api` would
   violate separation of concerns.
4. The Snowflake-native approach introduces zero new infrastructure.

**Formal decisions made by this ADR:**

1. **PII data flow approved**: Conversation transcripts may be sent to Azure OpenAI
   [REGION] for batch analytics enrichment under the following conditions:
   [complete after legal review]

2. **`raw_response` storage posture**: [CHOOSE ONE]
   - Approved: full `raw_response` VARIANT stored, access restricted to
     `LLM_ENRICHMENT_ADMIN` role
   - OR: `raw_response` stripped to structured fields only before Snowflake insert

3. **Platform rule exemption granted**: Direct Azure OpenAI calls from analytics batch
   pipelines are exempt from the "all LLM calls via axiom-conversation-api" rule.
   Scope: analytics batch enrichment workloads only. Real-time conversation LLM calls
   continue to route through `axiom-conversation-api`.

---

## Consequences

**Positive:**
- Reduces classification cost (~50% vs. current tagging solution at scale)
- Brings conversation enrichment into contractual compliance (GPT-4.1)
- Enables richer structured analytics on conversation outcomes: funnel analysis, cohort
  labelling, resolution tracking
- Establishes a reusable pattern for future analytics enrichment (e.g. message-level labels)
- Zero infrastructure overhead; Snowflake-native orchestration

**Negative / Trade-offs:**
- New external trust boundary: Snowflake → Azure OpenAI. Requires ongoing credential
  management, network rule maintenance, and cost monitoring.
- New PII egress point. Must be monitored and documented in the data privacy register.
- If `raw_response` is stored: PII may be echoed back into Snowflake in a less-governed field.
  Access must be restricted and this field excluded from any broader data access grants.
- The platform rule exemption, if interpreted broadly, could be used to justify future direct
  LLM calls that should go through `axiom-conversation-api`. The exemption scope must be
  enforced narrowly.

---

## Implementation Notes

**Deployment order:**
1. ADR merged (this document)
2. Snowflake DDL deployed: schema, tables, view, security objects
3. Stored procedures and tasks deployed (tasks remain SUSPENDED)
4. Azure credentials provisioned and connectivity verified
5. Pilot run: 10 conversations, manual execution
6. dbt models deployed to datawarehouse
7. Phase 2 planning: task scheduling, backfill, PII redaction pre-processing

**Rollback trigger:** Any failed legal review, data breach, or cost overrun triggers
immediate task suspension and schema DROP. No existing services are affected.

**Monitoring:** Azure batch job status tracked in `batch_tracking` table.
Cost estimate stored per batch in `cost_estimate_usd`. Phase 2 must add Snowflake Alerts
on failed batch counts and cost thresholds.

---

## Links

- Jira design brief: [ticket URL]
- Implementation plan: `batch_api_enrichment_service/halo-artefacts/AE/AE-1540/plan.md`
- Affected repos: `15gifts/batch_api_enrichment_service`, `15gifts/datawarehouse`
- Domain custodians notified:
  - Global Guardrails: Jack Barker-Davy, James Pearce, Max Yousif (#ask-global-guardrails)
  - Tracking: Ed Mitchell, Carlos Olivet, Tom Levy (#ask-tracking)
