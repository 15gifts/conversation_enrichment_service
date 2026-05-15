"""Audit page — security drift and recent privileged activity.

This is the "Option A" monitoring from the security discussion:
llm_enrichment_role retains CREATE TASK / CREATE PROCEDURE via inherited
DATALAKE_RW_ROLE, so the mitigation is detection — alert on any unexpected
task or procedure in the schema.
"""
from __future__ import annotations

import streamlit as st

import utils

st.set_page_config(page_title="Audit", layout="wide")
st.title("Security audit")
st.caption(
    "Refresh cadence: daily. "
    "`account_usage` views have up to ~3h latency — not suitable for "
    "real-time alerting."
)

# ---------------------------------------------------------------------------
# Allowlist checks — unexpected tasks / procedures
# ---------------------------------------------------------------------------
st.subheader("Object allowlist")
col_t, col_p = st.columns(2)

with col_t:
    st.markdown("**Tasks in `datalake.llm_enrichments`**")
    unexpected_t = utils.unexpected_tasks()
    if unexpected_t.empty:
        st.success("Only the two expected tasks are present.")
    else:
        st.error(
            f"{len(unexpected_t)} unexpected task(s) detected — security event."
        )
        st.dataframe(unexpected_t, hide_index=True, use_container_width=True)

with col_p:
    st.markdown("**Stored procedures in `datalake.llm_enrichments`**")
    unexpected_p = utils.unexpected_procedures()
    if unexpected_p.empty:
        st.success("Only the two expected procedures are present.")
    else:
        st.error(
            f"{len(unexpected_p)} unexpected procedure(s) detected — "
            "security event."
        )
        st.dataframe(unexpected_p, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# Recent grants on EAI / secret
# ---------------------------------------------------------------------------
st.subheader("Grants on EAI / secret (last 30 days)")

grants = utils.recent_eai_grants(days=30)
if grants.empty:
    st.success("No recent grants on AZURE_OPENAI_EAI or AZURE_OPENAI_KEY.")
else:
    st.warning(
        f"{len(grants)} grant(s) recorded in the last 30 days. "
        "Confirm each is expected — these are the keys to the Azure egress."
    )
    st.dataframe(grants, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# Recent procedure / task executions
# ---------------------------------------------------------------------------
st.subheader("Recent enrichment procedure / task calls (last 7 days)")

calls = utils.recent_procedure_calls(days=7)
if calls.empty:
    st.info("No CALL or EXECUTE TASK activity recorded.")
else:
    st.dataframe(calls, hide_index=True, use_container_width=True)
    st.caption(
        "Filter by `role_name` to confirm smoke tests ran under the "
        "expected role (llm_enrichment_role or analytics_engineer)."
    )
