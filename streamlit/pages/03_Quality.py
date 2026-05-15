"""Quality page — per-row outcomes, parse-error samples, failing-field tally.

Use this page to drive prompt iteration. If `engagement_trajectory` keeps
appearing in the failing-field tally, the prompt's allowed-values list
needs review.
"""
from __future__ import annotations

import plotly.express as px
import streamlit as st

import utils

st.set_page_config(page_title="Quality", layout="wide")
st.title("Per-row quality")
st.caption("Refresh cadence: 5 minutes for outcomes · 1 hour for tallies.")

# ---------------------------------------------------------------------------
# Outcome breakdown
# ---------------------------------------------------------------------------
st.subheader("Outcome distribution (last 7 days)")

outcomes = utils.outcome_breakdown(days=7)
if outcomes.empty:
    st.info("No enrichment results in the last 7 days.")
else:
    col_chart, col_table = st.columns([2, 1])
    fig = px.bar(
        outcomes,
        x="OUTCOME",
        y="TOTAL_ROWS",
        text="PCT",
        color="OUTCOME",
        color_discrete_map={
            "success": "#22c55e",
            "parse_error": "#f97316",
            "guardrail_block": "#a855f7",
            "azure_error": "#ef4444",
            "other": "#94a3b8",
        },
    )
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(showlegend=False, height=350,
                      margin=dict(t=10, b=10, l=10, r=10))
    col_chart.plotly_chart(fig, use_container_width=True)
    col_table.dataframe(outcomes, hide_index=True, use_container_width=True)

    parse_pct = outcomes.loc[
        outcomes["OUTCOME"] == "parse_error", "PCT"
    ].sum()
    crit = utils.ALERT_THRESHOLDS["PARSE_ERROR_PCT_CRIT"]
    warn = utils.ALERT_THRESHOLDS["PARSE_ERROR_PCT_WARN"]
    if parse_pct >= crit:
        st.error(f"Parse error rate is {parse_pct}% — critical (>= {crit}%).")
    elif parse_pct >= warn:
        st.warning(f"Parse error rate is {parse_pct}% — above {warn}% target.")

# ---------------------------------------------------------------------------
# Failing fields
# ---------------------------------------------------------------------------
st.subheader("Most-failing fields (last 30 days)")

fields = utils.failing_fields(days=30)
if fields.empty:
    st.info("No parsable field-level failures in the last 30 days.")
else:
    st.bar_chart(fields.set_index("FAILING_FIELD")["FAILURES"])
    st.dataframe(fields, hide_index=True, use_container_width=True)
    st.caption(
        "Fields that recur here need attention in `enrichment_field_config` "
        "(allowed_values) or the prompt template."
    )

# ---------------------------------------------------------------------------
# Recent parse errors
# ---------------------------------------------------------------------------
st.subheader("Recent parse errors (last 7 days, up to 50)")

errors = utils.recent_parse_errors(days=7, limit=50)
if errors.empty:
    st.success("No parse errors in the last 7 days.")
else:
    st.dataframe(errors, hide_index=True, use_container_width=True)
