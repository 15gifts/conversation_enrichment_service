"""Failures page — batch-level failures and orphaned mapping rows.

Per-row failures live on the Quality page (parse_error, guardrail).
This page is for whole-batch failures: submission errors, Azure-side
rejection, network/auth issues.
"""
from __future__ import annotations

import streamlit as st

import utils

st.set_page_config(page_title="Failures", layout="wide")
st.title("Batch-level failures")
st.caption("Refresh cadence: 5 minutes.")

# ---------------------------------------------------------------------------
# Category breakdown
# ---------------------------------------------------------------------------
st.subheader("Failure categories (last 30 days)")

categories = utils.failure_categories(days=30)
if categories.empty:
    st.success("No failed batches in the last 30 days.")
else:
    col_chart, col_table = st.columns([2, 1])
    col_chart.bar_chart(categories.set_index("FAILURE_CATEGORY")["BATCHES"])
    col_table.dataframe(categories, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# Recent failed batches
# ---------------------------------------------------------------------------
st.subheader("Recent failed batches")

failed = utils.failed_batches(days=30)
if failed.empty:
    st.info("No failed batches.")
else:
    st.dataframe(failed, hide_index=True, use_container_width=True)
    st.caption(
        "`error_preview` is the first 200 chars of the error_message column "
        "in batch_tracking — the full string is in Snowflake."
    )

# ---------------------------------------------------------------------------
# Orphan rows — mapping not closed out past SLA
# ---------------------------------------------------------------------------
st.subheader("Orphaned mapping rows")

orphans = utils.orphan_rows()
if orphans.empty:
    st.success("No orphaned mapping rows — every batch closed out cleanly.")
else:
    st.warning(
        "These rows are in batch_row_mapping with a non-terminal status, "
        "but their parent batch is past the 26h SLA window. They are "
        "candidates for the manual reset path in backlog_management_runbook.md."
    )
    st.dataframe(orphans, hide_index=True, use_container_width=True)
