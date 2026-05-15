"""Throughput page — queue depth, submission rate, completion rate.

Answers: is the pipeline keeping up with ingestion? If the queue is growing
faster than the submission rate, this page surfaces it before it becomes
a backlog incident.
"""
from __future__ import annotations

import streamlit as st

import utils

st.set_page_config(page_title="Throughput", layout="wide")
st.title("Throughput")
st.caption("Refresh cadence: 1 hour for daily aggregates · 5 min for queue.")

# ---------------------------------------------------------------------------
# Queue depth by age
# ---------------------------------------------------------------------------
st.subheader("Queue depth by conversation age")

queue_df = utils.queue_age_buckets()
if queue_df.empty:
    st.info("Queue is empty.")
else:
    st.bar_chart(queue_df.set_index("AGE_BUCKET")["ROWS_IN_QUEUE"])
    st.caption(
        "Rows older than 24h indicate the pipeline is not keeping up. "
        "See backlog_management_runbook.md for triage."
    )

# ---------------------------------------------------------------------------
# Daily submissions vs completions
# ---------------------------------------------------------------------------
st.subheader("Daily submissions vs completions (last 30 days)")

submissions = utils.daily_submissions(days=30)
completions = utils.daily_completions(days=30)

if submissions.empty and completions.empty:
    st.info("No batch activity in the last 30 days.")
else:
    # Inner-join on day so the two series share an x-axis.
    submissions = submissions.rename(columns={"SUBMIT_DAY": "day"})
    completions = completions[["DAY", "ROWS_COMPLETED"]].rename(
        columns={"DAY": "day"}
    )
    submissions = submissions[["day", "ROWS_SUBMITTED"]]
    merged = submissions.merge(completions, on="day", how="outer").sort_values("day")
    merged = merged.fillna(0).set_index("day")
    st.line_chart(merged.rename(
        columns={"ROWS_SUBMITTED": "Rows submitted", "ROWS_COMPLETED": "Rows completed"}
    ))
    st.caption(
        "Sustained gap (submissions > completions) means batches are not "
        "completing fast enough — check Azure-side batch status."
    )

# ---------------------------------------------------------------------------
# Active batches (concurrency snapshot)
# ---------------------------------------------------------------------------
st.subheader("In-flight batches (now)")

active = utils.active_batches()
if active.empty:
    st.info("No batches in flight.")
else:
    st.dataframe(active, hide_index=True, use_container_width=True)
    st.caption(
        "If `BATCHES` consistently hits the `MAX_ACTIVE_BATCHES` ceiling "
        "(set in 07_tasks.sql), raise the ceiling or increase `CHUNK_SIZE` "
        "to push more rows per batch."
    )
