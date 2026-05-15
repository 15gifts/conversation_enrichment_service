"""Overview page — at-a-glance health of the batch enrichment pipeline.

This is the landing page operators see first. It surfaces the five things
that distinguish "everything is fine" from "something needs attention":
  1. Both Snowflake tasks have fired recently (heartbeat)
  2. Queue depth is bounded
  3. Active batch count is within the configured concurrency ceiling
  4. Per-row outcome distribution is dominated by `success`
  5. No batches are stuck past Azure's 24h SLA

Each tile cross-links to a detail page for deeper investigation.
"""
from __future__ import annotations

import plotly.express as px
import streamlit as st

import utils

st.set_page_config(
    page_title="Enrichment Pipeline — Overview",
    layout="wide",
)

st.title("Batch Enrichment Pipeline")
st.caption(
    "Operational dashboard for AE-1540. "
    "See the sidebar for detailed throughput, latency, quality, failure, "
    "cost, and audit panels."
)

# ---------------------------------------------------------------------------
# Row 1 — Heartbeat
# ---------------------------------------------------------------------------
st.subheader("Pipeline heartbeat")

liveness = utils.task_liveness()

col1, col2 = st.columns(2)

submit_threshold = utils.ALERT_THRESHOLDS["SUBMIT_BATCH_TASK_MAX_AGE_MIN"]
retrieve_threshold = utils.ALERT_THRESHOLDS["RETRIEVE_BATCH_TASK_MAX_AGE_MIN"]


def _heartbeat_metric(column, task_name: str, threshold_min: int) -> None:
    row = liveness[liveness["TASK_NAME"] == task_name]
    if row.empty:
        column.metric(task_name, "no runs in 48h", delta="P1 alert", delta_color="inverse")
        return
    minutes = int(row.iloc[0]["MINUTES_SINCE_LAST_RUN"])
    last_at = row.iloc[0]["LAST_RUN_AT"]
    status = "OK" if minutes <= threshold_min else f"stale (>{threshold_min} min)"
    column.metric(
        task_name,
        f"{minutes} min ago",
        delta=status,
        delta_color="normal" if minutes <= threshold_min else "inverse",
        help=f"Last SUCCEEDED run: {last_at}",
    )


_heartbeat_metric(col1, "SUBMIT_BATCH_TASK", submit_threshold)
_heartbeat_metric(col2, "RETRIEVE_BATCH_TASK", retrieve_threshold)

# ---------------------------------------------------------------------------
# Row 2 — Queue + in-flight
# ---------------------------------------------------------------------------
st.subheader("Queue and in-flight")

queue_df = utils.queue_age_buckets()
active_df = utils.active_batches()

col_q, col_a = st.columns(2)

queue_total = int(queue_df["ROWS_IN_QUEUE"].sum()) if not queue_df.empty else 0
overdue = (
    int(queue_df.loc[queue_df["AGE_BUCKET"] == "3d+", "ROWS_IN_QUEUE"].sum())
    if not queue_df.empty
    else 0
)
col_q.metric(
    "Queue depth",
    f"{queue_total:,}",
    delta=f"{overdue:,} > 3 days old" if overdue else "no overdue rows",
    delta_color="inverse" if overdue else "normal",
)

in_flight_rows = int(active_df["TOTAL_ROWS"].sum()) if not active_df.empty else 0
in_flight_batches = int(active_df["BATCHES"].sum()) if not active_df.empty else 0
col_a.metric(
    "In flight (rows)",
    f"{in_flight_rows:,}",
    delta=f"across {in_flight_batches} batches",
)

with st.expander("Queue age breakdown"):
    if queue_df.empty:
        st.info("Queue is empty.")
    else:
        st.bar_chart(queue_df.set_index("AGE_BUCKET")["ROWS_IN_QUEUE"])

# ---------------------------------------------------------------------------
# Row 3 — Outcome donut + stuck-batches alarm
# ---------------------------------------------------------------------------
st.subheader("Per-row outcomes (last 7 days)")

outcome_df = utils.outcome_breakdown(days=7)
stuck_df = utils.stuck_batches()

col_donut, col_stuck = st.columns([2, 1])

if outcome_df.empty:
    col_donut.info("No enrichment results in the last 7 days.")
else:
    fig = px.pie(
        outcome_df,
        names="OUTCOME",
        values="TOTAL_ROWS",
        hole=0.5,
        color="OUTCOME",
        color_discrete_map={
            "success": "#22c55e",
            "parse_error": "#f97316",
            "guardrail_block": "#a855f7",
            "azure_error": "#ef4444",
            "other": "#94a3b8",
        },
    )
    fig.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=300)
    col_donut.plotly_chart(fig, use_container_width=True)

if stuck_df.empty:
    col_stuck.success("No stuck batches.")
else:
    col_stuck.error(
        f"{len(stuck_df)} batch(es) stuck > "
        f"{utils.ALERT_THRESHOLDS['STUCK_BATCH_HOURS']}h"
    )
    col_stuck.dataframe(
        stuck_df[["AZURE_BATCH_ID", "STATUS", "HOURS_IN_FLIGHT", "ROW_COUNT"]],
        hide_index=True,
        use_container_width=True,
    )
    col_stuck.caption("See Latency page for full detail.")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.divider()
st.caption(
    f"Query tag: `{utils.QUERY_TAG}` · "
    "Cache TTLs: heartbeat 60s · operational 5m · throughput 1h · audit 24h"
)
