"""Latency page — submitted → completed time per batch.

Azure batch SLA is 24h (1440 min). p99 above ~1500 minutes means some
batches are timing out — investigate those `azure_batch_id`s directly.
"""
from __future__ import annotations

import plotly.express as px
import streamlit as st

import utils

st.set_page_config(page_title="Latency", layout="wide")
st.title("End-to-end latency")
st.caption("Refresh cadence: 5 minutes.")

# ---------------------------------------------------------------------------
# Stuck batches — incident-only panel, surface FIRST if non-empty
# ---------------------------------------------------------------------------
stuck = utils.stuck_batches()
if not stuck.empty:
    st.error(
        f"{len(stuck)} batch(es) have been in flight for more than "
        f"{utils.ALERT_THRESHOLDS['STUCK_BATCH_HOURS']} hours — past Azure's "
        "24h SLA. Investigate immediately."
    )
    st.dataframe(stuck, hide_index=True, use_container_width=True)
    st.divider()

# ---------------------------------------------------------------------------
# Percentile cards (last 14 days)
# ---------------------------------------------------------------------------
st.subheader("Latency distribution (last 14 days)")

pct = utils.latency_percentiles(days=14)
if pct.empty or pct.iloc[0]["BATCHES"] == 0:
    st.info("No completed batches in the last 14 days.")
else:
    row = pct.iloc[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("p50", f"{int(row['P50_MINUTES'])} min")
    c2.metric("p90", f"{int(row['P90_MINUTES'])} min")
    c3.metric(
        "p99",
        f"{int(row['P99_MINUTES'])} min",
        delta="over SLA" if row["P99_MINUTES"] > 1500 else "within SLA",
        delta_color="inverse" if row["P99_MINUTES"] > 1500 else "normal",
    )
    c4.metric("max", f"{int(row['MAX_MINUTES'])} min")
    c5.metric("batches", f"{int(row['BATCHES']):,}")

# ---------------------------------------------------------------------------
# Scatter — per-batch latency over time
# ---------------------------------------------------------------------------
st.subheader("Per-batch latency over time")

scatter_df = utils.batch_latencies(days=14)
if scatter_df.empty:
    st.info("No completed batches to plot.")
else:
    fig = px.scatter(
        scatter_df,
        x="SUBMITTED_AT",
        y="MINUTES_E2E",
        size="ROW_COUNT",
        hover_data=["BATCH_TRACKING_ID", "AZURE_BATCH_ID"],
        labels={
            "SUBMITTED_AT": "Submitted",
            "MINUTES_E2E": "End-to-end minutes",
            "ROW_COUNT": "Rows in batch",
        },
    )
    fig.add_hline(
        y=1440, line_dash="dash", line_color="red",
        annotation_text="24h SLA",
    )
    fig.update_layout(height=400, margin=dict(t=10, b=10, l=10, r=10))
    st.plotly_chart(fig, use_container_width=True)
