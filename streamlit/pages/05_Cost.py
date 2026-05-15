"""Cost page — Snowflake compute attributable to enrichment.

Azure OpenAI token spend is not in Snowflake. Until it's wired in via an
Azure metrics export, rows-completed-per-day is the best proxy
(near-linear at fixed prompt/transcript length).
"""
from __future__ import annotations

import streamlit as st

import utils

st.set_page_config(page_title="Cost", layout="wide")
st.title("Cost")
st.caption(
    "Refresh cadence: daily. "
    "Source: `snowflake.account_usage.warehouse_metering_history` "
    "(45 min – 3 h latency)."
)

# ---------------------------------------------------------------------------
# Snowflake compute
# ---------------------------------------------------------------------------
st.subheader("ENRICHMENT_WH credits used (last 30 days)")

credits_df = utils.warehouse_credits(days=30)
if credits_df.empty:
    st.info("No warehouse usage recorded in account_usage yet.")
else:
    col_chart, col_summary = st.columns([3, 1])
    col_chart.line_chart(credits_df.set_index("DAY")["CREDITS"])
    total_credits = float(credits_df["CREDITS"].sum())
    total_usd = float(credits_df["APPROX_USD_AT_3_PER_CREDIT"].sum())
    col_summary.metric("Total credits (30d)", f"{total_credits:,.2f}")
    col_summary.metric(
        "Approx USD (@ $3/credit)",
        f"${total_usd:,.2f}",
        help="Replace 3.0 with your contract rate in utils.warehouse_credits.",
    )
    st.dataframe(credits_df, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# Throughput proxy for Azure spend
# ---------------------------------------------------------------------------
st.subheader("Rows completed per day (Azure cost proxy)")

completions = utils.daily_completions(days=30)
if completions.empty:
    st.info("No completed batches in the last 30 days.")
else:
    st.line_chart(completions.set_index("DAY")["ROWS_COMPLETED"])
    st.caption(
        "Multiply by your average tokens-per-row × Azure $/1k tokens for "
        "a back-of-envelope Azure spend estimate. For real numbers, pull "
        "from the Azure portal."
    )

st.divider()
st.info(
    "**Future work:** wire Azure cost metrics into Snowflake (e.g. via the "
    "Azure Cost Management export to ADLS, then COPY INTO) so this page "
    "can show real $/day end-to-end."
)
