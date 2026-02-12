"""
RetailNexus — Streamlit Dashboard  (Person C)
===============================================
READ-ONLY access to data/gold/.
Gracefully handles missing files with "Waiting for Pipeline…" states.
All data loaded via st.cache_data; visuals via Plotly Express.
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path so analytics imports work
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import plotly.express as px
import pandas as pd

from src.analytics.kpi_queries import (
    compute_clv,
    compute_market_basket,
    compute_summary_kpis,
)

# ── Page Config ──────────────────────────────────
st.set_page_config(
    page_title="RetailNexus Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────
st.markdown("""
<style>
    .block-container {padding-top: 1rem;}
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5986 100%);
        border-radius: 10px; padding: 15px 20px; color: white;
    }
    div[data-testid="stMetric"] label {color: #a0c4e8 !important;}
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {color: white !important;}
</style>
""", unsafe_allow_html=True)


# ── Cached loaders ───────────────────────────────
def load_kpis() -> dict:
    return compute_summary_kpis()


def load_clv() -> pd.DataFrame:
    return compute_clv()


def load_basket() -> pd.DataFrame:
    return compute_market_basket(min_support=2)


# ── Sidebar ──────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/shopping-cart.png", width=64)
    st.title("RetailNexus")
    st.caption("Smart Retail Supply Chain & Customer Intelligence")
    st.divider()
    page = st.radio(
        "Navigate",
        ["🏠 Overview", "💎 Customer Lifetime Value", "🛒 Market Basket"],
        label_visibility="collapsed",
    )
    st.divider()
    st.info("Data refreshes every 60 s via `st.cache_data`.")


# ══════════════════════════════════════════════════
# PAGE: Overview
# ══════════════════════════════════════════════════
if page == "🏠 Overview":
    st.title("📊 RetailNexus: Supply Chain Intelligence")
    st.markdown("---")

    kpis = load_kpis()

    if kpis["total_revenue"] == 0:
        st.warning(
            "⏳ **Waiting for Pipeline…** "
            "No gold-layer data found yet. Run the ingestion → transformation "
            "pipeline to populate `data/gold/`."
        )

    # ── KPI Cards ────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Revenue", f"${kpis['total_revenue']:,.2f}")
    with col2:
        st.metric("Active Users", f"{kpis['active_users']:,}")
    with col3:
        st.metric("Total Orders", f"{kpis['total_orders']:,}")

    st.markdown("---")

    # ── Quick CLV preview ────────────────────────
    st.subheader("🔝 Top Customers by Lifetime Value")
    clv_df = load_clv()
    if clv_df.empty:
        st.info("🔌 **Placeholder** — CLV data will appear once "
                "`data/gold/fact_transactions` and `data/gold/dim_customers` exist.")
    else:
        top = clv_df.head(10)
        fig = px.bar(
            top,
            x="customer_name",
            y="estimated_clv",
            color="estimated_clv",
            color_continuous_scale="Tealgrn",
            labels={"estimated_clv": "Est. CLV ($)", "customer_name": "Customer"},
            title="Top 10 Customers — Estimated CLV",
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════
# PAGE: Customer Lifetime Value
# ══════════════════════════════════════════════════
elif page == "💎 Customer Lifetime Value":
    st.title("💎 Customer Lifetime Value (CLV)")
    st.markdown("---")

    clv_df = load_clv()

    if clv_df.empty:
        st.warning(
            "⏳ **Waiting for Pipeline…** "
            "Gold-layer Parquet files not found. "
            "Ensure `data/gold/fact_transactions/` and "
            "`data/gold/dim_customers/` contain data."
        )
        st.markdown(
            "**🔌 Placeholder columns expected:**\n"
            "`user_id`, `customer_name`, `customer_city`, "
            "`purchase_count`, `total_spend`, `avg_order_value`, "
            "`customer_lifespan_days`, `estimated_clv`"
        )
    else:
        # KPI row
        c1, c2, c3 = st.columns(3)
        c1.metric("Avg CLV", f"${clv_df['estimated_clv'].mean():,.2f}")
        c2.metric("Max CLV", f"${clv_df['estimated_clv'].max():,.2f}")
        c3.metric("Customers Analysed", f"{len(clv_df):,}")

        st.markdown("---")

        # Bar chart
        fig_bar = px.bar(
            clv_df.head(20),
            x="customer_name", y="estimated_clv",
            color="customer_city",
            title="Top 20 Customers by CLV",
            labels={"estimated_clv": "CLV ($)"},
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        # Scatter: spend vs frequency
        fig_scatter = px.scatter(
            clv_df,
            x="purchase_count", y="total_spend",
            size="estimated_clv", color="customer_city",
            hover_name="customer_name",
            title="Spend vs Purchase Frequency",
            labels={
                "purchase_count": "# Purchases",
                "total_spend": "Total Spend ($)",
            },
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        # Raw data
        with st.expander("📋 Raw CLV Table"):
            st.dataframe(clv_df, use_container_width=True)


# ══════════════════════════════════════════════════
# PAGE: Market Basket
# ══════════════════════════════════════════════════
elif page == "🛒 Market Basket":
    st.title("🛒 Market Basket Analysis")
    st.markdown("*What products are frequently bought together?*")
    st.markdown("---")

    basket_df = load_basket()

    if basket_df.empty:
        st.info(
            "📦 **No Product Pairs Found** \n\n"
            "Market basket analysis requires transactions with **multiple products**. "
            "The current data model has one product per transaction.\n\n"
            "**To enable market basket analysis:**\n"
            "- Modify the ingestion generator to create transactions with multiple products\n"
            "- Or aggregate by user/time window to find products bought in the same session"
        )
    else:
        # Heatmap-style bar
        fig = px.bar(
            basket_df.head(15),
            x="times_bought_together",
            y=basket_df.head(15).apply(
                lambda r: f"{r['product_a_name']}  ×  {r['product_b_name']}",
                axis=1,
            ),
            orientation="h",
            color="times_bought_together",
            color_continuous_scale="Sunset",
            title="Top 15 Product Pairs",
            labels={
                "times_bought_together": "Co-Purchases",
                "y": "Product Pair",
            },
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

        # Raw data
        with st.expander("📋 Raw Market Basket Table"):
            st.dataframe(basket_df, use_container_width=True)


# ── Footer ───────────────────────────────────────
st.markdown("---")
st.caption("RetailNexus v1.0 • Data Lakehouse Dashboard • DuckDB + Parquet + Streamlit")
