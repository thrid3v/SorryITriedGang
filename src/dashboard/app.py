import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="RetailNexus Dashboard", layout="wide")

st.title("📊 RetailNexus: Supply Chain Intelligence")

# KPI Section
col1, col2, col3 = st.columns(3)
with col1:
    st.metric(label="Total Revenue", value="$0")
with col2:
    st.metric(label="Inventory Turnover", value="0.0")
with col3:
    st.metric(label="Active Users", value="0")

# TODO: Connect to Parquet files in 'data/gold'
