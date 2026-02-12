# RetailNexus: Smart Retail Supply Chain & Customer Intelligence Platform

## 🚀 Project Overview
An end-to-end Data Lakehouse solution addressing data fragmentation in retail.
**Key Features:**
- **Ingestion:** Real-time & Batch (Handling Schema Evolution)
- **Transformation:** SCD Type 2 (History Tracking) & Star Schema
- **Analytics:** CLV, Market Basket Analysis, Inventory Turnover

## 🛠️ Tech Stack
- **Language:** Python 3.10+
- **Database:** DuckDB (OLAP Engine)
- **Storage:** Parquet (Columnar Storage)
- **Dashboard:** Streamlit
- **Orchestration:** Python Scripts

## 👥 Team Roles (Vertical Split)
1. **Ingestion (Person A):** `src/ingestion/` - Data Generators & Watchers.
2. **Transformation (Person B):** `src/transformation/` - Cleaning, SCD Logic, Star Schema.
3. **Analytics/UI (Person C):** `src/analytics/` & `src/dashboard/` - KPIs & Visuals.

## ⚡ Quick Start
1. `pip install -r requirements.txt`
2. Run Generator: `python src/ingestion/main.py`
3. Run Dashboard: `streamlit run src/dashboard/app.py`
