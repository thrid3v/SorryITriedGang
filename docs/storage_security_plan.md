# RetailNexus — Storage, Access & Security Plan

## 1  Optimized Storage

| Property | Choice | Rationale |
|---|---|---|
| **File Format** | Apache Parquet (`.parquet`) | Columnar layout → fast aggregation; Snappy compression → 60-80 % disk savings vs CSV |
| **Engine** | DuckDB (in-process OLAP) | Zero-infrastructure; vectorized query execution on Parquet without ETL into a server |
| **Compression** | Snappy (default) | Best balance of speed vs size for analytics workloads |

### Why Parquet?
- Columnar storage means queries like `SUM(amount)` only read the `amount` column, not the entire row.
- Predicate push-down lets DuckDB skip irrelevant row-groups at the file level.
- Schema is embedded in the file footer — no external catalog needed.

---

## 2  Partitioning Strategy

We use **Hive-style partitioning** (`column=value/` directory layout).

```
data/gold/
├── fact_transactions/
│   └── region=<region>/order_date=<YYYY-MM-DD>/part-*.parquet
├── dim_customers/
│   └── region=<region>/part-*.parquet
└── dim_products/
    └── category=<category>/part-*.parquet
```

### Partition Columns

| Table | Partition Key(s) | Why |
|---|---|---|
| `fact_transactions` | `region`, `order_date` | Most analyst queries filter by geography and date range |
| `dim_customers` | `region` | Region-scoped joins are common |
| `dim_products` | `category` | Category-level filtering for market basket analysis |

### Benefits
- **Partition pruning:** DuckDB reads `hive_partitioning=true` and automatically skips directories that don't match the `WHERE` clause.
- **Cost reduction:** Queries like `WHERE region='East' AND order_date >= '2025-01-01'` read only the matching subdirectories instead of full table scans.

---

## 3  Secure Access Model

### 3.1  Role-Based Column Masking

Access is enforced at the **application layer** via `secure_read()` in `src/analytics/storage_utils.py`.

| Role | Allowed Tables | Masked Columns |
|---|---|---|
| **analyst** | `fact_transactions`, `dim_customers`, `dim_products` | `email`, `phone` (PII) |
| **manager** | all | *none* (full access) |
| **viewer** | `fact_transactions` only | `email`, `phone`, `amount` |

Masked columns are replaced with `***MASKED***` at read time — raw files are never exposed.

### 3.2  Filesystem-Level Controls (Recommended for Production)

| Layer | Mechanism |
|---|---|
| **OS Permissions** | `data/gold/` directory is read-only for the dashboard service account |
| **Git** | `.gitignore` excludes `data/` — raw PII never enters version control |
| **Environment** | Dashboard reads `RETAILNEXUS_ROLE` env var to select the access policy |

### 3.3  Dashboard Access

The Streamlit dashboard (`src/dashboard/app.py`) operates in **READ-ONLY** mode:
- Only reads from `data/gold/` via `st.cache_data`.
- Never writes back to any data directory.
- Gracefully shows "Waiting for Pipeline…" when files are missing.

---

## 4  Implementation Reference

| File | Purpose |
|---|---|
| [`storage_utils.py`](file:///c:/Users/Dibyendu/Devspace/SorryITriedGang/src/analytics/storage_utils.py) | Parquet read/write, partitioning, `secure_read()` |
| [`kpi_queries.py`](file:///c:/Users/Dibyendu/Devspace/SorryITriedGang/src/analytics/kpi_queries.py) | CLV, Market Basket, Summary KPIs via `duckdb.sql()` |
| [`app.py`](file:///c:/Users/Dibyendu/Devspace/SorryITriedGang/src/dashboard/app.py) | Streamlit dashboard (3 pages, Plotly visuals) |
