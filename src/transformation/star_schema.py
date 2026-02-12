"""
RetailNexus — Silver → Gold Star Schema Builder
Builds dimension tables (dim_products, dim_stores, dim_dates) and fact_transactions
from Silver-layer Parquet files.  dim_users is handled by scd_logic.py.
All transformations via duckdb.sql().
"""
import os

import duckdb

_BASE = os.path.join(os.path.dirname(__file__), "..", "..")
SILVER_DIR = os.path.join(_BASE, "data", "silver")
GOLD_DIR = os.path.join(_BASE, "data", "gold")

SILVER_TXN = os.path.join(SILVER_DIR, "transactions.parquet").replace("\\", "/")
SILVER_PRODUCTS = os.path.join(SILVER_DIR, "products.parquet").replace("\\", "/")
GOLD_DIM_PRODUCTS = os.path.join(GOLD_DIR, "dim_products.parquet").replace("\\", "/")
GOLD_DIM_STORES = os.path.join(GOLD_DIR, "dim_stores.parquet").replace("\\", "/")
GOLD_DIM_DATES = os.path.join(GOLD_DIR, "dim_dates.parquet").replace("\\", "/")
GOLD_FACT_TXN = os.path.join(GOLD_DIR, "fact_transactions.parquet").replace("\\", "/")
GOLD_DIM_USERS = os.path.join(GOLD_DIR, "dim_users.parquet").replace("\\", "/")


def _ensure_gold():
    os.makedirs(GOLD_DIR, exist_ok=True)


def build_dim_products():
    """Straight copy from Silver with a surrogate key."""
    duckdb.sql(f"""
        COPY (
            SELECT
                ROW_NUMBER() OVER (ORDER BY product_id)::INTEGER AS product_key,
                product_id,
                product_name,
                category,
                price
            FROM '{SILVER_PRODUCTS}'
        ) TO '{GOLD_DIM_PRODUCTS}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"SELECT COUNT(*) FROM '{GOLD_DIM_PRODUCTS}'").fetchone()[0]
    print(f"[StarSchema] dim_products: {cnt} rows")


def build_dim_stores():
    """Extract distinct stores from transactions."""
    duckdb.sql(f"""
        COPY (
            SELECT
                ROW_NUMBER() OVER (ORDER BY store_id)::INTEGER AS store_key,
                store_id,
                'Region_' || RIGHT(store_id, 3)  AS region
            FROM (
                SELECT DISTINCT store_id FROM '{SILVER_TXN}'
            ) s
        ) TO '{GOLD_DIM_STORES}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"SELECT COUNT(*) FROM '{GOLD_DIM_STORES}'").fetchone()[0]
    print(f"[StarSchema] dim_stores: {cnt} rows")


def build_dim_dates():
    """Generate a date dimension from the range of transaction timestamps."""
    duckdb.sql(f"""
        COPY (
            SELECT
                CAST(STRFTIME(d.date_val, '%Y%m%d') AS INTEGER) AS date_key,
                d.date_val::DATE                                AS full_date,
                EXTRACT(YEAR FROM d.date_val)::INTEGER          AS year,
                EXTRACT(QUARTER FROM d.date_val)::INTEGER       AS quarter,
                EXTRACT(MONTH FROM d.date_val)::INTEGER         AS month,
                DAYNAME(d.date_val)                             AS day_of_week,
                CASE WHEN DAYOFWEEK(d.date_val) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
            FROM (
                SELECT UNNEST(
                    GENERATE_SERIES(
                        (SELECT MIN(timestamp)::DATE FROM '{SILVER_TXN}'),
                        (SELECT MAX(timestamp)::DATE FROM '{SILVER_TXN}'),
                        INTERVAL 1 DAY
                    )
                ) AS date_val
            ) d
        ) TO '{GOLD_DIM_DATES}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"SELECT COUNT(*) FROM '{GOLD_DIM_DATES}'").fetchone()[0]
    print(f"[StarSchema] dim_dates: {cnt} rows")


def build_fact_transactions():
    """
    Join Silver transactions with Gold dim tables to produce the fact table.
    Uses duckdb.sql for all joins as per project rules.
    """
    duckdb.sql(f"""
        COPY (
            SELECT
                t.transaction_id,
                COALESCE(du.surrogate_key, -1)                          AS user_key,
                COALESCE(dp.product_key, -1)                            AS product_key,
                COALESCE(ds.store_key, -1)                              AS store_key,
                CAST(STRFTIME(t.timestamp, '%Y%m%d') AS INTEGER)        AS date_key,
                t.timestamp,
                t.amount
            FROM '{SILVER_TXN}' t
            LEFT JOIN '{GOLD_DIM_USERS}' du
                ON t.user_id = du.user_id AND du.is_current = TRUE
            LEFT JOIN '{GOLD_DIM_PRODUCTS}' dp
                ON t.product_id = dp.product_id
            LEFT JOIN '{GOLD_DIM_STORES}' ds
                ON t.store_id = ds.store_id
        ) TO '{GOLD_FACT_TXN}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"SELECT COUNT(*) FROM '{GOLD_FACT_TXN}'").fetchone()[0]
    print(f"[StarSchema] fact_transactions: {cnt} rows")


def build_star_schema():
    """Build all Gold-layer tables (excluding dim_users, handled by SCD)."""
    _ensure_gold()
    try:
        build_dim_products()
        build_dim_stores()
        build_dim_dates()
        build_fact_transactions()
        print("[StarSchema] Silver → Gold complete ✓")
    except FileNotFoundError as e:
        print(f"[StarSchema] Waiting for Silver data... ({e})")


if __name__ == "__main__":
    build_star_schema()
