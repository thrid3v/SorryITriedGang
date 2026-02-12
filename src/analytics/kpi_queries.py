"""
RetailNexus — KPI Queries
==========================
All heavy lifts use duckdb.sql() — no Python for-loops on data.
Placeholders (🔌) mark spots that depend on upstream pipeline output.
"""
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.utils.retry_utils import retry_with_backoff

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR     = PROJECT_ROOT / "data" / "gold"

# Paths for partitioned and non-partitioned tables
FACT_TXN = str(GOLD_DIR / "fact_transactions.parquet" / "**" / "*.parquet").replace("\\", "/")
DIM_USERS = str(GOLD_DIR / "dim_users.parquet").replace("\\", "/")
DIM_PRODUCTS = str(GOLD_DIR / "dim_products.parquet").replace("\\", "/")

# ── helper ──────────────────────────────────────
def _gold_path(table: str) -> str:
    return str(GOLD_DIR / table / "**/*.parquet")


# ─────────────────────────────────────────────────
# 1.  CUSTOMER LIFETIME VALUE  (CLV)
# ─────────────────────────────────────────────────
@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, FileNotFoundError, OSError))
def compute_clv() -> pd.DataFrame:
    """
    CLV = total_spend per customer, plus average order value and
    purchase frequency.

    🔌 PLACEHOLDER: reads from gold/fact_transactions & gold/dim_users.
       These files will be created by the transformation pipeline (Person B).
    """
    txn_path = str(GOLD_DIR / "fact_transactions.parquet")
    users_path = str(GOLD_DIR / "dim_users.parquet")
    try:
        df = duckdb.sql(f"""
            WITH user_purchases AS (
                SELECT
                    ft.user_key,
                    COUNT(DISTINCT ft.transaction_id) AS purchase_count,
                    SUM(ft.amount)                     AS total_spend,
                    MIN(ft.timestamp)::DATE            AS first_purchase,
                    MAX(ft.timestamp)::DATE            AS last_purchase
                FROM read_parquet('{FACT_TXN}', hive_partitioning=true) ft
                WHERE ft.user_key != -1
                GROUP BY ft.user_key
            ),
            clv_calc AS (
                SELECT
                    up.user_key,
                    up.purchase_count,
                    up.total_spend,
                    up.total_spend / NULLIF(up.purchase_count, 0) AS avg_order_value,
                    DATEDIFF('day', up.first_purchase, up.last_purchase) AS customer_lifespan_days,
                    -- Simple CLV = total spend (could be enhanced with predictive models)
                    up.total_spend AS estimated_clv
                FROM user_purchases up
            )
            SELECT
                du.user_id,
                du.name           AS customer_name,
                du.city           AS customer_city,
                clv.purchase_count,
                clv.total_spend,
                clv.avg_order_value,
                clv.customer_lifespan_days,
                clv.estimated_clv
            FROM clv_calc clv
            LEFT JOIN '{DIM_USERS}' du
                ON clv.user_key = du.surrogate_key
            WHERE du.is_current = TRUE
            ORDER BY clv.estimated_clv DESC
        """).df()
        return df
    except FileNotFoundError:
        print("[KPI] fact_transactions or dim_users not found. Returning empty frame.")
        return pd.DataFrame(columns=[
            "user_id", "customer_name", "customer_city",
            "purchase_count", "total_spend", "avg_order_value",
            "customer_lifespan_days", "estimated_clv",
        ])


# ─────────────────────────────────────────────────
# 2.  MARKET BASKET ANALYSIS  (What sells together?)
# ─────────────────────────────────────────────────
@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, FileNotFoundError, OSError))
def compute_market_basket(min_support: int = 3) -> pd.DataFrame:
    """
    Pairs of products frequently bought in the same transaction.

    🔌 PLACEHOLDER: reads from gold/fact_transactions & gold/dim_products.
       These files will be created by the transformation pipeline (Person B).
    """
    try:
        df = duckdb.sql(f"""
            WITH basket AS (
                SELECT
                    t1.transaction_id,
                    t1.product_key AS product_a,
                    t2.product_key AS product_b
                FROM read_parquet('{FACT_TXN}', hive_partitioning=true) t1
                JOIN read_parquet('{FACT_TXN}', hive_partitioning=true) t2
                    ON  t1.transaction_id = t2.transaction_id
                    AND t1.product_key < t2.product_key
                WHERE t1.product_key != -1 AND t2.product_key != -1
            ),
            pair_counts AS (
                SELECT
                    product_a,
                    product_b,
                    COUNT(*) AS times_bought_together
                FROM basket
                GROUP BY product_a, product_b
                HAVING COUNT(*) >= {min_support}
            )
            SELECT
                pa.product_name  AS product_a_name,
                pb.product_name  AS product_b_name,
                pc.times_bought_together,
                pc.product_a,
                pc.product_b
            FROM pair_counts pc
            LEFT JOIN '{DIM_PRODUCTS}' pa
                ON pc.product_a = pa.product_key
            LEFT JOIN '{DIM_PRODUCTS}' pb
                ON pc.product_b = pb.product_key
            ORDER BY pc.times_bought_together DESC
        """).df()
        return df
    except FileNotFoundError:
        print("[KPI] fact_transactions or dim_products not found. Returning empty frame.")
        return pd.DataFrame(columns=[
            "product_a_name", "product_b_name",
            "times_bought_together", "product_a", "product_b",
        ])


# ─────────────────────────────────────────────────
# 3.  SUMMARY KPIs  (Revenue, Users, Turnover)
# ─────────────────────────────────────────────────
@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, FileNotFoundError, OSError))
def compute_summary_kpis() -> dict:
    """
    Quick headline numbers for the dashboard top bar.

    🔌 PLACEHOLDER: depends on gold/fact_transactions & gold/dim_products.
    """
    try:
        result = duckdb.sql(f"""
            SELECT
                SUM(amount)::DOUBLE                   AS total_revenue,
                COUNT(DISTINCT user_key)::INTEGER     AS active_users,
                COUNT(DISTINCT transaction_id)::INTEGER AS total_orders
            FROM read_parquet('{FACT_TXN}', hive_partitioning=true)
            WHERE user_key != -1
        """).fetchone()

        return {
            "total_revenue": result[0] or 0.0,
            "active_users": result[1] or 0,
            "total_orders": result[2] or 0,
        }
    except Exception:
        return {"total_revenue": 0.0, "active_users": 0, "total_orders": 0}


# ─────────────────────────────────────────────────
# CLI quick-test
# ─────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== Summary KPIs ===")
    print(compute_summary_kpis())

    print("\n=== CLV (top 5) ===")
    print(compute_clv().head())

    print("\n=== Market Basket (top 5) ===")
    print(compute_market_basket(min_support=2).head())
