"""
RetailNexus — KPI Queries
==========================
All heavy lifts use duckdb.sql() — no Python for-loops on data.
Placeholders (🔌) mark spots that depend on upstream pipeline output.
"""

import duckdb
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR     = PROJECT_ROOT / "data" / "gold"

# ── helper ──────────────────────────────────────
def _gold_path(table: str) -> str:
    return str(GOLD_DIR / table / "**/*.parquet")


# ─────────────────────────────────────────────────
# 1.  CUSTOMER LIFETIME VALUE  (CLV)
# ─────────────────────────────────────────────────
def compute_clv() -> pd.DataFrame:
    """
    CLV = total_spend per customer, plus average order value and
    purchase frequency.

    🔌 PLACEHOLDER: reads from gold/fact_transactions & gold/dim_users.
       These files will be created by the transformation pipeline (Person B).
    """
    txn_path = str(GOLD_DIR / "fact_transactions.parquet")
    users_path = str(GOLD_DIR / "dim_users.parquet")

    query = f"""
    WITH customer_txns AS (
        SELECT
            t.user_key,
            COUNT(*)                              AS purchase_count,
            SUM(t.amount)                         AS total_spend,
            AVG(t.amount)                         AS avg_order_value,
            MIN(t.timestamp)                      AS first_purchase,
            MAX(t.timestamp)                      AS last_purchase,
            DATEDIFF('day',
                     MIN(t.timestamp),
                     MAX(t.timestamp))             AS customer_lifespan_days
        FROM '{txn_path}' t
        WHERE t.user_key != -1
        GROUP BY t.user_key
    )
    SELECT
        u.user_id,
        u.name                                    AS customer_name,
        u.city                                    AS customer_city,
        ct.purchase_count,
        ROUND(ct.total_spend, 2)                  AS total_spend,
        ROUND(ct.avg_order_value, 2)              AS avg_order_value,
        ct.customer_lifespan_days,
        -- Simple CLV = avg_order_value × purchase_frequency × lifespan
        ROUND(
            ct.avg_order_value
            * (ct.purchase_count /
               GREATEST(ct.customer_lifespan_days / 30.0, 1))
            * GREATEST(ct.customer_lifespan_days / 365.0, 1),
            2
        )                                         AS estimated_clv
    FROM customer_txns ct
    LEFT JOIN '{users_path}' u
        ON ct.user_key = u.surrogate_key
           AND u.is_current = true
    ORDER BY estimated_clv DESC
    """
    try:
        return duckdb.sql(query).df()
    except Exception as e:
        print(f"[WARN] Gold data not yet available for CLV: {e}")
        return pd.DataFrame(columns=[
            "user_id", "customer_name", "customer_city",
            "purchase_count", "total_spend", "avg_order_value",
            "customer_lifespan_days", "estimated_clv",
        ])


# ─────────────────────────────────────────────────
# 2.  MARKET BASKET ANALYSIS  (What sells together?)
# ─────────────────────────────────────────────────
def compute_market_basket(min_support: int = 3) -> pd.DataFrame:
    """
    Pairs of products frequently bought in the same transaction.

    🔌 PLACEHOLDER: reads from gold/fact_transactions & gold/dim_products.
       These files will be created by the transformation pipeline (Person B).
    """
    txn_path  = str(GOLD_DIR / "fact_transactions.parquet")
    prod_path = str(GOLD_DIR / "dim_products.parquet")

    query = f"""
    WITH basket AS (
        SELECT
            t1.transaction_id,
            t1.product_key AS product_a,
            t2.product_key AS product_b
        FROM '{txn_path}' t1
        JOIN '{txn_path}' t2
            ON  t1.transaction_id = t2.transaction_id
            AND t1.product_key < t2.product_key          -- avoid self & duplicates
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
    LEFT JOIN '{prod_path}' pa
        ON pc.product_a = pa.product_key
    LEFT JOIN '{prod_path}' pb
        ON pc.product_b = pb.product_key
    ORDER BY pc.times_bought_together DESC
    """
    try:
        return duckdb.sql(query).df()
    except Exception as e:
        print(f"[WARN] Gold data not yet available for Market Basket: {e}")
        return pd.DataFrame(columns=[
            "product_a_name", "product_b_name",
            "times_bought_together", "product_a", "product_b",
        ])


# ─────────────────────────────────────────────────
# 3.  SUMMARY KPIs  (Revenue, Users, Turnover)
# ─────────────────────────────────────────────────
def compute_summary_kpis() -> dict:
    """
    Quick headline numbers for the dashboard top bar.

    🔌 PLACEHOLDER: depends on gold/fact_transactions & gold/dim_products.
    """
    txn_path = str(GOLD_DIR / "fact_transactions.parquet")

    try:
        row = duckdb.sql(f"""
            SELECT
                ROUND(SUM(amount), 2)           AS total_revenue,
                COUNT(DISTINCT user_key)        AS active_users,
                COUNT(DISTINCT transaction_id)  AS total_orders
            FROM '{txn_path}'
            WHERE user_key != -1
        """).df().iloc[0]
        return {
            "total_revenue": row["total_revenue"],
            "active_users":  int(row["active_users"]),
            "total_orders":  int(row["total_orders"]),
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
