"""
RetailNexus — KPI Queries
==========================
All heavy lifts use duckdb — no Python for-loops on data.
Schema-agnostic: Uses dynamic table discovery instead of hardcoded paths.
"""
import os
import sys
from pathlib import Path
from typing import Dict

import duckdb
import pandas as pd

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.utils.retry_utils import retry_with_backoff
from src.analytics.schema_inspector import load_business_context, discover_tables

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ── Dynamic Table Path Resolution ──────────────────────
_table_cache = None

def _get_table_paths() -> Dict[str, str]:
    """
    Get table paths dynamically from Gold Layer.
    Caches result to avoid repeated file system scans.
    
    Returns:
        Dict mapping table names to their DuckDB read paths
    """
    global _table_cache
    if _table_cache is None:
        context = load_business_context()
        _table_cache = discover_tables(context['gold_layer_path'])
    return _table_cache

def _get_conn():
    """Create a fresh DuckDB connection for each query to avoid concurrency issues."""
    return duckdb.connect()


# ─────────────────────────────────────────────────
# 1.  CUSTOMER LIFETIME VALUE  (CLV)
# ─────────────────────────────────────────────────
@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, FileNotFoundError, OSError))
def compute_clv() -> pd.DataFrame:
    """
    CLV = total_spend per customer, plus average order value and
    purchase frequency.
    
    Schema-agnostic: Uses dynamic table discovery.
    """
    try:
        tables = _get_table_paths()
        fact_txn = tables.get('fact_transactions')
        dim_users = tables.get('dim_users')
        
        if not fact_txn or not dim_users:
            raise FileNotFoundError("Required tables not found in Gold Layer")
        
        conn = _get_conn()
        df = conn.sql(f"""
            WITH user_purchases AS (
                SELECT
                    ft.user_key,
                    COUNT(DISTINCT ft.transaction_id) AS purchase_count,
                    SUM(ft.amount)                     AS total_spend,
                    MIN(ft.timestamp)::DATE            AS first_purchase,
                    MAX(ft.timestamp)::DATE            AS last_purchase
                FROM {fact_txn} ft
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
            LEFT JOIN {dim_users} du
                ON clv.user_key = du.surrogate_key
            WHERE du.is_current = TRUE
            ORDER BY clv.estimated_clv DESC
        """).df()
        conn.close()
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
    Schema-agnostic: Uses dynamic table discovery.
    """
    try:
        tables = _get_table_paths()
        fact_txn = tables.get('fact_transactions')
        dim_products = tables.get('dim_products')
        
        if not fact_txn or not dim_products:
            raise FileNotFoundError("Required tables not found in Gold Layer")
        
        conn = _get_conn()
        df = conn.sql(f"""
            WITH basket AS (
                SELECT
                    t1.transaction_id,
                    t1.product_key AS product_a,
                    t2.product_key AS product_b
                FROM {fact_txn} t1
                JOIN {fact_txn} t2
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
            LEFT JOIN {dim_products} pa
                ON pc.product_a = pa.product_key
            LEFT JOIN {dim_products} pb
                ON pc.product_b = pb.product_key
            ORDER BY pc.times_bought_together DESC
        """).df()
        conn.close()
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
    Schema-agnostic: Uses dynamic table discovery.
    """
    try:
        tables = _get_table_paths()
        fact_txn = tables.get('fact_transactions')
        
        if not fact_txn:
            raise FileNotFoundError("fact_transactions not found in Gold Layer")
        
        conn = _get_conn()
        result = conn.sql(f"""
            SELECT
                SUM(amount)::DOUBLE                                              AS total_revenue,
                COUNT(DISTINCT user_key) FILTER (WHERE user_key != -1)::INTEGER  AS active_users,
                COUNT(DISTINCT transaction_id)::INTEGER                          AS total_orders
            FROM {fact_txn}
        """).fetchone()
        conn.close()

        return {
            "total_revenue": result[0] or 0.0,
            "active_users": result[1] or 0,
            "total_orders": result[2] or 0,
        }
    except Exception:
        return {"total_revenue": 0.0, "active_users": 0, "total_orders": 0}


# ─────────────────────────────────────────────────
# 4. DAILY/MONTHLY REVENUE TIME-SERIES
# ─────────────────────────────────────────────────
@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, FileNotFoundError, OSError))
def compute_revenue_timeseries(granularity: str = 'daily') -> pd.DataFrame:
    """
    Revenue breakdown by day or month.
    Schema-agnostic: Uses dynamic table discovery.
    
    Args:
        granularity: 'daily' or 'monthly'
    """
    try:
        tables = _get_table_paths()
        fact_txn = tables.get('fact_transactions')
        dim_dates = tables.get('dim_dates')
        
        if not fact_txn or not dim_dates:
            raise FileNotFoundError("Required tables not found in Gold Layer")
        
        conn = _get_conn()
        
        if granularity == 'monthly':
            df = conn.sql(f"""
                SELECT
                    dd.year,
                    dd.month,
                    SUM(ft.amount) as revenue,
                    COUNT(DISTINCT ft.transaction_id) as order_count
                FROM {fact_txn} ft
                JOIN {dim_dates} dd ON ft.date_key = dd.date_key
                GROUP BY dd.year, dd.month
                ORDER BY dd.year, dd.month
            """).df()
        else:  # daily
            df = conn.sql(f"""
                SELECT
                    dd.full_date,
                    dd.day_of_week,
                    SUM(ft.amount) as revenue,
                    COUNT(DISTINCT ft.transaction_id) as order_count
                FROM {fact_txn} ft
                JOIN {dim_dates} dd ON ft.date_key = dd.date_key
                GROUP BY dd.full_date, dd.day_of_week
                ORDER BY dd.full_date
            """).df()
        
        conn.close()
        return df
    except FileNotFoundError:
        print("[KPI] fact_transactions or dim_dates not found. Returning empty frame.")
        return pd.DataFrame()


# ─────────────────────────────────────────────────
# 5. CITY-WISE SALES
# ─────────────────────────────────────────────────
@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, FileNotFoundError, OSError))
def compute_city_sales() -> pd.DataFrame:
    """Revenue and order count by customer city. Schema-agnostic."""
    try:
        tables = _get_table_paths()
        fact_txn = tables.get('fact_transactions')
        dim_users = tables.get('dim_users')
        
        if not fact_txn or not dim_users:
            raise FileNotFoundError("Required tables not found in Gold Layer")
        
        conn = _get_conn()
        df = conn.sql(f"""
            SELECT
                du.city,
                COUNT(DISTINCT ft.transaction_id) as order_count,
                SUM(ft.amount) as total_revenue,
                AVG(ft.amount) as avg_order_value,
                COUNT(DISTINCT ft.user_key) as unique_customers
            FROM {fact_txn} ft
            JOIN {dim_users} du ON ft.user_key = du.surrogate_key
            WHERE du.is_current = TRUE AND ft.user_key != -1
            GROUP BY du.city
            ORDER BY total_revenue DESC
        """).df()
        conn.close()
        return df
    except FileNotFoundError:
        print("[KPI] fact_transactions or dim_users not found. Returning empty frame.")
        return pd.DataFrame(columns=["city", "order_count", "total_revenue", "avg_order_value", "unique_customers"])


# ─────────────────────────────────────────────────
# 6. TOP-SELLING PRODUCTS
# ─────────────────────────────────────────────────
@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, FileNotFoundError, OSError))
def compute_top_products(limit: int = 10) -> pd.DataFrame:
    """Top products by revenue and quantity sold. Schema-agnostic."""
    try:
        tables = _get_table_paths()
        fact_txn = tables.get('fact_transactions')
        dim_products = tables.get('dim_products')
        
        if not fact_txn or not dim_products:
            raise FileNotFoundError("Required tables not found in Gold Layer")
        
        conn = _get_conn()
        df = conn.sql(f"""
            SELECT
                dp.product_name,
                dp.category,
                dp.price,
                COUNT(*) as units_sold,
                SUM(ft.amount) as total_revenue,
                AVG(ft.amount) as avg_sale_price
            FROM {fact_txn} ft
            JOIN {dim_products} dp ON ft.product_key = dp.product_key
            WHERE ft.product_key != -1
            GROUP BY dp.product_name, dp.category, dp.price
            ORDER BY total_revenue DESC
            LIMIT {limit}
        """).df()
        conn.close()
        return df
    except FileNotFoundError:
        print("[KPI] fact_transactions or dim_products not found. Returning empty frame.")
        return pd.DataFrame(columns=["product_name", "category", "price", "units_sold", "total_revenue", "avg_sale_price"])


# ─────────────────────────────────────────────────
# 7. INVENTORY TURNOVER RATIO
# ─────────────────────────────────────────────────
@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, FileNotFoundError, OSError))
def compute_inventory_turnover() -> pd.DataFrame:
    """Inventory turnover ratio: Sales / Average Inventory. Schema-agnostic."""
    try:
        tables = _get_table_paths()
        fact_txn = tables.get('fact_transactions')
        fact_inventory = tables.get('fact_inventory')
        dim_products = tables.get('dim_products')
        
        if not fact_txn or not fact_inventory or not dim_products:
            raise FileNotFoundError("Required tables not found in Gold Layer")
        
        conn = _get_conn()
        df = conn.sql(f"""
            WITH sales AS (
                SELECT 
                    product_key, 
                    COUNT(*) as units_sold
                FROM {fact_txn}
                WHERE product_key != -1
                GROUP BY product_key
            ),
            avg_inventory AS (
                SELECT 
                    product_key, 
                    AVG(stock_level) as avg_stock,
                    SUM(CASE WHEN needs_reorder THEN 1 ELSE 0 END) as reorder_instances
                FROM {fact_inventory}
                WHERE product_key != -1
                GROUP BY product_key
            )
            SELECT
                dp.product_name,
                dp.category,
                COALESCE(s.units_sold, 0) as units_sold,
                COALESCE(ai.avg_stock, 0) as avg_stock,
                COALESCE(s.units_sold / NULLIF(ai.avg_stock, 0), 0) as turnover_ratio,
                ai.reorder_instances
            FROM {dim_products} dp
            LEFT JOIN sales s ON dp.product_key = s.product_key
            LEFT JOIN avg_inventory ai ON dp.product_key = ai.product_key
            ORDER BY turnover_ratio DESC
        """).df()
        conn.close()
        return df
    except FileNotFoundError:
        print("[KPI] fact_inventory or fact_transactions not found. Returning empty frame.")
        return pd.DataFrame(columns=["product_name", "category", "units_sold", "avg_stock", "turnover_ratio", "reorder_instances"])


# ─────────────────────────────────────────────────
# 8. AVERAGE DELIVERY TIMES
# ─────────────────────────────────────────────────
@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, FileNotFoundError, OSError))
def compute_delivery_metrics() -> pd.DataFrame:
    """Average delivery times by carrier and region. Schema-agnostic."""
    try:
        tables = _get_table_paths()
        fact_shipments = tables.get('fact_shipments')
        dim_stores = tables.get('dim_stores')
        
        if not fact_shipments or not dim_stores:
            raise FileNotFoundError("Required tables not found in Gold Layer")
        
        conn = _get_conn()
        df = conn.sql(f"""
            SELECT
                fs.carrier,
                ds.region as origin_region,
                AVG(fs.delivery_days) as avg_delivery_days,
                MIN(fs.delivery_days) as min_delivery_days,
                MAX(fs.delivery_days) as max_delivery_days,
                COUNT(*) as shipment_count,
                SUM(CASE WHEN fs.delivery_category = 'fast' THEN 1 ELSE 0 END) as fast_deliveries,
                SUM(CASE WHEN fs.delivery_category = 'delayed' THEN 1 ELSE 0 END) as delayed_deliveries,
                AVG(fs.shipping_cost) as avg_shipping_cost
            FROM {fact_shipments} fs
            JOIN {dim_stores} ds ON fs.origin_store_key = ds.store_key
            WHERE fs.status = 'delivered' AND fs.origin_store_key != -1
            GROUP BY fs.carrier, ds.region
            ORDER BY avg_delivery_days
        """).df()
        conn.close()
        return df
    except FileNotFoundError:
        print("[KPI] fact_shipments or dim_stores not found. Returning empty frame.")
        return pd.DataFrame(columns=["carrier", "origin_region", "avg_delivery_days", "min_delivery_days", "max_delivery_days", "shipment_count", "fast_deliveries", "delayed_deliveries", "avg_shipping_cost"])


# ─────────────────────────────────────────────────
# 9. SEASONAL DEMAND TRENDS
# ─────────────────────────────────────────────────
@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, FileNotFoundError, OSError))
def compute_seasonal_trends() -> pd.DataFrame:
    """Monthly/quarterly demand trends by product category. Schema-agnostic."""
    try:
        tables = _get_table_paths()
        fact_txn = tables.get('fact_transactions')
        dim_dates = tables.get('dim_dates')
        dim_products = tables.get('dim_products')
        
        if not fact_txn or not dim_dates or not dim_products:
            raise FileNotFoundError("Required tables not found in Gold Layer")
        
        conn = _get_conn()
        df = conn.sql(f"""
            SELECT
                dd.year,
                dd.quarter,
                dd.month,
                dp.category,
                COUNT(*) as units_sold,
                SUM(ft.amount) as revenue,
                AVG(ft.amount) as avg_transaction_value
            FROM {fact_txn} ft
            JOIN {dim_dates} dd ON ft.date_key = dd.date_key
            JOIN {dim_products} dp ON ft.product_key = dp.product_key
            WHERE ft.product_key != -1
            GROUP BY dd.year, dd.quarter, dd.month, dp.category
            ORDER BY dd.year, dd.quarter, dd.month, revenue DESC
        """).df()
        conn.close()
        return df
    except FileNotFoundError:
        print("[KPI] fact_transactions, dim_dates, or dim_products not found. Returning empty frame.")
        return pd.DataFrame(columns=["year", "quarter", "month", "category", "units_sold", "revenue", "avg_transaction_value"])


# ─────────────────────────────────────────────────
# 10. NEW VS. RETURNING CUSTOMERS
# ─────────────────────────────────────────────────
@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, FileNotFoundError, OSError))
def compute_customer_segmentation() -> pd.DataFrame:
    """New vs. Returning customers based on purchase history. Schema-agnostic."""
    try:
        tables = _get_table_paths()
        fact_txn = tables.get('fact_transactions')
        
        if not fact_txn:
            raise FileNotFoundError("fact_transactions not found in Gold Layer")
        
        conn = _get_conn()
        df = conn.sql(f"""
            WITH first_purchase AS (
                SELECT
                    user_key,
                    MIN(timestamp)::DATE as first_purchase_date
                FROM {fact_txn}
                WHERE user_key != -1
                GROUP BY user_key
            ),
            customer_classification AS (
                SELECT
                    ft.user_key,
                    ft.transaction_id,
                    ft.amount,
                    ft.timestamp,
                    fp.first_purchase_date,
                    DATEDIFF('day', fp.first_purchase_date, ft.timestamp::DATE) as days_since_first,
                    CASE
                        WHEN DATEDIFF('day', fp.first_purchase_date, ft.timestamp::DATE) <= 7 THEN 'New'
                        ELSE 'Returning'
                    END as customer_type
                FROM {fact_txn} ft
                JOIN first_purchase fp ON ft.user_key = fp.user_key
                WHERE ft.user_key != -1
            )
            SELECT
                customer_type,
                COUNT(DISTINCT user_key) as customer_count,
                COUNT(DISTINCT transaction_id) as order_count,
                SUM(amount) as total_revenue,
                AVG(amount) as avg_order_value
            FROM customer_classification
            GROUP BY customer_type
            ORDER BY customer_type
        """).df()
        conn.close()
        return df
    except FileNotFoundError:
        print("[KPI] fact_transactions not found. Returning empty frame.")
        return pd.DataFrame(columns=["customer_type", "customer_count", "order_count", "total_revenue", "avg_order_value"])


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
    
    print("\n=== Revenue Time-Series (Daily, last 7 days) ===")
    print(compute_revenue_timeseries('daily').tail(7))
    
    print("\n=== City-Wise Sales ===")
    print(compute_city_sales().head())
    
    print("\n=== Top Products ===")
    print(compute_top_products(5))
    
    print("\n=== Inventory Turnover ===")
    print(compute_inventory_turnover().head())
    
    print("\n=== Delivery Metrics ===")
    print(compute_delivery_metrics().head())
    
    print("\n=== Seasonal Trends (last 3 months) ===")
    print(compute_seasonal_trends().tail(15))
    
    print("\n=== Customer Segmentation ===")
    print(compute_customer_segmentation())
