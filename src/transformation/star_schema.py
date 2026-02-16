"""
RetailNexus - Silver -> Gold Star Schema Builder
Builds dimension tables (dim_products, dim_stores, dim_dates) and fact_transactions
from Silver-layer Parquet files.  dim_users is handled by scd_logic.py.
All transformations via duckdb.sql().
Each builder is OPTIONAL — if its Silver source is missing, it is skipped.
"""
import os
import sys
from pathlib import Path

import duckdb

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.utils.retry_utils import retry_with_backoff

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


def _silver_exists(filename: str) -> bool:
    """Check if a Silver-layer parquet file exists."""
    return os.path.isfile(os.path.join(SILVER_DIR, filename))


def _gold_exists(filename: str) -> bool:
    """Check if a Gold-layer parquet file or directory exists."""
    path = os.path.join(GOLD_DIR, filename)
    return os.path.isfile(path) or os.path.isdir(path)


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError))
def build_dim_products():
    """Straight copy from Silver with a surrogate key."""
    if not _silver_exists("products.parquet"):
        print("[StarSchema] No Silver products - skipping dim_products")
        return False
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
    return True


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError))
def build_dim_stores():
    """Extract distinct stores from transactions."""
    if not _silver_exists("transactions.parquet"):
        print("[StarSchema] No Silver transactions - skipping dim_stores")
        return False
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
    return True


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError))
def build_dim_dates():
    """Generate a date dimension from the range of transaction timestamps."""
    if not _silver_exists("transactions.parquet"):
        print("[StarSchema] No Silver transactions - skipping dim_dates")
        return False
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
    return True


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError))
def build_fact_transactions():
    """
    Join Silver transactions with Gold dim tables to produce the fact table.
    Handles missing dims by using LEFT JOINs with fallback values.
    """
    if not _silver_exists("transactions.parquet"):
        print("[StarSchema] No Silver transactions - skipping fact_transactions")
        return False

    # Build the query dynamically based on available dims
    select_parts = [
        "t.transaction_id",
        "CAST(STRFTIME(t.timestamp, '%Y%m%d') AS INTEGER) AS date_key",
        "t.timestamp",
        "t.amount",
    ]
    join_parts = []

    if _gold_exists("dim_users.parquet"):
        select_parts.append("COALESCE(du.surrogate_key, -1) AS user_key")
        join_parts.append(f"LEFT JOIN '{GOLD_DIM_USERS}' du ON t.user_id = du.user_id AND du.is_current = TRUE")
    else:
        select_parts.append("-1 AS user_key")

    if _gold_exists("dim_products.parquet"):
        select_parts.append("COALESCE(dp.product_key, -1) AS product_key")
        join_parts.append(f"LEFT JOIN '{GOLD_DIM_PRODUCTS}' dp ON t.product_id = dp.product_id")
    else:
        select_parts.append("-1 AS product_key")

    if _gold_exists("dim_stores.parquet"):
        select_parts.append("COALESCE(ds.store_key, -1) AS store_key")
        select_parts.append("COALESCE(ds.region, 'Unknown') AS region")
        join_parts.append(f"LEFT JOIN '{GOLD_DIM_STORES}' ds ON t.store_id = ds.store_id")
    else:
        select_parts.append("-1 AS store_key")
        select_parts.append("'Unknown' AS region")

    select_clause = ",\n            ".join(select_parts)
    join_clause = "\n        ".join(join_parts)

    duckdb.sql(f"""
        CREATE OR REPLACE TABLE fact_transactions_temp AS
        SELECT
            {select_clause}
        FROM '{SILVER_TXN}' t
        {join_clause}
    """)
    
    
    # Write to temp file first, then atomic rename to prevent file locking
    # This ensures backend can always read a complete, valid file
    import shutil
    temp_file = f"{GOLD_FACT_TXN}.tmp"
    
    try:
        # Write to temporary file (won't interfere with reads)
        duckdb.sql(f"""
            COPY fact_transactions_temp 
            TO '{temp_file}' 
            (FORMAT PARQUET)
        """)
        
        # Clean up temp table now that data is written
        duckdb.sql("DROP TABLE IF EXISTS fact_transactions_temp")
        
        # Remove old file if exists (handle both file and directory cases)
        if os.path.exists(GOLD_FACT_TXN):
            if os.path.isdir(GOLD_FACT_TXN):
                shutil.rmtree(GOLD_FACT_TXN)
            else:
                os.remove(GOLD_FACT_TXN)
        
        # Atomic rename - this is instantaneous, no lock window
        os.rename(temp_file, GOLD_FACT_TXN)
        
    except Exception as e:
        # Clean up temp table if it still exists
        try:
            duckdb.sql("DROP TABLE IF EXISTS fact_transactions_temp")
        except:
            pass
        # Clean up temp file on error
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
        raise e
    
    cnt = duckdb.sql(f"SELECT COUNT(*) FROM '{GOLD_FACT_TXN}'").fetchone()[0]
    print(f"[StarSchema] fact_transactions: {cnt} rows")
    return True


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError))
def build_fact_inventory():
    """Build inventory fact table from Silver inventory data."""
    SILVER_INVENTORY = os.path.join(SILVER_DIR, "inventory.parquet").replace("\\", "/")
    GOLD_FACT_INVENTORY = os.path.join(GOLD_DIR, "fact_inventory.parquet").replace("\\", "/")
    
    if not _silver_exists("inventory.parquet"):
        print("[StarSchema] No Silver inventory - skipping fact_inventory")
        return False
    
    import shutil
    
    duckdb.sql(f"""
        CREATE OR REPLACE TABLE fact_inventory_temp AS
        SELECT
            COALESCE(dp.product_key, -1)                     AS product_key,
            COALESCE(ds.store_key, -1)                       AS store_key,
            COALESCE(ds.region, 'Unknown')                   AS region,
            CAST(STRFTIME(i.last_restock_date, '%Y%m%d') AS INTEGER) AS date_key,
            i.stock_level,
            i.reorder_point,
            i.last_restock_date,
            i.stock_status,
            CASE 
                WHEN i.stock_level <= i.reorder_point THEN TRUE 
                ELSE FALSE 
            END AS needs_reorder,
            DATEDIFF('day', i.last_restock_date, CURRENT_DATE) AS days_since_restock
        FROM '{SILVER_INVENTORY}' i
        LEFT JOIN '{GOLD_DIM_PRODUCTS}' dp
            ON i.product_id = dp.product_id
        LEFT JOIN '{GOLD_DIM_STORES}' ds
            ON i.store_id = ds.store_id
    """)
    
    try:
        if os.path.exists(GOLD_FACT_INVENTORY):
            shutil.rmtree(GOLD_FACT_INVENTORY)
    except PermissionError:
        pass
    
    duckdb.sql(f"""
        COPY fact_inventory_temp
        TO '{GOLD_FACT_INVENTORY}'
        (FORMAT PARQUET, PARTITION_BY (region, date_key), OVERWRITE_OR_IGNORE)
    """)
    
    duckdb.sql("DROP TABLE IF EXISTS fact_inventory_temp")
    cnt = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{GOLD_FACT_INVENTORY}/**/*.parquet', hive_partitioning=true)").fetchone()[0]
    print(f"[StarSchema] fact_inventory: {cnt} rows")
    return True


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError))
def build_fact_shipments():
    """Build shipments fact table from Silver shipment data."""
    SILVER_SHIPMENTS = os.path.join(SILVER_DIR, "shipments.parquet").replace("\\", "/")
    GOLD_FACT_SHIPMENTS = os.path.join(GOLD_DIR, "fact_shipments.parquet").replace("\\", "/")
    
    if not _silver_exists("shipments.parquet"):
        print("[StarSchema] No Silver shipments - skipping fact_shipments")
        return False
    
    import shutil
    
    duckdb.sql(f"""
        CREATE OR REPLACE TABLE fact_shipments_temp AS
        SELECT
            s.shipment_id,
            s.transaction_id,
            COALESCE(ds_origin.store_key, -1)                AS origin_store_key,
            COALESCE(ds_dest.store_key, -1)                  AS dest_store_key,
            COALESCE(ds_origin.region, 'Unknown')            AS origin_region,
            COALESCE(ds_dest.region, 'Unknown')              AS dest_region,
            CAST(STRFTIME(s.shipped_date, '%Y%m%d') AS INTEGER) AS date_key,
            s.shipped_date,
            s.delivered_date,
            s.delivery_days,
            s.carrier,
            s.tracking_number,
            s.status,
            s.shipping_cost,
            CASE 
                WHEN s.status = 'delivered' AND s.delivery_days <= 3 THEN 'fast'
                WHEN s.status = 'delivered' AND s.delivery_days <= 7 THEN 'normal'
                WHEN s.status = 'delivered' THEN 'slow'
                WHEN s.status = 'delayed' THEN 'delayed'
                ELSE 'pending'
            END AS delivery_category
        FROM '{SILVER_SHIPMENTS}' s
        LEFT JOIN '{GOLD_DIM_STORES}' ds_origin
            ON s.origin_store_id = ds_origin.store_id
        LEFT JOIN '{GOLD_DIM_STORES}' ds_dest
            ON s.dest_store_id = ds_dest.store_id
    """)
    
    try:
        if os.path.exists(GOLD_FACT_SHIPMENTS):
            shutil.rmtree(GOLD_FACT_SHIPMENTS)
    except PermissionError:
        pass
    
    duckdb.sql(f"""
        COPY fact_shipments_temp
        TO '{GOLD_FACT_SHIPMENTS}'
        (FORMAT PARQUET, PARTITION_BY (origin_region, date_key), OVERWRITE_OR_IGNORE)
    """)
    
    duckdb.sql("DROP TABLE IF EXISTS fact_shipments_temp")
    cnt = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{GOLD_FACT_SHIPMENTS}/**/*.parquet', hive_partitioning=true)").fetchone()[0]
    print(f"[StarSchema] fact_shipments: {cnt} rows")
    return True


def build_star_schema():
    """Build all Gold-layer tables (excluding dim_users, handled by SCD).
    Each builder is independent — missing Silver files are skipped."""
    _ensure_gold()
    results = {}
    
    for name, func in [
        ("dim_products", build_dim_products),
        ("dim_stores", build_dim_stores),
        ("dim_dates", build_dim_dates),
        ("fact_transactions", build_fact_transactions),
        ("fact_inventory", build_fact_inventory),
        ("fact_shipments", build_fact_shipments),
    ]:
        try:
            results[name] = func()
        except Exception as e:
            print(f"[StarSchema] {name} failed: {e}")
            results[name] = False
    
    built = [k for k, v in results.items() if v]
    if built:
        print(f"[StarSchema] Silver -> Gold complete. Built: {', '.join(built)}")
    else:
        print("[StarSchema] No Silver data available to build Gold layer.")
    
    return results


if __name__ == "__main__":
    build_star_schema()
