"""
RetailNexus - Bronze -> Silver Cleaner
Reads raw CSVs from data/raw/, deduplicates, handles nulls, casts types,
and writes cleaned Parquet to data/silver/.
Uses union_by_name=true for schema evolution resilience.
Each cleaner is OPTIONAL — if raw files don't exist, it is skipped.
"""
import os
import sys
import glob as globmod
from pathlib import Path

import duckdb

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.utils.retry_utils import retry_with_backoff

_BASE = os.path.join(os.path.dirname(__file__), "..", "..")
RAW_DIR = os.path.join(_BASE, "data", "raw")
SILVER_DIR = os.path.join(_BASE, "data", "silver")


def _ensure_dirs():
    os.makedirs(SILVER_DIR, exist_ok=True)


def _glob(pattern: str) -> str:
    """Build an absolute glob path for DuckDB's read_csv."""
    return os.path.join(RAW_DIR, pattern).replace("\\", "/")


def _has_files(pattern: str) -> bool:
    """Check if any files match the given glob pattern in the raw directory."""
    return len(globmod.glob(os.path.join(RAW_DIR, pattern))) > 0


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError))
def clean_transactions():
    """Deduplicate, drop null PKs, validate positive amounts, cast types.
    Handles multiple column naming conventions via COALESCE fallbacks.
    When union_by_name=true merges CSVs with different schemas, both column
    names exist but only one will be non-NULL per row."""
    if not _has_files("transactions_*.csv"):
        print("[Cleaner] No transactions CSVs found - skipping")
        return False
    glob_path = _glob("transactions_*.csv")
    
    # Discover all columns across all CSV files
    try:
        cols = [c[0].lower() for c in duckdb.sql(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true))"
        ).fetchall()]
    except Exception as e:
        print(f"[Cleaner] Cannot read transactions CSVs: {e}")
        return False
    
    print(f"[Cleaner] Detected columns: {cols}")
    
    # Build COALESCE expressions — cast each part to target type BEFORE coalescing
    # to avoid type mismatches (e.g., TIMESTAMP vs VARCHAR)
    
    # transaction_id (all cast to VARCHAR)
    tid_parts = [f'TRY_CAST("{c}" AS VARCHAR)' for c in ['transaction_id', 'invoice', 'order_id', 'invoice_no'] if c in cols]
    tid_expr = f"COALESCE({', '.join(tid_parts)})" if tid_parts else "CAST(ROW_NUMBER() OVER () AS VARCHAR)"
    
    # product_id (all cast to VARCHAR)
    pid_parts = [f'TRY_CAST("{c}" AS VARCHAR)' for c in ['product_id', 'stockcode', 'sku', 'item_id', 'product_code'] if c in cols]
    pid_expr = f"COALESCE({', '.join(pid_parts)})" if pid_parts else "'UNKNOWN'"
    
    # user_id (all cast to VARCHAR — handles numeric user IDs like 17850.0)
    uid_parts = [f'TRY_CAST("{c}" AS VARCHAR)' for c in ['user_id', 'customer_id', 'client_id', 'customerid'] if c in cols]
    uid_expr = f"COALESCE({', '.join(uid_parts)})" if uid_parts else "'U001'"
    
    # timestamp (all cast to TIMESTAMP)
    ts_parts = [f'TRY_CAST("{c}" AS TIMESTAMP)' for c in ['timestamp', 'invoicedate', 'date', 'order_date', 'invoice_date'] if c in cols]
    ts_expr = f"COALESCE({', '.join(ts_parts)})" if ts_parts else "CURRENT_TIMESTAMP"
    
    # amount (try amount, price*quantity, price — all cast to DOUBLE)
    amt_candidates = []
    if 'amount' in cols:
        amt_candidates.append('TRY_CAST("amount" AS DOUBLE)')
    if 'price' in cols and 'quantity' in cols:
        amt_candidates.append('TRY_CAST("price" AS DOUBLE) * TRY_CAST("quantity" AS DOUBLE)')
    if 'price' in cols:
        amt_candidates.append('TRY_CAST("price" AS DOUBLE)')
    if 'total' in cols:
        amt_candidates.append('TRY_CAST("total" AS DOUBLE)')
    amt_expr = f"COALESCE({', '.join(amt_candidates)}, 0)" if amt_candidates else "0"
    
    # store_id (all cast to VARCHAR)
    sid_parts = [f'TRY_CAST("{c}" AS VARCHAR)' for c in ['store_id', 'country', 'location', 'branch'] if c in cols]
    sid_expr = f"COALESCE({', '.join(sid_parts)})" if sid_parts else "'S001'"

    
    out_path = os.path.join(SILVER_DIR, "transactions.parquet").replace(chr(92), "/")
    
    # Use aliases in DISTINCT ON to avoid expression repetition
    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT ON (txn_id, prod_id)
                txn_id   AS transaction_id,
                uid      AS user_id,
                prod_id  AS product_id,
                ts       AS timestamp,
                amt      AS amount,
                sid      AS store_id
            FROM (
                SELECT
                    {tid_expr} AS txn_id,
                    {uid_expr} AS uid,
                    {pid_expr} AS prod_id,
                    {ts_expr}  AS ts,
                    {amt_expr} AS amt,
                    {sid_expr} AS sid
                FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true)
            ) sub
            WHERE txn_id IS NOT NULL 
              AND prod_id IS NOT NULL
              AND amt > 0
            ORDER BY txn_id, prod_id, ts
        ) TO '{out_path}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"SELECT COUNT(*) FROM '{out_path}'").fetchone()[0]
    print(f"[Cleaner] Silver transactions: {cnt} rows")
    return True




@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError))
def clean_users():
    """Deduplicate on user_id, keeping the LATEST record."""
    if not _has_files("users_*.csv"):
        print("[Cleaner] No users CSVs found - skipping")
        return False
    glob_path = _glob("users_*.csv")
    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT ON (user_id)
                user_id::VARCHAR                      AS user_id,
                name::VARCHAR                         AS name,
                email::VARCHAR                        AS email,
                COALESCE(city, 'Unknown')::VARCHAR    AS city,
                signup_date::DATE                     AS signup_date
            FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true, filename=true)
            WHERE user_id IS NOT NULL
            ORDER BY user_id, filename DESC, signup_date DESC
        ) TO '{os.path.join(SILVER_DIR, "users.parquet").replace(chr(92), "/")}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"""
        SELECT COUNT(*) FROM '{os.path.join(SILVER_DIR, "users.parquet").replace(chr(92), "/")}'
    """).fetchone()[0]
    print(f"[Cleaner] Silver users: {cnt} rows")
    return True


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError))
def clean_products():
    """Deduplicate on product_id, validate positive prices."""
    if not _has_files("products_*.csv"):
        print("[Cleaner] No products CSVs found - skipping")
        return False
    glob_path = _glob("products_*.csv")
    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT ON (product_id)
                product_id::VARCHAR               AS product_id,
                product_name::VARCHAR             AS product_name,
                category::VARCHAR                 AS category,
                COALESCE(price, 0)::DOUBLE        AS price
            FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true)
            WHERE product_id IS NOT NULL
              AND COALESCE(price, 0) > 0
            ORDER BY product_id
        ) TO '{os.path.join(SILVER_DIR, "products.parquet").replace(chr(92), "/")}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"""
        SELECT COUNT(*) FROM '{os.path.join(SILVER_DIR, "products.parquet").replace(chr(92), "/")}'
    """).fetchone()[0]
    print(f"[Cleaner] Silver products: {cnt} rows")
    return True


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError))
def clean_inventory():
    """Clean inventory data, validate stock levels."""
    if not _has_files("inventory_*.csv"):
        print("[Cleaner] No inventory CSVs found - skipping")
        return False
    glob_path = _glob("inventory_*.csv")
    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT ON (product_id, store_id)
                product_id::VARCHAR                AS product_id,
                store_id::VARCHAR                  AS store_id,
                stock_level::INTEGER               AS stock_level,
                reorder_point::INTEGER             AS reorder_point,
                last_restock_date::DATE            AS last_restock_date,
                stock_status::VARCHAR              AS stock_status
            FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true)
            WHERE product_id IS NOT NULL 
              AND store_id IS NOT NULL
              AND stock_level >= 0
            ORDER BY product_id, store_id
        ) TO '{os.path.join(SILVER_DIR, "inventory.parquet").replace(chr(92), "/")}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"""
        SELECT COUNT(*) FROM '{os.path.join(SILVER_DIR, "inventory.parquet").replace(chr(92), "/")}'
    """).fetchone()[0]
    print(f"[Cleaner] Silver inventory: {cnt} rows")
    return True


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError))
def clean_shipments():
    """Clean shipment data, validate dates and costs."""
    if not _has_files("shipments_*.csv"):
        print("[Cleaner] No shipments CSVs found - skipping")
        return False
    glob_path = _glob("shipments_*.csv")
    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT ON (shipment_id)
                shipment_id::VARCHAR               AS shipment_id,
                transaction_id::VARCHAR            AS transaction_id,
                origin_store_id::VARCHAR           AS origin_store_id,
                dest_store_id::VARCHAR             AS dest_store_id,
                shipped_date::DATE                 AS shipped_date,
                delivered_date::DATE               AS delivered_date,
                delivery_days::INTEGER             AS delivery_days,
                carrier::VARCHAR                   AS carrier,
                tracking_number::VARCHAR           AS tracking_number,
                status::VARCHAR                    AS status,
                shipping_cost::DOUBLE              AS shipping_cost
            FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true)
            WHERE shipment_id IS NOT NULL
              AND COALESCE(shipping_cost, 0) >= 0
            ORDER BY shipment_id
        ) TO '{os.path.join(SILVER_DIR, "shipments.parquet").replace(chr(92), "/")}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"""
        SELECT COUNT(*) FROM '{os.path.join(SILVER_DIR, "shipments.parquet").replace(chr(92), "/")}'
    """).fetchone()[0]
    print(f"[Cleaner] Silver shipments: {cnt} rows")
    return True


def clean_all():
    """Run all cleaners: Bronze -> Silver. Each is independent and optional."""
    _ensure_dirs()
    results = {}
    
    for name, func in [
        ("transactions", clean_transactions),
        ("users", clean_users),
        ("products", clean_products),
        ("inventory", clean_inventory),
        ("shipments", clean_shipments),
    ]:
        try:
            results[name] = func()
        except Exception as e:
            print(f"[Cleaner] {name} failed: {e}")
            results[name] = False
    
    cleaned = [k for k, v in results.items() if v]
    if cleaned:
        print(f"[Cleaner] Bronze -> Silver complete. Cleaned: {', '.join(cleaned)}")
    else:
        print("[Cleaner] No raw data files found to clean.")
    
    return results


if __name__ == "__main__":
    clean_all()
