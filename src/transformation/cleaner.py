"""
RetailNexus — Bronze → Silver Cleaner
Reads raw CSVs from data/raw/, deduplicates, handles nulls, casts types,
and writes cleaned Parquet to data/silver/.
Uses union_by_name=true for schema evolution resilience.
"""
import os
import sys
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


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError, FileNotFoundError))
def clean_transactions():
    """Deduplicate, drop null PKs, fill null amounts with 0, cast types."""
    glob_path = _glob("transactions_*.csv")
    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT ON (transaction_id, product_id)
                transaction_id::VARCHAR   AS transaction_id,
                user_id::VARCHAR          AS user_id,
                product_id::VARCHAR       AS product_id,
                timestamp::TIMESTAMP      AS timestamp,
                COALESCE(amount, 0)::DOUBLE AS amount,
                store_id::VARCHAR         AS store_id
            FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true)
            WHERE transaction_id IS NOT NULL AND product_id IS NOT NULL
            ORDER BY transaction_id, product_id, timestamp
        ) TO '{os.path.join(SILVER_DIR, "transactions.parquet").replace(chr(92), "/")}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"""
        SELECT COUNT(*) FROM '{os.path.join(SILVER_DIR, "transactions.parquet").replace(chr(92), "/")}'
    """).fetchone()[0]
    print(f"[Cleaner] Silver transactions: {cnt} rows")


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError, FileNotFoundError))
def clean_users():
    """Deduplicate on user_id, keeping the LATEST record per user_id (most recent signup_date).
    This ensures SCD2 sees the latest version of each user from the latest batch."""
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


@retry_with_backoff(max_attempts=3, exceptions=(duckdb.IOException, OSError, FileNotFoundError))
def clean_products():
    """Deduplicate on product_id, fill null prices with 0."""
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
            ORDER BY product_id
        ) TO '{os.path.join(SILVER_DIR, "products.parquet").replace(chr(92), "/")}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"""
        SELECT COUNT(*) FROM '{os.path.join(SILVER_DIR, "products.parquet").replace(chr(92), "/")}'
    """).fetchone()[0]
    print(f"[Cleaner] Silver products: {cnt} rows")


def clean_all():
    """Run all cleaners: Bronze → Silver."""
    _ensure_dirs()
    try:
        clean_transactions()
        clean_users()
        clean_products()
        print("[Cleaner] Bronze → Silver complete ✓")
    except Exception as e:
        if "No files found" in str(e) or isinstance(e, FileNotFoundError):
            print(f"[Cleaner] Waiting for raw data... ({e})")
        else:
            raise


if __name__ == "__main__":
    clean_all()
