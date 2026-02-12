"""
RetailNexus — Bronze → Silver Cleaner
Reads raw CSVs from data/raw/, deduplicates, handles nulls, casts types,
and writes cleaned Parquet to data/silver/.
Uses union_by_name=true for schema evolution resilience.
"""
import os

import duckdb

_BASE = os.path.join(os.path.dirname(__file__), "..", "..")
RAW_DIR = os.path.join(_BASE, "data", "raw")
SILVER_DIR = os.path.join(_BASE, "data", "silver")


def _ensure_dirs():
    os.makedirs(SILVER_DIR, exist_ok=True)


def _glob(pattern: str) -> str:
    """Build an absolute glob path for DuckDB's read_csv."""
    return os.path.join(RAW_DIR, pattern).replace("\\", "/")


def clean_transactions():
    """Deduplicate, drop null PKs, fill null amounts with 0, cast types."""
    glob_path = _glob("transactions_*.csv")
    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT ON (transaction_id)
                transaction_id::VARCHAR   AS transaction_id,
                user_id::VARCHAR          AS user_id,
                product_id::VARCHAR       AS product_id,
                timestamp::TIMESTAMP      AS timestamp,
                COALESCE(amount, 0)::DOUBLE AS amount,
                store_id::VARCHAR         AS store_id
            FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true)
            WHERE transaction_id IS NOT NULL
            ORDER BY transaction_id, timestamp
        ) TO '{os.path.join(SILVER_DIR, "transactions.parquet").replace(chr(92), "/")}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"""
        SELECT COUNT(*) FROM '{os.path.join(SILVER_DIR, "transactions.parquet").replace(chr(92), "/")}'
    """).fetchone()[0]
    print(f"[Cleaner] Silver transactions: {cnt} rows")


def clean_users():
    """Deduplicate on user_id (keep latest by signup_date), fill null cities with 'Unknown'."""
    glob_path = _glob("users_*.csv")
    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT ON (user_id)
                user_id::VARCHAR                      AS user_id,
                name::VARCHAR                         AS name,
                email::VARCHAR                        AS email,
                COALESCE(city, 'Unknown')::VARCHAR    AS city,
                signup_date::DATE                     AS signup_date
            FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true)
            WHERE user_id IS NOT NULL
            ORDER BY user_id, signup_date DESC
        ) TO '{os.path.join(SILVER_DIR, "users.parquet").replace(chr(92), "/")}' (FORMAT PARQUET)
    """)
    cnt = duckdb.sql(f"""
        SELECT COUNT(*) FROM '{os.path.join(SILVER_DIR, "users.parquet").replace(chr(92), "/")}'
    """).fetchone()[0]
    print(f"[Cleaner] Silver users: {cnt} rows")


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
