"""
RetailNexus — Storage, Access & Security Utilities
====================================================
Handles:
  • Parquet-based optimized storage with partitioning
  • DuckDB-backed secure read access
  • Column-level access policies (role-based)
"""

import os
import duckdb
import pandas as pd
from pathlib import Path

# ──────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR     = PROJECT_ROOT / "data"
RAW_DIR      = DATA_DIR / "raw"
SILVER_DIR   = DATA_DIR / "silver"
GOLD_DIR     = DATA_DIR / "gold"

for d in [RAW_DIR, SILVER_DIR, GOLD_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ──────────────────────────────────────────────
# 1. OPTIMIZED STORAGE  (Parquet + Partitioning)
# ──────────────────────────────────────────────
def write_partitioned_parquet(
    df: pd.DataFrame,
    target_dir: Path,
    partition_cols: list[str],
    *,
    compression: str = "snappy",
) -> None:
    """
    Write a DataFrame to Parquet partitioned by the given columns.
    Example: partition_cols=["region", "order_date"] creates
    target_dir/region=East/order_date=2025-01-01/part-0.parquet
    """
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(
            target_dir,
            engine="pyarrow",
            partition_cols=partition_cols,
            compression=compression,
            index=False,
        )
    except FileNotFoundError:
        print(f"[WARN] Target directory {target_dir} could not be created.")


def read_parquet_with_duckdb(parquet_path: str) -> pd.DataFrame:
    """
    Read a Parquet file/directory (including Hive-partitioned datasets)
    using DuckDB for fast OLAP scans.
    """
    try:
        return duckdb.sql(
            f"SELECT * FROM read_parquet('{parquet_path}', hive_partitioning=true)"
        ).df()
    except FileNotFoundError:
        print(f"[WARN] Parquet path not found: {parquet_path}. Returning empty frame.")
        return pd.DataFrame()


# ──────────────────────────────────────────────
# 2. PARTITIONING STRATEGY
# ──────────────────────────────────────────────
"""
Recommended partition layout for RetailNexus gold tables:

  data/gold/
  ├── fact_transactions/
  │   └── region=<region>/order_date=<date>/part-*.parquet
  ├── dim_customers/
  │   └── region=<region>/part-*.parquet
  └── dim_products/
      └── category=<cat>/part-*.parquet

Why?
  • Region + Date are the most common filter predicates in analyst queries.
  • DuckDB prunes partitions automatically via hive_partitioning=true,
    so queries like  WHERE region='East' AND order_date >= '2025-01-01'
    skip irrelevant files entirely → lower I/O, faster answers.
"""


# ──────────────────────────────────────────────
# 3. SECURE ACCESS  (role-based column masking)
# ──────────────────────────────────────────────
# Role definitions — maps each role to the columns it may see.
ACCESS_POLICIES: dict[str, dict] = {
    "analyst": {
        "allowed_tables": ["fact_transactions", "dim_customers", "dim_products"],
        "masked_columns": ["email", "phone"],       # PII masked
    },
    "manager": {
        "allowed_tables": ["fact_transactions", "dim_customers", "dim_products"],
        "masked_columns": [],                        # full access
    },
    "viewer": {
        "allowed_tables": ["fact_transactions"],
        "masked_columns": ["email", "phone", "amount"],
    },
}


def secure_read(
    table_name: str,
    role: str = "analyst",
    parquet_base: Path = GOLD_DIR,
) -> pd.DataFrame:
    """
    Read a gold-layer table applying column-level masking based on role.
    Sensitive columns are replaced with '***MASKED***'.
    """
    policy = ACCESS_POLICIES.get(role)
    if policy is None:
        raise PermissionError(f"Unknown role: {role}")

    if table_name not in policy["allowed_tables"]:
        raise PermissionError(
            f"Role '{role}' is not authorized to access table '{table_name}'."
        )

    parquet_path = str(parquet_base / table_name / "**/*.parquet")
    try:
        df = duckdb.sql(
            f"SELECT * FROM read_parquet('{parquet_path}', hive_partitioning=true)"
        ).df()
    except FileNotFoundError:
        print(f"[WARN] No data found for {table_name}. Returning empty frame.")
        return pd.DataFrame()

    # mask sensitive columns
    for col in policy["masked_columns"]:
        if col in df.columns:
            df[col] = "***MASKED***"

    return df
