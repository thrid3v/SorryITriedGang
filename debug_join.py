import duckdb
import os

_BASE = os.path.join(os.path.dirname(__file__), "..", "..")
SILVER_DIR = os.path.join(_BASE, "data", "silver")
GOLD_DIR = os.path.join(_BASE, "data", "gold")

SILVER_TXN = os.path.join(SILVER_DIR, "transactions.parquet").replace("\\", "/")
GOLD_DIM_PRODUCTS = os.path.join(GOLD_DIR, "dim_products.parquet").replace("\\", "/")

print("=== Silver transactions (first 5) ===")
silver = duckdb.sql(f"SELECT transaction_id, product_id FROM '{SILVER_TXN}' LIMIT 5").df()
print(silver)
print(f"\nproduct_id dtype: {silver['product_id'].dtype}")

print("\n=== dim_products (first 5) ===")
dim = duckdb.sql(f"SELECT product_key, product_id FROM '{GOLD_DIM_PRODUCTS}' LIMIT 5").df()
print(dim)

print("\n=== Manual join test ===")
result = duckdb.sql(f"""
    SELECT
        t.transaction_id,
        t.product_id AS txn_product_id,
        dp.product_id AS dim_product_id,
        COALESCE(dp.product_key, -1) AS product_key
    FROM '{SILVER_TXN}' t
    LEFT JOIN '{GOLD_DIM_PRODUCTS}' dp
        ON t.product_id = dp.product_id
    LIMIT 10
""").df()
print(result)
print(f"\nProduct keys that are -1: {(result['product_key'] == -1).sum()} out of {len(result)}")
