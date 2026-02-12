import duckdb

print("=== dim_products ===")
print(duckdb.sql("SELECT * FROM 'data/gold/dim_products.parquet' LIMIT 10").df())

print("\n=== Silver transactions sample ===")
print(duckdb.sql("SELECT product_id FROM 'data/silver/transactions.parquet' LIMIT 10").df())

print("\n=== Testing join manually ===")
result = duckdb.sql("""
    SELECT
        t.product_id AS txn_product_id,
        dp.product_id AS dim_product_id,
        dp.product_key
    FROM 'data/silver/transactions.parquet' t
    LEFT JOIN 'data/gold/dim_products.parquet' dp
        ON t.product_id = dp.product_id
    LIMIT 10
""").df()
print(result)
