import duckdb

# Debug market basket query
print("=== Checking fact_transactions ===")
txn_df = duckdb.sql("SELECT * FROM 'data/gold/fact_transactions.parquet' LIMIT 10").df()
print(txn_df)

print("\n=== Checking for multiple products per transaction ===")
multi = duckdb.sql("""
    SELECT transaction_id, COUNT(*) as product_count
    FROM 'data/gold/fact_transactions.parquet'
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
    LIMIT 10
""").df()
print(multi)
print(f"Transactions with multiple products: {len(multi)}")

print("\n=== Testing basket query ===")
basket = duckdb.sql("""
    SELECT
        t1.transaction_id,
        t1.product_key AS product_a,
        t2.product_key AS product_b
    FROM 'data/gold/fact_transactions.parquet' t1
    JOIN 'data/gold/fact_transactions.parquet' t2
        ON  t1.transaction_id = t2.transaction_id
        AND t1.product_key < t2.product_key
    WHERE t1.product_key != -1 AND t2.product_key != -1
    LIMIT 10
""").df()
print(basket)
print(f"Product pairs found: {len(basket)}")
