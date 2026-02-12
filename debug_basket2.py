import duckdb

# Check the actual data in fact_transactions
print("=== Sample transactions with same transaction_id ===")
sample = duckdb.sql("""
    SELECT transaction_id, product_key
    FROM 'data/gold/fact_transactions.parquet'
    WHERE transaction_id IN (
        SELECT transaction_id
        FROM 'data/gold/fact_transactions.parquet'
        GROUP BY transaction_id
        HAVING COUNT(*) > 1
        LIMIT 3
    )
    ORDER BY transaction_id, product_key
""").df()
print(sample)

print("\n=== Testing basket join manually ===")
basket = duckdb.sql("""
    WITH multi_txn AS (
        SELECT transaction_id
        FROM 'data/gold/fact_transactions.parquet'
        GROUP BY transaction_id
        HAVING COUNT(*) > 1
        LIMIT 1
    )
    SELECT
        t1.transaction_id,
        t1.product_key AS product_a,
        t2.product_key AS product_b
    FROM 'data/gold/fact_transactions.parquet' t1
    JOIN 'data/gold/fact_transactions.parquet' t2
        ON  t1.transaction_id = t2.transaction_id
        AND t1.product_key < t2.product_key
    WHERE t1.transaction_id IN (SELECT transaction_id FROM multi_txn)
      AND t1.product_key != -1 
      AND t2.product_key != -1
""").df()
print(basket)
print(f"\nProduct pairs found: {len(basket)}")
