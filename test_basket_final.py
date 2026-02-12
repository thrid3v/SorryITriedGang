import duckdb

print("=== Checking for multi-product transactions ===")
multi = duckdb.sql("""
    SELECT transaction_id, COUNT(*) as product_count
    FROM 'data/gold/fact_transactions.parquet'
    WHERE product_key != -1
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
    LIMIT 10
""").df()
print(multi)
print(f"\nTransactions with multiple products: {len(multi)}")

print("\n=== Testing market basket query ===")
basket = duckdb.sql("""
    WITH basket AS (
        SELECT
            t1.transaction_id,
            t1.product_key AS product_a,
            t2.product_key AS product_b
        FROM 'data/gold/fact_transactions.parquet' t1
        JOIN 'data/gold/fact_transactions.parquet' t2
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
        HAVING COUNT(*) >= 2
    )
    SELECT
        pa.product_name  AS product_a_name,
        pb.product_name  AS product_b_name,
        pc.times_bought_together
    FROM pair_counts pc
    LEFT JOIN 'data/gold/dim_products.parquet' pa
        ON pc.product_a = pa.product_key
    LEFT JOIN 'data/gold/dim_products.parquet' pb
        ON pc.product_b = pb.product_key
    ORDER BY pc.times_bought_together DESC
    LIMIT 10
""").df()
print(basket)
print(f"\nProduct pairs found: {len(basket)}")
