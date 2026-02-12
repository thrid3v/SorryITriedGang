import duckdb

df = duckdb.sql("SELECT * FROM 'data/gold/fact_transactions.parquet' LIMIT 10").df()
print(df)

print("\nProduct key value counts:")
print(df['product_key'].value_counts().head())

print("\nChecking for non--1 product_keys:")
non_neg = duckdb.sql("SELECT COUNT(*) as cnt FROM 'data/gold/fact_transactions.parquet' WHERE product_key != -1").df()
print(f"Rows with product_key != -1: {non_neg['cnt'].iloc[0]}")
