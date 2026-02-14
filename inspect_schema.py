import duckdb

conn = duckdb.connect()

print("=" * 60)
print("FACT_TRANSACTIONS SCHEMA:")
print("=" * 60)
result = conn.execute("SELECT * FROM read_parquet('data/gold/fact_transactions.parquet/**/*.parquet', hive_partitioning=true) LIMIT 0")
for col in result.description:
    print(f"{col[0]}: {col[1]}")

print("\n" + "=" * 60)
print("DIM_PRODUCTS SCHEMA:")
print("=" * 60)
result = conn.execute("SELECT * FROM 'data/gold/dim_products.parquet' LIMIT 0")
for col in result.description:
    print(f"{col[0]}: {col[1]}")

print("\n" + "=" * 60)
print("DIM_USERS SCHEMA:")
print("=" * 60)
result = conn.execute("SELECT * FROM 'data/gold/dim_users.parquet' LIMIT 0")
for col in result.description:
    print(f"{col[0]}: {col[1]}")

print("\n" + "=" * 60)
print("DIM_STORES SCHEMA:")
print("=" * 60)
result = conn.execute("SELECT * FROM 'data/gold/dim_stores.parquet' LIMIT 0")
for col in result.description:
    print(f"{col[0]}: {col[1]}")

conn.close()
