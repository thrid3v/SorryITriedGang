import duckdb

conn = duckdb.connect()

# Check fact_transactions schema
print("=== fact_transactions schema ===")
try:
    result = conn.execute("""
        SELECT * FROM read_parquet('data/gold/fact_transactions/**/*.parquet', hive_partitioning=true) 
        LIMIT 0
    """)
    for col in result.description:
        print(f"  - {col[0]} ({col[1]})")
except Exception as e:
    print(f"Error: {e}")

# Check dim_users schema
print("\n=== dim_users schema ===")
try:
    result = conn.execute("SELECT * FROM 'data/gold/dim_users.parquet' LIMIT 0")
    for col in result.description:
        print(f"  - {col[0]} ({col[1]})")
except Exception as e:
    print(f"Error: {e}")

# Check dim_products schema
print("\n=== dim_products schema ===")
try:
    result = conn.execute("SELECT * FROM 'data/gold/dim_products.parquet' LIMIT 0")
    for col in result.description:
        print(f"  - {col[0]} ({col[1]})")
except Exception as e:
    print(f"Error: {e}")

# Check dim_stores schema
print("\n=== dim_stores schema ===")
try:
    result = conn.execute("SELECT * FROM 'data/gold/dim_stores.parquet' LIMIT 0")
    for col in result.description:
        print(f"  - {col[0]} ({col[1]})")
except Exception as e:
    print(f"Error: {e}")

conn.close()
