"""
Quick verification script for RetailNexus Gold layer.
"""
import duckdb

print("=" * 60)
print("  GOLD LAYER VERIFICATION")
print("=" * 60)

tables = ['dim_users', 'dim_products', 'dim_stores', 'dim_dates', 'fact_transactions']

print("\n--- Table Row Counts ---")
for t in tables:
    count = duckdb.sql(f"SELECT COUNT(*) FROM 'data/gold/{t}.parquet'").fetchone()[0]
    print(f"{t:20s}: {count:5d} rows")

print("\n" + "=" * 60)
print("  dim_users (SCD Type 2 Sample)")
print("=" * 60)
df_users = duckdb.sql("""
    SELECT surrogate_key, user_id, city, effective_date, end_date, is_current
    FROM 'data/gold/dim_users.parquet'
    ORDER BY user_id, effective_date
    LIMIT 10
""").df()
print(df_users.to_string(index=False))

print("\n" + "=" * 60)
print("  fact_transactions Sample")
print("=" * 60)
df_fact = duckdb.sql("""
    SELECT transaction_id, user_key, product_key, store_key, date_key, amount
    FROM 'data/gold/fact_transactions.parquet'
    LIMIT 5
""").df()
print(df_fact.to_string(index=False))

print("\n" + "=" * 60)
print("  SCD Type 2 Verification")
print("=" * 60)
# Check for users with history (multiple versions)
history_check = duckdb.sql("""
    SELECT 
        user_id,
        COUNT(*) as version_count,
        SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_count
    FROM 'data/gold/dim_users.parquet'
    GROUP BY user_id
    HAVING COUNT(*) > 1
    LIMIT 5
""").df()

if len(history_check) > 0:
    print("Users with history (SCD Type 2 working):")
    print(history_check.to_string(index=False))
else:
    print("No users with history yet (first run)")

print("\n✅ Verification complete!")
