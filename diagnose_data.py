import duckdb
import os
import glob

def check_status():
    print("--- RAW LAYER ---")
    raw_files = glob.glob("data/raw/transactions_*.csv")
    print(f"Transaction CSVs: {len(raw_files)}")
    
    print("\n--- SILVER LAYER ---")
    silver_file = "data/silver/transactions.parquet"
    if os.path.exists(silver_file):
        count = duckdb.sql(f"SELECT COUNT(*) FROM '{silver_file}'").fetchone()[0]
        print(f"Transactions: {count}")
    else:
        print("Silver transactions missing")

    print("\n--- GOLD LAYER ---")
    gold_dir = "data/gold/fact_transactions.parquet"
    if os.path.exists(gold_dir):
        count = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{gold_dir}/**/*.parquet', hive_partitioning=true)").fetchone()[0]
        print(f"Fact Transactions: {count}")
    else:
        print("Gold fact_transactions missing")

if __name__ == "__main__":
    check_status()
