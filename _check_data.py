import duckdb
from pathlib import Path

# Use relative paths
PROJECT_ROOT = Path(__file__).resolve().parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
glob_path = str(RAW_DIR / "transactions_*.csv").replace("\\", "/")

# What columns does DuckDB see?
cols = duckdb.sql(f"""
    SELECT column_name FROM (
        DESCRIBE SELECT * FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true)
    )
""").fetchall()
print("Combined columns:", [c[0] for c in cols])

# Check total row count
total = duckdb.sql(f"SELECT COUNT(*) FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true)").fetchone()[0]
print(f"Total rows across all CSVs: {total}")

# Check which CSVs have which columns
import glob
for f in sorted(RAW_DIR.glob("transactions_*.csv")):
    try:
        f_path = str(f).replace("\\", "/")
        c = duckdb.sql(f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_csv('{f_path}', auto_detect=true))").fetchall()
        cnt = duckdb.sql(f"SELECT COUNT(*) FROM read_csv('{f_path}', auto_detect=true)").fetchone()[0]
        print(f"\n{f.name}: {cnt} rows, cols={[x[0] for x in c]}")
    except Exception as e:
        print(f"\n{f.name}: ERROR {e}")
