"""
Schema Inspector - Runtime DuckDB Introspection
================================================
Dynamically discovers and inspects Gold Layer tables without hardcoding.
Extracts schema information and sample data for LLM context injection.
"""
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any
import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"


def load_business_context() -> dict:
    """Load the active business context configuration."""
    context_file = CONFIG_DIR / "business_contexts.json"
    with open(context_file, 'r') as f:
        contexts = json.load(f)
    
    active_name = contexts.get("active_context", "retail_general")
    return contexts["contexts"][active_name]


def discover_tables(gold_layer_path: str) -> Dict[str, str]:
    """
    Auto-discover all parquet tables in the Gold Layer.
    
    Args:
        gold_layer_path: Path to Gold Layer directory (e.g., "data/gold")
    
    Returns:
        Dictionary mapping table names to their DuckDB read paths
        Example: {"fact_transactions": "read_parquet('data/gold/fact_transactions.parquet/**/*.parquet', hive_partitioning=true)"}
    """
    gold_path = PROJECT_ROOT / gold_layer_path
    tables = {}
    
    if not gold_path.exists():
        print(f"[WARN] Gold Layer path does not exist: {gold_path}")
        return tables
    
    # Discover all parquet files/directories
    for item in gold_path.iterdir():
        if item.name.startswith('.') or item.name.startswith('_'):
            continue
        
        table_name = item.stem  # Remove .parquet extension if present
        
        if item.is_dir():
            # Partitioned table (e.g., fact_transactions.parquet/)
            parquet_path = str(item / "**" / "*.parquet").replace("\\", "/")
            tables[table_name] = f"read_parquet('{parquet_path}', hive_partitioning=true)"
        elif item.suffix == '.parquet':
            # Single parquet file (e.g., dim_users.parquet)
            parquet_path = str(item).replace("\\", "/")
            tables[table_name] = f"'{parquet_path}'"
    
    return tables


def inspect_schema_with_samples(gold_layer_path: str, sample_rows: int = 5) -> Dict[str, Any]:
    """
    Introspect all tables in Gold Layer and extract schema + sample data.
    
    Args:
        gold_layer_path: Path to Gold Layer directory
        sample_rows: Number of sample rows to extract per table
    
    Returns:
        Dictionary with schema information:
        {
            "table_name": {
                "columns": [("col_name", "TYPE"), ...],
                "samples": [{row1}, {row2}, ...],
                "row_count": 1234,
                "path": "read_parquet(...)"
            }
        }
    """
    conn = duckdb.connect()
    schema_info = {}
    
    # Auto-discover tables
    tables = discover_tables(gold_layer_path)
    
    for table_name, table_path in tables.items():
        try:
            # Get schema (0 rows, just structure)
            result = conn.execute(f"SELECT * FROM {table_path} LIMIT 0")
            columns = [(col[0], str(col[1]).upper()) for col in result.description]
            
            # Get sample rows
            samples_df = conn.execute(f"SELECT * FROM {table_path} LIMIT {sample_rows}").fetchdf()
            
            # Get total row count (approximate for partitioned tables)
            try:
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_path}").fetchone()
                row_count = count_result[0] if count_result else 0
            except:
                row_count = len(samples_df)  # Fallback to sample size
            
            # Convert samples to dict, handling datetime/date types
            samples = samples_df.to_dict(orient='records')
            for sample in samples:
                for key, value in sample.items():
                    if pd.isna(value):
                        sample[key] = None
                    elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
                        sample[key] = str(value)
            
            schema_info[table_name] = {
                "columns": columns,
                "samples": samples,
                "row_count": row_count,
                "path": table_path
            }
            
        except Exception as e:
            print(f"[WARN] Could not introspect {table_name}: {e}")
            continue
    
    conn.close()
    return schema_info


def build_schema_prompt(schema_info: Dict[str, Any], context: dict) -> str:
    """
    Build a comprehensive schema description for the LLM.
    
    Args:
        schema_info: Output from inspect_schema_with_samples()
        context: Business context from business_contexts.json
    
    Returns:
        Formatted schema prompt string
    """
    parts = []
    
    # Business context header
    parts.append(f"BUSINESS CONTEXT: {context['name']}")
    parts.append(f"Description: {context['description']}")
    parts.append(f"Domain: {', '.join(context['domain_hints']['product_types'])}")
    parts.append(f"Key Metrics: {', '.join(context['domain_hints']['key_metrics'])}")
    parts.append("\nYou have access to the following tables in this data warehouse:\n")
    
    # Table schemas with samples
    for table_name, info in schema_info.items():
        parts.append(f"\n{'='*60}")
        parts.append(f"TABLE: {table_name}")
        parts.append(f"Rows: {info['row_count']:,}")
        parts.append(f"Path: {info['path']}")
        parts.append("\nColumns:")
        
        for col_name, col_type in info['columns']:
            parts.append(f"  - {col_name} ({col_type})")
        
        # Sample data
        if info['samples']:
            parts.append(f"\nSample Data (first {len(info['samples'])} rows):")
            for i, sample in enumerate(info['samples'], 1):
                parts.append(f"  Row {i}: {sample}")
    
    # Important rules
    parts.append(f"\n{'='*60}")
    parts.append("\nIMPORTANT RULES:")
    parts.append("- Always use table aliases for clarity")
    parts.append("- Use the exact table paths shown above in your queries")
    parts.append("- For partitioned tables (fact_*), use read_parquet() with hive_partitioning=true")
    parts.append("- For dimension tables (dim_*), use the direct parquet file paths")
    parts.append(f"- All monetary values are in {context['business_rules']['currency']}")
    parts.append(f"- Timestamps are in {context['business_rules']['timezone']}")
    parts.append("- Limit results to 100 rows maximum unless specifically asked for more")
    parts.append("- When querying dim_users, filter by is_current = TRUE to get current records only")
    
    return "\n".join(parts)


# CLI test
if __name__ == "__main__":
    print("=== Schema Inspector Test ===\n")
    
    # Load context
    context = load_business_context()
    print(f"Active Context: {context['name']}")
    print(f"Gold Layer: {context['gold_layer_path']}\n")
    
    # Discover tables
    tables = discover_tables(context['gold_layer_path'])
    print(f"Discovered {len(tables)} tables:")
    for name, path in tables.items():
        print(f"  - {name}: {path}")
    
    # Inspect schema
    print("\n=== Inspecting Schema ===\n")
    schema_info = inspect_schema_with_samples(context['gold_layer_path'], sample_rows=3)
    
    for table_name, info in schema_info.items():
        print(f"\n{table_name}:")
        print(f"  Columns: {len(info['columns'])}")
        print(f"  Rows: {info['row_count']:,}")
        print(f"  Sample: {len(info['samples'])} rows")
    
    # Build prompt
    print("\n=== Schema Prompt (first 500 chars) ===\n")
    prompt = build_schema_prompt(schema_info, context)
    print(prompt[:500] + "...")
