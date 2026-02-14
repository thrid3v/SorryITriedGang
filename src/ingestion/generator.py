"""
RetailNexus Schema-Driven Data Generator
=========================================
Generates retail data based on business context schema definitions.
Adapts to any retail vertical (bakery, clothing, etc.) without hardcoding.
"""
import os
import random
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
from faker import Faker

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.field_generator import FieldGenerator

# Initialize Faker with time-based seed for unique data each run
fake = Faker()
_seed = int(datetime.now().timestamp() * 1000) % (2**32)
Faker.seed(_seed)
random.seed(_seed)

RAW_DIR = PROJECT_ROOT / "data" / "raw"


def _ensure_raw_dir():
    """Create raw data directory if it doesn't exist."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def load_business_context() -> Dict[str, Any]:
    """Load active business context from config."""
    config_file = PROJECT_ROOT / "config" / "business_contexts.json"
    with open(config_file, 'r') as f:
        data = json.load(f)
    
    active = data.get("active_context", "retail_general")
    return data["contexts"][active]


def generate_table(
    table_name: str,
    table_config: Dict[str, Any],
    field_gen: FieldGenerator,
    fk_pools: Dict[str, List[Any]] = None
) -> pd.DataFrame:
    """
    Generate a table based on schema configuration.
    
    Args:
        table_name: Name of the table
        table_config: Table configuration from schema
        field_gen: FieldGenerator instance
        fk_pools: Dictionary of foreign key pools
    
    Returns:
        DataFrame with generated data
    """
    fk_pools = fk_pools or {}
    fields = table_config["fields"]
    
    # Determine number of rows
    if table_name == "products":
        num_rows = table_config.get("pool_size", 30)
    elif table_name == "users":
        num_rows = table_config.get("pool_size", 50)
    elif table_name == "transactions":
        # Transactions are special - multiple products per transaction
        return generate_transactions(table_config, field_gen, fk_pools)
    elif table_name == "inventory":
        # Inventory is product x store combinations
        return generate_inventory(table_config, field_gen, fk_pools)
    elif table_name == "shipments":
        num_rows = table_config.get("num_shipments", 50)
    else:
        num_rows = 100
    
    # Generate rows
    rows = []
    for i in range(1, num_rows + 1):
        row = {}
        for field_name, field_config in fields.items():
            if field_config["type"] == "fk":
                # Handle foreign keys
                ref = field_config["references"]
                if ref in fk_pools and fk_pools[ref]:
                    row[field_name] = random.choice(fk_pools[ref])
                else:
                    row[field_name] = None
            else:
                row[field_name] = field_gen.generate(field_name, field_config, row_index=i)
        rows.append(row)
    
    return pd.DataFrame(rows)


def generate_transactions(
    table_config: Dict[str, Any],
    field_gen: FieldGenerator,
    fk_pools: Dict[str, List[Any]]
) -> pd.DataFrame:
    """
    Generate transaction line items (multiple products per transaction).
    
    Args:
        table_config: Transaction table configuration
        field_gen: FieldGenerator instance
        fk_pools: Foreign key pools
    
    Returns:
        DataFrame with transaction line items
    """
    num_transactions = table_config.get("num_transactions", 100)
    products_per_txn = table_config.get("products_per_transaction", [1, 5])
    fields = table_config["fields"]
    
    rows = []
    for txn_num in range(1, num_transactions + 1):
        # Generate transaction ID
        txn_id_config = fields["transaction_id"]
        txn_id = field_gen.generate("transaction_id", txn_id_config, row_index=txn_num)
        
        # Generate common transaction fields
        user_id = random.choice(fk_pools.get("users.user_id", ["USR_0001"]))
        timestamp = field_gen.generate("timestamp", fields["timestamp"])
        store_id = field_gen.generate("store_id", fields["store_id"])
        
        # Generate 1-5 product line items
        num_products = random.randint(products_per_txn[0], products_per_txn[1])
        for _ in range(num_products):
            product_id = random.choice(fk_pools.get("products.product_id", ["PRD_0001"]))
            amount = field_gen.generate("amount", fields["amount"])
            
            rows.append({
                "transaction_id": txn_id,
                "user_id": user_id,
                "product_id": product_id,
                "timestamp": timestamp,
                "amount": amount,
                "store_id": store_id,
            })
    
    df = pd.DataFrame(rows)
    
    # Inject duplicates
    duplicate_rate = table_config.get("duplicate_rate", 0.02)
    if len(df) > 10 and duplicate_rate > 0:
        num_dupes = max(1, int(len(df) * duplicate_rate))
        dupes = df.sample(n=num_dupes)
        df = pd.concat([df, dupes], ignore_index=True)
    
    return df


def generate_inventory(
    table_config: Dict[str, Any],
    field_gen: FieldGenerator,
    fk_pools: Dict[str, List[Any]]
) -> pd.DataFrame:
    """
    Generate inventory records (product x store combinations).
    
    Args:
        table_config: Inventory table configuration
        field_gen: FieldGenerator instance
        fk_pools: Foreign key pools
    
    Returns:
        DataFrame with inventory snapshots
    """
    fields = table_config["fields"]
    products = fk_pools.get("products.product_id", [])
    
    # Get store options from config
    store_options = fields["store_id"]["options"]
    
    rows = []
    for product_id in products:
        for store_id in store_options:
            row = {
                "product_id": product_id,
                "store_id": store_id,
            }
            
            # Generate other fields
            for field_name, field_config in fields.items():
                if field_name not in ["product_id", "store_id"]:
                    if field_config["type"] == "fk":
                        continue
                    row[field_name] = field_gen.generate(field_name, field_config)
            
            # Calculate stock status based on stock level
            if "stock_level" in row and "stock_status" in fields:
                stock_level = row.get("stock_level", 0)
                reorder_point = row.get("reorder_point", 50)
                
                if stock_level == 0:
                    row["stock_status"] = "out_of_stock"
                elif stock_level <= reorder_point:
                    row["stock_status"] = "low_stock"
                else:
                    # Use first option that's not out_of_stock or low_stock
                    options = fields["stock_status"]["options"]
                    valid_options = [o for o in options if o not in ["out_of_stock", "low_stock"]]
                    if valid_options:
                        row["stock_status"] = valid_options[0]
            
            rows.append(row)
    
    return pd.DataFrame(rows)


def main(num_transactions: int = 100, context_name: str = None):
    """
    Generate all data files based on business context schema.
    
    Args:
        num_transactions: Number of transactions to generate
        context_name: Optional context name to override active context
    """
    # Configure stdout for UTF-8
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    
    _ensure_raw_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Load business context
    if context_name:
        # Load specific context
        config_file = PROJECT_ROOT / "config" / "business_contexts.json"
        with open(config_file, 'r') as f:
            data = json.load(f)
        context = data["contexts"][context_name]
    else:
        context = load_business_context()
    
    schema = context.get("schema", {})
    if not schema:
        print(f"[ERROR] No schema defined for context: {context['name']}")
        return
    
    print(f"[START] Generating data for: {context['name']}")
    print(f"[INFO] Schema tables: {', '.join(schema.keys())}")
    
    field_gen = FieldGenerator(fake)
    fk_pools = {}
    
    # Generate tables in dependency order
    table_order = ["users", "products", "transactions", "inventory", "shipments"]
    
    for table_name in table_order:
        if table_name not in schema:
            continue
        
        table_config = schema[table_name]
        
        # Override num_transactions if specified
        if table_name == "transactions" and num_transactions:
            table_config["num_transactions"] = num_transactions
        
        # Generate table
        df = generate_table(table_name, table_config, field_gen, fk_pools)
        
        # Save to CSV
        file_path = RAW_DIR / f"{table_name}_{timestamp}.csv"
        df.to_csv(file_path, index=False)
        
        # Collect foreign key pools
        id_fields = [f for f, cfg in table_config["fields"].items() if cfg.get("type") == "id"]
        for id_field in id_fields:
            if id_field in df.columns:
                fk_key = f"{table_name}.{id_field}"
                fk_pools[fk_key] = df[id_field].unique().tolist()
        
        # Print stats
        null_counts = df.isnull().sum()
        null_fields = null_counts[null_counts > 0]
        
        print(f"[OK] Generated {len(df)} {table_name} → {file_path.name}")
        if len(null_fields) > 0:
            print(f"     Nulls: {dict(null_fields)}")
    
    print(f"[OK] Generation complete for {context['name']}!")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="RetailNexus Schema-Driven Data Generator")
    parser.add_argument(
        "--num-transactions",
        type=int,
        default=100,
        help="Number of transactions to generate (default: 100)",
    )
    parser.add_argument(
        "--context",
        type=str,
        default=None,
        help="Business context to use (default: active context from config)",
    )
    args = parser.parse_args()
    
    main(num_transactions=args.num_transactions, context_name=args.context)
