"""
RetailNexus Data Generator
Generates dirty retail transaction, user, and product CSVs to data/raw/.
Intentionally injects nulls and duplicates to test pipeline resilience.
"""
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()
# Use time-based seeds so each run produces unique data
_seed = int(datetime.now().timestamp() * 1000) % (2**32)
Faker.seed(_seed)
random.seed(_seed)

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")

# ── Stable pools so user_ids / product_ids are consistent across batches ──
_USER_POOL_SIZE = 50
_PRODUCT_POOL_SIZE = 30
_STORE_IDS = [f"STORE_{i:03d}" for i in range(1, 11)]

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "San Antonio", "Dallas", "San Jose", "Austin", "Jacksonville",
]

CATEGORIES = ["Electronics", "Clothing", "Grocery", "Home & Garden", "Sports"]

PRODUCT_NAMES = [
    "Wireless Earbuds", "Running Shoes", "Organic Milk", "LED Desk Lamp",
    "Yoga Mat", "Smartphone Case", "Cotton T-Shirt", "Protein Bars",
    "Throw Pillow", "Tennis Racket", "Bluetooth Speaker", "Denim Jeans",
    "Olive Oil", "Wall Clock", "Resistance Bands", "Screen Protector",
    "Winter Jacket", "Green Tea", "Scented Candle", "Soccer Ball",
    "Laptop Stand", "Wool Socks", "Almond Butter", "Bookshelf",
    "Jump Rope", "USB-C Cable", "Flannel Shirt", "Honey",
    "Desk Organizer", "Basketball",
]


def _ensure_raw_dir():
    os.makedirs(RAW_DIR, exist_ok=True)


def _generate_users(num_users: int = _USER_POOL_SIZE) -> pd.DataFrame:
    """Generate user records with ~3% null cities and occasional city changes."""
    rows = []
    for i in range(1, num_users + 1):
        city = random.choice(CITIES) if random.random() > 0.03 else None
        rows.append({
            "user_id": f"USR_{i:04d}",
            "name": fake.name(),
            "email": fake.email(),
            "city": city,
            "signup_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
        })
    return pd.DataFrame(rows)


def _generate_products(num_products: int = _PRODUCT_POOL_SIZE) -> pd.DataFrame:
    """Generate product records with ~2% null prices."""
    rows = []
    for i in range(1, num_products + 1):
        price = round(random.uniform(5.0, 500.0), 2) if random.random() > 0.02 else None
        rows.append({
            "product_id": f"PRD_{i:04d}",
            "product_name": PRODUCT_NAMES[i - 1] if i <= len(PRODUCT_NAMES) else fake.word(),
            "category": random.choice(CATEGORIES),
            "price": price,
        })
    return pd.DataFrame(rows)


def _generate_transactions(num: int = 100) -> pd.DataFrame:
    """
    Generate transaction line items.
    Each transaction = 1-5 products.
    Intentionally inject ~5% null amounts and ~2% duplicates.
    """
    rows = []
    txn_id_counter = 1
    start_date = datetime.now() - timedelta(days=30)

    for _ in range(num):
        txn_id = f"TXN_{txn_id_counter:06d}"
        user_id = f"USR_{random.randint(1, _USER_POOL_SIZE):04d}"
        store_id = random.choice(_STORE_IDS)
        timestamp = start_date + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        # Each transaction has 1-5 products
        num_products = random.randint(1, 5)
        for _ in range(num_products):
            product_id = f"PRD_{random.randint(1, _PRODUCT_POOL_SIZE):04d}"
            amount = round(random.uniform(10.0, 500.0), 2) if random.random() > 0.05 else None

            rows.append({
                "transaction_id": txn_id,
                "user_id": user_id,
                "product_id": product_id,
                "timestamp": timestamp.isoformat(),
                "amount": amount,
                "store_id": store_id,
            })

        txn_id_counter += 1

    df = pd.DataFrame(rows)

    # Inject ~2% duplicates
    if len(df) > 10:
        num_dupes = max(1, int(len(df) * 0.02))
        dupes = df.sample(n=num_dupes)
        df = pd.concat([df, dupes], ignore_index=True)

    return df


def _generate_inventory() -> pd.DataFrame:
    """
    Generate inventory snapshots for each product at each store.
    Includes stock levels, reorder points, and last restock date.
    """
    rows = []
    for product_id in [f"PRD_{i:04d}" for i in range(1, _PRODUCT_POOL_SIZE + 1)]:
        for store_id in _STORE_IDS:
            stock_level = random.randint(0, 500)
            reorder_point = random.randint(20, 100)
            last_restock = fake.date_between(start_date="-60d", end_date="today")
            
            # Determine stock status
            if stock_level == 0:
                status = "out_of_stock"
            elif stock_level <= reorder_point:
                status = "low_stock"
            else:
                status = "in_stock"
            
            rows.append({
                "product_id": product_id,
                "store_id": store_id,
                "stock_level": stock_level,
                "reorder_point": reorder_point,
                "last_restock_date": last_restock.isoformat(),
                "stock_status": status,
            })
    
    return pd.DataFrame(rows)


def _generate_shipments(num_shipments: int = 50) -> pd.DataFrame:
    """
    Generate shipment records for order fulfillment.
    Includes shipping dates, delivery dates, carriers, and tracking.
    """
    rows = []
    carriers = ["FedEx", "UPS", "USPS", "DHL", "Amazon Logistics"]
    statuses = ["pending", "in_transit", "delivered", "delayed"]
    
    for i in range(1, num_shipments + 1):
        shipment_id = f"SHIP_{i:06d}"
        transaction_id = f"TXN_{random.randint(1, 200):06d}"  # Reference to transaction
        origin_store = random.choice(_STORE_IDS)
        dest_store = random.choice([s for s in _STORE_IDS if s != origin_store])
        
        shipped_date = fake.date_between(start_date="-30d", end_date="today")
        delivery_days = random.randint(1, 10)
        delivered_date = shipped_date + timedelta(days=delivery_days)
        
        carrier = random.choice(carriers)
        tracking_number = fake.bothify(text="??########")
        status = random.choice(statuses)
        shipping_cost = round(random.uniform(5.0, 50.0), 2)
        
        rows.append({
            "shipment_id": shipment_id,
            "transaction_id": transaction_id,
            "origin_store_id": origin_store,
            "dest_store_id": dest_store,
            "shipped_date": shipped_date.isoformat(),
            "delivered_date": delivered_date.isoformat() if status == "delivered" else None,
            "delivery_days": delivery_days if status == "delivered" else None,
            "carrier": carrier,
            "tracking_number": tracking_number,
            "status": status,
            "shipping_cost": shipping_cost,
        })
    
    return pd.DataFrame(rows)


def main(num_transactions: int = 100):
    """Generate all data files."""
    import sys
    # Configure stdout for UTF-8 to handle emoji on Windows
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    
    _ensure_raw_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Generate transactions
    print("[START] Starting Data Generator...")
    txn_df = _generate_transactions(num_transactions)
    txn_path = os.path.join(RAW_DIR, f"transactions_{timestamp}.csv")
    txn_df.to_csv(txn_path, index=False)
    null_count = txn_df["amount"].isna().sum()
    dupe_count = txn_df.duplicated(subset=["transaction_id", "product_id"]).sum()
    print(f"[OK] Generated {num_transactions} transactions -> {txn_path}")
    print(f"     Stats: {null_count} NULLs, {dupe_count} duplicates")

    # Generate users
    users_df = _generate_users()
    users_path = os.path.join(RAW_DIR, f"users_{timestamp}.csv")
    users_df.to_csv(users_path, index=False)
    print(f"[Ingestion] Wrote {len(users_df)} users -> {users_path}")

    # Generate products
    products_df = _generate_products()
    products_path = os.path.join(RAW_DIR, f"products_{timestamp}.csv")
    products_df.to_csv(products_path, index=False)
    print(f"[Ingestion] Wrote {len(products_df)} products -> {products_path}")

    # Generate inventory
    inventory_df = _generate_inventory()
    inventory_path = os.path.join(RAW_DIR, f"inventory_{timestamp}.csv")
    inventory_df.to_csv(inventory_path, index=False)
    print(f"[Ingestion] Wrote {len(inventory_df)} inventory records -> {inventory_path}")

    # Generate shipments
    shipments_df = _generate_shipments(num_shipments=max(10, num_transactions // 2))
    shipments_path = os.path.join(RAW_DIR, f"shipments_{timestamp}.csv")
    shipments_df.to_csv(shipments_path, index=False)
    print(f"[Ingestion] Wrote {len(shipments_df)} shipments -> {shipments_path}")

    print("[OK] Generation complete!")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="RetailNexus Data Generator")
    parser.add_argument(
        "--num-transactions",
        type=int,
        default=100,
        help="Number of transactions to generate (default: 100)",
    )
    args = parser.parse_args()

    main(num_transactions=args.num_transactions)
