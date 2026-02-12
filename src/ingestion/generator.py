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
Faker.seed(42)
random.seed(42)

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
    """Generate transaction records with ~5% null amounts and ~3% duplicate rows."""
    user_ids = [f"USR_{i:04d}" for i in range(1, _USER_POOL_SIZE + 1)]
    product_ids = [f"PRD_{i:04d}" for i in range(1, _PRODUCT_POOL_SIZE + 1)]

    rows = []
    for i in range(1, num + 1):
        amount = round(random.uniform(1.0, 1000.0), 2) if random.random() > 0.05 else None
        quantity = random.randint(1, 10) if random.random() > 0.05 else None
        rows.append({
            "transaction_id": f"TXN_{fake.uuid4()[:8].upper()}",
            "user_id": random.choice(user_ids),
            "product_id": random.choice(product_ids),
            "timestamp": fake.date_time_between(
                start_date="-30d", end_date="now"
            ).isoformat(),
            "quantity": quantity,
            "amount": amount,
            "store_id": random.choice(_STORE_IDS),
        })

    df = pd.DataFrame(rows)

    # inject ~3% duplicate rows
    num_dupes = max(1, int(len(df) * 0.03))
    dupes = df.sample(n=num_dupes, random_state=42)
    df = pd.concat([df, dupes], ignore_index=True)

    return df


def generate_transactions(num: int = 100):
    """
    Main entry point — generates timestamped CSVs for transactions, users, and products.
    Some users will have randomised city changes between batches to exercise SCD Type 2.
    """
    _ensure_raw_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── Transactions ──
    txn_df = _generate_transactions(num)
    txn_path = os.path.join(RAW_DIR, f"transactions_{ts}.csv")
    txn_df.to_csv(txn_path, index=False)

    # ── Users (city may differ between batches) ──
    users_df = _generate_users()
    users_path = os.path.join(RAW_DIR, f"users_{ts}.csv")
    users_df.to_csv(users_path, index=False)

    # ── Products ──
    products_df = _generate_products()
    products_path = os.path.join(RAW_DIR, f"products_{ts}.csv")
    products_df.to_csv(products_path, index=False)

    print(f"✅ Generated {len(txn_df)} transactions → {txn_path}")
    print(f"   📊 Stats: {txn_df.isnull().sum().sum()} NULLs, {txn_df.duplicated().sum()} duplicates")
    print(f"[Ingestion] Wrote {len(users_df)} users → {users_path}")
    print(f"[Ingestion] Wrote {len(products_df)} products → {products_path}")

    return txn_path, users_path, products_path


if __name__ == "__main__":
    print("🚀 Starting Data Generator...")
    generate_transactions(num=200)
    print("✅ Generation complete!")
