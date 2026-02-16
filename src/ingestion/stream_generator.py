"""
RetailNexus Stream Generator
Simulates real-time retail events: orders, users, products, inventory, and shipments.
Writes all event types to a single streaming buffer (JSONL).
"""
import os
import json
import time
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()

# Paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
STREAM_DIR = PROJECT_ROOT / "data" / "streaming"
STREAM_BUFFER = STREAM_DIR / "events.jsonl"

# Stable pools
_USER_POOL_SIZE = 50
_PRODUCT_POOL_SIZE = 30
_STORE_IDS = [f"STORE_{i:03d}" for i in range(1, 11)]

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "San Antonio", "Dallas", "San Jose", "Austin", "Jacksonville",
]

PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Books"]

PRODUCT_NAMES = {
    "Electronics": ["Wireless Headphones", "Smart Watch", "Bluetooth Speaker", "USB-C Hub", "Power Bank", "Tablet Stand"],
    "Clothing": ["Running Shoes", "Denim Jacket", "Cotton T-Shirt", "Wool Scarf", "Leather Belt", "Sunglasses"],
    "Home": ["LED Desk Lamp", "Coffee Maker", "Air Purifier", "Throw Pillow", "Wall Clock", "Candle Set"],
    "Sports": ["Yoga Mat", "Resistance Bands", "Water Bottle", "Tennis Racket", "Jump Rope", "Gym Bag"],
    "Books": ["Python Handbook", "Data Science Guide", "AI Fundamentals", "ML Engineering", "Cloud Architecture", "DevOps Manual"],
}

CARRIERS = ["FedEx", "UPS", "USPS", "DHL", "Amazon Logistics"]
SHIP_STATUSES = ["shipped", "delivered", "delivered", "delivered", "delayed"]  # weighted toward delivered
STOCK_STATUSES = ["in_stock", "in_stock", "in_stock", "low_stock", "out_of_stock"]


def _ensure_dirs():
    """Create streaming directory if it doesn't exist."""
    STREAM_DIR.mkdir(parents=True, exist_ok=True)


# ── Event Generators ──────────────────────────────────

def generate_order_event() -> dict:
    """Generate a single order event (1-5 products)."""
    transaction_id = f"TXN_{int(datetime.now().timestamp() * 1000)}"
    user_id = f"USR_{random.randint(1, _USER_POOL_SIZE):04d}"
    store_id = random.choice(_STORE_IDS)
    timestamp = datetime.now().isoformat()

    num_products = random.randint(1, 5)
    products = []

    for _ in range(num_products):
        product_id = f"PRD_{random.randint(1, _PRODUCT_POOL_SIZE):04d}"
        amount = round(random.uniform(10, 500), 2)
        products.append({"product_id": product_id, "amount": amount})

    return {
        "event_type": "order_created",
        "transaction_id": transaction_id,
        "user_id": user_id,
        "store_id": store_id,
        "timestamp": timestamp,
        "products": products,
    }


def generate_user_event() -> dict:
    """Generate a user registration/update event."""
    user_id = f"USR_{random.randint(1, _USER_POOL_SIZE):04d}"
    return {
        "event_type": "user_update",
        "user_id": user_id,
        "name": fake.name(),
        "email": fake.email(),
        "city": random.choice(CITIES),
        "signup_date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
    }


def generate_product_event() -> dict:
    """Generate a product catalog event."""
    product_id = f"PRD_{random.randint(1, _PRODUCT_POOL_SIZE):04d}"
    category = random.choice(PRODUCT_CATEGORIES)
    product_name = random.choice(PRODUCT_NAMES[category])
    return {
        "event_type": "product_update",
        "product_id": product_id,
        "product_name": product_name,
        "category": category,
        "price": round(random.uniform(9.99, 499.99), 2),
    }


def generate_inventory_event() -> dict:
    """Generate an inventory snapshot event."""
    product_id = f"PRD_{random.randint(1, _PRODUCT_POOL_SIZE):04d}"
    store_id = random.choice(_STORE_IDS)
    stock_level = random.randint(0, 500)
    reorder_point = random.randint(10, 50)
    stock_status = "out_of_stock" if stock_level == 0 else ("low_stock" if stock_level <= reorder_point else "in_stock")
    return {
        "event_type": "inventory_update",
        "product_id": product_id,
        "store_id": store_id,
        "stock_level": stock_level,
        "reorder_point": reorder_point,
        "last_restock_date": (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
        "stock_status": stock_status,
    }


def generate_shipment_event(transaction_id: str = None) -> dict:
    """Generate a shipment event, optionally tied to a transaction."""
    if not transaction_id:
        transaction_id = f"TXN_{int(datetime.now().timestamp() * 1000) - random.randint(1000, 100000)}"
    
    shipped_date = datetime.now() - timedelta(days=random.randint(1, 14))
    delivery_days = random.randint(1, 10)
    delivered_date = shipped_date + timedelta(days=delivery_days)
    status = random.choice(SHIP_STATUSES)
    
    if status == "delayed":
        delivery_days = random.randint(8, 21)
        delivered_date = shipped_date + timedelta(days=delivery_days)
    
    return {
        "event_type": "shipment_update",
        "shipment_id": f"SHP_{int(datetime.now().timestamp() * 1000)}_{random.randint(100,999)}",
        "transaction_id": transaction_id,
        "origin_store_id": random.choice(_STORE_IDS),
        "dest_store_id": random.choice(_STORE_IDS),
        "shipped_date": shipped_date.strftime("%Y-%m-%d"),
        "delivered_date": delivered_date.strftime("%Y-%m-%d") if status == "delivered" else None,
        "delivery_days": delivery_days if status == "delivered" else None,
        "carrier": random.choice(CARRIERS),
        "tracking_number": fake.bothify("??########"),
        "status": status,
        "shipping_cost": round(random.uniform(5.0, 50.0), 2),
    }


def append_event(event: dict):
    """Append event to streaming buffer (JSONL format)."""
    _ensure_dirs()
    with open(STREAM_BUFFER, 'a') as f:
        f.write(json.dumps(event) + '\n')


def generate_initial_seed_data():
    """
    Generate a burst of seed data on first start so all tabs populate immediately.
    Creates users, products, inventory, and some historical shipments.
    """
    events = []
    
    # Generate all users
    for i in range(1, _USER_POOL_SIZE + 1):
        events.append({
            "event_type": "user_update",
            "user_id": f"USR_{i:04d}",
            "name": fake.name(),
            "email": fake.email(),
            "city": random.choice(CITIES),
            "signup_date": (datetime.now() - timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d"),
        })
    
    # Generate all products
    for i in range(1, _PRODUCT_POOL_SIZE + 1):
        category = random.choice(PRODUCT_CATEGORIES)
        events.append({
            "event_type": "product_update",
            "product_id": f"PRD_{i:04d}",
            "product_name": random.choice(PRODUCT_NAMES[category]),
            "category": category,
            "price": round(random.uniform(9.99, 499.99), 2),
        })
    
    # Generate inventory for each product at each store
    for i in range(1, _PRODUCT_POOL_SIZE + 1):
        for store_id in _STORE_IDS:
            stock_level = random.randint(0, 500)
            reorder_point = random.randint(10, 50)
            stock_status = "out_of_stock" if stock_level == 0 else ("low_stock" if stock_level <= reorder_point else "in_stock")
            events.append({
                "event_type": "inventory_update",
                "product_id": f"PRD_{i:04d}",
                "store_id": store_id,
                "stock_level": stock_level,
                "reorder_point": reorder_point,
                "last_restock_date": (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
                "stock_status": stock_status,
            })
    
    # Generate some shipments
    for _ in range(20):
        events.append(generate_shipment_event())
    
    return events


def generate_burst_data(num_orders: int = 50):
    """
    Generate a burst of diverse data to populate all KPIs.
    Ensures all data types are generated: orders, users, products, inventory, shipments.
    
    Args:
        num_orders: Number of orders to generate in the burst
    """
    events = []
    
    # Generate orders with varied products
    for _ in range(num_orders):
        order = generate_order_event()
        events.append(order)
        
        # 50% chance: generate shipment for this order
        if random.random() < 0.5:
            events.append(generate_shipment_event(order["transaction_id"]))
    
    # Generate some user updates (ensure user diversity)
    for _ in range(10):
        events.append(generate_user_event())
    
    # Generate some product updates
    for _ in range(10):
        events.append(generate_product_event())
    
    # Generate inventory updates for various products/stores
    for _ in range(20):
        events.append(generate_inventory_event())
    
    return events


def run_stream_generator(interval_seconds: float = 5.0, max_events: int = None, burst_on_start: bool = True):
    """
    Run the stream generator continuously.

    Args:
        interval_seconds: Average time between events (default: 5s)
        max_events: Maximum events to generate (None = infinite)
        burst_on_start: Generate burst of data when starting (default: True)
    """
    _ensure_dirs()
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')

    print(f"[STREAM] Stream Generator started (interval: {interval_seconds}s)")
    
    # Seed data: check if we need initial data
    seed_marker = STREAM_DIR / ".seeded"
    if not seed_marker.exists():
        print("[STREAM] Generating initial seed data (users, products, inventory, shipments)...")
        seed_events = generate_initial_seed_data()
        for event in seed_events:
            append_event(event)
        seed_marker.touch()
        print(f"[STREAM] Seeded {len(seed_events)} initial events")
    
    # Generate burst of data on start to populate all KPIs
    if burst_on_start:
        print("[STREAM] Generating burst data to populate all KPIs...")
        burst_events = generate_burst_data(num_orders=50)
        for event in burst_events:
            append_event(event)
        print(f"[STREAM] Generated {len(burst_events)} burst events")

    events_generated = 0

    try:
        while True:
            # Always generate an order
            order = generate_order_event()
            append_event(order)
            events_generated += 1

            # 50% chance: also generate a shipment for this order (increased from 30%)
            if random.random() < 0.5:
                shipment = generate_shipment_event(order["transaction_id"])
                append_event(shipment)
                events_generated += 1

            # 20% chance: inventory update (increased from 10%)
            if random.random() < 0.2:
                append_event(generate_inventory_event())
                events_generated += 1

            # 10% chance: user update (increased from 5%)
            if random.random() < 0.1:
                append_event(generate_user_event())
                events_generated += 1

            # 10% chance: product update (increased from 5%)
            if random.random() < 0.1:
                append_event(generate_product_event())
                events_generated += 1

            print(f"[EVENT] #{events_generated}: {order['transaction_id']} ({len(order['products'])} products)")

            if max_events and events_generated >= max_events:
                print(f"[OK] Reached max events ({max_events})")
                break

            delay = random.uniform(interval_seconds * 0.5, interval_seconds * 1.5)
            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\n[STOP] Stream Generator stopped ({events_generated} events generated)")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="RetailNexus Stream Generator")
    parser.add_argument("--interval", type=float, default=5.0, help="Average seconds between events (default: 5.0)")
    parser.add_argument("--max-events", type=int, default=None, help="Maximum events to generate (default: infinite)")
    parser.add_argument("--burst-on-start", action="store_true", help="Generate burst of data on start to populate all KPIs")
    parser.add_argument("--no-burst", dest="burst_on_start", action="store_false", help="Skip burst data generation")
    parser.set_defaults(burst_on_start=True)
    args = parser.parse_args()

    run_stream_generator(interval_seconds=args.interval, max_events=args.max_events, burst_on_start=args.burst_on_start)
