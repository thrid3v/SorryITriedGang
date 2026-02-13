"""
RetailNexus Stream Generator
Simulates real-time online order events for near real-time ingestion.
Generates individual transactions and writes to streaming buffer.
"""
import os
import json
import time
import random
from datetime import datetime
from pathlib import Path

from faker import Faker

fake = Faker()

# Paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
STREAM_DIR = PROJECT_ROOT / "data" / "streaming"
STREAM_BUFFER = STREAM_DIR / "events.jsonl"

# Stable pools to match batch generator
_USER_POOL_SIZE = 50
_PRODUCT_POOL_SIZE = 30
_STORE_IDS = [f"STORE_{i:03d}" for i in range(1, 11)]

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "San Antonio", "Dallas", "San Jose", "Austin", "Jacksonville",
]

PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Books"]


def _ensure_dirs():
    """Create streaming directory if it doesn't exist."""
    STREAM_DIR.mkdir(parents=True, exist_ok=True)


def generate_order_event() -> dict:
    """
    Generate a single order event (1-5 products).
    Returns a dict representing one transaction with multiple line items.
    """
    transaction_id = f"TXN_{int(datetime.now().timestamp() * 1000)}"
    user_id = f"USR_{random.randint(1, _USER_POOL_SIZE):04d}"
    store_id = random.choice(_STORE_IDS)
    timestamp = datetime.now().isoformat()
    
    # 1-5 products per order
    num_products = random.randint(1, 5)
    products = []
    
    for _ in range(num_products):
        product_id = f"PRD_{random.randint(1, _PRODUCT_POOL_SIZE):04d}"
        amount = round(random.uniform(10, 500), 2)
        
        products.append({
            "product_id": product_id,
            "amount": amount
        })
    
    return {
        "transaction_id": transaction_id,
        "user_id": user_id,
        "store_id": store_id,
        "timestamp": timestamp,
        "products": products,
        "event_type": "order_created"
    }


def append_event(event: dict):
    """Append event to streaming buffer (JSONL format)."""
    _ensure_dirs()
    with open(STREAM_BUFFER, 'a') as f:
        f.write(json.dumps(event) + '\n')


def run_stream_generator(interval_seconds: float = 5.0, max_events: int = None):
    """
    Run the stream generator continuously.
    
    Args:
        interval_seconds: Average time between events (default: 5s)
        max_events: Maximum events to generate (None = infinite)
    """
    _ensure_dirs()
    import sys
    # Configure stdout for UTF-8 to handle emoji on Windows
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    
    print(f"[STREAM] Stream Generator started (interval: {interval_seconds}s)")
    
    events_generated = 0
    
    try:
        while True:
            # Generate and append event
            event = generate_order_event()
            append_event(event)
            events_generated += 1
            
            print(f"[EVENT] Event {events_generated}: {event['transaction_id']} "
                  f"({len(event['products'])} products)")
            
            # Stop if max_events reached
            if max_events and events_generated >= max_events:
                print(f"[OK] Reached max events ({max_events})")
                break
            
            # Random delay to simulate realistic inter-arrival times
            delay = random.uniform(interval_seconds * 0.5, interval_seconds * 1.5)
            time.sleep(delay)
            
    except KeyboardInterrupt:
        print(f"\n[STOP] Stream Generator stopped ({events_generated} events generated)")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="RetailNexus Stream Generator")
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Average seconds between events (default: 5.0)"
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Maximum events to generate (default: infinite)"
    )
    args = parser.parse_args()
    
    run_stream_generator(interval_seconds=args.interval, max_events=args.max_events)
