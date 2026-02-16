"""
RetailNexus Stream Processor
Watches streaming buffer and processes events in micro-batches.
Handles all event types: orders, users, products, inventory, shipments.
Writes to Bronze CSVs and triggers the full transformation pipeline.
"""
import os
import json
import time
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict

# Paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
STREAM_DIR = PROJECT_ROOT / "data" / "streaming"
STREAM_BUFFER = STREAM_DIR / "events.jsonl"
PROCESSED_MARKER = STREAM_DIR / "last_processed.txt"
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def _ensure_dirs():
    """Create necessary directories."""
    STREAM_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def get_last_processed_line() -> int:
    if PROCESSED_MARKER.exists():
        with open(PROCESSED_MARKER, 'r') as f:
            return int(f.read().strip())
    return 0


def set_last_processed_line(line_num: int):
    with open(PROCESSED_MARKER, 'w') as f:
        f.write(str(line_num))


def read_new_events() -> List[Dict]:
    """Read new events from streaming buffer since last processed line."""
    if not STREAM_BUFFER.exists():
        return []

    last_processed = get_last_processed_line()
    events = []

    with open(STREAM_BUFFER, 'r') as f:
        for i, line in enumerate(f, start=1):
            if i > last_processed:
                try:
                    event = json.loads(line.strip())
                    events.append(event)
                except json.JSONDecodeError:
                    print(f"[WARN] Skipping malformed event at line {i}")

    return events


def write_events_to_csv(events: List[Dict]):
    """
    Route events by type and write to appropriate Bronze CSV files.
    """
    if not events:
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Group events by type
    orders = [e for e in events if e.get("event_type") == "order_created"]
    users = [e for e in events if e.get("event_type") == "user_update"]
    products = [e for e in events if e.get("event_type") == "product_update"]
    inventory = [e for e in events if e.get("event_type") == "inventory_update"]
    shipments = [e for e in events if e.get("event_type") == "shipment_update"]

    # ── Transactions ──
    if orders:
        txn_rows = []
        for event in orders:
            for product in event.get("products", []):
                txn_rows.append({
                    'transaction_id': event['transaction_id'],
                    'user_id': event['user_id'],
                    'product_id': product['product_id'],
                    'timestamp': event['timestamp'],
                    'amount': product['amount'],
                    'store_id': event['store_id']
                })
        if txn_rows:
            _write_csv(f"transactions_stream_{timestamp}.csv", txn_rows,
                       ['transaction_id', 'user_id', 'product_id', 'timestamp', 'amount', 'store_id'])
            print(f"  [CSV] {len(txn_rows)} transaction rows")

    # ── Users ──
    if users:
        user_rows = [{
            'user_id': e['user_id'],
            'name': e['name'],
            'email': e['email'],
            'city': e['city'],
            'signup_date': e['signup_date'],
        } for e in users]
        _write_csv(f"users_stream_{timestamp}.csv", user_rows,
                   ['user_id', 'name', 'email', 'city', 'signup_date'])
        print(f"  [CSV] {len(user_rows)} user rows")

    # ── Products ──
    if products:
        prod_rows = [{
            'product_id': e['product_id'],
            'product_name': e['product_name'],
            'category': e['category'],
            'price': e['price'],
        } for e in products]
        _write_csv(f"products_stream_{timestamp}.csv", prod_rows,
                   ['product_id', 'product_name', 'category', 'price'])
        print(f"  [CSV] {len(prod_rows)} product rows")

    # ── Inventory ──
    if inventory:
        inv_rows = [{
            'product_id': e['product_id'],
            'store_id': e['store_id'],
            'stock_level': e['stock_level'],
            'reorder_point': e['reorder_point'],
            'last_restock_date': e['last_restock_date'],
            'stock_status': e['stock_status'],
        } for e in inventory]
        _write_csv(f"inventory_stream_{timestamp}.csv", inv_rows,
                   ['product_id', 'store_id', 'stock_level', 'reorder_point', 'last_restock_date', 'stock_status'])
        print(f"  [CSV] {len(inv_rows)} inventory rows")

    # ── Shipments ──
    if shipments:
        ship_rows = [{
            'shipment_id': e['shipment_id'],
            'transaction_id': e['transaction_id'],
            'origin_store_id': e['origin_store_id'],
            'dest_store_id': e['dest_store_id'],
            'shipped_date': e['shipped_date'],
            'delivered_date': e.get('delivered_date', ''),
            'delivery_days': e.get('delivery_days', ''),
            'carrier': e['carrier'],
            'tracking_number': e['tracking_number'],
            'status': e['status'],
            'shipping_cost': e['shipping_cost'],
        } for e in shipments]
        _write_csv(f"shipments_stream_{timestamp}.csv", ship_rows,
                   ['shipment_id', 'transaction_id', 'origin_store_id', 'dest_store_id',
                    'shipped_date', 'delivered_date', 'delivery_days', 'carrier',
                    'tracking_number', 'status', 'shipping_cost'])
        print(f"  [CSV] {len(ship_rows)} shipment rows")


def _write_csv(filename: str, rows: List[Dict], fieldnames: List[str]):
    """Write rows to a CSV file in the raw directory."""
    filepath = RAW_DIR / filename
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def trigger_incremental_pipeline():
    """
    Run the full transformation pipeline:
      1. clean_all  — picks up all new raw CSVs and writes Silver parquets
      2. apply_scd_type_2 — builds dim_users with SCD logic
      3. build_star_schema — builds all Gold dimensions and facts
      4. Clear KPI cache so new data is immediately available
    """
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))

    try:
        from src.transformation.cleaner import clean_all
        print("[PIPELINE] Running cleaner (Bronze -> Silver)...")
        clean_all()
    except Exception as e:
        print(f"[ERROR] Cleaner failed: {e}")
        return

    try:
        from src.transformation.scd_logic import apply_scd_type_2
        print("[PIPELINE] Applying SCD Type 2 (dim_users)...")
        apply_scd_type_2()
    except Exception as e:
        print(f"[WARN] SCD failed (non-fatal): {e}")

    try:
        from src.transformation.star_schema import build_star_schema
        print("[PIPELINE] Building star schema (Gold)...")
        build_star_schema()
        print("[OK] Full pipeline complete")
        
        # Clear KPI cache so new data is immediately available for all KPIs
        try:
            import src.analytics.kpi_queries as kpi_mod
            kpi_mod._table_cache = None
            print("[PIPELINE] KPI cache cleared - new data available")
        except Exception as e:
            print(f"[WARN] Could not clear KPI cache: {e}")
    except Exception as e:
        print(f"[ERROR] Star schema failed: {e}")


def process_micro_batch():
    """Process one micro-batch of events."""
    events = read_new_events()

    if not events:
        return 0

    # Count event types for logging
    type_counts = {}
    for e in events:
        t = e.get("event_type", "unknown")
        type_counts[t] = type_counts.get(t, 0) + 1
    
    summary = ", ".join(f"{v} {k}" for k, v in type_counts.items())
    print(f"\n[BATCH] Processing {len(events)} events: {summary}")

    # Write to Bronze CSVs
    write_events_to_csv(events)

    # Update marker
    if STREAM_BUFFER.exists():
        with open(STREAM_BUFFER, 'r') as f:
            total_lines = sum(1 for _ in f)
        set_last_processed_line(total_lines)

    # Run full pipeline
    trigger_incremental_pipeline()

    return len(events)


def run_stream_processor(batch_interval: float = 10.0):
    """
    Run the stream processor continuously.

    Args:
        batch_interval: Seconds between micro-batch processing (default: 10s)
    """
    _ensure_dirs()
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')

    print(f"[PROCESSOR] Stream Processor started (batch interval: {batch_interval}s)")

    total_processed = 0

    try:
        while True:
            events_processed = process_micro_batch()
            total_processed += events_processed

            if events_processed > 0:
                print(f"[STATS] Total events processed: {total_processed}")

            time.sleep(batch_interval)

    except KeyboardInterrupt:
        print(f"\n[STOP] Stream Processor stopped ({total_processed} total events)")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="RetailNexus Stream Processor")
    parser.add_argument("--interval", type=float, default=10.0,
                        help="Seconds between micro-batch processing (default: 10.0)")
    args = parser.parse_args()

    run_stream_processor(batch_interval=args.interval)
