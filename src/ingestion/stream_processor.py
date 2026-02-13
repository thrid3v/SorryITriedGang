"""
RetailNexus Stream Processor
Watches streaming buffer and processes events in micro-batches.
Appends to Bronze CSV files and triggers incremental pipeline updates.
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
    """Get the last processed line number from marker file."""
    if PROCESSED_MARKER.exists():
        with open(PROCESSED_MARKER, 'r') as f:
            return int(f.read().strip())
    return 0


def set_last_processed_line(line_num: int):
    """Update the last processed line marker."""
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


def append_to_bronze_csv(events: List[Dict]):
    """
    Append events to Bronze CSV files.
    Each event contains multiple products, so we expand them into transaction rows.
    """
    if not events:
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    txn_file = RAW_DIR / f"transactions_stream_{timestamp}.csv"
    
    # Append transactions
    rows = []
    for event in events:
        for product in event['products']:
            rows.append({
                'transaction_id': event['transaction_id'],
                'user_id': event['user_id'],
                'product_id': product['product_id'],
                'timestamp': event['timestamp'],
                'amount': product['amount'],
                'store_id': event['store_id']
            })
    
    # Write to CSV
    if rows:
        with open(txn_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'transaction_id', 'user_id', 'product_id', 
                'timestamp', 'amount', 'store_id'
            ])
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"[OK] Appended {len(rows)} transaction rows to {txn_file.name}")


def trigger_incremental_pipeline():
    """
    Trigger incremental pipeline update.
    For now, just run the cleaner which will pick up new CSV files.
    """
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    
    try:
        from src.transformation.cleaner import clean_all
        from src.transformation.scd_logic import apply_scd_type_2
        from src.transformation.star_schema import build_star_schema
        
        print("[PIPELINE] Running incremental pipeline...")
        clean_all()
        apply_scd_type_2()
        build_star_schema()
        print("[OK] Incremental pipeline complete")
    except Exception as e:
        print(f"[ERROR] Pipeline error: {e}")


def process_micro_batch():
    """Process one micro-batch of events."""
    events = read_new_events()
    
    if not events:
        return 0
    
    print(f"\n📊 Processing micro-batch: {len(events)} events")
    
    # Append to Bronze
    append_to_bronze_csv(events)
    
    # Update marker
    if STREAM_BUFFER.exists():
        with open(STREAM_BUFFER, 'r') as f:
            total_lines = sum(1 for _ in f)
        set_last_processed_line(total_lines)
    
    # Trigger pipeline
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
    # Configure stdout for UTF-8 to handle emoji on Windows
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
    parser.add_argument(
        "--interval",
        type=float,
        default=10.0,
        help="Seconds between micro-batch processing (default: 10.0)"
    )
    args = parser.parse_args()
    
    run_stream_processor(batch_interval=args.interval)
