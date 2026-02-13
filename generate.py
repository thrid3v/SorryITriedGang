"""
generate.py - Generate raw retail data
=======================================
Creates raw CSV files in data/raw/ with transactions, users, and products.

Usage:
    python generate.py [num_transactions]

Example:
    python generate.py 200
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.generator import generate_transactions

if __name__ == "__main__":
    num = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    print(f"[GENERATE] Generating {num} transactions...")
    generate_transactions(num=num)
    print("[GENERATE] Complete!")
