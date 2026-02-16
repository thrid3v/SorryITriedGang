"""
RetailNexus - Transformation Pipeline Orchestrator
Runs the full Bronze -> Silver -> Gold pipeline in order:
  1. Cleaner   (Bronze -> Silver)
  2. SCD Type 2 (Silver -> Gold dim_users)
  3. Star Schema (Silver -> Gold dims + fact)

Each step is independent — missing data is skipped gracefully.
"""
import sys
import os

# ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.transformation.cleaner import clean_all
from src.transformation.scd_logic import apply_scd_type_2
from src.transformation.star_schema import build_star_schema


def run_pipeline():
    """Execute the full transformation pipeline.
    Each step runs independently — failures in one step do not block others."""
    print("=" * 60)
    print("  RetailNexus Transformation Pipeline")
    print("=" * 60)

    # Step 1: Bronze -> Silver
    print("\n> Step 1/3: Cleaning raw data (Bronze -> Silver)")
    try:
        clean_results = clean_all()
        any_cleaned = any(v for v in clean_results.values()) if isinstance(clean_results, dict) else False
    except Exception as e:
        print(f"[Pipeline] Cleaner error: {e}")
        any_cleaned = False

    # Step 2: SCD Type 2 on Users (only if users were cleaned)
    print("\n> Step 2/3: Applying SCD Type 2 (dim_users)")
    try:
        apply_scd_type_2()
    except FileNotFoundError:
        print("[Pipeline] Silver users not found - skipping SCD (no user data uploaded)")
    except Exception as e:
        print(f"[Pipeline] SCD error: {e}")

    # Step 3: Build Star Schema (works with whatever Silver data is available)
    print("\n> Step 3/3: Building Star Schema (Silver -> Gold)")
    try:
        build_star_schema()
    except Exception as e:
        print(f"[Pipeline] Star Schema error: {e}")

    print("\n" + "=" * 60)
    print("  Pipeline complete OK")
    print("=" * 60)


if __name__ == "__main__":
    run_pipeline()
