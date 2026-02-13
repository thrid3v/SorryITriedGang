"""
clean.py - Clean and transform data
====================================
Runs the full transformation pipeline:
1. Cleaner - removes nulls, duplicates
2. SCD Logic - handles slowly changing dimensions
3. Star Schema - builds fact and dimension tables

Usage:
    python clean.py
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

PIPELINE_SCRIPTS = [
    "src/transformation/cleaner.py",
    "src/transformation/scd_logic.py",
    "src/transformation/star_schema.py",
]

def run_script(script_path):
    """Execute a transformation script."""
    full_path = PROJECT_ROOT / script_path
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Running: {script_path}")
    
    try:
        result = subprocess.run(
            [sys.executable, str(full_path)],
            capture_output=True,
            text=True,
            check=True
        )
        # Print output
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] in {script_path}:")
        print(e.stderr if e.stderr else e.stdout)
        return False

if __name__ == "__main__":
    print("[CLEAN] Starting transformation pipeline...")
    
    success = True
    for script in PIPELINE_SCRIPTS:
        if not run_script(script):
            success = False
            break
    
    if success:
        print("[CLEAN] Complete! Data cleaned and transformed to gold layer.")
    else:
        print("[CLEAN] Failed. Check errors above.")
        sys.exit(1)
