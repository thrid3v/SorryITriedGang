"""
Direct CSV Ingestion Script
Bypasses Kaggle API and directly ingests CSV files
"""
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.schema_detector import SchemaDetector

# Directories
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

def ingest_csv_direct(csv_path: str):
    """Directly ingest a CSV file without Kaggle API"""
    csv_path = Path(csv_path)
    
    if not csv_path.exists():
        print(f"[ERROR] File not found: {csv_path}")
        return None
    
    print(f"\n{'='*60}")
    print(f"Ingesting CSV: {csv_path.name}")
    print(f"{'='*60}\n")
    
    # Detect schema
    print("[1/3] Detecting schema...")
    detector = SchemaDetector()
    schema = detector.detect_schema(str(csv_path))
    detector.print_schema_report(schema)
    
    # Map columns
    print("\n[2/3] Mapping columns...")
    column_mapping = detector.suggest_mappings(schema)
    for col, suggestion in column_mapping.items():
        print(f"[Mapping] {col} → {suggestion}")
    
    # Normalize data
    print("\n[3/3] Normalizing data...")
    
    # Try different encodings
    encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
    df = None
    for encoding in encodings:
        try:
            df = pd.read_csv(csv_path, encoding=encoding)
            print(f"[OK] Successfully read CSV with {encoding} encoding")
            break
        except UnicodeDecodeError:
            continue
    
    if df is None:
        print("[ERROR] Could not read CSV with any known encoding")
        return None
    
    df_normalized = df.rename(columns=column_mapping)
    df_normalized['data_source'] = 'kaggle'
    df_normalized['source_file'] = csv_path.name
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_name = f"kaggle_{csv_path.stem}_{timestamp}.csv"
    output_path = RAW_DIR / output_name
    
    # Save normalized data
    df_normalized.to_csv(output_path, index=False)
    print(f"\n[OK] Normalized data → {output_path.name}")
    print(f"     Rows: {len(df_normalized)}, Columns: {len(df_normalized.columns)}")
    
    print(f"\n{'='*60}")
    print(f"[SUCCESS] Ingestion complete!")
    print(f"Output: {output_path}")
    print(f"{'='*60}\n")
    
    return output_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python direct_ingest.py <path_to_csv>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    ingest_csv_direct(csv_file)
