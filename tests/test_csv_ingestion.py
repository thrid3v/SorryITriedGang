"""
Simple CSV Ingestion Test
==========================
Tests the ingestion pipeline with a sample CSV without Kaggle API.
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.schema_detector import SchemaDetector
import pandas as pd
from datetime import datetime

# Paths
CSV_FILE = PROJECT_ROOT / "data" / "kaggle" / "sample_ecommerce.csv"
RAW_DIR = PROJECT_ROOT / "data" / "raw"

def main():
    print("\n" + "="*60)
    print("CSV Ingestion Test - E-commerce Dataset")
    print("="*60 + "\n")
    
    # Step 1: Detect schema
    print("[1/3] Detecting schema...")
    detector = SchemaDetector()
    schema = detector.detect_schema(str(CSV_FILE))
    detector.print_schema_report(schema)
    
    # Step 2: Get suggested mappings
    print("[2/3] Mapping columns...")
    mappings = detector.suggest_mappings(schema)
    
    # Add custom mappings for this dataset
    custom_mappings = {
        'InvoiceNo': 'transaction_id',
        'StockCode': 'product_id',
        'Description': 'product_name',
        'Quantity': 'quantity',
        'InvoiceDate': 'timestamp',
        'UnitPrice': 'amount',
        'CustomerID': 'user_id',
        'Country': 'store_id'  # Treat country as store location
    }
    
    print("\nColumn Mappings:")
    for orig, mapped in custom_mappings.items():
        print(f"  {orig:<20} → {mapped}")
    
    # Step 3: Normalize data
    print("\n[3/3] Normalizing data...")
    df = pd.read_csv(CSV_FILE)
    df_normalized = df.rename(columns=custom_mappings)
    
    # Add source tracking
    df_normalized['data_source'] = 'kaggle'
    df_normalized['source_file'] = 'sample_ecommerce.csv'
    
    # Save to raw directory
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = RAW_DIR / f"kaggle_ecommerce_{timestamp}.csv"
    
    df_normalized.to_csv(output_file, index=False)
    
    print(f"\n[SUCCESS] Ingestion complete!")
    print(f"Output: {output_file}")
    print(f"Rows: {len(df_normalized)}, Columns: {len(df_normalized.columns)}")
    print("\n" + "="*60 + "\n")
    
    return output_file

if __name__ == "__main__":
    main()
