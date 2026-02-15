"""
Kaggle Dataset Ingestion
=========================
Downloads and ingests Kaggle retail datasets into the pipeline.
Auto-detects schema and maps columns to universal retail concepts.
"""
import os
import sys
import json
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.schema_detector import SchemaDetector

# Directories
KAGGLE_DIR = PROJECT_ROOT / "data" / "kaggle"
RAW_DIR = PROJECT_ROOT / "data" / "raw"


class KaggleIngestion:
    """Handles Kaggle dataset download and ingestion."""
    
    def __init__(self):
        self.kaggle_dir = KAGGLE_DIR
        self.raw_dir = RAW_DIR
        self.detector = SchemaDetector()
        # Ensure directories exist
        self.kaggle_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Try to import Kaggle API (optional)
        self.kaggle_available = False
        self.api = None
        try:
            from kaggle.api.kaggle_api_extended import KaggleApi
            self.api = KaggleApi()
            try:
                self.api.authenticate()
                self.kaggle_available = True
                print("[INFO] Kaggle API authenticated successfully")
            except (Exception, SystemExit) as auth_error:
                print(f"[INFO] Kaggle API not authenticated: {auth_error}")
                print(f"[INFO] You can still use local CSV files with --csv-file option")
        except (ImportError, Exception, SystemExit):
            print(f"[INFO] Kaggle library not available")
            print(f"[INFO] You can still use local CSV files with --csv-file option")
    
    def search_datasets(self, query: str = "retail sales", max_results: int = 10) -> List[Dict]:
        """
        Search for datasets on Kaggle.
        
        Args:
            query: Search query
            max_results: Maximum number of results
        
        Returns:
            List of dataset metadata dicts
        """
        if not self.kaggle_available:
            print("[ERROR] Kaggle API not available")
            return []
        
        print(f"[Kaggle] Searching for: '{query}'...")
        datasets = self.api.dataset_list(search=query, page_size=max_results)
        
        results = []
        for ds in datasets:
            results.append({
                'ref': ds.ref,
                'title': ds.title,
                'size': ds.size,
                'downloadCount': ds.downloadCount,
                'voteCount': ds.voteCount
            })
        
        return results
    
    def download_dataset(self, dataset_ref: str) -> Optional[Path]:
        """
        Download a Kaggle dataset.
        
        Args:
            dataset_ref: Dataset reference (e.g., "username/dataset-name")
        
        Returns:
            Path to downloaded directory
        """
        if not self.kaggle_available:
            print("[ERROR] Kaggle API not available")
            return None
        
        print(f"[Kaggle] Downloading: {dataset_ref}...")
        
        # Create dataset-specific directory
        dataset_name = dataset_ref.split('/')[-1]
        dataset_dir = self.kaggle_dir / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        # Download and unzip
        try:
            self.api.dataset_download_files(
                dataset_ref,
                path=str(dataset_dir),
                unzip=True
            )
            print(f"[OK] Downloaded to: {dataset_dir}")
            return dataset_dir
        except Exception as e:
            print(f"[ERROR] Download failed: {e}")
            return None
    
    def find_csv_files(self, directory: Path) -> List[Path]:
        """Find all CSV files in a directory."""
        return list(directory.glob("*.csv"))
    
    def map_columns_semantic(self, csv_columns: List[str]) -> Dict[str, str]:
        """
        Map CSV columns to universal concepts using semantic matching.
        
        Args:
            csv_columns: List of column names from CSV
        
        Returns:
            Mapping dict: original_column → concept_name
        """
        mapping = {}
        
        # Try to import semantic matcher (optional)
        try:
            from src.analytics.semantic_matcher import semantic_concept_match
        except Exception:
            print("[INFO] Semantic matching not available, using rule-based mapping only")
            return mapping
        
        for col in csv_columns:
            try:
                # Use semantic matcher
                matches = semantic_concept_match(col, top_k=1)
                if matches and len(matches) > 0:
                    concept_name, common_columns, score = matches[0]
                    if score > 0.6:  # Confidence threshold
                        # Use first common column name
                        if common_columns:
                            mapping[col] = common_columns[0]
                        print(f"[Mapping] {col} → {common_columns[0] if common_columns else concept_name} (confidence: {score:.2f})")
            except Exception as e:
                # Semantic matching failed, skip this column
                pass
        
        return mapping
    
    def normalize_data(
        self,
        csv_path: Path,
        column_mapping: Dict[str, str],
        output_name: str = None
    ) -> Path:
        """
        Normalize Kaggle CSV to match pipeline schema.
        
        Args:
            csv_path: Path to Kaggle CSV
            column_mapping: Column name mapping
            output_name: Optional output filename
        
        Returns:
            Path to normalized CSV in raw directory
        """
        print(f"[Normalize] Processing: {csv_path.name}")
        
        # Read CSV (try multiple encodings)
        for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
            try:
                df = pd.read_csv(csv_path, encoding=encoding)
                print(f"[OK] Read CSV with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        else:
            print(f"[ERROR] Could not decode {csv_path.name} with any encoding")
            return None
        
        # Rename columns based on mapping
        df_normalized = df.rename(columns=column_mapping)
        
        # Add source tracking
        df_normalized['data_source'] = 'kaggle'
        df_normalized['source_file'] = csv_path.name
        
        # Generate output filename
        if not output_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_name = f"kaggle_{csv_path.stem}_{timestamp}.csv"
        
        output_path = self.raw_dir / output_name
        
        # Save normalized data
        df_normalized.to_csv(output_path, index=False)
        print(f"[OK] Normalized data → {output_path.name}")
        print(f"     Rows: {len(df_normalized)}, Columns: {len(df_normalized.columns)}")
        
        return output_path
    
    def ingest_csv(self, csv_path: str, auto_map: bool = True) -> Optional[Path]:
        """
        Ingest a CSV file (local or from Kaggle download).
        
        Args:
            csv_path: Path to CSV file
            auto_map: Whether to auto-map columns
        
        Returns:
            Path to normalized CSV
        """
        csv_path = Path(csv_path)
        
        if not csv_path.exists():
            print(f"[ERROR] File not found: {csv_path}")
            return None
        
        print(f"\n{'='*60}")
        print(f"Ingesting CSV: {csv_path.name}")
        print(f"{'='*60}\n")
        
        # Detect schema
        print("[1/3] Detecting schema...")
        schema = self.detector.detect_schema(str(csv_path))
        self.detector.print_schema_report(schema)
        
        # Map columns
        print("[2/3] Mapping columns...")
        if auto_map:
            # Try semantic mapping first
            column_mapping = self.map_columns_semantic(list(schema['columns'].keys()))
            
            # Fall back to suggested mappings for unmapped columns
            suggested = self.detector.suggest_mappings(schema)
            for col, suggestion in suggested.items():
                if col not in column_mapping:
                    column_mapping[col] = suggestion
                    print(f"[Mapping] {col} → {suggestion} (rule-based)")
        else:
            column_mapping = self.detector.suggest_mappings(schema)
        
        # Normalize data
        print("[3/3] Normalizing data...")
        output_path = self.normalize_data(csv_path, column_mapping)
        
        print(f"\n{'='*60}")
        print(f"[SUCCESS] Ingestion complete!")
        print(f"Output: {output_path}")
        print(f"{'='*60}\n")
        
        return output_path
    
    def ingest_dataset(self, dataset_ref: str, auto_map: bool = True) -> List[Path]:
        """
        Download and ingest a Kaggle dataset.
        
        Args:
            dataset_ref: Kaggle dataset reference
            auto_map: Whether to auto-map columns
        
        Returns:
            List of normalized CSV paths
        """
        # Download dataset
        dataset_dir = self.download_dataset(dataset_ref)
        if not dataset_dir:
            return []
        
        # Find CSV files
        csv_files = self.find_csv_files(dataset_dir)
        if not csv_files:
            print(f"[WARNING] No CSV files found in {dataset_dir}")
            return []
        
        print(f"[INFO] Found {len(csv_files)} CSV file(s)")
        
        # Ingest each CSV
        normalized_paths = []
        for csv_file in csv_files:
            output_path = self.ingest_csv(str(csv_file), auto_map=auto_map)
            if output_path:
                normalized_paths.append(output_path)
        
        return normalized_paths


# CLI
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Kaggle Dataset Ingestion")
    parser.add_argument("--search", type=str, help="Search for datasets")
    parser.add_argument("--download", type=str, help="Download dataset (username/dataset-name)")
    parser.add_argument("--ingest", type=str, help="Download and ingest dataset")
    parser.add_argument("--csv-file", type=str, help="Ingest local CSV file")
    parser.add_argument("--no-auto-map", action="store_true", help="Disable semantic column mapping")
    
    args = parser.parse_args()
    
    ingestion = KaggleIngestion()
    
    if args.search:
        results = ingestion.search_datasets(args.search)
        print(f"\nFound {len(results)} datasets:\n")
        for i, ds in enumerate(results, 1):
            print(f"{i}. {ds['ref']}")
            print(f"   Title: {ds['title']}")
            print(f"   Downloads: {ds['downloadCount']}, Votes: {ds['voteCount']}")
            print()
    
    elif args.download:
        ingestion.download_dataset(args.download)
    
    elif args.ingest:
        auto_map = not args.no_auto_map
        ingestion.ingest_dataset(args.ingest, auto_map=auto_map)
    
    elif args.csv_file:
        auto_map = not args.no_auto_map
        ingestion.ingest_csv(args.csv_file, auto_map=auto_map)
    
    else:
        parser.print_help()
