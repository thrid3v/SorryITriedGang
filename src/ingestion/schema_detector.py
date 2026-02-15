"""
Schema Detector
===============
Auto-detects column types and schema from CSV files.
Used for Kaggle dataset ingestion to infer data structure.
"""
import pandas as pd
from typing import Dict, List, Any
from pathlib import Path
import re


class SchemaDetector:
    """Detects schema and column types from CSV files."""
    
    def __init__(self):
        self.type_patterns = {
            'id': [r'.*_?id$', r'^id$', r'.*_?key$', r'.*_?no$', r'.*_?number$'],
            'price': [r'.*price.*', r'.*amount.*', r'.*cost.*', r'.*total.*'],
            'quantity': [r'.*quantity.*', r'.*qty.*', r'.*count.*', r'.*units.*'],
            'date': [r'.*date.*', r'.*time.*', r'.*timestamp.*'],
            'category': [r'.*category.*', r'.*type.*', r'.*class.*'],
            'name': [r'.*name.*', r'.*description.*', r'.*title.*'],
            'email': [r'.*email.*', r'.*mail.*'],
            'phone': [r'.*phone.*', r'.*mobile.*', r'.*tel.*'],
        }
    
    def detect_column_type(self, column_name: str, sample_values: pd.Series) -> str:
        """
        Detect column type based on name and sample values.
        
        Args:
            column_name: Name of the column
            sample_values: Sample values from the column
        
        Returns:
            Detected type: 'id', 'float', 'int', 'datetime', 'string', etc.
        """
        col_lower = column_name.lower()
        
        # Check name patterns
        for type_name, patterns in self.type_patterns.items():
            for pattern in patterns:
                if re.match(pattern, col_lower, re.IGNORECASE):
                    return type_name
        
        # Check data types
        if pd.api.types.is_numeric_dtype(sample_values):
            if pd.api.types.is_float_dtype(sample_values):
                return 'float'
            else:
                return 'int'
        elif pd.api.types.is_datetime64_any_dtype(sample_values):
            return 'datetime'
        else:
            return 'string'
    
    def detect_schema(self, csv_path: str, sample_rows: int = 100) -> Dict[str, Any]:
        """
        Auto-detect schema from CSV file.
        
        Args:
            csv_path: Path to CSV file
            sample_rows: Number of rows to sample for detection
        
        Returns:
            Schema dict with column types and metadata
        """
        for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
            try:
                df = pd.read_csv(csv_path, nrows=sample_rows, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError(f"Could not decode {csv_path} with any encoding")
        
        schema = {
            'file_path': csv_path,
            'total_columns': len(df.columns),
            'sample_rows': len(df),
            'columns': {}
        }
        
        for col in df.columns:
            col_type = self.detect_column_type(col, df[col])
            
            schema['columns'][col] = {
                'type': col_type,
                'null_count': int(df[col].isnull().sum()),
                'null_rate': float(df[col].isnull().sum() / len(df)),
                'unique_count': int(df[col].nunique()),
                'sample_values': df[col].dropna().head(3).tolist()
            }
        
        return schema
    
    def suggest_mappings(self, schema: Dict[str, Any]) -> Dict[str, str]:
        """
        Suggest column mappings to universal retail concepts.
        
        Args:
            schema: Schema dict from detect_schema()
        
        Returns:
            Mapping of original column → suggested concept
        """
        mappings = {}
        
        for col_name, col_info in schema['columns'].items():
            col_lower = col_name.lower()
            col_type = col_info['type']
            
            # Suggest mappings based on patterns
            if col_type == 'id':
                if 'transaction' in col_lower or 'order' in col_lower or 'invoice' in col_lower:
                    mappings[col_name] = 'transaction_id'
                elif 'product' in col_lower or 'item' in col_lower or 'stock' in col_lower:
                    mappings[col_name] = 'product_id'
                elif 'customer' in col_lower or 'user' in col_lower:
                    mappings[col_name] = 'user_id'
                elif 'store' in col_lower or 'location' in col_lower:
                    mappings[col_name] = 'store_id'
            
            elif col_type == 'price':
                mappings[col_name] = 'amount'
            
            elif col_type == 'quantity':
                mappings[col_name] = 'quantity'
            
            elif col_type == 'date':
                mappings[col_name] = 'timestamp'
            
            elif col_type == 'category':
                mappings[col_name] = 'category'
            
            elif col_type == 'name':
                if 'product' in col_lower or 'item' in col_lower:
                    mappings[col_name] = 'product_name'
                elif 'customer' in col_lower or 'user' in col_lower:
                    mappings[col_name] = 'user_name'
        
        return mappings
    
    def print_schema_report(self, schema: Dict[str, Any]):
        """Print a human-readable schema report."""
        print(f"\n{'='*60}")
        print(f"Schema Detection Report")
        print(f"{'='*60}")
        print(f"File: {schema['file_path']}")
        print(f"Columns: {schema['total_columns']}")
        print(f"Sample Rows: {schema['sample_rows']}")
        print(f"\n{'Column':<30} {'Type':<15} {'Nulls':<10} {'Unique':<10}")
        print(f"{'-'*65}")
        
        for col_name, col_info in schema['columns'].items():
            null_pct = f"{col_info['null_rate']*100:.1f}%"
            print(f"{col_name:<30} {col_info['type']:<15} {null_pct:<10} {col_info['unique_count']:<10}")
        
        print(f"\nSuggested Mappings:")
        print(f"{'-'*65}")
        mappings = self.suggest_mappings(schema)
        for orig, suggested in mappings.items():
            print(f"  {orig:<30} → {suggested}")
        print(f"{'='*60}\n")


# CLI test
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python schema_detector.py <csv_file>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    
    detector = SchemaDetector()
    schema = detector.detect_schema(csv_file)
    detector.print_schema_report(schema)
