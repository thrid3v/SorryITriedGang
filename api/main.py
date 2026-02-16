"""
RetailNexus FastAPI Backend
============================
Exposes analytics endpoints that reuse existing kpi_queries.py functions.
Serves data to the React frontend.
Simplified auth: Role-based via X-User-Role header (no JWT).
"""
from __future__ import annotations

import sys
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Header, UploadFile, File, status, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field, validator

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analytics.kpi_queries import (
    compute_summary_kpis,
    compute_clv,
    compute_market_basket,
    compute_revenue_timeseries,
    compute_city_sales,
    compute_top_products,
    compute_inventory_turnover,
    compute_delivery_metrics,
    compute_seasonal_trends,
    compute_customer_segmentation,
    _table_cache,
)
from src.analytics.schema_inspector import load_business_context
from api.context_manager import get_business_contexts, save_business_contexts
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# ── Input Validation Models ────────────────────────────────

class RevenueTimeseriesRequest(BaseModel):
    granularity: str = Field(default="daily", description="Time granularity: daily, monthly, or yearly")
    
    @validator('granularity')
    def validate_granularity(cls, v):
        allowed = ['daily', 'monthly', 'yearly']
        if v.lower() not in allowed:
            raise ValueError(f'granularity must be one of: {", ".join(allowed)}')
        return v.lower()

class TopProductsRequest(BaseModel):
    limit: int = Field(default=10, ge=1, le=100, description="Maximum number of products to return (1-100)")

class MarketBasketRequest(BaseModel):
    min_support: int = Field(default=2, ge=1, le=1000, description="Minimum support count (1-1000)")

# ── Streaming State ──────────────────────────────────
stream_state = {
    "status": "stopped",  # stopped | running
    "started_at": None,
    "events_processed": 0,
    "generator_pid": None,
    "processor_pid": None,
}

app = FastAPI(
    title="RetailNexus API",
    version="1.0.0",
)

@app.on_event("startup")
def clear_caches_on_startup():
    """Invalidate table cache on server (re)start so we always discover fresh paths."""
    import src.analytics.kpi_queries as kpi_mod
    kpi_mod._table_cache = None
    logger.info("Table cache cleared on startup")

# ── Auth Helpers ─────────────────────────────────────

def get_role(x_user_role: str = Header(default="customer")) -> str:
    """Extract user role from X-User-Role header."""
    if x_user_role not in ("admin", "customer"):
        return "customer"
    return x_user_role


def require_admin(x_user_role: str = Header(default="customer")) -> str:
    """Require admin role for endpoint access."""
    role = get_role(x_user_role)
    if role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return role


# ── CORS Configuration ───────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)


import traceback

# ── Logging & Error Handling ─────────────────────────

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests and their response status."""
    logger.info(f"{request.method} {request.url.path}")
    try:
        response = await call_next(request)
        logger.debug(f"Response: {response.status_code} for {request.method} {request.url.path}")
        return response
    except Exception as e:
        logger.error(f"Request failed: {request.method} {request.url.path}", exc_info=True)
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal Server Error: {str(e)}", "traceback": traceback.format_exc()}
        )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.critical(f"Unhandled {type(exc).__name__}: {str(exc)}", exc_info=True)
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "traceback": traceback.format_exc()}
    )

# ── Endpoints ────────────────────────────────────────

@app.get("/")
def root():
    """Health check endpoint"""
    return {
        "status": "ok",
        "message": "RetailNexus API is running",
        "version": "1.0.0"
    }


# ── Analytics Endpoints ─────────────────────────────

@app.get("/api/kpis", response_model=Dict[str, Any])
def get_summary_kpis(role: str = Header(default="customer", alias="X-User-Role")):
    """Get summary KPIs: total revenue, active users, total orders."""
    try:
        return compute_summary_kpis()
    except Exception as e:
        logger.warning(f"KPIs unavailable: {e}")
        return {"total_revenue": 0.0, "active_users": 0, "total_orders": 0}


@app.get("/api/clv", response_model=List[Dict])
def get_clv(role: str = Header(default="customer", alias="X-User-Role")):
    """Get Customer Lifetime Value analysis."""
    try:
        df = compute_clv()
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.warning(f"CLV unavailable: {e}")
        return []


@app.get("/api/basket", response_model=List[Dict])
def get_market_basket(
    role: str = Header(default="customer", alias="X-User-Role"), 
    min_support: int = Query(default=2, ge=1, le=1000)
):
    """Get market basket analysis - product pairs frequently bought together."""
    try:
        # Validate min_support
        if min_support < 1 or min_support > 1000:
            raise HTTPException(status_code=400, detail="min_support must be between 1 and 1000")
        
        df = compute_market_basket(min_support=min_support)
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Market basket unavailable: {e}")
        return []


@app.get("/api/revenue/timeseries", response_model=List[Dict])
def get_revenue_timeseries(granularity: str = Query(default='daily', pattern='^(daily|monthly|yearly)$')):
    """Get revenue time-series (daily, monthly, or yearly)."""
    try:
        # Validate granularity
        if granularity.lower() not in ['daily', 'monthly', 'yearly']:
            raise HTTPException(status_code=400, detail="granularity must be 'daily', 'monthly', or 'yearly'")
        
        df = compute_revenue_timeseries(granularity=granularity.lower())
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Revenue timeseries unavailable: {e}")
        return []


@app.get("/api/sales/city", response_model=List[Dict])
def get_city_sales():
    """Get city-wise sales breakdown."""
    try:
        df = compute_city_sales()
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.warning(f"City sales unavailable: {e}")
        return []


@app.get("/api/products/top", response_model=List[Dict])
def get_top_products(limit: int = Query(default=10, ge=1, le=100)):
    """Get top-selling products by revenue."""
    try:
        # Validate limit
        if limit < 1 or limit > 100:
            raise HTTPException(status_code=400, detail="limit must be between 1 and 100")
        
        df = compute_top_products(limit=limit)
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Top products unavailable: {e}")
        return []


@app.get("/api/inventory/turnover", response_model=List[Dict])
def get_inventory_turnover(role: str = Header(default="customer", alias="X-User-Role")):
    """Get inventory turnover ratio analysis."""
    try:
        df = compute_inventory_turnover()
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.warning(f"Inventory turnover unavailable: {e}")
        return []


@app.get("/api/delivery/metrics", response_model=List[Dict])
def get_delivery_metrics(role: str = Header(default="customer", alias="X-User-Role")):
    """Get delivery performance metrics by carrier and region."""
    try:
        df = compute_delivery_metrics()
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.warning(f"Delivery metrics unavailable: {e}")
        return []


@app.get("/api/trends/seasonal", response_model=List[Dict])
def get_seasonal_trends(role: str = Header(default="customer", alias="X-User-Role")):
    """Get seasonal demand trends by category."""
    try:
        df = compute_seasonal_trends()
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.warning(f"Seasonal trends unavailable: {e}")
        return []


@app.get("/api/customers/segmentation", response_model=List[Dict])
def get_customer_segmentation(role: str = Header(default="customer", alias="X-User-Role")):
    """Get new vs. returning customer segmentation."""
    try:
        df = compute_customer_segmentation()
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.warning(f"Customer segmentation unavailable: {e}")
        return []


def check_duckdb_connection() -> bool:
    """Check if DuckDB connection can be established."""
    try:
        import duckdb
        conn = duckdb.connect()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"DuckDB connection check failed: {e}")
        return False

def check_data_availability() -> bool:
    """Check if data files exist in gold layer."""
    try:
        gold_dir = PROJECT_ROOT / "data" / "gold"
        if not gold_dir.exists():
            return False
        
        # Check for at least one parquet file
        parquet_files = list(gold_dir.glob("*.parquet"))
        return len(parquet_files) > 0
    except Exception as e:
        logger.warning(f"Data availability check failed: {e}")
        return False

def check_disk_space() -> dict:
    """Check available disk space."""
    try:
        import shutil
        total, used, free = shutil.disk_usage(PROJECT_ROOT)
        return {
            "total_gb": round(total / (1024**3), 2),
            "used_gb": round(used / (1024**3), 2),
            "free_gb": round(free / (1024**3), 2),
            "percent_free": round((free / total) * 100, 2)
        }
    except Exception as e:
        logger.warning(f"Disk space check failed: {e}")
        return {"error": str(e)}

@app.get("/api/health")
def health_check():
    """Comprehensive health check with multiple system checks."""
    checks = {
        "api": "healthy",
        "database": check_duckdb_connection(),
        "data_available": check_data_availability(),
        "disk_space": check_disk_space(),
    }
    
    # Determine overall status
    critical_checks = ["api", "database"]
    all_critical_healthy = all(checks.get(c) for c in critical_checks)
    
    if all_critical_healthy and checks.get("data_available"):
        status = "healthy"
        message = "API and data pipeline operational"
    elif all_critical_healthy:
        status = "degraded"
        message = "API running but no data available"
    else:
        status = "unhealthy"
        message = "Critical system components unavailable"
    
    try:
        kpis = compute_summary_kpis()
        checks["sample_kpis"] = {
            "total_orders": kpis.get("total_orders", 0),
            "total_revenue": kpis.get("total_revenue", 0),
            "active_users": kpis.get("active_users", 0)
        }
    except Exception as e:
        logger.warning(f"Could not fetch sample KPIs: {e}")
        checks["sample_kpis"] = None
    
    return {
        "status": status,
        "message": message,
        "checks": checks,
        "timestamp": datetime.now().isoformat()
    }



# ── File Upload & Pipeline Endpoints ────────────────

@app.post("/api/upload/scan")
async def scan_file_headers(
    file: UploadFile = File(...),
    x_user_role: str = Header(default="customer", alias="X-User-Role"),
):
    """
    Step 1: Scan uploaded file headers and save to staging.
    Returns detected headers and recommended column mappings.
    """
    if x_user_role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Validate file type
    filename = file.filename or "uploaded_data"
    allowed_extensions = {".csv", ".xlsx", ".xls", ".tsv", ".parquet", ".json"}
    file_ext = Path(filename).suffix.lower()
    
    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{file_ext}'. Allowed: {', '.join(allowed_extensions)}"
        )
    
    try:
        import pandas as pd
        import io
        
        # Read file to get headers
        contents = await file.read()
        
        if file_ext == ".csv":
            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    df = pd.read_csv(io.BytesIO(contents), encoding=encoding, nrows=5)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise HTTPException(status_code=400, detail="Could not decode CSV file")
        elif file_ext == ".tsv":
            df = pd.read_csv(io.BytesIO(contents), sep='\t', nrows=5)
        elif file_ext in (".xlsx", ".xls"):
            df = pd.read_excel(io.BytesIO(contents), nrows=5)
        elif file_ext == ".parquet":
            df = pd.read_parquet(io.BytesIO(contents))
            df = df.head(5)
        elif file_ext == ".json":
            df = pd.read_json(io.BytesIO(contents))
            df = df.head(5)
        
        if df.empty:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")
        
        # Save to staging directory
        staging_dir = PROJECT_ROOT / "data" / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Save original file to staging
        staging_path = staging_dir / filename
        with open(staging_path, 'wb') as f:
            f.write(contents)
        
        # Detect headers and recommend mappings
        headers = df.columns.tolist()
        recommended_mapping = recommend_column_mapping(headers)
        
        # Detect likely file type
        file_type = detect_file_type(headers)
        
        return {
            "status": "success",
            "filename": filename,
            "headers": headers,
            "recommended_mapping": recommended_mapping,
            "detected_type": file_type,
            "row_count": len(df),
            "sample_data": df.head(3).to_dict(orient="records"),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to scan file: {str(e)}")


def recommend_column_mapping(headers: list) -> dict:
    """
    Recommend mappings from user columns to system standard columns.
    Returns dict: { system_column: user_column }
    """
    import re
    
    mapping = {}
    headers_lower = {h.lower(): h for h in headers}
    
    # Define standard columns and their patterns
    standard_patterns = {
        'transaction_id': [r'(transaction|order|invoice|sale)[_\s]*(id|no|number|#)?', r'^id$', r'invoiceno'],
        'user_id': [r'(user|customer|client|member)[_\s]*(id|no|number)?', r'customerid'],
        'product_id': [r'(product|item|sku|stock)[_\s]*(id|no|code)?', r'stockcode'],
        'timestamp': [r'(date|time|timestamp|created|ordered|invoice[_\s]*date)', r'invoicedate'],
        'amount': [r'(amount|total|revenue|sales?|price)[_\s]*(amount)?$'],
        'quantity': [r'quantity|qty'],
        'store_id': [r'(store|location|branch|shop|country)[_\s]*(id|no)?'],
        'name': [r'(full[_\s]*)?name$', r'description'],
        'email': [r'email[_\s]*(address)?'],
        'city': [r'city'],
        'category': [r'category'],
        'price': [r'(unit[_\s]*)?price$'],
    }
    
    for std_col, patterns in standard_patterns.items():
        for header_lower, header_orig in headers_lower.items():
            for pattern in patterns:
                if re.search(pattern, header_lower, re.IGNORECASE):
                    mapping[std_col] = header_orig
                    break
            if std_col in mapping:
                break
    
    return mapping


def detect_file_type(headers: list) -> str:
    """Detect the likely file type based on headers."""
    headers_lower = [h.lower() for h in headers]
    
    transaction_indicators = ['transaction', 'order', 'invoice', 'sale', 'amount', 'price']
    user_indicators = ['user', 'customer', 'name', 'email']
    product_indicators = ['product', 'item', 'sku', 'category']
    
    has_transaction = any(any(ind in h for ind in transaction_indicators) for h in headers_lower)
    has_user = any(any(ind in h for ind in user_indicators) for h in headers_lower)
    has_product = any(any(ind in h for ind in product_indicators) for h in headers_lower)
    
    if has_transaction:
        return "transactions"
    elif has_user and not has_product:
        return "users"
    elif has_product:
        return "products"
    else:
        return "unknown"



@app.post("/api/upload/process")
async def process_mapped_file(
    request: dict,
    x_user_role: str = Header(default="customer", alias="X-User-Role"),
):
    """
    Step 2: Process file from staging with user-defined column mapping.
    Applies mapping, fills missing columns, and runs pipeline.
    
    Request body:
    {
        "filename": "data.csv",
        "file_type": "transactions",
        "mapping": {"transaction_id": "InvoiceNo", "amount": "Price", ...}
    }
    """
    if x_user_role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        import pandas as pd
        
        filename = request.get("filename")
        file_type = request.get("file_type", "transactions")
        mapping = request.get("mapping", {})
        
        if not filename:
            raise HTTPException(status_code=400, detail="filename is required")
        
        # Read from staging
        staging_path = PROJECT_ROOT / "data" / "staging" / filename
        if not staging_path.exists():
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found in staging")
        
        # Read file
        file_ext = Path(filename).suffix.lower()
        if file_ext == ".csv":
            df = pd.read_csv(staging_path)
        elif file_ext == ".tsv":
            df = pd.read_csv(staging_path, sep='\t')
        elif file_ext in (".xlsx", ".xls"):
            df = pd.read_excel(staging_path)
        elif file_ext == ".parquet":
            df = pd.read_parquet(staging_path)
        elif file_ext == ".json":
            df = pd.read_json(staging_path)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_ext}")
        
        # Apply column mapping (reverse: user_col -> system_col)
        reverse_mapping = {v: k for k, v in mapping.items()}
        df = df.rename(columns=reverse_mapping)
        
        # Define required columns per file type with defaults
        required_columns = {
            "transactions": {
                "transaction_id": lambda: [f"TXN_{i}" for i in range(len(df))],
                "user_id": lambda: "U001",
                "product_id": lambda: "P001",
                "timestamp": lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "amount": lambda: 0.0,
                "store_id": lambda: "S001",
            },
            "users": {
                "user_id": lambda: [f"U{i}" for i in range(len(df))],
                "name": lambda: "Unknown",
                "email": lambda: "unknown@example.com",
                "city": lambda: "Unknown",
                "signup_date": lambda: datetime.now().strftime('%Y-%m-%d'),
            },
            "products": {
                "product_id": lambda: [f"P{i}" for i in range(len(df))],
                "product_name": lambda: "Unknown Product",
                "category": lambda: "General",
                "price": lambda: 0.0,
            },
        }
        
        # Fill missing required columns
        if file_type in required_columns:
            for col, default_func in required_columns[file_type].items():
                if col not in df.columns:
                    df[col] = default_func()
        
        # Handle special cases for transactions
        if file_type == "transactions":
            if 'amount' not in df.columns or df['amount'].isna().all():
                if 'price' in df.columns and 'quantity' in df.columns:
                    df['amount'] = df['price'] * df['quantity']
                elif 'price' in df.columns:
                    df['amount'] = df['price']
        
        # Save to raw directory
        raw_dir = PROJECT_ROOT / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = raw_dir / f"{file_type}_{timestamp}.csv"
        df.to_csv(output_path, index=False)
        
        # Clear KPI cache
        import src.analytics.kpi_queries as kpi_mod
        kpi_mod._table_cache = None
        
        # Run pipeline
        pipeline_result = run_transformation_pipeline()
        
        # Clean up staging
        try:
            staging_path.unlink()
        except Exception:
            pass
        
        return {
            "status": "success",
            "message": f"File processed successfully as {file_type}",
            "rows": len(df),
            "columns": list(df.columns),
            "output_file": output_path.name,
            "pipeline": pipeline_result,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process file: {str(e)}\n{traceback.format_exc()}"
        )


@app.post("/api/upload")
async def upload_dataset(
    file: UploadFile = File(...),
    x_user_role: str = Header(default="customer", alias="X-User-Role"),
):
    """
    Upload a CSV or Excel file for processing.
    Auto-detects data type and saves to raw data directory.
    Then runs the transformation pipeline.
    """
    if x_user_role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Validate file type
    filename = file.filename or "uploaded_data"
    allowed_extensions = {".csv", ".xlsx", ".xls", ".tsv"}
    file_ext = Path(filename).suffix.lower()
    
    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{file_ext}'. Allowed: {', '.join(allowed_extensions)}"
        )
    
    try:
        import pandas as pd
        
        # Read the uploaded file into a DataFrame
        contents = await file.read()
        
        if file_ext == ".csv":
            import io
            # Try multiple encodings
            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    df = pd.read_csv(io.BytesIO(contents), encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise HTTPException(status_code=400, detail="Could not decode CSV file")
        elif file_ext == ".tsv":
            import io
            df = pd.read_csv(io.BytesIO(contents), sep='\t')
        elif file_ext in (".xlsx", ".xls"):
            import io
            df = pd.read_excel(io.BytesIO(contents))
        
        if df.empty:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")
        
        # Auto-detect what type of data this is and save to raw/
        raw_dir = PROJECT_ROOT / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        tables_detected = auto_detect_and_save(df, raw_dir, filename)
        
        # Clear the KPI table cache so new data is picked up
        import src.analytics.kpi_queries as kpi_mod
        kpi_mod._table_cache = None
        
        # Run the transformation pipeline
        pipeline_result = run_transformation_pipeline()
        
        return {
            "status": "success",
            "message": f"File '{filename}' processed successfully",
            "rows": len(df),
            "columns": list(df.columns),
            "tables_detected": tables_detected,
            "pipeline": pipeline_result,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")


def auto_detect_and_save(df, raw_dir: Path, original_filename: str) -> list:
    """
    Auto-detect the type of data in the DataFrame and save as appropriate CSV files.
    Returns list of detected table types.
    """
    import re
    
    columns_lower = [c.lower() for c in df.columns]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tables_detected = []
    
    # Check for transaction-like data
    transaction_indicators = ['transaction', 'order', 'invoice', 'sale', 'purchase', 'amount', 'total', 'price']
    has_transaction = any(any(ind in col for ind in transaction_indicators) for col in columns_lower)
    
    # Check for user/customer-like data
    user_indicators = ['user', 'customer', 'client', 'member', 'name', 'email']
    has_user = any(any(ind in col for ind in user_indicators) for col in columns_lower)
    
    # Check for product-like data
    product_indicators = ['product', 'item', 'sku', 'category', 'brand']
    has_product = any(any(ind in col for ind in product_indicators) for col in columns_lower)
    
    # Check for inventory-like data
    inventory_indicators = ['stock', 'inventory', 'warehouse', 'reorder', 'quantity_on_hand']
    has_inventory = any(any(ind in col for ind in inventory_indicators) for col in columns_lower)
    
    # Check for shipment-like data
    shipment_indicators = ['shipment', 'shipping', 'delivery', 'carrier', 'tracking', 'shipped']
    has_shipment = any(any(ind in col for ind in shipment_indicators) for col in columns_lower)
    
    # Normalize column names for saving
    col_mapping = normalize_columns(df.columns.tolist())
    df_normalized = df.rename(columns=col_mapping)
    
    # Compute derived columns after normalization
    if 'amount' not in df_normalized.columns and 'price' in df_normalized.columns:
        if 'quantity' in df_normalized.columns:
            df_normalized['amount'] = df_normalized['price'] * df_normalized['quantity']
        else:
            df_normalized['amount'] = df_normalized['price']
    
    # Generate store_id from country or other fields if missing
    if 'store_id' not in df_normalized.columns:
        if 'country' in df_normalized.columns:
            df_normalized['store_id'] = df_normalized['country']
        else:
            df_normalized['store_id'] = 'S001'
    
    if has_transaction and has_product and has_user:
        # This is a combined transaction dataset - split it
        tables_detected.extend(split_and_save_combined(df_normalized, raw_dir, timestamp))
    elif has_transaction:
        df_normalized.to_csv(raw_dir / f"transactions_{timestamp}.csv", index=False)
        tables_detected.append("transactions")
    elif has_user and not has_product:
        df_normalized.to_csv(raw_dir / f"users_{timestamp}.csv", index=False)
        tables_detected.append("users")
    elif has_product and not has_transaction:
        if has_inventory:
            df_normalized.to_csv(raw_dir / f"inventory_{timestamp}.csv", index=False)
            tables_detected.append("inventory")
        else:
            df_normalized.to_csv(raw_dir / f"products_{timestamp}.csv", index=False)
            tables_detected.append("products")
    elif has_shipment:
        df_normalized.to_csv(raw_dir / f"shipments_{timestamp}.csv", index=False)
        tables_detected.append("shipments")
    elif has_inventory:
        df_normalized.to_csv(raw_dir / f"inventory_{timestamp}.csv", index=False)
        tables_detected.append("inventory")
    else:
        # Unknown schema - save as transactions with best-effort column mapping
        df_mapped = best_effort_transaction_mapping(df)
        df_mapped.to_csv(raw_dir / f"transactions_{timestamp}.csv", index=False)
        tables_detected.append("transactions (auto-mapped)")
    
    return tables_detected


def normalize_columns(columns: list) -> dict:
    """Map common column name variations to standard names."""
    import re
    
    mapping = {}
    standard_names = {
        # Transaction fields
        r'(transaction|order|invoice|sale)[\s_]*(id|no|number|#)?$': 'transaction_id',
        r'(user|customer|client|member)[\s_]*(id|no|number)?$': 'user_id',
        r'(product|item|sku|stock)[\s_]*(id|no|number|code)?$': 'product_id',
        r'(store|location|branch|shop|country)[\s_]*(id|no|number)?$': 'store_id',
        r'(amount|total|revenue|sales?)[\s_]*(amount)?$': 'amount',
        r'(unit[\s_]*)?price$': 'price',
        r'(date|time|timestamp|created|ordered|invoice[\s_]*date)[\s_]*(at|on|stamp)?$': 'timestamp',
        r'quantity|qty$': 'quantity',
        # User fields
        r'(full[\s_]*)?name$': 'name',
        r'email[\s_]*(address)?$': 'email',
        r'city$': 'city',
        r'(signup|registration|created|join)[\s_]*(date|at)?$': 'signup_date',
        # Product fields
        r'(product[\s_]*)?name|description$': 'product_name',
        r'category$': 'category',
        # Inventory fields
        r'stock[\s_]*(level|qty|quantity)?$': 'stock_level',
        r'reorder[\s_]*(point|level)$': 'reorder_point',
        r'(last[\s_]*)?restock[\s_]*(date)?$': 'last_restock_date',
        r'stock[\s_]*status$': 'stock_status',
    }
    
    for col in columns:
        col_lower = col.lower().strip()
        matched = False
        for pattern, standard in standard_names.items():
            if re.match(pattern, col_lower, re.IGNORECASE):
                mapping[col] = standard
                matched = True
                break
        if not matched:
            # Clean up the column name (replace spaces with underscores, lowercase)
            mapping[col] = re.sub(r'[^\w]', '_', col.lower()).strip('_')
    
    return mapping


def split_and_save_combined(df, raw_dir: Path, timestamp: str) -> list:
    """Split a combined dataset into separate tables."""
    tables = []
    
    # Extract unique users if user columns exist
    user_cols = [c for c in df.columns if any(x in c.lower() for x in ['user_id', 'name', 'email', 'city', 'signup'])]
    if user_cols and 'user_id' in df.columns:
        users_df = df[user_cols].drop_duplicates(subset=['user_id'])
        if 'signup_date' not in users_df.columns:
            users_df['signup_date'] = datetime.now().strftime('%Y-%m-%d')
        users_df.to_csv(raw_dir / f"users_{timestamp}.csv", index=False)
        tables.append("users")
    
    # Extract unique products if product columns exist
    product_cols = [c for c in df.columns if any(x in c.lower() for x in ['product_id', 'product_name', 'category', 'price'])]
    if product_cols and 'product_id' in df.columns:
        products_df = df[product_cols].drop_duplicates(subset=['product_id'])
        products_df.to_csv(raw_dir / f"products_{timestamp}.csv", index=False)
        tables.append("products")
    
    # Save transaction data
    txn_cols = [c for c in df.columns if any(x in c.lower() for x in ['transaction_id', 'user_id', 'product_id', 'amount', 'timestamp', 'store_id'])]
    if txn_cols:
        txn_df = df[txn_cols]
        txn_df.to_csv(raw_dir / f"transactions_{timestamp}.csv", index=False)
        tables.append("transactions")
    
    return tables if tables else ["transactions"]


def best_effort_transaction_mapping(df) -> 'pd.DataFrame':
    """Map unknown columns to transaction schema using heuristics."""
    import pandas as pd
    
    result = pd.DataFrame()
    columns_lower = {c.lower(): c for c in df.columns}
    
    # Try to find an ID column
    id_col = None
    for pattern in ['id', 'number', 'no', 'key']:
        for col_lower, col_orig in columns_lower.items():
            if pattern in col_lower:
                id_col = col_orig
                break
        if id_col:
            break
    
    if id_col:
        result['transaction_id'] = df[id_col].astype(str)
    else:
        result['transaction_id'] = [f"TXN_{i}" for i in range(len(df))]
    
    # Find numeric columns for amount
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    if numeric_cols:
        result['amount'] = df[numeric_cols[0]]
    else:
        result['amount'] = 1.0
    
    # Add minimal required fields
    result['user_id'] = 'U001'
    result['product_id'] = 'P001'
    result['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    result['store_id'] = 'S001'
    
    return result


def run_transformation_pipeline() -> dict:
    """Run the Bronze -> Silver -> Gold transformation pipeline."""
    try:
        pipeline_path = PROJECT_ROOT / "src" / "transformation" / "pipeline.py"
        
        result = subprocess.run(
            [sys.executable, str(pipeline_path)],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=120,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
        
        if result.returncode == 0:
            return {"status": "success", "message": "Pipeline completed", "output": result.stdout}
        else:
            return {"status": "error", "message": "Pipeline failed", "error": result.stderr}
    except subprocess.TimeoutExpired:
        return {"status": "error", "message": "Pipeline timed out (>120s)"}
    except Exception as e:
        return {"status": "error", "message": f"Pipeline error: {str(e)}"}


@app.post("/api/pipeline/run")
async def trigger_pipeline(x_user_role: str = Header(default="customer", alias="X-User-Role")):
    """Manually trigger the transformation pipeline."""
    if x_user_role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Clear the KPI table cache
    import src.analytics.kpi_queries as kpi_mod
    kpi_mod._table_cache = None
    
    result = run_transformation_pipeline()
    return result


@app.delete("/api/data/reset")
async def reset_data(x_user_role: str = Header(default="customer", alias="X-User-Role")):
    """Clear all data and reset to empty state."""
    if x_user_role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Clear data directories (but NOT streaming - it needs to persist)
        for dir_name in ["raw", "silver", "gold"]:
            dir_path = PROJECT_ROOT / "data" / dir_name
            if dir_path.exists():
                shutil.rmtree(dir_path)
                dir_path.mkdir(parents=True, exist_ok=True)
        
        # Clear streaming buffer but keep directory structure
        streaming_dir = PROJECT_ROOT / "data" / "streaming"
        streaming_dir.mkdir(parents=True, exist_ok=True)
        
        streaming_buffer = streaming_dir / "events.jsonl"
        marker_file = streaming_dir / "last_processed.txt"
        seed_marker = streaming_dir / ".seeded"
        
        # Clear existing files
        if streaming_buffer.exists():
            streaming_buffer.unlink()
        if marker_file.exists():
            marker_file.unlink()
        # Also clear the seed marker so the next stream start will fully reseed
        # users/products/inventory, not assume prior base data still exists.
        if seed_marker.exists():
            seed_marker.unlink()
        
        # Recreate marker at 0
        marker_file.write_text("0")
        
        # Ensure staging directory exists for column mapping
        staging_dir = PROJECT_ROOT / "data" / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Clear the KPI table cache
        import src.analytics.kpi_queries as kpi_mod
        kpi_mod._table_cache = None
        
        return {"status": "success", "message": "All data has been cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reset data: {str(e)}")


# ── Data Quality Endpoints ──────────────────────────

@app.get("/api/data-quality/kpis")
def get_data_quality_kpis(role: str = Header(default="customer", alias="X-User-Role")):
    """Get data quality KPIs computed from actual data."""
    try:
        import duckdb
        
        gold_dir = PROJECT_ROOT / "data" / "gold"
        fact_txn_path = gold_dir / "fact_transactions.parquet"
        
        if not fact_txn_path.exists():
            return {"completeness": 0, "accuracy": 0, "consistency": 0, "timeliness": 0}
        
        conn = duckdb.connect()
        gold_path = str(gold_dir).replace("\\", "/")
        
        # Completeness: % of non-null values in key columns
        completeness_result = conn.sql(f"""
            SELECT 
                (1.0 - (
                    CAST(SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) +
                         SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) +
                         SUM(CASE WHEN user_key IS NULL THEN 1 ELSE 0 END) AS DOUBLE) /
                    (COUNT(*) * 3.0)
                )) * 100 as completeness
            FROM read_parquet('{gold_path}/fact_transactions.parquet')
        """).fetchone()
        
        # Accuracy: % of positive amounts
        accuracy_result = conn.sql(f"""
            SELECT 
                CAST(SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) * 100 as accuracy
            FROM read_parquet('{gold_path}/fact_transactions.parquet')
        """).fetchone()
        
        # Consistency: % of records with valid foreign keys
        consistency_result = conn.sql(f"""
            SELECT 
                CAST(SUM(CASE WHEN user_key != -1 AND product_key != -1 THEN 1 ELSE 0 END) AS DOUBLE) / 
                NULLIF(COUNT(*), 0) * 100 as consistency
            FROM read_parquet('{gold_path}/fact_transactions.parquet')
        """).fetchone()
        
        conn.close()
        
        return {
            "completeness": round(completeness_result[0] or 0, 1),
            "accuracy": round(accuracy_result[0] or 0, 1),
            "consistency": round(consistency_result[0] or 0, 1),
            "timeliness": 95.0,  # Would need timestamp comparison for real-time
        }
    except Exception as e:
        return {"completeness": 0, "accuracy": 0, "consistency": 0, "timeliness": 0}


@app.get("/api/data-quality/trend")
def get_data_quality_trend(role: str = Header(default="customer", alias="X-User-Role")):
    """Get data quality trends over time."""
    try:
        import duckdb
        
        gold_dir = PROJECT_ROOT / "data" / "gold"
        fact_txn_path = gold_dir / "fact_transactions.parquet"
        
        if not fact_txn_path.exists():
            return []
        
        conn = duckdb.connect()
        gold_path = str(gold_dir).replace("\\", "/")
        
        df = conn.sql(f"""
            SELECT 
                strftime(timestamp, '%Y-%m') as month,
                (1.0 - CAST(SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0)) * 100 as completeness,
                CAST(SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) * 100 as accuracy,
                CAST(SUM(CASE WHEN user_key != -1 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) * 100 as consistency
            FROM read_parquet('{gold_path}/fact_transactions.parquet')
            GROUP BY strftime(timestamp, '%Y-%m')
            ORDER BY month
        """).df()
        conn.close()
        
        return df.to_dict(orient="records")
    except Exception:
        return []


@app.get("/api/data-quality/checks")
def get_data_quality_checks(role: str = Header(default="customer", alias="X-User-Role")):
    """Get data quality check results."""
    try:
        import duckdb
        
        gold_dir = PROJECT_ROOT / "data" / "gold"
        checks = []
        now = datetime.now().isoformat()
        
        # Check each gold table
        for table_file in gold_dir.glob("*.parquet"):
            try:
                conn = duckdb.connect()
                table_path = str(table_file).replace("\\", "/")
                result = conn.sql(f"SELECT COUNT(*) as cnt FROM read_parquet('{table_path}')").fetchone()
                conn.close()
                
                checks.append({
                    "check": table_file.stem,
                    "status": "pass" if result[0] > 0 else "warning",
                    "records": result[0],
                    "issues": 0,
                    "timestamp": now,
                })
            except Exception:
                checks.append({
                    "check": table_file.stem,
                    "status": "fail",
                    "records": 0,
                    "issues": 1,
                    "timestamp": now,
                })
        
        if not checks:
            return [{
                "check": "No data",
                "status": "warning",
                "records": 0,
                "issues": 0,
                "timestamp": now,
            }]
        
        return checks
    except Exception:
        return []


# ── Streaming Endpoints ─────────────────────────────

@app.post("/api/stream/start")
async def start_stream(x_user_role: str = Header(default="customer", alias="X-User-Role")):
    """Start the real-time ingestion stream. Generates burst data on start to populate all KPIs."""
    if x_user_role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    global stream_state
    
    if stream_state["status"] == "running":
        raise HTTPException(status_code=409, detail="Stream is already running")
    
    try:
        env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
        logs_dir = PROJECT_ROOT / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Open log files for stream generator & processor so failures aren't silent.
        gen_log_path = logs_dir / "stream_generator.log"
        proc_log_path = logs_dir / "stream_processor.log"
        gen_log = open(gen_log_path, "a", encoding="utf-8")
        proc_log = open(proc_log_path, "a", encoding="utf-8")
        
        # On Windows, child processes share the same console/process group.
        # If a child crashes or exits, it can send CTRL_C_EVENT to the whole group,
        # killing the uvicorn server. CREATE_NEW_PROCESS_GROUP isolates them.
        creation_flags = 0
        if sys.platform == "win32":
            creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
        
        # Start generator (with burst_on_start=True by default)
        generator_proc = subprocess.Popen(
            [sys.executable, str(PROJECT_ROOT / "src" / "ingestion" / "stream_generator.py"),
             "--interval", "5", "--burst-on-start"],
            cwd=str(PROJECT_ROOT),
            stdout=gen_log,
            stderr=gen_log,
            stdin=subprocess.DEVNULL,
            env=env,
            creationflags=creation_flags
        )
        
        # Start processor
        processor_proc = subprocess.Popen(
            [sys.executable, str(PROJECT_ROOT / "src" / "ingestion" / "stream_processor.py"),
             "--interval", "10"],
            cwd=str(PROJECT_ROOT),
            stdout=proc_log,
            stderr=proc_log,
            stdin=subprocess.DEVNULL,
            env=env,
            creationflags=creation_flags
        )
        
        stream_state["status"] = "running"
        stream_state["started_at"] = datetime.now().isoformat()
        stream_state["generator_pid"] = generator_proc.pid
        stream_state["processor_pid"] = processor_proc.pid
        
        logger.info("Stream started with burst data generation")
        
        return {
            "status": "success",
            "message": "Stream started with initial data burst to populate all KPIs",
            "generator_pid": generator_proc.pid,
            "processor_pid": processor_proc.pid
        }
    except Exception as e:
        logger.error(f"Failed to start stream: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to start stream: {str(e)}")


@app.post("/api/stream/stop")
async def stop_stream(x_user_role: str = Header(default="customer", alias="X-User-Role")):
    """Stop the real-time ingestion stream."""
    if x_user_role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    global stream_state
    
    if stream_state["status"] == "stopped":
        raise HTTPException(status_code=409, detail="Stream is not running")
    
    try:
        for pid_key in ["generator_pid", "processor_pid"]:
            pid = stream_state.get(pid_key)
            if pid:
                try:
                    if sys.platform == "win32":
                        # DETACHED_PROCESS requires taskkill, not SIGTERM
                        subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                                       capture_output=True, timeout=5)
                    else:
                        import signal
                        os.kill(pid, signal.SIGTERM)
                except (OSError, ProcessLookupError, subprocess.TimeoutExpired):
                    pass
        
        stream_state["status"] = "stopped"
        stream_state["generator_pid"] = None
        stream_state["processor_pid"] = None
        
        return {"status": "success", "message": "Stream stopped"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop stream: {str(e)}")


@app.get("/api/stream/status")
def get_stream_status():
    """Get the current stream status."""
    if stream_state["status"] == "running":
        for pid_key in ["generator_pid", "processor_pid"]:
            pid = stream_state.get(pid_key)
            if pid:
                try:
                    os.kill(pid, 0)  # Check if process exists
                except OSError:
                    stream_state["status"] = "stopped"
                    stream_state["generator_pid"] = None
                    stream_state["processor_pid"] = None
                    break
    
    # Count events from buffer
    buffer_path = PROJECT_ROOT / "data" / "streaming" / "events.jsonl"
    events_count = 0
    if buffer_path.exists():
        with open(buffer_path, 'r') as f:
            events_count = sum(1 for _ in f)
    
    return {
        "status": stream_state["status"],
        "started_at": stream_state["started_at"],
        "events_in_buffer": events_count,
    }


# ── AI Analyst (RAG) Endpoint ───────────────────────────

@app.post("/api/ask")
async def ask_analyst(request: ChatAskRequest):
    """AI Analyst: Natural Language to SQL Query Engine."""
    try:
        from src.analytics.nl_query import ask
        
        if not request.question or not request.question.strip():
            raise HTTPException(status_code=400, detail="Question cannot be empty")
        
        # Validate question length
        if len(request.question) > 1000:
            raise HTTPException(status_code=400, detail="Question must be 1000 characters or less")
        
        result = ask(request.question.strip())
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to process question: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to process question: {str(e)}")


class ChatAskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=1000, description="Natural language question to ask the AI analyst")

@app.post("/api/chat/ask")
async def chat_ask(request: ChatAskRequest):
    """Natural language query endpoint."""
    try:
        from src.analytics.nl_query import ask
        
        if not request.question or not request.question.strip():
            raise HTTPException(status_code=400, detail="Question cannot be empty")
        
        # Validate question length
        if len(request.question) > 1000:
            raise HTTPException(status_code=400, detail="Question must be 1000 characters or less")
        
        result = ask(request.question.strip())
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to process question: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to process question: {str(e)}")


# ── Context Switching Endpoints ─────────────────────────

@app.get("/api/context/current")
async def get_current_context():
    """Get the currently active business context."""
    try:
        contexts = get_business_contexts()
        active_name = contexts.get("active_context", "retail_general")
        active_context = contexts["contexts"][active_name]
        return {
            "active_context": active_name,
            "context": active_context
        }
    except Exception as e:
        raise HTTPException(500, f"Failed to load context: {str(e)}")


@app.get("/api/context/list")
async def list_contexts(x_user_role: str = Header(default="customer", alias="X-User-Role")):
    """List all available business contexts. Admin-only."""
    if x_user_role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    try:
        contexts = get_business_contexts()
        return {
            "contexts": contexts["contexts"],
            "active_context": contexts["active_context"]
        }
    except Exception as e:
        raise HTTPException(500, f"Failed to load contexts: {str(e)}")


@app.post("/api/context/switch")
async def switch_context(
    context_name: str = Query(..., min_length=1, max_length=100, description="Name of the context to switch to"),
    x_user_role: str = Header(default="customer", alias="X-User-Role"),
):
    """Switch the active business context. Admin-only."""
    if x_user_role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Validate context_name format (alphanumeric, underscore, hyphen)
    import re
    if not re.match(r'^[a-zA-Z0-9_-]+$', context_name):
        raise HTTPException(status_code=400, detail="context_name must contain only alphanumeric characters, underscores, or hyphens")
    
    try:
        contexts = get_business_contexts()
        
        if context_name not in contexts["contexts"]:
            available = list(contexts["contexts"].keys())
            raise HTTPException(
                404,
                f"Context '{context_name}' not found. Available: {available}"
            )
        
        contexts["active_context"] = context_name
        save_business_contexts(contexts)
        logger.info(f"Switched active context to: {context_name}")
        
        return {
            "status": "success",
            "active_context": context_name,
            "context": contexts["contexts"][context_name]
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to switch context: {e}", exc_info=True)
        raise HTTPException(500, f"Failed to switch context: {str(e)}")


# ── Run with: uvicorn api.main:app --reload --port 8000 ──
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
