"""
RetailNexus FastAPI Backend
============================
Exposes analytics endpoints that reuse existing kpi_queries.py functions.
Serves data to the React frontend.
"""
import sys
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analytics.kpi_queries import (
    compute_summary_kpis,
    compute_clv,
    compute_market_basket,
)

# ── FastAPI App ──────────────────────────────────────
app = FastAPI(
    title="RetailNexus API",
    description="Analytics API for retail data lakehouse",
    version="1.0.0",
)

# ── CORS Configuration ───────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",  # Alternative React port
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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


@app.get("/api/kpis", response_model=Dict[str, Any])
def get_summary_kpis():
    """
    Get summary KPIs: total revenue, active users, total orders.
    """
    try:
        return compute_summary_kpis()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute KPIs: {str(e)}")


@app.get("/api/clv", response_model=List[Dict])
def get_clv():
    """
    Get Customer Lifetime Value analysis.
    """
    try:
        df = compute_clv()
        # Replace infinity and NaN with None for valid JSON serialization
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute CLV: {str(e)}")


@app.get("/api/basket", response_model=List[Dict])
def get_market_basket(min_support: int = 2):
    """
    Get market basket analysis - product pairs frequently bought together.
    """
    try:
        df = compute_market_basket(min_support=min_support)
        # Replace infinity and NaN with None for valid JSON serialization
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute market basket: {str(e)}")


@app.get("/api/health")
def health_check():
    """Detailed health check with data availability status"""
    try:
        kpis = compute_summary_kpis()
        data_available = kpis.get("total_orders", 0) > 0

        return {
            "status": "healthy",
            "data_available": data_available,
            "total_orders": kpis.get("total_orders", 0),
            "message": "API and data pipeline operational" if data_available else "API running, waiting for data"
        }
    except Exception as e:
        return {
            "status": "degraded",
            "data_available": False,
            "message": f"API running but data unavailable: {str(e)}"
        }


# ── Run with: uvicorn api.main:app --reload --port 8000 ──
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
