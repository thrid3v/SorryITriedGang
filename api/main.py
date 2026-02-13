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
from api.pipeline_runner import run_full_pipeline, run_generator, run_pipeline
from fastapi import BackgroundTasks

# ── FastAPI App ──────────────────────────────────────
app = FastAPI(
    title="RetailNexus API",
    description="Analytics API for retail data lakehouse",
    version="1.0.0",
)

# ── CORS Configuration ───────────────────────────────
# Allow React dev server (localhost:5173) to call API
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
    
    Returns:
        {
            "total_revenue": 559172.02,
            "active_users": 50,
            "total_orders": 400
        }
    """
    try:
        return compute_summary_kpis()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute KPIs: {str(e)}")


@app.get("/api/clv", response_model=List[Dict])
def get_clv():
    """
    Get Customer Lifetime Value analysis.
    
    Returns list of customers with CLV metrics:
        [
            {
                "user_id": "USR_0001",
                "customer_name": "John Doe",
                "customer_city": "New York",
                "purchase_count": 15,
                "total_spend": 2500.50,
                "avg_order_value": 166.70,
                "customer_lifespan_days": 120,
                "estimated_clv": 2500.50
            },
            ...
        ]
    """
    try:
        df = compute_clv()
        # Replace NaN/inf with None for valid JSON, then convert
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute CLV: {str(e)}")


@app.get("/api/basket", response_model=List[Dict])
def get_market_basket(min_support: int = 2):
    """
    Get market basket analysis - product pairs frequently bought together.
    
    Args:
        min_support: Minimum number of times products must be bought together (default: 2)
    
    Returns list of product pairs:
        [
            {
                "product_a_name": "Wireless Earbuds",
                "product_b_name": "Smartphone Case",
                "times_bought_together": 12,
                "product_a": 1,
                "product_b": 6
            },
            ...
        ]
    """
    try:
        df = compute_market_basket(min_support=min_support)
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


@app.post("/api/pipeline/run", status_code=202)
async def trigger_pipeline(background_tasks: BackgroundTasks, num_transactions: int = 200):
    """
    Trigger the full data pipeline (generator + transformation).
    Runs in background to avoid blocking the API server.
    
    Args:
        num_transactions: Number of transactions to generate (default: 200)
    
    Returns:
        {
            "status": "accepted",
            "message": "Pipeline started in background"
        }
    
    Note: Returns 202 Accepted immediately. Pipeline runs asynchronously.
    Check /api/health for data availability after completion.
    """
    background_tasks.add_task(run_full_pipeline, num_transactions)
    return {
        "status": "accepted",
        "message": f"Pipeline started in background (generating {num_transactions} transactions)"
    }


@app.post("/api/pipeline/generate", status_code=202)
async def trigger_generator(background_tasks: BackgroundTasks, num_transactions: int = 200):
    """
    Trigger just the data generator in background.
    
    Args:
        num_transactions: Number of transactions to generate (default: 200)
    """
    background_tasks.add_task(run_generator, num_transactions)
    return {
        "status": "accepted",
        "message": f"Generator started in background ({num_transactions} transactions)"
    }


# ── Run with: uvicorn api.main:app --reload --port 8000 ──
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
