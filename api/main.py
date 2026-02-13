"""
RetailNexus FastAPI Backend
============================
Exposes analytics endpoints that reuse existing kpi_queries.py functions.
Serves data to the React frontend.
"""
import sys
import asyncio
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

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

# ── Streaming State ──────────────────────────────────
stream_state = {
    "status": "stopped",  # stopped | running
    "started_at": None,
    "events_processed": 0,
    "generator_pid": None,
    "processor_pid": None,
}

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


# ── Streaming Endpoints ─────────────────────────────

@app.post("/api/stream/start")
async def start_stream():
    """
    Start the real-time ingestion stream.
    Launches both generator and processor in background.
    """
    global stream_state
    
    if stream_state["status"] == "running":
        raise HTTPException(status_code=409, detail="Stream is already running")
    
    try:
        # Start generator
        generator_proc = subprocess.Popen(
            [sys.executable, str(PROJECT_ROOT / "src" / "ingestion" / "stream_generator.py"),
             "--interval", "5"],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Start processor
        processor_proc = subprocess.Popen(
            [sys.executable, str(PROJECT_ROOT / "src" / "ingestion" / "stream_processor.py"),
             "--interval", "10"],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        stream_state["status"] = "running"
        stream_state["started_at"] = datetime.now().isoformat()
        stream_state["generator_pid"] = generator_proc.pid
        stream_state["processor_pid"] = processor_proc.pid
        
        return {
            "status": "success",
            "message": "Stream started",
            "generator_pid": generator_proc.pid,
            "processor_pid": processor_proc.pid
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start stream: {str(e)}")


@app.post("/api/stream/stop")
async def stop_stream():
    """
    Stop the real-time ingestion stream.
    Terminates both generator and processor processes.
    """
    global stream_state
    
    if stream_state["status"] == "stopped":
        raise HTTPException(status_code=409, detail="Stream is not running")
    
    try:
        import psutil
        
        # Kill generator
        if stream_state["generator_pid"]:
            try:
                proc = psutil.Process(stream_state["generator_pid"])
                proc.terminate()
                proc.wait(timeout=5)
            except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                pass
        
        # Kill processor
        if stream_state["processor_pid"]:
            try:
                proc = psutil.Process(stream_state["processor_pid"])
                proc.terminate()
                proc.wait(timeout=5)
            except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                pass
        
        stream_state["status"] = "stopped"
        stream_state["generator_pid"] = None
        stream_state["processor_pid"] = None
        
        return {
            "status": "success",
            "message": "Stream stopped"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop stream: {str(e)}")


@app.get("/api/stream/status")
def get_stream_status():
    """
    Get the current stream status.
    """
    # Check if processes are actually running
    if stream_state["status"] == "running":
        try:
            import psutil
            gen_alive = stream_state["generator_pid"] and psutil.pid_exists(stream_state["generator_pid"])
            proc_alive = stream_state["processor_pid"] and psutil.pid_exists(stream_state["processor_pid"])
            
            if not (gen_alive and proc_alive):
                stream_state["status"] = "stopped"
                stream_state["generator_pid"] = None
                stream_state["processor_pid"] = None
        except ImportError:
            pass
    
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
        "generator_pid": stream_state["generator_pid"],
        "processor_pid": stream_state["processor_pid"],
    }


# ── Run with: uvicorn api.main:app --reload --port 8000 ──
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
