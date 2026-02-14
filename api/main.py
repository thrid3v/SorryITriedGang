"""
RetailNexus FastAPI Backend
============================
Exposes analytics endpoints that reuse existing kpi_queries.py functions.
Serves data to the React frontend.
"""
import sys
import os
import asyncio
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel

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
)
from src.analytics.schema_inspector import load_business_context
from api.auth import (
    authenticate_user,
    create_user,
    create_access_token,
    decode_token,
    get_user,
)
from api.context_manager import get_business_contexts, save_business_contexts

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

# ── Auth Models ──────────────────────────────────────
class RegisterRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    role: str
    username: str

class UserInfo(BaseModel):
    username: str
    role: str

# ── Auth Dependencies ────────────────────────────────
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/login")

def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    """Decode JWT token and return current user info."""
    payload = decode_token(token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    username = payload.get("sub")
    if username is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    user = get_user(username)
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    
    return {"username": user["username"], "role": user["role"], "id": user["id"]}

def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    """Require admin role for endpoint access."""
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return current_user

# ── CORS Configuration ───────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://localhost:5174",  # Vite dev server (alternate port)
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


# ── Auth Endpoints ───────────────────────────────────

@app.post("/api/register", response_model=LoginResponse)
def register(request: RegisterRequest):
    """Register a new customer account."""
    # Check if user already exists
    existing_user = get_user(request.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username already exists"
        )
    
    # Create new user (always as customer)
    user = create_user(request.username, request.password, role="customer")
    
    # Generate JWT token
    access_token = create_access_token(data={"sub": user["username"], "role": user["role"]})
    
    return LoginResponse(
        access_token=access_token,
        token_type="bearer",
        role=user["role"],
        username=user["username"]
    )


@app.post("/api/login", response_model=LoginResponse)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """Login and receive JWT token."""
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Generate JWT token
    access_token = create_access_token(data={"sub": user["username"], "role": user["role"]})
    
    return LoginResponse(
        access_token=access_token,
        token_type="bearer",
        role=user["role"],
        username=user["username"]
    )


@app.get("/api/me", response_model=UserInfo)
def get_me(current_user: dict = Depends(get_current_user)):
    """Get current user information."""
    return UserInfo(
        username=current_user["username"],
        role=current_user["role"]
    )


# ── Customer-Specific Endpoints ─────────────────────

@app.get("/api/customers/me/sales", response_model=Dict[str, Any])
def get_my_sales(current_user: dict = Depends(get_current_user)):
    """
    Get sales analytics for the current customer.
    Returns purchase history, total spent, and order count.
    """
    try:
        import duckdb
        from pathlib import Path
        
        gold_path = PROJECT_ROOT / "data" / "gold"
        
        # Query customer's transactions
        query = f"""
        SELECT 
            COUNT(*) as total_orders,
            SUM(sale_price) as total_spent,
            AVG(sale_price) as avg_order_value,
            MIN(transaction_date) as first_purchase,
            MAX(transaction_date) as last_purchase
        FROM read_parquet('{gold_path}/fact_transactions.parquet')
        WHERE customer_name = '{current_user["username"]}'
        """
        
        result = duckdb.query(query).fetchdf().to_dict(orient="records")[0]
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch sales data: {str(e)}")


@app.get("/api/customers/me/orders", response_model=List[Dict])
def get_my_orders(current_user: dict = Depends(get_current_user), limit: int = 20):
    """
    Get recent orders for the current customer.
    """
    try:
        import duckdb
        from pathlib import Path
        
        gold_path = PROJECT_ROOT / "data" / "gold"
        
        query = f"""
        SELECT 
            transaction_id,
            transaction_date,
            product_name,
            category,
            sale_price,
            quantity,
            payment_method
        FROM read_parquet('{gold_path}/fact_transactions.parquet')
        WHERE customer_name = '{current_user["username"]}'
        ORDER BY transaction_date DESC
        LIMIT {limit}
        """
        
        df = duckdb.query(query).fetchdf()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch orders: {str(e)}")


@app.get("/api/customers/me/clv", response_model=Dict[str, Any])
def get_my_clv(current_user: dict = Depends(get_current_user)):
    """
    Get Customer Lifetime Value for the current customer.
    """
    try:
        import duckdb
        from pathlib import Path
        
        gold_path = PROJECT_ROOT / "data" / "gold"
        
        query = f"""
        SELECT 
            customer_name,
            COUNT(DISTINCT transaction_id) as total_orders,
            SUM(sale_price) as total_revenue,
            AVG(sale_price) as avg_order_value,
            (SUM(sale_price) * 1.5) as estimated_clv
        FROM read_parquet('{gold_path}/fact_transactions.parquet')
        WHERE customer_name = '{current_user["username"]}'
        GROUP BY customer_name
        """
        
        result = duckdb.query(query).fetchdf().to_dict(orient="records")
        if result:
            return result[0]
        return {"customer_name": current_user["username"], "total_orders": 0, "total_revenue": 0, "estimated_clv": 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch CLV: {str(e)}")



@app.get("/api/kpis", response_model=Dict[str, Any])
def get_summary_kpis(current_user: dict = Depends(require_admin)):
    """
    Get summary KPIs: total revenue, active users, total orders.
    """
    try:
        return compute_summary_kpis()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute KPIs: {str(e)}")


@app.get("/api/clv", response_model=List[Dict])
def get_clv(current_user: dict = Depends(require_admin)):
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
def get_market_basket(current_user: dict = Depends(require_admin), min_support: int = 2):
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


@app.get("/api/revenue/timeseries", response_model=List[Dict])
def get_revenue_timeseries(granularity: str = 'daily'):
    """
    Get revenue time-series (daily or monthly).
    """
    try:
        df = compute_revenue_timeseries(granularity=granularity)
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute revenue timeseries: {str(e)}")


@app.get("/api/sales/city", response_model=List[Dict])
def get_city_sales():
    """
    Get city-wise sales breakdown.
    """
    try:
        df = compute_city_sales()
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute city sales: {str(e)}")


@app.get("/api/products/top", response_model=List[Dict])
def get_top_products(limit: int = 10):
    """
    Get top-selling products by revenue.
    """
    try:
        df = compute_top_products(limit=limit)
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute top products: {str(e)}")


@app.get("/api/inventory/turnover", response_model=List[Dict])
def get_inventory_turnover(current_user: dict = Depends(require_admin)):
    """
    Get inventory turnover ratio analysis.
    """
    try:
        df = compute_inventory_turnover()
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute inventory turnover: {str(e)}")


@app.get("/api/delivery/metrics", response_model=List[Dict])
def get_delivery_metrics(current_user: dict = Depends(require_admin)):
    """
    Get delivery performance metrics by carrier and region.
    """
    try:
        df = compute_delivery_metrics()
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute delivery metrics: {str(e)}")


@app.get("/api/trends/seasonal", response_model=List[Dict])
def get_seasonal_trends(current_user: dict = Depends(require_admin)):
    """
    Get seasonal demand trends by category.
    """
    try:
        df = compute_seasonal_trends()
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute seasonal trends: {str(e)}")


@app.get("/api/customers/segmentation", response_model=List[Dict])
def get_customer_segmentation(current_user: dict = Depends(require_admin)):
    """
    Get new vs. returning customer segmentation.
    """
    try:
        df = compute_customer_segmentation()
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute customer segmentation: {str(e)}")


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
async def start_stream(current_user: dict = Depends(require_admin)):
    """
    Start the real-time ingestion stream.
    Launches both generator and processor in background.
    """
    global stream_state
    
    if stream_state["status"] == "running":
        raise HTTPException(status_code=409, detail="Stream is already running")
    
    try:
        # On Windows, the default charmap codec (cp1252) can't encode emoji
        # characters used in the generator/processor print() statements.
        # Setting PYTHONIOENCODING=utf-8 prevents UnicodeEncodeError crashes.
        env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
        
        # Start generator
        generator_proc = subprocess.Popen(
            [sys.executable, str(PROJECT_ROOT / "src" / "ingestion" / "stream_generator.py"),
             "--interval", "5"],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env
        )
        
        # Start processor
        processor_proc = subprocess.Popen(
            [sys.executable, str(PROJECT_ROOT / "src" / "ingestion" / "stream_processor.py"),
             "--interval", "10"],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env
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
async def stop_stream(current_user: dict = Depends(require_admin)):
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
    }


# ── AI Analyst (RAG) Endpoint ───────────────────────────

@app.post("/api/ask")
async def ask_analyst(request: dict):
    """
    AI Analyst: Natural Language to SQL Query Engine.
    Converts user questions into SQL queries using RAG with embeddings.
    
    Request body:
        {
            "question": "What are my top 5 products by revenue?"
        }
    """
    try:
        from src.analytics.nl_query import ask
        
        question = request.get("question", "")
        if not question:
            raise HTTPException(status_code=400, detail="Question is required")
        
        result = ask(question)
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process question: {str(e)}")


# ── Text-to-SQL Endpoint ────────────────────────────

@app.post("/api/chat/ask")
async def chat_ask(request: Dict[str, str]):
    """
    Natural language query endpoint.
    Converts English questions to SQL and returns results with AI summary.
    
    Request body:
        {
            "question": "What are my top 5 products by revenue?"
        }
    
    Returns:
        {
            "question": str,
            "sql": str,
            "data": List[Dict],
            "summary": str,
            "row_count": int,
            "error": Optional[str]
        }
    """
    try:
        from src.analytics.nl_query import ask
        
        question = request.get("question", "")
        if not question:
            raise HTTPException(status_code=400, detail="Question is required")
        
        result = ask(question)
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process question: {str(e)}")


# ── Context Switching Endpoints ─────────────────────────

@app.get("/api/context/current")
async def get_current_context():
    """
    Get the currently active business context.
    Available to all authenticated users.
    """
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
async def list_contexts(current_user: dict = Depends(require_admin)):
    """
    List all available business contexts.
    Admin-only endpoint.
    """
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
    context_name: str,
    current_user: dict = Depends(require_admin)
):
    """
    Switch the active business context.
    Admin-only endpoint.
    
    Args:
        context_name: Name of the context to switch to (e.g., "bakery", "clothing")
    """
    try:
        contexts = get_business_contexts()
        
        if context_name not in contexts["contexts"]:
            available = list(contexts["contexts"].keys())
            raise HTTPException(
                404,
                f"Context '{context_name}' not found. Available: {available}"
            )
        
        # Update active context
        contexts["active_context"] = context_name
        save_business_contexts(contexts)
        
        return {
            "status": "success",
            "active_context": context_name,
            "context": contexts["contexts"][context_name]
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Failed to switch context: {str(e)}")


# ── Run with: uvicorn api.main:app --reload --port 8000 ──
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
