"""
RetailNexus - Flask API
========================
Exposes Gold-layer analytics as a REST API for React/Frontend consumption.
Reuses the existing vectorized DuckDB logic from src/analytics/kpi_queries.py.
"""

import sys
from pathlib import Path
from flask import Flask, jsonify, request
from flask_cors import CORS

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import existing logic
from src.analytics.kpi_queries import (
    compute_summary_kpis,
    compute_clv,
    compute_market_basket,
)

app = Flask(__name__)
CORS(app)  # Enable Cross-Origin Resource Sharing for React


# --- API Routes ---

@app.route("/api/v1/kpis/summary", methods=["GET"])
def get_summary_kpis():
    """
    Returns top-level metrics for dashboard cards.
    
    Response:
    {
        "status": "success",
        "data": {
            "total_revenue": 296667.0,
            "active_users": 50,
            "total_orders": 200
        }
    }
    """
    try:
        data = compute_summary_kpis()
        return jsonify({
            "status": "success",
            "data": data
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/v1/kpis/clv", methods=["GET"])
def get_clv_data():
    """
    Returns Customer Lifetime Value data as JSON records.
    
    Query params:
    - limit: int (default: 50) - max number of records to return
    
    Response:
    {
        "status": "success",
        "count": 50,
        "data": [
            {
                "user_id": "USR_0001",
                "customer_name": "John Doe",
                "customer_city": "New York",
                "purchase_count": 15,
                "total_spend": 5432.10,
                "avg_order_value": 362.14,
                "customer_lifespan_days": 120,
                "estimated_clv": 5432.10
            },
            ...
        ]
    }
    """
    try:
        limit = request.args.get("limit", default=50, type=int)
        df = compute_clv()
        
        # Apply limit
        if limit > 0:
            df = df.head(limit)
        
        # Convert DataFrame to list of dicts for JSON serialization
        records = df.to_dict(orient="records")
        
        return jsonify({
            "status": "success",
            "count": len(records),
            "data": records
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/v1/kpis/market-basket", methods=["GET"])
def get_market_basket():
    """
    Returns co-purchase pairs for Market Basket Analysis.
    
    Query params:
    - support: int (default: 2) - minimum times products must be bought together
    - limit: int (default: 50) - max number of pairs to return
    
    Response:
    {
        "status": "success",
        "count": 214,
        "data": [
            {
                "product_a_name": "Wireless Earbuds",
                "product_b_name": "Smartphone Case",
                "times_bought_together": 12,
                "product_a": 1,
                "product_b": 6
            },
            ...
        ]
    }
    """
    try:
        support = request.args.get("support", default=2, type=int)
        limit = request.args.get("limit", default=50, type=int)
        
        df = compute_market_basket(min_support=support)
        
        # Apply limit
        if limit > 0:
            df = df.head(limit)
        
        records = df.to_dict(orient="records")
        
        return jsonify({
            "status": "success",
            "count": len(records),
            "data": records
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for monitoring."""
    return jsonify({
        "status": "healthy",
        "service": "retail-nexus-api",
        "version": "1.0.0"
    })


@app.route("/", methods=["GET"])
def root():
    """API documentation."""
    return jsonify({
        "service": "RetailNexus Analytics API",
        "version": "1.0.0",
        "endpoints": {
            "/health": "Health check",
            "/api/v1/kpis/summary": "Summary KPIs (revenue, users, orders)",
            "/api/v1/kpis/clv": "Customer Lifetime Value data",
            "/api/v1/kpis/market-basket": "Market Basket Analysis (product pairs)"
        }
    })


# --- Main ---

if __name__ == "__main__":
    print("[START] RetailNexus API starting...")
    print("Local:   http://localhost:5000")
    print("Network: http://0.0.0.0:5000")
    print("\nAPI Docs: http://localhost:5000/")
    print("Health:   http://localhost:5000/health")
    
    # In production, use a production WSGI server like Gunicorn:
    # gunicorn -w 4 -b 0.0.0.0:5000 src.api.app:app
    app.run(host="0.0.0.0", port=5000, debug=True)
