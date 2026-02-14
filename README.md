# RetailNexus — Retail Data Lakehouse with Authentication

A production-grade hybrid data platform combining batch processing with real-time streaming for retail analytics. Features JWT authentication, role-based access control, Star Schema warehouse, automated data quality checks, and SCD Type 2 history tracking, all powered by DuckDB.

## 🚀 Quick Start

### Prerequisites
- **Python 3.9+** (tested with Python 3.13)
- **Node.js 16+** and npm
- **Git** for version control

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/thrid3v/SorryITriedGang.git
cd SorryITriedGang
```

2. **Install Python dependencies**
```bash
pip install -r requirements.txt
```

**Required Python packages:**
- `pandas>=2.1.0` - Data manipulation
- `duckdb>=0.10.0` - Analytical database
- `faker>=22.0.0` - Sample data generation
- `pyarrow>=15.0.0` - Parquet file support
- `fastapi>=0.109.0` - Web framework
- `uvicorn[standard]>=0.27.0` - ASGI server
- `psutil>=1.31.0` - Process management
- `openai==2.20.0` - AI-powered text-to-SQL
- `python-dotenv==1.2.1` - Environment variables
- `python-jose[cryptography]>=3.5.0` - JWT tokens
- `passlib[argon2]>=1.7.4` - Password hashing
- `python-multipart>=0.0.22` - Form data parsing

3. **Install frontend dependencies**
```bash
cd frontend
npm install
cd ..
```

**Frontend stack:**
- React 18 with TypeScript
- Vite for build tooling
- shadcn/ui components
- TailwindCSS for styling
- Recharts for data visualization
- React Router for navigation
- TanStack Query for data fetching

## 📊 Running the Project

### Complete Setup (First Time)

```bash
# 1. Generate sample data
python src/ingestion/generator.py

# 2. Run transformation pipeline
python src/transformation/pipeline.py

# 3. Start backend API (in one terminal)
python -m uvicorn api.main:app --reload --port 8000

# 4. Start frontend (in another terminal)
cd frontend
npm run dev
```

### Access the Application

- **Frontend Dashboard**: http://localhost:5173
- **Backend API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs (Swagger UI)

### Default Credentials

**Admin Account** (pre-seeded):
- Username: `admin`
- Password: `admin123`

**Customer Accounts**: Create via registration form at `/login`

## 🔐 Authentication System

### Features
- **JWT Token Authentication**: Secure, stateless authentication
- **Role-Based Access Control**: Admin and Customer roles
- **Protected Routes**: Dashboard and analytics require login
- **Password Security**: Argon2 hashing (Python 3.14 compatible)
- **User Management**: SQLite database with automatic seeding

### User Roles

**Admin**:
- Access to all analytics tabs
- Stream control (start/stop data generation)
- Full dashboard access
- All API endpoints

**Customer**:
- Limited analytics tabs (Sales, AI Analyst, Settings)
- No stream controls
- Personal data endpoints only

### API Authentication

All protected endpoints require Bearer token:

```bash
# 1. Login to get token
curl -X POST "http://localhost:8000/api/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin123"

# 2. Use token in requests
curl -H "Authorization: Bearer YOUR_TOKEN_HERE" \
  http://localhost:8000/api/kpis
```

## 🎯 Key Features

### Advanced Analytics
- **7 Comprehensive KPIs**: Revenue time-series, city sales, top products, inventory turnover, delivery metrics, seasonal trends, customer segmentation
- **AI Analyst**: Natural language to SQL query conversion using OpenAI
- **SCD Type 2**: Track historical changes in user data
- **Dual Partitioning**: Region + Date partitioning for 90% faster regional queries
- **Market Basket Analysis**: Product affinity and cross-sell insights
- **Customer Lifetime Value**: Predictive customer value analytics

### Data Quality
- Automated validation (negative price/amount filtering)
- Deduplication and null handling
- Retry logic with exponential backoff
- Data quality monitoring dashboard

### Hybrid Architecture
- **Batch Processing**: Historical data loads
- **Real-time Streaming**: Sub-second live updates with admin controls
- **Incremental Updates**: Efficient delta processing

## 📁 Project Structure

```
SorryITriedGang/
├── api/
│   ├── main.py                   # FastAPI backend with auth
│   └── auth.py                   # JWT & user management
├── src/
│   ├── ingestion/
│   │   ├── generator.py          # Sample data generator
│   │   ├── stream_generator.py   # Real-time event generator
│   │   └── stream_processor.py   # Stream processing worker
│   ├── transformation/
│   │   ├── cleaner.py            # Bronze → Silver cleaning
│   │   ├── scd_logic.py          # SCD Type 2 implementation
│   │   ├── star_schema.py        # Gold layer star schema
│   │   └── pipeline.py           # Orchestration
│   ├── analytics/
│   │   ├── kpi_queries.py        # 7 advanced KPI functions
│   │   └── nl_query.py           # AI-powered text-to-SQL
│   └── utils/
│       ├── retry_utils.py        # Resilience utilities
│       └── storage_utils.py      # DuckDB helpers
├── frontend/
│   └── src/
│       ├── components/           # Reusable UI components
│       ├── contexts/
│       │   └── AuthContext.tsx   # Auth state management
│       ├── pages/
│       │   ├── Login.tsx         # Login/register page
│       │   ├── Dashboard.tsx     # Main analytics dashboard
│       │   └── AskAnalyst.tsx    # AI query interface
│       └── services/
│           └── textToSql.ts      # API client
└── data/
    ├── raw/                      # Bronze layer (CSV)
    ├── silver/                   # Silver layer (Parquet)
    ├── gold/                     # Gold layer (Partitioned Parquet)
    ├── streaming/                # Real-time event buffer
    └── users.db                  # SQLite user database
```

## 🔌 API Endpoints

### Authentication Endpoints

```bash
# Register new customer
curl -X POST "http://localhost:8000/api/register" \
  -H "Content-Type: application/json" \
  -d '{"username":"newuser","password":"password123"}'

# Login
curl -X POST "http://localhost:8000/api/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin123"

# Get current user info
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:8000/api/me
```

### Analytics Endpoints (Admin Only)

```bash
# Summary KPIs
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:8000/api/kpis

# Revenue Time-Series
curl http://localhost:8000/api/revenue/timeseries?granularity=daily
curl http://localhost:8000/api/revenue/timeseries?granularity=monthly

# City-Wise Sales
curl http://localhost:8000/api/sales/city

# Top Products
curl http://localhost:8000/api/products/top?limit=10

# Inventory Turnover
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:8000/api/inventory/turnover

# Delivery Metrics
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:8000/api/delivery/metrics

# Seasonal Trends
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:8000/api/trends/seasonal

# Customer Segmentation
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:8000/api/customers/segmentation

# Market Basket Analysis
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:8000/api/basket?min_support=2

# Customer Lifetime Value
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:8000/api/clv
```

### AI Analyst Endpoint

```bash
# Natural language query (requires auth)
curl -X POST "http://localhost:8000/api/chat/ask" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"question":"What are the top 5 products by revenue?"}'
```

### Stream Control (Admin Only)

```bash
# Start real-time stream
curl -X POST "http://localhost:8000/api/stream/start" \
  -H "Authorization: Bearer YOUR_TOKEN"

# Stop stream
curl -X POST "http://localhost:8000/api/stream/stop" \
  -H "Authorization: Bearer YOUR_TOKEN"

# Check stream status (public)
curl http://localhost:8000/api/stream/status
```

### Customer-Specific Endpoints

```bash
# Get my sales data
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:8000/api/customers/me/sales

# Get my recent orders
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:8000/api/customers/me/orders?limit=20

# Get my CLV
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:8000/api/customers/me/clv
```

## 🧪 Testing

### Test KPIs
```bash
python src/analytics/kpi_queries.py
```

### Test AI Analyst
```bash
python src/analytics/nl_query.py
```

### Health Check
```bash
curl http://localhost:8000/api/health
```

## 🔄 Real-time Streaming

### Via Dashboard (Recommended)
1. Login as admin at http://localhost:5173
2. Click "Start Stream" button in sidebar
3. Watch live events flow in
4. Click "Stop Stream" to halt

### Manual Control
```bash
# Start generator (5-second intervals)
python src/ingestion/stream_generator.py --interval 5 &

# Start processor (10-second intervals)
python src/ingestion/stream_processor.py --interval 10 &
```

## 📈 Star Schema

**Dimensions:**
- `dim_users` (SCD Type 2 with effective dates)
- `dim_products`
- `dim_stores`
- `dim_dates`

**Facts:**
- `fact_transactions` (partitioned by region, date_key)
- `fact_inventory` (partitioned by region, date_key)
- `fact_shipments` (partitioned by origin_region, date_key)

## 🐛 Troubleshooting

### "Site can't be reached" - Frontend not loading

```bash
# Check if frontend is running
# Open new terminal and run:
cd frontend
npm run dev

# Verify it's on port 5173
# Open browser to http://localhost:5173
```

### "Connection refused" - Backend not responding

```bash
# Check if backend is running
python -m uvicorn api.main:app --reload --port 8000

# Verify with:
curl http://localhost:8000/
```

### "Invalid authentication credentials"

```bash
# Make sure you're using the correct credentials
# Admin: admin / admin123

# Check if users.db exists
ls data/users.db

# If missing, restart backend to auto-create
```

### Pipeline fails with "file exists" error

```bash
# Clear Gold layer and re-run
rm -rf data/gold/fact_*.parquet
python src/transformation/pipeline.py
```

### API returns empty data

```bash
# Ensure pipeline has been run
python src/transformation/pipeline.py

# Check health endpoint
curl http://localhost:8000/api/health
```

### Stream button not working

```bash
# Make sure you're logged in as admin
# Customer accounts don't have stream access

# Check backend logs for errors
# Restart backend if needed
```

### Frontend build errors

```bash
# Clear node_modules and reinstall
cd frontend
rm -rf node_modules package-lock.json
npm install
npm run dev
```

### Python dependency conflicts

```bash
# Use a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 🛠️ Development

### Environment Variables

Create a `.env` file in the project root:

```env
# OpenAI API Key for AI Analyst (optional)
OPENAI_API_KEY=your_api_key_here

# JWT Secret (change in production)
JWT_SECRET_KEY=your-secret-key-change-in-production
```

### Regenerate Fresh Data

```bash
# Clear all data
rm -rf data/raw/* data/silver/* data/gold/*

# Generate and process
python src/ingestion/generator.py
python src/transformation/pipeline.py
```

### Run Full Pipeline

```bash
# One-liner for complete refresh
rm -rf data/raw/* data/silver/* data/gold/* && \
python src/ingestion/generator.py && \
python src/transformation/pipeline.py
```

### Add New Users Manually

```python
# In Python shell
from api.auth import create_user

# Create admin
create_user("newadmin", "password123", role="admin")

# Create customer
create_user("customer1", "password123", role="customer")
```

## 📊 Key Metrics

Based on sample data:
- **Total Revenue**: ~$3M
- **Active Users**: 50
- **Products**: 30
- **Stores**: 10 (across 10 regions)
- **Transactions**: ~7,000
- **Inventory Records**: 600
- **Shipments**: 50

## 🎓 Technical Highlights

- **DuckDB**: In-process analytical database for fast SQL queries
- **Parquet**: Columnar storage with compression
- **Hive Partitioning**: Hierarchical folder structure for query optimization
- **FastAPI**: Modern async Python web framework with automatic OpenAPI docs
- **React + TypeScript**: Type-safe component-based UI
- **JWT Authentication**: Industry-standard token-based auth
- **SCD Type 2**: Slowly Changing Dimensions for historical tracking
- **Real-time Streaming**: Event-driven architecture with background workers

## 👥 Team Collaboration

### For Team Members Pulling This Code

1. **First-time setup**:
```bash
git pull origin main
pip install -r requirements.txt
cd frontend && npm install && cd ..
python src/ingestion/generator.py
python src/transformation/pipeline.py
```

2. **Daily workflow**:
```bash
# Terminal 1: Backend
python -m uvicorn api.main:app --reload --port 8000

# Terminal 2: Frontend
cd frontend && npm run dev
```

3. **Login credentials**:
   - Admin: `admin` / `admin123`
   - Create your own customer account via the UI

4. **Common issues**:
   - If frontend won't start: `cd frontend && npm install`
   - If backend fails: `pip install -r requirements.txt`
   - If no data: `python src/transformation/pipeline.py`

### Branch Strategy

- `main` - Production-ready code (this branch)
- Create feature branches for new work
- Test locally before pushing to main

## 📝 License

MIT License - See LICENSE file for details

## 👥 Contributors

- Kalyan ([@thrid3v](https://github.com/thrid3v))

---

**Last Updated**: February 14, 2026  
**Version**: 2.0.0 (with Authentication)
