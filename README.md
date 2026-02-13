# RetailNexus — Retail Data Lakehouse

A production-grade hybrid data platform combining batch processing with real-time streaming for retail analytics. Features a robust Star Schema warehouse, automated data quality checks, and SCD Type 2 history tracking, all powered by DuckDB.

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Node.js 16+
- npm or yarn

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

3. **Install frontend dependencies**
```bash
cd frontend
npm install
cd ..
```

## 📊 Running the Project

### Step 1: Generate Sample Data
```bash
python src/ingestion/generator.py
```
This creates raw CSV files in `data/raw/` with realistic retail data including:
- Transactions (with intentional nulls and duplicates)
- Users, Products, Inventory, Shipments

### Step 2: Run the Transformation Pipeline
```bash
python src/transformation/pipeline.py
```
This processes data through 3 layers:
- **Bronze → Silver**: Cleaning, deduplication, validation
- **SCD Type 2**: User dimension history tracking
- **Silver → Gold**: Star schema creation with dual partitioning

### Step 3: Start the API Server
```bash
python -m uvicorn api.main:app --reload --port 8000
```
API will be available at `http://localhost:8000`

### Step 4: Start the Frontend Dashboard
```bash
cd frontend
npm run dev
```
Dashboard will be available at `http://localhost:5173`

## 🎯 Key Features

### Advanced Analytics
- **7 Comprehensive KPIs**: Revenue time-series, city sales, top products, inventory turnover, delivery metrics, seasonal trends, customer segmentation
- **SCD Type 2**: Track historical changes in user data
- **Dual Partitioning**: Region + Date partitioning for 90% faster regional queries

### Data Quality
- Automated validation (negative price/amount filtering)
- Deduplication and null handling
- Retry logic with exponential backoff

### Hybrid Architecture
- **Batch Processing**: Historical data loads
- **Real-time Streaming**: Sub-second live updates

## 📁 Project Structure

```
SorryITriedGang/
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
│   │   └── kpi_queries.py        # 7 advanced KPI functions
│   └── utils/
│       └── retry_utils.py        # Resilience utilities
├── api/
│   └── main.py                   # FastAPI backend
├── frontend/
│   └── src/                      # React dashboard
└── data/
    ├── raw/                      # Bronze layer (CSV)
    ├── silver/                   # Silver layer (Parquet)
    └── gold/                     # Gold layer (Partitioned Parquet)
```

## 🔌 API Endpoints

### Summary KPIs
```bash
curl http://localhost:8000/api/kpis
```

### Revenue Time-Series
```bash
# Daily
curl http://localhost:8000/api/revenue/timeseries?granularity=daily

# Monthly
curl http://localhost:8000/api/revenue/timeseries?granularity=monthly
```

### City-Wise Sales
```bash
curl http://localhost:8000/api/sales/city
```

### Top Products
```bash
curl http://localhost:8000/api/products/top?limit=10
```

### Inventory Turnover
```bash
curl http://localhost:8000/api/inventory/turnover
```

### Delivery Metrics
```bash
curl http://localhost:8000/api/delivery/metrics
```

### Seasonal Trends
```bash
curl http://localhost:8000/api/trends/seasonal
```

### Customer Segmentation
```bash
curl http://localhost:8000/api/customers/segmentation
```

## 🧪 Testing KPIs

Run comprehensive KPI tests:
```bash
python src/analytics/kpi_queries.py
```

## 🔄 Real-time Streaming

### Start the Stream (via Dashboard)
1. Open dashboard at `http://localhost:5173`
2. Click "Start Stream" button
3. Watch live events flow in

### Manual Stream Control
```bash
# Start generator
python src/ingestion/stream_generator.py &

# Start processor
python src/ingestion/stream_processor.py &
```

## 📈 Star Schema

The data warehouse uses a dimensional model:

**Dimensions:**
- `dim_users` (SCD Type 2)
- `dim_products`
- `dim_stores`
- `dim_dates`

**Facts:**
- `fact_transactions` (partitioned by region, date_key)
- `fact_inventory` (partitioned by region, date_key)
- `fact_shipments` (partitioned by origin_region, date_key)

## 🐛 Troubleshooting

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
```

### Frontend can't connect to API
```bash
# Check API is running on port 8000
curl http://localhost:8000/
```

## 🛠️ Development

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
- **FastAPI**: Modern async Python web framework
- **React**: Component-based UI with real-time updates

## 📝 License

MIT License - See LICENSE file for details

## 👥 Contributors

- Kalyan ([@thrid3v](https://github.com/thrid3v))
