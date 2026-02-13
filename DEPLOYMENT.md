# RetailNexus — Flask + React Deployment Guide

## 📦 What Was Built

### Backend: Flask API (`src/api/`)
- **4 REST endpoints** exposing DuckDB analytics as JSON
- **CORS enabled** for cross-origin requests from React
- **Query parameters** for filtering (limit, support)
- **Health check** endpoint for monitoring

### Frontend: React App (`webapp/`)
- **Vite** for fast dev server and optimized builds
- **Tailwind CSS** for modern, responsive styling
- **Recharts** for data visualization
- **Axios** for API communication
- **Auto-refresh** every 60 seconds

---

## 🚀 Quick Start

### 1. Install Dependencies

#### Backend (Flask API)
```bash
pip install flask flask-cors
```

#### Frontend (React)
```bash
cd webapp
npm install
```

---

### 2. Run the Stack

#### Terminal 1: Start Flask API
```bash
python src/api/app.py
```
✅ API running at **http://localhost:5000**

#### Terminal 2: Start React Dev Server
```bash
cd webapp
npm run dev
```
✅ Frontend running at **http://localhost:3000**

---

### 3. Verify It Works

1. **Test API directly**:
   ```bash
   curl http://localhost:5000/api/v1/kpis/summary
   ```

2. **Open React app** in browser:
   ```
   http://localhost:3000
   ```

3. **Check data flow**:
   - KPI cards should show real revenue/users/orders
   - CLV chart should display top 10 customers
   - Market Basket table should list product pairs

---

## 🔄 Full Pipeline Test

Run this to generate fresh data and see it in the React dashboard:

```bash
# Generate new data
python src/ingestion/generator.py
python src/transformation/cleaner.py
python src/transformation/scd_logic.py
python src/transformation/star_schema.py

# Refresh the React dashboard (it auto-refreshes every 60s, or click "Refresh" button)
```

---

## 🏗️ Project Structure

```
SorryITriedGang/
├── src/
│   ├── api/
│   │   └── app.py              # Flask API server
│   ├── analytics/
│   │   ├── kpi_queries.py      # DuckDB query logic (reused by API)
│   │   └── storage_utils.py    # Parquet/security utilities
│   ├── ingestion/
│   │   └── generator.py        # Data generator
│   └── transformation/
│       ├── cleaner.py          # Bronze → Silver
│       ├── scd_logic.py        # SCD Type 2
│       └── star_schema.py      # Silver → Gold
├── webapp/
│   ├── src/
│   │   ├── components/
│   │   │   ├── KpiCard.jsx
│   │   │   ├── CLVChart.jsx
│   │   │   └── MarketBasketTable.jsx
│   │   ├── services/
│   │   │   └── api.js          # Axios API client
│   │   ├── App.jsx             # Main React component
│   │   ├── main.jsx            # React entry point
│   │   └── index.css           # Tailwind styles
│   ├── package.json
│   ├── vite.config.js
│   └── tailwind.config.js
└── data/
    ├── raw/                    # Generated CSVs
    ├── silver/                 # Cleaned Parquet
    └── gold/                   # Star schema (DuckDB reads this)
```

---

## 🌐 Production Deployment

### Backend (Flask API)

**Option 1: Gunicorn (Linux/Mac)**
```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 src.api.app:app
```

**Option 2: Waitress (Windows)**
```bash
pip install waitress
waitress-serve --port=5000 src.api.app:app
```

**Option 3: Docker**
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "src.api.app:app"]
```

### Frontend (React)

**Build for production:**
```bash
cd webapp
npm run build
```

**Deploy to:**
- **Vercel**: `vercel deploy`
- **Netlify**: `netlify deploy --prod`
- **Static hosting**: Upload `webapp/dist/` to any CDN

**Environment variable** (for production API URL):
```bash
# .env.production
VITE_API_URL=https://your-api-domain.com
```

---

## 🔧 Troubleshooting

### CORS Errors
If React can't reach the API:
1. Ensure Flask API is running on port 5000
2. Check `flask_cors` is installed: `pip install flask-cors`
3. Verify Vite proxy in `webapp/vite.config.js`

### Empty Dashboard
If KPI cards show $0:
1. Run the full pipeline to populate `data/gold/`
2. Check Flask API returns data: `curl http://localhost:5000/api/v1/kpis/summary`
3. Check browser console for API errors

### Port Already in Use
```bash
# Kill process on port 5000 (Flask)
netstat -ano | findstr :5000
taskkill /PID <PID> /F

# Kill process on port 3000 (React)
netstat -ano | findstr :3000
taskkill /PID <PID> /F
```

---

## ✅ Migration Complete!

You now have a **production-ready** decoupled architecture:
- ✅ Flask REST API serving analytics from DuckDB
- ✅ React SPA with modern UI and real-time refresh
- ✅ Reusable components and API service layer
- ✅ Ready for cloud deployment (Vercel + AWS/GCP)
