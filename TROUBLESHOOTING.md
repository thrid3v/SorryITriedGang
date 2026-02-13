# Troubleshooting Guide

## Real-time Streaming Not Working

If the real-time streaming feature isn't working after cloning the repository, follow these steps:

### 1. Verify Files Exist

Check that streaming files are present:
```bash
ls -la src/ingestion/stream_generator.py
ls -la src/ingestion/stream_processor.py
```

If missing, pull the latest code:
```bash
git pull origin main
```

### 2. Create Required Directories

The streaming system needs a `data/streaming/` directory:
```bash
mkdir -p data/streaming
```

### 3. Verify Dependencies

Ensure all Python packages are installed:
```bash
pip install -r requirements.txt
```

Key dependencies for streaming:
- `faker` - For generating realistic data
- `duckdb` - For data processing

### 4. Check API Server is Running

The streaming controls require the API server:
```bash
# Start API server
python -m uvicorn api.main:app --reload --port 8000

# Verify it's running
curl http://localhost:8000/
```

### 5. Test Streaming Manually

Try running the components separately:

**Terminal 1 - Start Generator:**
```bash
python src/ingestion/stream_generator.py --interval 2
```
You should see:
```
🌊 Stream Generator started (interval: 2.0s)
📦 Event 1: TXN_1707849600000 (3 products)
📦 Event 2: TXN_1707849602000 (2 products)
```

**Terminal 2 - Check Buffer File:**
```bash
# Watch events being written
tail -f data/streaming/events.jsonl
```

**Terminal 3 - Start Processor:**
```bash
python src/ingestion/stream_processor.py --interval 5
```
You should see:
```
🔄 Stream Processor started (checking every 5s)
📊 Processed 2 new events
```

### 6. Common Issues

#### Issue: "No module named 'faker'"
**Solution:**
```bash
pip install faker
```

#### Issue: "data/streaming directory not found"
**Solution:**
```bash
mkdir -p data/streaming
```

#### Issue: "API returns 500 error"
**Solution:**
```bash
# Ensure pipeline has been run first
python src/transformation/pipeline.py
```

#### Issue: "Stream button does nothing in dashboard"
**Solution:**
1. Check browser console for errors (F12)
2. Verify API is running on port 8000
3. Check CORS settings in `api/main.py`

#### Issue: "Events generated but not appearing in dashboard"
**Solution:**
```bash
# Restart the API server
# Kill existing server
lsof -ti:8000 | xargs kill -9

# Start fresh
python -m uvicorn api.main:app --reload --port 8000
```

### 7. Verify End-to-End

Complete test sequence:
```bash
# 1. Generate initial data
python src/ingestion/generator.py

# 2. Run pipeline
python src/transformation/pipeline.py

# 3. Start API (in background or separate terminal)
python -m uvicorn api.main:app --reload --port 8000 &

# 4. Start frontend (in separate terminal)
cd frontend
npm run dev

# 5. Open dashboard
# Navigate to http://localhost:5173
# Click "Start Stream" button
```

### 8. Check Logs

If streaming still doesn't work, check for errors:

**API Logs:**
```bash
# API server shows requests
# Look for POST /api/stream/start and /api/stream/stop
```

**Browser Console:**
```javascript
// Open DevTools (F12)
// Check Console tab for errors
// Check Network tab for failed requests
```

### 9. Platform-Specific Issues

#### Windows
- Use `python` instead of `python3`
- Path separators: Use forward slashes `/` or escape backslashes `\\\\`
- Kill process: `taskkill /F /PID <pid>`

#### macOS/Linux
- May need `python3` explicitly
- Check file permissions: `chmod +x src/ingestion/*.py`
- Kill process: `kill -9 <pid>`

### 10. Reset Everything

If all else fails, complete reset:
```bash
# Stop all running processes
lsof -ti:8000 | xargs kill -9  # API
lsof -ti:5173 | xargs kill -9  # Frontend

# Clear all data
rm -rf data/raw/* data/silver/* data/gold/* data/streaming/*

# Reinstall dependencies
pip install -r requirements.txt
cd frontend && npm install && cd ..

# Fresh start
python src/ingestion/generator.py
python src/transformation/pipeline.py
python -m uvicorn api.main:app --reload --port 8000
```

## Still Having Issues?

1. Check Python version: `python --version` (should be 3.9+)
2. Check Node version: `node --version` (should be 16+)
3. Verify git clone was complete: `git status`
4. Check for firewall blocking ports 8000 or 5173

## Quick Diagnostic Script

Run this to check your setup:
```bash
python -c "
import sys
print(f'Python: {sys.version}')

try:
    import faker
    print('✅ faker installed')
except:
    print('❌ faker missing')

try:
    import duckdb
    print('✅ duckdb installed')
except:
    print('❌ duckdb missing')

try:
    import fastapi
    print('✅ fastapi installed')
except:
    print('❌ fastapi missing')

from pathlib import Path
streaming_dir = Path('data/streaming')
print(f'Streaming dir exists: {streaming_dir.exists()}')
"
```
