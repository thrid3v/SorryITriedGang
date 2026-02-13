# Text-to-SQL Setup Instructions

## Prerequisites
- OpenAI API key (get one from https://platform.openai.com/api-keys)
- Existing Gold Layer data (run the pipeline at least once)

## Setup Steps

### 1. Configure OpenAI API Key

Create a `.env` file in the project root:

```bash
# Copy the example file
cp .env.example .env
```

Edit `.env` and add your actual API key:
```
OPENAI_API_KEY=sk-proj-your-actual-key-here
```

### 2. Restart the API Server

The API server needs to reload to pick up the environment variable:

```bash
# Stop the current server (Ctrl+C)
# Then restart:
cd api
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 3. Access the AI Analyst

1. Open the frontend at http://localhost:5173
2. Click "🤖 AI Analyst" in the sidebar
3. Try asking a question like: "What is my total revenue?"

## Testing

### Test the API Endpoint Directly

```bash
curl -X POST http://localhost:8000/api/chat/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "What is my total revenue?"}'
```

Expected response:
```json
{
  "question": "What is my total revenue?",
  "sql": "SELECT SUM(amount) AS total_revenue FROM ...",
  "data": [{"total_revenue": 52340.50}],
  "summary": "Your total revenue is $52,340.50",
  "row_count": 1,
  "error": null
}
```

### Verify Existing Features Still Work

1. Dashboard KPIs: http://localhost:8000/api/kpis
2. Stream status: http://localhost:8000/api/stream/status
3. CLV data: http://localhost:8000/api/clv

All should return 200 OK.

## Example Questions to Try

- "What is my total revenue?"
- "Who are my top 5 customers by spending?"
- "Which products sell best in New York?"
- "Show me revenue by category"
- "How many orders did I get today?"
- "What's the average order value?"

## Troubleshooting

### "OPENAI_API_KEY not found"
- Make sure `.env` file exists in project root
- Restart the API server after creating `.env`

### "No files found" or empty results
- Run the pipeline first: `python src/transformation/clean.py`
- Or start the stream to generate data

### API returns 500 error
- Check API server logs for detailed error message
- Verify OpenAI API key is valid
- Ensure Gold Layer Parquet files exist in `data/gold/`
