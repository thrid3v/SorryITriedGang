# Text-to-SQL Workflow — RetailNexus AI Analyst

## The Big Picture

```
User types: "What are my top 5 products by revenue?"
         |
   [1. SCHEMA INJECTION]  -- Tell the LLM what tables/columns exist
         |
   [2. SQL GENERATION]    -- LLM writes a DuckDB SQL query
         |
   [3. SAFE EXECUTION]    -- FastAPI runs the SQL against Gold Layer
         |
   [4. SUMMARIZATION]     -- LLM converts raw results into English
         |
User sees: "Your top product is 'Running Shoes' with $12,400 in revenue..."
```

---

## Step 1: Schema Injection (The "Memory")

**Concept**: The LLM doesn't know your database exists. You have to **tell it** what tables and columns are available by injecting the schema into the system prompt.

**How it works**: Before any user question is processed, we build a text description of your Gold Layer:

```
You have access to the following tables:

TABLE: fact_transactions (Parquet, Hive-partitioned)
  - transaction_id (VARCHAR)
  - user_key (INTEGER) -- FK to dim_users
  - product_key (INTEGER) -- FK to dim_products
  - amount (DOUBLE) -- transaction value in USD
  - timestamp (TIMESTAMP)
  - store_key (INTEGER) -- FK to dim_stores
  - region (VARCHAR) -- partition column

TABLE: dim_users
  - surrogate_key (INTEGER) -- PK
  - user_id (VARCHAR)
  - name (VARCHAR)
  - city (VARCHAR)
  - is_current (BOOLEAN) -- SCD2 flag

TABLE: dim_products
  - product_key (INTEGER) -- PK
  - product_id (VARCHAR)
  - product_name (VARCHAR)
  - category (VARCHAR)
  - price (DOUBLE)

TABLE: dim_stores
  - store_key (INTEGER) -- PK
  - store_id (VARCHAR)

TABLE: dim_dates
  - date_key (DATE) -- PK
  - day_of_week (VARCHAR)
  - month (INTEGER)
  - year (INTEGER)
```

This is **not RAG**. This is **Schema-Augmented Prompting** -- we give the LLM the "map" of the database so it can write correct SQL.

---

## Step 2: SQL Generation (The "Brain")

**Concept**: The OpenAI API (GPT-4o) receives:
1. The schema from Step 1 (system prompt)
2. The user's question (user prompt)
3. Rules like "Only write SELECT queries, never DELETE or DROP"

**What happens**:
- User asks: *"How are my New York customers doing?"*
- GPT-4o generates:
  ```sql
  SELECT du.name, SUM(ft.amount) AS total_spend, COUNT(*) AS orders
  FROM fact_transactions ft
  JOIN dim_users du ON ft.user_key = du.surrogate_key
  WHERE du.city = 'New York' AND du.is_current = TRUE
  GROUP BY du.name
  ORDER BY total_spend DESC
  LIMIT 10
  ```

**Key concept -- "Tool Use"**: We're not asking GPT to "answer" the question. We're asking it to **write a tool call** (the SQL query). The LLM is the translator, DuckDB is the calculator.

---

## Step 3: Safe Execution (The "Guard")

**Concept**: You never blindly run LLM-generated SQL. We add safety checks:

1. **Read-Only**: Only `SELECT` statements are allowed. If the LLM generates `DROP TABLE`, we reject it.
2. **Timeout**: DuckDB queries are capped at 5 seconds to prevent infinite loops.
3. **Row Limit**: Results are capped at 100 rows to prevent memory issues.

**Where it runs**: A new FastAPI endpoint `/api/chat/ask` that:
1. Receives the English question
2. Sends it to OpenAI with the schema
3. Extracts the SQL from the response
4. Runs it against DuckDB (read-only connection)
5. Returns the results

---

## Step 4: Summarization (The "Voice")

**Concept**: Raw SQL results are not user-friendly. We send the results **back** to the LLM for a final pass.

**Flow**:
1. DuckDB returns: `[{"name": "John", "total_spend": 5000}, {"name": "Jane", "total_spend": 4200}]`
2. We send this + the original question back to GPT
3. GPT responds: *"Your top New York customers are John ($5,000 in total spend) and Jane ($4,200). John has made 12 orders, making him your most loyal NYC customer."*

---

## Technologies Involved

| Component | Technology | Role |
|-----------|-----------|------|
| **LLM** | OpenAI GPT-4o (via API key) | Translates English -> SQL, and Results -> English |
| **Database** | DuckDB | Executes the SQL against Parquet Gold layer |
| **API** | FastAPI (`api/main.py`) | New `/api/chat/ask` endpoint |
| **Frontend** | React (`frontend/`) | New chat input component |
| **Security** | SQL validation logic | Ensures only `SELECT` queries run |

---

## Why Text-to-SQL is Better Than Standard RAG

| Approach | Weakness |
|----------|----------|
| **Standard RAG** (embed text) | Bad at math. If you ask "total revenue," it guesses from text snippets instead of calculating. |
| **Text-to-SQL** (our approach) | DuckDB does the actual math. The answer is always **accurate** because it comes from a real query, not a language model's approximation. |

---

## What We Need to Build (3 pieces)

1. **Backend** (`api/main.py`): A new `/api/chat/ask` endpoint that orchestrates the flow
2. **SQL Engine** (`src/analytics/nl_query.py`): Schema injection + OpenAI call + SQL validation + DuckDB execution
3. **Frontend** (`frontend/src/pages/` or component): A chat input box where users type questions

**Dependencies**: `pip install openai` and an OpenAI API key stored in a `.env` file.

---

## The Role of the OpenAI API Key

The API key powers two specific steps:

### A. The "Translator" (Embeddings / SQL Generation)
- You send a user question to OpenAI's Chat API
- OpenAI returns the SQL query
- Cost: fractions of a cent per query

### B. The "Voice" (Summarization)
- You send the raw DuckDB results back to OpenAI
- OpenAI returns a human-friendly summary
- Cost: fractions of a cent per response

**Without the API key, you have no "Brain." Without DuckDB, you have no "Calculator." Together, they make your data interactive.**

---

## Security Considerations

- **API Key**: Store in `.env` file, never commit to Git
- **SQL Injection**: Validate all LLM output before execution (SELECT only)
- **Rate Limiting**: Cap queries per minute to control OpenAI costs
- **Data Privacy**: All data stays local in DuckDB; only the question text goes to OpenAI
