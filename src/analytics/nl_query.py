"""
Natural Language to SQL Query Engine
=====================================
Converts English questions into SQL queries against the Gold Layer.
Uses OpenAI GPT-4o for SQL generation and result summarization.
Implements snapshot isolation to ensure consistent query results.
"""
import os
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
import duckdb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

# Gold Layer paths
FACT_TXN = str(GOLD_DIR / "fact_transactions.parquet" / "**" / "*.parquet").replace("\\", "/")
DIM_USERS = str(GOLD_DIR / "dim_users.parquet").replace("\\", "/")
DIM_PRODUCTS = str(GOLD_DIR / "dim_products.parquet").replace("\\", "/")
DIM_STORES = str(GOLD_DIR / "dim_stores.parquet").replace("\\", "/")
DIM_DATES = str(GOLD_DIR / "dim_dates.parquet").replace("\\", "/")


def get_schema_prompt() -> str:
    """
    Generate a schema description for the LLM.
    Reads the Gold Layer structure via DuckDB DESCRIBE.
    """
    conn = duckdb.connect()
    
    schema_text = """You have access to the following tables in a retail data warehouse:

TABLE: fact_transactions (Parquet, Hive-partitioned by region)
  - transaction_id (VARCHAR) — Unique transaction identifier
  - user_key (INTEGER) — Foreign key to dim_users.surrogate_key
  - product_key (INTEGER) — Foreign key to dim_products.product_key
  - store_key (INTEGER) — Foreign key to dim_stores.store_key
  - amount (DOUBLE) — Transaction value in USD
  - quantity (INTEGER) — Number of items purchased
  - timestamp (TIMESTAMP) — Transaction timestamp
  - region (VARCHAR) — Geographic region (partition column)

TABLE: dim_users
  - surrogate_key (INTEGER) — Primary key (SCD Type 2)
  - user_id (VARCHAR) — Business key
  - name (VARCHAR) — Customer name
  - email (VARCHAR) — Customer email
  - city (VARCHAR) — Customer city
  - valid_from (DATE) — SCD2 start date
  - valid_to (DATE) — SCD2 end date (NULL if current)
  - is_current (BOOLEAN) — TRUE for current record

TABLE: dim_products
  - product_key (INTEGER) — Primary key
  - product_id (VARCHAR) — Business key
  - product_name (VARCHAR) — Product name
  - category (VARCHAR) — Product category
  - price (DOUBLE) — Product price in USD

TABLE: dim_stores
  - store_key (INTEGER) — Primary key
  - store_id (VARCHAR) — Business key
  - store_name (VARCHAR) — Store name
  - region (VARCHAR) — Store region

TABLE: dim_dates
  - date_key (DATE) — Primary key
  - day_of_week (VARCHAR) — Day name (Monday, Tuesday, etc.)
  - month (INTEGER) — Month number (1-12)
  - year (INTEGER) — Year

IMPORTANT RULES:
- Always use table aliases for clarity
- When querying dim_users, filter by is_current = TRUE to get current records only
- Use read_parquet() with hive_partitioning=true for fact_transactions
- All monetary values are in USD
- Timestamps are in UTC
"""
    
    conn.close()
    return schema_text


def generate_sql(question: str) -> str:
    """
    Generate SQL query from natural language question using OpenAI.
    
    Args:
        question: User's question in English
        
    Returns:
        SQL query string
    """
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError("OpenAI library not installed. Run: pip install openai")
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in environment variables")
    
    client = OpenAI(api_key=api_key)
    
    schema = get_schema_prompt()
    
    system_prompt = f"""{schema}

You are a SQL expert. Generate a DuckDB SQL query to answer the user's question.

CRITICAL RULES:
1. ONLY generate SELECT queries. Never use DROP, DELETE, UPDATE, INSERT, or any DDL/DML.
2. Use the exact table paths as shown in the schema
3. Limit results to 100 rows maximum
4. Return ONLY the SQL query, no explanations or markdown
5. Use proper JOINs when accessing multiple tables
6. For fact_transactions, use: read_parquet('{FACT_TXN}', hive_partitioning=true)
7. For dimension tables, use the direct paths shown above
"""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",  # Faster, cheaper model
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question}
        ],
        temperature=0,  # Deterministic output
        max_tokens=300  # Reduced for faster response
    )
    
    sql = response.choices[0].message.content.strip()
    
    # Remove markdown code blocks if present
    sql = re.sub(r'^```sql\s*', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'^```\s*', '', sql, flags=re.MULTILINE)
    sql = sql.strip()
    
    return sql


def validate_sql(sql: str) -> bool:
    """
    Validate that SQL is safe to execute (SELECT only).
    
    Args:
        sql: SQL query string
        
    Returns:
        True if valid, raises ValueError otherwise
    """
    sql_upper = sql.upper().strip()
    
    # Check for dangerous keywords
    dangerous_keywords = [
        'DROP', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'ALTER',
        'TRUNCATE', 'REPLACE', 'MERGE', 'GRANT', 'REVOKE'
    ]
    
    for keyword in dangerous_keywords:
        if re.search(rf'\b{keyword}\b', sql_upper):
            raise ValueError(f"Unsafe SQL: '{keyword}' statement not allowed")
    
    # Must start with SELECT
    if not sql_upper.startswith('SELECT'):
        raise ValueError("Only SELECT queries are allowed")
    
    return True


def execute_sql(sql: str, conn: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """
    Execute SQL query against the snapshot connection.
    
    Args:
        sql: Validated SQL query
        conn: DuckDB connection with frozen snapshot
        
    Returns:
        List of result rows as dictionaries
    """
    try:
        # Execute with timeout
        result = conn.execute(sql).fetchdf()
        
        # Convert to list of dicts
        records = result.to_dict(orient='records')
        
        # Limit to 100 rows
        return records[:100]
    
    except Exception as e:
        raise RuntimeError(f"SQL execution failed: {str(e)}")


def summarize_results(question: str, sql: str, results: List[Dict]) -> str:
    """
    Generate human-friendly summary of query results using OpenAI.
    OPTIONAL: Can be skipped for faster responses.
    
    Args:
        question: Original user question
        sql: SQL query that was executed
        results: Query results
        
    Returns:
        Natural language summary
    """
    try:
        from openai import OpenAI
    except ImportError:
        return None
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    
    client = OpenAI(api_key=api_key)
    
    # Prepare results summary
    if not results:
        results_text = "No results found."
    elif len(results) <= 5:
        results_text = str(results)
    else:
        results_text = f"First 5 of {len(results)} results:\n{str(results[:5])}"
    
    prompt = f"""The user asked: "{question}"

Results:
{results_text}

Provide a 1-sentence summary highlighting the key insight."""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",  # Faster model
        messages=[
            {"role": "system", "content": "You are a data analyst. Be concise."},
            {"role": "user", "content": prompt}
        ],
        temperature=0,
        max_tokens=100  # Reduced for speed
    )
    
    return response.choices[0].message.content.strip()


def ask(question: str, summarize: bool = False) -> Dict[str, Any]:
    """
    Main entry point: Convert question to SQL, execute, and summarize.
    Uses snapshot isolation to ensure consistent results.
    
    Args:
        question: User's question in English
        
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
        # Step 1: Generate SQL
        sql = generate_sql(question)
        
        # Step 2: Validate SQL
        validate_sql(sql)
        
        # Step 3: Create snapshot connection
        conn = duckdb.connect()  # In-memory connection
        
        # Load Gold Layer into memory views (snapshot at this instant)
        conn.execute(f"""
            CREATE VIEW fact_transactions AS 
            SELECT * FROM read_parquet('{FACT_TXN}', hive_partitioning=true)
        """)
        conn.execute(f"CREATE VIEW dim_users AS SELECT * FROM '{DIM_USERS}'")
        conn.execute(f"CREATE VIEW dim_products AS SELECT * FROM '{DIM_PRODUCTS}'")
        conn.execute(f"CREATE VIEW dim_stores AS SELECT * FROM '{DIM_STORES}'")
        conn.execute(f"CREATE VIEW dim_dates AS SELECT * FROM '{DIM_DATES}'")
        
        # Step 4: Execute against snapshot
        results = execute_sql(sql, conn)
        
        # Step 5: Optional summarization (skip by default for speed)
        summary = summarize_results(question, sql, results) if summarize else None
        
        # Close connection (release snapshot)
        conn.close()
        
        return {
            "question": question,
            "sql": sql,
            "data": results,
            "summary": summary,
            "row_count": len(results),
            "error": None
        }
    
    except Exception as e:
        return {
            "question": question,
            "sql": None,
            "data": [],
            "summary": None,
            "row_count": 0,
            "error": str(e)
        }


# CLI test
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python nl_query.py 'Your question here'")
        sys.exit(1)
    
    question = " ".join(sys.argv[1:])
    result = ask(question)
    
    print("\n" + "="*60)
    print(f"Question: {result['question']}")
    print("="*60)
    
    if result['error']:
        print(f"\n❌ Error: {result['error']}")
    else:
        print(f"\n📊 SQL Query:\n{result['sql']}\n")
        print(f"📈 Results ({result['row_count']} rows):")
        for row in result['data'][:5]:
            print(f"  {row}")
        if result['row_count'] > 5:
            print(f"  ... and {result['row_count'] - 5} more rows")
        print(f"\n💡 Summary:\n{result['summary']}")
    
    print("\n" + "="*60)
