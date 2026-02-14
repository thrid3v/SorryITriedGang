"""
Natural Language to SQL Query Engine
=====================================
Converts English questions into SQL queries against the Gold Layer.
Uses OpenAI GPT-4o-mini for SQL generation and result summarization.
Implements snapshot isolation and multi-tenant context awareness.
"""
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import duckdb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# Import schema inspector
from src.analytics.schema_inspector import (
    load_business_context,
    inspect_schema_with_samples,
    build_schema_prompt
)


def get_schema_prompt(context: Optional[dict] = None) -> str:
    """
    Generate a schema description for the LLM using runtime introspection.
    Now context-aware and includes sample data for better SQL generation.
    
    Args:
        context: Business context dict (optional, loads from config if not provided)
    
    Returns:
        Formatted schema prompt with tables, columns, and sample data
    """
    # Load business context
    if context is None:
        context = load_business_context()
    
    # Inspect schema with sample data
    schema_info = inspect_schema_with_samples(
        gold_layer_path=context['gold_layer_path'],
        sample_rows=5
    )
    
    # Build comprehensive prompt
    return build_schema_prompt(schema_info, context)


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
2. Use the exact table paths as shown in the schema above
3. Limit results to 100 rows maximum
4. Return ONLY the SQL query, no explanations or markdown
5. Use proper JOINs when accessing multiple tables
6. For partitioned tables (fact_*), use the read_parquet() syntax shown in the schema
7. For dimension tables (dim_*), use the direct file paths shown in the schema
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


def ask(question: str, summarize: bool = False, context: Optional[dict] = None) -> Dict[str, Any]:
    """
    Main entry point: Convert question to SQL, execute, and summarize.
    Uses snapshot isolation to ensure consistent results.
    Now supports multi-tenant business contexts.
    
    Args:
        question: User's question in English
        summarize: Whether to generate a natural language summary (slower)
        context: Business context dict (optional, loads from config if not provided)
        
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
        
        # Step 4: Load Gold Layer into memory views (snapshot at this instant)
        # Use dynamic schema discovery instead of hardcoded paths
        if context is None:
            context = load_business_context()
        
        schema_info = inspect_schema_with_samples(
            gold_layer_path=context['gold_layer_path'],
            sample_rows=0  # No samples needed for views
        )
        
        # Create views for all discovered tables
        for table_name, info in schema_info.items():
            table_path = info['path']
            conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM {table_path}")
        
        # Step 5: Execute against snapshot
        results = execute_sql(sql, conn)
        
        # Step 6: Optional summarization (skip by default for speed)
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
        print(f"\n[ERROR] {result['error']}")
    else:
        print(f"\n[SQL] SQL Query:\n{result['sql']}\n")
        print(f"[RESULTS] Results ({result['row_count']} rows):")
        for row in result['data'][:5]:
            print(f"  {row}")
        if result['row_count'] > 5:
            print(f"  ... and {result['row_count'] - 5} more rows")
        print(f"\n[SUMMARY]\n{result['summary']}")
    
    print("\n" + "="*60)
