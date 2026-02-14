"""
Complex RAG Query Test Suite
=============================
Test the RAG system with challenging queries to evaluate performance.
"""
import sys
from pathlib import Path
from src.analytics.nl_query import ask
from src.analytics.vector_store import get_vector_store
import json

# Color codes for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

# Complex test queries organized by category
TEST_QUERIES = {
    "Temporal Queries": [
        "What was my total revenue last month?",
        "Show me revenue comparison between this month and last month",
        "What is my year-to-date revenue?",
        "How much did I earn in Q1 2026?",
        "What was revenue yesterday compared to the day before?",
        "Show me weekly revenue trend for the past 4 weeks",
        "What is my revenue growth rate month over month?",
    ],
    
    "Customer Analytics": [
        "Who are my top 10 customers by total spending?",
        "Show me customer lifetime value for all customers",
        "How many new customers did I acquire last month?",
        "What is the average order value per customer?",
        "Which customers haven't purchased in the last 30 days?",
        "Show me customer retention rate",
        "What cities generate the most revenue?",
    ],
    
    "Product Analytics": [
        "What are my best selling products this month?",
        "Which products have the highest profit margins?",
        "Show me products that are frequently bought together",
        "What is the revenue contribution of each product category?",
        "Which products are underperforming?",
        "Show me product sales by category and month",
        "What products should I promote based on sales trends?",
    ],
    
    "Inventory Management": [
        "Which products are running low on stock?",
        "Show me inventory turnover ratio by category",
        "What products need immediate reordering?",
        "What is my average inventory level by product?",
        "Which products have the slowest turnover?",
        "Show me stock levels for Electronics category",
        "What is my total inventory value?",
    ],
    
    "Advanced Analytics": [
        "Show me revenue by day of week to identify peak sales days",
        "What is the correlation between product price and sales volume?",
        "Calculate customer cohort analysis by signup month",
        "Show me the Pareto analysis - which products generate 80% of revenue?",
        "What is the average time between customer purchases?",
        "Show me seasonal demand patterns by category",
        "What is my customer acquisition cost trend?",
    ],
    
    "Multi-Dimensional Queries": [
        "Show me revenue by category, city, and month",
        "What are the top products in each city?",
        "Compare customer spending patterns between different cities",
        "Show me revenue breakdown by payment method and category",
        "What is the average order value by customer segment and product category?",
        "Show me inventory levels and sales velocity by product",
    ],
    
    "Business Intelligence": [
        "What percentage of revenue comes from repeat customers?",
        "Show me the distribution of order values (small, medium, large)",
        "What is my average customer acquisition rate per month?",
        "Which product categories have the highest return on investment?",
        "Show me revenue per customer by city",
        "What is the average basket size?",
    ],
}


def test_rag_search(query: str) -> dict:
    """Test RAG search to see what examples are retrieved."""
    store = get_vector_store()
    results = store.search_similar(query, top_k=3)
    return results


def test_query(query: str, show_rag: bool = True, show_sql: bool = True) -> dict:
    """Test a single query and return results."""
    print(f"\n{BLUE}{'='*80}{RESET}")
    print(f"{YELLOW}Query:{RESET} {query}")
    print(f"{BLUE}{'='*80}{RESET}")
    
    # Show RAG matches
    if show_rag:
        print(f"\n{GREEN}RAG Matches:{RESET}")
        rag_results = test_rag_search(query)
        if rag_results:
            for i, match in enumerate(rag_results, 1):
                print(f"  {i}. {match['question']}")
                print(f"     Category: {match['category']}, Distance: {match['distance']:.4f}")
        else:
            print(f"  {RED}No RAG matches found{RESET}")
    
    # Execute query
    print(f"\n{GREEN}Executing query...{RESET}")
    result = ask(query, summarize=True)
    
    if result['error']:
        print(f"{RED}ERROR:{RESET} {result['error']}")
        return result
    
    # Show SQL
    if show_sql and result['sql']:
        print(f"\n{GREEN}Generated SQL:{RESET}")
        print(f"{result['sql']}")
    
    # Show results
    print(f"\n{GREEN}Results:{RESET} {result['row_count']} rows")
    if result['data']:
        # Show first few rows
        for i, row in enumerate(result['data'][:3], 1):
            print(f"  {i}. {row}")
        if result['row_count'] > 3:
            print(f"  ... and {result['row_count'] - 3} more rows")
    
    # Show summary
    if result['summary']:
        print(f"\n{GREEN}Summary:{RESET} {result['summary']}")
    
    return result


def run_test_suite(categories: list = None, show_rag: bool = True, show_sql: bool = False):
    """Run the full test suite or specific categories."""
    print(f"\n{BLUE}{'='*80}{RESET}")
    print(f"{BLUE}RAG Query Test Suite{RESET}")
    print(f"{BLUE}{'='*80}{RESET}")
    
    # Filter categories if specified
    if categories:
        test_queries = {k: v for k, v in TEST_QUERIES.items() if k in categories}
    else:
        test_queries = TEST_QUERIES
    
    results = {}
    total_queries = sum(len(queries) for queries in test_queries.values())
    current = 0
    
    for category, queries in test_queries.items():
        print(f"\n\n{BLUE}{'#'*80}{RESET}")
        print(f"{BLUE}Category: {category}{RESET}")
        print(f"{BLUE}{'#'*80}{RESET}")
        
        category_results = []
        for query in queries:
            current += 1
            print(f"\n{YELLOW}[{current}/{total_queries}]{RESET}")
            result = test_query(query, show_rag=show_rag, show_sql=show_sql)
            category_results.append({
                'query': query,
                'success': result['error'] is None,
                'row_count': result['row_count'],
                'error': result['error']
            })
            
            # Pause between queries
            input(f"\n{YELLOW}Press Enter to continue...{RESET}")
        
        results[category] = category_results
    
    # Summary
    print(f"\n\n{BLUE}{'='*80}{RESET}")
    print(f"{BLUE}Test Summary{RESET}")
    print(f"{BLUE}{'='*80}{RESET}")
    
    for category, category_results in results.items():
        total = len(category_results)
        successful = sum(1 for r in category_results if r['success'])
        print(f"\n{category}: {successful}/{total} successful")
        
        # Show failures
        failures = [r for r in category_results if not r['success']]
        if failures:
            print(f"  {RED}Failures:{RESET}")
            for f in failures:
                print(f"    - {f['query']}")
                print(f"      Error: {f['error']}")


def interactive_mode():
    """Interactive mode - test queries one at a time."""
    print(f"\n{BLUE}{'='*80}{RESET}")
    print(f"{BLUE}Interactive RAG Query Tester{RESET}")
    print(f"{BLUE}{'='*80}{RESET}")
    print(f"\nEnter queries to test (or 'quit' to exit)")
    print(f"Commands:")
    print(f"  - Type a query to test it")
    print(f"  - 'list' - Show all test queries")
    print(f"  - 'category <name>' - Show queries for a category")
    print(f"  - 'quit' - Exit")
    
    while True:
        query = input(f"\n{YELLOW}Query>{RESET} ").strip()
        
        if query.lower() == 'quit':
            break
        
        if query.lower() == 'list':
            for category, queries in TEST_QUERIES.items():
                print(f"\n{GREEN}{category}:{RESET}")
                for i, q in enumerate(queries, 1):
                    print(f"  {i}. {q}")
            continue
        
        if query.lower().startswith('category '):
            cat_name = query[9:].strip()
            matching = [k for k in TEST_QUERIES.keys() if cat_name.lower() in k.lower()]
            if matching:
                for cat in matching:
                    print(f"\n{GREEN}{cat}:{RESET}")
                    for i, q in enumerate(TEST_QUERIES[cat], 1):
                        print(f"  {i}. {q}")
            else:
                print(f"{RED}Category not found{RESET}")
            continue
        
        if query:
            test_query(query, show_rag=True, show_sql=True)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test RAG system with complex queries")
    parser.add_argument('--category', '-c', help='Test specific category')
    parser.add_argument('--interactive', '-i', action='store_true', help='Interactive mode')
    parser.add_argument('--show-sql', '-s', action='store_true', help='Show generated SQL')
    parser.add_argument('--no-rag', action='store_true', help='Hide RAG matches')
    parser.add_argument('--query', '-q', help='Test a single query')
    
    args = parser.parse_args()
    
    if args.interactive:
        interactive_mode()
    elif args.query:
        test_query(args.query, show_rag=not args.no_rag, show_sql=args.show_sql)
    else:
        categories = [args.category] if args.category else None
        run_test_suite(categories=categories, show_rag=not args.no_rag, show_sql=args.show_sql)
