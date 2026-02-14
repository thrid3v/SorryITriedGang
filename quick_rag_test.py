"""
Quick RAG Test - Top 10 Complex Queries
========================================
Test the most challenging queries quickly.
"""
from src.analytics.nl_query import ask
from src.analytics.vector_store import get_vector_store
import json

# Top 10 most complex queries to test RAG
COMPLEX_QUERIES = [
    # Temporal complexity
    "What was my revenue last month compared to the same month last year?",
    
    # Multi-dimensional aggregation
    "Show me top 5 products by revenue in each city",
    
    # Advanced analytics
    "Which customers are in the top 20% by spending (Pareto analysis)?",
    
    # Temporal + filtering
    "What products sold more than 10 units in the last 7 days?",
    
    # Customer segmentation
    "Show me new vs returning customer revenue breakdown for this month",
    
    # Market basket analysis
    "What products are frequently purchased together with Electronics?",
    
    # Inventory + sales correlation
    "Which products have high inventory but low sales in the last month?",
    
    # Cohort analysis
    "Show me customer retention rate by signup month",
    
    # Revenue attribution
    "What percentage of total revenue comes from the top 10 products?",
    
    # Seasonal trends
    "Compare Q1 and Q2 revenue by product category"
]


def test_query(query: str, index: int, total: int):
    """Test a single query."""
    print(f"\n{'='*80}")
    print(f"[{index}/{total}] Testing Query:")
    print(f"{'='*80}")
    print(f"❓ {query}\n")
    
    # Check RAG matches
    print("🔍 RAG Search Results:")
    store = get_vector_store()
    rag_results = store.search_similar(query, top_k=2)
    
    if rag_results:
        for i, match in enumerate(rag_results, 1):
            print(f"  {i}. {match['question']}")
            print(f"     📁 Category: {match['category']}")
            print(f"     📊 Distance: {match['distance']:.4f}")
            print(f"     🏷️  Tags: {', '.join(match['tags'])}")
    else:
        print("  ⚠️  No RAG matches found")
    
    # Execute query
    print("\n⚙️  Executing query...")
    result = ask(query, summarize=True)
    
    if result['error']:
        print(f"❌ ERROR: {result['error']}\n")
        return False
    
    # Show results
    print(f"\n✅ Success!")
    print(f"📝 Generated SQL:")
    print(f"   {result['sql'][:150]}..." if len(result['sql']) > 150 else f"   {result['sql']}")
    print(f"\n📊 Results: {result['row_count']} rows")
    
    if result['data'] and len(result['data']) > 0:
        print(f"   Sample: {result['data'][0]}")
    
    if result['summary']:
        print(f"\n💡 Summary: {result['summary']}")
    
    return True


def main():
    """Run all complex queries."""
    print("\n" + "="*80)
    print("🚀 RAG System - Complex Query Test Suite")
    print("="*80)
    print(f"\nTesting {len(COMPLEX_QUERIES)} complex queries...\n")
    
    results = []
    for i, query in enumerate(COMPLEX_QUERIES, 1):
        success = test_query(query, i, len(COMPLEX_QUERIES))
        results.append({
            'query': query,
            'success': success
        })
        
        if i < len(COMPLEX_QUERIES):
            input("\n⏸️  Press Enter to continue to next query...")
    
    # Summary
    print("\n" + "="*80)
    print("📈 Test Summary")
    print("="*80)
    
    successful = sum(1 for r in results if r['success'])
    total = len(results)
    success_rate = (successful / total) * 100
    
    print(f"\n✅ Successful: {successful}/{total} ({success_rate:.1f}%)")
    
    failures = [r for r in results if not r['success']]
    if failures:
        print(f"\n❌ Failed Queries:")
        for f in failures:
            print(f"   - {f['query']}")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
