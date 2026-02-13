"""
Text-to-SQL Test Suite
=======================
Comprehensive tests from simple to complex queries.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.analytics.nl_query import ask
import json

def run_test(question, test_name):
    """Run a single test and print results"""
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"QUESTION: {question}")
    print('='*70)
    
    try:
        result = ask(question)
        
        if result['error']:
            print(f"❌ ERROR: {result['error']}")
            return False
        else:
            print(f"✅ SUCCESS")
            print(f"\n📊 SQL:\n{result['sql']}\n")
            print(f"📈 Results ({result['row_count']} rows):")
            for i, row in enumerate(result['data'][:3]):
                print(f"  {i+1}. {row}")
            if result['row_count'] > 3:
                print(f"  ... and {result['row_count'] - 3} more rows")
            return True
    except Exception as e:
        print(f"❌ EXCEPTION: {str(e)}")
        return False

def main():
    print("\n" + "="*70)
    print("TEXT-TO-SQL COMPREHENSIVE TEST SUITE")
    print("="*70)
    
    tests = [
        # Simple aggregations
        ("What is my total revenue?", "Simple SUM aggregation"),
        ("How many orders do I have?", "Simple COUNT"),
        ("What is the average order value?", "Simple AVG"),
        
        # Filtering
        ("What is the total revenue from New York?", "Filter by city"),
        ("How many Electronics products do I have?", "Filter by category"),
        
        # Joins
        ("Who are my top 5 customers by spending?", "Join with dim_users"),
        ("What are my top 5 products by revenue?", "Join with dim_products"),
        
        # Complex queries
        ("Show me revenue by product category", "GROUP BY with JOIN"),
        ("Which store has the highest revenue?", "JOIN + GROUP BY + ORDER BY"),
    ]
    
    results = []
    for question, test_name in tests:
        success = run_test(question, test_name)
        results.append((test_name, success))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    passed = sum(1 for _, success in results if success)
    total = len(results)
    print(f"\nPassed: {passed}/{total}")
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print("\n" + "="*70)

if __name__ == "__main__":
    main()
