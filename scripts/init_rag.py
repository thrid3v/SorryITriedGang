"""
Initialize RAG Vector Store
============================
One-time setup script to populate the vector store with query examples.
Run this after installing dependencies to set up the RAG system.
"""
import sys
import json
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.analytics.vector_store import VectorStore


def load_examples(examples_file: Path) -> list:
    """Load query examples from JSON file."""
    with open(examples_file, 'r') as f:
        data = json.load(f)
    return data['examples']


def initialize_vector_store():
    """Initialize vector store with query examples."""
    print("=" * 60)
    print("RAG Vector Store Initialization")
    print("=" * 60)
    
    # Load examples
    examples_file = PROJECT_ROOT / "data" / "rag" / "query_examples.json"
    print(f"\nLoading examples from: {examples_file}")
    
    if not examples_file.exists():
        print(f"ERROR: Examples file not found: {examples_file}")
        return False
    
    examples = load_examples(examples_file)
    print(f"Loaded {len(examples)} examples")
    
    # Initialize vector store
    print("\nInitializing vector store...")
    store = VectorStore()
    
    # Clear existing data (optional - comment out to preserve existing examples)
    if store.count() > 0:
        print(f"Found {store.count()} existing examples. Clearing...")
        store.clear_all()
    
    # Add all examples
    print("\nAdding examples to vector store...")
    for i, example in enumerate(examples, 1):
        store.add_example(
            example_id=example['id'],
            question=example['question'],
            sql=example['sql'],
            description=example['description'],
            category=example['category'],
            tags=example['tags']
        )
        print(f"  [{i}/{len(examples)}] Added: {example['id']} - {example['question'][:50]}...")
    
    print(f"\n✓ Successfully added {store.count()} examples to vector store")
    
    # Test search
    print("\n" + "=" * 60)
    print("Testing Semantic Search")
    print("=" * 60)
    
    test_queries = [
        "What was revenue last month?",
        "Show me top customers",
        "Which products are low on stock?"
    ]
    
    for query in test_queries:
        print(f"\nQuery: '{query}'")
        results = store.search_similar(query, top_k=2)
        for i, result in enumerate(results, 1):
            print(f"  {i}. {result['question']}")
            print(f"     Category: {result['category']}, Distance: {result['distance']:.4f}")
    
    print("\n" + "=" * 60)
    print("✓ RAG Vector Store Initialized Successfully!")
    print("=" * 60)
    print(f"\nTotal examples in store: {store.count()}")
    print("You can now use the AI Analyst with enhanced RAG capabilities.")
    
    return True


if __name__ == "__main__":
    success = initialize_vector_store()
    sys.exit(0 if success else 1)
