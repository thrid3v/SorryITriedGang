"""
Semantic Column Matcher
========================
Uses OpenAI embeddings to match user terms to actual database columns.
Example: "earnings" → matches to "amount" column with 95% confidence.
"""
import os
import json
from pathlib import Path
from typing import List, Tuple, Dict, Any
import numpy as np
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"


def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """Calculate cosine similarity between two vectors."""
    vec1_np = np.array(vec1)
    vec2_np = np.array(vec2)
    
    dot_product = np.dot(vec1_np, vec2_np)
    norm1 = np.linalg.norm(vec1_np)
    norm2 = np.linalg.norm(vec2_np)
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return float(dot_product / (norm1 * norm2))


def load_metadata_map() -> dict:
    """Load retail concept metadata mappings."""
    metadata_file = CONFIG_DIR / "metadata_map.json"
    with open(metadata_file, 'r') as f:
        return json.load(f)


def semantic_column_match(
    user_term: str,
    schema_info: Dict[str, Any],
    top_k: int = 3
) -> List[Tuple[str, float]]:
    """
    Find best matching columns for a user term using embeddings.
    
    Args:
        user_term: User's search term (e.g., "earnings", "revenue")
        schema_info: Schema information from schema_inspector
        top_k: Number of top matches to return
    
    Returns:
        List of (column_path, similarity_score) tuples
        Example: [("fact_transactions.amount", 0.95), ("dim_products.price", 0.82)]
    """
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError("OpenAI library not installed. Run: pip install openai")
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in environment variables")
    
    client = OpenAI(api_key=api_key)
    
    # Get embedding for user term
    user_response = client.embeddings.create(
        model="text-embedding-3-small",
        input=user_term
    )
    user_embedding = user_response.data[0].embedding
    
    # Collect all columns from schema
    all_columns = []
    for table_name, info in schema_info.items():
        for col_name, col_type in info["columns"]:
            all_columns.append((table_name, col_name))
    
    # Create column descriptions for embedding
    column_descriptions = []
    for table, col in all_columns:
        # Use column name as description
        column_descriptions.append(f"{table}.{col}")
    
    # Get embeddings for all columns (batch request)
    if not column_descriptions:
        return []
    
    column_response = client.embeddings.create(
        model="text-embedding-3-small",
        input=column_descriptions
    )
    
    # Compute similarities
    similarities = []
    for i, (table, col) in enumerate(all_columns):
        col_embedding = column_response.data[i].embedding
        similarity = cosine_similarity(user_embedding, col_embedding)
        similarities.append((f"{table}.{col}", similarity))
    
    # Sort by similarity and return top K
    similarities.sort(key=lambda x: x[1], reverse=True)
    return similarities[:top_k]


def semantic_concept_match(
    user_term: str,
    top_k: int = 3
) -> List[Tuple[str, List[str], float]]:
    """
    Match user term to retail concepts from metadata_map.json.
    
    Args:
        user_term: User's search term
        top_k: Number of top concept matches to return
    
    Returns:
        List of (concept_name, common_columns, similarity_score) tuples
        Example: [("revenue", ["amount", "total", "sales"], 0.92)]
    """
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError("OpenAI library not installed. Run: pip install openai")
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in environment variables")
    
    client = OpenAI(api_key=api_key)
    metadata = load_metadata_map()
    
    # Get embedding for user term
    user_response = client.embeddings.create(
        model="text-embedding-3-small",
        input=user_term
    )
    user_embedding = user_response.data[0].embedding
    
    # Get embeddings for all concepts
    concepts = metadata["retail_concepts"]
    concept_names = list(concepts.keys())
    concept_descriptions = [
        f"{name}: {info['description']}"
        for name, info in concepts.items()
    ]
    
    concept_response = client.embeddings.create(
        model="text-embedding-3-small",
        input=concept_descriptions
    )
    
    # Compute similarities
    similarities = []
    for i, concept_name in enumerate(concept_names):
        concept_embedding = concept_response.data[i].embedding
        similarity = cosine_similarity(user_embedding, concept_embedding)
        common_columns = concepts[concept_name]["common_columns"]
        similarities.append((concept_name, common_columns, similarity))
    
    # Sort and return top K
    similarities.sort(key=lambda x: x[2], reverse=True)
    return similarities[:top_k]


# CLI test
if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.analytics.schema_inspector import load_business_context, inspect_schema_with_samples
    
    print("=== Semantic Matcher Test ===\n")
    
    # Load schema
    context = load_business_context()
    schema_info = inspect_schema_with_samples(context['gold_layer_path'], sample_rows=1)
    
    # Test column matching
    test_terms = ["earnings", "customer", "location", "quantity"]
    
    for term in test_terms:
        print(f"\nSearching for: '{term}'")
        matches = semantic_column_match(term, schema_info, top_k=3)
        print("Top column matches:")
        for col_path, score in matches:
            print(f"  {col_path}: {score:.3f}")
    
    # Test concept matching
    print("\n\n=== Concept Matching ===\n")
    for term in test_terms:
        print(f"\nSearching for: '{term}'")
        matches = semantic_concept_match(term, top_k=2)
        print("Top concept matches:")
        for concept, columns, score in matches:
            print(f"  {concept} ({', '.join(columns[:3])}): {score:.3f}")
