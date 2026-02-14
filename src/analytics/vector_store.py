"""
Vector Store for RAG-based Query Examples
==========================================
Manages embeddings and semantic search for SQL query examples.
Uses ChromaDB for vector storage and sentence-transformers for embeddings.
"""
import os
import json
from pathlib import Path
from typing import List, Dict, Optional, Any
import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAG_DATA_DIR = PROJECT_ROOT / "data" / "rag"
CHROMA_DB_DIR = RAG_DATA_DIR / "chroma_db"


class VectorStore:
    """
    Manages vector embeddings for query examples using ChromaDB.
    Provides semantic search to find similar queries.
    """
    
    def __init__(self, collection_name: str = "query_examples"):
        """
        Initialize vector store with ChromaDB and embedding model.
        
        Args:
            collection_name: Name of the ChromaDB collection
        """
        # Ensure directories exist
        RAG_DATA_DIR.mkdir(parents=True, exist_ok=True)
        CHROMA_DB_DIR.mkdir(parents=True, exist_ok=True)
        
        # Initialize ChromaDB client
        self.client = chromadb.PersistentClient(
            path=str(CHROMA_DB_DIR),
            settings=Settings(
                anonymized_telemetry=False,
                allow_reset=True
            )
        )
        
        # Initialize embedding model (lightweight, fast model)
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Get or create collection
        try:
            self.collection = self.client.get_collection(name=collection_name)
            print(f"[VectorStore] Loaded existing collection: {collection_name}")
        except:
            self.collection = self.client.create_collection(
                name=collection_name,
                metadata={"description": "SQL query examples for RAG"}
            )
            print(f"[VectorStore] Created new collection: {collection_name}")
    
    def add_example(
        self,
        example_id: str,
        question: str,
        sql: str,
        description: str,
        category: str,
        tags: List[str],
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Add a query example to the vector store.
        
        Args:
            example_id: Unique identifier for the example
            question: Natural language question
            sql: SQL query
            description: Description of what the query does
            category: Category (e.g., "revenue", "inventory")
            tags: List of tags (e.g., ["temporal", "aggregation"])
            metadata: Additional metadata
        """
        # Combine question and description for better semantic matching
        text_to_embed = f"{question} {description}"
        
        # Generate embedding
        embedding = self.embedding_model.encode(text_to_embed).tolist()
        
        # Prepare metadata
        meta = {
            "question": question,
            "sql": sql,
            "description": description,
            "category": category,
            "tags": json.dumps(tags),
            **(metadata or {})
        }
        
        # Add to collection
        self.collection.add(
            ids=[example_id],
            embeddings=[embedding],
            documents=[text_to_embed],
            metadatas=[meta]
        )
    
    def search_similar(
        self,
        question: str,
        top_k: int = 3,
        category_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar query examples using semantic search.
        
        Args:
            question: User's question
            top_k: Number of similar examples to return
            category_filter: Optional category filter
        
        Returns:
            List of similar examples with metadata
        """
        # Generate embedding for the question
        query_embedding = self.embedding_model.encode(question).tolist()
        
        # Build filter
        where_filter = None
        if category_filter:
            where_filter = {"category": category_filter}
        
        # Search
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            where=where_filter
        )
        
        # Format results
        examples = []
        if results['ids'] and len(results['ids'][0]) > 0:
            for i in range(len(results['ids'][0])):
                example = {
                    "id": results['ids'][0][i],
                    "question": results['metadatas'][0][i]['question'],
                    "sql": results['metadatas'][0][i]['sql'],
                    "description": results['metadatas'][0][i]['description'],
                    "category": results['metadatas'][0][i]['category'],
                    "tags": json.loads(results['metadatas'][0][i]['tags']),
                    "distance": results['distances'][0][i] if 'distances' in results else None
                }
                examples.append(example)
        
        return examples
    
    def get_all_examples(self) -> List[Dict[str, Any]]:
        """
        Retrieve all examples from the vector store.
        
        Returns:
            List of all examples
        """
        results = self.collection.get()
        
        examples = []
        if results['ids']:
            for i in range(len(results['ids'])):
                example = {
                    "id": results['ids'][i],
                    "question": results['metadatas'][i]['question'],
                    "sql": results['metadatas'][i]['sql'],
                    "description": results['metadatas'][i]['description'],
                    "category": results['metadatas'][i]['category'],
                    "tags": json.loads(results['metadatas'][i]['tags'])
                }
                examples.append(example)
        
        return examples
    
    def delete_example(self, example_id: str) -> None:
        """Delete an example from the vector store."""
        self.collection.delete(ids=[example_id])
    
    def clear_all(self) -> None:
        """Clear all examples from the collection."""
        self.client.delete_collection(self.collection.name)
        self.collection = self.client.create_collection(
            name=self.collection.name,
            metadata={"description": "SQL query examples for RAG"}
        )
    
    def count(self) -> int:
        """Return the number of examples in the store."""
        return self.collection.count()


# Singleton instance
_vector_store_instance: Optional[VectorStore] = None


def get_vector_store() -> VectorStore:
    """Get or create the singleton vector store instance."""
    global _vector_store_instance
    if _vector_store_instance is None:
        _vector_store_instance = VectorStore()
    return _vector_store_instance


# CLI test
if __name__ == "__main__":
    print("=== Vector Store Test ===\n")
    
    store = VectorStore()
    
    # Test adding an example
    print("Adding test example...")
    store.add_example(
        example_id="test_001",
        question="What was my total revenue last month?",
        sql="SELECT SUM(amount) as total_revenue FROM fact_transactions WHERE MONTH(timestamp) = MONTH(CURRENT_DATE - INTERVAL 1 MONTH)",
        description="Monthly revenue aggregation with date filtering",
        category="revenue",
        tags=["temporal", "aggregation", "monthly"]
    )
    
    print(f"Total examples: {store.count()}\n")
    
    # Test search
    print("Searching for similar queries...")
    results = store.search_similar("show me revenue from previous month", top_k=1)
    
    for result in results:
        print(f"Found: {result['question']}")
        print(f"SQL: {result['sql']}")
        print(f"Distance: {result['distance']:.4f}\n")
    
    # Cleanup
    print("Cleaning up test data...")
    store.delete_example("test_001")
    print(f"Total examples after cleanup: {store.count()}")
