"""
Test script for all three core features: Stream, Upload, RAG
"""
import requests
import json
import time
from pathlib import Path

API_BASE = "http://localhost:8000/api"
HEADERS = {"X-User-Role": "admin"}

def test_stream():
    """Test streaming functionality"""
    print("\n=== TESTING STREAM ===")
    
    # Check status
    resp = requests.get(f"{API_BASE}/stream/status")
    print(f"Stream status: {resp.json()}")
    
    # Start stream
    resp = requests.post(f"{API_BASE}/stream/start", headers=HEADERS)
    if resp.status_code == 200:
        print(f"✓ Stream started: {resp.json()}")
    else:
        print(f"✗ Stream start failed: {resp.text}")
        return False
    
    # Wait for events
    print("Waiting 15s for events...")
    time.sleep(15)
    
    # Check status again
    resp = requests.get(f"{API_BASE}/stream/status")
    status = resp.json()
    print(f"Stream status after 15s: {status}")
    
    if status.get("events_in_buffer", 0) > 0:
        print(f"✓ Events generated: {status['events_in_buffer']}")
    else:
        print("✗ No events in buffer")
    
    # Stop stream
    resp = requests.post(f"{API_BASE}/stream/stop", headers=HEADERS)
    print(f"Stream stopped: {resp.json()}")
    
    return True

def test_upload():
    """Test data upload with column mapping"""
    print("\n=== TESTING UPLOAD ===")
    
    # Create test CSV
    test_csv = Path("test_data.csv")
    test_csv.write_text("""OrderID,Customer,Item,Date,Total
ORD001,CUST001,ITEM001,2024-01-01,100.50
ORD002,CUST002,ITEM002,2024-01-02,250.00
ORD003,CUST003,ITEM003,2024-01-03,75.25
""")
    
    # Step 1: Scan
    with open(test_csv, 'rb') as f:
        files = {'file': ('test_data.csv', f, 'text/csv')}
        resp = requests.post(f"{API_BASE}/upload/scan", headers=HEADERS, files=files)
    
    if resp.status_code == 200:
        scan_result = resp.json()
        print(f"✓ Scan successful")
        print(f"  Headers: {scan_result['headers']}")
        print(f"  Recommended mapping: {scan_result['recommended_mapping']}")
    else:
        print(f"✗ Scan failed: {resp.text}")
        test_csv.unlink()
        return False
    
    # Step 2: Process
    process_data = {
        "filename": scan_result["filename"],
        "file_type": "transactions",
        "mapping": scan_result["recommended_mapping"]
    }
    
    resp = requests.post(
        f"{API_BASE}/upload/process",
        headers={**HEADERS, "Content-Type": "application/json"},
        json=process_data
    )
    
    if resp.status_code == 200:
        result = resp.json()
        print(f"✓ Process successful: {result['message']}")
        print(f"  Rows: {result['rows']}")
    else:
        print(f"✗ Process failed: {resp.text}")
        test_csv.unlink()
        return False
    
    test_csv.unlink()
    return True

def test_rag():
    """Test RAG/NL query functionality"""
    print("\n=== TESTING RAG ===")
    
    questions = [
        "What is the total revenue?",
        "How many orders do we have?",
        "Show me top products"
    ]
    
    for question in questions:
        resp = requests.post(
            f"{API_BASE}/ask",
            json={"question": question}
        )
        
        if resp.status_code == 200:
            result = resp.json()
            print(f"✓ Q: {question}")
            print(f"  A: {result.get('answer', 'No answer')[:100]}...")
        else:
            print(f"✗ Q: {question} - Failed: {resp.text[:100]}")
    
    return True

def test_reset():
    """Test data reset"""
    print("\n=== TESTING RESET ===")
    
    resp = requests.delete(f"{API_BASE}/data/reset", headers=HEADERS)
    if resp.status_code == 200:
        print(f"✓ Reset successful: {resp.json()}")
        return True
    else:
        print(f"✗ Reset failed: {resp.text}")
        return False

if __name__ == "__main__":
    print("Testing RetailNexus Core Features")
    print("=" * 50)
    
    # Test in order
    test_stream()
    test_upload()
    test_rag()
    
    # Test reset and verify stream still works
    print("\n" + "=" * 50)
    print("Testing after reset...")
    test_reset()
    time.sleep(2)
    test_stream()
    
    print("\n" + "=" * 50)
    print("Testing complete!")
