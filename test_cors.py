"""
CORS and API Diagnostic Script
================================
Tests the backend API endpoints and CORS configuration.
"""
import requests
import json

API_BASE = "http://localhost:8000"

def test_health():
    """Test health endpoint"""
    print("\n" + "="*60)
    print("Testing /api/health endpoint...")
    print("="*60)
    try:
        response = requests.get(f"{API_BASE}/api/health")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        print(f"CORS Headers:")
        for key, value in response.headers.items():
            if 'access-control' in key.lower() or 'origin' in key.lower():
                print(f"  {key}: {value}")
    except Exception as e:
        print(f"ERROR: {e}")

def test_ask_endpoint():
    """Test /api/ask endpoint"""
    print("\n" + "="*60)
    print("Testing /api/ask endpoint...")
    print("="*60)
    try:
        headers = {
            'Content-Type': 'application/json',
            'Origin': 'http://localhost:5173'
        }
        data = {
            "question": "What are my top 5 products by revenue?"
        }
        response = requests.post(
            f"{API_BASE}/api/ask",
            headers=headers,
            json=data
        )
        print(f"Status Code: {response.status_code}")
        print(f"CORS Headers:")
        for key, value in response.headers.items():
            if 'access-control' in key.lower() or 'origin' in key.lower():
                print(f"  {key}: {value}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"\nQuestion: {result.get('question')}")
            print(f"SQL: {result.get('sql')}")
            print(f"Row Count: {result.get('row_count')}")
            print(f"Summary: {result.get('summary')}")
            if result.get('error'):
                print(f"ERROR: {result.get('error')}")
        else:
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"ERROR: {e}")

def test_preflight():
    """Test OPTIONS preflight request"""
    print("\n" + "="*60)
    print("Testing OPTIONS preflight request...")
    print("="*60)
    try:
        headers = {
            'Origin': 'http://localhost:5173',
            'Access-Control-Request-Method': 'POST',
            'Access-Control-Request-Headers': 'content-type'
        }
        response = requests.options(
            f"{API_BASE}/api/ask",
            headers=headers
        )
        print(f"Status Code: {response.status_code}")
        print(f"CORS Headers:")
        for key, value in response.headers.items():
            if 'access-control' in key.lower() or 'origin' in key.lower():
                print(f"  {key}: {value}")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("CORS and API Diagnostic Test")
    print("="*60)
    
    test_health()
    test_preflight()
    test_ask_endpoint()
    
    print("\n" + "="*60)
    print("Diagnostic Complete")
    print("="*60)
