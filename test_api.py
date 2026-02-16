import requests

# Test all API endpoints
API_BASE = "http://localhost:8000/api"
HEADERS = {"X-User-Role": "admin"}

print("Testing API endpoints...")
print("=" * 50)

endpoints = [
    ("GET", "/kpis", {}),
    ("GET", "/revenue/timeseries?granularity=daily", {}),
    ("GET", "/sales/city", {}),
    ("GET", "/products/top", {}),
    ("GET", "/stream/status", {}),
]

for method, path, data in endpoints:
    url = f"{API_BASE}{path}"
    try:
        if method == "GET":
            resp = requests.get(url, headers=HEADERS, timeout=5)
        else:
            resp = requests.post(url, headers=HEADERS, json=data, timeout=5)
        
        print(f"\n{method} {path}")
        print(f"Status: {resp.status_code}")
        if resp.status_code != 200:
            print(f"Error: {resp.text[:200]}")
        else:
            print(f"Success: {str(resp.json())[:100]}")
    except Exception as e:
        print(f"\n{method} {path}")
        print(f"FAILED: {str(e)}")
