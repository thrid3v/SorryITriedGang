import requests

BASE = "http://localhost:8000"

endpoints = [
    ("GET", "/api/health"),
    ("GET", "/api/kpis"),
    ("GET", "/api/clv"),
    ("GET", "/api/basket"),
    ("GET", "/api/revenue/timeseries"),
    ("GET", "/api/sales/city"),
    ("GET", "/api/products/top"),
    ("GET", "/api/inventory/turnover"),
    ("GET", "/api/delivery/metrics"),
    ("GET", "/api/trends/seasonal"),
    ("GET", "/api/stream/status"),
    ("GET", "/api/data/status"),
]

print("=" * 60)
print(f"{'Endpoint':<45} {'Status':>6}")
print("=" * 60)

for method, path in endpoints:
    try:
        r = requests.get(f"{BASE}{path}", timeout=30)
        status_icon = "OK" if r.status_code == 200 else "FAIL"
        print(f"{status_icon} {path:<43} {r.status_code:>6}")
        if r.status_code != 200:
            body = r.json() if 'json' in r.headers.get('content-type','') else r.text[:100]
            print(f"  Error: {str(body)[:100]}")
    except Exception as e:
        print(f"ERR {path:<43} {str(e)[:50]}")

print("=" * 60)
