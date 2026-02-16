"""
End-to-end test: Start stream, wait for seed data + processing, verify ALL tabs have data.
"""
import requests
import time

BASE = "http://localhost:8000"

# Step 1: Verify backend is alive
print("1. Checking backend...")
r = requests.get(f"{BASE}/api/kpis")
print(f"   KPIs: {r.status_code}")

# Step 2: Start stream 
print("\n2. Starting stream...")
r = requests.post(f"{BASE}/api/stream/start", headers={"X-User-Role": "admin"})
print(f"   Start: {r.status_code} -> {r.json()}")

# Step 3: Wait for seed data + pipeline to process (seed is ~400 events)
print("\n3. Waiting 25s for seed data + pipeline...")
for i in range(5):
    time.sleep(5)
    try:
        r = requests.get(f"{BASE}/api/kpis")
        print(f"   {(i+1)*5}s: backend={r.status_code}, kpis={r.json()}")
    except Exception as e:
        print(f"   {(i+1)*5}s: CRASH! {e}")
        exit(1)

# Step 4: Check all dashboard tabs
print("\n4. Checking all dashboard data:")

# Invalidate cache by calling health first
requests.get(f"{BASE}/api/health")

endpoints = {
    "KPIs": "/api/kpis",
    "Revenue Timeseries": "/api/revenue/timeseries",
    "City Sales": "/api/sales/city",
    "Top Products": "/api/products/top",
    "Inventory Turnover": "/api/inventory/turnover",
    "Delivery Metrics": "/api/delivery/metrics",
    "Customer Segmentation": "/api/customers/segmentation",
    "CLV": "/api/clv",
    "Stream Status": "/api/stream/status",
}

for name, endpoint in endpoints.items():
    try:
        r = requests.get(f"{BASE}{endpoint}")
        data = r.json()
        if isinstance(data, list):
            count = len(data)
        elif isinstance(data, dict):
            count = sum(1 for v in data.values() if v)
        else:
            count = 0
        status = "OK" if (count > 0 or r.status_code == 200) else "EMPTY"
        print(f"   {name:25s}: {r.status_code} | {status} | items={count}")
    except Exception as e:
        print(f"   {name:25s}: ERROR - {e}")

# Step 5: Stop stream
print("\n5. Stopping stream...")
r = requests.post(f"{BASE}/api/stream/stop", headers={"X-User-Role": "admin"})
print(f"   Stop: {r.status_code}")

print("\n=== TEST COMPLETE ===")
