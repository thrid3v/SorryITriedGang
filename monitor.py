import requests
import time

print("Monitoring backend for 30 seconds...")
for i in range(6):
    try:
        r = requests.get('http://localhost:8000/api/kpis')
        print(f"{i*5}s: Status {r.status_code}")
    except Exception as e:
        print(f"{i*5}s: ERROR - {e}")
        break
    time.sleep(5)
print("Done")
