import requests

endpoints = [
    '/api/kpis',
    '/api/data-quality/kpis',
    '/api/data-quality/trend',
    '/api/data-quality/checks',
    '/api/revenue/timeseries?granularity=daily',
]

for endpoint in endpoints:
    try:
        r = requests.get(f'http://localhost:8000{endpoint}', headers={'X-User-Role': 'admin'})
        print(f'{endpoint}: {r.status_code}')
        if r.status_code != 200:
            print(f'  Error: {r.text[:200]}')
    except Exception as e:
        print(f'{endpoint}: ERROR - {e}')
