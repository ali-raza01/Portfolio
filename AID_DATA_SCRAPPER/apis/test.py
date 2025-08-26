# test_apis.py
import requests, json, sys

BASE = "http://127.0.0.1:8000"

tests = [
    # (description, path, params)
    ("API 1 - Year/Country", "/api/v1/projects/summary/year-country",
     dict(start_year=2015, end_year=2020)),
    ("API 2 - Title/Desc", "/api/v1/projects/title-description",
     dict(search="water", limit=5)),
    # ("API 3 - Age Groups", "/api/v1/beneficiaries/summary/age-group",
    #  dict(project_id="P-001")),
    ("API 4 - Funders", "/api/v1/funding/summary/by-group",
     dict(country="Nigeria", limit=3)),
]

for label, path, params in tests:
    try:
        r = requests.get(BASE + path, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        print(data)
        print(f"{label}: OK  ({len(data)} rows)")
        # Uncomment next line to dump JSON
        # print(json.dumps(data, indent=2)[:500])
    except Exception as exc:
        print(f"{label}: FAILED → {exc}")
        sys.exit(1)

print("All API checks passed.")