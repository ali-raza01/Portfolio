# utils/sdg_geo_lookup.py

import json
import os

def load_sdg_country_codes():
    path = os.path.join(os.path.dirname(__file__), "..\lookup_files\sdg_geocodes.txt")
    with open(path, "r", encoding="utf-8") as f:
        geo_list = json.load(f)
    return {
        item["geoAreaName"].strip().lower(): int(item["geoAreaCode"])
        for item in geo_list
    }

SDG_GEO_LOOKUP = load_sdg_country_codes()
