# --- main.py ---

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uuid
from typing import List, Dict, Any, Optional
from unified_mapping import FILTER_KEY_MAPPING, SCRAPE_RUN_ID_FIELD
from db_setup_and_ingest_org import parse_fcdo_jsons, parse_worldbank_data, parse_iati_csvs, parse_worldbank_projects, parse_foreign_assistance_data, parse_who_ghed_data, parse_oecd_csvs
from scrappers.fcdo_scrapper import run_fcdo_scraper
from scrappers.iati_scrapper import run_iati_scraper
from scrappers.world_bank_scrapper import fetch_indicator_data
import pandas as pd
from scrappers.oecd_scrapper import run_oecd_scraper
import sys
from loguru import logger
import os
import json 
from utils import wb_utils
import logging
from scrappers.bii_scraper import scrape_bii
from db_setup_and_ingest_org import parse_bii_csvs

class NoTracebackFormatter(logging.Formatter):
    # strip any traceback delivered via exc_info
    def formatException(self, exc_info):
        return ""                      # completely omit traceback
    def format(self, record):
        record.exc_info = None         # safety-belt
        return super().format(record)

LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
log_file = "world_bank_scraper.log"

logger_wb = logging.getLogger("world_bank_scraper")
logger_wb.setLevel(logging.INFO)
logger_wb.propagate = False              # don’t pass records to root logger

# clean stream handler ------------------------------------------------------
stream_h = logging.StreamHandler()
stream_h.setFormatter(NoTracebackFormatter(LOG_FMT))
logger_wb.addHandler(stream_h)

# clean file handler --------------------------------------------------------
if not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith(log_file)
           for h in logger_wb.handlers):
    file_h = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_h.setFormatter(NoTracebackFormatter(LOG_FMT))
    logger_wb.addHandler(file_h)

logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add("ingestion.log", level="INFO", rotation="500 KB", enqueue=True)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class DataRequest(BaseModel):
    sources: List[str]
    filters: Dict[str, Any] = {}
    ingest_only: bool = False
    proceed_if_duplicate: Optional[bool] = False

from unified_mapping import FILTER_VALUE_FIXES

PAYLOAD_LOG_FILE = "payload.logs"

def load_logged_payloads() -> list:
    if not os.path.exists(PAYLOAD_LOG_FILE):
        return []
    with open(PAYLOAD_LOG_FILE, "r") as f:
        return [json.loads(line.strip()) for line in f if line.strip()]

def has_been_executed(payload: dict, logged_payloads: list) -> bool:
    return payload in logged_payloads

def log_payload(payload: dict):
    with open(PAYLOAD_LOG_FILE, "a") as f:
        f.write(json.dumps(payload, sort_keys=True) + "\n")

def normalize_country_filter(filters: Dict[str, Any], source: str) -> Dict[str, Any]:
    """
    Replaces full country names with corresponding country codes based on FILTER_VALUE_FIXES.
    """
    country_val = filters.get("country")
    if not country_val:
        return filters

    source_mapping = FILTER_VALUE_FIXES.get(source.upper(), {}).get("Country", {})
    reverse_map = {v.lower(): k for k, v in source_mapping.items()}

    match = reverse_map.get(country_val.lower())
    if match:
        filters["country"] = match
    return filters

def translate_filters(general_filters: Dict[str, Any], source: str) -> Dict[str, Any]:
    general_filters = normalize_country_filter(general_filters.copy(), source)

    mapped = {}
    for k, v in general_filters.items():
        mapped_key = FILTER_KEY_MAPPING.get(source, {}).get(k, k)
        mapped[mapped_key] = v
    return mapped

# foreign_assistance_scraper.py
from unified_mapping import FILTER_KEY_MAPPING   #  ← new import

def _translate_filters(filters: dict | None) -> dict:
    """
    Convert generic keys (country, sector, …) to the Foreign-Assistance
    column names using unified_mapping.FILTER_KEY_MAPPING.
    Keys that are already source-specific pass straight through.
    """
    if not filters:
        return {}
    translated = {}
    for key, val in filters.items():
        # generic → source-specific
        mapped_key = FILTER_KEY_MAPPING.get(key, {}).get("foreignassistance", key)
        # handle year ranges cleanly
        if key in ("start_year", "end_year"):
            mapped_key = "Fiscal Year"
        translated.setdefault(mapped_key, [])
        # keep lists if caller already provided one
        if isinstance(val, (list, tuple)):
            translated[mapped_key].extend(val)
        else:
            translated[mapped_key].append(val)
    # collapse single-value lists
    for k in list(translated):
        if len(translated[k]) == 1:
            translated[k] = translated[k][0]
    return translated


def load_fcdo_filter_mappings(filepath: str = "utils/fcdo_filters.txt") -> Dict[str, Dict[str, str]]:
    mappings = {}
    current_key = None
    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.endswith(":"):
                    current_key = line[:-1].strip()
                    mappings[current_key] = {}
                elif current_key and ":" in line:
                    label, code = map(str.strip, line.split(":", 1))
                    mappings[current_key][label] = code
    except FileNotFoundError:
        print("⚠️ fcdo_filters.txt not found.")
    return mappings

def apply_fcdo_value_mapping(user_filters: Dict[str, Any], mapping: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    mapped_filters = {}
    for k, v in user_filters.items():
        if k in mapping and v in mapping[k]:
            mapped_filters[f"{k}_code"] = mapping[k][v]
        else:
            mapped_filters[k] = v  # fallback
    return mapped_filters

def load_iati_filters(path="utils/iati_filters.txt"):
    filters = {}
    current_group = None
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if not line.startswith("-"):
                current_group = line
                filters[current_group] = {}
            else:
                try:
                    name, code = line[1:].strip().rsplit("(", 1)
                    name = name.strip()
                    code = code.strip(")")
                    filters[current_group][code] = name
                except:
                    continue  # skip malformed lines
    return filters

def get_iati_code_by_name(filters_dict, group_name, value_name):
    group = filters_dict.get(group_name)
    if not group:
        return None
    for code, label in group.items():
        if label.lower() == value_name.lower():
            return code
    return None

@app.post("/check-payload")
async def check_payload(data: DataRequest):
    payload_dict = {
        "sources": data.sources,
        "filters": data.filters,
        "ingest_only": data.ingest_only
    }
    logged_payloads = load_logged_payloads()
    already_executed = has_been_executed(payload_dict, logged_payloads)
    
    return {
        "already_executed": already_executed,
        "message": "This payload has been executed before. Do you want to continue?"
                  if already_executed else "Payload is new. Ready to execute."
    }

@app.post("/get-data")
async def get_data(data: DataRequest):
    print("🟢 Incoming request:", data)
    print("🔍 Filters:", data.filters)
    
    payload_dict = {
        "sources": data.sources,
        "filters": data.filters,
        "ingest_only": data.ingest_only
    }
    logged_payloads = load_logged_payloads()

    if has_been_executed(payload_dict, logged_payloads):
        if not data.proceed_if_duplicate:
            return {
                "status": "duplicate",
                "message": "Payload has already been executed. Please confirm execution by setting `proceed_if_duplicate=true` in the request."
            }

    # Log and execute
    log_payload(payload_dict)

    scrape_run_id = str(uuid.uuid4())
    all_data = []

    for source in data.sources:
        print(f"Running scraper for: {source}")
        

        if source == "fcdo":
            # source_filters = translate_filters(data.filters, source)
            fcdo_mappings = load_fcdo_filter_mappings()
            source_filters = apply_fcdo_value_mapping(data.filters, fcdo_mappings)
            fcdo_jsons = run_fcdo_scraper(source_filters)
            parse_fcdo_jsons(fcdo_jsons)

        elif source == "iati":
            # translated_filters = translate_filters(source_filters, source)
            # print("🎯 Translated filters:", translated_filters)  # Debug
            run_iati_scraper(data.filters)
            parse_iati_csvs()

        elif source == "worldbank":
            try:
                country = data.filters["country"]
                logger_wb.info(f"🌐 Requesting World Bank project data for: {country}")
                # 1. Resolve country ISO2 from name
                country_iso2 = wb_utils.get_iso2_from_country(data.filters["country"])

                # 2. Resolve indicator code from sector (topic)
                indicator_code = wb_utils.get_indicator_code_from_topic(data.filters["sector"])

                # 3. Time range
                start_year = data.filters.get("start_year")
                end_year   = data.filters.get("end_year")
                date_range = f"{start_year}:{end_year}" if start_year and end_year else ""

                # 4. Fetch and parse data
                # print(country_iso2)
                # df = fetch_indicator_data(country_iso2, indicator_code, date_range)
                parse_worldbank_projects(country_iso2,indicator_code,date_range)  # or any processing you apply

            except (wb_utils.CountryNotFoundError, wb_utils.TopicNotFoundError) as e:
                logging.error(str(e))
                return  # or raise if you want to stop the entire pipeline

        elif source == "foreignassistance":
            source_filters = _translate_filters(data.filters)
            parse_foreign_assistance_data(source_filters)

        elif source == "ghed":
            source_filters = translate_filters(data.filters, source)
            country     = data.filters.get("country")
            start_year  = data.filters.get("start_year")
            end_year    = data.filters.get("end_year")
            parse_who_ghed_data(country, start_year, end_year)

        elif source == "sdg":
            source_filters = translate_filters(data.filters, source)
            from scrappers.sdgs_scraper import run_sdg_scraper
            from utils.sdg_geo_lookup import SDG_GEO_LOOKUP

            sdg_filters = translate_filters(data.filters, source)
            indicator = sdg_filters.get("indicator")

            # Attempt to resolve area_code
            area_code = sdg_filters.get("areaCode")
            if isinstance(area_code, str) and not area_code.isdigit():
                area_code_lookup = area_code.strip().lower()
                area_code = SDG_GEO_LOOKUP.get(area_code_lookup)

            if not area_code:
                country_name = data.filters.get("country", "").strip().lower()
                area_code = SDG_GEO_LOOKUP.get(country_name)

            if not area_code:
                print(f"⚠️  Could not resolve area code for country: {data.filters.get('country')}")
                area_code = None   # fallback to Pakistan

            start = data.filters.get("start_year")
            end   = data.filters.get("end_year")

            run_sdg_scraper(indicator, int(area_code), start, end)

        elif source == "oecd":
            source_filters = translate_filters(data.filters, source)
            run_oecd_scraper(source_filters)
            print("🚀 Starting parse_oecd_csvs() function.")
            parse_oecd_csvs()

        elif source == "bii":
            # 1. Run Selenium / cookies scraper
            scrape_bii(data.filters)          # pass filters dict; "TEST" ⇒ built-in demo
            # 2. Ingest freshly downloaded CSVs
            parse_bii_csvs() 

    return {
        "scrape_run_id": scrape_run_id,
        "record_count": len(all_data),
        "data": all_data if not data.ingest_only else []
    }