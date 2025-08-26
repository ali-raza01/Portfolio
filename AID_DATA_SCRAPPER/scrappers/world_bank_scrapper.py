# scrappers/world_bank_scrapper.py

import requests
import pandas as pd

import logging

class NoTracebackFormatter(logging.Formatter):
    # strip any traceback delivered via exc_info
    def formatException(self, exc_info):
        return ""                      # completely omit traceback
    def format(self, record):
        record.exc_info = None         # safety-belt
        return super().format(record)

LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
log_file = "world_bank_scraper.log"

logger = logging.getLogger("world_bank_scraper")
logger.setLevel(logging.INFO)
logger.propagate = False              # don’t pass records to root logger

# clean stream handler ------------------------------------------------------
stream_h = logging.StreamHandler()
stream_h.setFormatter(NoTracebackFormatter(LOG_FMT))
logger.addHandler(stream_h)

# clean file handler --------------------------------------------------------
if not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith(log_file)
           for h in logger.handlers):
    file_h = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_h.setFormatter(NoTracebackFormatter(LOG_FMT))
    logger.addHandler(file_h)

def list_data_sources():
    url = "https://api.worldbank.org/v2/sources?format=json&per_page=1000"
    res = requests.get(url)
    data = res.json()
    return pd.DataFrame(data[1])[['id', 'name']]

def get_indicators_for_source(source_id):
    url = f"https://api.worldbank.org/v2/indicator?source={source_id}&format=json&per_page=1000"
    res = requests.get(url)
    data = res.json()
    return pd.DataFrame(data[1]) if len(data) > 1 else pd.DataFrame()

def fetch_indicator_data(country_code, indicator_code, date_range):
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}"
    params = {
        "format": "json",
        "date": date_range,
        "per_page": 1
    }
    res = requests.get(url, params=params)
    try:
        data = res.json()
        return pd.json_normalize(data[1]) if len(data) > 1 else pd.DataFrame()
    except Exception:
        return pd.DataFrame()
