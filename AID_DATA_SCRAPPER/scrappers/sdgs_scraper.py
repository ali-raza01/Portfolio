import os
import shutil
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
# from scrappers.utils_ingest import ingest_data
import logging
from requests.exceptions import ReadTimeout
import sys
import os

# Add parent directory to sys.path (run-time import hack)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_setup_and_ingest_org import ingest_data

class NoTracebackFormatter(logging.Formatter):
    # strip any traceback delivered via exc_info
    def formatException(self, exc_info):
        return ""                      # completely omit traceback
    def format(self, record):
        record.exc_info = None         # safety-belt
        return super().format(record)

LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
log_file = "sdgs_scraper.log"

logger = logging.getLogger("sdgs_scraper")
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

# CONFIG – tweak if you keep downloads elsewhere
DOWNLOAD_DIR = Path(os.path.abspath("sdgs_downloads"))
ARCHIVE_DIR  = DOWNLOAD_DIR / "archive"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://unstats.un.org/sdgs/UNSDGAPIV5/v1/sdg"

def run_sdg_scraper(indicator: str,
                    area_code: int,
                    start_year: int,
                    end_year: int):

    params = {
        "indicator": [indicator],
        "areaCode": [area_code],
        "timePeriodStart": start_year,
        "timePeriodEnd": end_year
    }

    logger.info(f"🌐  UN-SDG request → indicator={indicator} | area={area_code} | {start_year}-{end_year}")
    # Compose and print the final URL
    req = requests.Request("GET", f"{BASE_URL}/Indicator/Data", params=params).prepare()
    logger.info(f"🔗 Final SDG API URL → {req.url}")

    try:
        # Increase timeout from 60s to 120s (or more)
        r = requests.get(req.url, timeout=120)
        r.raise_for_status()
        payload = r.json()
    except ReadTimeout:
        logger.info("⏱️ SDG API request timed out. Try a smaller year range or wait and retry.")
        return

    if "data" not in payload or not payload["data"]:
        logger.info("⚠️  No data returned by SDG API.")
        return

    records = []
    first_item = payload["data"][0]
    logger.info(f"📦 Total raw items from SDG API: {len(payload['data'])}")
    logger.info(f"🔍 First item: {first_item}")

    country = first_item.get("geoAreaName", "Unknown")

    for item in payload["data"]:
        value = item.get("value") or item.get("Value")
        time_period = item.get("timePeriodStart") or item.get("timePeriod") or item.get("TimePeriodStart")

        if not value or not time_period:
            continue

        records.append({
            "project_id": f"SDG-{indicator}-{item.get('geoAreaCode')}-{time_period}",
            "project_title": f"SDG {indicator} - {item.get('indicator')[0] if isinstance(item.get('indicator'), list) else item.get('indicator')}",
            "donor_name": "United Nations",
            "country_code": item.get('geoAreaCode'),
            "country": country,
            "year_active": time_period,
            "indicator_values": value,
            "output_indicators": item.get('indicator')[0] if isinstance(item.get('indicator'), list) else item.get('indicator'),
            "source_of_data": "UNSDG",
            "last_updated": pd.to_datetime('today').strftime('%Y-%m-%d'),
            "source": "SDGS"
        })

    df = pd.DataFrame(records)
    logger.info(f"✅ Parsed {len(df)} SDG records")
    base_name = f"sdg_{indicator}_{area_code}_{start_year}_{end_year}"
    ts        = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = DOWNLOAD_DIR / f"{base_name}_{ts}.csv"

    # Save CSV
    df.to_csv(file_path, index=False)
    logger.info(f"💾 Saved raw file → {file_path}")
    
    # ────────────────────────────────────────────────────────
    # 2️⃣  Duplicate check in archive
    duplicate_in_archive = any(p.stem.startswith(base_name) for p in ARCHIVE_DIR.glob(f"{base_name}*.csv"))
    if duplicate_in_archive:
        logger.warning(f"🚫 Duplicate found in archive → skipping ingestion for {base_name}")
        file_path.unlink(missing_ok=True)            # delete the freshly-saved file
        return df                                    # still return df so caller can inspect

    # ────────────────────────────────────────────────────────
    # 3️⃣  Ingest and archive
    if not df.empty:
        try:
            ingest_data(df)
            logger.info("📥 Ingestion complete")
            shutil.move(str(file_path), ARCHIVE_DIR / file_path.name)
            logger.info(f"📦 Moved to archive → {ARCHIVE_DIR / file_path.name}")
        except Exception as e:
            logger.error(f"❌ Ingestion failed: {e}")
            # File will stay in downloads folder for debugging
    else:
        logger.info("⚠️  No valid records to ingest.")

    return df
