# --- foreign_assistance_scraper.py ---

import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import quote
import pandas as pd
from io import StringIO
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import json
from datetime import datetime
import urllib.parse

class NoTracebackFormatter(logging.Formatter):
    # strip any traceback delivered via exc_info
    def formatException(self, exc_info):
        return ""                      # completely omit traceback
    def format(self, record):
        record.exc_info = None         # safety-belt
        return super().format(record)

LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
log_file = "foreign_scraper.log"

logger = logging.getLogger("foreign_scraper")
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

# Known columns from the dataset
KNOWN_COLUMNS = [
    'Country ID', 'Country Code', 'Country Name', 'Region ID', 'Region Name',
    'Income Group ID', 'Income Group Name', 'Income Group Acronym',
    'Managing Agency ID', 'Managing Agency Acronym', 'Managing Agency Name',
    'Managing Sub-agency or Bureau ID', 'Managing Sub-agency or Bureau Acronym',
    'Managing Sub-agency or Bureau Name', 'Implementing Partner Category ID',
    'Implementing Partner Category Name', 'Implementing Partner Sub-category ID',
    'Implementing Partner Sub-category Name', 'Implementing Partner ID',
    'Implementing Partner Name', 'International Category ID',
    'International Category Name', 'International Sector Code',
    'International Sector Name', 'International Purpose Code',
    'International Purpose Name', 'US Category ID', 'US Category Name',
    'US Sector ID', 'US Sector Name', 'Funding Account ID', 'Funding Account Name',
    'Funding Agency ID', 'Funding Agency Name', 'Funding Agency Acronym',
    'Foreign Assistance Objective ID', 'Foreign Assistance Objective Name',
    'Aid Type Group ID', 'Aid Type Group Name', 'Activity ID', 'Submission ID',
    'Activity Name', 'Activity Description', 'Activity Project Number',
    'Activity Start Date', 'Activity End Date', 'Transaction Type ID',
    'Transaction Type Name', 'Fiscal Year', 'Transaction Date',
    'Current Dollar Amount', 'Constant Dollar Amount', 'aid_type_id',
    'aid_type_name', 'activity_budget_amount', 'submission_activity_id'
]

def _retry_plan(original):
    """
    Return a list of filter-dicts to try, progressively loosening:
    1. everything
    2. minus Funding Agency Name
    3. minus Fiscal Year
    4. minus US Sector Name
    Country is **never** dropped.
    """
    attempts = [original or {}]
    for col in ["Funding Agency Name", "Fiscal Year", "US Sector Name"]:
        if col in attempts[-1]:
            weaker = {k: v for k, v in attempts[-1].items() if k != col}
            attempts.append(weaker)
    return attempts

def build_filtered_url(base_url, filters=None):
    logger.info(f"❗ Current filters used in URL build: {filters}")
    base_sql = "select * from [./us_foreign_aid_complete]"
    if not filters:
        return f"{base_url}?sql={quote(base_sql)}"
    
    where_clauses = []
    for k, v in filters.items():
        k_sql = f'"{k}"'
        if isinstance(v, list):
            vals = ",".join(f"'{str(x)}'" for x in v)
            where_clauses.append(f"{k_sql} IN ({vals})")
        else:
            where_clauses.append(f"{k_sql} LIKE '%{v}%'")

    where_sql = " AND ".join(where_clauses)
    full_sql = f"select * from [./us_foreign_aid_complete] WHERE {where_sql}"
    encoded_sql = urllib.parse.quote(full_sql)

    return f"{base_url}?sql={encoded_sql}"

def run_foreign_assistance_scraper(filters=None):
    base_url = "https://foreignassistance-data.andrewheiss.com/2025-02-03_foreign-assistance"
    driver = None

    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        # -------- RETRY LOOP --------
        for attempt, filt in enumerate(_retry_plan(filters), start=1):
            logger.info(f"🔄 Attempt {attempt}: filters ➜ {filt}")
            url = build_filtered_url(base_url, filt)
            logger.info(f"🌐 Loading filtered URL:\n{url}")
            driver.get(url)
            time.sleep(4)

            # ① QUICK CHECK – did Datasette abort the query?
            if "sql interrupted" in driver.page_source.lower():
                logger.error(
                    "🛑 SQL query was interrupted by the server – try removing some "
                    "filters or selecting fewer fiscal years."
                )
                continue         # nothing more to do

            # 🖱️ Click the CSV link at specified XPath
            logger.info("🖱️ Click the CSV link at specified XPath")
            csv_link_xpath = "/html/body/div/section/p/a[2]"
            try:
                csv_link = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, csv_link_xpath))
                )
                csv_link.click()
            except (NoSuchElementException, TimeoutException):
                # ② If the link is missing, double-check for an interrupted query
                if "sql interrupted" in driver.page_source.lower():
                    logger.error(
                        "🛑 SQL query was interrupted after page load – "
                        "reduce filter combinations (country, agency, sector, year) "
                        "and try again."
                    )
                else:
                    logger.error(
                        "❌ CSV link not found – page structure may have changed or "
                        "the filters returned zero rows."
                    )
                return pd.DataFrame()
            
            time.sleep(2)
            driver.switch_to.window(driver.window_handles[-1])
            # Success path – leave the loop with the filters that worked
            filters = filt          # keep for filename construction ↓
            break
        # 🔽 NEW: Switch to the new tab
        logger.info("🧭 Switch to the new tab")
        driver.switch_to.window(driver.window_handles[-1])
        time.sleep(2)

        # Extract CSV text content
        logger.info("📄 Extract CSV text content")
        csv_text = driver.find_element(By.XPATH, "/html/body/pre").text

        # 🔽 NEW: Construct a unique filename from filters and timestamp
        # logger.info("🔽 Construct a unique filename from filters and timestamp")
        filter_name = "_".join(f"{k}-{v}" for k, v in filters.items()) if filters else "all"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filter_name}_{timestamp}.csv"

        # 🔽 NEW: Setup download and archive paths
        # logger.info("🔽 Setup download and archive paths")
        download_dir = "foreign_assistance_downloads"
        archive_dir = os.path.join(download_dir, "archive")
        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(archive_dir, exist_ok=True)
        download_path = os.path.join(download_dir, filename)

        # 🔽 NEW: Check if filter already exists in archive
        logger.info("🔍 Check if filter already exists in archive")
        existing = [
            f for f in os.listdir(archive_dir)
            if f.startswith(filter_name + "_") and f.endswith(".csv")
        ]
        if existing:
            logger.info(f"⚠️ Filter '{filter_name}' already downloaded previously ({existing[0]}). Skipping save.")
            return pd.DataFrame()

        # 🔽 NEW: Save CSV data
        logger.info("🔽 Save CSV data")
        with open(download_path, "w", encoding="utf-8") as f:
            f.write(csv_text)
        logger.info(f"✅ Saved CSV to: {download_path}")

        logger.info("🔽 Check if file already exists in archive")
        # Normalize filter name
        filter_name = "_".join(f"{k}-{v}" for k, v in filters.items()) if filters else "all"
        filter_name_safe = filter_name.replace(" ", "_").lower()

        # Check archive folder
        archive_dir = os.path.join("foreign_assistance_downloads", "archive")
        os.makedirs(archive_dir, exist_ok=True)

        matching_archives = [
            f for f in os.listdir(archive_dir)
            if f.startswith(filter_name_safe + "_") and f.endswith(".csv")
        ]

        if matching_archives:
            logger.info(f"⚠️ Data for filter '{filter_name}' already exists in archive: {matching_archives[0]}")
        # else:
            # Fell through the loop ⇒ every attempt failed
            # logger.error("❌ All attempts failed even after removing filters.")
            # return pd.DataFrame()
        
        return download_path
    
    
    except (NoSuchElementException, TimeoutException) as e:
        # e.msg  → just the readable one-liner  (“no such element: …”)
        logger.error("❌ Selenium error: %s", e.msg)   # no server stack trace
        return pd.DataFrame()

    finally:
        if driver:
            try:
                driver.quit()
                logger.info("🧹 Browser closed")
            except Exception as cleanup_error:
                logger.info(f"⚠️ Failed to quit driver: {cleanup_error}")