"""
BII (British International Investment) – Investments search‑page scraper
=======================================================================

* Select filters (country / sector / dates) via Selenium (using **undetected‑chromedriver**).
* Click the **Search** button, then download the CSV:
  1. Fast path → `requests` with pre‑solved cookies.
  2. Fallback → submit the CSV form in the live browser.
* Archive downloaded file and return its final path.

-----------------------------------------------------------------------
Usage
-----------------------------------------------------------------------
from bii_scraper import scrape_bii

filters = {
    "country": "Nigeria",
    "sector": "Health",
    "start_year": 2015,
    "end_year": 2022,
}

scrape_bii(filters)
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union

import requests
import undetected_chromedriver as uc
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.keys import Keys

# ---------------------------------------------------------------------
# CLEAR UC CACHE (avoids mismatched driver versions)
# ---------------------------------------------------------------------
shutil.rmtree(os.path.expandvars(r"%LOCALAPPDATA%\undetected_chromedriver"), ignore_errors=True)
uc.Chrome.__del__ = lambda self: None
# ---------------------------------------------------------------------
# CONSTANTS / SELECTORS
# ---------------------------------------------------------------------
BASE_URL = "https://www.bii.co.uk/en/our-impact/search-results/"
# absolute string (needed for Chrome prefs)
DOWNLOAD_DIR = os.path.abspath("bii_downloads")

# pathlib.Path version for all file ops
DOWNLOAD_PATH = Path(DOWNLOAD_DIR).resolve()
DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)

ARCHIVE_PATH  = DOWNLOAD_PATH / "archive"
ARCHIVE_PATH.mkdir(exist_ok=True)
LOGFILE = "bii_scraper.log"

COUNTRY_DROPDOWN = (
    '//*[@id="main-grid"]/main/section[1]/div/div[2]/div/form/div[2]/span/span[1]/span'
)
SECTOR_DROPDOWN = (
    '//*[@id="main-grid"]/main/section[1]/div/div[2]/div/form/div[3]/span/span[1]/span'
)
START_DATE_INPUT = '//*[@id="inv-datefrom"]'
END_DATE_INPUT = '//*[@id="inv-dateto"]'
SEARCH_BUTTON = '//*[@id="main-grid"]/main/section[1]/div/div[2]/div/form/button'
DOWNLOAD_CSV_PATH = '//*[@id="tab-direct"]/form[1]'
FUNDS_TAB_XPATH       = '//*[@id="tab-funds-label"]'
UNDERLYING_TAB_XPATH  = '//*[@id="tab-underlying-label"]'

TIMEOUT = 15

# ── retry / rename helpers ─────────────────────────────────────────────
MAX_DOWNLOAD_RETRIES   = 3
BROKEN_URL_SIGNATURES = [
    "chrome-error://",
    "data:text/html,chromewebdata",
    "ERR_INVALID_RESPONSE",
]

class ValueNotFoundError(Exception):
    pass


TEST_FILTERS: Dict[str, Union[str, List[str], int]] = {
    "country": "Nigeria",
    "sector": "Health",
    "start_year": 2015,
    "end_year": 2022,
}

# ---------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------

def _setup_logging() -> None:
    DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)
    ARCHIVE_PATH.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        encoding="utf-8",
        handlers=[
            logging.FileHandler(LOGFILE, mode="a", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

# ---------------------------------------------------------------------
# WAITING FOR DOWNLOAD BUTTON TO APPEAR
# ---------------------------------------------------------------------

def _submit_search_and_wait(driver):
    """Click the search button and wait until the results + download button load."""
    try:
        logging.info("🔍 Clicking the Search button …")
        driver.find_element(By.XPATH, SEARCH_BUTTON).click()
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, DOWNLOAD_CSV_PATH))
        )
        logging.info("✅ Search completed and CSV download is now available")
    except TimeoutException:
        raise RuntimeError("Search results did not load in time")


# ---------------------------------------------------------------------
# UNDETECTED‑CHROMEDRIVER
# ---------------------------------------------------------------------

def _get_undetected_driver(version_main: int = 137):
    opts = uc.ChromeOptions()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--start-maximized")

    # NEW ↓↓↓  force Chrome to use our project download folder
    prefs = {
        "download.default_directory": str(DOWNLOAD_PATH.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    opts.add_experimental_option("prefs", prefs)
    # opts.add_argument("--headless=new")
    driver = uc.Chrome(options=opts, version_main=version_main)

    # extra safety for some Chrome builds
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": str(DOWNLOAD_PATH.resolve())},
        )
    except Exception:
        pass

    return driver


# ---------------------------------------------------------------------
# POP‑UP HELPER
# ---------------------------------------------------------------------

def _handle_popups(driver):
    try:
        gdpr = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="moove_gdpr_cookie_info_bar"]/div/div/div[2]/button'))
        )
        gdpr.click()
        logging.info("🍪 Cookie banner accepted")
    except Exception:
        logging.info("🍪 No cookie banner")

    try:
        sub = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="subscription-close"]'))
        )
        sub.click()
        logging.info("❌ Subscription popup closed")
    except Exception:
        logging.info("❌ No subscription popup")

# ---------------------------------------------------------------------
# FILTER UTILITIES
# ---------------------------------------------------------------------

def _normalise_filters(f):
    return TEST_FILTERS if f in (None, "TEST") else f


def _select_from_multiselect(driver, xpath: str, values: List[str], label: str):
    elem = WebDriverWait(driver, TIMEOUT).until(EC.element_to_be_clickable((By.XPATH, xpath)))
    elem.click()

    for val in values:
        box = WebDriverWait(driver, TIMEOUT).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "li.select2-search input"))
        )
        box.clear(); box.send_keys(val)
        opt_xpath = f"//li[contains(@class,'select2-results__option') and normalize-space()='{val}']"
        try:
            WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.XPATH, opt_xpath))).click()
            logging.info(f"{'🌍' if label=='country' else '🏗️'} {label} selected: {val}")
        except Exception:
            logging.warning(f"⚠️ {label} value not found: {val}")
            raise ValueNotFoundError(val)
        time.sleep(0.3)

    driver.find_element(By.TAG_NAME, "body").click()


def _click_search(driver):
    try:
        btn = WebDriverWait(driver, TIMEOUT).until(
            EC.element_to_be_clickable((By.XPATH, SEARCH_BUTTON))
        )
        
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.5)  # ✅ Wait to ensure scroll has completed

        try:
            btn.click()
        except ElementClickInterceptedException:
            driver.execute_script("arguments[0].click();", btn)

        logging.info("🔍 Search clicked")
        
        # ✅ Wait for the results form to appear (indicating search results have loaded)
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="tab-direct"]/form[1]'))
        )
        logging.info("✅ Search completed and CSV download is now available")

    except Exception as e:
        logging.warning(f"⚠️ Could not click search: {e}")


def _apply_filters(driver, f):
    c = f.get("country", [])
    s = f.get("sector", [])
    c = [c] if isinstance(c, str) else c
    s = [s] if isinstance(s, str) else s

    # Retry wrapper
    def try_select(label, xpath, values):
        for attempt in range(2):  # try up to 2 times
            try:
                _select_from_multiselect(driver, xpath, values, label)
                logging.info(f"✅ Selected {label} filter(s): {values}")
                break
            except Exception as e:
                logging.warning(f"⚠️ {label.title()} filter attempt {attempt + 1} failed: {e}")
                time.sleep(1.5)
        else:
            logging.error(f"❌ Failed to apply {label} filter after retries: {values}")
            raise ValueNotFoundError(f"{label} not found in dropdown.")
    if c:
        try_select("country", COUNTRY_DROPDOWN, c)
    if s:
        try_select("sector", SECTOR_DROPDOWN, s)

    if f.get("start_year"):
        try:
            sd = driver.find_element(By.XPATH, START_DATE_INPUT)
            sd.clear()
            v = f"01/01/{f['start_year']}"
            # Force full overwrite
            sd.send_keys(Keys.CONTROL + "a")
            sd.send_keys(Keys.DELETE)
            time.sleep(0.2)  # let DOM settle
            sd.send_keys(v)
            logging.info(f"⏱️ Start date {v}")
        except Exception as e:
            logging.error(f"❌ Failed to set start date: {e}")

    if f.get("end_year"):
        try:
            ed = driver.find_element(By.XPATH, END_DATE_INPUT)
            ed.clear()
            v = f"31/12/{f['end_year']}"
            # Force full overwrite
            ed.send_keys(Keys.CONTROL + "a")
            ed.send_keys(Keys.DELETE)
            time.sleep(0.2)  # let DOM settle
            ed.send_keys(v)
            logging.info(f"⏱️ End date {v}")
        except Exception as e:
            logging.error(f"❌ Failed to set end date: {e}")

    time.sleep(1.2)
    # _click_search(driver)

# ---------------------------------------------------------------------
# DOWNLOAD (BROWSER FALLBACK)
# ---------------------------------------------------------------------

def _slug(txt: str) -> str:
    """simple slug → lower-case, spaces→dashes"""
    return txt.strip().lower().replace(" ", "-")

def _build_filename(filters: dict, tab: str) -> str:
    """<countries>_<sectors>_<tab>_<timestamp>.csv"""
    if isinstance(filters.get("country"), str):
        country = _slug(filters["country"])
    else:
        country = "+".join(_slug(c) for c in filters.get("country", [])) or "all"

    if isinstance(filters.get("sector"), str):
        sector = _slug(filters["sector"])
    else:
        sector = "+".join(_slug(s) for s in filters.get("sector", [])) or "all"

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{country}_{sector}_{tab}_{ts}.csv"

def _rename_and_archive(raw_csv: Path, filters: dict, tab: str) -> Path:
    """Rename + move to archive folder; return final Path."""
    new_name = _build_filename(filters, tab)
    logging.info(f"New Name of file {new_name}")
    renamed  = raw_csv.with_name(new_name)
    logging.info(f"Renamed Name of file {renamed}")
    raw_csv.rename(renamed)

    ARCHIVE_PATH.mkdir(exist_ok=True)
    dest = ARCHIVE_PATH / new_name
    # if dest.exists():            # duplicate guard
    #     logging.warning("⚠️ duplicate archive, keeping existing file")
    #     renamed.unlink(missing_ok=True)
    #     return dest

    # shutil.move(renamed, dest)
    # logging.info(f"📦 archived → {dest.name}")
    return dest

def _is_chrome_net_error(drv) -> bool:
    url  = drv.current_url
    if url.startswith(("chrome-error://", "data:text/html,chromewebdata")):
        return True
    if drv.title in (
        "This site can’t be reached",
        "This page isn’t working",
        "ERR_INVALID_RESPONSE",
    ):
        return True
    try:
        return bool(drv.find_elements(By.ID, "main-frame-error"))
    except Exception:
        return False

PATTERNS      = (
    "export_investment_*.csv",
    "export_fund_*.csv",
    "export_underlying_*.csv",
)

from typing import Iterable

def _wait_for_download(
    folder: str | os.PathLike,
    patterns: Iterable[str] = ("*.csv",),   # same idea as your PATTERNS list
    timeout: int = 120,                     # seconds
    poll: float = 1.0                       # how often to check
) -> Path:
    """
    Wait until Chrome (or any browser) finishes writing a CSV file that matches
    *patterns* inside *folder*.  Returns the newest complete CSV `Path`.

    Raises TimeoutError if nothing completes within *timeout* seconds.
    """
    folder   = Path(folder)
    deadline = time.time() + timeout

    while time.time() < deadline:
        # All files currently in the directory (handy for verbose logging)
        files_here = [p.name for p in folder.iterdir()]
        logging.info(f"[{int(time.time() - (deadline - timeout))} s] Files: {files_here}")

        # Any partially-written downloads?
        partials = list(folder.glob("*.crdownload"))

        # Fully-written CSVs that match the supplied patterns
        finished: list[Path] = []
        for pat in patterns:
            finished.extend(folder.glob(pat))

        # If we’ve got at least one CSV *and* there are no partials left,
        # assume the newest CSV is the one we want.
        if finished and not partials:
            newest_csv = max(finished, key=lambda p: p.stat().st_ctime)
            logging.info(f"✅ Found final CSV: {newest_csv.name}")
            return newest_csv

        time.sleep(poll)

    raise TimeoutError("❌ Timed out waiting for CSV download to finish.")

# def _wait_for_download(timeout=120, poll=0.5):
#     """
#     Waits until Chrome finishes writing one of the three expected CSVs.
#     Returns the Path to the newest completed download.
#     Raises TimeoutError if nothing completes within *timeout* seconds.
#     """
#     end_time = time.time() + timeout
#     candidate = None

#     while time.time() < end_time:
#         finished = []

#         for pat in PATTERNS:
#             for f in DOWNLOAD_PATH.glob(pat):
#                 # if *.crdownload exists, Chrome is still writing
#                 if (f.with_suffix(f.suffix + ".crdownload")).exists():
#                     continue
#                 finished.append(f)

#         if finished:                       # at least one is fully written
#             candidate = max(finished, key=lambda p: p.stat().st_ctime)
#             break

#         time.sleep(poll)

#     if candidate is None:
#         raise TimeoutError("Timed out waiting for CSV download to finish")

#     logging.info(f"✅ downloaded {candidate.name}")
#     return candidate


def _download_csv_with_retry(
    driver,
    csv_button_xpath: str,
    filters: dict,
    tab: str,
) -> Path:
    """
    Clicks the CSV download button up to MAX_DOWNLOAD_RETRIES.
    If a Chrome “site can’t be reached” page appears, retries the process.
    """
    for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        logging.info(f"⬇️  {tab} CSV attempt {attempt}/{MAX_DOWNLOAD_RETRIES}")

        
        # Ensure download button is present
        WebDriverWait(driver, TIMEOUT).until(
            EC.element_to_be_clickable((By.XPATH, csv_button_xpath))
        )
        button = driver.find_element(By.XPATH, csv_button_xpath)

        # Scroll and click
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", button)
        time.sleep(1)

        prev_url = driver.current_url
        try:
            # --- click the button ------------------------------------------------
            WebDriverWait(driver, TIMEOUT).until(
                EC.element_to_be_clickable((By.XPATH, csv_button_xpath))
            )
            button = driver.find_element(By.XPATH, csv_button_xpath)
            driver.execute_script(
                "arguments[0].scrollIntoView({block:'center'});", button
            )
            button.click()
            if _is_chrome_net_error(driver):    # <-- only then do the recovery
                logging.warning("⚠️  Chrome net-error detected → retrying")
                try:
                    driver.execute_script("window.history.go(-1)")
                except WebDriverException:
                    pass                        # history disabled in error page
                driver.get(prev_url)            # back to the search results
                continue      
            # --- wait for the file ----------------------------------------------
            latest = _wait_for_download(
                        folder       = DOWNLOAD_PATH,
                        patterns     = PATTERNS,   # reuse your existing tuple of glob patterns
                        timeout      = 8000,       # match your earlier example if you like
                        poll         = 1           # check every second
                    )
            # latest = _wait_for_download()          # your helper from earlier
            return _rename_and_archive(latest, filters, tab)

        except StaleElementReferenceException:
            logging.warning("⚠️  Stale element – page refreshed, retrying")

        except TimeoutException:
            logging.warning("⚠️  Button not clickable – retrying")

        except TimeoutError as e:
            # raised by _wait_for_download()
            logging.warning(f"⚠️  {e} – retrying")

        # short back-off between attempts (optional)
        time.sleep(2)

    # all retries exhausted
    raise WebDriverException(
        f"❌ failed to download {tab} CSV after {MAX_DOWNLOAD_RETRIES} tries"
    )

        

def _download_tab_csv(driver, filters: dict, tab_key: str) -> Path:
    """
    tab_key: 'direct' | 'funds' | 'underlying'
    Tries CSV download up to MAX_DOWNLOAD_RETRIES times for each tab.
    """
    for tab_attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        logging.info(f"🔁 Tab '{tab_key}' download attempt {tab_attempt}/{MAX_DOWNLOAD_RETRIES}")

        try:
            # 1) switch to the requested tab
            if tab_key == "funds":
                tab_xpath = '//*[@id="tab-funds-label"]'
                button_xpath = '//*[@id="tab-funds"]/form[1]/button'
                WebDriverWait(driver, TIMEOUT).until(
                    EC.element_to_be_clickable((By.XPATH, tab_xpath))
                ).click()
                logging.info(f"📑 switched → {tab_key.capitalize()} tab")
                time.sleep(1)
            elif tab_key == "underlying":
                tab_xpath = '//*[@id="tab-underlying-label"]'
                button_xpath = '//*[@id="tab-underlying"]/form[1]/button'
                WebDriverWait(driver, TIMEOUT).until(
                    EC.element_to_be_clickable((By.XPATH, tab_xpath))
                ).click()
                logging.info(f"📑 switched → {tab_key.capitalize()} tab")
                time.sleep(1)
            else:  # direct
                tab_xpath = '//*[@id="tab-direct-label"]'
                button_xpath = '//*[@id="tab-direct"]/form[1]/button'


            # 2) try download via retry loop
            return _download_csv_with_retry(
                driver=driver,
                csv_button_xpath=button_xpath,
                filters=filters,
                tab=tab_key,
            )

        except Exception as e:
            logging.error(f"❌ {tab_key} tab failed attempt {tab_attempt}: {e}")
            time.sleep(2)

    raise WebDriverException(f"❌ Failed to download from {tab_key} tab after {MAX_DOWNLOAD_RETRIES} retries.")

# ---------------------------------------------------------------------
# MAIN ENTRY POINT
# ---------------------------------------------------------------------

def scrape_bii(filters: Dict | str | None = "TEST") -> Path | None:
    _setup_logging()
    f = _normalise_filters(filters)
    logging.info("🚀 Starting BII scrape")
    logging.info(f"Filters → {json.dumps(f)}")

    driver = _get_undetected_driver()
    try:
        driver.get(BASE_URL)
        WebDriverWait(driver, TIMEOUT).until(EC.presence_of_element_located((By.XPATH, COUNTRY_DROPDOWN)))
        _handle_popups(driver)
        _apply_filters(driver, f)
        
        _submit_search_and_wait(driver)
        # _click_search(driver)
        # ── CSV downloads ───────────────────────────────────────────
        archived: List[Path] = []

        for tab_key in ("direct", "funds", "underlying"):
            try:
                archived.append(_download_tab_csv(driver, f, tab_key))
            except Exception as err:
                archived.append(None)
                logging.error(f"❌ {tab_key} tab failed: {err}")

        if all(a is None for a in archived):
            raise RuntimeError("All CSV downloads failed")

        # filter out None entries
        return [p for p in archived if p]
    except ValueNotFoundError as e:
            logging.error(str(e))
            driver.quit()
            return
    finally:
        try:
            driver.quit()              # close browser
        except Exception:
            pass
        try:
            driver.service.stop()      # ensure chromedriver.exe is closed
        except Exception:
            pass
        del driver
        import gc
        gc.collect()

# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
if __name__ == "__main__":
    import argparse, json, sys
    p = argparse.ArgumentParser()
    p.add_argument("payload", help="JSON string/file or 'TEST'")
    a = p.parse_args()
    if a.payload.upper() == "TEST":
        scrape_bii("TEST")
    elif Path(a.payload).exists():
        scrape_bii(json.load(open(a.payload)))
    else:
        scrape_bii(json.loads(a.payload))
