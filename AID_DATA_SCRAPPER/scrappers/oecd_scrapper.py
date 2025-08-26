from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import time
import os
from selenium.webdriver.common.action_chains import ActionChains
import logging
import re
import shutil

class NoTracebackFormatter(logging.Formatter):
    # strip any traceback delivered via exc_info
    def formatException(self, exc_info):
        return ""                      # completely omit traceback
    def format(self, record):
        record.exc_info = None         # safety-belt
        return super().format(record)

LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
log_file = "oecd_scraper.log"

logger = logging.getLogger("oecd_scraper")
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

DOWNLOAD_DIR = os.path.join(os.getcwd(), "oecd_downloads")
ARCHIVE_DIR  = os.path.join(DOWNLOAD_DIR, "archive")        # NEW
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR,  exist_ok=True)     

SECTOR_XPATHS = {
    "Agriculture and fisheries": '//*[@id="id_home_page"]/div/div[2]/div[2]/div/div[2]/div/div/div/div/div[1]/button/div/span[1]',
    "Development": '//*[@id="id_home_page"]/div/div[2]/div[2]/div/div[2]/div/div/div/div/div[2]/button/div/span[1]',
    "Economy": '//*[@id="id_home_page"]/div/div[2]/div[2]/div/div[2]/div/div/div/div/div[3]/button/div/span[1]',
    "Education and skills": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[4]/button/div/span[1]',
    "Environment and climate change": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[5]/button/div/span[1]',
    "Finance and investment": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[6]/button/div/span[1]',
    "Public governance": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[7]/button/div/span[1]',
    "Health": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[8]/button/div/span[1]',
    "Industry, business and entrepreneurship": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[9]/button/div/span[1]',
    "Science, technology and innovation": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[10]/button/div/span[1]',
    "Employment": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[11]/button/div/span[1]',
    "Society": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[12]/button/div/span[1]',
    "Regional, rural and urban development": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[13]/button/div/span[1]',
    "Trade": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[14]/button/div/span[1]',
    "Transport": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[15]/button/div/span[1]',
    "Taxation": '//*[@id="id_home_page"]/div/div[2]/div/div[2]/div/div/div/div/div[16]/button/div/span[1]'
}

def load_country_lookup():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(base_dir, 'utils', 'oecd.txt')

    lookup = {}

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            raw = line.strip().rstrip(',')
            if not raw or '(' not in raw:
                continue

            name, _ = raw[:-1].split('(', 1) if raw.endswith(')') else (raw, '')
            name = name.strip().rstrip(',').lower()  # ← this is the fix
            lookup[name] = raw  # full text with code
    return lookup

COUNTRY_LOOKUP = load_country_lookup()

def with_retry(func, *args, attempts: int = 3, delay: int = 2, **kwargs):
    """
    Call *func* and automatically retry if it raises.
    """
    for i in range(1, attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"[RETRY] {func.__name__} failed on attempt {i}/{attempts}: {type(e).__name__}: {e.msg if hasattr(e, 'msg') else str(e).splitlines()[0]}")
            if i == attempts:         # give up on the last attempt
                raise
            time.sleep(delay)


def _normalize(fname: str) -> str:
    """
    Strip Chrome's duplicate suffix like ' (1)', ' (2)' etc. from filenames.
    E.g., 'data (1).csv' → 'data.csv'
    """
    return re.sub(r" \(\d+\)(?=\.)", "", fname)

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    return webdriver.Chrome(service=Service(), options=options)


def click_element(driver, xpath, timeout=20):
    return WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    ).click()


def select_sector(driver, sector_name):
    logger.info(f"[INFO] Selecting sector: {sector_name}")
    try:
        logger.info("[INFO] Locating all sector buttons...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.XPATH, '//button/div/span[1]'))
        )
        sectors = driver.find_elements(By.XPATH, '//button/div/span[1]')

        logger.info(f"[INFO] Found {len(sectors)} sector options. Scanning for match...")

        found = False
        for sector in sectors:
            label = sector.text.strip().lower()
            logger.info(f"  - Found sector: {label}")
            if sector_name.lower() in label:
                logger.info(f"[INFO] Clicking sector: {label}")
                driver.execute_script("arguments[0].scrollIntoView(true);", sector)
                time.sleep(0.5)
                sector.click()
                found = True
                break

        if not found:
            raise ValueError(f"Sector '{sector_name}' not found on the page.")

    except Exception as e:
        logger.info(f"[ERROR] Failed while selecting sector '{sector_name}': {e}")
        raise e




def set_time_period(driver, start_year, end_year):
    logger.info(f"[INFO] Attempting to set time period: {start_year} to {end_year}")
    
    try:
        # Check if the panel is present
        time_panel_xpath = '//*[@id="PANEL_PERIOD"]/div[1]/div/p'
        time_panel_present = len(driver.find_elements(By.XPATH, time_panel_xpath)) > 0

        if not time_panel_present:
            logger.info("[WARN] Time period panel not available for this sector. Skipping year selection.")
            return

        logger.info("[INFO] Time panel found. Expanding filter...")
        click_element(driver, time_panel_xpath)
        time.sleep(1)

        # Start year
        logger.info("[INFO] Setting start year...")
        click_element(driver, '//*[@id="year-Start"]/p')
        start_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="year-Start"]/div/input'))
        )
        start_input.clear()
        start_input.send_keys(str(start_year))
        time.sleep(1)

        # End year
        logger.info("[INFO] Setting end year...")
        click_element(driver, '//*[@id="year-End"]/p')
        end_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="year-End"]/div/input'))
        )
        end_input.clear()
        end_input.send_keys(str(end_year))
        time.sleep(1)

        logger.info("[INFO] Time period set successfully.")

    except Exception as e:
        logger.info(f"[ERROR] Failed to set time period: {e}")
        raise e


# def select_countries(driver, countries):
#     logger.info(f"[INFO] Selecting countries: {countries}")

#     try:
#         # Step 1: Expand Reference Area panel
#         panel_xpath = '//*[@id="Reference area"]'
#         country_panel = WebDriverWait(driver, 10).until(
#             EC.presence_of_element_located((By.XPATH, panel_xpath))
#         )

#         logger.info("[INFO] Expanding 'Reference area' panel...")
#         driver.execute_script("arguments[0].scrollIntoView(true);", country_panel)
#         time.sleep(0.5)
#         country_panel.click()
#         time.sleep(1)

#         # Step 2: Get all country <li> elements, skipping the search box (li[1])
#         list_items_xpath = f'{panel_xpath}/div/div[1]/div/div/li[position() > 1]'
#         country_items = WebDriverWait(driver, 10).until(
#             EC.presence_of_all_elements_located((By.XPATH, list_items_xpath))
#         )

#         available_countries = [item.text.strip() for item in country_items if item.text.strip()]
#         logger.info("[INFO] Available countries:")
#         for c in available_countries:
#             logger.info(f"  - {c}")

#         found_any = False
#         for country in countries:
#             matched = False
#             for item in country_items:
#                 label = item.text.strip().lower()
#                 if country.lower() in label:
#                     checkbox = item.find_element(By.TAG_NAME, 'span')
#                     if 'Mui-checked' not in checkbox.get_attribute('class'):
#                         checkbox.click()
#                         logger.info(f"[INFO] Country selected: {label}")
#                     else:
#                         logger.info(f"[INFO] Country already selected: {label}")
#                     matched = True
#                     found_any = True
#                     break
#             if not matched:
#                 logger.info(f"[WARN] Country not found in filter: {country}")

#         if not found_any:
#             logger.info("[WARN] No matching countries selected.")

#     except Exception as e:
#         logger.info(f"[ERROR] Failed during country selection: {e}")
#         raise e

def _plain(text: str) -> str:
    """'Nigeria (NGA)' → 'Nigeria'"""
    return text.split('(')[0].strip()


def _select_country(driver, full_label: str):
    wait = WebDriverWait(driver, 10)

    # 1️⃣  Type only the plain name -----------------------------------------
    header_xpath = '//*[@id="Reference area"]//input'
    box = wait.until(EC.element_to_be_clickable((By.XPATH, header_xpath)))
    box.click()
    box.send_keys(Keys.CONTROL, 'a', Keys.DELETE)
    box.send_keys(_plain(full_label))
    time.sleep(0.4)                               # let React filter
    logger.info(f"[INFO] Typed country name: {full_label}")
    
    # 2️⃣  Locate fresh reference to the 2nd <li> *just before* clicking
    option_xpath = '//*[@id="Reference area"]/div/div[1]/div/div/li[2]'
    option = wait.until(EC.element_to_be_clickable((By.XPATH, option_xpath)))

    # 3️⃣  Scroll & real mouse-click via ActionChains (fires full event sequence)
    ActionChains(driver) \
        .move_to_element(option) \
        .pause(0.05) \
        .click(option) \
        .perform()

    # 4️⃣  Confirm the listbox closed ⇒ selection succeeded
    wait.until(EC.invisibility_of_element_located((
        By.XPATH, '//*[@id="Reference area"]//ul[@role="listbox"]')))
    logger.info(f"[INFO] ✅ Selected country: {full_label}")
    time.sleep(10)  # Let UI settle

def select_countries(driver, countries):
    """
    Click the MUI “Reference area” dropdown and tick the requested countries.
    Uses with_retry() so stale DOM swaps don’t blow up the run.
    """
    logger.info(f"[INFO] Selecting countries: {countries}")

    # 1️⃣ open the dropdown ------------------------------------------------------------------
    with_retry(
        lambda: WebDriverWait(driver, 10)
                   .until(EC.element_to_be_clickable((By.XPATH, '//*[@id="Reference area"]/div[2]')))
                   .click(),
        attempts=3
    )
    time.sleep(1)           # let the list render
    if isinstance(countries, str):
        countries = [countries] 
    # 2️⃣ for each requested country, re-fetch the <li>/<button> list every time -------------
    for country in countries:
        try:
            actual_name = COUNTRY_LOOKUP.get(country.strip().lower())
            if not actual_name:
                logger.warning(f"[WARN] ❌ No mapping found for country '{country}'. Skipping.")
                continue
            with_retry(lambda: _select_country(driver, actual_name), attempts=3, delay=1)
        except ValueError as ve:
            logger.warning(f"[WARN] {ve}")
        except StaleElementReferenceException as se:
            logger.warning(f"[WARN] Stale reference while selecting '{country}': {se.msg}")
        except Exception as e:
            logger.error(f"[ERROR] Unexpected failure selecting '{country}': {type(e).__name__}: {str(e).splitlines()[0]}")

def setup_driver():
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    # options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--log-level=3")
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--start-maximized")
    return webdriver.Chrome(service=Service(), options=options)

def _wait_for_download(before: set, timeout: int = 30) -> str:
    """
    Wait until a new, fully-written file (no .crdownload) appears
    in DOWNLOAD_DIR and return its full path.
    """
    end = time.time() + timeout
    while time.time() < end:
        now = {f for f in os.listdir(DOWNLOAD_DIR)
               if not f.endswith(".crdownload") and os.path.isfile(os.path.join(DOWNLOAD_DIR, f))}
        new = now - before
        if new:
            return os.path.join(DOWNLOAD_DIR, new.pop())
        time.sleep(1)
    raise TimeoutError("Download did not finish in time.")

def _archive_or_delete(file_path: str) -> None:
    """
    If the same *base* name is already in ARCHIVE_DIR, delete the new file;
    otherwise move it into ARCHIVE_DIR.
    """
    base_name     = _normalize(os.path.basename(file_path))
    archived_path = os.path.join(ARCHIVE_DIR, base_name)

    if os.path.exists(archived_path):
        logger.info(f"[DUPLICATE] '{base_name}' already archived → deleting new copy.")
        os.remove(file_path)
    # else:
    #     shutil.move(file_path, archived_path)
    #     logger.info(f"[ARCHIVED ] '{base_name}' moved to archive.")

def _wait_for_download(before: set, timeout: int = 30) -> str:
    """
    Waits for a new .csv file to appear in the DOWNLOAD_DIR and not be partial.
    Returns the full path to the downloaded file.
    """
    end = time.time() + timeout
    while time.time() < end:
        now = {f for f in os.listdir(DOWNLOAD_DIR)
               if f.endswith(".csv") and not f.endswith(".crdownload")}
        new = now - before
        if new:
            return os.path.join(DOWNLOAD_DIR, new.pop())
        time.sleep(1)
    raise TimeoutError("Download did not finish in time.")

# ── download logic (only the inner part changed) ───────────────────────────
def download_all_files(driver):
    logger.info("[INFO] Starting dataset download loop...")
    file_index = 1
    while True:
        try:
            file_xpath = f'//*[@id="id_search_page"]/div/div/div[2]/div[4]/div[{file_index}]/div/div[1]/div[1]/div/span/div/h6/a'
            logger.info(f"[INFO] Trying file link #{file_index}")

            link = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, file_xpath)))
            driver.execute_script("arguments[0].scrollIntoView(true);", link)

            # --- remember what we already have before clicking ---
            before_dl = {f for f in os.listdir(DOWNLOAD_DIR)
                         if not f.endswith(".crdownload") and os.path.isfile(os.path.join(DOWNLOAD_DIR, f))}

            link.click()
            logger.info(f"[INFO] Opened file detail page #{file_index}")
            time.sleep(3)

            # Step 1: click download icon
            download_btn_xpath = '//*[@id="id_vis_page"]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div[2]/div[2]/button'
            download_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, download_btn_xpath)))
            driver.execute_script("arguments[0].scrollIntoView(true);", download_button)
            download_button.click()
            logger.info(f"[INFO] Clicked download icon")

            # Step 2: click CSV option
            csv_button_xpath = '//*[@id="csv.selection"]'
            csv_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, csv_button_xpath)))

            # Record pre-download state
            before_dl = {f for f in os.listdir(DOWNLOAD_DIR)
                        if f.endswith(".csv") and not f.endswith(".crdownload")}

            csv_button.click()
            logger.info("[INFO] Clicked CSV download option")

            # Wait for and process download
            downloaded_file = _wait_for_download(before_dl)
            _archive_or_delete(downloaded_file)

            # Step 3: back to search page
            driver.back()
            logger.info(f"[INFO] Returning to file list...")

            # quick check for next result row
            next_file_xpath = f'//*[@id="id_search_page"]/div/div/div[2]/div[4]/div[{file_index + 1}]/div/div[1]/div[1]/div/span/div/h6/a'
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, next_file_xpath))
            )
            file_index += 1

        except (NoSuchElementException, TimeoutException) as e:
            # e.msg  → just the readable one-liner  (“no such element: …”)
            logger.error("❌ [INFO] No more files found or error occurred: %s", e.msg)
            break

    logger.info("[DONE] All available datasets downloaded and processed.")



def run_scraper(sector_name, start_year, end_year, countries):
    logger.info(f"[START] Running OECD Data Explorer scraper")
    driver = setup_driver()
    driver.get("https://data-explorer.oecd.org/")
    time.sleep(3)

    try:
        # STEP 1: Sector
        try:
            logger.info("[STEP 1] Selecting Sector...")
            with_retry(select_sector,   driver, sector_name)
        except Exception as e:
            logger.info(f"[WARNING] Sector selection skipped: {e}")

        # STEP 2: Time Period
        try:
            logger.info("[STEP 2] Setting Time Period...")
            with_retry(set_time_period, driver, start_year, end_year)
        except Exception as e:
            logger.info(f"[WARNING] Time period setting skipped: {e}")

        # STEP 3: Countries
        try:
            logger.info("[STEP 3] Selecting Countries...")
            with_retry(select_countries, driver, countries)
        except Exception as e:
            logger.info(f"[WARNING] Country selection skipped: {e}")

        logger.info("[STEP 4] Downloading Filtered Datasets...")
        download_all_files(driver)

    except Exception as e:
        logger.info(f"[FAILURE] Unhandled error occurred: {e}")

    finally:
        logger.info("[EXIT] Closing browser")
        driver.quit()


def run_oecd_scraper(filters: dict = {}):
    sector = filters.get("sector")
    start_year = filters.get("start_year")
    end_year = filters.get("end_year")
    countries = filters.get("country")
    print(sector)
    print(start_year)
    print(end_year)
    print(countries)
    run_scraper(sector, start_year, end_year, countries)
   

