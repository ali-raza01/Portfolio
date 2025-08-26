import os
import time
from typing import Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
from datetime import datetime

class NoTracebackFormatter(logging.Formatter):
    # strip any traceback delivered via exc_info
    def formatException(self, exc_info):
        return ""                      # completely omit traceback
    def format(self, record):
        record.exc_info = None         # safety-belt
        return super().format(record)

LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
log_file = "ghed_scraper.log"

logger = logging.getLogger("ghed_scraper")
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

def run_who_ghed_scraper(country: str, start_year: int, end_year: int, download_dir: str) -> Optional[str]:
    download_dir = os.path.abspath(download_dir)
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    options = Options()
    # Use visible mode for download reliability
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "profile.default_content_settings.popups": 0,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 30)
    logger.info(f"💡 Effective download directory: {download_dir}")
    driver.get("https://apps.who.int/nha/database/Select/Indicators/en")

    try:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": download_dir
        })
    except Exception as e:
        logger.info(f"⚠️ Could not enable download behavior: {e}")

    def wait_for_overlay_to_disappear():
        try:
            wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, "PPWaitProgressBackground")))
        except:
            logger.info("Timeout waiting for overlay to disappear.")

    def select_checkbox_by_label(label):
        list_items = driver.find_elements(By.CSS_SELECTOR, 'ul#mixed li')
        for li in list_items:
            if label in li.text:
                checkbox = li.find_element(By.CSS_SELECTOR, 'input[type="checkbox"]')
                if not checkbox.is_selected():
                    checkbox.click()
                return True
        return False

    try:
        wait_for_overlay_to_disappear()
        countries_tab = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="qb_Container"]/div[1]/ul/li[2]')))
        countries_tab.click()
        wait_for_overlay_to_disappear()

        if not select_checkbox_by_label(country):
            logger.info(f"❌ Country '{country}' not found.")
            return None
        time.sleep(1)

        years_tab = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="qb_Container"]/div[1]/ul/li[3]')))
        years_tab.click()
        wait_for_overlay_to_disappear()

        for year in range(start_year, end_year + 1):
            select_checkbox_by_label(str(year))
            time.sleep(0.5)

        wait_for_overlay_to_disappear()
        download_link = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="qb_Container"]/div[1]/div/div/div/p/a')))
        download_link.click()
        logger.info("📥 Download initiated...")

        def wait_for_download(folder, timeout=8000):
            seconds = 0
            while seconds < timeout:
                files = os.listdir(folder)
                logger.info(f"[{seconds}s] Files in directory: {files}")
                xlsx_files = [f for f in files if f.lower().endswith(".xlsx")]
                partial_files = [f for f in files if f.endswith(".crdownload")]

                if xlsx_files and not partial_files:
                    final_file = os.path.join(folder, xlsx_files[0])
                    logger.info(f"✅ Found final file: {final_file}")
                    return final_file

                time.sleep(1)
                seconds += 1

            logger.info("❌ Timeout waiting for .xlsx file.")
            return None

        downloaded_file = wait_for_download(download_dir)
        if downloaded_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Sanitize values and construct filename using filters
            country_clean = country.replace(" ", "_")
            new_filename = f"{country_clean}_{start_year}_{end_year}_{timestamp}.xlsx"
            new_filepath = os.path.join(download_dir, new_filename)
            os.rename(downloaded_file, new_filepath)
            logger.info(f"📄 File renamed to: {new_filename}")
            return new_filepath
        else:
            logger.info("❌ Timeout: File not downloaded.")
            return None

    except Exception as e:
        logger.info(f"❌ Error: {str(e)}")
        return None

    finally:
        driver.quit()
        logger.info("🧹 Browser closed")
