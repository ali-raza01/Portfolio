# --- fcdo_scrapper.py ---

import os
import time
import logging
import traceback
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from unified_mapping import FILTER_VALUE_FIXES  # now centralized
from datetime import datetime
import logging


class NoTracebackFormatter(logging.Formatter):
    # strip any traceback delivered via exc_info
    def formatException(self, exc_info):
        return ""                      # completely omit traceback
    def format(self, record):
        record.exc_info = None         # safety-belt
        return super().format(record)

LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
log_file = "fcdo_scrapper_errors.log"

logger = logging.getLogger("fcdo_scrapper_errors")
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


FILTER_XPATH_OVERRIDES = {
    "Activity Status": "//*[@id='searchFilters']/fieldset[1]/div",
    "Government Department(s)": "//*[@id='searchFilters']/fieldset[2]/div"
}

def run_fcdo_scraper(filters: dict):
    # Replace folder name
    download_dir = os.path.abspath("fcdo_downloads")
    os.makedirs(download_dir, exist_ok=True)

    # Setup archive directory
    archive_dir = os.path.join(download_dir, "archive")
    os.makedirs(archive_dir, exist_ok=True)

    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--window-size=1920,1080')

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 20)

    json_links = []

    try:
        logger.info("🔄 Opening FCDO DevTracker...")
        driver.get("https://devtracker.fcdo.gov.uk/department/FCDO")

        try:
            cookie_btn = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[text()="Accept analytics cookies"]')))
            cookie_btn.click()
            logger.info("🍪 Accepted cookies.")
        except:
            logger.info("✅ No cookie prompt.")

        # Expand all filters
        try:
            logger.info("⏬ Expanding filter sections...")
            filter_headers = driver.find_elements(By.XPATH, '//*[@id="searchFilters"]/fieldset/legend')
            for header in filter_headers:
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", header)
                    header.click()
                    time.sleep(0.5)
                except:
                    continue
        except Exception as e:
            logger.warning(f"⚠️ Could not expand all filters: {e}")

        # Apply filters
        for filter_label, raw_filter_value in filters.items():
            filter_value = FILTER_VALUE_FIXES.get("FCDO", {}).get(filter_label, {}).get(raw_filter_value, raw_filter_value)
            logger.info(f"🔍 Applying filter: {filter_label} = {filter_value}")

            # First try checkbox-style label
            try:
                section_expand_xpath = f'//a[text()="{filter_label}"]'
                try:
                    toggle = wait.until(EC.element_to_be_clickable((By.XPATH, section_expand_xpath)))
                    toggle.click()
                    logger.info(f"📂 Expanded '{filter_label}' section")
                    time.sleep(1)
                except:
                    logger.debug(f"ℹ️ Could not expand section '{filter_label}' — may already be expanded.")

                label_xpath = f'//label[normalize-space()="{filter_value}"]'
                label_elem = wait.until(EC.element_to_be_clickable((By.XPATH, label_xpath)))
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", label_elem)
                driver.execute_script("arguments[0].click();", label_elem)
                logger.info(f"☑️ Selected checkbox: {filter_value}")
                time.sleep(1)
                continue  # Success — skip to next filter

            except Exception as checkbox_error:
                logger.warning(f"⚠️ Checkbox not found for '{filter_label}' = '{filter_value}', trying fallback text input...")

            # Fallback to text input field
            try:
                base_xpath = FILTER_XPATH_OVERRIDES.get(
                    filter_label,
                    f"//div[label[text()='{filter_label}']]/following-sibling::div[1]"
                )
                input_xpath = base_xpath + "/ul/li/input"

                wait.until(EC.element_to_be_clickable((By.XPATH, base_xpath))).click()
                input_box = wait.until(EC.element_to_be_clickable((By.XPATH, input_xpath)))

                input_box.clear()
                input_box.send_keys(filter_value)
                time.sleep(1)
                input_box.send_keys(Keys.ENTER)
                logger.info(f"⌨️ Entered text value: {filter_value}")
                time.sleep(2)

            except Exception as text_input_error:
                logger.warning(f"❌ Could not apply filter '{filter_label}' with fallback either: {text_input_error}")


        wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="response-container"]/div/h3/a')))
        logger.info("✅ Filtered projects loaded.")

        page = 1
        while True:
            logger.info(f"📄 Scraping page {page}...")
            project_links = driver.find_elements(By.XPATH, '//*[@id="response-container"]/div/h3/a')
            project_urls = [link.get_attribute("href") for link in project_links]

            for url in project_urls:
                logger.info(f"🔗 Visiting project: {url}")
                driver.get(url)
                json_link = None

                # Try both XPaths for JSON link
                try:
                    json_elem = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="content"]/div[9]/div/div/a/strong')))
                    json_link = json_elem.find_element(By.XPATH, './..').get_attribute("href")
                except:
                    try:
                        json_elem = driver.find_element(By.XPATH, '//*[@id="content"]/div[8]/div/div/a')
                        json_link = json_elem.get_attribute("href")
                    except:
                        pass

                if json_link:
                    logger.info(f"✅ Found JSON link: {json_link}")
                    json_links.append(json_link)
                    # break
                else:
                    logger.warning("❌ No JSON link found.")

                driver.back()
                time.sleep(2)

            # Pagination handling (multiple layouts)
            pagination_clicked = False
            for next_xpath in [
                '//*[@id="ctrack_div"]/div/div[1]/div[2]/div[5]/div[2]/ul/li[3]/a',
                '//*[@id="content"]/div[3]/div[2]/div[3]/ul/li[3]/a'
            ]:
                try:
                    next_btn = driver.find_element(By.XPATH, next_xpath)
                    driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
                    next_btn.click()
                    page += 1
                    time.sleep(3)
                    logger.info(f"➡️ Clicked next page (XPath: {next_xpath})")
                    pagination_clicked = True
                    break
                except:
                    continue

            if not pagination_clicked:
                logger.info("⛔ No more pages.")
                break

    except Exception as e:
        logger.error("❌ FCDO Scraper error:")
        logger.error(traceback.format_exc())
        driver.save_screenshot("fcdo_debug.png")
        logger.info("🖼 Screenshot saved as fcdo_debug.png")

    finally:
        driver.quit()

    filter_str = "_".join([f"{str(k).replace(' ', '')}-{str(v).replace(' ', '')}" for k, v in filters.items()])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{filter_str}_{timestamp}.txt"
    output_path = os.path.join(download_dir, output_file)
    with open(output_path, "w") as f:
        for link in json_links:
            f.write(link + "\n")
    archived_links = set()
    for root, _, files in os.walk(archive_dir):
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    archived_links.add(line.strip())

    # Filter out duplicates
    filtered_links = []
    for link in json_links:
        stripped_link = link.strip()
        if stripped_link in archived_links:
            logging.info(f"Skipping already archived link: {stripped_link}")
        else:
            filtered_links.append(stripped_link)
    json_links = filtered_links

    # Save to uniquely named file
    with open(output_path, "w", encoding="utf-8") as f:
        for link in json_links:
            f.write(link + "\n")

    print(f"\n📦 Done! Saved {len(json_links)} JSON links to '{output_path}'")
    return output_path  # still returns path to saved file

    
