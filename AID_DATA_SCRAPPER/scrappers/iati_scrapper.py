import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
from urllib.parse import urlparse, quote_plus
import hashlib
from selenium.webdriver.common.keys import Keys

class NoTracebackFormatter(logging.Formatter):
    # strip any traceback delivered via exc_info
    def formatException(self, exc_info):
        return ""                      # completely omit traceback
    def format(self, record):
        record.exc_info = None         # safety-belt
        return super().format(record)

LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
log_file = "iati_scraper.log"

logger = logging.getLogger("iati_scraper")
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


def is_element_clickable(driver, xpath):
    try:
        el = driver.find_element(By.XPATH, xpath)
        return el.is_enabled() and el.is_displayed()
    except:
        return False

def clear_filter(filters, key):
    if key in filters:
        del filters[key]
        logger.info(f"🧹 Removed filter from dict: {key}")
    else:
        logger.info(f"ℹ️ Filter '{key}' not in current filters.")

def select_from_dropdown(driver, wait, input_xpath, value):
    try:
        dropdown_input = wait.until(EC.presence_of_element_located((By.XPATH, input_xpath)))
        dropdown_input.click()
        time.sleep(0.5)
        dropdown_input.clear()
        dropdown_input.send_keys(value)
        time.sleep(1)
        dropdown_input.send_keys(Keys.ENTER)
        logger.info(f"✅ Selected value: {value}")
    except Exception as e:
        logger.error(f"❌ Failed to select '{value}' at {input_xpath}: {e}")

def select_year_field(driver, wait, container_xpath, input_xpath, value):
    try:
        # Open the dropdown
        container = wait.until(EC.element_to_be_clickable((By.XPATH, container_xpath)))
        container.click()
        time.sleep(0.5)

        # Get the correct input field and type
        input_element = wait.until(EC.element_to_be_clickable((By.XPATH, input_xpath)))
        input_element.clear()
        input_element.send_keys(value)
        time.sleep(0.3)
        input_element.send_keys(Keys.ENTER)

        logger.info(f"✅ Year '{value}' selected at {container_xpath}")
    except Exception as e:
        logger.error(f"❌ Failed to select year '{value}' at {container_xpath}: {e}")



def run_iati_scraper(filters):
    base_url = "https://d-portal.org/ctrack.html#view=search"
    filter_query = "&" + "&".join([f"{k}={v}" for k, v in filters.items()])
    search_url = base_url + filter_query
    main_url = f"https://d-portal.org/ctrack.html?{filter_query}#view=ended"

    download_dir = os.path.abspath("iati_downloads")
    os.makedirs(download_dir, exist_ok=True)

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--log-level=3")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 10)

    try:
        driver.get(base_url)
        time.sleep(6)

        # Fill in filters via dropdowns
        if "country" in filters:
            select_from_dropdown(driver, wait, '//*[@id="view_search_select_country_chosen"]/ul/li/input', filters["country"])
        if "sector" in filters:
            select_from_dropdown(driver, wait, '//*[@id="view_search_select_sector_chosen"]/ul/li/input', filters["sector"])
        if "reporting_org" in filters:
            select_from_dropdown(driver, wait, '//*[@id="view_search_select_publisher_chosen"]/ul/li/input', filters["reporting_org"])
        if "status" in filters:
            select_from_dropdown(driver, wait, '//*[@id="view_search_select_status_chosen"]/ul/li/input', filters["status"])
        if "sector_group" in filters:
            select_from_dropdown(driver, wait, '//*[@id="view_search_select_category_chosen"]/ul/li/input', filters["sector_group"])

        # Year selection (click approach)
        if "start_year" in filters:
            select_year_field(
                driver, wait,
                '//*[@id="view_search_select_year_min_chosen"]',
                '//*[@id="view_search_select_year_min_chosen"]/div/div/input',
                filters["start_year"]
            )

        if "end_year" in filters:
            select_year_field(
                driver, wait,
                '//*[@id="view_search_select_year_max_chosen"]',
                '//*[@id="view_search_select_year_max_chosen"]/div/div/input',
                filters["end_year"]
            )
        # Loop to try filter reductions until View All appears
        filter_reduction_level = 0
        max_reductions = 3  # org, year(s), sector
        while filter_reduction_level <= max_reductions:
            try:
                
                explore_button = wait.until(EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="ctrack_div"]/div/div[1]/div[2]/div[4]/div[3]/a')))
                explore_button.click()
                logger.info("✅ Clicked 'Explore' button")
                time.sleep(5)
            except Exception as e:
                logger.error("❌ 'Explore' button not found — exiting scraper. Reason: %s", e)
                driver.quit()
                return []
            
            try:
                view_all_xpath = '//*[@id="ctrack_div"]/div/div[1]/div[2]/div[8]/div[2]/a'
                if is_element_clickable(driver, view_all_xpath):
                    view_all_button = wait.until(EC.element_to_be_clickable((By.XPATH, view_all_xpath)))
                    view_all_button.click()
                    logger.info("✅ Clicked 'View All' — filters sufficient")
                    time.sleep(6)
                    break  # success — exit loop
                else:
                    logger.warning("❌ 'View All' not clickable — trying with reduced filters...")
                    filter_reduction_level += 1
                    if filter_reduction_level > max_reductions:
                        logger.error("❌ Maximum filter reductions reached. Exiting.")
                        driver.quit()
                        return []

                    # Go back and reload base page
                    driver.get(base_url)
                    time.sleep(6)

                    # Re-apply filters based on current reduction level
                    if filter_reduction_level == 1 and "reporting_org" in filters:
                        clear_filter(filters, 'reporting_org')
                    if filter_reduction_level == 2:
                        if "start_year" in filters:
                            clear_filter(filters, 'start_year')
                        if "end_year" in filters:
                            clear_filter(filters, 'end_year')
                    if filter_reduction_level == 3 and "sector" in filters:
                        clear_filter(filters, 'sector')

                    # Re-select remaining filters
                    if "country" in filters:
                        select_from_dropdown(driver, wait, '//*[@id="view_search_select_country_chosen"]/ul/li/input', filters["country"])
                    if "sector_group" in filters:
                        select_from_dropdown(driver, wait, '//*[@id="view_search_select_category_chosen"]/ul/li/input', filters["sector_group"])
                    if "reporting_org" in filters:
                        select_from_dropdown(driver, wait, '//*[@id="view_search_select_publisher_chosen"]/ul/li/input', filters["reporting_org"])
                    if "status" in filters:
                        select_from_dropdown(driver, wait, '//*[@id="view_search_select_status_chosen"]/ul/li/input', filters["status"])
                    if "sector" in filters:
                        select_from_dropdown(driver, wait, '//*[@id="view_search_select_category_chosen"]/ul/li/input', filters["sector"])
                    if "start_year" in filters:
                        select_year_field(driver, wait, '//*[@id="view_search_select_year_min_chosen"]', '//*[@id="view_search_select_year_min_chosen"]/div/div/input', filters["start_year"])
                    if "end_year" in filters:
                        select_year_field(driver, wait, '//*[@id="view_search_select_year_max_chosen"]', '//*[@id="view_search_select_year_max_chosen"]/div/div/input', filters["end_year"])

            except Exception as e:
                logger.error(f"❌ Failed during explore/view all process: {e}")
                driver.quit()
                # return []
            #     view_all_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ctrack_div"]/div/div[1]/div[2]/div[8]/div[2]/a')))
            #     view_all_button.click()
            #     logger.info("Clicked 'View All'")
            #     time.sleep(6)
            #     # WebDriverWait(driver, 10).until(lambda d: "#view=ended" in d.current_url)
            # except:
            #     logger.info("'View All' button not found or already applied")

        csv_files = []
        project_index = 2
        while True:
            try:
                # driver.get(main_url)
                # time.sleep(5)

                row_xpath = f'//*[@id="ctrack_div"]/div/div[1]/div[2]/div[5]/div[1]/table/tbody/tr[{project_index}]'
                row = wait.until(EC.presence_of_element_located((By.XPATH, row_xpath)))
                driver.execute_script("arguments[0].scrollIntoView(true);", row)
                time.sleep(1)

                try:
                    wait.until(EC.element_to_be_clickable((By.XPATH, row_xpath)))
                    row.click()
                    logger.info(f"Clicked row {project_index}")
                except Exception as click_error:
                    logger.info(f"Element not clickable at index {project_index}, retrying with JS: {click_error}")
                    try:
                        driver.execute_script("arguments[0].click();", row)
                        logger.info(f"Clicked row {project_index} using JavaScript")
                    except Exception as js_error:
                        logger.info(f"JavaScript click also failed at index {project_index}: {js_error}")
                        project_index += 1
                        continue

                time.sleep(4)

                try:
                    csv_xpath = '//*[@id="ctrack_div"]/div/div[1]/div[2]/div[4]/div/div[5]/div[2]/a[6]'
                    csv_link = wait.until(EC.presence_of_element_located((By.XPATH, csv_xpath)))
                    driver.execute_script("arguments[0].scrollIntoView(true);", csv_link)
                    

                    href = csv_link.get_attribute("href")
                    logger.info(f"🔗 CSV link: {href}")

                    # Extract original filename and clean up link-based name
                    original_filename = os.path.basename(urlparse(href).path)

                    # Use a hash or encoded form of the full URL to name the file uniquely
                    safe_name = quote_plus(href)  # URL-safe filename version of the href
                    filename = f"{safe_name}.csv"

                    local_path = os.path.join(download_dir, filename)

                    try:
                        csv_link.click()
                    except Exception as e:
                        driver.execute_script("arguments[0].click();", csv_link)
                        logger.info(f"Clicked CSV with JS fallback for row {project_index}")

                    # Wait for actual download
                    downloaded_file = os.path.join(download_dir, original_filename)
                    wait_time = 0
                    while not os.path.exists(downloaded_file) and wait_time < 30:
                        time.sleep(1)
                        wait_time += 1

                    if os.path.exists(downloaded_file):
                        os.rename(downloaded_file, local_path)
                        csv_files.append(local_path)
                        logger.info(f"✅ File downloaded and renamed to: {local_path}")
                    else:
                        logger.error(f"❌ File not found after click: {downloaded_file}")
                        # Optional: clean up any partials
                        partial = local_path + ".crdownload"
                        if os.path.exists(partial):
                            os.remove(partial)
                            logger.info(f"🧹 Removed incomplete download: {partial}")
                    # break
                except Exception as download_error:
                    logger.info(f"Failed to download CSV at row {project_index}: {download_error}")

                project_index += 1

            except Exception as loop_error:
                logger.info(f"Finished or no more rows at index {project_index}: {loop_error}")
                break

        logger.info(f"All CSVs downloaded to: {download_dir}")
        # return csv_files

    finally:
        driver.quit()
