import os
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

start_date = "2024-10-01"
today_date = datetime.now().strftime("%Y-%m-%d")

url_to_button_xpath = {
        f"https://manage.arboxapp.com/reports-v5/active-members-report?created_at={start_date}%2C{today_date}": '//*[@id="native-base-main-view"]/div/div/div[1]/div[2]/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/div[3]/div[2]/div/button',
        f"https://manage.arboxapp.com/reports-v5/trial-classes-report?date={start_date}%2C{today_date}" : '//*[@id="native-base-main-view"]/div/div/div[1]/div[2]/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/div[3]/div/div/button',
        f"https://manage.arboxapp.com/reports-v5/all-leads-report?created_at={start_date}%2C{today_date}": '//*[@id="native-base-main-view"]/div/div/div[1]/div[2]/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/div[3]/div/div/button',
        f"https://manage.arboxapp.com/reports-v5/active-memberships-report?created_at_user_box={start_date}%2C{today_date}": '//*[@id="native-base-main-view"]/div/div/div[1]/div[2]/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/div[3]/div/div/button',
        f"https://manage.arboxapp.com/reports-v5/converted-leads-report?last_modified={start_date}%2C{today_date}": '//*[@id="native-base-main-view"]/div/div/div[1]/div[2]/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/div[3]/div/div/button',
        f"https://manage.arboxapp.com/reports-v5/inactive-members-report?unactiveFrom={start_date}%2C{today_date}": '//*[@id="native-base-main-view"]/div/div/div[1]/div[2]/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/div[3]/div/div/button',
        f"https://manage.arboxapp.com/reports-v5/lost-leads-report?updated_at={start_date}%2C{today_date}": '//*[@id="native-base-main-view"]/div/div/div[1]/div[2]/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/div[3]/div/div/button'
}


urls = [url.format(start_date, today_date) for url in url_to_button_xpath.keys()]

def setup_driver():
    chrome_options = Options()
    current_directory = os.path.dirname(os.path.realpath(__file__))
    download_directory = os.path.join(current_directory, "data")
    
    # Ensure the download directory exists
    if not os.path.exists(download_directory):
        os.makedirs(download_directory)

    # Specify the download directory, disable popup for downloads
    prefs = {
        "download.default_directory": download_directory,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def clear_data_directory(directory):
    """Remove all files in the specified directory."""
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')


def download_report(driver, url):
    button_xpath = url_to_button_xpath[url]
    driver.get(url)
    time.sleep(1)
    driver.refresh()
    actions_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, button_xpath))
    )
        
    actions_button.click()

    # Wait for the dropdown and the download button to appear
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="csv"]')))
    download_button = driver.find_element(By.XPATH, '//*[@id="csv"]')

    if download_button:
        download_button.click()
        print("File is downloading...")
    else:
        print("Download button not found.")


def login(driver):
    data_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")
    clear_data_directory(data_directory)
    driver.get("https://manage.arboxapp.com/login")
    
    # Use WebDriverWait to ensure the page has loaded
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="native-base-main-view"]/div[2]/div[8]/div[1]/input')))

    username = driver.find_element(By.XPATH, '//*[@id="native-base-main-view"]/div[2]/div[8]/div[1]/input')
    password = driver.find_element(By.XPATH, '//*[@id="native-base-main-view"]/div[2]/div[8]/div[3]/span/input')

    username.send_keys(os.getenv('EMAIL'))
    password.send_keys(os.getenv('PASSWORD'))
    password.send_keys(Keys.RETURN)

    time.sleep(5)

def login_and_download():
    driver = setup_driver()
    login(driver)

    for url in urls:
        download_report(driver, url)
        time.sleep(5)

    driver.quit()

if __name__ == "__main__":
    login_and_download()
    

    