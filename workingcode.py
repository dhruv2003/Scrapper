import os
import time
import math
import json
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.common.exceptions import WebDriverException
from app.services.utils import log

def load_credentials():
    with open("credentials.json") as f:
        return json.load(f)

def load_credentials():
    with open("credentials1.json") as f:
        return json.load(f)
    
def load_credentials():
    with open("credentials2.json") as f:
        return json.load(f)

def start_scraper(cred):
    try:
        email = cred["email"]
        password = cred["password"]

        log(f"🚀 Starting Chrome for user {email}...")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        driver.maximize_window()
        driver.implicitly_wait(10)

        driver.get('https://eprplastic.cpcb.gov.in/#/plastic/home')
        time.sleep(2)

        user_field = driver.find_element(By.XPATH, '//*[@id="user_name"]')
        pass_field = driver.find_element(By.XPATH, '//*[@id="password_pass"]')

        user_field.send_keys(email)
        pass_field.send_keys(password)

        log("✅ Email & Password filled! Please complete Captcha + OTP manually...")

        WebDriverWait(driver, 600).until(
            EC.presence_of_element_located((By.XPATH, '//span[@class="account-name"]'))
        )
        log("✅ Login successful!")

        scrape_all(driver, email)

    except Exception as e:
        log(f"❌ Error during login: {e}")

def scrape_all(driver, email):
    time.sleep(2)
    driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-dashboard-view")
    time.sleep(2)

    entity_type = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//p[text()="User Type"]/following::span[1]'))
    ).text.strip()

    entity_name = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//p[text()="Company Name"]/following::span[1]'))
    ).text.strip()

    log(f"🔎 Entity: {entity_name} | Type: {entity_type}")

    df_procurement = scrape_procurement(driver, entity_type, entity_name, email)
    df_sales = scrape_sales(driver, entity_type, entity_name, email)
    df_wallet = scrape_wallet(driver, entity_type, entity_name, email)
    df_target = scrape_target(driver, entity_type, entity_name, email)
    df_annual, df_compliance, df_next_target = scrape_annual(driver, entity_type, entity_name, email)

    filename = save_excel(df_procurement, df_sales, df_wallet, df_target, df_annual, df_compliance, df_next_target)

    try:
        logout(driver)
    except:
        pass

    driver.quit()

    return filename

def save_excel(df_procurement, df_sales, df_wallet, df_target, df_annual, df_compliance, df_next_target):
    now = datetime.now()
    os.makedirs("scraped_data", exist_ok=True)
    filename = f"scraped_data/PWM_Scraped_{now.strftime('%d%m%Y_%H%M%S')}.xlsx"

    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df_sales.to_excel(writer, sheet_name='Sales', index=False)
        df_procurement.to_excel(writer, sheet_name='Procurement', index=False)
        df_wallet.to_excel(writer, sheet_name='Wallet', index=False)
        df_target.to_excel(writer, sheet_name='Target', index=False)
        df_annual.to_excel(writer, sheet_name='Annual', index=False)
        df_compliance.to_excel(writer, sheet_name='Compliance', index=False)
        df_next_target.to_excel(writer, sheet_name='Next_Target', index=False)

    log(f"✅ Excel Saved: {filename}")
    return filename

def scrape_procurement(driver, entity_type, entity_name, email):
    log("📦 Fetching Procurement Data...")
    try:
        driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-operations/material")
        time.sleep(3)

        start_date = driver.find_element(By.ID, "date_from")
        end_date = driver.find_element(By.ID, "date_to")

        start_date.clear()
        start_date.send_keys("01/04/2020")
        today = datetime.now().strftime("%d/%m/%Y")
        end_date.clear()
        end_date.send_keys(today)

        driver.find_element(By.XPATH, '//button[contains(text(),"Fetch")]').click()
        time.sleep(5)

        df = scrape_table(driver)

        if df.empty:
            return df

        df["Type_of_entity"] = entity_type
        df["Entity_Name"] = entity_name
        df["Email"] = email

        return df

    except Exception as e:
        log(f"❌ Procurement Error: {e}")
        return pd.DataFrame()

def scrape_sales(driver, entity_type, entity_name, email):
    log("💰 Fetching Sales Data...")
    try:
        driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-operations/sales")
        time.sleep(3)

        start_year = 2020
        end_year = datetime.now().year if datetime.now().month >= 4 else datetime.now().year - 1

        all_data = pd.DataFrame()

        for year in range(start_year, end_year + 1):
            start_date = driver.find_element(By.ID, "date_from")
            end_date = driver.find_element(By.ID, "date_to")

            start_date.clear()
            start_date.send_keys(f"01/04/{year}")
            end_date.clear()
            end_date.send_keys(f"31/03/{year+1}")

            driver.find_element(By.XPATH, '//button[contains(text(),"Fetch")]').click()
            time.sleep(5)

            df = scrape_table(driver)

            if not df.empty:
                df["Type_of_entity"] = entity_type
                df["Entity_Name"] = entity_name
                df["Email"] = email
                all_data = pd.concat([all_data, df], ignore_index=True)

        return all_data

    except Exception as e:
        log(f"❌ Sales Error: {e}")
        return pd.DataFrame()

def scrape_wallet(driver, entity_type, entity_name, email):
    log("🪪 Fetching Wallet Data...")
    try:
        driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-wallet")
        time.sleep(5)

        df = scrape_table(driver)

        if df.empty:
            return df

        df["Type_of_entity"] = entity_type
        df["Entity_Name"] = entity_name
        df["Email"] = email

        return df

    except Exception as e:
        log(f"❌ Wallet Error: {e}")
        return pd.DataFrame()

def scrape_target(driver, entity_type, entity_name, email):
    log("🎯 Fetching Target Data...")
    try:
        driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-dashboard-view")
        time.sleep(5)

        df = scrape_table(driver)

        if df.empty:
            return df

        df["Type_of_entity"] = entity_type
        df["Entity_Name"] = entity_name
        df["Email"] = email

        return df

    except Exception as e:
        log(f"❌ Target Error: {e}")
        return pd.DataFrame()

def scrape_annual(driver, entity_type, entity_name, email):
    log("📝 Fetching Annual Data...")
    try:
        driver.get("https://eprplastic.cpcb.gov.in/#/epr/annual-report-filing")
        time.sleep(5)

        df_annual = scrape_table(driver)
        df_compliance = scrape_table(driver)
        df_next_target = scrape_table(driver)

        return df_annual, df_compliance, df_next_target

    except Exception as e:
        log(f"❌ Annual Error: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def logout(driver):
    log("👋 Logging out...")
    driver.find_element(By.ID, "user_profile").click()
    WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((By.XPATH, '//a[contains(text(),"Log")]'))
    ).click()

def scrape_table(driver):
    try:
        page = driver.find_element(By.ID, 'ScrollableSimpleTableBody')
        soup = BeautifulSoup(page.get_attribute('innerHTML'), 'html.parser')
        rows = soup.find_all("tr")
        data = []
        for row in rows:
            cols = row.find_all("td")
            data.append([ele.text.strip() for ele in cols])
        return pd.DataFrame(data)
    except:
        return pd.DataFrame()
