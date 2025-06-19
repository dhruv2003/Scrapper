# app/services/pwmr_scraper.py

import os
import time
import json
import re
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException, TimeoutException, StaleElementReferenceException
from lxml import html

from app.services.utils import log
from app.services.mongo_utils import save_to_mongodb  # Import the MongoDB utility

# --- SQLAlchemy imports ---
from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.models.pwmr import PwmrJob, NextTarget


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def save_next_targets(db: Session, job_id: int, df_next: pd.DataFrame):
    """
    Save rows of the Next Target DataFrame into next_target table.
    """
    try:
        for _, row in df_next.iterrows():
            try:
                next_year = int(row.get("Next Year", 0) or 0)
                projected_amount = float(row.get("Projected Amount", 0.0) or 0.0)
                
                # Also try to get target from new format
                if "Target" in row and not projected_amount:
                    projected_amount = float(row.get("Target", 0.0) or 0.0)
            except (ValueError, TypeError):
                next_year = 0
                projected_amount = 0.0
                
            nt = NextTarget(
                job_id=job_id,
                next_year=next_year,
                projected_amount=projected_amount,
                Type_of_entity=row.get("Type_of_entity", ""),
                Entity_Name=row.get("Entity_Name", ""),
                Email=row.get("Email", ""),
            )
            db.add(nt)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        log(f"❌ Error saving next targets: {e}")
        return False


def load_credentials():
    creds = {}
    for filename in ["credentials.json", "credentials1.json", "credentials2.json"]:
        if os.path.exists(filename):
            with open(filename) as f:
                creds.update(json.load(f))
    return creds


def convert_cat(text):
    """Convert category text to standardized format (from latest code)"""
    if not text:
        return text
    roman = ['I', 'II', 'III', 'IV', 'V']
    text = re.sub(r'\b([1-5])\b', lambda m: roman[int(m.group(1))-1], text)
    text = text.replace('-', ' ').replace('CAT', 'Cat').replace('cat', 'Cat').strip()
    return text


def custom_wait_clickable_and_click(driver, element, attempts=20):
    """Attempt to click an element multiple times with waits"""
    count = 0
    success = False
    while count < attempts and not success:
        try:
            element.click()
            success = True
        except:
            time.sleep(1)
            count += 1


def save_excel(data_dict, email=None, entity_name=None):
    """
    Save the scraped DataFrames to an Excel file
    """
    try:
        now = datetime.now()
        os.makedirs("scraped_data", exist_ok=True)
        
        # Create a filename with entity name if available
        if entity_name:
            safe_entity_name = "".join(c if c.isalnum() else "_" for c in entity_name)
            filename = f"scraped_data/PWM_{safe_entity_name}_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
        else:
            email_prefix = email.split('@')[0] if email else "unknown"
            filename = f"scraped_data/PWM_{email_prefix}_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Write each DataFrame to a different sheet
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    df.to_excel(writer, sheet_name=sheet_name.capitalize(), index=False)
        
        log(f"📊 Excel saved: {filename}")
        return filename
    except Exception as e:
        log(f"❌ Error saving Excel: {e}")
        return None


def logout(driver):
    """Logs out of the CPCB portal with retry for stale elements"""
    log("👋 Logging out...")
    max_attempts = 3
    
    for attempt in range(max_attempts):
        try:
            # Find and click profile button
            profile_btn = driver.find_element(By.ID, "user_profile")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", profile_btn)
            profile_btn.click()
            
            # Wait for logout link and click it
            logout_link = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//a[contains(text(),"Log")]'))
            )
            logout_link.click()
            
            log("✅ Logged out successfully")
            return True
        except StaleElementReferenceException:
            log(f"⚠️ Stale element reference on attempt {attempt+1}/{max_attempts}, retrying...")
            time.sleep(1)
        except Exception as e:
            log(f"⚠️ Error during logout attempt {attempt+1}/{max_attempts}: {e}")
            if attempt == max_attempts - 1:
                log("❌ Failed to log out cleanly after all attempts")
                return False
            time.sleep(1)
    
    return False

def start_scraper(cred: dict) -> dict:
    """
    1) Performs login + manual captcha/OTP wait.
    2) Scrapes each section into a pandas.DataFrame.
    3) Persists the `next_target` section into database.
    4) Saves all DataFrames to MongoDB.
    5) Saves all DataFrames to Excel.
    6) Returns dict of all DataFrames plus `job_id`.
    """
    results = {}
    db_gen = get_db()
    db: Session = next(db_gen)
    job_id = None
    driver = None

    try:
        # 1️⃣ Create job record before scraping
        job = PwmrJob(
            created_at=str(datetime.now()),
            Type_of_entity=cred.get("entity_type", ""),
            Entity_Name=cred.get("entity_name", ""),
            Email=cred["email"]
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        job_id = job.id
        log(f"🆔 Created job_id={job_id}")

        # 2️⃣ Launch Selenium
        email = cred["email"]
        password = cred["password"]
        log(f"✨ Launching Chrome for {email}...")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        driver.maximize_window()
        driver.implicitly_wait(10)
        driver.get("https://eprplastic.cpcb.gov.in/#/plastic/home")

        # Login Flow
        driver.find_element(By.ID, "user_name").send_keys(email)
        driver.find_element(By.ID, "password_pass").send_keys(password)
        log("🔒 Credentials entered. Awaiting captcha/OTP...")
        WebDriverWait(driver, 600).until(
            EC.presence_of_element_located((By.XPATH, '//span[@class="account-name"]'))
        )
        log("✅ Logged in successfully.")

        # 3️⃣ Scrape
        all_sections = scrape_all(driver, email)

        # 4️⃣ Persist Next Target to SQL database
        df_next = all_sections.get("next_target", pd.DataFrame())
        if not df_next.empty:
            save_next_targets(db, job_id, df_next)
            log(f"💾 Saved {len(df_next)} next_target rows for job {job_id}")
        else:
            log("ℹ️ No next_target data to save.")

        # 5️⃣ Save all data to MongoDB
        entity_name = cred.get("entity_name") or (
            all_sections.get("procurement", pd.DataFrame()).get("Entity_Name", [None])[0]
            if not all_sections.get("procurement", pd.DataFrame()).empty
            else None
        )
        entity_type = cred.get("entity_type") or (
            all_sections.get("procurement", pd.DataFrame()).get("Type_of_entity", [None])[0]
            if not all_sections.get("procurement", pd.DataFrame()).empty 
            else None
        )
        
        # Get current year or override from credentials if provided
        year = cred.get("year", datetime.now().year)
        
        mongo_result = save_to_mongodb(
            data_dict=all_sections,
            email=email,
            company_name=entity_name,
            entity_type=entity_type,
            year=year
        )
        log(f"📦 MongoDB: {mongo_result}")
        
        # 6️⃣ Save to Excel file
        excel_file = save_excel(all_sections, email, entity_name)
        if excel_file:
            results["excel_file"] = excel_file

        # 7️⃣ Return job_id plus all DataFrames
        results = {"job_id": job_id, **all_sections}
        
        # 8️⃣ Logout at the end of scraping
        logout(driver)
        
        return results

    except Exception as e:
        log(f"❌ start_scraper error: {e}")
        if job_id:
            results["job_id"] = job_id
        raise

    finally:
        # Clean up
        if driver:
            try:
                # Try to logout if not already done
                try:
                    logout(driver)
                except Exception as e:
                    log(f"⚠️ Failed final logout attempt: {e}")
                    
                driver.quit()
            except Exception as e:
                log(f"⚠️ Failed to quit driver: {e}")

        # Close DB generator
        try:
            next(db_gen)
        except StopIteration:
            pass


def scrape_all(driver, email: str) -> dict:
    """
    Scrape all sections and return a dict of DataFrames.
    """
    # Navigate to dashboard
    driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-dashboard-view")
    time.sleep(2)

    # Extract entity info
    entity_type = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//p[text()="User Type"]/following::span[1]'))
    ).text.strip()
    entity_name = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//p[text()="Company Name"]/following::span[1]'))
    ).text.strip()

    log(f"🏷 Entity: {entity_name} | Type: {entity_type}")

    # Scrape each section
    df_proc = scrape_procurement(driver, entity_type, entity_name, email)
    df_sales = scrape_sales(driver, entity_type, entity_name, email)
    df_wallet_credit, df_wallet_debit, df_credit2, df_debit2, df_filing = scrape_wallet_data(driver, entity_type, entity_name, email)
    df_target = scrape_target(driver, entity_type, entity_name, email)
    df_annual, df_compliance, df_next = scrape_annual(driver, entity_type, entity_name, email)
    df_consumption_regn = scrape_consumption_regn(driver, entity_type, entity_name, email)
    df_consumption_ar = scrape_consumption_ar(driver, entity_type, entity_name, email)

    return {
        "procurement": df_proc,
        "sales": df_sales,
        "wallet_credit": df_wallet_credit,
        "wallet_debit": df_wallet_debit,
        "credit_transactions": df_credit2,
        "debit_transactions": df_debit2,
        "filing_transactions": df_filing,
        "target": df_target,
        "annual": df_annual,
        "compliance": df_compliance,
        "next_target": df_next,
        "consumption_regn": df_consumption_regn,
        "consumption_ar": df_consumption_ar
    }


def scrape_procurement(driver, etype, ename, email):
    log("🛒 Scraping Procurement...")
    driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-operations/material")
    time.sleep(3)
    
    try:
        # Set date range from 2020 to today
        date_input = driver.find_element(By.XPATH, "//input[@id='date_from']")
        date_input.clear()
        date_input.send_keys("01/04/2020")
        
        today_date = datetime.now().strftime("%d/%m/%Y")
        date_end_input = driver.find_element(By.XPATH, "//input[@id='date_to']")
        date_end_input.clear()
        date_end_input.send_keys(today_date)
        
        # Click fetch button
        fetch_btn = driver.find_element(By.XPATH, '//button[contains(text(),"Fetch")]')
        custom_wait_clickable_and_click(driver, fetch_btn)
        
        # Get pagination info
        pagination_info = driver.find_element(By.XPATH, '//table/tbody/tr/td/div[1]/div/span').text
        try:
            total_records = [int(i) for i in pagination_info.split() if i.isdigit()][-1]
            total_pages = (total_records + 49) // 50  # Ceiling division
        except (IndexError, ValueError):
            total_pages = 1
        
        # Collect data across all pages
        all_data = []
        for page in range(total_pages):
            time.sleep(2)
            table_data = scrape_complex_table(driver)
            if table_data:
                all_data.extend(table_data)
            
            # Click next if not on last page
            if page < total_pages - 1:
                next_buttons = driver.find_elements(By.CLASS_NAME, 'action-button')
                if len(next_buttons) > 1:
                    custom_wait_clickable_and_click(driver, next_buttons[1])
                    input_field = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, 
                            '//div[contains(@class, "paginator")]/input'))
                    )
                    custom_wait_clickable_and_click(driver, input_field)
        
        # Convert to DataFrame and process
        df = process_complex_data(all_data)
        return enrich_df(df, etype, ename, email)
        
    except Exception as e:
        log(f"❌ Error scraping procurement data: {str(e)}")
        return pd.DataFrame()


def scrape_sales(driver, etype, ename, email):
    log("💵 Scraping Sales...")
    driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-operations/sales")
    time.sleep(3)
    
    try:
        # Get current year and month to determine fiscal year end
        current_year = datetime.now().year
        current_month = datetime.now().month
        end_year = current_year if current_month >= 4 else current_year - 1
        
        all_data = []
        
        # Loop through fiscal years from 2020 to current
        for year in range(2020, end_year + 1):
            driver.refresh()
            time.sleep(2)
            
            from_date = f"01/04/{year}"
            to_date = f"31/03/{year+1}"
            
            try:
                # Set date inputs
                date_input = driver.find_element(By.XPATH, "//input[@id='date_from']")
                date_input.clear()
                date_input.send_keys(from_date)
                
                date_end_input = driver.find_element(By.XPATH, "//input[@id='date_to']")
                date_end_input.clear()
                date_end_input.send_keys(to_date)
                
                # Click fetch
                fetch_btn = driver.find_element(By.XPATH, '//button[contains(text(),"Fetch")]')
                custom_wait_clickable_and_click(driver, fetch_btn)
                
                # Process pagination
                try:
                    pagination_info = driver.find_element(By.XPATH, '//table/tbody/tr/td/div[1]/div/span').text
                    total_records = [int(i) for i in pagination_info.split() if i.isdigit()][-1]
                    total_pages = (total_records + 49) // 50  # Ceiling division
                except:
                    total_pages = 1
                    
                # Collect data across all pages for this year
                for page in range(total_pages):
                    time.sleep(2)
                    table_data = scrape_complex_table(driver)
                    if table_data:
                        all_data.extend(table_data)
                    
                    # Click next if not on last page
                    if page < total_pages - 1:
                        next_buttons = driver.find_elements(By.CLASS_NAME, 'action-button')
                        if len(next_buttons) > 1:
                            custom_wait_clickable_and_click(driver, next_buttons[1])
                            input_field = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.XPATH, 
                                    '//div[contains(@class, "paginator")]/input'))
                            )
                            custom_wait_clickable_and_click(driver, input_field)
                            
            except Exception as e:
                log(f"⚠️ Error scraping sales for year {year}: {str(e)}")
                continue
        
        # Convert to DataFrame and process
        df = process_complex_data(all_data)
        return enrich_df(df, etype, ename, email)
        
    except Exception as e:
        log(f"❌ Error scraping sales data: {str(e)}")
        return pd.DataFrame()


def scrape_complex_table(driver):
    """Scrapes a complex table with spans and returns raw data"""
    try:
        job = driver.find_element(By.ID, 'ScrollableSimpleTableBody')
        soup = BeautifulSoup(job.get_attribute('innerHTML'), 'html.parser')
        data = soup.find_all("span", class_="ng-star-inserted") or soup.find_all("td", class_="row-item")
        values = [i.text.replace("\n", "").strip() for i in data]
        
        # Process the raw data into rows
        processed_data = []
        i = 0
        while i < len(values):
            # Handle variable row formats
            if i+16 <= len(values):
                row_data = values[i:i+16]
                
                # Check if we need to skip some empty fields at the end
                if len(values) > i+18 and values[i+16] == "" and values[i+17] == "" and values[i+18] == "":
                    i += 19
                elif len(values) > i+17 and values[i+16] == "" and values[i+17] == "":
                    i += 18
                else:
                    i += 16
                    
                processed_data.append(row_data)
            else:
                # If we don't have enough data for a full row, break
                break
                
        return processed_data
    
    except Exception as e:
        log(f"⚠️ Error scraping complex table: {str(e)}")
        return []


def process_complex_data(data):
    """Process complex table data into a DataFrame with calculated fields"""
    if not data:
        return pd.DataFrame()
        
    try:
        # Define column headers
        columns = [
            'Registration Type', 'Entity Type', 'Name of the Entity', 'State', 
            'Address', 'Mobile Number', 'Plastic Material Type', 'Category of Plastic',
            'Financial Year', 'Date', 'Total Plastic Qty (Tons)', 'Recycled Plastic %',
            'GST', 'GST Paid', 'EPR invoice No', 'GST E-Invoice No'
        ]
        
        # Create DataFrame from raw data
        df = pd.DataFrame(data, columns=columns)
        
        # Add calculated columns
        df['Category'] = df['Category of Plastic'].apply(
            lambda x: "Cat I" if "Containers" in str(x) else x
        )
        
        # Calculate recycled content
        df['Recycle Consumption'] = pd.to_numeric(df['Total Plastic Qty (Tons)'], errors='coerce') * \
                                   pd.to_numeric(df['Recycled Plastic %'], errors='coerce') / 100
        
        # Replace NaN with "N/A"
        df['Recycle Consumption'] = df['Recycle Consumption'].fillna("N/A")
        
        return df
        
    except Exception as e:
        log(f"❌ Error processing complex data: {str(e)}")
        return pd.DataFrame()


def scrape_wallet_data(driver, etype, ename, email):
    log("👛 Scraping Wallet Data (Credit, Debit, Transactions)...")
    
    # Initialize empty DataFrames
    df_wallet_credit = pd.DataFrame()
    df_wallet_debit = pd.DataFrame()
    df_credit2 = pd.DataFrame()
    df_debit2 = pd.DataFrame()
    df_filing = pd.DataFrame()
    
    try:
        # 1. Scrape Credit Wallet Data
        driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-wallet")
        time.sleep(3)
        driver.refresh()
        time.sleep(2)
        
        try:
            credit_data = []
            row_num = 1
            
            # Loop through credit entries
            while True:
                try:
                    # Find and click on details button
                    detail_btn_xpath = f"//table[@id='simple-table-with-pagination']/tbody/tr[{row_num}]/td[8]/span/span/em"
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, detail_btn_xpath))
                    )
                    
                    # Get row data before clicking
                    sno = driver.find_element(By.XPATH, f'//table[@id="simple-table-with-pagination"]/tbody/tr[{row_num}]/td[1]').text
                    date = driver.find_element(By.XPATH, f'//table[@id="simple-table-with-pagination"]/tbody/tr[{row_num}]/td[2]/span').text
                    credit = driver.find_element(By.XPATH, f'//table[@id="simple-table-with-pagination"]/tbody/tr[{row_num}]/td[5]/span').text
                    
                    # Click and wait for details
                    detail_btn = driver.find_element(By.XPATH, detail_btn_xpath)
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_btn)
                    custom_wait_clickable_and_click(driver, detail_btn)
                    time.sleep(2)
                    
                    # Get certificate details using lxml (more reliable for complex modal data)
                    tree = html.fromstring(driver.page_source)
                    cert_cells = tree.xpath('//h5[text()="Transfered Certificates"]/parent::div/parent::div//table[@id="simple-table-with-pagination"]/tbody/tr//td/span[@title]')
                    
                    cert_data = []
                    for cell in cert_cells:
                        details = cell.xpath('./@title')[0].strip()
                        if details:
                            cert_data.append(details)
                    
                    # Process certificate data in groups of 13
                    i = 0
                    while i < len(cert_data):
                        if i+13 <= len(cert_data):
                            row = [sno, date, credit] + cert_data[i:i+13]
                            credit_data.append(row)
                            i += 13
                        else:
                            break
                    
                    # Close the modal
                    close_btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, '//button[@id="closeSubmitModal"]/span'))
                    )
                    custom_wait_clickable_and_click(driver, close_btn)
                    time.sleep(1)
                    
                    row_num += 1
                    
                except (TimeoutException, WebDriverException) as e:
                    break
                    
            # Convert credit data to DataFrame
            if credit_data:
                columns = [
                    'SL_No', 'Date', 'Credited_From', 'Certificate_ID', 'Value',
                    'Certificate_Owner', 'Category', 'Processing_Type', 'Transaction_ID',
                    'Available_Potential_Prior_Generation', 'Available_Potential_After_Generation',
                    'Used_Potential_Prior_Generation', 'Used_Potential_After_Generation',
                    'Cumulative_Potential', 'Generated_At', 'Validity'
                ]
                
                df_wallet_credit = pd.DataFrame(credit_data, columns=columns)
                df_wallet_credit = enrich_df(df_wallet_credit, etype, ename, email)
                
        except Exception as e:
            log(f"⚠️ Error scraping credit wallet: {str(e)}")
            
        # 2. Scrape Debit Wallet Data
        try:
            driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-wallet")
            time.sleep(3)
            driver.refresh()
            time.sleep(2)
            
            # Click on Debit Transactions tab
            debit_tab = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//a[text()="Debit Transactions"]'))
            )
            custom_wait_clickable_and_click(driver, debit_tab)
            time.sleep(2)
            
            debit_data = []
            row_num = 1
            
            # Loop through debit entries with same logic as credit
            while True:
                try:
                    # Find and click on details button
                    detail_btn_xpath = f"//table[@id='simple-table-with-pagination']/tbody/tr[{row_num}]/td[8]/span/span/em"
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, detail_btn_xpath))
                    )
                    
                    # Get row data before clicking
                    sno = driver.find_element(By.XPATH, f'//table[@id="simple-table-with-pagination"]/tbody/tr[{row_num}]/td[1]').text
                    date = driver.find_element(By.XPATH, f'//table[@id="simple-table-with-pagination"]/tbody/tr[{row_num}]/td[2]/span').text
                    debit_to = driver.find_element(By.XPATH, f'//table[@id="simple-table-with-pagination"]/tbody/tr[{row_num}]/td[5]/span').text
                    
                    # Click and wait for details
                    detail_btn = driver.find_element(By.XPATH, detail_btn_xpath)
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_btn)
                    custom_wait_clickable_and_click(driver, detail_btn)
                    time.sleep(2)
                    
                    # Get certificate details
                    tree = html.fromstring(driver.page_source)
                    cert_cells = tree.xpath('//h5[text()="Transfered Certificates"]/parent::div/parent::div//table[@id="simple-table-with-pagination"]/tbody/tr//td/span[@title]')
                    
                    cert_data = []
                    for cell in cert_cells:
                        details = cell.xpath('./@title')[0].strip()
                        if details:
                            cert_data.append(details)
                    
                    # Process certificate data in groups of 13
                    i = 0
                    while i < len(cert_data):
                        if i+13 <= len(cert_data):
                            row = [sno, date, debit_to] + cert_data[i:i+13]
                            debit_data.append(row)
                            i += 13
                        else:
                            break
                    
                    # Close the modal
                    close_btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, '//button[@id="closeSubmitModal"]/span'))
                    )
                    custom_wait_clickable_and_click(driver, close_btn)
                    time.sleep(1)
                    
                    row_num += 1
                    
                except (TimeoutException, WebDriverException) as e:
                    break
                    
            # Convert debit data to DataFrame
            if debit_data:
                columns = [
                    'SL_No', 'Date', 'Transfer To (PIBO)', 'Certificate_ID', 'Value',
                    'Certificate_Owner', 'Category', 'Processing_Type', 'Transaction_ID',
                    'Available_Potential_Prior_Generation', 'Available_Potential_After_Generation',
                    'Used_Potential_Prior_Generation', 'Used_Potential_After_Generation',
                    'Cumulative_Potential', 'Generated_At', 'Validity'
                ]
                
                df_wallet_debit = pd.DataFrame(debit_data, columns=columns)
                df_wallet_debit = enrich_df(df_wallet_debit, etype, ename, email)
                
        except Exception as e:
            log(f"⚠️ Error scraping debit wallet: {str(e)}")
         
        # 3. Scrape Credit Transactions summary
        try:
            driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-wallet")
            time.sleep(3)
            driver.refresh()
            time.sleep(2)
            
            # Wait for table to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//tbody[@id="ScrollableSimpleTableBody"]/tr'))
            )
            time.sleep(1)
            
            # Scrape table
            tree = html.fromstring(driver.page_source)
            rows = tree.xpath('//tbody[@id="ScrollableSimpleTableBody"]/tr')
            
            credit_summary_data = []
            for row in rows:
                cells = row.xpath('./td')
                if len(cells) >= 7:
                    # Extract category and processing type from combined field
                    category_text = cells[3].text_content().strip()
                    cat_parts = category_text.split()
                    category = convert_cat(cat_parts[0].strip()) if cat_parts else ""
                    proc_type = ' '.join(cat_parts[1:]) if len(cat_parts) > 1 else ""
                    
                    row_data = [
                        cells[0].text_content().strip(),  # Sr.No
                        cells[1].text_content().strip(),  # Date
                        cells[2].text_content().strip(),  # Transaction ID
                        category,                         # Category
                        proc_type,                        # Processing Type
                        cells[4].text_content().strip(),  # Credited From
                        cells[5].text_content().strip(),  # Status
                        cells[6].text_content().strip(),  # Amount
                    ]
                    credit_summary_data.append(row_data)
            
            if credit_summary_data:
                columns = [
                    'Sr.No', 'Date', 'Transaction ID', 'Category', 'Processing Type',
                    'Credited From', 'Status', 'Amount'
                ]
                df_credit2 = pd.DataFrame(credit_summary_data, columns=columns)
                df_credit2 = enrich_df(df_credit2, etype, ename, email)
                
        except Exception as e:
            log(f"⚠️ Error scraping credit transactions: {str(e)}")

        # 4. Scrape Debit Transactions summary
        try:
            driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-wallet")
            time.sleep(3)
            driver.refresh()
            time.sleep(2)
            
            # Click on Debit Transactions tab
            debit_tab = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//a[text()="Debit Transactions"]'))
            )
            custom_wait_clickable_and_click(driver, debit_tab)
            time.sleep(2)
            
            # Wait for table to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//tbody[@id="ScrollableSimpleTableBody"]/tr'))
            )
            time.sleep(1)
            
            # Scrape table
            tree = html.fromstring(driver.page_source)
            rows = tree.xpath('//tbody[@id="ScrollableSimpleTableBody"]/tr')
            
            debit_summary_data = []
            for row in rows:
                cells = row.xpath('./td')
                if len(cells) >= 7:
                    # Extract category and processing type from combined field
                    category_text = cells[3].text_content().strip()
                    cat_parts = category_text.split()
                    category = convert_cat(cat_parts[0].strip()) if cat_parts else ""
                    proc_type = ' '.join(cat_parts[1:]) if len(cat_parts) > 1 else ""
                    
                    row_data = [
                        cells[0].text_content().strip(),  # Sr.No
                        cells[1].text_content().strip(),  # Date
                        cells[2].text_content().strip(),  # Transaction ID
                        category,                         # Category
                        proc_type,                        # Processing Type
                        cells[4].text_content().strip(),  # Transfer To (PIBO)
                        cells[5].text_content().strip(),  # Status
                        cells[6].text_content().strip(),  # Amount
                    ]
                    debit_summary_data.append(row_data)
            
            if debit_summary_data:
                columns = [
                    'Sr.No', 'Date', 'Transaction ID', 'Category', 'Processing Type',
                    'Transfer To (PIBO)', 'Status', 'Amount'
                ]
                df_debit2 = pd.DataFrame(debit_summary_data, columns=columns)
                df_debit2 = enrich_df(df_debit2, etype, ename, email)
                
        except Exception as e:
            log(f"⚠️ Error scraping debit transactions: {str(e)}")

        # 5. Scrape Filing Transactions
        try:
            driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-wallet")
            time.sleep(3)
            driver.refresh()
            time.sleep(2)
            
            # Click on Filing Transactions tab
            filing_tab = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//a[text()="Filing Transactions"]'))
            )
            custom_wait_clickable_and_click(driver, filing_tab)
            time.sleep(2)
            
            # Wait for table to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//tbody[@id="ScrollableSimpleTableBody"]/tr'))
            )
            time.sleep(1)
            
            # Scrape table
            tree = html.fromstring(driver.page_source)
            rows = tree.xpath('//tbody[@id="ScrollableSimpleTableBody"]/tr')
            
            filing_data = []
            for row in rows:
                cells = row.xpath('./td')
                if len(cells) >= 7:
                    # Extract category and processing type from combined field
                    category_text = cells[3].text_content().strip()
                    cat_parts = category_text.split()
                    category = convert_cat(cat_parts[0].strip()) if cat_parts else ""
                    proc_type = ' '.join(cat_parts[1:]) if len(cat_parts) > 1 else ""
                    
                    row_data = [
                        cells[0].text_content().strip(),  # Sr.No
                        cells[1].text_content().strip(),  # Date
                        cells[2].text_content().strip(),  # Transaction ID
                        category,                         # Category
                        proc_type,                        # Processing Type
                        cells[4].text_content().strip(),  # Operation Type
                        cells[5].text_content().strip(),  # Amount
                        cells[6].text_content().strip(),  # Number of Certificates
                    ]
                    filing_data.append(row_data)
            
            if filing_data:
                columns = [
                    'Sr.No', 'Date', 'Transaction ID', 'Category', 'Processing Type',
                    'Operation Type', 'Amount', 'Number of Certificates'
                ]
                df_filing = pd.DataFrame(filing_data, columns=columns)
                df_filing = enrich_df(df_filing, etype, ename, email)
                
        except Exception as e:
            log(f"⚠️ Error scraping filing transactions: {str(e)}")
        
    except Exception as e:
        log(f"❌ Error scraping wallet data: {str(e)}")
    
    return df_wallet_credit, df_wallet_debit, df_credit2, df_debit2, df_filing


def scrape_target(driver, etype, ename, email):
    log("🎯 Scraping Target...")
    try:
        driver.get('https://eprplastic.cpcb.gov.in/#/epr/pibo-dashboard-view')
        time.sleep(5)
        
        target_data = []
        year_count = 1
        
        while True:
            try:
                # Open financial year dropdown
                dropdown = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, '//span[@title="Clear all"]/following::span[1]'))
                )
                custom_wait_clickable_and_click(driver, dropdown)
                
                # Get all dropdown options
                section_links = WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//div[@role="option"]'))
                )
                
                # If we've gone through all years, break
                if year_count > len(section_links):
                    break
                
                # Get current year text and click it
                financial_year = section_links[year_count-1].text.strip()
                section_links[year_count-1].click()
                time.sleep(2)
                
                # Scrape target table rows
                rows = driver.find_elements(By.XPATH, '//tbody[@id="ScrollableSimpleTableBody"]/tr[position()>1]')
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if len(cells) >= 5:
                        row_data = [
                            convert_cat(cells[0].text),  # Category
                            cells[1].text,               # Min_Recycling_Target
                            cells[2].text,               # Max_EOL_Target
                            cells[3].text,               # Min_Of_Recycling_Material
                            cells[4].text,               # Min_Reuse_Target
                            financial_year               # Financial_Year
                        ]
                        target_data.append(row_data)
                
                # Increment for next year
                year_count += 1
                
            except Exception as e:
                log(f"⚠️ Error scraping target data for year {year_count}: {str(e)}")
                break
        
        # Convert to DataFrame
        if target_data:
            df = pd.DataFrame(target_data, columns=[
                'Category', 'Min_Recycling_Target', 'Max_EOL_Target', 
                'Min_Of_Recycling_Material', 'Min_Reuse_Target', 'Financial_Year'
            ])
            return enrich_df(df, etype, ename, email)
        
        return pd.DataFrame()
        
    except Exception as e:
        log(f"❌ Error scraping target data: {str(e)}")
        return pd.DataFrame()


def scrape_annual(driver, etype, ename, email):
    log("📈 Scraping Annual, Compliance & Next Target...")
    try:
        driver.get('https://eprplastic.cpcb.gov.in/#/epr/annual-report-filing')
        time.sleep(5)
        driver.refresh()
        time.sleep(2)
        
        # 1. Annual Report
        annual_data = []
        try:
            rows = WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.XPATH, '//div[contains(text(),"Annual Report (")]/following::div[1]//tbody[@id="ScrollableSimpleTableBody"]/tr[position()>0]'))
            )
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) >= 7:
                    row_data = [
                        convert_cat(cells[1].text),  # Category
                        cells[2].text,               # Procurement_Tons
                        cells[3].text,               # Sales_Tons
                        cells[4].text,               # Export_Tons
                        cells[5].text,               # Reuse_Tons
                        cells[6].text                # UREP_Tons
                    ]
                    annual_data.append(row_data)
            
            annual_df = pd.DataFrame(annual_data, columns=[
                'Category', 'Procurement_Tons', 'Sales_Tons', 
                'Export_Tons', 'Reuse_Tons', 'UREP_Tons'
            ])
            annual_df = enrich_df(annual_df, etype, ename, email)
            
        except Exception as e:
            log(f"⚠️ Error scraping annual data: {str(e)}")
            annual_df = pd.DataFrame()
            
        # 2. Compliance Status
        compliance_data = []
        try:
            # Try both possible XPath structures
            try:
                rows = WebDriverWait(driver, 3).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//div[contains(text(),"Fulfilment of EPR Targets")]/following::div[1]//table[@id="simple-table-with-pagination"]/tbody/tr[position()>0]'))
                )
            except:
                rows = WebDriverWait(driver, 3).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//div[contains(text(),"Fulfilment of EPR Targets")]/following::div[1]//tbody/tr[position()>0]'))
                )
                
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) >= 6:
                    # Split category into Category and Rec_Eol
                    cat_text = cells[1].text.strip()
                    if "-" in cat_text:
                        cat_parts = cat_text.split("-", 1)
                        category = convert_cat(cat_parts[0].strip())
                        rec_eol = cat_parts[1].strip()
                    else:
                        category = convert_cat(cat_text)
                        rec_eol = ""
                    
                    row_data = [
                        category,     # Category
                        rec_eol,      # Rec_Eol
                        cells[2].text,  # Target
                        cells[3].text,  # Achieved
                        cells[4].text,  # Available_Potential
                        cells[5].text   # Remarks
                    ]
                    compliance_data.append(row_data)
            
            compliance_df = pd.DataFrame(compliance_data, columns=[
                'Category', 'Rec_Eol', 'Target', 'Achieved', 
                'Available_Potential', 'Remarks'
            ])
            compliance_df = enrich_df(compliance_df, etype, ename, email)
            
        except Exception as e:
            log(f"⚠️ Error scraping compliance data: {str(e)}")
            compliance_df = pd.DataFrame()
            
        # 3. Next Year Target
        next_target_data = []
        try:
            rows = driver.find_elements(By.XPATH, '//div[contains(text(),"Next year Targets (")]/following::div[1]//tbody[@id="ScrollableSimpleTableBody"]/tr[position()>0]')
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) >= 3:
                    # Split category into Category and Rec_Eol
                    cat_text = cells[1].text.strip()
                    if "-" in cat_text:
                        cat_parts = cat_text.split("-", 1)
                        category = convert_cat(cat_parts[0].strip())
                        rec_eol = cat_parts[1].replace("Plastic", "").strip()
                    else:
                        category = convert_cat(cat_text)
                        rec_eol = ""
                    
                    row_data = [
                        category,     # Category
                        rec_eol,      # Rec_Eol
                        cells[2].text,  # Target (Projected Amount)
                    ]
                    next_target_data.append(row_data)
            
            # Extract year from section header
            try:
                next_year_header = driver.find_element(By.XPATH, '//div[contains(text(),"Next year Targets (")]').text
                next_year_match = re.search(r'\((\d{4})-\d{2}', next_year_header)
                next_year = int(next_year_match.group(1)) if next_year_match else datetime.now().year + 1
            except:
                next_year = datetime.now().year + 1
                
            next_target_df = pd.DataFrame(next_target_data, columns=[
                'Category', 'Rec_Eol', 'Target'
            ])
            # Add the next year column 
            next_target_df['Next Year'] = next_year
            next_target_df['Projected Amount'] = next_target_df['Target']
            next_target_df = enrich_df(next_target_df, etype, ename, email)
            
        except Exception as e:
            log(f"⚠️ Error scraping next target data: {str(e)}")
            next_target_df = pd.DataFrame()
        
        return annual_df, compliance_df, next_target_df
        
    except Exception as e:
        log(f"❌ Error scraping annual, compliance, next target data: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def scrape_consumption_regn(driver, etype, ename, email):
    log("📊 Scraping Consumption Registration Data...")
    try:
        driver.get('https://eprplastic.cpcb.gov.in/#/epr/producer-list')
        time.sleep(5)
        
        # Click on view details
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//em[@class="fa fa-eye"]'))
        ).click()
        
        # Click on comments tab
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '//a[@id="product-comments-tab"]'))
        ).click()
        
        # Wait for table to load
        time.sleep(3)
        
        # Get rows using lxml for more reliable extraction
        tree = html.fromstring(driver.page_source)
        rows = tree.xpath('//div[contains(text(),"Pertaining to Waste")]/following::div[1]//tbody/tr[position()>0]')
        
        # Process rows
        consumption_data = []
        sl_no = ""
        state = ""
        year = ""
        
        for row in rows:
            cells = [cell.text_content().strip() for cell in row.xpath('./td')]
            
            if len(cells) == 2:  # Header row with sl_no and state
                sl_no = cells[0] or sl_no
                state = cells[1] or state
            elif len(cells) == 1:  # Year row
                year = cells[0] or year
            elif len(cells) == 5:  # Data row
                # Extract category and material from first cell
                try:
                    cat_material = cells[0].split("(")
                    material = cat_material[0].strip()
                    category = convert_cat(cat_material[1].replace(")", "").strip())
                except:
                    material = cells[0]
                    category = ""
                
                pre_qty = cells[1].strip()
                pre_recycled = cells[2].strip()
                
                # Calculate pre recycle consumption
                try:
                    pre_recycle_consumption = float(pre_qty) * float(pre_recycled) / 100
                except (ValueError, TypeError):
                    pre_recycle_consumption = 0
                    
                post_qty = cells[3].strip()
                post_recycled = cells[4].strip()
                
                # Calculate post recycle consumption
                try:
                    post_recycle_consumption = float(post_qty) * float(post_recycled) / 100
                except (ValueError, TypeError):
                    post_recycle_consumption = 0
                    
                # Calculate total consumption
                try:
                    total_consumption = float(pre_qty) + float(post_qty)
                except:
                    total_consumption = 0
                    
                consumption_data.append([
                    sl_no, state, year, category, material, 
                    pre_qty, pre_recycled, pre_recycle_consumption,
                    post_qty, post_recycled, post_recycle_consumption, 
                    total_consumption
                ])
                
            elif cells and "Total" in cells[0]:  # Total row
                category = cells[0].strip()
                pre_qty = cells[1] if len(cells) > 1 else "0"
                pre_recycled = cells[2] if len(cells) > 2 else "0"
                
                # Calculate pre recycle consumption
                try:
                    pre_recycle_consumption = float(pre_qty) * float(pre_recycled) / 100
                except (ValueError, TypeError):
                    pre_recycle_consumption = 0
                
                # Handle different length rows
                post_qty = cells[3] if len(cells) > 3 else "0"
                post_recycled = cells[4] if len(cells) > 4 else "0"
                
                # Calculate post recycle consumption
                if post_qty and post_recycled:
                    try:
                        post_recycle_consumption = float(post_qty) * float(post_recycled) / 100
                    except (ValueError, TypeError):
                        post_recycle_consumption = 0
                else:
                    post_recycle_consumption = 0
                    
                # Calculate total consumption
                try:
                    total_consumption = float(pre_qty) + float(post_qty)
                except:
                    total_consumption = 0
                    
                consumption_data.append([
                    category, "", "", "", "",
                    pre_qty, pre_recycled, pre_recycle_consumption,
                    post_qty, post_recycled, post_recycle_consumption,
                    total_consumption
                ])
        
        # Create DataFrame
        columns = [
            "Sl. No.", "State Name", "Year", "Category of Plastic", "Material type",
            "Pre Consumer Waste Plastic Quantity (TPA)", "Pre Consumer Waste Recycled Plastic %",
            "Pre Consumer Waste Recycle Consumption", "Post Consumer Waste Plastic Quantity (TPA)",
            "Post Consumer Waste Recycled Plastic %", "Post Consumer Waste Recycle Consumption",
            "Total Consumption"
        ]
        
        df = pd.DataFrame(consumption_data, columns=columns)
        return enrich_df(df, etype, ename, email)
        
    except Exception as e:
        log(f"❌ Error scraping consumption registration data: {str(e)}")
        return pd.DataFrame()


def scrape_consumption_ar(driver, etype, ename, email):
    log("📊 Scraping Consumption Annual Report Data...")
    try:
        driver.get('https://eprplastic.cpcb.gov.in/#/epr/filing/state-wise-plastic-waste')
        time.sleep(5)
        
        consumption_data = []
        year_count = 1
        
        while True:
            try:
                # Open financial year dropdown
                dropdown = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@name="select_fin_year"]/div/span'))
                )
                custom_wait_clickable_and_click(driver, dropdown)
                
                # Get all year options
                section_links = WebDriverWait(driver, 2).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//div[@role="option"]'))
                )
                
                # If we've gone through all years, break
                if year_count > len(section_links):
                    break
                
                # Select current year
                financial_year = section_links[year_count-1].text.strip()
                section_links[year_count-1].click()
                time.sleep(2)
                
                # Wait for table to load
                rows = WebDriverWait(driver, 5).until(
                    EC.presence_of_all_elements_located((By.XPATH, 
                        '//table[@class="table table-bordered scrollable-table pw-generated"]/tbody/tr'))
                )
                time.sleep(2)
                
                # Get data using lxml for reliability
                tree = html.fromstring(driver.page_source)
                rows = tree.xpath('//table[@class="table table-bordered scrollable-table pw-generated"]/tbody/tr')
                
                # Process rows
                sl_no = ""
                state = ""
                year = financial_year
                
                for row in rows:
                    cells = [cell.text_content().strip() for cell in row.xpath('./td')]
                    
                    if len(cells) == 2:  # Header row with sl_no and state
                        sl_no = cells[0] or sl_no
                        state = cells[1] or state
                    elif len(cells) == 1:  # Year row
                        year = cells[0] or year
                    elif len(cells) == 7:  # Complete data row
                        # Extract category and material from first cell
                        try:
                            cat_material = cells[0].split("(")
                            material = cat_material[0].strip()
                            category = convert_cat(cat_material[1].replace(")", "").strip())
                        except:
                            material = cells[0]
                            category = ""
                        
                        # Pre-consumer data
                        pre_qty = cells[1].strip()
                        pre_recycled = cells[2].strip()
                        try:
                            pre_recycle_consumption = float(pre_qty) * float(pre_recycled) / 100
                        except (ValueError, TypeError):
                            pre_recycle_consumption = 0
                            
                        # Post-consumer data
                        post_qty = cells[3].strip()
                        post_recycled = cells[4].strip()
                        try:
                            post_recycle_consumption = float(post_qty) * float(post_recycled) / 100
                        except (ValueError, TypeError):
                            post_recycle_consumption = 0
                            
                        # Export data
                        export_qty = cells[5].strip()
                        export_recycled = cells[6].strip()
                        try:
                            export_recycle_consumption = float(export_qty) * float(export_recycled) / 100
                        except (ValueError, TypeError):
                            export_recycle_consumption = 0
                            
                        # Calculate total consumption
                        try:
                            total_consumption = float(pre_qty) + float(post_qty) + float(export_qty)
                        except:
                            total_consumption = 0
                            
                        consumption_data.append([
                            sl_no, state, year, category, material, 
                            pre_qty, pre_recycled, pre_recycle_consumption,
                            post_qty, post_recycled, post_recycle_consumption,
                            export_qty, export_recycled, export_recycle_consumption,
                            total_consumption
                        ])
                        
                    elif len(cells) == 5:  # Partial data row
                        # Extract category and material from first cell
                        try:
                            cat_material = cells[0].split("(")
                            material = cat_material[0].strip()
                            category = convert_cat(cat_material[1].replace(")", "").strip())
                        except:
                            material = cells[0]
                            category = ""
                        
                        pre_qty = cells[1].strip()
                        post_qty = cells[2].strip()
                        export_qty = cells[3].strip()
                        export_recycled = cells[4].strip()
                        
                        # Calculate export recycle consumption
                        try:
                            export_recycle_consumption = float(export_qty) * float(export_recycled) / 100
                        except (ValueError, TypeError):
                            export_recycle_consumption = 0
                            
                        # Calculate total consumption
                        try:
                            total_consumption = float(pre_qty) + float(post_qty) + float(export_qty)
                        except:
                            total_consumption = 0
                        
                        consumption_data.append([
                            sl_no, state, year, category, material, 
                            pre_qty, None, None,  # Pre-consumer data (incomplete)
                            post_qty, None, None,  # Post-consumer data (incomplete)
                            export_qty, export_recycled, export_recycle_consumption,
                            total_consumption
                        ])
                
                # Move to next year
                year_count += 1
                
            except Exception as e:
                log(f"⚠️ Error scraping consumption AR for year {year_count}: {str(e)}")
                year_count += 1
                if year_count > 10:  # Safety break
                    break
        
        # Create DataFrame
        columns = [
            "Sl. No.", "State Name", "Year", "Category of Plastic", "Material type",
            "Pre Consumer Waste Plastic Quantity (TPA)", "Pre Consumer Waste Recycled Plastic %",
            "Pre Consumer Waste Recycle Consumption", "Post Consumer Waste Plastic Quantity (TPA)",
            "Post Consumer Waste Recycled Plastic %", "Post Consumer Waste Recycle Consumption",
            "Export Quantity Plastic Quantity (TPA)", "Export Quantity Recycled Plastic %",
            "Export Quantity Recycle Consumption", "Total Consumption"
        ]
        
        df = pd.DataFrame(consumption_data, columns=columns)
        return enrich_df(df, etype, ename, email)
        
    except Exception as e:
        log(f"❌ Error scraping consumption AR data: {str(e)}")
        return pd.DataFrame()


def scrape_table(driver):
    try:
        page = driver.find_element(By.ID, "ScrollableSimpleTableBody")
        html = page.get_attribute("innerHTML")
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("tr")
        data = [[td.text.strip() for td in row.select("td")] for row in rows]
        return pd.DataFrame(data)
    except Exception as e:
        log(f"⚠️ Table scrape error: {str(e)}")
        return pd.DataFrame()


def enrich_df(df: pd.DataFrame, etype, ename, email):
    if df.empty:
        return df
    if len(df) > 0 and len(df.columns) > 0:
        # Only set header row if it appears to be data rows without headers
        if df.iloc[0].astype(str).str.contains(r'\d+').mean() > 0.5:  # If first row has mostly numeric values
            df.columns = df.iloc[0]        # set header row
            df = df.drop(index=0).reset_index(drop=True)
            
    # Add entity metadata
    df["Type_of_entity"] = etype
    df["Entity_Name"] = ename
    df["Email"] = email
    return df
