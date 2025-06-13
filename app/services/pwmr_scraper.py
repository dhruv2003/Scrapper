# app/services/pwmr_scraper.py

import os
import time
import json
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
from selenium.common.exceptions import WebDriverException

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
                driver.quit()
            except:
                pass

        # Close DB generator
        try:
            next(db_gen)
        except StopIteration:
            pass


def scrape_all(driver, email: str) -> dict:
    """
    Scrape all six sections and return a dict of DataFrames.
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
    df_wallet = scrape_wallet(driver, entity_type, entity_name, email)
    df_target = scrape_target(driver, entity_type, entity_name, email)
    df_annual, df_compliance, df_next = scrape_annual(driver, entity_type, entity_name, email)

    return {
        "procurement": df_proc,
        "sales":       df_sales,
        "wallet":      df_wallet,
        "target":      df_target,
        "annual":      df_annual,
        "compliance":  df_compliance,
        "next_target": df_next,
    }


def scrape_procurement(driver, etype, ename, email):
    log("🛒 Scraping Procurement...")
    driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-operations/material")
    time.sleep(3)
    # ... set dates and fetch ...
    df = scrape_table(driver)
    return enrich_df(df, etype, ename, email)


def scrape_sales(driver, etype, ename, email):
    log("💵 Scraping Sales...")
    driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-operations/sales")
    time.sleep(3)
    # ... loop years ...
    df = scrape_table(driver)
    return enrich_df(df, etype, ename, email)


def scrape_wallet(driver, etype, ename, email):
    log("👛 Scraping Wallet...")
    driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-wallet")
    time.sleep(3)
    df = scrape_table(driver)
    return enrich_df(df, etype, ename, email)


def scrape_target(driver, etype, ename, email):
    log("🎯 Scraping Target...")
    driver.get("https://eprplastic.cpcb.gov.in/#/epr/pibo-dashboard-view")
    time.sleep(3)
    df = scrape_table(driver)
    return enrich_df(df, etype, ename, email)


def scrape_annual(driver, etype, ename, email):
    log("📈 Scraping Annual, Compliance & Next Target...")
    driver.get("https://eprplastic.cpcb.gov.in/#/epr/annual-report-filing")
    time.sleep(3)
    # Assuming the page structure returns three consecutive tables
    df1 = scrape_table(driver)
    df2 = scrape_table(driver)
    df3 = scrape_table(driver)
    return (
        enrich_df(df1, etype, ename, email),
        enrich_df(df2, etype, ename, email),
        enrich_df(df3, etype, ename, email)
    )


def scrape_table(driver):
    try:
        page = driver.find_element(By.ID, "ScrollableSimpleTableBody")
        html = page.get_attribute("innerHTML")
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("tr")
        data = [[td.text.strip() for td in row.select("td")] for row in rows]
        return pd.DataFrame(data)
    except Exception:
        return pd.DataFrame()


def enrich_df(df: pd.DataFrame, etype, ename, email):
    if df.empty:
        return df
    df.columns = df.iloc[0]        # set header row if needed
    df = df.drop(index=0).reset_index(drop=True)
    df["Type_of_entity"] = etype
    df["Entity_Name"]      = ename
    df["Email"]            = email
    return df
