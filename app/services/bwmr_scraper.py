import os
import json
import pandas as pd
from datetime import datetime
from app.services.utils import log, set_scraper_status

def load_credentials():
    """Load credentials from credential files"""
    creds = {}
    for filename in ["credentials.json", "credentials1.json", "credentials2.json"]:
        if os.path.exists(filename):
            try:
                with open(filename) as f:
                    creds.update(json.load(f))
            except Exception as e:
                log(f"❌ Error loading credentials from {filename}: {e}")
    return creds

def save_excel_file(data, email, entity_name=None):
    """Save scraped data to Excel file"""
    try:
        now = datetime.now()
        os.makedirs("scraped_data", exist_ok=True)
        
        # Create filename with entity name if available
        if entity_name:
            safe_name = "".join(c if c.isalnum() else "_" for c in entity_name)
            filename = f"scraped_data/BWM_{safe_name}_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
        else:
            email_prefix = email.split('@')[0] if email else "unknown"
            filename = f"scraped_data/BWM_{email_prefix}_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # If we have DataFrame data
        if isinstance(data, dict):
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                for sheet_name, df in data.items():
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name.capitalize(), index=False)
            
            log(f"📊 BWM Excel saved: {filename}")
            return filename
        return None
    except Exception as e:
        log(f"❌ Error saving BWM Excel: {e}")
        return None

def start_scraper(cred):
    email = cred["email"]
    try:
        set_scraper_status(email, "Running")
        log(f"Starting BWM scraping for {email}")
        # You can write your BWM specific scraping logic here
        
        # Demo data - replace with actual scraping results
        demo_data = {
            "annual": pd.DataFrame({'Year': [2022, 2023], 'Value': [100, 200]}),
            "summary": pd.DataFrame({'Category': ['A', 'B'], 'Amount': [10, 20]})
        }
        
        # Save to Excel
        excel_file = save_excel_file(demo_data, email, cred.get("entity_name"))
        
        log(f"✅ BWM Scraping Completed for {email}")
        set_scraper_status(email, "Completed")
        
        return {"excel_file": excel_file} if excel_file else {}
    except Exception as e:
        log(f"❌ BWM Scraping Error: {e}")
        set_scraper_status(email, "Failed")
        return {"error": str(e)}
