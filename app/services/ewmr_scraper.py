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
            filename = f"scraped_data/EWM_{safe_name}_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
        else:
            email_prefix = email.split('@')[0] if email else "unknown"
            filename = f"scraped_data/EWM_{email_prefix}_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # If we have DataFrame data
        if isinstance(data, dict):
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                for sheet_name, df in data.items():
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name.capitalize(), index=False)
            
            log(f"📊 EWM Excel saved: {filename}")
            return filename
        return None
    except Exception as e:
        log(f"❌ Error saving EWM Excel: {e}")
        return None

def start_scraper(cred):
    email = cred["email"]
    try:
        set_scraper_status(email, "Running")
        log(f"Starting EWM scraping for {email}")
        # You can write your EWM specific scraping logic here
        
        # Demo data - replace with actual scraping results
        demo_data = {
            "collection": pd.DataFrame({'Month': ['Jan', 'Feb'], 'Amount': [500, 600]}),
            "recycling": pd.DataFrame({'Type': ['Electronic', 'Battery'], 'Weight': [50, 30]})
        }
        
        # Save to Excel
        excel_file = save_excel_file(demo_data, email, cred.get("entity_name"))
        
        log(f"✅ EWM Scraping Completed for {email}")
        set_scraper_status(email, "Completed")
        
        return {"excel_file": excel_file} if excel_file else {}
    except Exception as e:
        log(f"❌ EWM Scraping Error: {e}")
        set_scraper_status(email, "Failed")
        return {"error": str(e)}
