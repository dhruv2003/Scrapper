import os
import json
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

def start_scraper(cred):
    email = cred["email"]
    try:
        set_scraper_status(email, "Running")
        log(f"Starting EWM scraping for {email}")
        # You can write your EWM specific scraping logic here
        log(f"✅ EWM Scraping Completed for {email}")
        set_scraper_status(email, "Completed")
    except Exception as e:
        log(f"❌ EWM Scraping Error: {e}")
        set_scraper_status(email, "Failed")
