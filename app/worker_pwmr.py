# import multiprocessing
# multiprocessing.set_start_method('spawn', force=True)

# from app.services.pwmr_scraper import start_scraper

# def scrape_pwm_task(user_data):
#     email = user_data.get("email")
#     password = user_data.get("password")

#     if not email or not password:
#         print("❌ Missing email or password!")
#         return None

#     scraped_data = start_scraper({"email": email, "password": password})

#     print("✅ Scraping completed. Sample output:")

#     for section, df in scraped_data.items():
#         print(f"\nSection: {section}")
#         if df is not None and not df.empty:
#             print(df.head())
#         else:
#             print("No data found.")

#     return "Scraping finished"
