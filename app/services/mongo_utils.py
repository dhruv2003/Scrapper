import sys
from datetime import datetime
from typing import Dict, Optional
import pandas as pd

try:
    import pymongo
except ImportError:
    print("pymongo is not installed. Please fix your environment by following these steps:")
    print("1. Delete your current venv: 'rm -rf venv'")
    print("2. Create a new venv: 'python -m venv venv'") 
    print("3. Activate it: 'venv\\Scripts\\activate' (Windows) or 'source venv/bin/activate' (Unix)")
    print("4. Install pymongo: 'python -m pip install pymongo'")
    sys.exit(1)

from app.config import MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION

def get_mongo_client():
    """
    Create and return a MongoDB client connection.
    """
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Trigger a command to check the connection
        client.admin.command('ping')
        return client
    except pymongo.errors.ConnectionFailure as e:
        print(f"MongoDB connection error: {e}")
        print("Please check if MongoDB is running and MONGO_URI is correctly configured.")
        return None
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def save_to_mongodb(
    data_dict: Dict[str, pd.DataFrame], 
    email: str, 
    company_name: str, 
    entity_type: str,
    year: Optional[int] = None
):
    """
    Save scraped data to MongoDB with the following structure:
    {
        "_id": "email@example.com",
        "company_name": "XYZ Pvt Ltd",
        "entity_type": "Producer",
        "scrap_data": {
            "2025": {
                "procurement": [...],
                "sales": [...],
                ...
            }
        }
    }
    
    Args:
        data_dict: Dictionary of DataFrames from scraper
        email: Email ID to use as document _id
        company_name: Company name
        entity_type: Type of entity
        year: Year to use for the data (defaults to current year)
    
    Returns:
        Status message with MongoDB operation result
    """
    if year is None:
        # Use current year if not specified
        year = datetime.now().year
    
    year_str = str(year)
    
    # Convert DataFrames to list of dicts
    transformed_data = {}
    for section, df in data_dict.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            # Convert DataFrame to list of dictionaries
            # Drop the entity name, type and email as those are already stored at doc level
            if "Type_of_entity" in df.columns:
                df = df.drop("Type_of_entity", axis=1)
            if "Entity_Name" in df.columns:
                df = df.drop("Entity_Name", axis=1)
            if "Email" in df.columns:
                df = df.drop("Email", axis=1)
            
            # Convert to records
            transformed_data[section] = df.to_dict(orient="records")
        else:
            transformed_data[section] = []
    
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION]
        
        # Create update operation with upsert
        # This will either update the existing document or create a new one
        result = collection.update_one(
            {"_id": email},
            {
                "$set": {
                    "company_name": company_name,
                    "entity_type": entity_type,
                    f"scrap_data.{year_str}": transformed_data
                }
            },
            upsert=True
        )
        
        if result.upserted_id:
            return f"New document created with ID: {result.upserted_id}"
        else:
            return f"Document updated. Modified: {result.modified_count}"
    
    except Exception as e:
        return f"Error saving to MongoDB: {str(e)}"
    finally:
        if 'client' in locals():
            client.close()
