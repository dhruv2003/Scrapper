import sys
import math
from datetime import datetime
from typing import Dict, Optional, List, Tuple
import pandas as pd
import json
import re

try:
    import pymongo
    from pymongo.errors import DocumentTooLarge, BulkWriteError
except ImportError:
    print("pymongo is not installed. Please fix your environment by following these steps:")
    print("1. Delete your current venv: 'rm -rf venv'")
    print("2. Create a new venv: 'python -m venv venv'") 
    print("3. Activate it: 'venv\\Scripts\\activate' (Windows) or 'source venv/bin/activate' (Unix)")
    print("4. Install pymongo: 'python -m pip install pymongo'")
    sys.exit(1)

from app.config import MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION
from app.services.utils import log

# Maximum MongoDB document size (16MB - 1KB safety margin)
MAX_DOCUMENT_SIZE_BYTES = 16 * 1024 * 1024 - 1024

def get_mongo_client():
    """
    Create and return a MongoDB client connection.
    """
    try:
        log(f"📌 Connecting to MongoDB at: {MONGO_URI}")
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Trigger a command to check the connection
        client.admin.command('ping')
        log("✅ MongoDB connection successful")
        return client
    except pymongo.errors.ConnectionFailure as e:
        log(f"❌ MongoDB connection error: {e}")
        log("Please check if MongoDB is running and MONGO_URI is correctly configured.")
        return None
    except Exception as e:
        log(f"❌ Error connecting to MongoDB: {e}")
        return None

def extract_financial_year(text):
    """Extract financial year from text in format YYYY-YY"""
    if not text:
        return None
    
    # If already a year, return it
    if isinstance(text, int) or text.isdigit():
        return int(text)
        
    # Try to find financial year pattern (YYYY-YY)
    match = re.search(r'(\d{4})-\d{2}', str(text))
    if match:
        return int(match.group(1))
    
    # Try to extract any 4-digit year
    match = re.search(r'\b(20\d{2})\b', str(text))
    if match:
        return int(match.group(1))
    
    # Default to current year if nothing found
    return datetime.now().year

def estimate_bson_size(data: dict) -> int:
    """
    Estimate the BSON size of a document to avoid hitting MongoDB's 16MB limit.
    This is a rough estimate, but should be conservative enough.
    """
    try:
        # Convert to JSON and back to get a good size estimate
        json_str = json.dumps(data)
        return len(json_str) * 1.1  # Add 10% overhead for BSON encoding
    except Exception:
        # Fallback to a more conservative estimate
        return sys.getsizeof(str(data)) * 1.5  # 50% overhead to be safe

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
    # Use current year if not specified
    current_year = datetime.now().year
    if year is None:
        year = current_year
    
    # Make sure company_name and entity_type are strings, not pandas Series
    if isinstance(company_name, pd.Series):
        company_name = company_name.iloc[0] if len(company_name) > 0 else "Unknown Company"
    if isinstance(entity_type, pd.Series):
        entity_type = entity_type.iloc[0] if len(entity_type) > 0 else "Unknown Type"
    
    # Ensure email is a string
    if isinstance(email, pd.Series):
        email = email.iloc[0] if len(email) > 0 else "unknown@example.com"
    
    # Count actual data to save
    valid_dataframes = sum(1 for df in data_dict.values() if isinstance(df, pd.DataFrame) and not df.empty)
    log(f"📊 Preparing to save {valid_dataframes} dataframes to MongoDB")
    
    # Track metrics for final log
    sections_saved = 0
    rows_saved = 0
    errors = 0
    
    try:
        client = get_mongo_client()
        if not client:
            return "Failed to connect to MongoDB"
            
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION]
        overflow_collection = db[f"{MONGO_COLLECTION}_overflow"]
        
        # First, create or update the base document for this email
        base_doc = {
            "company_name": company_name,
            "entity_type": entity_type,
            "last_updated": datetime.now(),
            "scrap_data": {}  # Will be filled with year data
        }
        
        # Get existing document if it exists
        existing_doc = collection.find_one({"_id": email})
        
        # Initialize with existing scrap_data if available
        if existing_doc and "scrap_data" in existing_doc:
            base_doc["scrap_data"] = existing_doc["scrap_data"]
        
        # Create or update the base document
        collection.update_one(
            {"_id": email},
            {"$set": {
                "company_name": company_name,
                "entity_type": entity_type,
                "last_updated": datetime.now()
            }},
            upsert=True
        )
        
        # Process each DataFrame and organize by year
        for section_name, df in data_dict.items():
            if not isinstance(df, pd.DataFrame) or len(df) == 0:
                log(f"⚠️ Skipping empty section: {section_name}")
                continue
                
            log(f"Processing section: {section_name}, shape: {df.shape}")
            
            # For target and multi-year data, we need to handle year separately
            if "Financial_Year" in df.columns:
                # Convert Financial_Year column to string to avoid Series comparison issues
                df["Financial_Year"] = df["Financial_Year"].astype(str)
                
                # Get unique years safely
                unique_years = df["Financial_Year"].unique()
                
                # Process each year separately
                for year_label in unique_years:
                    # Filter dataframe safely for this year
                    year_df = df[df["Financial_Year"] == year_label]
                    
                    year_num = extract_financial_year(year_label) or current_year
                    year_str = str(year_num)
                    
                    # Check estimated size before trying to save
                    records = prepare_dataframe(year_df)
                    estimated_size = estimate_bson_size({"data": records})
                    
                    if estimated_size > MAX_DOCUMENT_SIZE_BYTES:
                        log(f"⚠️ Section '{section_name}' for year {year_str} is too large (~{estimated_size/1024/1024:.2f}MB), splitting into chunks")
                        chunks_saved = save_large_section(
                            collection, overflow_collection, email, year_str, section_name, records
                        )
                        sections_saved += chunks_saved
                        rows_saved += len(records)
                    else:
                        # Save directly to the document
                        success = save_section_to_main_document(
                            collection, email, year_str, section_name, records
                        )
                        if success:
                            sections_saved += 1
                            rows_saved += len(records)
                        else:
                            errors += 1
            else:
                # For regular data, use the provided year
                year_str = str(year)
                
                # Check estimated size before trying to save
                records = prepare_dataframe(df)
                estimated_size = estimate_bson_size({"data": records})
                
                if estimated_size > MAX_DOCUMENT_SIZE_BYTES:
                    log(f"⚠️ Section '{section_name}' is too large (~{estimated_size/1024/1024:.2f}MB), splitting into chunks")
                    chunks_saved = save_large_section(
                        collection, overflow_collection, email, year_str, section_name, records
                    )
                    sections_saved += chunks_saved
                    rows_saved += len(records)
                else:
                    # Save directly to the document
                    success = save_section_to_main_document(
                        collection, email, year_str, section_name, records
                    )
                    if success:
                        sections_saved += 1
                        rows_saved += len(records)
                    else:
                        errors += 1
        
        # Verify the document was saved
        doc = collection.find_one({"_id": email})
        if doc:
            years_saved = list(doc.get("scrap_data", {}).keys())
            success_msg = (
                f"✅ Data saved successfully for email: {email} | "
                f"Years: {', '.join(years_saved)} | "
                f"Sections: {sections_saved} | "
                f"Rows: {rows_saved} | "
                f"Errors: {errors}"
            )
            log(success_msg)
            return success_msg
        else:
            err_msg = f"⚠️ Document not found after save for email: {email}"
            log(err_msg)
            return err_msg
        
    except Exception as e:
        log(f"❌ Error saving to MongoDB: {str(e)}")
        return f"Error saving to MongoDB: {str(e)}"
    
    finally:
        if 'client' in locals() and client:
            client.close()

def prepare_dataframe(df: pd.DataFrame) -> list:
    """
    Convert a DataFrame to a list of records that's safe for MongoDB storage.
    Handles NaN values, Series objects, and other problematic data types.
    """
    # Handle NaN values - MongoDB doesn't like NaN
    df = df.fillna("")
    
    # Clean column names
    df.columns = [str(col).strip() for col in df.columns]
    
    # Drop entity metadata columns that are stored at document level
    metadata_cols = ["Type_of_entity", "Entity_Name", "entity_name", "Email", "email_id"]
    for col in metadata_cols:
        if col in df.columns:
            df = df.drop(col, axis=1)
    
    # Convert to records - SafeRowDict approach
    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            
            # Handle various data types appropriately
            if pd.isna(val) or pd.isnull(val):
                record[col] = ""
            elif isinstance(val, pd.Series):
                # If it's a Series, get the first value
                record[col] = val.iloc[0] if len(val) > 0 else ""
            elif isinstance(val, (int, float)):
                # For numeric values, check for NaN or infinity
                if pd.isna(val) or math.isinf(val):
                    record[col] = ""
                else:
                    record[col] = val
            elif isinstance(val, bool):
                record[col] = val
            elif isinstance(val, str):
                record[col] = val
            elif isinstance(val, (list, dict)):
                # Handle list and dict types
                try:
                    # Try to convert to json string and back to ensure it's MongoDB-safe
                    record[col] = json.loads(json.dumps(val))
                except:
                    record[col] = str(val)
            else:
                # For everything else, convert to string
                record[col] = str(val)
        
        records.append(record)
    
    return records

def save_section_to_main_document(collection, email, year_str, section_name, records):
    """
    Save a section directly to the main document.
    Returns True on success, False on failure.
    """
    try:
        # Save this section to MongoDB
        collection.update_one(
            {"_id": email},
            {"$set": {
                f"scrap_data.{year_str}.{section_name}": records
            }},
            upsert=True
        )
        
        log(f"✅ Saved {len(records)} records for section '{section_name}' in year {year_str}")
        return True
    except DocumentTooLarge:
        log(f"⚠️ Document too large when trying to save section '{section_name}' for {year_str}")
        return False
    except Exception as e:
        log(f"❌ Failed to save section '{section_name}' for year {year_str}: {e}")
        return False

def save_large_section(collection, overflow_collection, email, year_str, section_name, records):
    """
    Save a large section by splitting it into manageable chunks in the overflow collection.
    Returns the number of chunks saved.
    """
    try:
        # Determine how many chunks we need
        # Starting with max 1000 records per chunk, adjust if needed
        chunk_size = 1000
        num_records = len(records)
        num_chunks = max(1, math.ceil(num_records / chunk_size))
        
        # If we still have too many chunks, try larger chunks
        if num_chunks > 50:  # Arbitrary threshold
            chunk_size = max(1, math.ceil(num_records / 50))
            num_chunks = max(1, math.ceil(num_records / chunk_size))
            
        log(f"Splitting {num_records} records into {num_chunks} chunks of ~{chunk_size} records each")
        
        # Generate a unique base ID for this section's chunks
        base_chunk_id = f"{email}_{year_str}_{section_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Save each chunk
        chunks_saved = 0
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, num_records)
            
            # Extract records for this chunk
            chunk_records = records[start_idx:end_idx]
            
            # Generate chunk ID
            chunk_id = f"{base_chunk_id}_chunk_{i+1}"
            
            # Save to overflow collection
            overflow_collection.insert_one({
                "_id": chunk_id,
                "email": email,
                "year": year_str,
                "section": section_name,
                "chunk": i+1,
                "total_chunks": num_chunks,
                "data": chunk_records,
                "created_at": datetime.now()
            })
            
            chunks_saved += 1
            
        # Add reference to main document
        collection.update_one(
            {"_id": email},
            {"$set": {
                f"scrap_data.{year_str}.{section_name}_ref": {
                    "base_id": base_chunk_id,
                    "chunks": num_chunks,
                    "records": num_records
                }
            }}
        )
        
        log(f"✅ Saved {section_name} in {chunks_saved} chunks to overflow collection")
        return chunks_saved
    
    except Exception as e:
        log(f"❌ Failed to save large section '{section_name}': {e}")
        return 0

def check_existing_data(email: str, year: Optional[int] = None) -> bool:
    """
    Check if data already exists for the given email and year
    
    Args:
        email: Email ID to check
        year: Year to check (defaults to current year)
        
    Returns:
        True if data exists, False otherwise
    """
    if year is None:
        year = datetime.now().year
    
    year_str = str(year)
    
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION]
        
        # Check if document exists with data for this year
        result = collection.count_documents({
            "_id": email,
            f"scrap_data.{year_str}": {"$exists": True}
        })
        
        return result > 0
    
    except Exception as e:
        log(f"Error checking MongoDB: {str(e)}")
        return False
    finally:
        if 'client' in locals() and client:
            client.close()

def list_available_entities() -> List[Dict]:
    """
    List all entities available in the database
    
    Returns:
        List of dictionaries with entity information
    """
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION]
        
        # Find all documents and return email, company name, entity type and years
        entities = []
        for doc in collection.find({}, {"company_name": 1, "entity_type": 1, "scrap_data": 1}):
            entity_info = {
                "email": doc["_id"],
                "company_name": doc.get("company_name", "Unknown"),
                "entity_type": doc.get("entity_type", "Unknown"),
                "years": list(doc.get("scrap_data", {}).keys())
            }
            entities.append(entity_info)
        
        return entities
    
    except Exception as e:
        log(f"Error listing entities: {str(e)}")
        return []
    finally:
        if 'client' in locals() and client:
            client.close()

def get_entity_data(email: str, year: Optional[int] = None):
    """
    Get data for a specific entity and year
    
    Args:
        email: Email ID to get data for
        year: Year to get data for (defaults to all years)
        
    Returns:
        Entity data including all sections
    """
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION]
        overflow_collection = db[f"{MONGO_COLLECTION}_overflow"];
        
        # Find the document
        doc = collection.find_one({"_id": email})
        if not doc:
            log(f"No data found for email: {email}")
            return None
        
        # If year is specified, filter to just that year's data
        if year is not None:
            year_str = str(year)
            if "scrap_data" not in doc or year_str not in doc["scrap_data"]:
                log(f"No data found for email: {email}, year: {year}")
                return None
            
            # Get the data for this year
            year_data = doc["scrap_data"][year_str]
            
            # Check for overflow references and merge them
            for section_name, section_data in list(year_data.items()):
                if section_name.endswith("_ref"):
                    # This is a reference to data in the overflow collection
                    base_section = section_name.replace("_ref", "")
                    
                    # Handle both old and new chunking formats
                    if isinstance(section_data, dict) and "chunks" in section_data:
                        # New format with chunks
                        base_id = section_data["base_id"]
                        num_chunks = section_data["chunks"]
                        
                        # Fetch all chunks and combine them
                        combined_data = []
                        for i in range(1, num_chunks + 1):
                            chunk_id = f"{base_id}_chunk_{i}"
                            chunk_doc = overflow_collection.find_one({"_id": chunk_id})
                            if chunk_doc and "data" in chunk_doc:
                                combined_data.extend(chunk_doc["data"])
                        
                        # Add combined data to result
                        year_data[base_section] = combined_data
                        
                    else:
                        # Old format - single reference
                        overflow_id = section_data
                        overflow_doc = overflow_collection.find_one({"_id": overflow_id})
                        if overflow_doc and "data" in overflow_doc:
                            # Add the actual data to the result
                            year_data[base_section] = overflow_doc["data"]
            
            result = {
                "email": email,
                "company_name": doc.get("company_name"),
                "entity_type": doc.get("entity_type"),
                "year": year,
                "data": year_data
            }
        else:
            # Return data for all years, resolving overflow references
            result = {
                "email": email,
                "company_name": doc.get("company_name"),
                "entity_type": doc.get("entity_type"),
                "years": {},
            }
            
            for year_str, year_data in doc.get("scrap_data", {}).items():
                result["years"][year_str] = {}
                
                # Copy the year data
                for section_name, section_data in year_data.items():
                    if section_name.endswith("_ref"):
                        # This is a reference to data in the overflow collection
                        base_section = section_name.replace("_ref", "")
                        
                        # Handle both old and new chunking formats
                        if isinstance(section_data, dict) and "chunks" in section_data:
                            # New format with chunks
                            base_id = section_data["base_id"]
                            num_chunks = section_data["chunks"]
                            
                            # Fetch all chunks and combine them
                            combined_data = []
                            for i in range(1, num_chunks + 1):
                                chunk_id = f"{base_id}_chunk_{i}"
                                chunk_doc = overflow_collection.find_one({"_id": chunk_id})
                                if chunk_doc and "data" in chunk_doc:
                                    combined_data.extend(chunk_doc["data"])
                        
                            # Add combined data to result
                            result["years"][year_str][base_section] = combined_data
                        else:
                            # Old format - single reference
                            overflow_id = section_data
                            overflow_doc = overflow_collection.find_one({"_id": overflow_id})
                            if overflow_doc and "data" in overflow_doc:
                                # Add the actual data to the result
                                result["years"][year_str][base_section] = overflow_doc["data"]
                    else:
                        result["years"][year_str][section_name] = section_data
        
        return result
        
    except Exception as e:
        log(f"Error getting entity data: {str(e)}")
        return None
    finally:
        if 'client' in locals() and client:
            client.close()

def extract_entity_info(dataframes: Dict[str, pd.DataFrame]) -> Tuple[str, str, str]:
    """Extract entity information from DataFrames if available"""
    email = None
    entity_name = None
    entity_type = None
    
    # Try to find entity info in any DataFrame
    for sheet_name, df in dataframes.items():
        if df.empty:
            continue
            
        # Try common column names for email
        email_cols = ["Email", "email", "email_id"]
        for col in email_cols:
            if col in df.columns and len(df[col]) > 0 and not pd.isna(df[col].iloc[0]):
                email = df[col].iloc[0]
                break
        
        # Try common column names for entity name
        name_cols = ["Entity_Name", "entity_name", "Name of the Entity", "Company Name"]
        for col in name_cols:
            if col in df.columns and len(df[col]) > 0 and not pd.isna(df[col].iloc[0]):
                entity_name = df[col].iloc[0]
                break
                
        # Try common column names for entity type
        type_cols = ["Type_of_entity", "entity_type", "User Type", "Entity Type"]
        for col in type_cols:
            if col in df.columns and len(df[col]) > 0 and not pd.isna(df[col].iloc[0]):
                entity_type = df[col].iloc[0]
                break
        
        # If we found all three, we can stop searching
        if email and entity_name and entity_type:
            break
    
    return email, entity_name, entity_type
