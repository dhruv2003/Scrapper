import os
import pandas as pd
import re
from datetime import datetime
from typing import Dict, Optional, List, Tuple
from glob import glob

from app.services.mongo_utils import save_to_mongodb
from app.services.utils import log

def list_excel_files(directory: str = "scraped_data") -> List[str]:
    """Lists all Excel files in the specified directory"""
    if not os.path.exists(directory):
        log(f"❌ Directory not found: {directory}")
        return []
    
    excel_files = glob(os.path.join(directory, "*.xlsx"))
    log(f"📋 Found {len(excel_files)} Excel files")
    return excel_files

def extract_metadata_from_filename(filename: str) -> Tuple[str, str, Optional[datetime]]:
    """Extracts entity name and timestamp from filename"""
    base_name = os.path.basename(filename)
    # Expected format: PWM_EntityName_YYYYMMDD_HHMMSS.xlsx
    # or: PWM_email_YYYYMMDD_HHMMSS.xlsx
    
    # Extract entity name or email
    match = re.search(r'PWM_([^_]+)_(\d{8}_\d{6})\.xlsx', base_name)
    if match:
        entity_or_email = match.group(1)
        timestamp_str = match.group(2)
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
            return entity_or_email, base_name, timestamp
        except ValueError:
            return entity_or_email, base_name, None
    
    return "unknown", base_name, None

def read_excel_file(file_path: str) -> Dict[str, pd.DataFrame]:
    """Reads all sheets from an Excel file into a dictionary of DataFrames"""
    try:
        log(f"📖 Reading Excel file: {file_path}")
        
        # Read all sheets into a dictionary
        all_sheets = pd.read_excel(file_path, sheet_name=None)
        
        # Count rows in each sheet for logging
        total_rows = sum(len(df) for df in all_sheets.values())
        log(f"✅ Read {len(all_sheets)} sheets with {total_rows} total rows")
        
        return all_sheets
    except Exception as e:
        log(f"❌ Error reading Excel file: {str(e)}")
        return {}

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
            if col in df.columns and not df[col].empty:
                email = df[col].iloc[0]
                break
        
        # Try common column names for entity name
        name_cols = ["Entity_Name", "entity_name", "Name of the Entity", "Company Name"]
        for col in name_cols:
            if col in df.columns and not df[col].empty:
                entity_name = df[col].iloc[0]
                break
                
        # Try common column names for entity type
        type_cols = ["Type_of_entity", "entity_type", "User Type", "Entity Type"]
        for col in type_cols:
            if col in df.columns and not df[col].empty:
                entity_type = df[col].iloc[0]
                break
        
        # If we found all three, we can stop searching
        if email and entity_name and entity_type:
            break
    
    return email, entity_name, entity_type

def import_excel_to_mongodb(excel_file: str, year: Optional[int] = None) -> str:
    """
    Imports data from an Excel file into MongoDB
    
    Args:
        excel_file: Path to Excel file
        year: Year to use for the data (defaults to current year)
        
    Returns:
        Status message
    """
    # Extract metadata from filename
    entity_or_email, filename, _ = extract_metadata_from_filename(excel_file)
    
    # Read Excel file
    dataframes = read_excel_file(excel_file)
    if not dataframes:
        return f"❌ No data found in {filename}"
    
    # Extract entity info from data
    email, entity_name, entity_type = extract_entity_info(dataframes)
    
    # Use extracted email or fallback to entity_or_email if it looks like an email
    if not email and '@' in entity_or_email:
        email = entity_or_email
    
    # If we still don't have an email, we can't proceed
    if not email:
        return f"❌ Could not determine email for {filename}"
    
    # Use entity name from data or fallback to filename-extracted entity
    if not entity_name:
        entity_name = entity_or_email if '@' not in entity_or_email else "Unknown"
    
    # Default entity type if not found
    if not entity_type:
        entity_type = "Unknown"
    
    # Use provided year or current year
    if year is None:
        year = datetime.now().year
    
    # Save to MongoDB
    log(f"💾 Saving to MongoDB: {email}, {entity_name}, {entity_type}, year={year}")
    result = save_to_mongodb(
        data_dict=dataframes,
        email=email,
        company_name=entity_name, 
        entity_type=entity_type,
        year=year
    )
    
    return f"📊 Import result for {filename}: {result}"

def batch_import_excel_files(directory: str = "scraped_data", year: Optional[int] = None) -> List[str]:
    """
    Imports all Excel files from directory into MongoDB
    
    Args:
        directory: Directory containing Excel files
        year: Year to use for the data (defaults to current year)
        
    Returns:
        List of status messages
    """
    excel_files = list_excel_files(directory)
    if not excel_files:
        return ["❌ No Excel files found to import"]
    
    results = []
    for excel_file in excel_files:
        result = import_excel_to_mongodb(excel_file, year)
        results.append(result)
    
    return results
