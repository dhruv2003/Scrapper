"""
MongoDB Connection Check Script

This script helps verify your MongoDB connection and list existing databases and collections.
Run this script to diagnose MongoDB connection issues.
"""

import sys
import pymongo
from datetime import datetime

# Use the same connection string as your application
from app.config import MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION

def check_mongodb_connection():
    """Check MongoDB connection and print detailed information."""
    print(f"\n✅ MongoDB Connection Check")
    print(f"============================")
    print(f"Connection string: {MONGO_URI}")
    print(f"Target database: {MONGO_DB_NAME}")
    print(f"Target collection: {MONGO_COLLECTION}")
    print(f"-----------------------------")
    
    try:
        # Connect with a short timeout
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        
        # Check if we can actually connect
        print("Connecting to MongoDB server...")
        client.admin.command('ping')
        print("✅ MongoDB server is running and accessible!")
        
        # List databases
        print("\nAvailable databases:")
        databases = client.list_database_names()
        for db in databases:
            print(f" - {db}")
            
        # Check if our target DB exists
        if MONGO_DB_NAME in databases:
            db = client[MONGO_DB_NAME]
            print(f"\nCollections in '{MONGO_DB_NAME}':")
            collections = db.list_collection_names()
            if collections:
                for collection in collections:
                    doc_count = db[collection].count_documents({})
                    print(f" - {collection} ({doc_count} documents)")
            else:
                print(f" (No collections found - this is normal for a new database)")
        else:
            print(f"\nNote: Database '{MONGO_DB_NAME}' does not exist yet.")
            print("This is normal if you haven't saved any data. MongoDB creates databases and collections automatically when you first insert data.")
            
        # Insert a test document
        print("\nWould you like to insert a test document? (y/n)")
        choice = input().lower()
        if choice == 'y':
            db = client[MONGO_DB_NAME]
            collection = db[MONGO_COLLECTION]
            test_doc = {
                "_id": "test@example.com",
                "company_name": "Test Company",
                "entity_type": "Test",
                "created_at": datetime.now(),
                "scrap_data": {
                    "2023": {
                        "test": [{"value": "This is a test document"}]
                    }
                }
            }
            result = collection.update_one(
                {"_id": "test@example.com"},
                {"$set": test_doc},
                upsert=True
            )
            if result.upserted_id:
                print(f"✅ Test document inserted successfully!")
            else:
                print(f"✅ Test document updated successfully!")
                
            # Verify it's there
            doc_count = collection.count_documents({})
            print(f"Collection now has {doc_count} documents")
            
        print("\n📋 MongoDB Connection Check Complete")
        
    except pymongo.errors.ConnectionFailure as e:
        print(f"❌ ERROR: Could not connect to MongoDB server at {MONGO_URI}")
        print(f"Error details: {e}")
        print("\nPossible reasons for connection failure:")
        print(" 1. MongoDB server is not running")
        print(" 2. MongoDB is running on a different port or address")
        print(" 3. There is a firewall blocking the connection")
        print("\nSuggestions:")
        print(" - Make sure MongoDB is installed and running")
        print(" - Check if MongoDB is running on the correct port (default is 27017)")
        print(" - Try connecting with MongoDB Compass or another client")
        return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False
    finally:
        if 'client' in locals():
            client.close()

    return True

if __name__ == "__main__":
    print("MongoDB Connection Check Tool")
    print("This tool will check your MongoDB connection and show available databases.")
    
    if check_mongodb_connection():
        sys.exit(0)
    else:
        sys.exit(1)
