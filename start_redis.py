"""
Redis Server Launcher

This script helps start a Redis server instance on Windows.
It will download Redis if needed, making it easier to get started.
"""

import os
import sys
import subprocess
import zipfile
import requests
import urllib.request
from pathlib import Path
import shutil
import tempfile

REDIS_WINDOWS_URL = "https://github.com/tporadowski/redis/releases/download/v5.0.14.1/Redis-x64-5.0.14.1.zip"
REDIS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "redis")

def download_redis():
    """Download Redis for Windows if not already present"""
    print("📥 Downloading Redis for Windows...")
    
    # Create temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download Redis zip
        zip_path = os.path.join(temp_dir, "redis.zip")
        urllib.request.urlretrieve(REDIS_WINDOWS_URL, zip_path)
        
        # Extract zip
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Create redis directory in our app if it doesn't exist
        os.makedirs(REDIS_DIR, exist_ok=True)
        
        # Copy required files
        redis_files = ["redis-server.exe", "redis-cli.exe", "redis.windows.conf"]
        for file in redis_files:
            src = os.path.join(temp_dir, file)
            if os.path.exists(src):
                shutil.copy2(src, os.path.join(REDIS_DIR, file))
    
    print("✅ Redis downloaded successfully")

def start_redis_server():
    """Start Redis server"""
    redis_path = os.path.join(REDIS_DIR, "redis-server.exe")
    
    if not os.path.exists(redis_path):
        print("⚠️ Redis server not found. Downloading...")
        download_redis()
    
    print("🚀 Starting Redis server...")
    
    # Start Redis server
    redis_process = subprocess.Popen(
        [redis_path], 
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_CONSOLE  # Open in new console window
    )
    
    print("✅ Redis server started in a new console window")
    
    # Start the Redis maintenance script in background
    maintenance_script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "redis_maintenance.py")
    
    # Create the maintenance script if it doesn't exist
    if not os.path.exists(maintenance_script_path):
        print("📝 Creating Redis maintenance script for auto-cleanup...")
        with open(maintenance_script_path, 'w') as f:
            f.write("""import redis
import time
import json
from datetime import datetime, timedelta
import sys
import os

# Redis configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None
JOB_STATUS_PREFIX = "job_status:"

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def connect_to_redis():
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        client.ping()  # Test connection
        return client
    except Exception as e:
        log(f"Error connecting to Redis: {e}")
        return None

def clean_failed_jobs(client, max_age_minutes=5):
    try:
        # Get all job status keys
        status_keys = client.keys(f"{JOB_STATUS_PREFIX}*")
        
        if not status_keys:
            log("No job keys found")
            return 0
        
        deleted_count = 0
        current_time = datetime.now()
        
        for key in status_keys:
            job_id = key.replace(JOB_STATUS_PREFIX, "")
            job_data = client.hgetall(key)
            
            # Check if this is a failed job
            if job_data.get('status') == 'failed':
                # Extract timestamp
                update_time = None
                try:
                    updated_at = job_data.get('updated_at')
                    if updated_at:
                        updated_at_obj = json.loads(updated_at)
                        update_time = datetime.fromisoformat(updated_at_obj.get('timestamp').replace('Z', '+00:00'))
                except (json.JSONDecodeError, ValueError, AttributeError) as e:
                    log(f"Error parsing timestamp for job {job_id}: {e}")
                    continue
                    
                # If no valid timestamp, try created_at
                if not update_time:
                    try:
                        created_at = job_data.get('created_at')
                        if created_at:
                            created_at_obj = json.loads(created_at)
                            update_time = datetime.fromisoformat(created_at_obj.get('timestamp').replace('Z', '+00:00'))
                    except (json.JSONDecodeError, ValueError, AttributeError) as e:
                        log(f"Error parsing timestamp for job {job_id}: {e}")
                        continue
                
                # Skip if we couldn't determine when the job was updated
                if not update_time:
                    log(f"Could not determine age for job {job_id}")
                    continue
                
                # Calculate age in minutes
                age_minutes = (current_time - update_time).total_seconds() / 60
                
                if age_minutes >= max_age_minutes:
                    log(f"Deleting failed job {job_id} (age: {age_minutes:.2f} minutes)")
                    client.delete(key)
                    deleted_count += 1
        
        return deleted_count
    except Exception as e:
        log(f"Error cleaning failed jobs: {e}")
        return 0

def main_cleanup_loop():
    log("Redis maintenance process started")
    log("Cleaning up failed jobs older than 5 minutes")
    
    while True:
        try:
            # Connect to Redis
            client = connect_to_redis()
            if client:
                # Clean old failed jobs
                deleted = clean_failed_jobs(client)
                if deleted > 0:
                    log(f"Cleaned up {deleted} failed job(s)")
                
                # Close connection
                client.close()
            
            # Wait for next check interval (1 minute)
            time.sleep(60)
        except KeyboardInterrupt:
            log("Maintenance process stopped")
            break
        except Exception as e:
            log(f"Error in maintenance loop: {e}")
            time.sleep(60)  # Still wait before retrying

if __name__ == "__main__":
    main_cleanup_loop()
""")
        print("✅ Maintenance script created")
    
    # Start the maintenance script
    try:
        print("🧹 Starting Redis maintenance process for auto-cleanup...")
        maintenance_process = subprocess.Popen(
            [sys.executable, maintenance_script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_CONSOLE  # Open in new console window
        )
        print("✅ Redis maintenance process started")
    except Exception as e:
        print(f"⚠️ Failed to start maintenance process: {e}")
    
    print("ℹ️ Close the Redis console window to stop the Redis server")
    
    # Check if Redis is running by trying to connect with redis-cli
    redis_cli_path = os.path.join(REDIS_DIR, "redis-cli.exe")
    if os.path.exists(redis_cli_path):
        try:
            result = subprocess.run(
                [redis_cli_path, "ping"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5
            )
            if b"PONG" in result.stdout:
                print("✅ Redis server is responding to connections")
            else:
                print("⚠️ Redis server may not be running properly")
                print(f"Output: {result.stdout.decode('utf-8')}")
        except subprocess.TimeoutExpired:
            print("⚠️ Redis server is not responding")
        except Exception as e:
            print(f"⚠️ Error checking Redis: {e}")

if __name__ == "__main__":
    start_redis_server()
    
    print("\n🔍 Redis should now be running at localhost:6379")
    print("To test the connection, run: python redis_diagnostics.py")
    print("To start the worker, run: python start_workers.py")
    
    try:
        input("\nPress Enter to exit this script (Redis will continue running)...")
    except KeyboardInterrupt:
        pass
    
    print("Script exited. Redis server is still running in the other console window.")
