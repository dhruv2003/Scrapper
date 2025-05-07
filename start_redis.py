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
    print("ℹ️ Close that console window to stop the Redis server")
    
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
