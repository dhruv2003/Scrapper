"""
Quick Redis Check

Simple tool to test Redis connectivity and provide 
solutions to common Redis connection issues.
"""

import os
import sys
import redis
import socket
import platform
import subprocess
import time
from pathlib import Path

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

def print_header(text):
    print("\n" + "=" * 60)
    print(f" {text}")
    print("=" * 60)

def check_redis_installation():
    print_header("Checking Redis Installation")
    
    redis_dir = Path(__file__).parent / "redis"
    redis_server = redis_dir / "redis-server.exe"
    
    if platform.system() != 'Windows':
        print("Non-Windows system detected. Please install Redis according to your OS instructions.")
        print("For Linux: sudo apt-get install redis-server")
        print("For macOS: brew install redis")
        return False
    
    if not redis_server.exists():
        print("❌ Redis server executable not found!")
        print(f"Expected location: {redis_server}")
        print("\nWould you like to download Redis now? (y/n)")
        choice = input("> ").strip().lower()
        if choice.startswith('y'):
            # Run the start_redis.py script that has download functionality
            try:
                start_redis_script = Path(__file__).parent / "start_redis.py"
                if start_redis_script.exists():
                    print("\nRunning start_redis.py to download Redis...")
                    subprocess.run([sys.executable, str(start_redis_script)])
                    return True
                else:
                    print(f"❌ Could not find {start_redis_script}")
                    return False
            except Exception as e:
                print(f"❌ Error running start_redis.py: {e}")
                return False
        return False
    
    print(f"✅ Found Redis server at: {redis_server}")
    return True

def check_redis_process():
    print_header("Checking Redis Process")
    
    try:
        if platform.system() == 'Windows':
            output = subprocess.check_output('tasklist', shell=True).decode('utf-8')
            if 'redis-server.exe' in output:
                print("✅ Redis server is running")
                return True
            else:
                print("❌ Redis server is NOT running")
        else:
            output = subprocess.check_output(['ps', 'aux']).decode('utf-8')
            if 'redis-server' in output:
                print("✅ Redis server is running")
                return True
            else:
                print("❌ Redis server is NOT running")
        
        return False
    except Exception as e:
        print(f"❌ Error checking Redis process: {e}")
        return False

def check_port():
    print_header(f"Checking Port {REDIS_PORT}")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((REDIS_HOST, REDIS_PORT))
        sock.close()
        
        if result == 0:
            print(f"✅ Port {REDIS_PORT} is open on {REDIS_HOST}")
            return True
        else:
            print(f"❌ Port {REDIS_PORT} is closed on {REDIS_HOST}")
            print(f"Error code: {result}")
            return False
    except Exception as e:
        print(f"❌ Socket error: {e}")
        return False

def test_connection():
    print_header("Testing Redis Connection")
    
    try:
        print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_timeout=3
        )
        
        pong = client.ping()
        print(f"✅ Connection successful! Redis responded with: {pong}")
        
        # Try a simple set/get operation
        client.set("test_key", "test_value")
        value = client.get("test_key")
        print(f"✅ Basic operations working. Set/Get Test: {value}")
        
        # Cleanup
        client.delete("test_key")
        return True
    
    except redis.ConnectionError as e:
        print(f"❌ Connection error: {e}")
        return False
    except redis.AuthenticationError:
        print(f"❌ Authentication error: Wrong password")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def start_redis():
    print_header("Starting Redis Server")
    
    redis_dir = Path(__file__).parent / "redis"
    redis_server = redis_dir / "redis-server.exe"
    
    if not redis_server.exists():
        print(f"❌ Redis server not found at {redis_server}")
        return False
    
    try:
        print("Starting Redis server...")
        
        # Start in a new console window
        process = subprocess.Popen(
            [str(redis_server)],
            cwd=str(redis_dir),
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        
        print(f"✅ Redis server started in a new window (PID: {process.pid})")
        print("⏳ Waiting 2 seconds for Redis to initialize...")
        time.sleep(2)
        return True
    
    except Exception as e:
        print(f"❌ Error starting Redis: {e}")
        return False

def suggest_fixes(installation_ok, process_running, port_open, connection_ok):
    print_header("Diagnosis & Solutions")
    
    if connection_ok:
        print("✅ Redis is working correctly! No action needed.")
        return
    
    if not installation_ok:
        print("❌ Issue: Redis is not properly installed")
        print("   Solution: Run 'python start_redis.py' to download Redis")
        return
    
    if not process_running:
        print("❌ Issue: Redis server is not running")
        print("   Solution: ")
        print("     1. Run 'python start_redis.py' in a new console")
        print("     2. Or manually start Redis from the redis folder")
        return
    
    if not port_open:
        print("❌ Issue: Redis port is not accessible")
        print("   Possible causes:")
        print("     1. Redis is running but bound to a different interface")
        print("     2. Firewall is blocking the connection")
        print("     3. Another application is using the port")
        print("   Solutions:")
        print("     1. Check redis.windows.conf to ensure it's binding to 127.0.0.1")
        print("     2. Try changing the Redis port in your application")
        print("     3. Check Windows firewall settings")
        return
    
    print("❌ Unknown issue with Redis connection")
    print("   Suggestions:")
    print("     1. Restart your computer")
    print("     2. Run 'python redis_diagnostics.py' for detailed diagnostics")
    print("     3. Check if Redis server has any error messages in its console")

if __name__ == "__main__":
    print("\nQuick Redis Connection Checker")
    print("==============================")
    
    installation_ok = check_redis_installation()
    if not installation_ok:
        suggest_fixes(False, False, False, False)
        sys.exit(1)
    
    process_running = check_redis_process()
    
    if not process_running:
        choice = input("\nRedis server is not running. Start it now? (y/n) ").strip().lower()
        if choice.startswith('y'):
            start_redis()
            process_running = check_redis_process()
    
    port_open = check_port()
    connection_ok = False
    
    if port_open:
        connection_ok = test_connection()
    
    suggest_fixes(installation_ok, process_running, port_open, connection_ok)
    
    print("\nDiagnostic complete!")
    
    if connection_ok:
        print("✅ Redis is ready to use with your application!")
    else:
        print("❌ Redis is not working correctly. Please fix the issues above.")
        
    input("\nPress Enter to exit...")
