"""
Redis Diagnostics Tool

This script helps diagnose Redis connection issues by testing:
1. Basic connection to Redis server
2. Connection settings verification
3. Queue operations testing
"""

import redis
import socket
import time
import os
import sys
import subprocess
import json
from datetime import datetime

# Import settings from our client
sys.path.append(os.path.dirname(__file__))
from app.services.redis_client import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD, PWMR_QUEUE

def print_header(text):
    """Print a formatted header"""
    print("\n" + "=" * 60)
    print(f"  {text}")
    print("=" * 60)

def check_redis_process():
    """Check if Redis server is running on the system"""
    print_header("Checking Redis Process")
    
    redis_running = False
    
    try:
        # For Windows
        if os.name == 'nt':
            output = subprocess.check_output('tasklist', shell=True).decode('utf-8')
            redis_running = 'redis-server.exe' in output
            print(f"Redis process found in Windows tasklist: {'✅ Yes' if redis_running else '❌ No'}")
        # For Linux/Mac
        else:
            output = subprocess.check_output(['ps', 'aux']).decode('utf-8')
            redis_running = 'redis-server' in output
            print(f"Redis process found in process list: {'✅ Yes' if redis_running else '❌ No'}")
    except Exception as e:
        print(f"❌ Error checking Redis process: {e}")
    
    return redis_running

def check_port_availability():
    """Check if Redis port is open and accepting connections"""
    print_header(f"Checking Port {REDIS_PORT} on {REDIS_HOST}")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((REDIS_HOST, REDIS_PORT))
        
        if result == 0:
            print(f"✅ Port {REDIS_PORT} is open on {REDIS_HOST}")
            port_open = True
        else:
            print(f"❌ Port {REDIS_PORT} is closed on {REDIS_HOST}")
            port_open = False
        
        sock.close()
    except socket.gaierror:
        print(f"❌ Hostname {REDIS_HOST} could not be resolved")
        port_open = False
    except socket.error as e:
        print(f"❌ Socket error: {e}")
        port_open = False
        
    return port_open

def test_redis_connection():
    """Test basic Redis connection"""
    print_header("Testing Redis Connection")
    
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_timeout=5
        )
        
        pong = client.ping()
        print(f"✅ Redis ping successful: {pong}")
        
        info = client.info()
        print(f"✅ Redis version: {info.get('redis_version')}")
        print(f"✅ Redis mode: {info.get('redis_mode', 'standalone')}")
        print(f"✅ Connected clients: {info.get('connected_clients')}")
        
        return client
    except redis.ConnectionError as e:
        print(f"❌ Redis connection error: {e}")
    except redis.AuthenticationError:
        print("❌ Redis authentication failed. Check your password.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    return None

def test_queue_operations(client):
    """Test basic queue operations"""
    print_header("Testing Queue Operations")
    
    if not client:
        print("❌ Cannot test queue operations: No Redis connection")
        return
    
    test_queue = f"test_queue_{int(time.time())}"
    test_id = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    try:
        # Push test message
        test_data = {
            "id": test_id,
            "timestamp": str(datetime.now()),
            "test": True
        }
        
        client.lpush(test_queue, json.dumps(test_data))
        print(f"✅ Successfully pushed test data to {test_queue}")
        
        # Get test message
        result = client.brpop(test_queue, timeout=1)
        if result:
            retrieved_data = json.loads(result[1])
            print(f"✅ Successfully retrieved test data: {retrieved_data['id']}")
            
            if retrieved_data['id'] == test_id:
                print("✅ Data integrity verified")
            else:
                print("❌ Data integrity issue - ID mismatch")
        else:
            print("❌ Failed to retrieve test data")
        
        # Check the actual queue
        queue_length = client.llen(PWMR_QUEUE)
        print(f"ℹ️ Current length of {PWMR_QUEUE} queue: {queue_length}")
        
    except Exception as e:
        print(f"❌ Error during queue operations: {e}")

def print_recommendations():
    """Print recommendations based on findings"""
    print_header("Recommendations")
    
    print("If you're still having Redis connection issues:")
    print("")
    print("1. Check Redis server configuration:")
    print("   - Ensure Redis is running on the correct host and port")
    print("   - Check bind settings in redis.conf (should be 0.0.0.0 or specific IP)")
    print("   - Verify that Redis password is configured correctly if using authentication")
    print("")
    print("2. For Docker/containerized environments:")
    print("   - Ensure host ports are properly mapped to container ports")
    print("   - Check network settings to ensure the worker can reach Redis")
    print("")
    print("3. General troubleshooting:")
    print("   - Try connecting with redis-cli to validate connection")
    print("   - Check firewall settings - allow connections on port 6379")
    print("   - Inspect Redis logs for potential issues")
    print("")
    print("4. If using Redis on Windows:")
    print("   - Make sure to start Redis with 'redis-server.exe'")
    print("   - Check that the Windows service is running if installed as a service")

if __name__ == "__main__":
    print("\nRedis Diagnostics Tool")
    print("=============================================")
    print(f"Testing Redis at {REDIS_HOST}:{REDIS_PORT} (DB: {REDIS_DB})")
    print("Authentication: " + ("Enabled" if REDIS_PASSWORD else "Disabled"))
    print("=============================================\n")
    
    process_running = check_redis_process()
    port_open = check_port_availability()
    
    client = test_redis_connection()
    if client:
        test_queue_operations(client)
    
    print_recommendations()
    
    print("\n=============================================")
    print("Diagnostics Complete")
    print("=============================================\n")
