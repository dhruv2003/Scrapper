"""
Quick Redis Check Tool

This script performs a simple check to verify Redis is running correctly.
"""

import redis
import sys
import platform

def test_redis_connection():
    """Test if Redis is running and accepting connections"""
    try:
        # Connect to Redis
        r = redis.Redis(host='localhost', port=6379, db=0, socket_timeout=5)
        
        # Ping Redis
        if r.ping():
            print("✅ Redis is running and responding!")
            
            # Get Redis info
            info = r.info()
            print(f"\nRedis Server Information:")
            print(f"- Version: {info.get('redis_version', 'Unknown')}")
            print(f"- OS: {info.get('os', 'Unknown')}")
            print(f"- Uptime: {info.get('uptime_in_seconds', 0)} seconds")
            print(f"- Memory used: {info.get('used_memory_human', 'Unknown')}")
            print(f"- Clients connected: {info.get('connected_clients', 0)}")
            
            # Test set and get operations
            r.set('test_key', 'Redis is working!')
            value = r.get('test_key')
            if value and value.decode('utf-8') == 'Redis is working!':
                print("\n✅ Basic Redis operations (SET/GET) are working correctly")
                r.delete('test_key')
            else:
                print("\n❌ Failed to verify basic Redis operations")
                return False
            
            return True
        else:
            print("❌ Redis did not respond to PING command")
            return False
    except redis.exceptions.ConnectionError:
        print("❌ Could not connect to Redis. Is the Redis server running?")
        if platform.system().lower() == "windows":
            print("\nTry starting Redis with: python start_redis.py")
        else:
            print("\nTry starting Redis with: python start_redis.py")
            print("Or if installed system-wide: sudo systemctl start redis")
        return False
    except Exception as e:
        print(f"❌ Error checking Redis: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Testing Redis connection...")
    
    if test_redis_connection():
        print("\n✅ All checks passed! Redis is working correctly.")
        sys.exit(0)
    else:
        print("\n❌ Redis check failed.")
        sys.exit(1)
