import redis
import time
import json
import argparse
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

def clear_all_transactions(client):
    """Clear all jobs and transactions from Redis"""
    try:
        log("Starting to clear all transactions from Redis...")
        
        # Get all keys related to jobs
        job_keys = client.keys(f"{JOB_STATUS_PREFIX}*")
        queue_keys = client.keys("*_jobs")  # Pattern for job queues like pwmr_jobs
        transaction_keys = client.keys("*_queue*")  # Pattern for any queue-related keys
        
        # Combine all keys and remove duplicates
        all_keys = list(set(job_keys + queue_keys + transaction_keys))
        
        if not all_keys:
            log("No transaction or job keys found to clear")
            return 0
        
        log(f"Found {len(all_keys)} keys to delete")
        
        # Delete all keys in batches to avoid blocking Redis for too long
        batch_size = 100
        deleted_count = 0
        
        for i in range(0, len(all_keys), batch_size):
            batch = all_keys[i:i + batch_size]
            if batch:
                deleted = client.delete(*batch)
                deleted_count += deleted
                log(f"Deleted batch of {deleted} keys")
                # Small pause to prevent Redis overload
                time.sleep(0.1)
        
        log(f"Successfully cleared {deleted_count} keys from Redis")
        return deleted_count
    
    except Exception as e:
        log(f"Error clearing transactions: {e}")
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

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Redis Maintenance Tool')
    
    # Add arguments
    parser.add_argument('--clear-all', action='store_true', 
                        help='Clear all jobs and transactions from Redis')
    parser.add_argument('--clean-failed', action='store_true',
                        help='Clean up failed jobs older than specified minutes')
    parser.add_argument('--age', type=int, default=5,
                        help='Age in minutes for job cleanup (default: 5)')
    parser.add_argument('--loop', action='store_true',
                        help='Run the maintenance process in a continuous loop')
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    
    # Handle command line arguments
    if args.clear_all:
        client = connect_to_redis()
        if client:
            clear_all_transactions(client)
            client.close()
    elif args.clean_failed:
        client = connect_to_redis()
        if client:
            deleted = clean_failed_jobs(client, args.age)
            log(f"Cleaned up {deleted} failed job(s)")
            client.close()
    elif args.loop:
        main_cleanup_loop()
    else:
        # Default behavior - run the maintenance loop
        main_cleanup_loop()
