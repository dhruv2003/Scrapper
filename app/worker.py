import time
import traceback
from app.services.redis_client import RedisClient, PWMR_QUEUE
from app.services.pwmr_scraper import start_scraper, load_credentials
from app.services.utils import log
from sqlalchemy.exc import OperationalError

def process_jobs(queue_name=PWMR_QUEUE):
    """
    Continuous job processor for PWMR scraping jobs
    """
    redis_client = None
    reconnect_delay = 5  # Start with 5 seconds delay
    
    while True:
        try:
            # Ensure Redis client is available
            if redis_client is None:
                redis_client = RedisClient()
                if not redis_client.ensure_connection():
                    log(f"❌ Failed to connect to Redis. Retrying in {reconnect_delay} seconds...")
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 60)  # Exponential backoff up to 60 seconds
                    continue
                else:
                    reconnect_delay = 5  # Reset reconnect delay on successful connection
                    log(f"📋 Worker connected - waiting for jobs in queue: {queue_name}")
            
            # Get job with timeout (blocks for 1 second, then checks for interruptions)
            try:
                job = redis_client.get_job(queue_name, timeout=1)
            except ConnectionError:
                log("❌ Lost connection to Redis while getting job")
                redis_client = None
                continue
                
            if not job:
                continue
                
            job_id = job.get("job_id")
            email = job.get("email")
            
            log(f"✨ Processing job {job_id} for {email}")
            
            # Update job status
            try:
                redis_client.update_job_status(job_id, "processing", "Scraping in progress...")
            except ConnectionError:
                log("❌ Lost connection to Redis while updating job status")
                redis_client = None
                continue
            
            # Load credentials if not in job data
            if not job.get("password"):
                creds = load_credentials()
                if email in creds:
                    for key, value in creds[email].items():
                        if key not in job:
                            job[key] = value
                    log(f"📋 Loaded credentials for {email}")
                else:
                    try:
                        redis_client.update_job_status(job_id, "failed", f"No credentials found for {email}")
                    except ConnectionError:
                        log("❌ Lost connection to Redis while updating job status")
                        redis_client = None
                    log(f"❌ No credentials found for {email}")
                    continue
            
            # Process the job
            try:
                result = start_scraper(job)
                try:
                    redis_client.update_job_status(
                        job_id, 
                        "completed", 
                        f"Scraping completed successfully. Result ID: {result.get('job_id')}"
                    )
                except ConnectionError:
                    log("❌ Lost connection to Redis while updating job status")
                    redis_client = None
                log(f"✅ Job {job_id} completed successfully")
                
            except OperationalError as e:
                # Database connection error
                try:
                    redis_client.update_job_status(
                        job_id, 
                        "failed", 
                        f"Database error: {str(e)}"
                    )
                except ConnectionError:
                    log("❌ Lost connection to Redis while updating job status")
                    redis_client = None
                log(f"❌ Job {job_id} failed due to database error: {str(e)}")
                # Sleep a bit longer on database errors
                time.sleep(10)
                
            except Exception as e:
                error_details = traceback.format_exc()
                try:
                    redis_client.update_job_status(
                        job_id, 
                        "failed", 
                        f"Scraping failed: {str(e)}"
                    )
                except ConnectionError:
                    log("❌ Lost connection to Redis while updating job status")
                    redis_client = None
                log(f"❌ Job {job_id} failed: {str(e)}")
                log(f"Error details: {error_details}")
                
        except KeyboardInterrupt:
            log("👋 Worker stopping due to keyboard interrupt")
            break
            
        except Exception as e:
            log(f"❌ Unexpected worker error: {e}")
            # Sleep briefly to prevent CPU spinning on repeated errors
            time.sleep(5)
            
    log("👋 Worker stopped")

if __name__ == "__main__":
    process_jobs()
