from fastapi import APIRouter, Depends, HTTPException
from app.services import pwmr_scraper, bwmr_scraper, ewmr_scraper
from app.services.utils import verify_token, log
from app.services.redis_client import RedisClient, PWMR_QUEUE, JOB_STATUS_PREFIX
import threading
import json
from datetime import datetime

router = APIRouter(
    prefix="/cpcb",
    tags=["CPCB Scraper"]
)

@router.post("/pwmr")
def start_pwmr_scraper(user_email: str, token: dict = Depends(verify_token)):
    """Start a PWMR scraping job for the given email"""
    try:
        # Check if user email exists in credentials
        creds = pwmr_scraper.load_credentials()
        if user_email not in creds:
            raise HTTPException(status_code=404, detail=f"No credentials found for {user_email}")
        
        # Create job data
        job_data = {
            "email": user_email,
            "created_at": {
                "timestamp": str(datetime.now())
            }
        }
        
        # Queue job
        redis_client = RedisClient()
        job_id = redis_client.queue_job(PWMR_QUEUE, job_data)
        
        return {"message": "PWM Scraper Started!", "job_id": job_id}
    except Exception as e:
        log(f"❌ Error starting PWMR scraping: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bwmr")
def start_bwmr_scraper(user_email: str, token: dict = Depends(verify_token)):
    """Start a BWMR scraping job for the given email"""
    try:
        # Check if user email exists in credentials
        creds = bwmr_scraper.load_credentials()
        if user_email not in creds:
            raise HTTPException(status_code=404, detail=f"No credentials found for {user_email}")
        
        # Start scraper in background
        scraper_thread = threading.Thread(
            target=bwmr_scraper.start_scraper, 
            args=({"email": user_email},)
        )
        scraper_thread.daemon = True
        scraper_thread.start()
        
        return {"message": "BWM Scraper Started!"}
    except Exception as e:
        log(f"❌ Error starting BWMR scraping: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/ewmr")
def start_ewmr_scraper(user_email: str, token: dict = Depends(verify_token)):
    """Start a EWMR scraping job for the given email"""
    try:
        # Check if user email exists in credentials
        creds = ewmr_scraper.load_credentials()
        if user_email not in creds:
            raise HTTPException(status_code=404, detail=f"No credentials found for {user_email}")
        
        # Start scraper in background
        scraper_thread = threading.Thread(
            target=ewmr_scraper.start_scraper, 
            args=({"email": user_email},)
        )
        scraper_thread.daemon = True
        scraper_thread.start()
        
        return {"message": "EWM Scraper Started!"}
    except Exception as e:
        log(f"❌ Error starting EWMR scraping: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/queue")
def check_job_queue(token: dict = Depends(verify_token)):
    """
    Check jobs in the Redis queue and their statuses
    
    Returns:
    - queued_jobs: Jobs waiting in the queue to be processed
    - job_statuses: Status information about all jobs (queued, processing, completed, failed)
    """
    try:
        redis_client = RedisClient()
        if not redis_client.ensure_connection():
            raise HTTPException(status_code=503, detail="Redis server connection failed")
        
        # Get all job status keys
        status_keys = redis_client.client.keys(f"{JOB_STATUS_PREFIX}*")
        job_statuses = []
        
        # For each status key, get the job details
        for key in status_keys:
            job_id = key.replace(JOB_STATUS_PREFIX, "")
            job_status = redis_client.get_job_status(job_id)
            if job_status:
                job_statuses.append({
                    "job_id": job_id,
                    **job_status
                })
        
        # Get pending jobs in queue (not dequeued yet)
        queue_length = redis_client.client.llen(PWMR_QUEUE)
        queued_jobs = []
        
        # Only fetch if there are jobs in queue
        if queue_length > 0:
            # Use lrange to peek at the queue without removing items
            jobs_data = redis_client.client.lrange(PWMR_QUEUE, 0, queue_length-1)
            
            for job_data in jobs_data:
                try:
                    job = json.loads(job_data)
                    # Extract minimal info to avoid large response
                    queued_jobs.append({
                        "job_id": job.get("job_id", "unknown"),
                        "email": job.get("email", ""),
                        "entity_name": job.get("entity_name", ""),
                        "queued_at": job.get("created_at", {}).get("timestamp", "")
                    })
                except json.JSONDecodeError:
                    log(f"❌ Error decoding job data in queue")
        
        return {
            "queued_jobs": {
                "count": queue_length,
                "jobs": queued_jobs
            },
            "job_statuses": {
                "count": len(job_statuses),
                "jobs": job_statuses
            }
        }
    except Exception as e:
        log(f"❌ Error checking job queue: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error checking job queue: {str(e)}")
