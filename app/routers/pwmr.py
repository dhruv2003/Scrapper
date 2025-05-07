from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.security import HTTPAuthorizationCredentials
from app.services.redis_client import RedisClient, PWMR_QUEUE, JOB_STATUS_PREFIX
from app.services.utils import log, verify_token
from app.services.pwmr_scraper import load_credentials
from typing import Optional
from datetime import datetime
import json

router = APIRouter(
    prefix="/pwmr",
    tags=["PWMR Scraper"]
)

@router.post("/scrape")
async def pwmr_scrape(payload: dict, token: dict = Depends(verify_token)):
    """
    Queue a new PWMR scraping job
    
    Payload should include:
    - email: The email to use for login
    - password: The password to use for login (optional if in credentials file)
    """
    try:
        email = payload.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="Email is required")
            
        # If password not provided, try to load from credentials file
        if not payload.get("password"):
            creds = load_credentials()
            if email in creds:
                for key, value in creds[email].items():
                    if key not in payload:
                        payload[key] = value
                log(f"📋 Loaded credentials for {email}")
            else:
                raise HTTPException(status_code=404, detail=f"No credentials found for {email}")
        
        # Add timestamp to job
        payload["created_at"] = {
            "timestamp": str(datetime.now())
        }
        
        # Queue the job
        redis_client = RedisClient()
        job_id = redis_client.queue_job(PWMR_QUEUE, payload)
        
        return {
            "message": "PWMR scraping job queued successfully",
            "job_id": job_id
        }
    except ConnectionError as e:
        log(f"❌ Redis connection error: {e}")
        raise HTTPException(status_code=503, detail="Queue service unavailable")
    except Exception as e:
        log(f"❌ Error queueing job: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/{job_id}")
async def get_job_status(job_id: str, token: dict = Depends(verify_token)):
    """Get the status of a PWMR scraping job"""
    try:
        redis_client = RedisClient()
        status = redis_client.get_job_status(job_id)
        
        if not status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
            
        return {
            "job_id": job_id,
            "status": status
        }
    except Exception as e:
        log(f"❌ Error getting job status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/jobs")
async def list_jobs(token: dict = Depends(verify_token)):
    """List all job statuses"""
    try:
        redis_client = RedisClient()
        keys = redis_client.client.keys(f"{JOB_STATUS_PREFIX}*")
        
        jobs = []
        for key in keys:
            job_id = key.replace(JOB_STATUS_PREFIX, "")
            status = redis_client.get_job_status(job_id)
            if status:
                jobs.append({
                    "job_id": job_id,
                    **status
                })
                
        return {
            "jobs": jobs,
            "count": len(jobs)
        }
    except Exception as e:
        log(f"❌ Error listing jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

