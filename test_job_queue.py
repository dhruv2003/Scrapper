"""
Test Job Queue

This script helps test the Redis job queue by:
1. Logging in to get an auth token
2. Queueing a scraping job
3. Showing the job status
"""

import requests
import json
import time

# API Base URL
API_BASE = "http://localhost:8000"

def login():
    """Login to get an access token"""
    print("Logging in...")
    response = requests.post(
        f"{API_BASE}/auth/login",
        data={
            "username": "admin@cpcb.com",
            "password": "admin123"
        }
    )
    
    if response.status_code != 200:
        print(f"❌ Login failed with status code {response.status_code}")
        print(response.text)
        return None
    
    token = response.json()["access_token"]
    print("✅ Login successful!")
    return token

def queue_job(token, email):
    """Queue a new scraping job"""
    print(f"\nQueueing job for email: {email}")
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Try the CPCB router endpoint
    response = requests.post(
        f"{API_BASE}/cpcb/pwmr",
        params={"user_email": email},
        headers=headers
    )
    
    if response.status_code != 200:
        print(f"❌ Failed to queue job with status code {response.status_code}")
        print(response.text)
        
        # Try the alternative PWMR router endpoint
        print("\nTrying alternative endpoint...")
        response = requests.post(
            f"{API_BASE}/pwmr/scrape",
            json={"email": email},
            headers=headers
        )
        
        if response.status_code != 200:
            print(f"❌ Alternative endpoint failed with status code {response.status_code}")
            print(response.text)
            return None
    
    job_data = response.json()
    job_id = job_data.get("job_id", "unknown")
    print(f"✅ Job queued successfully with ID: {job_id}")
    return job_id

def check_queue(token):
    """Check all jobs in the queue"""
    print("\nChecking queue status...")
    
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{API_BASE}/cpcb/queue", headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Failed to check queue with status code {response.status_code}")
        print(response.text)
        return
    
    queue_data = response.json()
    
    # Print queued jobs
    queued_jobs = queue_data.get("queued_jobs", {"count": 0, "jobs": []})
    print(f"\n📋 Queued Jobs: {queued_jobs['count']}")
    for job in queued_jobs.get("jobs", []):
        print(f"   - Job ID: {job.get('job_id')}")
        print(f"     Email: {job.get('email')}")
        print(f"     Queued At: {job.get('queued_at')}")
    
    # Print job statuses
    job_statuses = queue_data.get("job_statuses", {"count": 0, "jobs": []})
    print(f"\n📊 Job Statuses: {job_statuses['count']}")
    for job in job_statuses.get("jobs", []):
        status = job.get("status", "unknown")
        email = job.get("email", "unknown")
        message = job.get("message", "")
        print(f"   - Job ID: {job.get('job_id')}")
        print(f"     Email: {email}")
        print(f"     Status: {status}")
        print(f"     Message: {message}")

def monitor_job(token, job_id, interval=5, max_checks=12):
    """Monitor a specific job until completion or failure"""
    print(f"\nMonitoring job {job_id}...")
    
    headers = {"Authorization": f"Bearer {token}"}
    checks = 0
    
    while checks < max_checks:
        response = requests.get(
            f"{API_BASE}/pwmr/status/{job_id}", 
            headers=headers
        )
        
        if response.status_code != 200:
            print(f"❌ Failed to check job status with code {response.status_code}")
            print(response.text)
            return
        
        status_data = response.json()
        status = status_data.get("status", {}).get("status", "unknown")
        message = status_data.get("status", {}).get("message", "")
        
        print(f"⏳ Status: {status} - {message}")
        
        if status in ["completed", "failed"]:
            print(f"✅ Job finished with status: {status}")
            break
            
        checks += 1
        time.sleep(interval)
    
    if checks >= max_checks:
        print("⚠️ Monitoring timed out. Job may still be processing.")

def main():
    # 1. Login to get token
    token = login()
    if not token:
        print("Cannot proceed without authentication token.")
        return
    
    # 2. Check current queue status
    check_queue(token)
    
    # 3. Ask user if they want to queue a job
    print("\nAvailable test emails:")
    print("1. eprp.a2am@uflexltd.com")
    print("2. eprp.a1@uflexltd.com")
    print("3. eprp.j1@uflexltd.com")
    
    choice = input("\nEnter email number to queue (or press Enter to skip): ").strip()
    
    if choice:
        emails = [
            "eprp.a2am@uflexltd.com",
            "eprp.a1@uflexltd.com",
            "eprp.j1@uflexltd.com"
        ]
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(emails):
                email = emails[idx]
                job_id = queue_job(token, email)
                
                if job_id:
                    # 4. Monitor the job
                    monitor = input("\nMonitor job progress? (y/n): ").strip().lower()
                    if monitor.startswith('y'):
                        monitor_job(token, job_id)
            else:
                print("Invalid selection.")
        except ValueError:
            print("Please enter a valid number.")
    
    # 5. Check final queue status
    check_queue(token)

if __name__ == "__main__":
    main()
