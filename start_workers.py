"""
Worker launcher for PWMR scraping jobs.
Run this script to start worker processes that will process scraping jobs.
"""

import os
import sys
import multiprocessing
import threading
import time
from app.worker import process_jobs
from app.services.redis_client import RedisClient, PWMR_QUEUE, JOB_STATUS_PREFIX
from app.services.utils import log

def start_worker(queue_name):
    """Start a worker process for the specified queue"""
    log(f"🚀 Starting worker for queue: {queue_name}")
    process_jobs(queue_name)

def monitor_queue_size(stop_event):
    """
    Monitor the queue size periodically and print status updates
    This runs in a separate thread to provide queue information
    """
    redis_client = RedisClient()
    last_count = 0
    last_job_ids = set()
    
    while not stop_event.is_set():
        try:
            if redis_client.ensure_connection():
                # Get current queue length
                queue_count = redis_client.client.llen(PWMR_QUEUE)
                
                # Get all job status keys
                all_jobs = redis_client.client.keys(f"{JOB_STATUS_PREFIX}*")
                all_job_ids = {key.replace(JOB_STATUS_PREFIX, "") for key in all_jobs}
                
                # Find new job IDs since last check
                new_job_ids = all_job_ids - last_job_ids
                
                # Print if queue size changed
                if queue_count != last_count or new_job_ids:
                    log(f"📊 Current queue size: {queue_count}")
                    
                    # Print info about new jobs
                    for job_id in new_job_ids:
                        job_info = redis_client.get_job_status(job_id)
                        if job_info:
                            status = job_info.get('status', 'unknown')
                            email = job_info.get('email', 'unknown')
                            log(f"📥 New job detected - ID: {job_id} | Email: {email} | Status: {status}")
                
                last_count = queue_count
                last_job_ids = all_job_ids
        except Exception as e:
            pass  # Silently handle errors in the monitoring thread
            
        # Sleep between checks
        time.sleep(5)

if __name__ == "__main__":
    # Set to spawn mode to avoid issues with fork in Windows
    multiprocessing.set_start_method('spawn', force=True)
    
    # Number of worker processes - default to 1, can be specified via command line
    num_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    
    log(f"🔄 CPCB Scraper Worker Manager (v1.1)")
    log(f"🔄 Starting {num_workers} worker process(es)")
    
    workers = []
    
    try:
        # Start a thread to monitor the queue
        stop_event = threading.Event()
        monitor_thread = threading.Thread(target=monitor_queue_size, args=(stop_event,))
        monitor_thread.daemon = True
        monitor_thread.start()
        log(f"📊 Queue monitoring started")
        
        # Start worker processes
        for i in range(num_workers):
            worker = multiprocessing.Process(
                target=start_worker,
                args=(PWMR_QUEUE,),
                name=f"PWMRWorker-{i+1}"
            )
            worker.daemon = True
            worker.start()
            workers.append(worker)
            log(f"✅ Worker {i+1} started with PID {worker.pid}")
        
        log("⌛ Workers are now processing jobs from the queue...")
        log("👀 Press Ctrl+C to stop all workers")
        
        # Keep the main process running
        for worker in workers:
            worker.join()
            
    except KeyboardInterrupt:
        log("\n👋 Shutdown signal received")
        
        # Stop the monitoring thread
        stop_event.set()
        
        # Terminate all workers
        for i, worker in enumerate(workers):
            if worker.is_alive():
                log(f"🛑 Terminating worker {i+1}")
                worker.terminate()
                
        log("👋 All workers terminated")
        sys.exit(0)
