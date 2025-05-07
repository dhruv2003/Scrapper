import os
import sys
import multiprocessing
from app.worker import process_jobs
from app.services.redis_client import PWMR_QUEUE
from app.services.utils import log

def start_worker(queue_name):
    """Start a worker process for the specified queue"""
    log(f"🚀 Starting worker for queue: {queue_name}")
    process_jobs(queue_name)

if __name__ == "__main__":
    # Set to spawn mode to avoid issues with fork in Windows
    multiprocessing.set_start_method('spawn', force=True)
    
    # Number of worker processes - adjust based on your server capability
    num_workers = 2
    
    workers = []
    
    try:
        log(f"🚀 Starting {num_workers} worker processes")
        
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
        
        # Keep the main process running
        for worker in workers:
            worker.join()
            
    except KeyboardInterrupt:
        log("👋 Shutdown signal received")
        
        # Terminate all workers
        for i, worker in enumerate(workers):
            if worker.is_alive():
                log(f"Terminating worker {i+1}")
                worker.terminate()
                
        log("👋 All workers terminated")
        sys.exit(0)
