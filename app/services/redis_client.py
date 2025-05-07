import json
import redis
import uuid
import time
from datetime import datetime
from app.services.utils import log

# Redis configuration - DO NOT CHANGE if your Redis connection is working
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

# Queue names
PWMR_QUEUE = "pwmr_jobs"
JOB_STATUS_PREFIX = "job_status:"

class RedisClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisClient, cls).__new__(cls)
            cls._instance.client = None
            # Don't attempt connection here - defer until actual use
        return cls._instance

    def ensure_connection(self):
        """Ensure Redis connection is active, reconnect if needed"""
        if self.client is None:
            try:
                log(f"📡 Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
                self.client = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    db=REDIS_DB,
                    password=REDIS_PASSWORD,
                    decode_responses=True,
                    socket_timeout=10,
                    socket_connect_timeout=5,
                    health_check_interval=30
                )
                
                # Test connection with ping
                if self.client.ping():
                    log(f"✅ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
                    return True
            except redis.ConnectionError as e:
                log(f"❌ Redis connection error: {e}")
                self.client = None
            except Exception as e:
                log(f"❌ Redis error: {type(e).__name__}: {e}")
                self.client = None
            return False
        
        # Check if existing connection is still valid
        try:
            self.client.ping()
            return True
        except:
            log("🔄 Redis connection lost. Attempting to reconnect...")
            self.client = None
            return self.ensure_connection()  # Recursive call to establish new connection

    def queue_job(self, queue_name, job_data):
        """Queue a new job and return the job_id"""
        if not self.ensure_connection():
            raise ConnectionError("Redis connection not available")
        
        try:
            job_id = str(uuid.uuid4())
            job_data["job_id"] = job_id
            
            # Set initial job status
            status_key = f"{JOB_STATUS_PREFIX}{job_id}"
            
            # Enhanced status with more metadata
            self.client.hset(status_key, mapping={
                "status": "queued",
                "message": "Job queued, waiting for worker",
                "created_at": json.dumps(job_data.get("created_at", "")),
                "email": job_data.get("email", ""),
                "entity_name": job_data.get("entity_name", ""),
                "entity_type": job_data.get("entity_type", ""),
                "queue": queue_name
            })
            
            # Set expiry for status (24 hours)
            self.client.expire(status_key, 86400)
            
            # Push to job queue
            self.client.lpush(queue_name, json.dumps(job_data))
            
            log(f"✅ Job {job_id} queued successfully in {queue_name}")
            return job_id
        except redis.RedisError as e:
            log(f"❌ Redis error during queue_job: {e}")
            self.client = None  # Reset connection for next attempt
            raise ConnectionError(f"Redis operation failed: {e}")
    
    def get_job(self, queue_name, timeout=0):
        """Get a job from the queue with blocking"""
        if not self.ensure_connection():
            raise ConnectionError("Redis connection not available")
        
        try:
            result = self.client.brpop(queue_name, timeout)
            if result:
                try:
                    return json.loads(result[1])
                except json.JSONDecodeError as e:
                    log(f"❌ Error decoding job data: {e}")
                    return None
            return None
        except redis.RedisError as e:
            log(f"❌ Redis error during get_job: {e}")
            self.client = None  # Reset connection for next attempt
            raise ConnectionError(f"Redis operation failed: {e}")
    
    def update_job_status(self, job_id, status, message):
        """Update job status in Redis"""
        if not self.ensure_connection():
            raise ConnectionError("Redis connection not available")
        
        try:
            status_key = f"{JOB_STATUS_PREFIX}{job_id}"
            pipeline = self.client.pipeline()
            pipeline.hset(status_key, "status", status)
            pipeline.hset(status_key, "message", message)
            pipeline.hset(status_key, "updated_at", json.dumps({"timestamp": str(datetime.now())}))
            pipeline.execute()
            return True
        except redis.RedisError as e:
            log(f"❌ Redis error during update_job_status: {e}")
            self.client = None  # Reset connection for next attempt
            raise ConnectionError(f"Redis operation failed: {e}")
        
    def get_job_status(self, job_id):
        """Get job status from Redis"""
        if not self.ensure_connection():
            raise ConnectionError("Redis connection not available")
        
        try:
            status_key = f"{JOB_STATUS_PREFIX}{job_id}"
            if not self.client.exists(status_key):
                return None
            
            return self.client.hgetall(status_key)
        except redis.RedisError as e:
            log(f"❌ Redis error during get_job_status: {e}")
            self.client = None  # Reset connection for next attempt
            raise ConnectionError(f"Redis operation failed: {e}")
