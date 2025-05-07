# app/services/utils.py

from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
import os
from datetime import datetime

# Logging
log_buffer = []
scraper_status = {
    "status": "idle",
    "last_message": ""
}

def log(msg: str):
    print(msg)
    log_buffer.append(msg)
    scraper_status["last_message"] = msg

def set_scraper_status(email, status):
    """Update the status of a specific scraper job"""
    scraper_status[email] = {
        "status": status,
        "timestamp": str(datetime.now())
    }
    log(f"Status update for {email}: {status}")

def get_logger():
    return log

# JWT Verification
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "secret")
ALGORITHM = "HS256"

# HTTP Bearer for token validation
security = HTTPBearer()

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verify JWT token from Authorization header
    """
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"}
        )
