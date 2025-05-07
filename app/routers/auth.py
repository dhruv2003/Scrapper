from fastapi import APIRouter, HTTPException, status, Form
from jose import jwt
from datetime import datetime, timedelta

from app.services.utils import SECRET_KEY, ALGORITHM

# Create router with proper prefix
router = APIRouter(
    prefix="/auth",
    tags=["Authentication"]
)

users_db = {
    "admin@cpcb.com": {
        "username": "admin@cpcb.com",
        "password": "admin123",
    }
}

@router.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    """
    Authenticate a user and return a JWT token
    """
    user = users_db.get(username)
    if not user or user["password"] != password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Create access token with 24 hours expiry
    expire = datetime.utcnow() + timedelta(hours=24)
    to_encode = {"sub": username, "exp": expire}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    return {"access_token": encoded_jwt, "token_type": "bearer"}
