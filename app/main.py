from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import auth, cpcb, logs, pwmr
import logging
import app.models.pwmr
from app.db import engine, Base

logging.basicConfig(level=logging.DEBUG)

app = FastAPI(
    title="CPCB Scraper API",
    description="Scraping PWM/BWM/EWM portals via Selenium + Manual Login",
    version="6.9"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Create all tables
Base.metadata.create_all(bind=engine)

# Register routers - fixed order and proper configuration
app.include_router(auth.router)
app.include_router(cpcb.router)
app.include_router(logs.router)
app.include_router(pwmr.router)

@app.get("/")
async def root():
    """Root endpoint returning API health status"""
    return {
        "status": "ok", 
        "message": "CPCB Scraper API is running. Go to /docs for API documentation."
    }