# CPCB Scraper API

A FastAPI-based application for scraping CPCB (Central Pollution Control Board) portals including PWM, BWM, and EWM data with Redis-backed job queuing system.

## Prerequisites

- Python 3.8 or higher
- Chrome browser installed
- Redis (automatically downloaded and configured by the application)
- Access credentials for CPCB portals

## Installation

1. Clone the repository
```bash
git clone https://github.com/dhruv2003/Scrapper
cd scrape
```

2. Create a virtual environment
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

## Configuration

1. Update credentials in `credentials.json` with your CPCB portal login details:
```json
{
    "user@example.com": {
        "password": "yourpassword",
        "entity_name": "Your Entity",
        "plant": "Location",
        "entity_type": "Producer/Brand Owner",
        "team_name": "Team Name",
        "person_name": "Contact Person",
        "recipient_email": ["email@example.com"]
    }
}
```

## Running the Complete System

### 1. Start Redis Server

Redis is required for the job queue system. The application will automatically download and configure Redis for your operating system:

```bash
python start_redis.py
```

This will download Redis if needed (supports Windows, macOS, and Linux) and start the server. The Redis server will run in a separate console window.

To verify Redis is running properly:

```bash
python quick_redis_check.py
```

### 2. Start the FastAPI Server

```bash
uvicorn app.main:app --reload  # For development
# or
uvicorn app.main:app --host 0.0.0.0 --port 8000  # For production
```

### 3. Start Worker Processes

Workers are needed to process the scraping jobs from the Redis queue:

```bash
python start_workers.py
```

You can specify the number of worker processes by passing a number:

```bash
python start_workers.py 2  # Starts 2 worker processes
```

### 4. Redis Maintenance

The Redis maintenance script helps you manage your Redis database:

```bash
# Run maintenance in a loop (cleans up failed jobs older than 5 minutes)
python redis_maintenance.py

# Clear all transactions and jobs from Redis
python redis_maintenance.py --clear-all

# Clean up failed jobs with custom age threshold
python redis_maintenance.py --clean-failed --age 10
```

Available options:
- `--clear-all`: Removes all jobs and transactions from Redis database
- `--clean-failed`: Performs a one-time cleanup of failed jobs
- `--age N`: Sets the age threshold in minutes (default: 5)
- `--loop`: Runs the maintenance process in a continuous loop (default behavior)

## Using the Application

### API Documentation

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Authentication

1. Login using the default admin credentials:
- Username: admin@cpcb.com
- Password: admin123

2. Use the received JWT token in the Authorization header for subsequent requests

### Queuing Scraping Jobs

There are two ways to start scraping jobs:

1. **Via API endpoints**:
   - `/cpcb/pwmr?user_email=user@example.com` - for Plastic Waste Management Rules
   - `/cpcb/bwmr?user_email=user@example.com` - for Bio-Medical Waste Management Rules
   - `/cpcb/ewmr?user_email=user@example.com` - for E-Waste Management Rules
   
   Or alternatively:
   
   - POST to `/pwmr/scrape` with JSON body: `{"email": "user@example.com"}`

2. **Using the test script**:
   ```bash
   python test_job_queue.py
   ```
   This script guides you through the process of queuing and monitoring jobs.

### Monitoring

#### 1. Job Queue Viewer (Web UI)

Open `job_queue_viewer.html` in your browser to see all jobs and their statuses.
- Login with admin@cpcb.com / admin123
- Click "Refresh Data" to update the view

#### 2. Live Logs

- Web UI: Open `log_viewer.html` in your browser for real-time logs
- API: Access the log stream at http://localhost:8000/logs/stream
- Console: Watch the worker and server console output

#### 3. Queue Status API

- GET `/cpcb/queue` - Shows all queued jobs and their statuses
- GET `/pwmr/status/{job_id}` - Shows status of a specific job
- GET `/pwmr/jobs` - Lists all jobs

## System Architecture

The application uses a distributed architecture with the following components:

### Core Components
1. **FastAPI Server**
   - Provides REST API endpoints for job submission and monitoring
   - Handles authentication and authorization
   - Exposes Swagger documentation

2. **Redis Queue**
   - Central job queue for storing pending scraping tasks
   - Maintains job statuses and results
   - Enables communication between API and workers

3. **Worker Processes**
   - Multiple independent processes consuming jobs from the queue
   - Each worker handles one scraping job at a time
   - Implements retry logic for failed jobs

4. **Selenium Scraper Engine**
   - Handles browser automation for data extraction
   - Manages Chrome WebDriver sessions
   - Implements portal-specific scraping logic

### Data Flow
1. Client submits scraping request via API
2. API validates request and adds job to Redis queue
3. Available worker picks up job from queue
4. Worker executes scraping task and updates job status
5. Client retrieves results via API or web interface

### Monitoring System
- Real-time log streaming
- Web-based job queue monitoring
- Job status tracking and reporting

### Security
- JWT-based authentication
- Secure credential storage
- Rate limiting on API endpoints

```
                 ┌─────────────────┐
                 │                 │
  ┌─────────────▶│  FastAPI Server │◀────────┐
  │              │                 │         │
  │              └────────┬────────┘         │
  │                       │                  │
  │                       ▼                  │
  │              ┌─────────────────┐         │
  │              │                 │         │
  │  Client      │   Redis Queue   │         │  API Requests
  │  Requests    │                 │         │
  │              └────────┬────────┘         │
  │                       │                  │
  │                       ▼                  │
  │              ┌─────────────────┐         │
  │              │  Worker Pool    │         │
  └──────────────┤  (Selenium)     ├─────────┘
                 │                 │
                 └─────────────────┘
```

# MongoDB Scraper Project

## Installation

When you encounter pip installation issues like:

```
ImportError: cannot import name 'RequirementInformation' from 'pip._vendor.resolvelib.structs'
```

This means your virtual environment is corrupted. Follow these steps to fix it:

### Method 1: Using the setup script

1. Run the setup script:
   ```
   python setup.py
   ```

2. Activate the new virtual environment:
   - Windows: `venv\Scripts\activate`
   - Unix/Mac: `source venv/bin/activate`

### Method 2: Manual setup

1. Delete the existing virtual environment:
   - Windows: `rmdir /s /q venv`
   - Unix/Mac: `rm -rf venv`

2. Create a new virtual environment:
   ```
   python -m venv venv
   ```

3. Activate the virtual environment:
   - Windows: `venv\Scripts\activate`
   - Unix/Mac: `source venv/bin/activate`

4. Install pymongo:
   ```
   python -m pip install --upgrade pip
   python -m pip install pymongo
   ```

5. Install other requirements:
   ```
   python -m pip install -r requirements.txt
   ```

## Usage

Once your environment is set up, you can use the MongoDB connection utilities to interact with your database.
