# API Endpoints Documentation

## Authentication

### Login
- **URL**: `/auth/login`
- **Method**: `POST`
- **Form Data**:
  - username: string
  - password: string
- **Response**:
```json
{
    "access_token": "jwt_token",
    "token_type": "bearer"
}
```

## CPCB Scrapers

All scraper endpoints require Bearer token authentication.

### PWM Scraper
- **URL**: `/cpcb/pwmr`
- **Method**: `POST`
- **Query Parameters**:
  - user_email: string
- **Response**:
```json
{
    "message": "PWM Scraper Started!"
}
```

### BWM Scraper
- **URL**: `/cpcb/bwmr`
- **Method**: `POST`
- **Query Parameters**:
  - user_email: string
- **Response**:
```json
{
    "message": "BWM Scraper Started!"
}
```

### EWM Scraper
- **URL**: `/cpcb/ewmr`
- **Method**: `POST`
- **Query Parameters**:
  - user_email: string
- **Response**:
```json
{
    "message": "EWM Scraper Started!"
}
```

## Job Queue Management

### Get All Jobs
- **URL**: `/cpcb/queue`
- **Method**: `GET`
- **Description**: Returns all jobs in the queue with their statuses
- **Response**:
```json
[
    {
        "job_id": "job123",
        "email": "user@example.com",
        "status": "completed",
        "message": "Scrape completed successfully",
        "created_at": "2023-07-01T12:00:00",
        "updated_at": "2023-07-01T12:05:00"
    },
    {
        "job_id": "job124",
        "email": "user2@example.com",
        "status": "failed",
        "message": "Error during login",
        "created_at": "2023-07-01T13:00:00",
        "updated_at": "2023-07-01T13:02:00"
    }
]
```

### Get Job Status
- **URL**: `/cpcb/queue/job/{job_id}`
- **Method**: `GET`
- **Description**: Returns detailed status for a specific job
- **Response**:
```json
{
    "job_id": "job123",
    "email": "user@example.com",
    "status": "completed",
    "message": "Scrape completed successfully",
    "created_at": "2023-07-01T12:00:00",
    "updated_at": "2023-07-01T12:05:00",
    "entity_name": "Example Entity",
    "worker": "worker-1"
}
```

### Delete Job
- **URL**: `/cpcb/queue/job/{job_id}`
- **Method**: `DELETE`
- **Description**: Deletes a job from the queue
- **Response**:
```json
{
    "status": "success",
    "message": "Job deleted successfully"
}
```

### Queue PWM Job (Alternative Endpoint)
- **URL**: `/pwmr/scrape`
- **Method**: `POST`
- **Request Body**:
```json
{
    "email": "user@example.com"
}
```
- **Response**:
```json
{
    "job_id": "job123",
    "status": "queued",
    "message": "Job successfully queued"
}
```

### Get PWM Jobs
- **URL**: `/pwmr/jobs`
- **Method**: `GET`
- **Description**: Returns all PWM jobs
- **Response**: Array of job objects

### Get PWM Job Status
- **URL**: `/pwmr/status/{job_id}`
- **Method**: `GET`
- **Description**: Gets status for a specific PWM job
- **Response**: Job status object

## Redis Maintenance

These are command-line utilities for Redis maintenance, not HTTP endpoints.

### Clean Failed Jobs
- **Command**: `python redis_maintenance.py --clean-failed [--age MINUTES]`
- **Description**: Removes failed jobs older than specified minutes (default: 5)
- **Options**:
  - `--age`: Age threshold in minutes

### Clear All Transactions
- **Command**: `python redis_maintenance.py --clear-all`
- **Description**: Removes all jobs and transactions from Redis database

### Continuous Maintenance
- **Command**: `python redis_maintenance.py [--loop]`
- **Description**: Runs the maintenance process in a continuous loop, cleaning failed jobs every minute

## Logging

### Stream Logs
- **URL**: `/logs/stream`
- **Method**: `GET`
- **Response**: Server-Sent Events (SSE) stream
- **Content-Type**: text/event-stream
- **Format**:
```
data: Log message 1

data: Log message 2

...
```

## Error Responses

All endpoints may return the following errors:

- **401 Unauthorized**:
```json
{
    "detail": "Invalid token"
}
```

- **404 Not Found**:
```json
{
    "detail": "Invalid user email."
}
```
