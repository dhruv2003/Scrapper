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
