from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from app.services.utils import log_buffer

router = APIRouter(
    prefix="/logs",
    tags=["Logs"]
)

def event_stream():
    last_index = 0
    while True:
        if last_index < len(log_buffer):
            log = log_buffer[last_index]
            last_index += 1
            yield f"data: {log}\n\n"

@router.get("/stream")
def stream_logs():
    return StreamingResponse(event_stream(), media_type="text/event-stream")
