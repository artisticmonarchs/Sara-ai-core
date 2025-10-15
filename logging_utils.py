"""
logging_utils.py — Phase 10K-E
Structured JSON Logging (Collision-Safe)
----------------------------------------
Fixes LogRecord overwrite issue from Phase 10K-A.
All structured fields are now passed under `extra={"structured": payload}`
to avoid 'message' collisions.

Integrated with logger_json.JsonFormatter (reads record.structured).
"""

import uuid
from logger_json import get_json_logger

# --------------------------------------------------------------------------
# Global JSON Logger
# --------------------------------------------------------------------------
logger = get_json_logger("sara-ai-core")


# --------------------------------------------------------------------------
# Trace Utility
# --------------------------------------------------------------------------
def get_trace_id() -> str:
    """Generate a unique trace ID for log correlation."""
    return str(uuid.uuid4())


# --------------------------------------------------------------------------
# Structured Logging Entry Point
# --------------------------------------------------------------------------
def log_event(
    service: str = "api",
    event: str = None,
    status: str = "ok",
    message: str = None,
    trace_id: str = None,
    session_id: str = None,
    extra: dict | None = None,
):
    """
    Unified structured logging wrapper — Phase 10K-E compliant.

    Emits standardized JSON logs for observability and traceability,
    without colliding with reserved LogRecord fields.
    """
    try:
        payload = {
            "service": service,
            "event": event or "event",
            "status": status,
            "message": message,
            "trace_id": trace_id,
            "session_id": session_id,
        }

        if extra:
            payload.update(extra)

        # Pass structured dict under a single 'structured' key
        logger.info(message or event or "log_event", extra={"structured": payload})

    except Exception as e:
        print(f"[log_event error] {e} :: {message}")


# --------------------------------------------------------------------------
# Smoke Test
# --------------------------------------------------------------------------
if __name__ == "__main__":
    tid = get_trace_id()
    log_event(
        service="logging_utils",
        event="init_test",
        status="ok",
        message="Structured JSON logging initialized ✅",
        trace_id=tid,
        session_id="demo-session",
        extra={"phase": "10K-E", "test": True},
    )
    print(f"Test log emitted for trace_id={tid}")
