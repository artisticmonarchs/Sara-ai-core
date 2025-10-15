"""
logging_utils.py — Phase 11-B Compatible
Structured JSON Logging (Self-Contained)
----------------------------------------
Fixes missing logger_json import (Phase 11-A transition).
All structured fields are passed under extra={"structured": payload}
to avoid LogRecord key collisions.
"""

import json
import logging
import sys
import uuid

# --------------------------------------------------------------------------
# Inline JSON Logger Factory (replaces old logger_json.py)
# --------------------------------------------------------------------------
def get_json_logger(name: str = "sara-ai-core"):
    """Return a structured JSON logger compatible with Render and Celery."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # Avoid duplicate handlers on reloads

    handler = logging.StreamHandler(sys.stdout)

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            base = {
                "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
                "level": record.levelname,
                "service": name,
                "message": record.getMessage(),
            }

            # Merge structured payload if present
            structured = getattr(record, "structured", None)
            if isinstance(structured, dict):
                base.update(structured)

            return json.dumps(base, ensure_ascii=False)

    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


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
    event: str | None = None,
    status: str = "ok",
    message: str | None = None,
    trace_id: str | None = None,
    session_id: str | None = None,
    extra: dict | None = None,
):
    """
    Unified structured logging wrapper — Phase 11-B compliant.

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
        extra={"phase": "11-B", "test": True},
    )
    print(f"Test log emitted for trace_id={tid}")
