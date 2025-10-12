"""
logging_utils.py — Phase 8 Unified Logging System
Combines Phase 7 bootstrap (rotating file + console) with Phase 8 structured JSON + trace logging.
Lightweight, dependency-free foundation shared by all Sara AI Core services.
"""

import os
import json
import uuid
import time
import logging
from logging.handlers import RotatingFileHandler


# --------------------------------------------------------------------------
# Phase 7 Bootstrap (preserved)
# --------------------------------------------------------------------------
def _bootstrap_logging():
    """
    Initialize root logger with rotating file and console handlers.
    Creates logs/ directory if missing.
    """
    os.makedirs("logs", exist_ok=True)
    log_file = os.path.join("logs", "sara_ai.log")

    root_logger = logging.getLogger()
    if not root_logger.hasHandlers():
        root_logger.setLevel(logging.INFO)

        # Rotating file handler
        file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        root_logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        root_logger.addHandler(console_handler)


# Initialize immediately when imported
_bootstrap_logging()


# --------------------------------------------------------------------------
# Phase 8 Structured JSON Logging
# --------------------------------------------------------------------------
def get_trace_id() -> str:
    """Generate a unique trace ID for log correlation."""
    return str(uuid.uuid4())


def log_event(service: str, event: str, level: str = "INFO", message: str = "", **extra) -> str:
    """
    Emit a structured JSON log entry.
    Compatible with both console and rotating file handlers initialized above.
    Returns the trace_id for observability chaining.
    """
    trace_id = extra.get("trace_id", get_trace_id())

    record = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "service": service,
        "event": event,
        "level": level,
        "message": message,
        "trace_id": trace_id,
        "extra": extra,
    }

    line = json.dumps(record, ensure_ascii=False)
    logger = logging.getLogger(service)
    level_upper = level.upper()

    if level_upper == "ERROR":
        logger.error(line)
    elif level_upper == "WARNING":
        logger.warning(line)
    else:
        logger.info(line)

    return trace_id


# --------------------------------------------------------------------------
# Smoke test (optional)
# --------------------------------------------------------------------------
if __name__ == "__main__":
    log_event("logging_utils", "init_test", message="Unified logging initialized ✅")
