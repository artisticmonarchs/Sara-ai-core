"""
logger_json.py — Phase 10K-E
Structured JSON Logger (Safe Formatter)
---------------------------------------
Updated for Phase 10K-E:
- Reads from record.structured (Phase 10K fix)
- Avoids overwriting 'message'
- Maintains ISO timestamps, trace/session awareness
"""

import json
import logging
import sys
from datetime import datetime


# --------------------------------------------------------------------------
# JSON Formatter
# --------------------------------------------------------------------------
class JsonFormatter(logging.Formatter):
    """Formats logs as one-line JSON with consistent field names."""

    def format(self, record: logging.LogRecord) -> str:
        try:
            base = {
                "timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
                "level": record.levelname,
                "service": getattr(record, "service", "sara-ai-core"),
            }

            structured = getattr(record, "structured", None)
            if structured:
                # Merge prebuilt structured payload
                base.update(structured)
            else:
                # Fallback for non-structured logs
                base.update({
                    "message": record.getMessage(),
                    "event": getattr(record, "event", None),
                    "trace_id": getattr(record, "trace_id", None),
                    "session_id": getattr(record, "session_id", None),
                    "status": getattr(record, "status", None),
                })

            # Clean None values for compact JSON
            cleaned = {k: v for k, v in base.items() if v is not None}
            return json.dumps(cleaned, ensure_ascii=False)

        except Exception as e:
            return f"[JsonFormatter error: {e}] {record.getMessage()}"


# --------------------------------------------------------------------------
# Logger Factory
# --------------------------------------------------------------------------
def get_json_logger(name: str = "sara-ai-core") -> logging.Logger:
    """Return a logger configured for structured JSON output."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False  # prevent duplicates

    return logger


# --------------------------------------------------------------------------
# Smoke Test
# --------------------------------------------------------------------------
if __name__ == "__main__":
    test_logger = get_json_logger("sara-ai-core-test")
    test_logger.info(
        "json_logger_ready",
        extra={"structured": {
            "service": "logger_json",
            "event": "init_test",
            "status": "ok",
            "trace_id": "test-trace-123",
            "session_id": "demo-session",
            "phase": "10K-E"
        }},
    )
    print("✅ logger_json.py smoke test complete — JSON log emitted.")
