from logging_utils import log_event
from metrics_collector import push_metric
from redis_client import get_redis_connection
import config

# ==============================================================
#  Phase 11-F Compliant – Unified Logging, Metrics & Config Integrated
#  Generated: 2025-11-03T17:29:04.915346
# TODO: Move hardcoded port number to config.py
#  Do not alter business logic – observability patch only.
# ==============================================================

"""
logger_json.py — Phase 11-D Compliant
Structured JSON Logger with trace/session awareness.
"""

import json
import logging
import sys
from datetime import datetime


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

try:
    push_metric(service="system_core", event="module_load", status="ok", message=__name__)
except Exception as e:
    log_event(service="system_core", event="metric_load_fail", status="error", message=str(e))
