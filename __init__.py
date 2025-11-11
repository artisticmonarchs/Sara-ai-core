from logging_utils import log_event
from metrics_collector import push_metric
from redis_client import get_redis_connection
import config

# ==============================================================
#  Phase 11-F Compliant – Unified Logging, Metrics & Config Integrated
#  Generated: 2025-11-03T17:29:04.919329
#  Do not alter business logic – observability patch only.
# ==============================================================

"""
__init__.py — Phase 12-Stable
Centralized structured logging bootstrap for Sara AI Core
Provides resilient JSON logging with trace correlation, observability tagging,
and metrics-safe integration for all subsystems.
"""

import os
import json
import logging
from logging.handlers import RotatingFileHandler


def setup_structured_logging():
    """
    Initialize structured JSON logging for Sara AI Core services.
    Safe to import multiple times — will not duplicate handlers.
    """

    root_logger = logging.getLogger()
    if getattr(root_logger, "_sara_logging_initialized", False):
        return  # Already initialized safely

    # Environment-aware log level
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    root_logger.setLevel(getattr(logging, log_level, logging.INFO))

    # Phase 12 JSON formatter with safe encoding
    class JSONFormatter(logging.Formatter):
        def format(self, record):
            try:
                log_entry = {
                    "timestamp": self.formatTime(record),
                    "level": record.levelname,
                    "service": getattr(record, "service", "unknown"),
                    "message": record.getMessage(),
                    "component": record.name,
                    "phase": "12",
                }

                # Trace & observability data
                trace_id = getattr(record, "trace_id", None)
                if trace_id:
                    log_entry["trace_id"] = trace_id

                extra_data = getattr(record, "structured_data", {})
                if extra_data:
                    log_entry.update(extra_data)

                return json.dumps(log_entry, ensure_ascii=False)
            except Exception as e:
                # Graceful fallback: plain text logging
                return f"[LOGGING ERROR] {e} | RawMessage={record.getMessage()}"

    formatter = JSONFormatter()

    # Console handler — primary for Render / local
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Optional file handler for local debugging
    enable_file_logs = os.getenv("ENABLE_FILE_LOGS", "false").lower() == "true"
    if enable_file_logs:
        try:
            log_dir = os.getenv("LOG_DIR", "logs")
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, "sara_ai_core.json.log")

            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=10_000_000,
                backupCount=5,
                encoding="utf-8"
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        except Exception as e:
            root_logger.warning(f"File logging setup failed: {e}")

    # Boot event
    root_logger.info(
        "Structured logging initialized",
        extra={
            "service": "logging_bootstrap",
            "structured_data": {
                "event": "logging_initialized",
                "phase": "12",
                "file_logging_enabled": enable_file_logs,
                "schema_version": "phase_12_v1",
                "environment": os.getenv("APP_ENV", "local"),
            },
        },
    )

    # Prevent re-init
    root_logger._sara_logging_initialized = True


# Auto-bootstrap
setup_structured_logging()


try:
    push_metric(service="system_core", event="module_load", status="ok", message=__name__)
except Exception as e:
    log_event(service="system_core", event="metric_load_fail", status="error", message=str(e))
