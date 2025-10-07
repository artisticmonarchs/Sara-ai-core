"""
celery_app.py
Central Celery configuration for Sara AI.
Ensures consistent startup, Redis connectivity, and structured logging.
"""

from __future__ import annotations

import os
import logging
from celery import Celery
from sara_ai.logging_utils import log_event

# ---------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------
BROKER_URL = os.getenv("CELERY_BROKER_URL") or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND") or BROKER_URL

# ---------------------------------------------------------------------
# Celery Application
# ---------------------------------------------------------------------
celery_app = Celery(
    "sara_ai",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    broker_connection_retry_on_startup=True,
    broker_transport_options={"visibility_timeout": 3600},
)

# ---------------------------------------------------------------------
# Startup Banner
# ---------------------------------------------------------------------
try:
    log_event(
        service="celery",
        event="startup",
        status="ok",
        message="Celery app initialized",
    )
except Exception as e:
    logging.error(f"Failed to log Celery startup: {e}")

# Export symbol
__all__ = ["celery_app"]
