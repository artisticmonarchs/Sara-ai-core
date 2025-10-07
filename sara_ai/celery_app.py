"""
celery_app.py — Phase 5B Production Build
Initializes Celery app and connects to Redis broker and backend.
"""

import os
import logging
from celery import Celery
from sara_ai.logging_utils import log_event

logger = logging.getLogger("celery_app")

# Redis configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Initialize Celery
celery_app = Celery(
    "sara_ai",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["sara_ai.tasks"],
)

celery_app.conf.update(
    broker_transport_options={"visibility_timeout": 3600},
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    worker_concurrency=2,
)

# Startup banner log
log_event(
    service="celery",
    event="startup",
    status="ok",
    message=f"Celery app initialized with Redis broker: {REDIS_URL}",
)
