"""
celery_app.py — Phase 6 Ready (Flattened Structure)
Initializes Celery app and connects to Redis broker and backend.
"""

import os
import logging
from celery import Celery
from logging_utils import log_event      # ✅ fixed import

logger = logging.getLogger("celery_app")

# Redis configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Initialize Celery (named 'celery' for Procfile alignment)
celery = Celery(
    "sara_ai",                           # name can stay as identifier; not a module path
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["tasks"],                   # ✅ fixed include path
)

celery.conf.update(
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
