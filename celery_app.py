"""
celery_app.py — Phase 11-D Compliant
Celery initialization with unified metrics, centralized config, and structured logging.
"""

import time
import traceback
from functools import wraps
from celery import Celery

# Unified Observability Imports
from config import Config
from logging_utils import log_event
from metrics_registry import restore_snapshot_to_collector
from global_metrics_store import start_background_sync
from redis_client import get_client, health_check as redis_health_check

# Celery Initialization
celery = Celery(
    "sara_ai",
    broker=Config.REDIS_URL,
    backend=Config.REDIS_URL,
    include=["tasks"],
)

celery.conf.update(
    broker_transport_options={"visibility_timeout": 3600},
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    worker_concurrency=2, # Fixed value for consistency
)

# Phase 11-D: Safe Task Wrapper
def safe_task_wrapper(func):
    """
    Wraps Celery tasks with error handling, structured logging, and metric hooks.
    Prevents worker crashes due to unhandled exceptions.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        task_name = func.__name__
        try:
            log_event(
                service="celery",
                event="task_started",
                status="info",
                message=f"Celery task {task_name} started",
                extra={"task": task_name}
            )
            
            result = func(*args, **kwargs)
            
            log_event(
                service="celery",
                event="task_completed",
                status="success",
                message=f"Celery task {task_name} completed successfully",
                extra={"task": task_name}
            )
            return result
            
        except Exception as e:
            log_event(
                service="celery",
                event="task_failed",
                status="error",
                message=f"Celery task {task_name} failed",
                extra={
                    "task": task_name,
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }
            )
            # Re-raise to maintain Celery's retry behavior
            raise
    
    return wrapper

def validate_redis_connection():
    """Validate Redis connectivity with retry logic."""
    retries = 3
    for i in range(retries):
        try:
            client = get_client()
            if client and client.ping():
                log_event(
                    service="celery",
                    event="redis_connection_ok",
                    status="ok",
                    message=f"Connected to Redis at {Config.REDIS_URL}"
                )
                return True
        except Exception as e:
            log_event(
                service="celery",
                event="redis_connection_failed",
                status="warning",
                message=f"Retry {i+1}/{retries}: {str(e)}"
            )
            time.sleep(2)

    log_event(
        service="celery",
        event="redis_connection_failed_permanent",
        status="error",
        message="Redis unreachable after retries."
    )
    return False

# Validate Redis on startup
validate_redis_connection()

# Metrics Restoration and Global Sync Startup
try:
    # Import metrics_collector for restoration
    import metrics_collector
    restored_ok = restore_snapshot_to_collector(metrics_collector)
    if restored_ok:
        log_event(
            service="celery",
            event="metrics_restored",
            status="info",
            message="Restored metrics registry at worker startup"
        )

    start_background_sync()
    log_event(
        service="celery",
        event="global_metrics_sync_started",
        status="info",
        message="Started background metrics sync"
    )
except Exception as e:
    log_event(
        service="celery",
        event="metrics_startup_failed",
        status="error",
        message=f"Metrics startup failed: {str(e)}"
    )

# Startup Banner Log
log_event(
    service="celery",
    event="startup",
    status="ok",
    message=f"Celery app initialized with Redis broker: {Config.REDIS_URL}"
)