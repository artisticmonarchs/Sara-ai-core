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
from redis_client import get_client, health_check as redis_health_check

# --------------------------------------------------------------------------
# Phase 11-D Metrics Integration - Lazy Import Shim
# --------------------------------------------------------------------------
def _get_metrics():
    """Lazy metrics shim to avoid circular imports at import-time"""
    try:
        import metrics_collector as metrics
        return metrics
    except Exception:
        # safe no-op fallbacks
        class NoopMetrics:
            pass
        return NoopMetrics()

# Lazy metrics instance - will be initialized on first use
_metrics = None

def get_metrics():
    """Get or initialize lazy metrics instance"""
    global _metrics
    if _metrics is None:
        _metrics = _get_metrics()
    return _metrics

# --------------------------------------------------------------------------
# Celery Initialization
# --------------------------------------------------------------------------
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

# --------------------------------------------------------------------------
# Phase 11-D: Safe Task Wrapper
# --------------------------------------------------------------------------
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

# --------------------------------------------------------------------------
# Lazy Initialization Functions (Phase 11-D - No side effects at import time)
# --------------------------------------------------------------------------
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

def _initialize_metrics_system():
    """Lazy metrics system initialization - called when worker starts"""
    try:
        from metrics_registry import restore_snapshot_to_collector
        from global_metrics_store import start_background_sync
        
        # Import metrics_collector for restoration
        restored_ok = restore_snapshot_to_collector(get_metrics())
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

def initialize_celery_app():
    """
    Initialize Celery application with all dependencies.
    This should be called when the worker starts, not at module import.
    """
    # Validate Redis connection
    validate_redis_connection()
    
    # Initialize metrics system
    _initialize_metrics_system()
    
    # Startup Banner Log
    log_event(
        service="celery",
        event="startup",
        status="ok",
        message=f"Celery app initialized with Redis broker: {Config.REDIS_URL}"
    )

# Note: The actual initialization is deferred to when the worker starts
# This prevents import-time side effects and circular imports