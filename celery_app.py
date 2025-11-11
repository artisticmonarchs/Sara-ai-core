"""
celery_app.py — Phase 12 (Celery Task Resilience)
Sara AI Core Celery bootstrapper with full fault tolerance, structured logging, 
Redis connectivity guard, Sentry integration, and consolidated task registration.
"""

import sys
import time
import random
from celery import Celery, Task
from kombu import Exchange, Queue
from logging import getLogger, StreamHandler, Formatter
from urllib.parse import urlsplit, urlunsplit

from config import config

# --------------------------------------------------------------------------- #
# Utility Functions
# --------------------------------------------------------------------------- #
def _mask_url(u: str) -> str:
    """Mask credentials in URLs for safe logging."""
    try:
        sp = urlsplit(u)
        host = f"[{sp.hostname}]" if sp.hostname and ':' in sp.hostname else (sp.hostname or "")
        netloc = host
        if sp.port: netloc += f":{sp.port}"
        if sp.username or sp.password:
            netloc = f"***:***@{netloc}"
        return urlunsplit((sp.scheme, netloc, sp.path, sp.query, sp.fragment))
    except Exception:
        return "<redacted>"

def _short(val) -> str:
    """Safely truncate large values for logging."""
    try:
        if isinstance(val, (bytes, bytearray)):
            return f"<bytes:{len(val)}>"
        s = str(val)
        return s if len(s) <= 200 else s[:200] + "..."
    except Exception:
        return "<unprintable>"

def _safe_kwargs_preview(kwargs: dict) -> str:
    """Safely preview kwargs while avoiding PII leakage."""
    try:
        SAFE_KEYS = getattr(config, "SAFE_TASK_LOG_KEYS", {"id", "uuid", "type", "task_id"})
        scrubbed = {k: (v if k in SAFE_KEYS else "***") for k, v in kwargs.items()}
        return _short(scrubbed)
    except Exception:
        return "<unprintable>"

# --------------------------------------------------------------------------- #
# Structured Logging (MOVE TO TOP - critical for early signals)
# --------------------------------------------------------------------------- #
logger = getLogger("celery_app")
logger.setLevel(config.LOG_LEVEL.upper())

# Prevent duplicate handlers on reload (gunicorn/celery multi-import)
if not logger.handlers:
    _handler = StreamHandler(sys.stdout)
    _handler.setFormatter(
        Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(_handler)

logger.info(f"[BOOT] Sara AI Core Celery starting (Phase {config.PHASE_VERSION})")

# Log versions and transport for incident debugging
import celery as _celery_pkg
logger.info(f"[BOOT] Celery v{_celery_pkg.__version__}")
try:
    transport = urlsplit(config.CELERY_BROKER_URL).scheme
    logger.info(f"[BOOT] Broker transport: {transport}")
except Exception:
    pass

# --------------------------------------------------------------------------- #
# Worker Lifecycle Signal Handlers (Better than manual signal handling)
# --------------------------------------------------------------------------- #
from celery.signals import worker_init, worker_shutdown

@worker_init.connect
def _on_worker_init(**_):
    """Worker initialization signal handler."""
    logger.info("[BOOT] Worker process initialized")

@worker_shutdown.connect
def _on_worker_shutdown(sig, how, exitcode, **_):
    """Worker shutdown signal handler."""
    logger.info(f"[SHUTDOWN] Celery worker shutting down (sig={sig}, how={how}, exit={exitcode})")

# --------------------------------------------------------------------------- #
# Sentry Integration (Error Capture)
# --------------------------------------------------------------------------- #
if config.SENTRY_DSN:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.celery import CeleryIntegration
        from sentry_sdk.integrations.redis import RedisIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration
        
        sentry_sdk.init(
            dsn=config.SENTRY_DSN,
            integrations=[
                CeleryIntegration(),
                RedisIntegration(),
                LoggingIntegration(level=config.LOG_LEVEL.upper(), event_level=None)
            ],
            traces_sample_rate=1.0,
            environment=config.ENV_MODE,
            release=f"sara-ai-core@{config.PHASE_VERSION}",
        )
        SENTRY_ENABLED = True
        logger.info("[SENTRY] Initialized successfully")
    except ImportError:
        SENTRY_ENABLED = False
        logger.warning("[SENTRY] Sentry SDK not available")
    except Exception as e:
        SENTRY_ENABLED = False
        logger.warning(f"[SENTRY] Init failed: {e}")
else:
    SENTRY_ENABLED = False

logger.info(f"[BOOT] Redis URL: {_mask_url(getattr(config, 'REDIS_URL', ''))}")
logger.info(f"[BOOT] Broker: {_mask_url(getattr(config, 'CELERY_BROKER_URL', ''))}")
logger.info(f"[BOOT] Result Backend: {_mask_url(getattr(config, 'CELERY_RESULT_BACKEND', ''))}")
logger.info(f"[BOOT] Sentry Enabled: {SENTRY_ENABLED}")

# --------------------------------------------------------------------------- #
# Custom Safe Task Base Class (Preserves Celery internals)
# --------------------------------------------------------------------------- #
class SafeTask(Task):
    """Custom task base with automatic error handling and structured logging,
    while preserving Celery's internals (retries, signals, etc.)."""

    def __call__(self, *args, **kwargs):
        # Opt-in task argument logging (avoid PII leakage by default)
        LOG_TASK_ARGS = getattr(config, "LOG_TASK_ARGS", False)
        arg_preview = _short(args) if LOG_TASK_ARGS else "<redacted>"
        kwargs_preview = _safe_kwargs_preview(kwargs) if LOG_TASK_ARGS else "<redacted>"
            
        logger.info(f"[SAFE_TASK] Executing {self.name} args={arg_preview} kwargs={kwargs_preview}")
        
        # CRITICAL: Preserve Celery's request/context flow, retries, and signals
        return super().__call__(*args, **kwargs)

    def on_success(self, retval, task_id, args, kwargs):
        """Log successful task completion for observability symmetry."""
        logger.info(f"[SAFE_TASK] Success {self.name} task_id={task_id}")
        return super().on_success(retval, task_id, args, kwargs)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Log task retries for better observability."""
        logger.warning(f"[SAFE_TASK] Retrying {self.name} due to: {exc}")
        return super().on_retry(exc, task_id, args, kwargs, einfo)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failures while preserving Celery's default behavior."""
        logger.error(f"[SAFE_TASK] Exception in {self.name}: {exc}", exc_info=True)
        if SENTRY_ENABLED:
            try:
                import sentry_sdk
                with sentry_sdk.configure_scope() as scope:
                    scope.set_tag("celery_task_id", task_id)
                    scope.set_tag("celery_task_name", self.name)
                    scope.set_context("task_args", {"args": args, "kwargs": kwargs})
                    sentry_sdk.capture_exception(exc)
            except Exception:
                pass
        # Preserve default failure behavior
        return super().on_failure(exc, task_id, args, kwargs, einfo)

# --------------------------------------------------------------------------- #
# Worker Ready Signal Handler
# --------------------------------------------------------------------------- #
from celery.signals import worker_ready

@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    """Emit structured log and increment metric when worker is ready."""
    logger.info("[WORKER_READY] Celery worker is ready and accepting tasks")
    try:
        from metrics_collector import increment_metric
        increment_metric("celery_worker_ready_total")
        logger.info("[WORKER_READY] Successfully incremented celery_worker_ready_total metric")
    except Exception as e:
        logger.warning(f"[WORKER_READY] Failed to increment metric: {e}")

# --------------------------------------------------------------------------- #
# Task Failure Signal Handler (Global fallback - avoids duplicates)
# --------------------------------------------------------------------------- #
from celery.signals import task_failure

@task_failure.connect
def on_task_failure_signal(sender=None, task_id=None, exception=None, args=None, kwargs=None, einfo=None, **extra):
    """Global task failure handler for Sentry integration (fallback for non-SafeTask tasks)."""
    # Skip if this task already uses SafeTask (its on_failure handles logging/Sentry)
    try:
        if isinstance(sender, SafeTask):
            return
    except Exception:
        pass

    logger.error(
        f"[TASK_FAILURE] Task {getattr(sender, 'name', '?')} failed: {exception}",
        exc_info=exception
    )
    
    if SENTRY_ENABLED:
        import sentry_sdk
        with sentry_sdk.configure_scope() as scope:
            scope.set_tag("celery_task_id", task_id)
            scope.set_tag("celery_task_name", getattr(sender, 'name', '?'))
            scope.set_context("task_args", {"args": args, "kwargs": kwargs})
            sentry_sdk.capture_exception(exception)

# --------------------------------------------------------------------------- #
# Fault-tolerant Redis Connectivity Guard
# --------------------------------------------------------------------------- #
def wait_for_redis(max_retries: int = 5, initial_backoff: float = 2.0):
    """Wait for Redis to become available before booting Celery."""
    delay = initial_backoff
    for attempt in range(1, max_retries + 1):
        try:
            from redis_client import get_client
            r = get_client()
            try:
                r.ping()
                logger.info(f"[REDIS] Connection successful on attempt {attempt}.")
                return True
            finally:
                try:
                    r.close()
                except Exception:
                    pass  # Client might not have close() or already closed
        except Exception as e:
            # Emit metric for Redis failure
            try:
                from metrics_collector import increment_metric
                increment_metric("redis_circuit_breaker_open")
                increment_metric("redis_call_error")
            except Exception:
                pass
            logger.warning(f"[REDIS] Connection failed ({attempt}/{max_retries}): {e}")
            jittered = delay * (0.8 + 0.4 * random.random())  # 0.8x–1.2x jitter
            time.sleep(jittered)
            delay = min(delay * 2, 30)  # Exponential backoff with cap
    logger.error("[REDIS] All connection attempts failed. Circuit open.")
    return False


if not wait_for_redis():
    logger.critical("[BOOT] Redis unavailable — aborting startup for safety.")
    sys.exit(1)

# --------------------------------------------------------------------------- #
# Celery Application Factory
# --------------------------------------------------------------------------- #
# Use config-driven result ignoring for consistency
TASK_IGNORE_RESULT = getattr(config, "TASK_IGNORE_RESULT", False)

celery = Celery(
    "sara_core",
    broker=config.CELERY_BROKER_URL,
    backend=config.CELERY_RESULT_BACKEND,
)

# Set custom task base class (all tasks inherit SafeTask behavior)
celery.Task = SafeTask

celery.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    worker_concurrency=config.CELERY_WORKER_CONCURRENCY,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=50,
    # Prevent Celery from hijacking root logger
    worker_hijack_root_logger=False,
    # Durability knobs
    worker_max_memory_per_child=getattr(config, "CELERY_WORKER_MAX_MB", 0) or None,  # restart leaky workers
    acks_on_failure_or_timeout=False,  # avoid double-ack surprises
    task_soft_time_limit=300,  # 5 minutes soft limit
    task_time_limit=360,       # 6 minutes hard limit
    # Result handling
    task_ignore_result=TASK_IGNORE_RESULT,
    # Optional: default task priority (if using RabbitMQ priorities)
    task_default_priority=getattr(config, "TASK_DEFAULT_PRIORITY", 5),
    # Broker hardening for production
    broker_heartbeat=30,
    broker_pool_limit=10,
    event_queue_expires=60,
    # Connection resiliency
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,  # infinite
    broker_connection_timeout=10,  # fail fast on dead broker
    broker_failover_strategy="round-robin",  # if using multiple brokers
    # Publish reliability
    task_default_delivery_mode="persistent",  # survive broker restarts
    task_publish_retry=True,
    task_publish_retry_policy={
        "max_retries": 7,
        "interval_start": 0.5,   # first retry delay
        "interval_step": 1.0,    # add 1s per attempt
        "interval_max": 10.0,    # cap
    },
    # Result backend retry for transient hiccups
    result_backend_transport_options={
        "retry_on_timeout": True,
        "retry_policy": {
            "max_retries": 7,
            "interval_start": 0.5,
            "interval_step": 1.0,
            "interval_max": 10.0,
        }
    },
    # Additional Celery hardening
    task_send_sent_event=False,
    worker_send_task_events=False,
    result_expires=3600,  # prevents backend bloat if results are enabled
    task_compression="gzip",
    broker_heartbeat_checkrate=2,  # faster dead-conn detection
    broker_transport_options={"visibility_timeout": 3600},
    task_default_queue="default",
    task_default_exchange="default",
    task_default_exchange_type="direct",
    task_default_routing_key="default",
    task_serializer=config.CELERY_TASK_SERIALIZER,
    result_serializer=config.CELERY_RESULT_SERIALIZER,
    accept_content=config.CELERY_ACCEPT_CONTENT,
    timezone=config.CELERY_TIMEZONE,
    enable_utc=config.CELERY_ENABLE_UTC,
    task_routes={
        'sara.outbound_call_task': {'queue': 'priority'},
        'health.ping': {'queue': 'default'},
        'maintenance.redis_cleanup': {'queue': 'default'},
        'maintenance.metrics_flush': {'queue': 'default'},
    }
)

# Optional: Rate limits for specific tasks
celery.conf.task_annotations = {
    "sara.outbound_call_task": {"rate_limit": getattr(config, "OUTBOUND_RATE", "20/m")}
}

# Explicit timezone configuration for Beat consistency
# Note: Beat schedules are interpreted in config.CELERY_TIMEZONE
# For minimal drift when running beat separately: 
# celery.conf.beat_sync_every = 1
# celery.conf.beat_max_loop_interval = 10
celery.conf.timezone = config.CELERY_TIMEZONE
celery.conf.enable_utc = config.CELERY_ENABLE_UTC

# Optional: Autodiscover tasks from installed apps (non-breaking)
celery.autodiscover_tasks(lambda: getattr(config, "CELERY_INSTALLED_APPS", []))

# --------------------------------------------------------------------------- #
# Queue Definitions (explicitly durable with optional lazy mode)
# --------------------------------------------------------------------------- #
# For RabbitMQ with large queues, consider lazy mode to reduce RAM usage:
# queue_arguments={"x-queue-mode": "lazy"}
# For RabbitMQ priorities: "x-max-priority": 10
celery.conf.task_queues = (
    Queue(
        "default", 
        Exchange("default", durable=True), 
        routing_key="default", 
        durable=True,
        # queue_arguments={"x-queue-mode": "lazy", "x-max-priority": 10}  # Uncomment for RabbitMQ
    ),
    Queue(
        "priority", 
        Exchange("priority", durable=True), 
        routing_key="priority", 
        durable=True,
        # queue_arguments={"x-queue-mode": "lazy", "x-max-priority": 10}  # Uncomment for RabbitMQ
    ),
)

# --------------------------------------------------------------------------- #
# Consolidated Task Registration
# --------------------------------------------------------------------------- #
try:
    from outbound_tasks import outbound_call_task, outbound_tasks_health_check
    logger.info("[TASKS] Successfully imported outbound_tasks")
except ImportError as e:
    logger.error(f"[TASKS] Failed to import outbound_tasks: {e}")
    # Create fallback task if import fails
    @celery.task(name="sara.outbound_call_task", bind=True, acks_late=True)
    def outbound_call_task(self, payload):
        logger.error("[TASK] outbound_call_task imported in fallback mode - original task not available")
        return {"status": "error", "message": "Outbound tasks module not available"}

# Consolidated Task Registration (add this block)
try:
    import tasks as _core_tasks  # ensures resilient tasks are registered
    logger.info("[TASKS] Successfully imported core tasks (run_inference, _run_tts_task)")
except Exception as e:
    logger.error(f"[TASKS] Failed to import core tasks: {e}")

# --------------------------------------------------------------------------- #
# Phase 12: Maintenance Tasks with Resilience
# --------------------------------------------------------------------------- #
@celery.task(
    name="maintenance.redis_cleanup",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={'max_retries': 3},
    acks_late=True
)
def redis_cleanup_task(self):
    """Periodic Redis cleanup task for expired keys and maintenance with resilience."""
    # Phase 12: Idempotency check
    try:
        from redis_client import get_client
        redis_client = get_client()
        if redis_client:
            task_id = f"task:{self.request.id}"
            if redis_client.exists(task_id):
                logger.info("[MAINTENANCE] Duplicate Redis cleanup task skipped")
                return {"status": "skipped", "reason": "duplicate_execution"}
            redis_client.setex(task_id, 3600, "done")
    except Exception:
        pass
    
    logger.info("[MAINTENANCE] Starting Redis cleanup task")
    
    try:
        from redis_client import get_client
        
        # Use explicit connection management
        redis_client = get_client()
        try:
            # Clean up expired session keys with safe prefix fallback
            session_pattern = f"{getattr(config, 'MEMORY_REDIS_PREFIX', '')}session:*"
            cursor = 0
            ttl_enforced_count = 0
            
            while True:
                cursor, keys = redis_client.scan(cursor=cursor, match=session_pattern, count=100)
                if keys:
                    # Check TTL and handle appropriately
                    for key in keys:
                        ttl = redis_client.ttl(key)
                        if ttl == -2:  # Key doesn't exist
                            continue
                        elif ttl == -1:  # Key exists but has no TTL - enforce one
                            ttl_to_set = getattr(config, "SESSION_TTL_SECONDS", 3600)  # Safe fallback
                            redis_client.expire(key, ttl_to_set)
                            ttl_enforced_count += 1
                            # Gate verbose logging in production
                            if getattr(config, "VERBOSE_MAINTENANCE_LOGS", False):
                                logger.debug(f"[REDIS_CLEANUP] Enforced TTL on key: {key}")
                        # ttl >= 0: Key has TTL, leave it alone
                
                if cursor == 0:
                    break
            
            # Clean up old circuit breaker states with safe prefix fallback
            cb_pattern = f"{getattr(config, 'CB_PREFIX', 'cb:')}*"
            cursor = 0
            cb_cleaned = 0
            
            while True:
                cursor, keys = redis_client.scan(cursor=cursor, match=cb_pattern, count=50)
                if keys:
                    for key in keys:
                        ttl = redis_client.ttl(key)
                        if ttl == -2:  # Doesn't exist
                            continue
                        elif ttl == -1:  # No TTL - clean up
                            redis_client.delete(key)
                            cb_cleaned += 1
                            # Gate verbose logging in production
                            if getattr(config, "VERBOSE_MAINTENANCE_LOGS", False):
                                logger.debug(f"[REDIS_CLEANUP] Deleted circuit breaker key without TTL: {key}")
                
                if cursor == 0:
                    break
            
            logger.info(f"[MAINTENANCE] Redis cleanup completed - TTL enforced: {ttl_enforced_count}, Circuit Breakers: {cb_cleaned}")
            return {
                "status": "completed",
                "ttl_enforced": ttl_enforced_count,
                "circuit_breakers_cleaned": cb_cleaned
            }
        finally:
            try:
                redis_client.close()
            except Exception:
                pass
            
    except Exception as e:
        # Emit metric for Redis failure
        try:
            from metrics_collector import increment_metric
            increment_metric("redis_call_error")
        except Exception:
            pass
        
        logger.error(f"[MAINTENANCE] Redis cleanup failed: {e}", exc_info=True)
        raise  # Let autoretry_for handle it

@celery.task(
    name="maintenance.metrics_flush", 
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={'max_retries': 3},
    acks_late=True
)
def metrics_flush_task(self):
    """Periodic task to flush metrics with resilience patterns."""
    # Phase 12: Idempotency check
    try:
        from redis_client import get_client
        redis_client = get_client()
        if redis_client:
            task_id = f"task:{self.request.id}"
            if redis_client.exists(task_id):
                logger.info("[MAINTENANCE] Duplicate metrics flush task skipped")
                return {"status": "skipped", "reason": "duplicate_execution"}
            redis_client.setex(task_id, 3600, "done")
    except Exception:
        pass
    
    logger.info("[MAINTENANCE] Starting metrics flush task")
    
    try:
        # Import metrics utilities
        try:
            from metrics_collector import flush_metrics, get_metrics_summary
            metrics_flushed = flush_metrics()
            metrics_summary = get_metrics_summary()
            
            logger.info(f"[METRICS] Flushed {metrics_flushed} metrics")
            return {
                "status": "completed",
                "metrics_flushed": metrics_flushed,
                "summary": metrics_summary
            }
            
        except ImportError:
            logger.warning("[METRICS] Metrics collector not available, skipping flush")
            return {
                "status": "skipped", 
                "reason": "metrics_collector not available"
            }
            
    except Exception as e:
        logger.error(f"[METRICS] Metrics flush failed: {e}", exc_info=True)
        raise  # Let autoretry_for handle it

# --------------------------------------------------------------------------- #
# Phase 12: Health Check Task with Resilience
# --------------------------------------------------------------------------- #
@celery.task(
    name="health.ping",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={'max_retries': 3},
    acks_late=True
)
def health_ping(self):
    """Enhanced health check with system status and resilience."""
    # Phase 12: Idempotency check
    try:
        from redis_client import get_client
        redis_client = get_client()
        if redis_client:
            task_id = f"task:{self.request.id}"
            if redis_client.exists(task_id):
                logger.info("[HEALTH] Duplicate health check task skipped")
                return {"status": "skipped", "reason": "duplicate_execution"}
            redis_client.setex(task_id, 3600, "done")
    except Exception:
        pass
    
    logger.info("[TASK] health.ping received → comprehensive health check")
    
    health_data = {
        "status": "ok", 
        "phase": config.PHASE_VERSION,
        "service": config.SERVICE_NAME,
        "timestamp": time.time(),
        "components": {}
    }
    
    # Add build info for dashboards
    health_data["components"]["build"] = {
        "phase": config.PHASE_VERSION,
        "env": config.ENV_MODE,
    }
    
    # Check Redis connectivity
    try:
        from redis_client import get_client
        redis_client = get_client()
        try:
            redis_ok = redis_client.ping()
            health_data["components"]["redis"] = "healthy" if redis_ok else "unhealthy"
        finally:
            try:
                redis_client.close()
            except Exception:
                pass
    except Exception as e:
        # Emit metric for Redis failure
        try:
            from metrics_collector import increment_metric
            increment_metric("redis_call_error")
        except Exception:
            pass
        health_data["components"]["redis"] = f"unhealthy: {str(e)}"
    
    # Check broker connectivity with timeout
    try:
        with celery.connection_for_read() as conn:
            conn.ensure_connection(max_retries=1, timeout=5)
        health_data["components"]["broker"] = "healthy"
    except Exception as e:
        health_data["components"]["broker"] = f"unhealthy: {e}"
    
    # Check result backend connectivity (skip if results are disabled)
    try:
        if celery.backend and not TASK_IGNORE_RESULT:
            probe_key = f"health:noop:{int(time.time())}"  # Namespaced to avoid clashes
            celery.backend.store_result(probe_key, None, "SUCCESS")
            # Best-effort cleanup - some backends ignore expire
            try:
                celery.backend.expire(probe_key, 60)
            except Exception:
                pass
        health_data["components"]["result_backend"] = "healthy"
    except Exception as e:
        health_data["components"]["result_backend"] = f"unhealthy: {e}"
    
    # Check outbound tasks health if available
    try:
        from outbound_tasks import outbound_tasks_health_check
        outbound_health = outbound_tasks_health_check()
        health_data["components"]["outbound_tasks"] = outbound_health
    except Exception as e:
        health_data["components"]["outbound_tasks"] = f"unavailable: {str(e)}"
    
    # Check Sentry
    health_data["components"]["sentry"] = "enabled" if SENTRY_ENABLED else "disabled"
    
    logger.info(f"[HEALTH] Comprehensive health check completed: {health_data}")
    return health_data

# --------------------------------------------------------------------------- #
# Periodic Task Schedule (Beat)
# --------------------------------------------------------------------------- #
celery.conf.beat_schedule = {
    'redis-cleanup-every-hour': {
        'task': 'maintenance.redis_cleanup',
        'schedule': 3600.0,  # Every hour (interpreted in config.CELERY_TIMEZONE)
        'options': {'queue': 'default'}
    },
    'metrics-flush-every-5-min': {
        'task': 'maintenance.metrics_flush', 
        'schedule': 300.0,   # Every 5 minutes (interpreted in config.CELERY_TIMEZONE)
        'options': {'queue': 'default'}
    },
    'health-check-every-2-min': {
        'task': 'health.ping',
        'schedule': 120.0,   # Every 2 minutes (interpreted in config.CELERY_TIMEZONE)
        'options': {'queue': 'default'}
    },
}

# --------------------------------------------------------------------------- #
# Startup Confirmation
# --------------------------------------------------------------------------- #
logger.info("[BOOT] Celery worker configuration loaded successfully.")
# Log final URLs in case they were overridden by Celery
logger.info(f"[BOOT] Final Broker: {_mask_url(celery.conf.broker_url)}")
logger.info(f"[BOOT] Final Backend: {_mask_url(celery.conf.result_backend)}")
logger.info(f"[BOOT] Observability namespace: {config.SERVICE_NAME}")
logger.info(f"[BOOT] Sentry error capture: {SENTRY_ENABLED}")
logger.info("[BOOT] Maintenance tasks registered: redis_cleanup, metrics_flush")
logger.info("[BOOT] Consolidated task registration completed.")
logger.info("[BOOT] Celery app is production-ready with Phase 12 resilience.")

# --------------------------------------------------------------------------- #
# Defensive Exports
# --------------------------------------------------------------------------- #
__all__ = ["celery", "health_ping", "redis_cleanup_task", "metrics_flush_task"]
if "outbound_call_task" in globals():
    __all__.append("outbound_call_task")