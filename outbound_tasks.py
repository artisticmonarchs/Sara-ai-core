"""
outbound_tasks.py — Phase 12 (Celery Task Resilience)
Sara AI Core Celery task for outbound calls with full fault tolerance, idempotency, and unified resilience patterns.
"""

import time
import json
import uuid

# Phase 12: Use shared_task with proper resilience decorators
from celery import shared_task
from celery.exceptions import Retry

# Phase 12: Import Celery app for task registration
from celery_app import celery

# Phase 12 Compliance Metadata
__phase__ = "12"
__service__ = "outbound_tasks"
__schema_version__ = "phase_12_v1"

# Phase 12: Transient error detection
class TransientError(Exception):
    """Base class for transient errors that should trigger retries"""
    pass

# Phase 12: Configuration isolation with lazy loading
def _get_config():
    try:
        from config import Config
        return Config
    except Exception:
        class FallbackConfig:
            SERVICE_NAME = "outbound_tasks"
            CELERY_MAX_RETRIES = 3
            CELERY_RETRY_BACKOFF_MAX = 600
        return FallbackConfig

Config = _get_config()

# Phase 12: Structured logging with proper observability integration
def _get_logger():
    try:
        from logging_utils import log_event, get_trace_id
        return log_event, get_trace_id
    except Exception:
        def _noop_log(*a, **k): pass
        def _fallback_trace_id(): return str(uuid.uuid4())[:8]
        return _noop_log, _fallback_trace_id

log_event, get_trace_id = _get_logger()

# Phase 12: Metrics with unified registry integration
def _get_metrics():
    try:
        from metrics_collector import increment_metric, observe_latency
        return increment_metric, observe_latency
    except ImportError:
        def _noop_metric(*args, **kwargs): pass
        def _noop_latency(*args, **kwargs): pass
        return _noop_metric, _noop_latency

# Phase 12: Duplex streaming integration
def _get_duplex_modules():
    try:
        from duplex_voice_controller import DuplexVoiceController
        from realtime_voice_engine import RealtimeVoiceEngine
        return DuplexVoiceController, RealtimeVoiceEngine
    except ImportError:
        return None, None

DuplexVoiceController, RealtimeVoiceEngine = _get_duplex_modules()
DUPLEX_AVAILABLE = DuplexVoiceController is not None and RealtimeVoiceEngine is not None

# Phase 12: Structured logging wrapper
def _structured_log(event: str, level: str = "info", message: str = None, trace_id: str = None, **extra):
    """Structured logging wrapper with Phase 12 schema"""
    log_event(
        service=__service__,
        event=event,
        status=level,
        message=message or event,
        trace_id=trace_id or get_trace_id(),
        extra={**extra, "schema_version": __schema_version__, "phase": __phase__}
    )

# Phase 12: Metrics recording helper
def _record_metrics(event_type: str, status: str, latency_ms: float = None, trace_id: str = None):
    """Record metrics for outbound task operations"""
    try:
        increment_metric, observe_latency = _get_metrics()
        increment_metric(f"outbound_tasks_{event_type}_{status}_total")
        if latency_ms is not None:
            observe_latency(f"outbound_tasks_{event_type}_latency_seconds", latency_ms / 1000.0)
    except Exception:
        pass

# Phase 12: Circuit breaker check
def _is_circuit_breaker_open(service: str = "outbound_tasks") -> bool:
    """Check if circuit breaker is open for outbound operations"""
    try:
        from redis_client import get_redis_client, safe_redis_operation
        client = get_redis_client()
        if not client:
            return False
        state = safe_redis_operation(
            op=lambda client: client.get(f"circuit_breaker:{service}:state"),
            fallback=None,
            operation_name="get_circuit_breaker_state"
        )
        if not state:
            return False
        if isinstance(state, bytes):
            state = state.decode("utf-8")
        return state.lower() == "open"
    except Exception:
        return False

# Phase 12: Idempotency key helper
def _check_idempotency_key(task_id: str) -> bool:
    """Check if task has already been executed"""
    try:
        from redis_client import get_redis_client
        redis_client = get_redis_client()
        if not redis_client:
            return False
            
        idempotency_key = f"task:{task_id}"
        if redis_client.exists(idempotency_key):
            return True
            
        # Set key with 1-hour TTL
        redis_client.setex(idempotency_key, 3600, "done")
        return False
    except Exception:
        return False

# Phase 12: Registered task for module import
@celery.task(name="outbound.noop")
def outbound_noop():
    """Registered no-op so the module always contributes at least one task."""
    return "ok"

# Phase 12: Outbound Call Task with Full Resilience
@celery.task(
    bind=True,
    name="outbound.call_task",
    autoretry_for=(TransientError,),
    retry_backoff=True,
    retry_backoff_max=getattr(Config, 'CELERY_RETRY_BACKOFF_MAX', 600),
    retry_kwargs={'max_retries': getattr(Config, "CELERY_MAX_RETRIES", 3)},
    acks_late=True
)
def outbound_call_task(self, payload):
    """
    Phase 12 Outbound Call Task with Duplex Streaming Support and Full Resilience
    
    payload = {
        "contact": {name, phone, industry, website},
        "campaign": "<campaign-name>",
        "meta": {... optional ...},
        "duplex_enabled": True/False  # Phase 12: Optional duplex flag
    }
    """
    # Phase 12: Idempotency check
    task_id = f"task:{self.request.id}"
    if _check_idempotency_key(task_id):
        _structured_log("task_duplicate_skipped", level="info",
                       message="Duplicate task execution skipped",
                       task_id=self.request.id)
        return {"status": "skipped", "reason": "duplicate_execution"}
    
    trace_id = get_trace_id()
    start_time = time.time()
    increment_metric, observe_latency = _get_metrics()
    
    contact = payload.get("contact", {})
    campaign = payload.get("campaign", "default")
    session_id = payload.get("meta", {}).get("session_id", str(uuid.uuid4()))
    phone = contact.get("phone", "unknown")
    
    # Phase 12: Duplex streaming configuration
    duplex_enabled = payload.get("duplex_enabled", DUPLEX_AVAILABLE)

    _structured_log("task_started", level="info",
                   message="Starting outbound_call_task",
                   contact_phone=phone, campaign=campaign, session_id=session_id, 
                   duplex_enabled=duplex_enabled, trace_id=trace_id, task_id=self.request.id)

    # Phase 12: Circuit breaker check
    if _is_circuit_breaker_open("outbound_tasks"):
        _structured_log("circuit_breaker_blocked", level="warning",
                      message="Outbound task blocked by circuit breaker",
                      trace_id=trace_id, contact_phone=phone, task_id=self.request.id)
        _record_metrics("circuit_breaker", "hit", None, trace_id)
        return {"status": "blocked", "reason": "circuit_breaker_open", "trace_id": trace_id}

    _record_metrics("task", "started", None, trace_id)

    # Phase 12: Initialize duplex controller if enabled
    duplex_controller = None
    if duplex_enabled and DUPLEX_AVAILABLE:
        try:
            duplex_controller = DuplexVoiceController()
            _structured_log("duplex_controller_initialized", level="info",
                          message="Duplex controller initialized for outbound task",
                          trace_id=trace_id, contact_phone=phone, task_id=self.request.id)
        except Exception as e:
            _structured_log("duplex_controller_init_failed", level="warn",
                          message="Failed to initialize duplex controller, falling back to standard mode",
                          trace_id=trace_id, contact_phone=phone, error=str(e), task_id=self.request.id)

    # Try several candidate pipeline entrypoints
    candidate_names = [
        "perform_outbound_call",
        "run_call_pipeline", 
        "perform_tts_core",
        "outbound_call_handler"
    ]

    result = None
    function_used = None
    
    for name in candidate_names:
        try:
            import tasks as tasks_module
            fn = getattr(tasks_module, name, None)
            if callable(fn):
                try:
                    # Phase 12: Pass duplex controller to pipeline functions if available
                    if duplex_controller:
                        try:
                            result = fn(contact=contact, session_id=session_id, campaign=campaign, 
                                      duplex_controller=duplex_controller)
                            function_used = name
                            _structured_log("pipeline_executed_duplex", level="info",
                                          message="Pipeline callable executed with duplex controller",
                                          function_name=name, contact_phone=phone, trace_id=trace_id, task_id=self.request.id)
                            break
                        except TypeError:
                            # Fallback to standard call if function doesn't accept duplex_controller
                            result = fn(contact=contact, session_id=session_id, campaign=campaign)
                            function_used = name
                            _structured_log("pipeline_executed_standard", level="info",
                                          message="Pipeline callable executed with standard signature",
                                          function_name=name, contact_phone=phone, trace_id=trace_id, task_id=self.request.id)
                            break
                    else:
                        result = fn(contact=contact, session_id=session_id, campaign=campaign)
                        function_used = name
                        _structured_log("pipeline_executed", level="info",
                                      message="Pipeline callable executed successfully",
                                      function_name=name, contact_phone=phone, trace_id=trace_id, task_id=self.request.id)
                        break
                except TypeError:
                    # maybe signature differs: try passing only contact
                    try:
                        result = fn(contact)
                        function_used = name
                        _structured_log("pipeline_executed_fallback", level="info",
                                      message="Pipeline callable executed with fallback signature",
                                      function_name=name, contact_phone=phone, trace_id=trace_id, task_id=self.request.id)
                        break
                    except Exception as e:
                        _structured_log("pipeline_execution_failed", level="error",
                                      message="Candidate function failed with fallback signature",
                                      function_name=name, contact_phone=phone, error=str(e), trace_id=trace_id, task_id=self.request.id)
                        _record_metrics("pipeline_execution", "failed", None, trace_id)
                except Exception as e:
                    _structured_log("pipeline_execution_failed", level="error",
                                  message="Candidate function failed",
                                  function_name=name, contact_phone=phone, error=str(e), trace_id=trace_id, task_id=self.request.id)
                    _record_metrics("pipeline_execution", "failed", None, trace_id)
        except Exception as e:
            _structured_log("module_import_failed", level="error",
                          message="Failed to import tasks module",
                          function_name=name, contact_phone=phone, error=str(e), trace_id=trace_id, task_id=self.request.id)

    # Phase 12: Clean up duplex controller if initialized
    if duplex_controller:
        try:
            duplex_controller.cleanup()
            _structured_log("duplex_controller_cleaned", level="info",
                          message="Duplex controller cleaned up",
                          trace_id=trace_id, contact_phone=phone, task_id=self.request.id)
        except Exception as e:
            _structured_log("duplex_controller_cleanup_failed", level="warn",
                          message="Failed to clean up duplex controller",
                          trace_id=trace_id, contact_phone=phone, error=str(e), task_id=self.request.id)

    # If the pipeline returned booking info, persist
    if isinstance(result, dict) and result.get("booking_confirmed"):
        booking = result.get("booking", {})
        try:
            import meeting_persist
            persist_res = meeting_persist.persist_booking(session_id, contact, booking)
            _structured_log("booking_persisted", level="info",
                          message="Booking persisted successfully",
                          session_id=session_id, contact_phone=phone, 
                          persist_result=persist_res, trace_id=trace_id, task_id=self.request.id)
            _record_metrics("booking_persist", "success", None, trace_id)
        except Exception as e:
            _structured_log("booking_persist_failed", level="error",
                          message="Failed to persist booking",
                          session_id=session_id, contact_phone=phone, error=str(e), trace_id=trace_id, task_id=self.request.id)
            _record_metrics("booking_persist", "failed", None, trace_id)

    # Record metrics and completion
    latency_ms = (time.time() - start_time) * 1000
    _record_metrics("task", "completed", latency_ms, trace_id)
    observe_latency("outbound_tasks_execution_latency_seconds", latency_ms / 1000.0)
    
    _structured_log("task_completed", level="info",
                   message="Outbound call task completed",
                   contact_phone=phone, function_used=function_used, 
                   latency_ms=latency_ms, duplex_enabled=duplex_enabled,
                   trace_id=trace_id, task_id=self.request.id)

    # Return result for introspection
    return {
        "status": "completed",
        "result": result,
        "function_used": function_used,
        "duplex_enabled": duplex_enabled,
        "trace_id": trace_id,
        "latency_ms": latency_ms,
        "task_id": self.request.id
    }

# Phase 12: Health check function for task monitoring
def outbound_tasks_health_check():
    """Health check for outbound tasks service"""
    trace_id = get_trace_id()
    start_time = time.time()
    
    try:
        # Check circuit breaker state
        circuit_breaker_open = _is_circuit_breaker_open("outbound_tasks")
        
        # Check Redis connectivity
        try:
            from redis_client import get_redis_client, safe_redis_operation
            redis_client = get_redis_client()
            redis_ok = safe_redis_operation(
                op=lambda client: client.ping() if client else False,
                fallback=False,
                operation_name="health_check_ping"
            )
        except Exception:
            redis_ok = False
        
        # Check duplex availability
        duplex_available = DUPLEX_AVAILABLE
        
        status = "healthy" if not circuit_breaker_open and redis_ok else "degraded"
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "success", latency_ms, trace_id)
        
        _structured_log("health_check", level="info",
                       message="Outbound tasks health check completed",
                       trace_id=trace_id, status=status, circuit_breaker_open=circuit_breaker_open,
                       redis_ok=redis_ok, duplex_available=duplex_available)
        
        return {
            "service": __service__,
            "status": status,
            "phase": __phase__,
            "schema_version": __schema_version__,
            "trace_id": trace_id,
            "components": {
                "circuit_breaker": "open" if circuit_breaker_open else "closed",
                "redis": "healthy" if redis_ok else "unhealthy",
                "duplex_available": duplex_available
            }
        }
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "failed", latency_ms, trace_id)
        _structured_log("health_check_failed", level="error",
                       message="Outbound tasks health check failed",
                       trace_id=trace_id, error=str(e))
        return {
            "service": __service__,
            "status": "unhealthy",
            "error": str(e),
            "phase": __phase__,
            "schema_version": __schema_version__,
            "trace_id": trace_id
        }

# Phase 12: Exports
__all__ = [
    "outbound_call_task",
    "outbound_noop",
    "outbound_tasks_health_check",
    "__phase__",
    "__service__",
    "__schema_version__"
]