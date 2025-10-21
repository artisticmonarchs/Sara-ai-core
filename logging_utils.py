"""
logging_utils.py — Phase 11-D Compatible
Structured JSON Logging (Self-Contained)
----------------------------------------
Fixes missing logger_json import (Phase 11-A transition).
All structured fields are passed under extra={"structured": payload}
to avoid LogRecord key collisions.

Phase 11-D Enhancements:
- Trace propagation context
- Redis log buffering for fault tolerance  
- Metrics integration
- Health monitoring
- Configuration integration
- Circuit breaker awareness
"""

import json
import logging
import sys
import uuid
import time
import threading
import os
from typing import Dict, Any, Optional, List
from collections import deque

# --------------------------------------------------------------------------
# Phase 11-D Configuration Integration
# --------------------------------------------------------------------------
try:
    from config import Config
    config = Config
except ImportError:
    # Fallback configuration for backward compatibility
    class FallbackConfig:
        SERVICE_NAME = os.getenv("SERVICE_NAME", "sara-ai-core")
        ENABLE_STRUCTURED_LOGGING = os.getenv("ENABLE_STRUCTURED_LOGGING", "true").lower() == "true"
        ENABLE_LOG_BUFFERING = os.getenv("ENABLE_LOG_BUFFERING", "true").lower() == "true"
        LOG_BUFFER_SIZE = int(os.getenv("LOG_BUFFER_SIZE", "1000"))
        LOG_FLUSH_INTERVAL = int(os.getenv("LOG_FLUSH_INTERVAL", "30"))
    
    config = FallbackConfig()

# --------------------------------------------------------------------------
# Lazy Metrics Shim to Avoid Circular Imports
# --------------------------------------------------------------------------
def _get_metrics():
    """Lazy import metrics functions to break circular imports."""
    try:
        from metrics_collector import increment_metric as _inc, observe_latency as _obs
        return _inc, _obs
    except Exception:
        # safe no-op fallbacks
        def _noop_inc(*a, **k): pass
        def _noop_obs(*a, **k): pass
        return _noop_inc, _noop_obs

# Safe metric wrappers to handle signature differences
def _increment_metric_safe(metric_name: str, value: int = 1):
    """Safe wrapper for increment_metric with flexible signature handling."""
    try:
        inc_func, _ = _get_metrics()
        inc_func(metric_name, value)
    except TypeError:
        try:
            # Try older API signature: increment_metric(key, field, amount)
            # Use metric_name as both key and field for compatibility
            inc_func, _ = _get_metrics()
            inc_func(metric_name, metric_name, value)
        except Exception:
            pass  # Silently fail if metrics are not available
    except Exception:
        pass  # Silently fail if metrics are not available

def _observe_latency_safe(metric_name: str, latency_ms: float):
    """Safe wrapper for observe_latency with flexible signature handling."""
    try:
        _, obs_func = _get_metrics()
        obs_func(metric_name, latency_ms)
    except Exception:
        pass  # Silently fail if metrics are not available

# --------------------------------------------------------------------------
# Lazy Redis Client Shim to Avoid Circular Imports
# --------------------------------------------------------------------------
def _get_redis_utils():
    """Lazy import redis utilities to break circular imports."""
    try:
        from redis_client import safe_redis_operation, get_circuit_breaker_status
        return safe_redis_operation, get_circuit_breaker_status, True
    except Exception:
        # Fixed fallback implementations to match expected usage pattern
        def safe_redis_operation(func, fallback=None, operation_name=None):
            """
            Execute a callable that accepts a single 'client' argument.
            If redis is not available, return fallback.
            """
            try:
                # Try to import minimal client
                from redis_client import get_client
                client = get_client()
                if not client:
                    return fallback
                try:
                    return func(client)
                except Exception:
                    return fallback
            except Exception:
                # No redis_client available - execute without a client
                try:
                    return func(None)  # Pass None as client
                except Exception:
                    return fallback
        
        def get_circuit_breaker_status():
            return {"state": "DISABLED", "enabled": False}
        
        return safe_redis_operation, get_circuit_breaker_status, False

# --------------------------------------------------------------------------
# Phase 11-D Log Buffer for Fault Tolerance
# --------------------------------------------------------------------------
_log_buffer: deque = deque(maxlen=config.LOG_BUFFER_SIZE)
_buffer_lock = threading.RLock()
_last_flush_time = 0
_buffer_enabled = config.ENABLE_LOG_BUFFERING

# --------------------------------------------------------------------------
# Phase 11-D Trace Context Management
# --------------------------------------------------------------------------
_trace_context = threading.local()

def set_trace_context(trace_id: str, session_id: Optional[str] = None):
    """Set trace context for current thread/async context."""
    _trace_context.trace_id = trace_id
    if session_id:
        _trace_context.session_id = session_id

def get_trace_context() -> Dict[str, Optional[str]]:
    """Get current trace context."""
    return {
        "trace_id": getattr(_trace_context, 'trace_id', None),
        "session_id": getattr(_trace_context, 'session_id', None)
    }

def clear_trace_context():
    """Clear trace context for current thread/async context."""
    if hasattr(_trace_context, 'trace_id'):
        del _trace_context.trace_id
    if hasattr(_trace_context, 'session_id'):
        del _trace_context.session_id

# --------------------------------------------------------------------------
# Inline JSON Logger Factory (replaces old logger_json.py)
# --------------------------------------------------------------------------
def get_json_logger(name: str = "sara-ai-core"):
    """Return a structured JSON logger compatible with Render and Celery."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # Avoid duplicate handlers on reloads

    handler = logging.StreamHandler(sys.stdout)

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            base = {
                "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
                "level": record.levelname,
                "service": name,
                "message": record.getMessage(),
            }

            # Merge structured payload if present
            structured = getattr(record, "structured", None)
            if isinstance(structured, dict):
                base.update(structured)

            return json.dumps(base, ensure_ascii=False)

    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    
    # REMOVED: _emit_log_event call that caused NameError
    # Initialization logging moved to initialize_logging_system()
    
    return logger

# --------------------------------------------------------------------------
# Phase 11-D Log Buffer Management
# --------------------------------------------------------------------------
def _buffer_log_entry(payload: Dict[str, Any]):
    """Buffer log entry for fault-tolerant persistence."""
    if not _buffer_enabled:
        return
    
    with _buffer_lock:
        _log_buffer.append({
            "payload": payload,
            "timestamp": time.time()
        })
    
    # Auto-flush if buffer reaches 80% capacity
    if len(_log_buffer) >= (config.LOG_BUFFER_SIZE * 0.8):
        _flush_log_buffer()

def _flush_log_buffer():
    """Flush buffered logs to Redis if available."""
    global _last_flush_time
    
    if not _buffer_enabled or not _log_buffer:
        return
    
    current_time = time.time()
    if current_time - _last_flush_time < config.LOG_FLUSH_INTERVAL:
        return
    
    # Lazy import Redis utilities
    safe_redis_operation, _, has_redis = _get_redis_utils()
    if not has_redis:
        return
    
    with _buffer_lock:
        if not _log_buffer:
            return
        
        logs_to_flush = list(_log_buffer)
        _log_buffer.clear()
    
    try:
        def persist_logs(client):
            if client is None:
                return False  # No Redis client available
            pipe = client.pipeline()
            for log_entry in logs_to_flush:
                log_key = f"log_buffer:{config.SERVICE_NAME}:{log_entry['timestamp']}"
                pipe.setex(log_key, 86400, json.dumps(log_entry))  # 24 hour TTL
            return pipe.execute()
        
        success = safe_redis_operation(persist_logs, operation_name="flush_log_buffer")
        
        if success:
            _last_flush_time = current_time
            _increment_metric_safe("log_buffer_flush_success_total")
            _emit_log_event(
                service="logging_utils",
                event="log_buffer_flushed",
                status="ok",
                message="Log buffer flushed to Redis",
                extra={"logs_flushed": len(logs_to_flush)}
            )
        else:
            _increment_metric_safe("log_buffer_flush_failed_total")
            # Re-buffer logs if persistence failed
            with _buffer_lock:
                _log_buffer.extendleft(reversed(logs_to_flush))
                
    except Exception as e:
        _increment_metric_safe("log_buffer_flush_error_total")
        # Re-buffer logs on error
        with _buffer_lock:
            _log_buffer.extendleft(reversed(logs_to_flush))

def recover_buffered_logs() -> int:
    """Recover buffered logs from Redis (typically on service startup)."""
    # Lazy import Redis utilities
    safe_redis_operation, _, has_redis = _get_redis_utils()
    if not has_redis:
        return 0
    
    try:
        def recover_logs(client):
            if client is None:
                return 0  # No Redis client available
                
            pattern = f"log_buffer:{config.SERVICE_NAME}:*"
            keys = []
            for key in client.scan_iter(match=pattern):
                keys.append(key)
            
            if not keys:
                return 0
            
            logs = client.mget(keys)
            recovered_count = 0
            
            for log_data in logs:
                if log_data:
                    try:
                        if isinstance(log_data, bytes):
                            log_data = log_data.decode('utf-8')
                        log_entry = json.loads(log_data)
                        with _buffer_lock:
                            _log_buffer.append(log_entry)
                        recovered_count += 1
                    except Exception:
                        continue
            
            # Clean up recovered logs
            if keys:
                client.delete(*keys)
            
            return recovered_count
        
        recovered = safe_redis_operation(recover_logs, operation_name="recover_logs")
        
        if recovered:
            _emit_log_event(
                service="logging_utils",
                event="logs_recovered",
                status="ok",
                message="Recovered buffered logs from Redis",
                extra={"logs_recovered": recovered}
            )
        
        return recovered if recovered else 0
        
    except Exception as e:
        _emit_log_event(
            service="logging_utils",
            event="log_recovery_failed",
            status="error",
            message="Failed to recover buffered logs",
            extra={"error": str(e)}
        )
        return 0

# --------------------------------------------------------------------------
# Phase 11-D Core Logging Function
# --------------------------------------------------------------------------
def _emit_log_event(
    service: str = "api",
    event: str | None = None,
    status: str = "ok",
    message: str | None = None,
    trace_id: str | None = None,
    session_id: str | None = None,
    extra: dict | None = None,
    level: str = "info"
):
    """
    Core logging function with Phase 11-D enhancements.
    """
    start_time = time.time()
    
    try:
        # Get current trace context if not provided
        current_context = get_trace_context()
        final_trace_id = trace_id or current_context.get("trace_id")
        final_session_id = session_id or current_context.get("session_id")
        
        # Generate trace ID if none exists
        if not final_trace_id:
            final_trace_id = get_trace_id()
        
        payload = {
            "service": service,
            "event": event or "event",
            "status": status,
            "message": message,
            "trace_id": final_trace_id,
            "session_id": final_session_id,
            "log_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        if extra:
            payload.update(extra)

        # Buffer log for fault tolerance
        _buffer_log_entry(payload)

        # Emit to structured logger
        if config.ENABLE_STRUCTURED_LOGGING:
            log_method = getattr(logger, level, logger.info)
            log_method(message or event or "log_event", extra={"structured": payload})
        
        # Increment metrics using safe wrapper
        _increment_metric_safe("log_events_total")
        _increment_metric_safe(f"log_events_{service}_total")
        if status != "ok":
            _increment_metric_safe("log_events_error_total")
        
        latency_ms = (time.time() - start_time) * 1000
        _observe_latency_safe("log_event_processing_latency_ms", latency_ms)
        
        return True

    except Exception as e:
        # Fallback to basic logging if structured logging fails
        try:
            print(f"[LOG_FALLBACK] service={service} event={event} status={status} message={message} error={e}")
        except Exception:
            pass  # Ultimate fallback - don't break the application
        return False

# --------------------------------------------------------------------------
# Global JSON Logger - MOVED AFTER _emit_log_event DEFINITION
# --------------------------------------------------------------------------
logger = get_json_logger(config.SERVICE_NAME)

# --------------------------------------------------------------------------
# Trace Utility
# --------------------------------------------------------------------------
def get_trace_id() -> str:
    """Generate a unique trace ID for log correlation."""
    return str(uuid.uuid4())

# --------------------------------------------------------------------------
# Structured Logging Entry Point
# --------------------------------------------------------------------------
def log_event(
    service: str = "api",
    event: str | None = None,
    status: str = "ok",
    message: str | None = None,
    trace_id: str | None = None,
    session_id: str | None = None,
    extra: dict | None = None,
):
    """
    Unified structured logging wrapper — Phase 11-D compliant.

    Emits standardized JSON logs for observability and traceability,
    without colliding with reserved LogRecord fields.
    """
    return _emit_log_event(
        service=service,
        event=event,
        status=status,
        message=message,
        trace_id=trace_id,
        session_id=session_id,
        extra=extra,
        level="info"
    )

# --------------------------------------------------------------------------
# Phase 11-D Enhanced Logging Methods
# --------------------------------------------------------------------------
def log_error(
    service: str = "api",
    event: str | None = None,
    message: str | None = None,
    trace_id: str | None = None,
    session_id: str | None = None,
    extra: dict | None = None,
):
    """Convenience method for error logging."""
    return _emit_log_event(
        service=service,
        event=event,
        status="error",
        message=message,
        trace_id=trace_id,
        session_id=session_id,
        extra=extra,
        level="error"
    )

def log_warning(
    service: str = "api",
    event: str | None = None,
    message: str | None = None,
    trace_id: str | None = None,
    session_id: str | None = None,
    extra: dict | None = None,
):
    """Convenience method for warning logging."""
    return _emit_log_event(
        service=service,
        event=event,
        status="warning",
        message=message,
        trace_id=trace_id,
        session_id=session_id,
        extra=extra,
        level="warning"
    )

def log_debug(
    service: str = "api",
    event: str | None = None,
    message: str | None = None,
    trace_id: str | None = None,
    session_id: str | None = None,
    extra: dict | None = None,
):
    """Convenience method for debug logging."""
    return _emit_log_event(
        service=service,
        event=event,
        status="debug",
        message=message,
        trace_id=trace_id,
        session_id=session_id,
        extra=extra,
        level="debug"
    )

# --------------------------------------------------------------------------
# Phase 11-D Health Monitoring
# --------------------------------------------------------------------------
def health_check() -> Dict[str, Any]:
    """Comprehensive health check for logging system."""
    try:
        # Test basic logging
        test_trace_id = get_trace_id()
        test_success = log_event(
            service="logging_utils",
            event="health_check",
            status="ok",
            message="Logging health check",
            trace_id=test_trace_id,
            extra={"health_check": True}
        )
        
        # Check buffer status
        with _buffer_lock:
            buffer_size = len(_log_buffer)
            buffer_utilization = (buffer_size / config.LOG_BUFFER_SIZE) * 100 if config.LOG_BUFFER_SIZE > 0 else 0
        
        # Check Redis connectivity
        redis_status = "unknown"
        _, get_circuit_breaker_status, has_redis = _get_redis_utils()
        if has_redis:
            cb_status = get_circuit_breaker_status()
            redis_status = cb_status["state"]
        
        health_info = {
            "status": "ok" if test_success else "degraded",
            "service": "logging_utils",
            "structured_logging_enabled": config.ENABLE_STRUCTURED_LOGGING,
            "log_buffering_enabled": _buffer_enabled,
            "buffer_utilization_percent": round(buffer_utilization, 2),
            "buffered_logs_count": buffer_size,
            "redis_connectivity": redis_status,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        
        # Log health status
        log_event(
            service="logging_utils",
            event="health_check_completed",
            status="ok" if test_success else "warning",
            message="Logging health check completed",
            extra=health_info
        )
        
        return health_info
        
    except Exception as e:
        # Ultimate fallback health check
        return {
            "status": "error",
            "service": "logging_utils",
            "error": str(e),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

# --------------------------------------------------------------------------
# Phase 11-D Context Manager for Trace Propagation
# --------------------------------------------------------------------------
class TraceContext:
    """Context manager for automatic trace propagation."""
    
    def __init__(self, trace_id: Optional[str] = None, session_id: Optional[str] = None, service: str = "unknown"):
        self.trace_id = trace_id or get_trace_id()
        self.session_id = session_id
        self.service = service
        self.previous_context = {}
    
    def __enter__(self):
        # Save previous context
        self.previous_context = get_trace_context()
        
        # Set new context
        set_trace_context(self.trace_id, self.session_id)
        
        log_event(
            service=self.service,
            event="trace_context_entered",
            status="ok",
            message="Trace context entered",
            trace_id=self.trace_id,
            session_id=self.session_id
        )
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore previous context
        if self.previous_context.get('trace_id'):
            set_trace_context(
                self.previous_context['trace_id'],
                self.previous_context.get('session_id')
            )
        else:
            clear_trace_context()
        
        if exc_type:
            log_error(
                service=self.service,
                event="trace_context_error",
                message=f"Trace context exited with error: {exc_val}",
                trace_id=self.trace_id,
                session_id=self.session_id,
                extra={"exception_type": exc_type.__name__}
            )
        else:
            log_event(
                service=self.service,
                event="trace_context_exited",
                status="ok",
                message="Trace context exited",
                trace_id=self.trace_id,
                session_id=self.session_id
            )

# --------------------------------------------------------------------------
# Phase 11-D Initialization
# --------------------------------------------------------------------------
def initialize_logging_system():
    """Initialize the logging system with Phase 11-D features."""
    # Recover any buffered logs from previous session
    recovered_logs = recover_buffered_logs()
    
    # Log initialization event now that all functions are defined
    log_event(
        service="logging_utils",
        event="system_initialized",
        status="ok",
        message="Logging system initialized with Phase 11-D features",
        extra={
            "phase": "11-D",
            "structured_logging": config.ENABLE_STRUCTURED_LOGGING,
            "log_buffering": _buffer_enabled,
            "buffer_size": config.LOG_BUFFER_SIZE,
            "recovered_logs": recovered_logs,
            "redis_available": _get_redis_utils()[2]  # has_redis
        }
    )

# --------------------------------------------------------------------------
# Smoke Test
# --------------------------------------------------------------------------
if __name__ == "__main__":
    # Initialize system
    initialize_logging_system()
    
    # Test health check
    health_status = health_check()
    print(f"Health Status: {health_status}")
    
    # Test trace context
    with TraceContext(service="logging_utils") as trace:
        log_event(
            service="logging_utils",
            event="trace_test",
            status="ok",
            message="Trace context test successful",
            trace_id=trace.trace_id
        )
    
    # Test various log levels
    log_debug(service="logging_utils", event="debug_test", message="Debug level test")
    log_warning(service="logging_utils", event="warning_test", message="Warning level test") 
    log_error(service="logging_utils", event="error_test", message="Error level test")
    
    print("All logging tests completed successfully")

# Auto-initialize on module import
initialize_logging_system()