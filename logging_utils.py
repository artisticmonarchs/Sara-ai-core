"""
logging_utils.py — Phase 11-F Production Ready
Structured JSON Logging (Self-Contained)
----------------------------------------
Final JSON log schema v11f with sensitive data masking.
Single-line JSON output for log ingestion.

Phase 11-F Enhancements:
- Final JSON log schema v11f
- Sensitive data masking (phones, tokens, PII)
- No external dependencies (removed logger_json.py)
- Convenience methods: log_event, log_error, log_metric
- Single-line JSON output for ingestion
"""

import json
import logging
import sys
import uuid
import time
import threading
import os
import re
from typing import Dict, Any, Optional, List
from collections import deque
import signal
from datetime import datetime  # Added for proper timestamp formatting

# --------------------------------------------------------------------------
# Phase 11-F Configuration Integration
# --------------------------------------------------------------------------
try:
    from config import Config
    config = Config
except ImportError:
    # Fallback configuration for backward compatibility
    class FallbackConfig:
        SERVICE_NAME = os.getenv("SERVICE_NAME", "sara-ai-core")
        PERSONA_VERSION = os.getenv("PERSONA_VERSION", "v2.1.0")
        ENABLE_STRUCTURED_LOGGING = os.getenv("ENABLE_STRUCTURED_LOGGING", "true").lower() == "true"
        ENABLE_LOG_BUFFERING = os.getenv("ENABLE_LOG_BUFFERING", "true").lower() == "true"
        ENABLE_SENSITIVE_DATA_MASKING = os.getenv("ENABLE_SENSITIVE_DATA_MASKING", "true").lower() == "true"
        LOG_BUFFER_SIZE = int(os.getenv("LOG_BUFFER_SIZE", "1000"))
        LOG_FLUSH_INTERVAL = int(os.getenv("LOG_FLUSH_INTERVAL", "30"))
    
    config = FallbackConfig()

# --------------------------------------------------------------------------
# Phase 11-F Sensitive Data Masking Patterns
# --------------------------------------------------------------------------
PHONE_PATTERN = re.compile(r'(\+?1?[-.\s]?\(?[2-9]\d{2}\)?[-.\s]?\d{3}[-.\s]?\d{4})')
TOKEN_PATTERN = re.compile(r'(sk-[a-zA-Z0-9]{20,48})')
EMAIL_PATTERN = re.compile(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})')
IP_PATTERN = re.compile(r'(\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b)')
SSN_PATTERN = re.compile(r'(\b\d{3}-\d{2}-\d{4}\b)')

def mask_sensitive_data(text: str) -> str:
    """
    Mask sensitive information in text strings.
    Returns masked version with PII replaced with [REDACTED] markers.
    """
    if not text or not config.ENABLE_SENSITIVE_DATA_MASKING:
        return text
    
    try:
        # Mask phone numbers (keep last 4 digits)
        text = PHONE_PATTERN.sub(lambda m: f"***-***-{m.group(1)[-4:]}" if len(m.group(1)) >= 4 else "[REDACTED_PHONE]", text)
        
        # Mask API tokens
        text = TOKEN_PATTERN.sub("sk-[REDACTED]", text)
        
        # Mask email addresses (keep domain)
        text = EMAIL_PATTERN.sub(lambda m: f"***@{m.group(1).split('@')[1]}", text)
        
        # Mask IP addresses
        text = IP_PATTERN.sub("[REDACTED_IP]", text)
        
        # Mask SSN
        text = SSN_PATTERN.sub("***-**-****", text)
        
    except Exception:
        # If masking fails, return original text rather than breaking
        return text
    
    return text

def mask_sensitive_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively mask sensitive data in dictionary values.
    """
    if not data or not config.ENABLE_SENSITIVE_DATA_MASKING:
        return data
    
    masked_data = {}
    sensitive_keys = {'phone', 'phonenumber', 'number', 'token', 'apikey', 'password', 
                     'secret', 'ssn', 'email', 'ip', 'authorization', 'auth'}
    
    for key, value in data.items():
        key_lower = str(key).lower()
        
        # Check if this is a sensitive field
        if any(sensitive in key_lower for sensitive in sensitive_keys):
            if isinstance(value, str) and value.strip():
                masked_data[key] = mask_sensitive_data(value)
            else:
                masked_data[key] = "[REDACTED]"
        elif isinstance(value, dict):
            masked_data[key] = mask_sensitive_dict(value)
        elif isinstance(value, list):
            masked_data[key] = [mask_sensitive_dict(item) if isinstance(item, dict) else 
                               mask_sensitive_data(str(item)) if isinstance(item, str) else item 
                               for item in value]
        elif isinstance(value, str):
            masked_data[key] = mask_sensitive_data(value)
        else:
            masked_data[key] = value
    
    return masked_data

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
# Phase 11-F Log Buffer for Fault Tolerance
# --------------------------------------------------------------------------
_log_buffer: deque = deque(maxlen=config.LOG_BUFFER_SIZE)
_buffer_lock = threading.RLock()
_last_flush_time = 0
_buffer_enabled = config.ENABLE_LOG_BUFFERING

# --------------------------------------------------------------------------
# Phase 11-F Trace Context Management
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
# Call Context Management for Per-Call Logging
# --------------------------------------------------------------------------
_call_context = threading.local()

def set_call_context(call_id: str, persona_id: Optional[str] = None, session_phase: Optional[str] = None):
    """Set call context for current thread/async context."""
    _call_context.call_id = call_id
    if persona_id:
        _call_context.persona_id = persona_id
    if session_phase:
        _call_context.session_phase = session_phase

def get_call_context() -> Dict[str, Optional[str]]:
    """Get current call context."""
    return {
        "call_id": getattr(_call_context, 'call_id', None),
        "persona_id": getattr(_call_context, 'persona_id', None),
        "session_phase": getattr(_call_context, 'session_phase', None)
    }

def clear_call_context():
    """Clear call context for current thread/async context."""
    if hasattr(_call_context, 'call_id'):
        del _call_context.call_id
    if hasattr(_call_context, 'persona_id'):
        del _call_context.persona_id
    if hasattr(_call_context, 'session_phase'):
        del _call_context.session_phase

# --------------------------------------------------------------------------
# Phase 11-F JSON Logger Factory (No logger_json.py dependency)
# --------------------------------------------------------------------------
def get_json_logger(name: str = "sara-ai-core"):
    """Return a structured JSON logger with v11f schema."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # Avoid duplicate handlers on reloads

    handler = logging.StreamHandler(sys.stdout)

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            # Base v11f schema fields
            base_payload = {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # Fixed: use datetime instead of time
                "level": record.levelname,
                "service": name,
                "message": record.getMessage(),
            }

            # Add structured payload if present
            structured = getattr(record, "structured", None)
            if isinstance(structured, dict):
                # Apply sensitive data masking to structured payload
                masked_structured = mask_sensitive_dict(structured)
                base_payload.update(masked_structured)
            
            # Ensure required v11f fields
            if "trace_id" not in base_payload:
                base_payload["trace_id"] = get_trace_id()
            if "persona_version" not in base_payload:
                base_payload["persona_version"] = getattr(config, "PERSONA_VERSION", "v2.1.0")

            # Single-line JSON output for ingestion
            return json.dumps(base_payload, ensure_ascii=False, separators=(',', ':'))

    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    
    return logger

# --------------------------------------------------------------------------
# Contextual Logger for Per-Call Logging
# --------------------------------------------------------------------------
def get_call_logger(call_id: str, persona_id: Optional[str] = None, session_phase: Optional[str] = None, name: str = "sara-ai-core"):
    """
    Return a contextual logger with call_id, persona_id, and session_phase.
    
    Args:
        call_id: Unique identifier for the call
        persona_id: Optional persona identifier
        session_phase: Optional session phase identifier
        name: Logger name (default: "sara-ai-core")
    
    Returns:
        Configured JSON logger with call context
    """
    logger = get_json_logger(name)
    
    # Create a wrapper that injects call context into all log messages
    class CallLogger:
        def __init__(self, logger, call_id, persona_id, session_phase):
            self.logger = logger
            self.call_id = call_id
            self.persona_id = persona_id
            self.session_phase = session_phase
        
        def _inject_call_context(self, extra=None):
            """Inject call context into extra fields."""
            call_extra = {
                "call_id": self.call_id,
                "persona_id": self.persona_id,
                "session_phase": self.session_phase
            }
            if extra:
                call_extra.update(extra)
            return call_extra
        
        def info(self, msg, extra=None, *args, **kwargs):
            self.logger.info(msg, extra={"structured": self._inject_call_context(extra)}, *args, **kwargs)
        
        def warn(self, msg, extra=None, *args, **kwargs):
            self.logger.warning(msg, extra={"structured": self._inject_call_context(extra)}, *args, **kwargs)
        
        def warning(self, msg, extra=None, *args, **kwargs):
            self.logger.warning(msg, extra={"structured": self._inject_call_context(extra)}, *args, **kwargs)
        
        def error(self, msg, extra=None, *args, **kwargs):
            self.logger.error(msg, extra={"structured": self._inject_call_context(extra)}, *args, **kwargs)
        
        def debug(self, msg, extra=None, *args, **kwargs):
            self.logger.debug(msg, extra={"structured": self._inject_call_context(extra)}, *args, **kwargs)
    
    return CallLogger(logger, call_id, persona_id, session_phase)

# --------------------------------------------------------------------------
# Phase 11-F Log Buffer Management
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
# Phase 11-F Core Logging Function
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
    Core logging function with Phase 11-F enhancements.
    Implements v11f JSON schema with sensitive data masking.
    """
    start_time = time.time()
    
    try:
        # Get current trace context if not provided
        current_context = get_trace_context()
        final_trace_id = trace_id or current_context.get("trace_id")
        final_session_id = session_id or current_context.get("session_id")
        
        # Get current call context if available
        call_context = get_call_context()
        call_id = call_context.get("call_id")
        persona_id = call_context.get("persona_id")
        session_phase = call_context.get("session_phase")
        
        # Generate trace ID if none exists
        if not final_trace_id:
            final_trace_id = get_trace_id()
        
        # Base v11f schema payload
        payload = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # Fixed: use datetime instead of time
            "level": level.upper(),
            "service": service,
            "trace_id": final_trace_id,
            "persona_version": getattr(config, "PERSONA_VERSION", "v2.1.0"),
            "event": event or "event",
            "status": status,
            "message": mask_sensitive_data(message) if message else None,
        }

        # Add optional fields if available
        if final_session_id:
            payload["session_id"] = final_session_id
        if call_id:
            payload["call_id"] = call_id
        if persona_id:
            payload["persona_id"] = persona_id
        if session_phase:
            payload["session_phase"] = session_phase

        # Add and mask extra fields
        if extra:
            masked_extra = mask_sensitive_dict(extra)
            payload.update(masked_extra)

        # Buffer log for fault tolerance
        _buffer_log_entry(payload)

        # Emit to structured logger
        if config.ENABLE_STRUCTURED_LOGGING:
            log_method = getattr(logger, level, logger.info)
            log_message = message or event or "log_event"
            log_method(mask_sensitive_data(log_message), extra={"structured": payload})
        
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
            # FIXED: This was the remaining time.strftime() call causing the ValueError
            fallback_payload = {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # Fixed: use datetime instead of time
                "level": "ERROR",
                "service": "logging_utils",
                "event": "log_fallback",
                "status": "error",
                "message": f"[LOG_FALLBACK] service={service} event={event} status={status} message={message} error={e}",
            }
            print(json.dumps(fallback_payload, ensure_ascii=False, separators=(',', ':')))
        except Exception:
            # Ultimate fallback - don't break the application
            print(f"[LOG_ULTIMATE_FALLBACK] service={service} event={event} status={status} message={message} error={e}")
        return False

# --------------------------------------------------------------------------
# Global JSON Logger
# --------------------------------------------------------------------------
logger = get_json_logger(config.SERVICE_NAME)

# --------------------------------------------------------------------------
# Graceful Shutdown Handler (Fixed - moved after logger definition)
# --------------------------------------------------------------------------
def _graceful_shutdown(signum, frame):
    """Phase 12: Graceful shutdown handler"""
    log_event(
        service="logging_utils",
        event="graceful_shutdown",
        status="ok",
        message=f"Received signal {signum}, shutting down gracefully...",
        extra={"signal": signum}
    )
    sys.exit(0)

# Register signal handlers after logger is defined
signal.signal(signal.SIGINT, _graceful_shutdown)
signal.signal(signal.SIGTERM, _graceful_shutdown)

# --------------------------------------------------------------------------
# Trace Utility
# --------------------------------------------------------------------------
def get_trace_id() -> str:
    """Generate a unique trace ID for log correlation."""
    return str(uuid.uuid4())

# --------------------------------------------------------------------------
# Phase 11-F Structured Logging Entry Points
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
    Unified structured logging wrapper — Phase 11-F compliant.
    
    Emits standardized JSON logs with v11f schema:
    - timestamp: ISO 8601 with milliseconds
    - level: log level
    - service: service name
    - trace_id: correlation ID
    - persona_version: persona version
    - event: event type
    - status: ok/warning/error
    - message: log message (with sensitive data masked)
    
    All sensitive data (phones, tokens, PII) is automatically masked.
    Outputs single-line JSON for log ingestion.
    """
    # 🔒 Prevent recursion: skip metrics_collector self-logging
    if service == "metrics_collector":
        logger.info(f"[LOG_FALLBACK] service={service} event={event} status={status} message={message}")
        return True

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

def log_error(
    service: str = "api",
    event: str | None = None,
    message: str | None = None,
    trace_id: str | None = None,
    session_id: str | None = None,
    extra: dict | None = None,
):
    """Convenience method for error logging with v11f schema."""
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
    """Convenience method for warning logging with v11f schema."""
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
    """Convenience method for debug logging with v11f schema."""
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

def log_metric(
    metric_name: str,
    value: float,
    service: str = "api",
    trace_id: str | None = None,
    extra: dict | None = None,
):
    """
    Convenience method for metric logging with v11f schema.
    
    Args:
        metric_name: Name of the metric being logged
        value: Numeric value of the metric
        service: Service name
        trace_id: Trace ID for correlation
        extra: Additional metric metadata
    """
    metric_extra = {
        "metric_name": metric_name,
        "metric_value": value,
        "metric_type": "gauge"
    }
    if extra:
        metric_extra.update(extra)
    
    return _emit_log_event(
        service=service,
        event="metric_recorded",
        status="ok",
        message=f"Metric recorded: {metric_name}={value}",
        trace_id=trace_id,
        extra=metric_extra,
        level="info"
    )

# --------------------------------------------------------------------------
# Phase 11-F Health Monitoring
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
        
        # Test sensitive data masking
        test_phone = "+1234567890"
        masked_message = mask_sensitive_data(f"Test phone: {test_phone}")
        masking_working = "***-***-7890" in masked_message
        
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
            "schema_version": "v11f",
            "structured_logging_enabled": config.ENABLE_STRUCTURED_LOGGING,
            "sensitive_data_masking_enabled": config.ENABLE_SENSITIVE_DATA_MASKING,
            "sensitive_data_masking_working": masking_working,
            "log_buffering_enabled": _buffer_enabled,
            "buffer_utilization_percent": round(buffer_utilization, 2),
            "buffered_logs_count": buffer_size,
            "redis_connectivity": redis_status,
            "persona_version": getattr(config, "PERSONA_VERSION", "v2.1.0"),
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # Fixed: use datetime instead of time
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
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # Fixed: use datetime instead of time
        }

# --------------------------------------------------------------------------
# Phase 11-F Context Manager for Trace Propagation
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
# Phase 11-F Context Manager for Call Propagation
# --------------------------------------------------------------------------
class CallContext:
    """Context manager for automatic call context propagation."""
    
    def __init__(self, call_id: str, persona_id: Optional[str] = None, session_phase: Optional[str] = None, service: str = "unknown"):
        self.call_id = call_id
        self.persona_id = persona_id
        self.session_phase = session_phase
        self.service = service
        self.previous_context = {}
    
    def __enter__(self):
        # Save previous context
        self.previous_context = get_call_context()
        
        # Set new context
        set_call_context(self.call_id, self.persona_id, self.session_phase)
        
        log_event(
            service=self.service,
            event="call_context_entered",
            status="ok",
            message="Call context entered",
            extra={
                "call_id": self.call_id,
                "persona_id": self.persona_id,
                "session_phase": self.session_phase
            }
        )
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore previous context
        if self.previous_context.get('call_id'):
            set_call_context(
                self.previous_context['call_id'],
                self.previous_context.get('persona_id'),
                self.previous_context.get('session_phase')
            )
        else:
            clear_call_context()
        
        if exc_type:
            log_error(
                service=self.service,
                event="call_context_error",
                message=f"Call context exited with error: {exc_val}",
                extra={
                    "call_id": self.call_id,
                    "persona_id": self.persona_id,
                    "session_phase": self.session_phase,
                    "exception_type": exc_type.__name__
                }
            )
        else:
            log_event(
                service=self.service,
                event="call_context_exited",
                status="ok",
                message="Call context exited",
                extra={
                    "call_id": self.call_id,
                    "persona_id": self.persona_id,
                    "session_phase": self.session_phase
                }
            )

# --------------------------------------------------------------------------
# Phase 11-F Initialization
# --------------------------------------------------------------------------
def initialize_logging_system():
    """Initialize the logging system with Phase 11-F features."""
    # Recover any buffered logs from previous session
    recovered_logs = recover_buffered_logs()
    
    # Log initialization event now that all functions are defined
    log_event(
        service="logging_utils",
        event="system_initialized",
        status="ok",
        message="Logging system initialized with Phase 11-F features",
        extra={
            "phase": "11-F",
            "schema_version": "v11f",
            "structured_logging": config.ENABLE_STRUCTURED_LOGGING,
            "sensitive_data_masking": config.ENABLE_SENSITIVE_DATA_MASKING,
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
    logger.info(f"Health Status: {health_status}")
    
    # Test sensitive data masking
    test_phone = "+1234567890"
    test_token = "sk-abc123def456ghi789jkl012mno345pqr678stu901"
    test_email = "test@example.com"
    
    logger.info(f"Masked phone: {mask_sensitive_data(test_phone)}")
    logger.info(f"Masked token: {mask_sensitive_data(test_token)}")
    logger.info(f"Masked email: {mask_sensitive_data(test_email)}")
    
    # Test trace context
    with TraceContext(service="logging_utils") as trace:
        log_event(
            service="logging_utils",
            event="trace_test",
            status="ok",
            message="Trace context test successful",
            trace_id=trace.trace_id
        )
    
    # Test call context
    with CallContext(call_id="test-call-123", persona_id="assistant", session_phase="duplex", service="logging_utils") as call:
        log_event(
            service="logging_utils", 
            event="call_test",
            status="ok",
            message="Call context test successful"
        )
    
    # Test call logger
    call_logger = get_call_logger(call_id="test-call-456", persona_id="user", session_phase="init")
    call_logger.info("Call logger test message", extra={"test_field": "test_value", "phone": test_phone})
    call_logger.warn("Call logger warning test")
    call_logger.error("Call logger error test")
    
    # Test various log levels and convenience methods
    log_debug(service="logging_utils", event="debug_test", message="Debug level test")
    log_warning(service="logging_utils", event="warning_test", message="Warning level test") 
    log_error(service="logging_utils", event="error_test", message="Error level test")
    log_metric(service="logging_utils", metric_name="test_metric", value=42.5, extra={"unit": "ms"})
    
    logger.info("All logging tests completed successfully")

# Auto-initialize on module import (with safety check to avoid double initialization)
if "LOGGING_SYSTEM_INITIALIZED" not in globals():
    initialize_logging_system()
    LOGGING_SYSTEM_INITIALIZED = True

# --- Phase 11-F Compatibility Shim (Non-destructive) ---
def get_logger(name=None):
    """
    Backward compatibility wrapper for unified logging interface.
    Returns a structured logger using get_json_logger().
    Safe to remove once all modules are updated to use logging_utils.get_json_logger().
    """
    return get_json_logger(name)