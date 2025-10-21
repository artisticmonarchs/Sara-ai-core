"""
sentry_utils.py — Phase 11-D (Unified Error Tracking with Circuit Breaker & Observability)
Handles Sentry initialization for centralized error tracking with full Phase 11-D compliance.
"""

import logging
from typing import Optional, Dict, Any

# ------------------------------------------------------------------
# Phase 11-D: Safe Import & Dependency Handling
# ------------------------------------------------------------------
try:
    import sentry_sdk
    from sentry_sdk import capture_exception as sentry_capture_exception, capture_message as sentry_capture_message
except ImportError:
    sentry_sdk = None
    sentry_capture_exception = None
    sentry_capture_message = None

try:
    from logging_utils import log_event, get_trace_id
except ImportError:
    # Fallback logging for dependency issues
    def log_event(service, event, status, message, trace_id=None, extra=None):  # type: ignore
        logging.info(f"[{service}] {event}: {message} - {status}")
    
    def get_trace_id():  # type: ignore
        return "no_trace_id"

try:
    from config import get_sentry_config, Config
except ImportError:
    # Fallback configuration without direct os.environ access
    def get_sentry_config():  # type: ignore
        return {
            "dsn": None,  # Fixed: No direct os.getenv access
            "environment": "development",
            "release": "unknown",
            "traces_sample_rate": 1.0,
            "enabled": True
        }

# ------------------------------------------------------------------
# Phase 11-D: Circuit Breaker Support - FIXED
# ------------------------------------------------------------------
def _is_sentry_circuit_breaker_open() -> bool:
    """Check if Sentry circuit breaker is open."""
    try:
        # CORRECTED: Use get_client() instead of get_redis_client()
        from redis_client import get_client
        client = get_client()
        if not client:
            return False
            
        key = "circuit_breaker:sentry:state"
        state = client.get(key)
        return state == b"open"
    except Exception:
        return False  # Proceed on circuit breaker check errors

# ------------------------------------------------------------------
# Phase 11-D: Self-Monitoring Metrics
# ------------------------------------------------------------------
def _safe_inc_metric(metric_name: str, increment: int = 1):
    """Safely increment a metric if metrics system is available.

    Tries multiple common function names to be compatible across versions:
    - metrics.increment_metric(name, amount)
    - metrics.inc_metric(name, amount)
    - metrics.inc(name, amount)
    """
    try:
        import metrics_collector as metrics  # type: ignore
        # Try known function names
        for fn_name in ("increment_metric", "inc_metric", "inc", "inc_counter"):
            fn = getattr(metrics, fn_name, None)
            if callable(fn):
                try:
                    fn(metric_name, increment)
                    return
                except TypeError:
                    # some implementations may expect only (name) or no args; try best-effort
                    try:
                        fn(metric_name)
                        return
                    except Exception:
                        continue
    except Exception:
        pass  # Silent fail on metric updates

# ------------------------------------------------------------------
# Phase 11-D: Module State
# ------------------------------------------------------------------
_SENTRY_ENABLED = False
_SENTRY_INITIALIZED = False

# ------------------------------------------------------------------
# Phase 11-D: Safe Initialization
# ------------------------------------------------------------------
def init_sentry() -> bool:
    """
    Initialize Sentry for error monitoring with full Phase 11-D compliance.
    Safe to call even when Sentry is disabled or misconfigured.
    Returns True if initialization successful, False otherwise.
    """
    global _SENTRY_ENABLED, _SENTRY_INITIALIZED
    
    try:
        config = get_sentry_config()
        
        if not config.get("enabled", True):
            log_event(
                service="sentry_utils",
                event="init",
                status="skipped",
                message="Sentry disabled via configuration",
                trace_id=get_trace_id()
            )
            _SENTRY_ENABLED = False
            _SENTRY_INITIALIZED = False
            return False

        dsn = config.get("dsn")
        if not dsn:
            log_event(
                service="sentry_utils",
                event="init",
                status="skipped",
                message="SENTRY_DSN not set; skipping initialization",
                trace_id=get_trace_id()
            )
            _SENTRY_ENABLED = False
            _SENTRY_INITIALIZED = False
            return False

        if sentry_sdk is None:
            log_event(
                service="sentry_utils",
                event="init",
                status="error",
                message="Sentry SDK not available; skipping initialization",
                trace_id=get_trace_id()
            )
            _SENTRY_ENABLED = False
            _SENTRY_INITIALIZED = False
            return False

        # Initialize Sentry with comprehensive configuration
        sentry_sdk.init(
            dsn=dsn,
            environment=config.get("environment", "development"),
            release=config.get("release", "unknown"),
            traces_sample_rate=config.get("traces_sample_rate", 1.0),
            enable_tracing=True,
            before_send=_before_send_check_circuit_breaker,  # Circuit breaker integration
        )
        
        _SENTRY_ENABLED = True
        _SENTRY_INITIALIZED = True
        
        _safe_inc_metric("sentry_initializations_total")
        log_event(
            service="sentry_utils",
            event="init",
            status="success",
            message="Sentry initialized successfully",
            trace_id=get_trace_id(),
            extra={
                "environment": config.get("environment"),
                "release": config.get("release"),
                "traces_sample_rate": config.get("traces_sample_rate")
            }
        )
        return True
        
    except Exception as e:
        _SENTRY_ENABLED = False
        _SENTRY_INITIALIZED = False
        _safe_inc_metric("sentry_initialization_failures_total")
        log_event(
            service="sentry_utils",
            event="init",
            status="error",
            message=f"Failed to initialize Sentry: {str(e)}",
            trace_id=get_trace_id(),
            extra={"error": str(e)}
        )
        return False

def _before_send_check_circuit_breaker(event, hint):
    """
    Sentry before_send hook to check circuit breaker status.
    Returns None to drop event if circuit breaker is open.
    """
    if _is_sentry_circuit_breaker_open():
        _safe_inc_metric("sentry_circuit_breaker_hits_total")
        return None  # Drop event
    
    return event  # Proceed with normal processing

# ------------------------------------------------------------------
# Phase 11-D: Safe Error Reporting API
# ------------------------------------------------------------------
def capture_exception_safe(exc: Exception, context: Optional[Dict[str, Any]] = None) -> bool:
    """
    Best-effort Sentry exception capture with full Phase 11-D compliance.
    Returns True if successfully reported, False otherwise.
    """
    trace_id = get_trace_id()
    
    # Always log the exception locally first
    log_event(
        service="sentry_utils",
        event="exception_capture",
        status="info",
        message=f"Capturing exception: {type(exc).__name__}: {str(exc)}",
        trace_id=trace_id,
        extra=context
    )
    
    if not _SENTRY_INITIALIZED or not is_sentry_enabled():
        log_event(
            service="sentry_utils",
            event="exception_capture_skipped",
            status="warn",
            message="Sentry not initialized; exception captured locally only",
            trace_id=trace_id,
            extra={"exception_type": type(exc).__name__}
        )
        return False
    
    if _is_sentry_circuit_breaker_open():
        _safe_inc_metric("sentry_circuit_breaker_hits_total")
        log_event(
            service="sentry_utils",
            event="exception_capture_blocked",
            status="warn",
            message="Sentry circuit breaker open; exception captured locally only",
            trace_id=trace_id,
            extra={"exception_type": type(exc).__name__}
        )
        return False
    
    try:
        # Add trace context to Sentry scope
        if sentry_sdk is not None and callable(getattr(sentry_sdk, "configure_scope", None)):
            try:
                with sentry_sdk.configure_scope() as scope:
                    scope.set_tag("trace_id", trace_id)
                    scope.set_tag("service", "sara_ai_core")
                    if context:
                        for key, value in context.items():
                            if isinstance(value, (str, int, float, bool)):
                                scope.set_tag(key, str(value))
            except Exception:
                # non-fatal - continue to attempt capture
                pass

        if callable(sentry_capture_exception):
            sentry_capture_exception(exc)
        else:
            # SDK absent or function missing — ensure we record a local log
            log_event(
                service="sentry_utils",
                event="exception_capture_no_sdk",
                status="warn",
                message="Sentry SDK missing when attempting to capture exception; logged locally only",
                trace_id=trace_id,
                extra={"exception_type": type(exc).__name__}
            )

        _safe_inc_metric("sentry_exceptions_captured_total")
        log_event(
            service="sentry_utils",
            event="exception_captured",
            status="success",
            message="Exception successfully captured by Sentry",
            trace_id=trace_id,
            extra={"exception_type": type(exc).__name__}
        )
        return True
        
    except Exception as capture_error:
        _safe_inc_metric("sentry_capture_failures_total")
        log_event(
            service="sentry_utils",
            event="exception_capture_failed",
            status="error",
            message=f"Failed to capture exception in Sentry: {str(capture_error)}",
            trace_id=trace_id,
            extra={
                "original_exception": type(exc).__name__,
                "capture_error": str(capture_error)
            }
        )
        return False

def capture_message_safe(msg: str, level: str = "error", context: Optional[Dict[str, Any]] = None) -> bool:
    """
    Best-effort Sentry message capture with full Phase 11-D compliance.
    Returns True if successfully reported, False otherwise.
    """
    trace_id = get_trace_id()
    
    # Always log the message locally first
    log_event(
        service="sentry_utils",
        event="message_capture",
        status="info",
        message=f"Capturing message: {msg}",
        trace_id=trace_id,
        extra=context
    )
    
    if not _SENTRY_INITIALIZED or not is_sentry_enabled():
        log_event(
            service="sentry_utils",
            event="message_capture_skipped",
            status="warn",
            message="Sentry not initialized; message captured locally only",
            trace_id=trace_id
        )
        return False
    
    if _is_sentry_circuit_breaker_open():
        _safe_inc_metric("sentry_circuit_breaker_hits_total")
        log_event(
            service="sentry_utils",
            event="message_capture_blocked",
            status="warn",
            message="Sentry circuit breaker open; message captured locally only",
            trace_id=trace_id
        )
        return False
    
    try:
        # Add trace context to Sentry scope
        if sentry_sdk is not None and callable(getattr(sentry_sdk, "configure_scope", None)):
            try:
                with sentry_sdk.configure_scope() as scope:
                    scope.set_tag("trace_id", trace_id)
                    scope.set_tag("service", "sara_ai_core")
                    if context:
                        for key, value in context.items():
                            if isinstance(value, (str, int, float, bool)):
                                scope.set_tag(key, str(value))
            except Exception:
                pass

        if callable(sentry_capture_message):
            # sentry_sdk.capture_message usually accepts (message, level=...)
            try:
                sentry_capture_message(msg, level=level)
            except TypeError:
                # fallback for SDKs expecting a single arg
                sentry_capture_message(msg)
        else:
            log_event(
                service="sentry_utils",
                event="message_capture_no_sdk",
                status="warn",
                message="Sentry SDK missing when attempting to capture message; logged locally only",
                trace_id=trace_id
            )

        _safe_inc_metric("sentry_messages_captured_total")
        log_event(
            service="sentry_utils",
            event="message_captured",
            status="success",
            message="Message successfully captured by Sentry",
            trace_id=trace_id
        )
        return True
        
    except Exception as capture_error:
        _safe_inc_metric("sentry_capture_failures_total")
        log_event(
            service="sentry_utils",
            event="message_capture_failed",
            status="error",
            message=f"Failed to capture message in Sentry: {str(capture_error)}",
            trace_id=trace_id,
            extra={"capture_error": str(capture_error)}
        )
        return False

# ------------------------------------------------------------------
# Phase 11-D: Utility Functions
# ------------------------------------------------------------------
def is_sentry_enabled() -> bool:
    """Return True if Sentry is initialized and enabled."""
    return _SENTRY_ENABLED and _SENTRY_INITIALIZED

def get_sentry_status() -> Dict[str, Any]:
    """Return current Sentry status for health checks."""
    return {
        "enabled": _SENTRY_ENABLED,
        "initialized": _SENTRY_INITIALIZED,
        "circuit_breaker_open": _is_sentry_circuit_breaker_open()
    }

# ------------------------------------------------------------------
# Phase 11-D: Explicit Exports
# ------------------------------------------------------------------
__all__ = [
    "init_sentry",
    "capture_exception_safe", 
    "capture_message_safe",
    "is_sentry_enabled",
    "get_sentry_status"
]

# ------------------------------------------------------------------
# Phase 11-D: Auto-initialization (Optional)
# ------------------------------------------------------------------
# Uncomment if you want Sentry to auto-initialize on import
# init_sentry()