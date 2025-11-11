"""
sentry_utils.py — Phase 11-D (Unified Error Tracking with Circuit Breaker & Observability)
Handles Sentry initialization for centralized error tracking with full Phase 11-D compliance.
"""

import os
import logging
from typing import Optional, Dict, Any

# ------------------------------------------------------------------
# Phase 11-D: Safe Import & Dependency Handling
# ------------------------------------------------------------------
try:
    import sentry_sdk
    from sentry_sdk import capture_exception as sentry_capture_exception, capture_message as sentry_capture_message
    SENTRY_SDK_AVAILABLE = True
except ImportError:
    sentry_sdk = None
    sentry_capture_exception = None
    sentry_capture_message = None
    SENTRY_SDK_AVAILABLE = False

try:
    from logging_utils import log_event, get_trace_id
    LOGGING_UTILS_AVAILABLE = True
except ImportError:
    # Fallback logging for dependency issues
    LOGGING_UTILS_AVAILABLE = False
    def log_event(service, event, status, message, trace_id=None, extra=None):  # type: ignore
        logging.info(f"[{service}] {event}: {message} - {status}")
    
    def get_trace_id():  # type: ignore
        return "no_trace_id"

try:
    from config import get_sentry_config, Config
    CONFIG_AVAILABLE = True
except ImportError:
    # Fallback configuration with environment variable support
    CONFIG_AVAILABLE = False
    def get_sentry_config():  # type: ignore
        return {
            "dsn": os.getenv("SENTRY_DSN"),
            "environment": os.getenv("SENTRY_ENVIRONMENT", "development"),
            "release": os.getenv("SENTRY_RELEASE", "unknown"),
            "traces_sample_rate": float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "1.0")),
            "enabled": os.getenv("ENABLE_SENTRY", "true").lower() == "true"
        }

# ------------------------------------------------------------------
# Phase 11-D: Enhanced Configuration with Environment Override
# ------------------------------------------------------------------
def _get_effective_sentry_config() -> Dict[str, Any]:
    """
    Get Sentry configuration with environment variable override support.
    Environment variables take precedence over config file settings.
    """
    base_config = get_sentry_config() if CONFIG_AVAILABLE else {}
    
    # Environment variable overrides
    env_overrides = {
        "dsn": os.getenv("SENTRY_DSN", base_config.get("dsn")),
        "environment": os.getenv("SENTRY_ENVIRONMENT", base_config.get("environment", "development")),
        "release": os.getenv("SENTRY_RELEASE", base_config.get("release", "unknown")),
        "traces_sample_rate": float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", 
                                            str(base_config.get("traces_sample_rate", 1.0)))),
        "enabled": os.getenv("ENABLE_SENTRY", str(base_config.get("enabled", True))).lower() == "true"
    }
    
    # Merge base config with environment overrides
    effective_config = {**base_config, **env_overrides}
    
    # Log configuration for debugging
    if LOGGING_UTILS_AVAILABLE:
        log_event(
            service="sentry_utils",
            event="config_loaded",
            status="debug",
            message="Sentry configuration loaded",
            trace_id=get_trace_id(),
            extra={
                "enabled": effective_config["enabled"],
                "environment": effective_config["environment"],
                "has_dsn": bool(effective_config["dsn"]),
                "config_source": "env_override" if any(k in os.environ for k in [
                    "SENTRY_DSN", "SENTRY_ENVIRONMENT", "SENTRY_RELEASE", "ENABLE_SENTRY"
                ]) else "config_file"
            }
        )
    
    return effective_config

# ------------------------------------------------------------------
# Phase 11-D: Circuit Breaker Support - Enhanced
# ------------------------------------------------------------------
def _is_sentry_circuit_breaker_open() -> bool:
    """Check if Sentry circuit breaker is open with safe fallback."""
    try:
        from redis_client import get_client
        client = get_client()
        if not client:
            return False
            
        key = "circuit_breaker:sentry:state"
        state = client.get(key)
        return state == b"open"
    except Exception as e:
        # Safe fallback - log but don't break
        if LOGGING_UTILS_AVAILABLE:
            log_event(
                service="sentry_utils",
                event="circuit_breaker_check_failed",
                status="debug",
                message="Circuit breaker check failed, proceeding with Sentry",
                trace_id=get_trace_id(),
                extra={"error": str(e)}
            )
        return False

def _open_sentry_circuit_breaker(ttl_seconds: int = 300) -> bool:
    """Open Sentry circuit breaker for specified TTL."""
    try:
        from redis_client import get_client
        client = get_client()
        if not client:
            return False
            
        key = "circuit_breaker:sentry:state"
        return client.setex(key, ttl_seconds, "open")
    except Exception:
        return False

# ------------------------------------------------------------------
# Phase 11-D: Enhanced Self-Monitoring Metrics
# ------------------------------------------------------------------
def _safe_inc_metric(metric_name: str, increment: int = 1, tags: Optional[Dict[str, str]] = None):
    """Safely increment a metric if metrics system is available."""
    try:
        import metrics_collector as metrics
        # Try known function names with tag support
        for fn_name in ("increment_metric", "inc_metric", "inc", "inc_counter"):
            fn = getattr(metrics, fn_name, None)
            if callable(fn):
                try:
                    # Try with tags if supported
                    if tags and hasattr(fn, '__code__') and fn.__code__.co_argcount >= 3:
                        fn(metric_name, increment, tags)
                    else:
                        fn(metric_name, increment)
                    return
                except (TypeError, AttributeError):
                    # Fallback to simple call
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
_SENTRY_CONFIG = {}

# ------------------------------------------------------------------
# Phase 11-D: Safe Initialization with Enhanced Error Handling
# ------------------------------------------------------------------
def init_sentry(service_name: str = "sara_ai_core") -> bool:
    """
    Initialize Sentry for error monitoring with full Phase 11-D compliance.
    Safe to call even when Sentry is disabled or misconfigured.
    Returns True if initialization successful, False otherwise.
    """
    global _SENTRY_ENABLED, _SENTRY_INITIALIZED, _SENTRY_CONFIG
    
    try:
        _SENTRY_CONFIG = _get_effective_sentry_config()
        
        # Check if Sentry is explicitly disabled via environment
        if not _SENTRY_CONFIG.get("enabled", True):
            _safe_inc_metric("sentry_disabled_total")
            if LOGGING_UTILS_AVAILABLE:
                log_event(
                    service="sentry_utils",
                    event="init",
                    status="skipped",
                    message="Sentry disabled via configuration",
                    trace_id=get_trace_id(),
                    extra={"service_name": service_name}
                )
            _SENTRY_ENABLED = False
            _SENTRY_INITIALIZED = False
            return False

        dsn = _SENTRY_CONFIG.get("dsn")
        if not dsn:
            _safe_inc_metric("sentry_missing_dsn_total")
            if LOGGING_UTILS_AVAILABLE:
                log_event(
                    service="sentry_utils",
                    event="init",
                    status="skipped",
                    message="SENTRY_DSN not set; skipping initialization",
                    trace_id=get_trace_id(),
                    extra={"service_name": service_name}
                )
            _SENTRY_ENABLED = False
            _SENTRY_INITIALIZED = False
            return False

        if not SENTRY_SDK_AVAILABLE:
            _safe_inc_metric("sentry_sdk_unavailable_total")
            if LOGGING_UTILS_AVAILABLE:
                log_event(
                    service="sentry_utils",
                    event="init",
                    status="error",
                    message="Sentry SDK not available; skipping initialization",
                    trace_id=get_trace_id(),
                    extra={"service_name": service_name}
                )
            _SENTRY_ENABLED = False
            _SENTRY_INITIALIZED = False
            return False

        # Initialize Sentry with comprehensive configuration
        sentry_sdk.init(
            dsn=dsn,
            environment=_SENTRY_CONFIG.get("environment", "development"),
            release=_SENTRY_CONFIG.get("release", "unknown"),
            traces_sample_rate=_SENTRY_CONFIG.get("traces_sample_rate", 1.0),
            enable_tracing=True,
            before_send=_before_send_check_circuit_breaker,
            # Additional safety settings
            shutdown_timeout=5,  # Fast shutdown
            max_breadcrumbs=50,  # Limit memory usage
        )
        
        # Set default tags for all events
        with sentry_sdk.configure_scope() as scope:
            scope.set_tag("service_name", service_name)
            scope.set_tag("sentry_initialized", "true")
        
        _SENTRY_ENABLED = True
        _SENTRY_INITIALIZED = True
        
        _safe_inc_metric("sentry_initializations_total", tags={"service": service_name})
        if LOGGING_UTILS_AVAILABLE:
            log_event(
                service="sentry_utils",
                event="init",
                status="success",
                message="Sentry initialized successfully",
                trace_id=get_trace_id(),
                extra={
                    "service_name": service_name,
                    "environment": _SENTRY_CONFIG.get("environment"),
                    "release": _SENTRY_CONFIG.get("release"),
                    "traces_sample_rate": _SENTRY_CONFIG.get("traces_sample_rate")
                }
            )
        return True
        
    except Exception as e:
        _SENTRY_ENABLED = False
        _SENTRY_INITIALIZED = False
        _safe_inc_metric("sentry_initialization_failures_total", tags={"service": service_name})
        
        # Enhanced error logging with fallback
        error_message = f"Failed to initialize Sentry: {str(e)}"
        if LOGGING_UTILS_AVAILABLE:
            log_event(
                service="sentry_utils",
                event="init",
                status="error",
                message=error_message,
                trace_id=get_trace_id(),
                extra={
                    "service_name": service_name,
                    "error": str(e),
                    "sentry_sdk_available": SENTRY_SDK_AVAILABLE,
                    "config_available": CONFIG_AVAILABLE
                }
            )
        else:
            # Ultimate fallback
            logging.error(f"[sentry_utils] init: {error_message}")
            
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
# Phase 11-D: Enhanced Safe Error Reporting API
# ------------------------------------------------------------------
def capture_exception_safe(
    error: Exception, 
    context: Optional[Dict[str, Any]] = None,
    service_name: str = "unknown_service",
    trace_id: Optional[str] = None
) -> bool:
    """
    Best-effort Sentry exception capture with full Phase 11-D compliance.
    Always includes service_name and trace_id for correlation.
    Returns True if successfully reported, False otherwise.
    """
    # Get or generate trace_id
    if not trace_id and LOGGING_UTILS_AVAILABLE:
        trace_id = get_trace_id()
    trace_id = trace_id or "no_trace_id"
    
    # Always log the exception locally first with enhanced context
    log_context = {
        "service_name": service_name,
        "exception_type": type(error).__name__,
        "trace_id": trace_id,
        **(context or {})
    }
    
    if LOGGING_UTILS_AVAILABLE:
        log_event(
            service="sentry_utils",
            event="exception_capture",
            status="info",
            message=f"Capturing exception: {type(error).__name__}: {str(error)}",
            trace_id=trace_id,
            extra=log_context
        )
    else:
        logging.error(f"[{service_name}] Exception: {type(error).__name__}: {str(error)}")
    
    # Check if Sentry is available and enabled
    if not _SENTRY_INITIALIZED or not is_sentry_enabled():
        _safe_inc_metric("sentry_capture_skipped_total", tags={"reason": "not_initialized", "service": service_name})
        if LOGGING_UTILS_AVAILABLE:
            log_event(
                service="sentry_utils",
                event="exception_capture_skipped",
                status="warn",
                message="Sentry not initialized; exception captured locally only",
                trace_id=trace_id,
                extra={"service_name": service_name, "exception_type": type(error).__name__}
            )
        return False
    
    # Check circuit breaker
    if _is_sentry_circuit_breaker_open():
        _safe_inc_metric("sentry_circuit_breaker_hits_total", tags={"service": service_name})
        if LOGGING_UTILS_AVAILABLE:
            log_event(
                service="sentry_utils",
                event="exception_capture_blocked",
                status="warn",
                message="Sentry circuit breaker open; exception captured locally only",
                trace_id=trace_id,
                extra={"service_name": service_name, "exception_type": type(error).__name__}
            )
        return False
    
    try:
        # Enhanced context with service_name and trace_id
        sentry_context = {
            "service_name": service_name,
            "trace_id": trace_id,
            **(context or {})
        }
        
        # Configure Sentry scope with all context
        with sentry_sdk.configure_scope() as scope:
            scope.set_tag("service_name", service_name)
            scope.set_tag("trace_id", trace_id)
            scope.set_tag("exception_type", type(error).__name__)
            
            # Add all context as tags (for string values) and extra (for complex values)
            for key, value in sentry_context.items():
                if isinstance(value, (str, int, float, bool)):
                    scope.set_tag(key, str(value))
                else:
                    scope.set_extra(key, value)

        # Capture the exception
        sentry_capture_exception(error)
        
        _safe_inc_metric("sentry_exceptions_captured_total", tags={"service": service_name})
        if LOGGING_UTILS_AVAILABLE:
            log_event(
                service="sentry_utils",
                event="exception_captured",
                status="success",
                message="Exception successfully captured by Sentry",
                trace_id=trace_id,
                extra={
                    "service_name": service_name,
                    "exception_type": type(error).__name__
                }
            )
        return True
        
    except Exception as capture_error:
        _safe_inc_metric("sentry_capture_failures_total", tags={"service": service_name, "error_type": "exception"})
        
        # Enhanced fallback error logging
        error_message = f"Failed to capture exception in Sentry: {str(capture_error)}"
        if LOGGING_UTILS_AVAILABLE:
            log_event(
                service="sentry_utils",
                event="exception_capture_failed",
                status="error",
                message=error_message,
                trace_id=trace_id,
                extra={
                    "service_name": service_name,
                    "original_exception": type(error).__name__,
                    "capture_error": str(capture_error),
                    "sentry_available": SENTRY_SDK_AVAILABLE
                }
            )
        else:
            logging.error(f"[{service_name}] {error_message}")
            
        return False

def capture_message_safe(
    message: str, 
    level: str = "error", 
    context: Optional[Dict[str, Any]] = None,
    service_name: str = "unknown_service",
    trace_id: Optional[str] = None
) -> bool:
    """
    Best-effort Sentry message capture with full Phase 11-D compliance.
    Always includes service_name and trace_id for correlation.
    Returns True if successfully reported, False otherwise.
    """
    # Get or generate trace_id
    if not trace_id and LOGGING_UTILS_AVAILABLE:
        trace_id = get_trace_id()
    trace_id = trace_id or "no_trace_id"
    
    # Always log the message locally first
    log_context = {
        "service_name": service_name,
        "trace_id": trace_id,
        "level": level,
        **(context or {})
    }
    
    if LOGGING_UTILS_AVAILABLE:
        log_event(
            service="sentry_utils",
            event="message_capture",
            status="info",
            message=f"Capturing message: {message}",
            trace_id=trace_id,
            extra=log_context
        )
    else:
        log_level = getattr(logging, level.upper(), logging.INFO)
        logging.log(log_level, f"[{service_name}] {message}")
    
    if not _SENTRY_INITIALIZED or not is_sentry_enabled():
        _safe_inc_metric("sentry_capture_skipped_total", tags={"reason": "not_initialized", "service": service_name})
        if LOGGING_UTILS_AVAILABLE:
            log_event(
                service="sentry_utils",
                event="message_capture_skipped",
                status="warn",
                message="Sentry not initialized; message captured locally only",
                trace_id=trace_id,
                extra={"service_name": service_name}
            )
        return False
    
    if _is_sentry_circuit_breaker_open():
        _safe_inc_metric("sentry_circuit_breaker_hits_total", tags={"service": service_name})
        if LOGGING_UTILS_AVAILABLE:
            log_event(
                service="sentry_utils",
                event="message_capture_blocked",
                status="warn",
                message="Sentry circuit breaker open; message captured locally only",
                trace_id=trace_id,
                extra={"service_name": service_name}
            )
        return False
    
    try:
        # Enhanced context with service_name and trace_id
        sentry_context = {
            "service_name": service_name,
            "trace_id": trace_id,
            **(context or {})
        }
        
        # Configure Sentry scope
        with sentry_sdk.configure_scope() as scope:
            scope.set_tag("service_name", service_name)
            scope.set_tag("trace_id", trace_id)
            scope.set_tag("message_level", level)
            
            for key, value in sentry_context.items():
                if isinstance(value, (str, int, float, bool)):
                    scope.set_tag(key, str(value))
                else:
                    scope.set_extra(key, value)

        # Capture the message
        sentry_capture_message(message, level=level)
        
        _safe_inc_metric("sentry_messages_captured_total", tags={"service": service_name, "level": level})
        if LOGGING_UTILS_AVAILABLE:
            log_event(
                service="sentry_utils",
                event="message_captured",
                status="success",
                message="Message successfully captured by Sentry",
                trace_id=trace_id,
                extra={"service_name": service_name, "level": level}
            )
        return True
        
    except Exception as capture_error:
        _safe_inc_metric("sentry_capture_failures_total", tags={"service": service_name, "error_type": "message"})
        
        # Enhanced fallback error logging
        error_message = f"Failed to capture message in Sentry: {str(capture_error)}"
        if LOGGING_UTILS_AVAILABLE:
            log_event(
                service="sentry_utils",
                event="message_capture_failed",
                status="error",
                message=error_message,
                trace_id=trace_id,
                extra={
                    "service_name": service_name,
                    "capture_error": str(capture_error),
                    "sentry_available": SENTRY_SDK_AVAILABLE
                }
            )
        else:
            logging.error(f"[{service_name}] {error_message}")
            
        return False

# ------------------------------------------------------------------
# Phase 11-D: Enhanced Utility Functions
# ------------------------------------------------------------------
def is_sentry_enabled() -> bool:
    """Return True if Sentry is initialized and enabled."""
    return _SENTRY_ENABLED and _SENTRY_INITIALIZED

def get_sentry_status() -> Dict[str, Any]:
    """Return current Sentry status for health checks."""
    return {
        "enabled": _SENTRY_ENABLED,
        "initialized": _SENTRY_INITIALIZED,
        "sdk_available": SENTRY_SDK_AVAILABLE,
        "circuit_breaker_open": _is_sentry_circuit_breaker_open(),
        "config": {
            "has_dsn": bool(_SENTRY_CONFIG.get("dsn")),
            "environment": _SENTRY_CONFIG.get("environment"),
            "enabled": _SENTRY_CONFIG.get("enabled", True)
        }
    }

def set_global_service_name(service_name: str) -> None:
    """Set global service name for all Sentry events."""
    if _SENTRY_INITIALIZED and SENTRY_SDK_AVAILABLE:
        with sentry_sdk.configure_scope() as scope:
            scope.set_tag("service_name", service_name)

# ------------------------------------------------------------------
# Phase 11-D: Explicit Exports
# ------------------------------------------------------------------
__all__ = [
    "init_sentry",
    "capture_exception_safe", 
    "capture_message_safe",
    "is_sentry_enabled",
    "get_sentry_status",
    "set_global_service_name"
]

# ------------------------------------------------------------------
# Phase 11-D: Auto-initialization (Optional)
# ------------------------------------------------------------------
# Uncomment if you want Sentry to auto-initialize on import
# init_sentry()