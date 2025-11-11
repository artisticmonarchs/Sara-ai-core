"""
check_redis.py — Sara AI Core (Phase 11-D)
Centralized Redis health verification and diagnostics.
"""
__phase__ = "11-D"
__service__ = "check_redis"
__schema_version__ = "phase_11d_v1"

import time
import os
import redis
from redis_client import get_redis_client, safe_redis_operation
from logging_utils import log_event, get_trace_id
from metrics_collector import increment_metric, observe_latency
from sentry_utils import capture_exception_safe
from config import Config
import signal
import sys

def _graceful_shutdown(signum, frame):
    """Phase 12: Graceful shutdown handler"""
    logger.info(f"Received signal {{signum}}, shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, _graceful_shutdown)
signal.signal(signal.SIGTERM, _graceful_shutdown)


# Initialize Redis client with safe configuration - use Config only
REDIS_URL = getattr(Config, "REDIS_URL", "redis://localhost:6379/0")
# TODO: Move hardcoded port number to config.py
redis_client = get_redis_client()

def _structured_log(event, level, message, **extra):
    """Helper for structured logging with trace ID"""
    log_event(
        service="check_redis",
        event=event,
        status=level,
        message=message,
        trace_id=get_trace_id(),
        phase="11-D",
        **extra
    )

def _record_metrics(event_type, status, latency_ms=None):
    """Record metrics for observability with phase/service labels"""
    increment_metric(
        f"check_redis_{event_type}_{status}_total",
        labels={"phase": "11-D", "service": "check_redis"}
    )
    if latency_ms is not None:
        observe_latency(
            "check_redis_operation_latency_seconds", 
            latency_ms / 1000.0,
            # TODO: Move hardcoded port number to config.py
            labels={"phase": "11-D", "service": "check_redis"}
        )

def check_redis_health():
    """
    Comprehensive Redis health check with Phase 11-D compliance
    Returns structured health response
    """
    start_time = time.time()
    trace_id = get_trace_id()
    health_data = {
        "service": "check_redis",
        "status": "unhealthy",
        "redis_ok": False,
        "breaker_open": False,
        "phase": "11-D",
        "schema_version": "phase_11d_v1",
        "trace_id": trace_id,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "latency_ms": 0,
        "details": {}
    }

    try:
        # Check circuit breaker state first
        breaker_state = safe_redis_operation(
            lambda: redis_client.get("circuit_breaker:redis:state"),
            fallback=None,
            operation_name="check_breaker_state"
        )
        
        # Defensive decoding of breaker state
        breaker_state = (breaker_state or b"").decode() if isinstance(breaker_state, (bytes, bytearray)) else (breaker_state or "")
        breaker_open = breaker_state.lower() == "open"
        health_data["breaker_open"] = breaker_open
        
        if breaker_open:
            health_data["status"] = "degraded"
            health_data["details"]["breaker_reason"] = "Circuit breaker is open"
            _structured_log(
                "redis_health_check",
                "warning",
                "Redis health check skipped - circuit breaker open",
                breaker_state=breaker_state
            )
            _record_metrics("health_check", "degraded_circuit_breaker")
            return health_data

        # Test Redis connection with ping
        ping_ok = safe_redis_operation(
            lambda: redis_client.ping(),
            fallback=False,
            operation_name="ping_redis"
        )
        health_data["redis_ok"] = ping_ok
        health_data["details"]["ping"] = "success" if ping_ok else "failed"

        if ping_ok:
            # Check persistence info
            persistence_info = safe_redis_operation(
                lambda: redis_client.info('persistence'),
                fallback={},
                operation_name="get_persistence_info"
            )
            health_data["details"]["persistence"] = persistence_info

            # Optional quick key test (with cleanup)
            key_test_ok = safe_redis_operation(
                lambda: (
                    redis_client.set('test_key', 'ok', ex=10),  # Set with 10s expiry
                    redis_client.get('test_key')
                )[1] == 'ok',  # Return True if value matches
                fallback=False,
                operation_name="key_operation_test"
            )
            health_data["details"]["key_test"] = "success" if key_test_ok else "failed"

            # Determine overall status
            if key_test_ok:
                health_data["status"] = "healthy"
                _record_metrics("health_check", "success")
            else:
                health_data["status"] = "degraded"
                health_data["details"]["degradation_reason"] = "Key operations failing"
                _record_metrics("health_check", "degraded")
        else:
            health_data["status"] = "unhealthy"
            _record_metrics("health_check", "failure")

        # Log the health check result
        _structured_log(
            "redis_health_check",
            "info" if health_data["status"] == "healthy" else "warning",
            f"Redis health check completed: {health_data['status']}",
            redis_ok=ping_ok,
            breaker_open=breaker_open,
            persistence_available=bool(health_data["details"].get("persistence"))
        )

    except Exception as e:
        error_msg = f"Redis health check failed: {str(e)}"
        health_data["status"] = "unhealthy"
        health_data["details"]["error"] = error_msg
        
        _structured_log(
            "redis_health_check",
            "error",
            error_msg,
            error_type=type(e).__name__
        )
        _record_metrics("health_check", "failure")
        capture_exception_safe(e)

    finally:
        # Calculate and record latency
        latency_ms = (time.time() - start_time) * 1000
        # TODO: Move hardcoded port number to config.py
        health_data["latency_ms"] = round(latency_ms, 2)
        _record_metrics("health_check", health_data["status"], latency_ms)

    return health_data

def get_redis_status():
    """
    Lightweight Redis status check for quick health assessments
    Returns boolean indicating basic Redis availability
    """
    try:
        return safe_redis_operation(
            lambda: redis_client.ping(),
            fallback=False,
            operation_name="quick_ping_redis"
        )
    except Exception as e:
        capture_exception_safe(e)
        return False

# Maintain backward compatibility for direct script execution
if __name__ == "__main__":
    health_result = check_redis_health()
    # Replace print with structured logging for Phase 11-D compliance
    _structured_log(
        "redis_health_check_cli",
        "info", 
        "Redis health check completed via CLI",
        health_status=health_result["status"],
        redis_ok=health_result["redis_ok"],
        breaker_open=health_result["breaker_open"]
    )

__all__ = ["check_redis_health", "get_redis_status", "redis_client"]