"""
utils.py — Phase 11-D Compliant
Utility helpers with trace/session propagation support.
"""

import uuid
import time
import json
import logging
import secrets
from typing import Any, Dict, Optional, Callable

# Safe import with fallbacks
try:
    from logging_utils import log_event, get_trace_id
except ImportError:
    # Fallback implementation
    logging.basicConfig(level=logging.INFO)
    
    def log_event(service: str, event: str, status: str, message: str, 
                 trace_id: Optional[str] = None, **extra) -> None:
        log_msg = f"[{service}] {event}: {message} ({status})"
        if trace_id:
            log_msg += f" [trace_id: {trace_id}]"
        if extra:
            log_msg += f" [extra: {extra}]"
        logging.info(log_msg)
    
    def get_trace_id() -> str:
        return "no-trace-id"

# Unified metrics API - Lazy import wrapper to avoid circular imports
def increment_metric(name: str, amount: int = 1, labels: dict = None):
    """Safe metrics increment wrapper that avoids circular imports."""
    try:
        from metrics_collector import increment_metric as _inc
        return _inc(name, amount, labels)
    except Exception:
        return None

try:
    from redis_client import get_redis_client
except ImportError:
    def get_redis_client():
        return None

def restore_metrics_snapshot(service_name: str = "metrics_collector"):
    """
    Restore last known metrics snapshot from Redis.
    Called during app startup to reload previous metric state.
    """
    try:
        from redis_client import get_redis_client
        client = get_redis_client()
        if not client:
            return None
        key = f"prometheus:registry_snapshot:{service_name}"
        snapshot = client.get(key)
        if not snapshot:
            return None
        return json.loads(snapshot)
    except Exception as e:
        # Replace print with structured logging
        try:
            log_event(
                service="utils",
                event="restore_metrics_snapshot",
                status="error",
                message=f"Error restoring snapshot: {e}",
                service_name=service_name
            )
        except Exception:
            # Ultimate fallback if log_event fails
            print(f"[utils] restore_metrics_snapshot: Error restoring snapshot: {e}")
        return None

def _safe_inc_metric(metric_name: str, value: int = 1, labels: Optional[dict] = None) -> None:
    """Safely increment metrics counter with fallback and label support"""
    try:
        increment_metric(metric_name, value, labels or {})
    except Exception:
        pass  # Silent fallback - metrics are non-critical

def _decode_redis_value(val):
    """Defensively decode Redis values from bytes to string"""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        try:
            return val.decode('utf-8')
        except Exception:
            return str(val)
    return val

def generate_id() -> str:
    """Generate a unique UUID string for traceable operations with observability."""
    trace_id = get_trace_id()
    try:
        result = str(uuid.uuid4())
        log_event(
            service="utils",
            event="generate_id",
            status="success",
            message="UUID generated successfully",
            trace_id=trace_id,
            generated_id=result
        )
        return result
    except Exception as e:
        error_msg = f"UUID generation failed: {str(e)}"
        fallback_id = f"fallback-{int(time.time())}-{secrets.token_hex(8)}"
        
        log_event(
            service="utils",
            event="generate_id",
            status="error",
            message=error_msg,
            trace_id=trace_id,
            fallback_id=fallback_id
        )
        _safe_inc_metric("utils_uuid_failures_total")
        return fallback_id

def safe_get(d: dict, key: str, default=None) -> Any:
    """Safely retrieve a value from a dictionary with a default fallback and observability."""
    trace_id = get_trace_id()
    try:
        value = d.get(key, default)
        log_event(
            service="utils",
            event="safe_get",
            status="success",
            message=f"Retrieved value for key: {key}",
            trace_id=trace_id,
            key=key,
            value_found=key in d
        )
        return value
    except Exception as e:
        log_event(
            service="utils",
            event="safe_get",
            status="error",
            message=f"Error retrieving key {key}: {str(e)}",
            trace_id=trace_id,
            key=key
        )
        _safe_inc_metric("utils_safe_get_failures_total")
        return default

def safe_json_loads(data: str, default=None) -> Any:
    """Safely parse JSON string with comprehensive observability."""
    trace_id = get_trace_id()
    try:
        result = json.loads(data)
        log_event(
            service="utils",
            event="safe_json_loads",
            status="success",
            message="JSON parsed successfully",
            trace_id=trace_id,
            data_length=len(data)
        )
        return result
    except Exception as e:
        log_event(
            service="utils",
            event="safe_json_loads",
            status="error",
            message=f"JSON parsing failed: {str(e)}",
            trace_id=trace_id,
            data_preview=str(data)[:100]  # First 100 chars for context
        )
        _safe_inc_metric("utils_json_load_failures_total")
        return default

def safe_json_dumps(obj: Any, default: Optional[str] = None) -> str:
    """Safely serialize object to JSON with observability."""
    trace_id = get_trace_id()
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception as e:
        log_event(
            service="utils",
            event="safe_json_dumps",
            status="error",
            message=f"JSON serialization failed: {str(e)}",
            trace_id=trace_id,
            obj_type=type(obj).__name__
        )
        _safe_inc_metric("utils_json_dump_failures_total")
        return default if isinstance(default, str) else "null"

def retry_operation(func: Callable, retries: int = 3, delay: float = 1.0, 
                   backoff: float = 2.0, operation_name: str = "unknown") -> Any:
    """
    Retry operation with exponential backoff and comprehensive observability.
    """
    trace_id = get_trace_id()
    current_delay = delay
    last_exception = None
    
    for attempt in range(1, retries + 1):
        try:
            result = func()
            if attempt > 1:  # Only log if retry was actually needed
                log_event(
                    service="utils",
                    event="retry_operation",
                    status="success",
                    message=f"Operation succeeded on attempt {attempt}",
                    trace_id=trace_id,
                    operation_name=operation_name,
                    attempt=attempt,
                    total_retries=retries
                )
            return result
        except Exception as e:
            last_exception = e
            log_event(
                service="utils",
                event="retry_operation",
                status="warning" if attempt < retries else "error",
                message=f"Attempt {attempt}/{retries} failed: {str(e)}",
                trace_id=trace_id,
                operation_name=operation_name,
                attempt=attempt,
                total_retries=retries,
                error_type=type(e).__name__
            )
            _safe_inc_metric("utils_retry_attempts_total")
            
            if attempt < retries:
                time.sleep(current_delay)
                current_delay *= backoff
    
    # All retries failed
    log_event(
        service="utils",
        event="retry_operation",
        status="error",
        message="All retry attempts failed",
        trace_id=trace_id,
        operation_name=operation_name,
        total_attempts=retries
    )
    _safe_inc_metric("utils_retry_failures_total")
    raise RuntimeError(f"retry_operation failed for {operation_name} after {retries} attempts") from last_exception

def _is_circuit_open(key: str = "circuit_breaker:utils:state") -> bool:
    """Check if circuit breaker is open for a specific operation."""
    try:
        from redis_client import get_redis_client
        client = get_redis_client()
        if not client:
            return False
        state = client.get(key)
        state = _decode_redis_value(state) or ""
        return state.lower() == "open"
    except Exception:
        return False  # Default to closed on failure

def ensure_trace_session(trace_id: str = None, session_id: str = None) -> Dict[str, str]:
    """
    Ensure trace and session context exists for propagation with observability.
    Returns dict with trace_id and session_id.
    """
    result_trace_id = trace_id or get_trace_id()
    result_session_id = session_id or generate_id()
    
    log_event(
        service="utils",
        event="ensure_trace_session",
        status="success",
        message="Trace session ensured",
        trace_id=result_trace_id,
        session_id=result_session_id,
        trace_id_provided=trace_id is not None,
        session_id_provided=session_id is not None
    )
    
    return {
        "trace_id": result_trace_id,
        "session_id": result_session_id
    }

def self_test() -> Dict[str, Any]:
    """Comprehensive self-test for utility functions with observability."""
    trace_id = get_trace_id()
    results = {}
    
    try:
        # Test UUID generation
        test_uuid = generate_id()
        results["uuid_generation"] = bool(test_uuid and len(test_uuid) > 0)
        
        # Test safe_get
        test_dict = {"test_key": "test_value"}
        results["safe_get"] = safe_get(test_dict, "test_key") == "test_value"
        results["safe_get_default"] = safe_get(test_dict, "missing_key", "default") == "default"
        
        # Test JSON operations
        test_json = '{"test": "value"}'
        results["json_loads"] = safe_json_loads(test_json) == {"test": "value"}
        
        # Test JSON dumps with round-trip equality
        d = safe_json_dumps({"test": "value"})
        try:
            results["json_dumps"] = json.loads(d) == {"test": "value"}
        except Exception:
            results["json_dumps"] = False
        
        # Test retry operation (with a simple successful function)
        results["retry_success"] = retry_operation(lambda: 42, retries=1) == 42
        
        # Test trace session
        trace_session = ensure_trace_session()
        results["trace_session"] = "trace_id" in trace_session and "session_id" in trace_session
        
        overall_success = all(results.values())
        status = "success" if overall_success else "degraded"
        
        log_event(
            service="utils",
            event="self_test",
            status=status,
            message=f"Self-test completed: {sum(results.values())}/{len(results)} passed",
            trace_id=trace_id,
            results=results
        )
        
        if overall_success:
            _safe_inc_metric("utils_self_test_success_total")
        else:
            _safe_inc_metric("utils_self_test_failures_total")
            
    except Exception as e:
        log_event(
            service="utils",
            event="self_test",
            status="error",
            message=f"Self-test failed: {str(e)}",
            trace_id=trace_id
        )
        _safe_inc_metric("utils_self_test_failures_total")
        results["error"] = str(e)
    
    return results

# Deterministic exports
__all__ = [
    "generate_id",
    "safe_get", 
    "safe_json_loads",
    "safe_json_dumps",
    "retry_operation",
    "ensure_trace_session",
    "self_test",
    "restore_metrics_snapshot",
    "_safe_inc_metric",
    "_is_circuit_open",
    "_decode_redis_value"
]

# Run self-test when executed directly
if __name__ == "__main__":
    print("Running utils.py self-test...")
    test_results = self_test()
    print("Self-test results:")
    for test_name, result in test_results.items():
        print(f"  {test_name}: {result}")