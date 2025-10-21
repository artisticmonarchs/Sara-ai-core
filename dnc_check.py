"""
dnc_check.py
Simple suppression (Do-Not-Call) list checker.
By default it uses Redis set 'dnc_numbers' if redis_client.get_redis_client exists.
Fallback: loads from a local file specified by env var DNC_FILE (one number per line).
"""
import os
import uuid

# Centralized configuration
try:
    from config import Config
    DNC_FILE = getattr(Config, "DNC_FILE", "dnc_list.txt")
except ImportError:
    # Minimal fallback
    DNC_FILE = "dnc_list.txt"

# Structured logging with lazy shim
def _get_logger():
    try:
        from logging_utils import log_event
        return log_event
    except Exception:
        def _noop_log(*a, **k): pass
        return _noop_log

log_event = _get_logger()

# Trace ID with fallback
def get_trace_id():
    try:
        from logging_utils import get_trace_id as _get_trace_id
        return _get_trace_id()
    except Exception:
        return str(uuid.uuid4())[:8]

# Metrics with lazy loading
def _get_metrics():
    try:
        from metrics_collector import increment_metric, observe_latency
        return increment_metric, observe_latency
    except ImportError:
        def _noop_metric(*args, **kwargs): pass
        def _noop_latency(*args, **kwargs): pass
        return _noop_metric, _noop_latency

def _get_redis():
    try:
        # CORRECTED: Use get_client() instead of get_redis_client()
        from redis_client import get_client, safe_redis_operation
        return get_client(), safe_redis_operation
    except Exception:
        return None, None

def is_suppressed(phone: str) -> bool:
    trace_id = get_trace_id()
    phone = (phone or "").strip()
    
    if not phone:
        log_event("dnc_check", "empty_phone_check", "warning",
                  "Empty phone number provided for DNC check",
                  trace_id=trace_id)
        return False
    
    start_time = time.time() if 'time' in globals() else None
    
    # Try Redis first with safe operation wrapper
    redis_client, safe_redis_operation = _get_redis()
    if redis_client and safe_redis_operation:
        def _redis_dnc_check(client):
            return client.sismember("dnc_numbers", phone)
        
        try:
            is_suppressed_result = safe_redis_operation(
                _redis_dnc_check,
                fallback=None,
                operation_name="dnc_redis_check"
            )
            
            if is_suppressed_result is not None:
                # Metrics - lazy loaded
                increment_metric, observe_latency = _get_metrics()
                increment_metric("dnc.redis_check")
                
                if start_time and 'time' in globals():
                    latency_ms = (time.time() - start_time) * 1000
                    observe_latency("dnc.redis_check_latency", latency_ms)
                
                if is_suppressed_result:
                    increment_metric("dnc.number_suppressed")
                    log_event("dnc_check", "number_suppressed_redis", "info",
                              f"Phone number suppressed via Redis DNC: {phone}",
                              phone=phone, source="redis", trace_id=trace_id)
                else:
                    log_event("dnc_check", "number_allowed_redis", "debug",
                              f"Phone number allowed via Redis DNC: {phone}",
                              phone=phone, source="redis", trace_id=trace_id)
                
                return bool(is_suppressed_result)
                
        except Exception as e:
            # Metrics - lazy loaded
            increment_metric, _ = _get_metrics()
            increment_metric("dnc.redis_check_failed")
            
            log_event("dnc_check", "redis_dnc_check_failed", "error",
                      f"Redis DNC check failed: {e}",
                      phone=phone, error=str(e), trace_id=trace_id)
    
    # Fallback to file-based DNC check
    import time
    start_time = time.time() if 'time' in globals() else None
    
    try:
        if os.path.exists(DNC_FILE):
            with open(DNC_FILE, "r", encoding="utf-8") as fh:
                nums = set(line.strip() for line in fh if line.strip())
            
            is_suppressed_result = phone in nums
            
            # Metrics - lazy loaded
            increment_metric, observe_latency = _get_metrics()
            increment_metric("dnc.file_check")
            
            if start_time and 'time' in globals():
                latency_ms = (time.time() - start_time) * 1000
                observe_latency("dnc.file_check_latency", latency_ms)
            
            if is_suppressed_result:
                increment_metric("dnc.number_suppressed")
                log_event("dnc_check", "number_suppressed_file", "info",
                          f"Phone number suppressed via file DNC: {phone}",
                          phone=phone, source="file", trace_id=trace_id)
            else:
                log_event("dnc_check", "number_allowed_file", "debug",
                          f"Phone number allowed via file DNC: {phone}",
                          phone=phone, source="file", trace_id=trace_id)
            
            return is_suppressed_result
            
    except Exception as e:
        # Metrics - lazy loaded
        increment_metric, _ = _get_metrics()
        increment_metric("dnc.file_check_failed")
        
        log_event("dnc_check", "file_dnc_check_failed", "error",
                  f"Failed to read DNC file: {e}",
                  phone=phone, dnc_file=DNC_FILE, error=str(e), trace_id=trace_id)
    
    # Default to allowing calls if both methods fail
    log_event("dnc_check", "dnc_check_fallback", "warning",
              f"DNC check failed for both Redis and file, allowing call: {phone}",
              phone=phone, trace_id=trace_id)
    return False

def add_to_dnc(phone: str):
    trace_id = get_trace_id()
    phone = (phone or "").strip()
    
    if not phone:
        log_event("dnc_check", "empty_phone_add", "warning",
                  "Empty phone number provided for DNC addition",
                  trace_id=trace_id)
        return False
    
    # Try Redis first with safe operation wrapper
    redis_client, safe_redis_operation = _get_redis()
    if redis_client and safe_redis_operation:
        def _redis_dnc_add(client):
            return client.sadd("dnc_numbers", phone)
        
        try:
            result = safe_redis_operation(
                _redis_dnc_add,
                fallback=None,
                operation_name="dnc_redis_add"
            )
            
            if result is not None:
                # Metrics - lazy loaded
                increment_metric, _ = _get_metrics()
                increment_metric("dnc.number_added_redis")
                
                log_event("dnc_check", "number_added_redis", "info",
                          f"Phone number added to Redis DNC: {phone}",
                          phone=phone, trace_id=trace_id)
                return True
                
        except Exception as e:
            # Metrics - lazy loaded
            increment_metric, _ = _get_metrics()
            increment_metric("dnc.redis_add_failed")
            
            log_event("dnc_check", "redis_dnc_add_failed", "error",
                      f"Failed to add to Redis DNC: {e}",
                      phone=phone, error=str(e), trace_id=trace_id)
    
    # Fallback to file-based DNC addition
    try:
        with open(DNC_FILE, "a", encoding="utf-8") as fh:
            fh.write(phone + "\n")
        
        # Metrics - lazy loaded
        increment_metric, _ = _get_metrics()
        increment_metric("dnc.number_added_file")
        
        log_event("dnc_check", "number_added_file", "info",
                  f"Phone number added to file DNC: {phone}",
                  phone=phone, dnc_file=DNC_FILE, trace_id=trace_id)
        return True
        
    except Exception as e:
        # Metrics - lazy loaded
        increment_metric, _ = _get_metrics()
        increment_metric("dnc.file_add_failed")
        
        log_event("dnc_check", "file_dnc_add_failed", "error",
                  f"Failed to add to file DNC: {e}",
                  phone=phone, dnc_file=DNC_FILE, error=str(e), trace_id=trace_id)
        return False

# Import time at the end to avoid side effects
import time