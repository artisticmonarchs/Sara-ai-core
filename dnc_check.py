"""
dnc_check.py — Phase 12 Compliant
Automatically hardened by phase12_auto_hardener.py
"""

"""
dnc_check.py
Simple suppression (Do-Not-Call) list checker.
By default it uses Redis set 'sara:dnc:numbers' if redis_client.get_client exists.
Fallback: loads from a local file specified by env var DNC_FILE (one number per line).
"""
import os
import uuid
import time
import signal
from threading import Timer
from typing import Optional, Callable

# Centralized configuration with proper imports
try:
    from config import Config
    DNC_FILE = getattr(Config, "DNC_FILE", "dnc_list.txt")
    DNC_SAFE_MODE_ON_FAILURE = getattr(Config, "DNC_SAFE_MODE_ON_FAILURE", True)
    DNC_REDIS_KEY = getattr(Config, "DNC_REDIS_KEY", "sara:dnc:numbers")
    DNC_FILE_OPERATION_TIMEOUT = getattr(Config, "DNC_FILE_OPERATION_TIMEOUT", 5)  # seconds
except ImportError:
    # Minimal fallback - these should be defined in config.py
    DNC_FILE = "dnc_list.txt"
    DNC_SAFE_MODE_ON_FAILURE = True
    DNC_REDIS_KEY = "sara:dnc:numbers"
    DNC_FILE_OPERATION_TIMEOUT = 5

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

# Metrics with lazy loading and proper namespacing
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
        from redis_client import get_client, safe_redis_operation
        client = get_client()
        # Test connection if possible
        if hasattr(client, 'ping'):
            try:
                client.ping()
            except Exception:
                log_event("dnc_check", "redis_connection_test_failed", "warning",
                         "Redis connection test failed, may need reconnection",
                         trace_id=get_trace_id())
        return client, safe_redis_operation
    except Exception:
        return None, None

def _mask_phone_number(phone: str) -> str:
    """Mask phone number for logging to protect PII"""
    if not phone or len(phone) < 4:
        return "***"
    return phone[-4:].rjust(len(phone), '*')

def _run_with_timeout(func: Callable, timeout: int, default=None):
    """
    Run a function with timeout protection.
    Returns the function result or default value if timeout occurs.
    """
    class TimeoutError(Exception):
        pass
    
    def timeout_handler(signum, frame):
        raise TimeoutError("Operation timed out")
    
    # Only use signal timeout on main thread
    try:
        if timeout > 0:
            # Set up signal alarm for timeout
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)
        
        try:
            result = func()
            return result
        finally:
            if timeout > 0:
                signal.alarm(0)  # Cancel alarm
                signal.signal(signal.SIGALRM, old_handler)  # Restore handler
    except (TimeoutError, Exception):
        return default

def _safe_file_operation(operation: Callable, operation_name: str, fallback=None):
    """
    Safely execute file operations with timeout and error handling
    """
    try:
        return _run_with_timeout(operation, DNC_FILE_OPERATION_TIMEOUT, fallback)
    except Exception as e:
        log_event("dnc_check", f"file_operation_failed", "warning",
                 f"File operation '{operation_name}' failed: {e}",
                 operation=operation_name, error=str(e), trace_id=get_trace_id())
        return fallback

def is_suppressed(phone: str) -> bool:
    """
    Check if phone number is in Do-Not-Call list.
    Returns: True if number is suppressed, False if allowed to dial
    """
    trace_id = get_trace_id()
    phone = (phone or "").strip()
    masked_phone = _mask_phone_number(phone)
    
    if not phone:
        log_event("dnc_check", "empty_phone_check", "debug",  # Demoted to debug
                  "Empty phone number provided for DNC check",
                  trace_id=trace_id)
        # Safe mode: treat empty numbers as suppressed
        return DNC_SAFE_MODE_ON_FAILURE
    
    start_time = time.time()
    
    # Try Redis first with safe operation wrapper
    redis_client, safe_redis_operation = _get_redis()
    if redis_client and safe_redis_operation:
        def _redis_dnc_check(client):
            return client.sismember(DNC_REDIS_KEY, phone)
        
        try:
            is_suppressed_result = safe_redis_operation(
                _redis_dnc_check,
                fallback=None,
                operation_name="dnc_redis_check"
            )
            
            if is_suppressed_result is not None:
                # Metrics - lazy loaded with proper namespacing
                increment_metric, observe_latency = _get_metrics()
                increment_metric("sara_dnc_redis_check_total")
                
                latency_ms = (time.time() - start_time) * 1000
                # TODO: Move hardcoded port number to config.py
                observe_latency("sara_dnc_redis_check_latency_ms", latency_ms)
                
                if is_suppressed_result:
                    increment_metric("sara_dnc_number_suppressed_total")
                    log_event("dnc_check", "number_suppressed_redis", "info",
                              f"Phone number suppressed via Redis DNC: {masked_phone}",
                              phone_masked=masked_phone, source="redis", trace_id=trace_id)
                    return True
                else:
                    log_event("dnc_check", "number_allowed_redis", "debug",
                              f"Phone number allowed via Redis DNC: {masked_phone}",
                              phone_masked=masked_phone, source="redis", trace_id=trace_id)
                    return False
                
        except Exception as e:
            # Metrics - lazy loaded with proper namespacing
            increment_metric, _ = _get_metrics()
            increment_metric("sara_dnc_redis_check_failed_total")
            
            log_event("dnc_check", "redis_dnc_check_failed", "warning",  # Demoted to warning
                      f"Redis DNC check failed: {e}",
                      phone_masked=masked_phone, error=str(e), trace_id=trace_id)
    
    # Fallback to file-based DNC check with timeout protection
    file_start_time = time.time()
    
    def file_check_operation():
        if os.path.exists(DNC_FILE):
            with open(DNC_FILE, "r", encoding="utf-8") as fh:
                # Use set for O(1) lookups
                nums = set(line.strip() for line in fh if line.strip())
            return phone in nums
        return False
    
    is_suppressed_result = _safe_file_operation(
        file_check_operation, 
        "dnc_file_check", 
        fallback=None
    )
    
    if is_suppressed_result is not None:
        # Metrics - lazy loaded with proper namespacing
        increment_metric, observe_latency = _get_metrics()
        increment_metric("sara_dnc_file_check_total")
        
        file_latency_ms = (time.time() - file_start_time) * 1000
        # TODO: Move hardcoded port number to config.py
        observe_latency("sara_dnc_file_check_latency_ms", file_latency_ms)
        
        if is_suppressed_result:
            increment_metric("sara_dnc_number_suppressed_total")
            log_event("dnc_check", "number_suppressed_file", "info",
                      f"Phone number suppressed via file DNC: {masked_phone}",
                      phone_masked=masked_phone, source="file", trace_id=trace_id)
            return True
        else:
            log_event("dnc_check", "number_allowed_file", "debug",
                      f"Phone number allowed via file DNC: {masked_phone}",
                      phone_masked=masked_phone, source="file", trace_id=trace_id)
            return False
    else:
        # File operation failed or timed out
        increment_metric, _ = _get_metrics()
        increment_metric("sara_dnc_file_check_failed_total")
        
        log_event("dnc_check", "file_dnc_check_failed", "warning",  # Demoted to warning
                  f"Failed to read DNC file for: {masked_phone}",
                  phone_masked=masked_phone, dnc_file=DNC_FILE, trace_id=trace_id)
    
    # Default behavior on failure - safe mode prevents dialing
    if DNC_SAFE_MODE_ON_FAILURE:
        log_event("dnc_check", "dnc_check_fallback_suppress", "warning",
                  f"DNC check failed for both Redis and file, SAFE MODE: suppressing call: {masked_phone}",
                  phone_masked=masked_phone, trace_id=trace_id)
        increment_metric, _ = _get_metrics()
        increment_metric("sara_dnc_safe_mode_suppressions_total")
        return True
    else:
        log_event("dnc_check", "dnc_check_fallback_allow", "warning",
                  f"DNC check failed for both Redis and file, allowing call: {masked_phone}",
                  phone_masked=masked_phone, trace_id=trace_id)
        increment_metric, _ = _get_metrics()
        increment_metric("sara_dnc_safe_mode_allowed_total")
        return False

def add_to_dnc(phone: str) -> bool:
    """Add phone number to Do-Not-Call list"""
    trace_id = get_trace_id()
    phone = (phone or "").strip()
    masked_phone = _mask_phone_number(phone)
    
    if not phone:
        log_event("dnc_check", "empty_phone_add", "debug",  # Demoted to debug
                  "Empty phone number provided for DNC addition",
                  trace_id=trace_id)
        return False
    
    # Try Redis first with safe operation wrapper
    redis_client, safe_redis_operation = _get_redis()
    if redis_client and safe_redis_operation:
        def _redis_dnc_add(client):
            return client.sadd(DNC_REDIS_KEY, phone)
        
        try:
            result = safe_redis_operation(
                _redis_dnc_add,
                fallback=None,
                operation_name="dnc_redis_add"
            )
            
            if result is not None:
                # Metrics - lazy loaded with proper namespacing
                increment_metric, _ = _get_metrics()
                increment_metric("sara_dnc_number_added_redis_total")
                
                log_event("dnc_check", "number_added_redis", "info",
                          f"Phone number added to Redis DNC: {masked_phone}",
                          phone_masked=masked_phone, trace_id=trace_id)
                return True
                
        except Exception as e:
            # Metrics - lazy loaded with proper namespacing
            increment_metric, _ = _get_metrics()
            increment_metric("sara_dnc_redis_add_failed_total")
            
            log_event("dnc_check", "redis_dnc_add_failed", "warning",  # Demoted to warning
                      f"Failed to add to Redis DNC: {e}",
                      phone_masked=masked_phone, error=str(e), trace_id=trace_id)
    
    # Fallback to file-based DNC addition with timeout protection
    def file_add_operation():
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(DNC_FILE)), exist_ok=True)
        
        with open(DNC_FILE, "a", encoding="utf-8") as fh:
            fh.write(phone + "\n")
        return True
    
    result = _safe_file_operation(file_add_operation, "dnc_file_add", fallback=False)
    
    if result:
        # Metrics - lazy loaded with proper namespacing
        increment_metric, _ = _get_metrics()
        increment_metric("sara_dnc_number_added_file_total")
        
        log_event("dnc_check", "number_added_file", "info",
                  f"Phone number added to file DNC: {masked_phone}",
                  phone_masked=masked_phone, dnc_file=DNC_FILE, trace_id=trace_id)
        return True
    else:
        # Metrics - lazy loaded with proper namespacing
        increment_metric, _ = _get_metrics()
        increment_metric("sara_dnc_file_add_failed_total")
        
        log_event("dnc_check", "file_dnc_add_failed", "warning",  # Demoted to warning
                  f"Failed to add to file DNC: {masked_phone}",
                  phone_masked=masked_phone, dnc_file=DNC_FILE, trace_id=trace_id)
        return False

def remove_from_dnc(phone: str) -> bool:
    """Remove phone number from Do-Not-Call list"""
    trace_id = get_trace_id()
    phone = (phone or "").strip()
    masked_phone = _mask_phone_number(phone)
    
    if not phone:
        log_event("dnc_check", "empty_phone_remove", "debug",  # Demoted to debug
                  "Empty phone number provided for DNC removal",
                  trace_id=trace_id)
        return False
    
    # Try Redis first with safe operation wrapper
    redis_client, safe_redis_operation = _get_redis()
    if redis_client and safe_redis_operation:
        def _redis_dnc_remove(client):
            return client.srem(DNC_REDIS_KEY, phone)
        
        try:
            result = safe_redis_operation(
                _redis_dnc_remove,
                fallback=None,
                operation_name="dnc_redis_remove"
            )
            
            if result is not None:
                # Metrics - lazy loaded with proper namespacing
                increment_metric, _ = _get_metrics()
                increment_metric("sara_dnc_number_removed_redis_total")
                
                log_event("dnc_check", "number_removed_redis", "info",
                          f"Phone number removed from Redis DNC: {masked_phone}",
                          phone_masked=masked_phone, trace_id=trace_id)
                return True
                
        except Exception as e:
            # Metrics - lazy loaded with proper namespacing
            increment_metric, _ = _get_metrics()
            increment_metric("sara_dnc_redis_remove_failed_total")
            
            log_event("dnc_check", "redis_dnc_remove_failed", "warning",  # Demoted to warning
                      f"Failed to remove from Redis DNC: {e}",
                      phone_masked=masked_phone, error=str(e), trace_id=trace_id)
    
    # Fallback to file-based DNC removal with timeout protection
    def file_remove_operation():
        if os.path.exists(DNC_FILE):
            with open(DNC_FILE, "r", encoding="utf-8") as fh:
                lines = fh.readlines()
            
            # Remove the number from the list
            new_lines = [line for line in lines if line.strip() != phone]
            
            # Only write back if something changed
            if len(new_lines) != len(lines):
                with open(DNC_FILE, "w", encoding="utf-8") as fh:
                    fh.writelines(new_lines)
                return True
        return False
    
    result = _safe_file_operation(file_remove_operation, "dnc_file_remove", fallback=False)
    
    if result:
        # Metrics - lazy loaded with proper namespacing
        increment_metric, _ = _get_metrics()
        increment_metric("sara_dnc_number_removed_file_total")
        
        log_event("dnc_check", "number_removed_file", "info",
                  f"Phone number removed from file DNC: {masked_phone}",
                  phone_masked=masked_phone, dnc_file=DNC_FILE, trace_id=trace_id)
        return True
    else:
        log_event("dnc_check", "number_not_found_file", "debug",
                  f"Phone number not found in file DNC: {masked_phone}",
                  phone_masked=masked_phone, dnc_file=DNC_FILE, trace_id=trace_id)
        return False

def get_dnc_stats() -> dict:
    """Get statistics about the DNC list"""
    stats = {
        "redis_available": False,
        "file_available": False,
        "redis_count": 0,
        "file_count": 0,
        "safe_mode_enabled": DNC_SAFE_MODE_ON_FAILURE
    }
    
    # Check Redis
    redis_client, safe_redis_operation = _get_redis()
    if redis_client and safe_redis_operation:
        try:
            def _redis_count(client):
                return client.scard(DNC_REDIS_KEY)
            
            count = safe_redis_operation(_redis_count, fallback=0, operation_name="dnc_redis_count")
            stats["redis_available"] = True
            stats["redis_count"] = count
        except Exception:
            pass
    
    # Check file with timeout protection
    def file_count_operation():
        if os.path.exists(DNC_FILE):
            with open(DNC_FILE, "r", encoding="utf-8") as fh:
                lines = [line.strip() for line in fh if line.strip()]
            return len(lines)
        return 0
    
    file_count = _safe_file_operation(file_count_operation, "dnc_file_count", fallback=0)
    if file_count is not None:
        stats["file_available"] = True
        stats["file_count"] = file_count
    
    return stats

# Simple test function for basic validation
def _simple_test():
    """Basic functionality test - for comprehensive tests see tests/test_dnc_check.py"""
    test_number = "+1234567890"
    masked_number = _mask_phone_number(test_number)
    
    logger.info(f"Testing DNC check with number: {masked_number}")
    
    # Test basic operations
    result = is_suppressed(test_number)
    logger.info(f"is_suppressed result: {result}")
    
    stats = get_dnc_stats()
    logger.info(f"DNC stats: {stats}")
    
    logger.info("Basic DNC check validation completed")

if __name__ == "__main__":
    _simple_test()