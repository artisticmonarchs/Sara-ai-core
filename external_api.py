"""
external_api.py — Phase 2 External API Wrapper
Standardized retry logic, circuit breaker, and metrics for all external API calls.
"""

import asyncio
import time
import functools
from typing import Optional, Dict, Any, Callable, TypeVar, Union
import logging

# Phase 2: Configuration imports
try:
    from config import config
    # Use config values if available, otherwise fallback to environment
    API_MAX_RETRIES = getattr(config, 'API_MAX_RETRIES', 3)
    API_TIMEOUT = getattr(config, 'API_TIMEOUT', 30)
    API_BACKOFF_BASE = getattr(config, 'API_BACKOFF_BASE', 2.0)
except ImportError:
    # Fallback to environment variables if Config not available
    import os
    API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "3"))
    API_TIMEOUT = int(os.getenv("API_TIMEOUT", "30"))
    API_BACKOFF_BASE = float(os.getenv("API_BACKOFF_BASE", "2.0"))

# Metrics integration
try:
    from metrics_collector import increment_metric, observe_latency
except ImportError:
    def increment_metric(*args, **kwargs): pass
    def observe_latency(*args, **kwargs): pass

# Redis circuit breaker
try:
    from redis_client import get_redis_client, safe_redis_operation
except ImportError:
    def get_redis_client(): return None
    def safe_redis_operation(operation, fallback=None, operation_name=None):
        try: return operation()
        except: return fallback

# Structured logging
try:
    from logging_utils import log_event
except ImportError:
    def log_event(service, event, status, message, trace_id=None, extra=None):
        logging.info(f"[{service}] {event}: {message}")

T = TypeVar('T')

class CircuitBreaker:
    """Redis-backed circuit breaker implementation"""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.failure_threshold = 3
        self.cooldown_period = 300  # 5 minutes
        
    def is_open(self) -> bool:
        """Check if circuit breaker is open"""
        client = get_redis_client()
        if not client:
            return False
            
        try:
            state_key = f"circuit_breaker:{self.service_name}:state"
            state = safe_redis_operation(lambda: client.get(state_key))
            
            if not state:
                return False
                
            if isinstance(state, bytes):
                state = state.decode("utf-8")
                
            if state.lower() == "open":
                # Check if cooldown period has expired
                cooldown_key = f"circuit_breaker:{self.service_name}:cooldown_until"
                cooldown_until = safe_redis_operation(lambda: client.get(cooldown_key))
                
                if cooldown_until:
                    try:
                        if time.time() < float(cooldown_until):
                            return True  # Still in cooldown
                        else:
                            # Cooldown expired, transition to half-open
                            safe_redis_operation(
                                lambda: client.set(state_key, "half-open")
                            )
                            return False
                    except (ValueError, TypeError):
                        return True
                return True
                
            return False
            
        except Exception as e:
            log_event(
                service="external_api",
                event="circuit_breaker_check_error",
                status="warn",
                message=f"Error checking circuit breaker: {e}",
                extra={"service": self.service_name}
            )
            return False
    
    def report_failure(self):
        """Report failure to circuit breaker"""
        client = get_redis_client()
        if not client:
            return
            
        try:
            fail_key = f"circuit_breaker:{self.service_name}:failures"
            state_key = f"circuit_breaker:{self.service_name}:state"
            
            # Increment failure count
            fail_count = safe_redis_operation(lambda: client.incr(fail_key))
            
            # Set expiration on first failure
            if fail_count == 1:
                safe_redis_operation(lambda: client.expire(fail_key, 60))
            
            # Open circuit breaker if threshold exceeded
            if fail_count >= self.failure_threshold:
                safe_redis_operation(lambda: client.set(state_key, "open"))
                cooldown_until = time.time() + self.cooldown_period
                safe_redis_operation(
                    lambda: client.set(
                        f"circuit_breaker:{self.service_name}:cooldown_until",
                        str(cooldown_until)
                    )
                )
                
                increment_metric(f"{self.service_name}.circuit_breaker.opened")
                log_event(
                    service="external_api",
                    event="circuit_breaker_opened",
                    status="warn",
                    message=f"Circuit breaker opened for {self.service_name}",
                    extra={"fail_count": fail_count}
                )
                
        except Exception as e:
            log_event(
                service="external_api",
                event="circuit_breaker_update_error",
                status="error",
                message=f"Failed to update circuit breaker: {e}",
                extra={"service": self.service_name}
            )
    
    def report_success(self):
        """Report success to circuit breaker"""
        client = get_redis_client()
        if not client:
            return
            
        try:
            fail_key = f"circuit_breaker:{self.service_name}:failures"
            state_key = f"circuit_breaker:{self.service_name}:state"
            
            current_state = safe_redis_operation(lambda: client.get(state_key))
            
            # If in half-open state, close the circuit
            if current_state and current_state.decode("utf-8") == "half-open":
                safe_redis_operation(lambda: client.set(state_key, "closed"))
                safe_redis_operation(lambda: client.delete(fail_key))
                safe_redis_operation(
                    lambda: client.delete(f"circuit_breaker:{self.service_name}:cooldown_until")
                )
                
                increment_metric(f"{self.service_name}.circuit_breaker.closed")
                log_event(
                    service="external_api",
                    event="circuit_breaker_closed",
                    status="info",
                    message=f"Circuit breaker closed for {self.service_name}"
                )
            elif not current_state or current_state.decode("utf-8") == "closed":
                # Normal operation, reset failure count
                safe_redis_operation(lambda: client.delete(fail_key))
                
        except Exception as e:
            log_event(
                service="external_api",
                event="circuit_breaker_success_error",
                status="warn",
                message=f"Failed to report success to circuit breaker: {e}",
                extra={"service": self.service_name}
            )


async def exponential_backoff(retry_count: int, base_delay: float = None) -> float:
    """Calculate exponential backoff delay with jitter"""
    if base_delay is None:
        base_delay = API_BACKOFF_BASE
    
    delay = base_delay * (2 ** retry_count)
    jitter = delay * 0.1  # 10% jitter
    return min(delay + jitter, 30.0)  # Max 30 seconds


def is_retryable_error(error: Exception) -> bool:
    """Check if an error is retryable"""
    error_str = str(error).lower()
    
    # Network and timeout errors
    if any(keyword in error_str for keyword in [
        "timeout", "connection", "network", "socket", "temporarily", "busy"
    ]):
        return True
    
    # HTTP status code patterns
    if any(code in error_str for code in ['500', '502', '503', '504', '429']):
        return True
        
    # Specific exception types
    error_type = type(error).__name__.lower()
    if any(keyword in error_type for keyword in [
        "timeout", "connection", "retry", "rate", "limit"
    ]):
        return True
        
    return False


async def external_api_call_async(
    service: str,
    operation: Callable,
    *args,
    trace_id: Optional[str] = None,
    **kwargs
) -> Any:
    """
    Make an external API call with standardized retry logic, circuit breaker, and metrics.
    
    Args:
        service: Service name for metrics and circuit breaker
        operation: Async function to call
        *args: Arguments to pass to operation
        trace_id: Trace ID for logging
        **kwargs: Keyword arguments to pass to operation
        
    Returns:
        Result of the operation
        
    Raises:
        Exception: If all retries fail
    """
    start_time = time.time()
    trace_id = trace_id or f"{service}_call_{int(time.time())}"
    circuit_breaker = CircuitBreaker(service)
    
    # Check circuit breaker
    if circuit_breaker.is_open():
        increment_metric(f"{service}.call.circuit_breaker")
        log_event(
            service="external_api",
            event="circuit_breaker_blocked",
            status="warn",
            message=f"API call blocked by circuit breaker: {service}",
            trace_id=trace_id,
            extra={"service": service}
        )
        raise Exception(f"Circuit breaker open for {service}")
    
    max_retries = API_MAX_RETRIES
    last_error = None
    
    for retry_count in range(max_retries + 1):
        try:
            # Make the API call
            result = await operation(*args, **kwargs)
            
            # Record success
            latency_ms = (time.time() - start_time) * 1000
            increment_metric(f"{service}.call.success")
            observe_latency(f"{service}.call.latency", latency_ms)
            circuit_breaker.report_success()
            
            log_event(
                service="external_api",
                event="api_call_success",
                status="info",
                message=f"API call successful: {service}",
                trace_id=trace_id,
                extra={
                    "service": service,
                    "retry_count": retry_count,
                    "latency_ms": round(latency_ms, 2)
                }
            )
            
            return result
            
        except Exception as e:
            last_error = e
            
            if retry_count < max_retries and is_retryable_error(e):
                # Retry with exponential backoff
                delay = await exponential_backoff(retry_count)
                log_event(
                    service="external_api",
                    event="api_call_retry",
                    status="warn",
                    message=f"API call failed, retrying: {service}",
                    trace_id=trace_id,
                    extra={
                        "service": service,
                        "error": str(e),
                        "retry_count": retry_count,
                        "delay_seconds": delay
                    }
                )
                await asyncio.sleep(delay)
                continue
            else:
                # Final failure
                break
    
    # All retries failed
    latency_ms = (time.time() - start_time) * 1000
    increment_metric(f"{service}.call.error")
    observe_latency(f"{service}.call.error_latency", latency_ms)
    circuit_breaker.report_failure()
    
    log_event(
        service="external_api",
        event="api_call_failed",
        status="error",
        message=f"API call failed after {max_retries} retries: {service}",
        trace_id=trace_id,
        extra={
            "service": service,
            "error": str(last_error),
            "retry_count": max_retries,
            "latency_ms": round(latency_ms, 2)
        }
    )
    
    raise last_error


def external_api_call(
    service: str,
    operation: Callable,
    *args,
    trace_id: Optional[str] = None,
    **kwargs
) -> Any:
    """
    Synchronous wrapper for external_api_call_async.
    """
    return asyncio.run(external_api_call_async(service, operation, *args, trace_id=trace_id, **kwargs))


# Decorator versions for convenience
def with_external_api_async(service: str):
    """Decorator for async functions to use external API wrapper"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            trace_id = kwargs.pop('trace_id', None)
            return await external_api_call_async(
                service,
                func,
                *args,
                trace_id=trace_id,
                **kwargs
            )
        return wrapper
    return decorator


def with_external_api(service: str):
    """Decorator for sync functions to use external API wrapper"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            trace_id = kwargs.pop('trace_id', None)
            return external_api_call(
                service,
                func,
                *args,
                trace_id=trace_id,
                **kwargs
            )
        return wrapper
    return decorator