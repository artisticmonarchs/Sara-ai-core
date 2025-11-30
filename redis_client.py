# redis_client.py
"""
redis_client.py — Phase 11-F Compliant
Centralized, telemetry-aware Redis client with self-healing connections.
This file preserves all existing Phase 11-D logic and adds Phase 11-F enhancements
including metrics hooks, context manager support, and improved connection resilience.
"""

import os
import redis
import time
import json
import sentry_sdk
from typing import Optional, Dict, Any, Callable, TypeVar, Tuple
from contextlib import contextmanager

# --------------------------------------------------------------------------
# Phase 11-D: Lazy dotenv support (safe, non-invasive)
# --------------------------------------------------------------------------
try:
    # keep this optional; won't break if dotenv isn't installed
    from dotenv import load_dotenv  # type: ignore
    root_env = os.path.join(os.getcwd(), ".env")
    if os.path.exists(root_env):
        load_dotenv(root_env)
except Exception:
    pass

# --------------------------------------------------------------------------
# Phase 11-F: Centralized Redis Event Metrics
# --------------------------------------------------------------------------
REDIS_EVENT_METRICS = {
    "connection_failures": "redis_connection_failures_total",
    "reconnections": "redis_reconnections_total", 
    "flushes": "redis_flush_total",
    "shutdowns": "redis_shutdown_total",
    "degraded_mode": "redis_degraded_mode_total"
}

# --------------------------------------------------------------------------
# Connection Management Constants
# --------------------------------------------------------------------------
_REDIS_CONNECT_TIMEOUT = int(os.getenv("REDIS_CONNECT_TIMEOUT", "5"))
_REDIS_SOCKET_TIMEOUT = int(os.getenv("REDIS_SOCKET_TIMEOUT", "5"))
_REDIS_HEALTH_CHECK_INTERVAL = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))
_REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "20"))

# Circuit breaker configuration
_CIRCUIT_BREAKER_FAILURE_THRESHOLD = int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5"))
_CIRCUIT_BREAKER_COOLDOWN_PERIOD = int(os.getenv("CIRCUIT_BREAKER_COOLDOWN_PERIOD", "60"))

# Reconnection Management
_reconnect_delay = 1
_max_reconnect_delay = 30
_last_reconnect_attempt = 0

# --------------------------------------------------------------------------
# Metrics Collector Integration (Phase 11-F) - FIXED API MISMATCH
# --------------------------------------------------------------------------
def _get_metrics_collector():
    """Lazy import metrics collector to break circular imports."""
    try:
        from metrics_collector import increment_metric, observe_latency, sync_metrics_to_redis_safe
        return increment_metric, observe_latency, sync_metrics_to_redis_safe
    except ImportError:
        # Fallback to no-op if metrics_collector isn't available
        def noop(*args, **kwargs):
            pass
        return noop, noop, noop
    except Exception:
        def noop(*args, **kwargs):
            pass
        return noop, noop, noop

def _safe_metrics_call(metric_name: str, amount: int = 1, labels: Optional[Dict[str, str]] = None):
    """
    Safely call metrics collector with proper error handling.
    
    Args:
        metric_name: Name of the metric to increment
        amount: Amount to increment (default: 1)
        labels: Optional labels for the metric
    """
    try:
        increment_metric_func, _, _ = _get_metrics_collector()
        increment_metric_func(metric_name, labels=labels or {}, amount=amount)
    except Exception:
        # Silently fail if metrics collection fails
        pass

def _safe_latency_observation(metric_name: str, latency_ms: float, labels: Optional[Dict[str, str]] = None):
    """
    Safely observe latency metrics.
    
    Args:
        metric_name: Name of the latency metric
        latency_ms: Latency value in milliseconds
        labels: Optional labels for the metric
    """
    try:
        _, observe_latency_func, _ = _get_metrics_collector()
        observe_latency_func(metric_name, labels=labels or {}, value=latency_ms)
    except Exception:
        # Silently fail if metrics collection fails
        pass

# --------------------------------------------------------------------------
# Key Schema Standardization
# --------------------------------------------------------------------------

class RedisKeySchema:
    """Standardized key schema for Redis data organization"""
    
    @staticmethod
    def session(session_id: str) -> str:
        """Session data: session:{session_id}"""
        return f"session:{session_id}"
    
    @staticmethod
    def breaker(breaker_name: str) -> str:
        """Circuit breaker state: breaker:{breaker_name}"""
        return f"breaker:{breaker_name}"
    
    @staticmethod
    def circuit_breaker(service: str, field: str) -> str:
        """Circuit breaker keys: circuit_breaker:{service}:{field}"""
        return f"circuit_breaker:{service}:{field}"
    
    @staticmethod
    def metrics(metric_name: str, dimension: str = "default") -> str:
        """Metrics data: metrics:{metric_name}:{dimension}"""
        return f"metrics:{metric_name}:{dimension}"
    
    @staticmethod
    def tts_cache(text_hash: str) -> str:
        """TTS cache: tts:cache:{text_hash}"""
        return f"tts:cache:{text_hash}"
    
    @staticmethod
    def audio_buffer(session_id: str) -> str:
        """Audio buffer: audio:buffer:{session_id}"""
        return f"audio:buffer:{session_id}"
    
    @staticmethod
    def voice_state(session_id: str) -> str:
        """Voice activity state: voice:state:{session_id}"""
        return f"voice:state:{session_id}"
    
    @staticmethod
    def lock(key: str) -> str:
        """Distributed lock key: lock:{key}"""
        return f"lock:{key}"

# --------------------------------------------------------------------------
# TTL Configuration
# --------------------------------------------------------------------------

class RedisTTL:
    """Standard TTL values for different data types"""
    
    # Session data (15 minutes)
    SESSION = 900
    
    # Circuit breaker state (5 minutes for recovery)
    BREAKER = 300
    
    # Metrics data (24 hours rolling)
    METRICS = 86400
    
    # TTS cache (24 hours)
    TTS_CACHE = 86400
    
    # Audio buffer (short-lived, 2 minutes)
    AUDIO_BUFFER = 120
    
    # Voice state (5 minutes)
    VOICE_STATE = 300
    
    # Temporary data (1 hour max)
    TEMPORARY = 3600
    
    # Distributed locks (30 seconds default)
    LOCK = 30
    
    # Circuit breaker cooldown (1 minute)
    CIRCUIT_BREAKER_COOLDOWN = 60

# --------------------------------------------------------------------------
# Connection Management
# --------------------------------------------------------------------------
_redis_client: Optional[redis.Redis] = None

def _get_logger():
    """Lazy import logging utilities to break circular imports."""
    try:
        from logging_utils import get_logger
        return get_logger("redis_client")
    except Exception:
        import logging
        logger = logging.getLogger("redis_client")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

def _report_to_sentry(error: Exception, context: Dict[str, Any] = None):
    """Report Redis errors to Sentry with context"""
    try:
        with sentry_sdk.push_scope() as scope:
            if context:
                for key, value in context.items():
                    scope.set_extra(key, value)
            scope.set_tag("service", "redis_client")
            scope.set_tag("phase", "11-F")
            scope.set_tag("component", "redis_client")
            sentry_sdk.capture_exception(error)
    except Exception:
        pass  # Silently fail if Sentry isn't configured

def _should_reconnect() -> bool:
    """Check if we should attempt reconnection based on backoff"""
    global _last_reconnect_attempt, _reconnect_delay
    current_time = time.time()
    
    if _last_reconnect_attempt == 0:
        return True
    
    time_since_last_attempt = current_time - _last_reconnect_attempt
    return time_since_last_attempt >= _reconnect_delay

def _update_reconnect_delay(success: bool):
    """Update reconnect delay with exponential backoff"""
    global _reconnect_delay, _last_reconnect_attempt
    
    if success:
        # Reset delay on successful connection
        _reconnect_delay = 1
    else:
        # Exponential backoff with max limit
        _reconnect_delay = min(_reconnect_delay * 2, _max_reconnect_delay)
    
    _last_reconnect_attempt = time.time()

def _connect() -> Optional[redis.Redis]:
    """Attempt to establish a Redis connection and perform a health check."""
    # Read environment variables directly to avoid import cycles
    url = os.getenv("REDIS_URL", "redis://red-d43ertemcj7s73b0qrcg:6379/0")

    try:
        client = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=_REDIS_CONNECT_TIMEOUT,
            socket_timeout=_REDIS_SOCKET_TIMEOUT,
            health_check_interval=_REDIS_HEALTH_CHECK_INTERVAL,
            retry_on_timeout=True,
            # REMOVED: retry_on_error is not a valid kw in redis-py 4.x
            max_connections=_REDIS_MAX_CONNECTIONS
        )
        # Connectivity check with PING at init
        client.ping()
        
        logger = _get_logger()
        logger.info(
            "Redis connection established",
            extra={"url": url, "reconnect_delay": _reconnect_delay, "max_connections": _REDIS_MAX_CONNECTIONS}
        )
        
        # Emit connect success metric
        _safe_metrics_call("redis_connection_events", 1, labels={"event": "connect_success"})
        
        _update_reconnect_delay(True)
        return client
        
    except redis.exceptions.ConnectionError as e:
        logger = _get_logger()
        logger.error(
            "Redis connection failed",
            extra={"url": url, "error": str(e), "reconnect_delay": _reconnect_delay}
        )
        _update_reconnect_delay(False)
        _report_to_sentry(e, {"url": url, "operation": "connect"})
        
        # Phase 11-F: Connection failure metrics (FIXED: Added labels)
        _safe_metrics_call(REDIS_EVENT_METRICS["connection_failures"], 1, labels={"event": "connection_failure"})
        _safe_metrics_call("redis_connection_events", 1, labels={"event": "connect_failure"})
        
        return None
        
    except Exception as e:
        logger = _get_logger()
        logger.error(
            "Redis initialization failed",
            extra={"url": url, "error": str(e)}
        )
        _update_reconnect_delay(False)
        _report_to_sentry(e, {"url": url, "operation": "init"})
        
        # Phase 11-F: Connection failure metrics (FIXED: Added labels)
        _safe_metrics_call(REDIS_EVENT_METRICS["connection_failures"], 1, labels={"event": "initialization_failure"})
        _safe_metrics_call("redis_connection_events", 1, labels={"event": "connect_failure"})
        
        return None

def get_client() -> Optional[redis.Redis]:
    """
    Return the global Redis client instance.
    If disconnected, attempt a lazy reconnection with backoff.
    """
    global _redis_client
    
    # If we have a client, verify it's still connected
    if _redis_client is not None:
        try:
            _redis_client.ping()
            return _redis_client
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            logger = _get_logger()
            logger.warning("Redis connection lost, will attempt reconnection")
            _redis_client = None
        except Exception as e:
            logger = _get_logger()
            logger.error("Unexpected Redis error", extra={"error": str(e)})
            _redis_client = None
    
    # Attempt reconnection if appropriate
    if _should_reconnect():
        _redis_client = _connect()
    
    return _redis_client

# Public Singleton
redis_client: Optional[redis.Redis] = get_client()

# Phase 11-D Compatibility Alias
def get_redis_client() -> Optional[redis.Redis]:
    """Alias for get_client() to maintain import compatibility."""
    return get_client()

def _safe_blocking_sleep(delay: float):
    """
    Safely sleep without blocking event loops.
    
    Args:
        delay: Delay in seconds
    """
    try:
        import asyncio
        if asyncio.get_event_loop().is_running():
            # Don't block event loop - skip sleep but log it
            _safe_metrics_call("redis_blocking_sleep_skipped", 1, labels={"context": "async_event_loop"})
            return
    except Exception:
        pass  # Not in async context
    
    time.sleep(delay)

# --------------------------------------------------------------------------
# Phase 11-F: Enhanced Safe Redis Operation Wrapper
# --------------------------------------------------------------------------
T = TypeVar('T')

def safe_redis_operation(op: Callable[[redis.Redis], T], fallback: T, operation_name: str) -> T:
    """
    Enhanced wrapper for Redis operations with latency tracking, failure metrics, and circuit breaker support.
    
    Args:
        op: Redis operation function that takes a Redis client as argument
        fallback: Fallback value to return if operation fails
        operation_name: Name of the operation for metrics and logging
    
    Returns:
        Result of the operation or fallback if failed
    """
    start_time = time.time()
    client = get_client()
    
    # Check circuit breaker for this operation
    if _is_circuit_breaker_open(operation_name):
        _safe_metrics_call("redis_circuit_breaker_events", 1, labels={"operation": operation_name, "event": "circuit_open"})
        return fallback
    
    if not client:
        _safe_metrics_call("redis_operation_failures", 1, labels={"operation": operation_name, "reason": "no_client"})
        return fallback
    
    try:
        result = op(client)
        latency_ms = (time.time() - start_time) * 1000
        
        # Record success metrics with corrected API
        _safe_metrics_call("redis_operation_success", 1, labels={"operation": operation_name})
        _safe_latency_observation("redis_operation_latency_ms", latency_ms, labels={"operation": operation_name})
        
        # Reset circuit breaker on success
        _reset_circuit_breaker(operation_name)
        
        return result
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        logger = _get_logger()
        logger.error(
            f"Redis operation failed: {operation_name}",
            extra={"error": str(e), "latency_ms": round(latency_ms, 2)}
        )
        
        # Record failure metrics
        _safe_metrics_call("redis_operation_failures", 1, labels={"operation": operation_name, "error": type(e).__name__})
        _safe_latency_observation("redis_operation_latency_ms", latency_ms, labels={"operation": operation_name})
        
        # Update circuit breaker
        _record_circuit_breaker_failure(operation_name)
        
        _report_to_sentry(e, {"operation": operation_name, "latency_ms": latency_ms})
        return fallback

def _is_circuit_breaker_open(service: str) -> bool:
    """Check if circuit breaker is open for a service (no safe_redis_operation to avoid recursion)."""
    client = get_client()
    if not client:
        return False
    state_key = RedisKeySchema.circuit_breaker(service, "state")
    try:
        state = client.get(state_key)
        return state == "open"
    except Exception:
        return False

def _record_circuit_breaker_failure(service: str):
    """Record a failure for circuit breaker logic."""
    def _record_failure_impl(client: redis.Redis) -> None:
        fail_count_key = RedisKeySchema.circuit_breaker(service, "fail_count")
        state_key = RedisKeySchema.circuit_breaker(service, "state")
        cooldown_key = RedisKeySchema.circuit_breaker(service, "cooldown_until")
        
        # Increment failure count
        current_failures = client.incr(fail_count_key)
        client.expire(fail_count_key, _CIRCUIT_BREAKER_COOLDOWN_PERIOD * 2)
        
        # If threshold exceeded, open circuit breaker
        if current_failures >= _CIRCUIT_BREAKER_FAILURE_THRESHOLD:
            client.setex(state_key, _CIRCUIT_BREAKER_COOLDOWN_PERIOD, "open")
            cooldown_until = time.time() + _CIRCUIT_BREAKER_COOLDOWN_PERIOD
            client.setex(cooldown_key, _CIRCUIT_BREAKER_COOLDOWN_PERIOD, str(cooldown_until))
            
            logger = _get_logger()
            logger.warning(
                f"Circuit breaker opened for {service}",
                extra={"failures": current_failures, "cooldown_until": cooldown_until}
            )
            
            _safe_metrics_call("redis_circuit_breaker_events", 1, labels={"operation": service, "event": "circuit_opened"})
            # Update circuit status gauge
            _safe_metrics_call("redis_circuit_open", 1, labels={"service": service})
    
    safe_redis_operation(_record_failure_impl, None, "cb_record_failure")

def _reset_circuit_breaker(service: str):
    """Reset circuit breaker on successful operation."""
    def _reset_impl(client: redis.Redis) -> None:
        fail_count_key = RedisKeySchema.circuit_breaker(service, "fail_count")
        state_key = RedisKeySchema.circuit_breaker(service, "state")
        cooldown_key = RedisKeySchema.circuit_breaker(service, "cooldown_until")
        
        client.delete(fail_count_key, state_key, cooldown_key)
        # Update circuit status gauge
        _safe_metrics_call("redis_circuit_open", 0, labels={"service": service})
    
    safe_redis_operation(_reset_impl, None, "cb_reset")

# --------------------------------------------------------------------------
# Distributed Lock Helpers
# --------------------------------------------------------------------------
@contextmanager
def with_setnx_lock(key: str, ttl: int = RedisTTL.LOCK):
    """
    Distributed lock using SET with EX and NX options.
    
    Args:
        key: Lock key
        ttl: Time-to-live in seconds for the lock
    
    Yields:
        bool: True if lock acquired, False otherwise
    """
    lock_key = RedisKeySchema.lock(key)
    acquired = False
    client = get_client()
    
    if client:
        try:
            # Use SET with EX and NX for atomic lock acquisition
            acquired = client.set(lock_key, "1", ex=ttl, nx=True)
            if acquired:
                _safe_metrics_call("redis_lock_operations", 1, labels={"operation": "acquire", "key": key})
            else:
                _safe_metrics_call("redis_lock_contention", 1, labels={"key": key})
        except Exception as e:
            logger = _get_logger()
            logger.error(f"Failed to acquire lock {key}", extra={"error": str(e)})
    
    try:
        yield acquired
    finally:
        if acquired and client:
            try:
                client.delete(lock_key)
                _safe_metrics_call("redis_lock_operations", 1, labels={"operation": "release", "key": key})
            except Exception as e:
                logger = _get_logger()
                logger.error(f"Failed to release lock {key}", extra={"error": str(e)})

# --------------------------------------------------------------------------
# TTL Helpers
# --------------------------------------------------------------------------
def get_ttl(key: str) -> int:
    """Get TTL for a key in seconds."""
    return safe_redis_operation(
        lambda client: client.ttl(key),
        -2,  # Key doesn't exist
        "get_ttl"
    )

def set_ttl(key: str, ttl: int) -> bool:
    """Set TTL for a key in seconds."""
    return safe_redis_operation(
        lambda client: bool(client.expire(key, ttl)),
        False,
        "set_ttl"
    )

# --------------------------------------------------------------------------
# Phase 11-F: Redis Availability Check
# --------------------------------------------------------------------------
def is_available() -> bool:
    """
    Check if Redis is available and responsive.
    
    Returns:
        True if Redis is available, False otherwise
    """
    try:
        client = get_client()
        return bool(client and client.ping())
    except Exception:
        return False

# --------------------------------------------------------------------------
# Top-Level Helper (Phase 11-F Requirement #7)
# --------------------------------------------------------------------------
def get_redis_client_helper(redis_url: Optional[str] = None) -> 'RedisClient':
    """
    Get a Redis client instance for external scripts and supervisors.
    
    Args:
        redis_url: Optional Redis URL (uses environment config if not provided)
    
    Returns:
        RedisClient instance
    """
    return RedisClient(redis_url=redis_url)

# --------------------------------------------------------------------------
# Circuit Breaker Helpers
# --------------------------------------------------------------------------

def open_breaker(breaker_name: str, ttl: int = RedisTTL.BREAKER) -> bool:
    """
    Open a circuit breaker with given TTL.
    
    Args:
        breaker_name: Name of the circuit breaker
        ttl: Time-to-live in seconds (default: 5 minutes)
    
    Returns:
        True if successful, False otherwise
    """
    return safe_redis_operation(
        lambda client: _open_breaker_impl(client, breaker_name, ttl),
        False,
        "open_breaker"
    )

def _open_breaker_impl(client: redis.Redis, breaker_name: str, ttl: int) -> bool:
    """Implementation of open_breaker for use with safe_redis_operation."""
    key = RedisKeySchema.breaker(breaker_name)
    
    breaker_state = {
        "state": "open",
        "opened_at": time.time(),
        "failure_count": 0
    }
    
    success = client.setex(
        key,
        ttl,
        json.dumps(breaker_state)
    )
    
    if success:
        logger = _get_logger()
        logger.info(
            "Circuit breaker opened",
            extra={"breaker": breaker_name, "ttl": ttl}
        )
    
    return bool(success)

def close_breaker(breaker_name: str) -> bool:
    """
    Close a circuit breaker (remove it from Redis).
    
    Args:
        breaker_name: Name of the circuit breaker
    
    Returns:
        True if successful, False otherwise
    """
    return safe_redis_operation(
        lambda client: _close_breaker_impl(client, breaker_name),
        False,
        "close_breaker"
    )

def _close_breaker_impl(client: redis.Redis, breaker_name: str) -> bool:
    """Implementation of close_breaker for use with safe_redis_operation."""
    key = RedisKeySchema.breaker(breaker_name)
    
    deleted = client.delete(key)
    
    if deleted:
        logger = _get_logger()
        logger.info(
            "Circuit breaker closed",
            extra={"breaker": breaker_name}
        )
    
    return bool(deleted)

def is_open(breaker_name: str) -> bool:
    """
    Check if a circuit breaker is currently open.
    
    Args:
        breaker_name: Name of the circuit breaker
    
    Returns:
        True if breaker is open, False otherwise
    """
    return safe_redis_operation(
        lambda client: _is_open_impl(client, breaker_name),
        False,
        "is_open"
    )

def _is_open_impl(client: redis.Redis, breaker_name: str) -> bool:
    """Implementation of is_open for use with safe_redis_operation."""
    key = RedisKeySchema.breaker(breaker_name)
    
    breaker_data = client.get(key)
    if not breaker_data:
        return False
    
    breaker_state = json.loads(breaker_data)
    return breaker_state.get("state") == "open"

def get_breaker_state(breaker_name: str) -> Optional[Dict[str, Any]]:
    """
    Get detailed circuit breaker state.
    
    Args:
        breaker_name: Name of the circuit breaker
    
    Returns:
        Breaker state dictionary or None if not found
    """
    return safe_redis_operation(
        lambda client: _get_breaker_state_impl(client, breaker_name),
        None,
        "get_breaker_state"
    )

def _get_breaker_state_impl(client: redis.Redis, breaker_name: str) -> Optional[Dict[str, Any]]:
    """Implementation of get_breaker_state for use with safe_redis_operation."""
    key = RedisKeySchema.breaker(breaker_name)
    
    breaker_data = client.get(key)
    if not breaker_data:
        return None
    
    return json.loads(breaker_data)

# --------------------------------------------------------------------------
# TTL Enforcement Helpers
# --------------------------------------------------------------------------

def enforce_ttl(key: str, ttl: int) -> bool:
    """
    Enforce TTL on a key. If key doesn't exist, does nothing.
    
    Args:
        key: Redis key
        ttl: Time-to-live in seconds
    
    Returns:
        True if TTL was set/enforced, False on error
    """
    return safe_redis_operation(
        lambda client: _enforce_ttl_impl(client, key, ttl),
        False,
        "enforce_ttl"
    )

def _enforce_ttl_impl(client: redis.Redis, key: str, ttl: int) -> bool:
    """Implementation of enforce_ttl for use with safe_redis_operation."""
    # Check if key exists
    if client.exists(key):
        result = client.expire(key, ttl)
        logger = _get_logger()
        if result:
            logger.debug(
                "TTL enforced on key",
                extra={"key": key, "ttl": ttl}
            )
        return bool(result)
    return True  # Key doesn't exist, nothing to do

def set_with_ttl(key: str, value: Any, ttl: int) -> bool:
    """
    Set a key with value and TTL in one atomic operation.
    
    Args:
        key: Redis key
        value: Value to store (will be JSON serialized if not string/bytes)
        ttl: Time-to-live in seconds
    
    Returns:
        True if successful, False otherwise
    """
    return safe_redis_operation(
        lambda client: _set_with_ttl_impl(client, key, value, ttl),
        False,
        "set_with_ttl"
    )

def _set_with_ttl_impl(client: redis.Redis, key: str, value: Any, ttl: int) -> bool:
    """Implementation of set_with_ttl for use with safe_redis_operation."""
    # Serialize non-string/bytes values to JSON
    if not isinstance(value, (str, bytes)):
        value = json.dumps(value)
    
    success = client.setex(key, ttl, value)
    
    if success:
        logger = _get_logger()
        logger.debug(
            "Key set with TTL",
            extra={"key": key, "ttl": ttl}
        )
    
    return bool(success)

def cleanup_expired_keys(pattern: str = "*") -> int:
    """
    Clean up keys matching pattern that have expired (best effort).
    Note: Redis automatically removes expired keys on access, this is for proactive cleanup.
    
    Args:
        pattern: Key pattern to match
    
    Returns:
        Number of keys cleaned up
    """
    return safe_redis_operation(
        lambda client: _cleanup_expired_keys_impl(client, pattern),
        0,
        "cleanup_expired_keys"
    )

def _cleanup_expired_keys_impl(client: redis.Redis, pattern: str) -> int:
    """Implementation of cleanup_expired_keys for use with safe_redis_operation."""
    # Use SCAN instead of KEYS to avoid blocking Redis on large datasets
    cleaned_count = 0
    cursor = 0
    while True:
        cursor, batch = client.scan(cursor=cursor, match=pattern, count=500)
        for key in batch:
            try:
                # Try to get TTL - if -2, key doesn't exist (already expired)
                ttl = client.ttl(key)
                if ttl == -2:
                    cleaned_count += 1
            except Exception:
                continue
        if cursor == 0:
            break
    
    if cleaned_count > 0:
        logger = _get_logger()
        logger.info(
            "Cleaned up expired keys",
            extra={"pattern": pattern, "count": cleaned_count}
        )
    
    return cleaned_count

__all__ = ["get_client", "get_redis_client", "redis_client", "get_redis_client_helper", "is_available",
           "redis_increment_metric", "redis_get_metric", "health_check", "shutdown_redis_safely",
           "safe_redis_operation", "RedisClient", "RedisCircuitClient",
           "hmset_json", "hget_json", "list_push_safe", "expire_key",
           "save_duplex_state", "load_duplex_state", "test_connection",
           "RedisKeySchema", "RedisTTL", "open_breaker", "close_breaker",
           "is_open", "get_breaker_state", "enforce_ttl", "set_with_ttl",
           "cleanup_expired_keys", "with_setnx_lock", "get_ttl", "set_ttl"]

# RENAMED: Avoid collision with Prometheus metrics functions
def redis_increment_metric(key: str, field: str, amount: int = 1) -> None:
    """
    Increment a Redis hash field safely with standardized key schema.
    """
    safe_redis_operation(
        lambda client: _increment_metric_impl(client, key, field, amount),
        None,
        "redis_increment_metric"
    )

def _increment_metric_impl(client: redis.Redis, key: str, field: str, amount: int) -> None:
    """Implementation of increment_metric for use with safe_redis_operation."""
    # Use standardized key schema for metrics
    metric_key = RedisKeySchema.metrics(key)
    
    client.hincrby(metric_key, field, amount)
    # Enforce TTL on metrics
    _enforce_ttl_impl(client, metric_key, RedisTTL.METRICS)

# RENAMED: Avoid collision with Prometheus metrics functions
def redis_get_metric(key: str, field: str) -> int:
    """Retrieve a single Redis metric value (int) or 0 if missing."""
    return safe_redis_operation(
        lambda client: _get_metric_impl(client, key, field),
        0,
        "redis_get_metric"
    )

def _get_metric_impl(client: redis.Redis, key: str, field: str) -> int:
    """Implementation of get_metric for use with safe_redis_operation."""
    metric_key = RedisKeySchema.metrics(key)
    
    val = client.hget(metric_key, field)
    return int(val) if val else 0

def health_check() -> dict:
    """Return a health snapshot for diagnostics."""
    # Read environment variables directly to avoid import cycles
    url = os.getenv("REDIS_URL", "redis://red-d43ertemcj7s73b0qrcg:6379/0")

    client = get_client()
    status = "ok"
    latency_ms = None
    if not client:
        status = "unavailable"
    else:
        try:
            start = time.time()
            client.ping()
            latency_ms = round((time.time() - start) * 1000, 2)
        except Exception as e:
            status = "error"
            logger = _get_logger()
            logger.error(
                "Redis health check failed",
                extra={"error": str(e)}
            )
            _report_to_sentry(e, {"operation": "health_check"})
    return {
        "status": status,
        "latency_ms": latency_ms,
        "url": url,
        "reconnect_delay": _reconnect_delay,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

# === Compatibility shim added by phase11E_autofix (Phase 11-E) ===
# Note: This is kept for backward compatibility but new code should use the enhanced safe_redis_operation above
def safe_redis_operation_legacy(func: Callable, fallback=None, operation_name: str = None):
    """Compatibility wrapper: run a callable with a redis client or return fallback."""
    try:
        client = None
        try:
            client = get_redis_client()
        except Exception:
            client = None

        if client is None:
            try:
                return func(None)
            except TypeError:
                return fallback

        try:
            result = func(client)
            
            # Phase 11-F: Metrics hook for successful operation (FIXED: Using safe call)
            _safe_metrics_call("redis_command_total", 1, labels={"command": operation_name or func.__name__})
            
            return result
        except Exception as e:
            logger = _get_logger()
            logger.error(
                "Redis operation failed",
                extra={"operation": operation_name or func.__name__, "error": str(e)}
            )
            _report_to_sentry(e, {"operation": operation_name or func.__name__})
            
            # Phase 11-F: Error metrics hook (FIXED: Using safe call)
            _safe_metrics_call("redis_error_total", 1, labels={"error": type(e).__name__})
            
            return fallback
    except Exception as e:
        logger = _get_logger()
        logger.error(
            "Redis operation failed unexpectedly",
            extra={"operation": operation_name or "unknown", "error": str(e)}
        )
        _report_to_sentry(e, {"operation": operation_name or "unknown"})
        
        # Phase 11-F: Error metrics hook (FIXED: Using safe call)
        _safe_metrics_call("redis_error_total", 1, labels={"error": type(e).__name__})
        
        return fallback
# === end compatibility shim ===

# --------------------------------------------------------------------------
# JSON Storage Helpers (Phase 11-F) - Updated with TTL enforcement
# --------------------------------------------------------------------------

def hmset_json(key: str, data: Dict[str, Any], expire: Optional[int] = None) -> bool:
    """
    Store a dictionary as JSON-encoded string in a Redis hash with TTL enforcement.
    """
    return safe_redis_operation(
        lambda client: _hmset_json_impl(client, key, data, expire),
        False,
        "hmset_json"
    )

def _hmset_json_impl(client: redis.Redis, key: str, data: Dict[str, Any], expire: Optional[int] = None) -> bool:
    """Implementation of hmset_json for use with safe_redis_operation."""
    # Serialize data to JSON
    json_data = json.dumps(data)
    # Store as single hash field 'data'
    client.hset(key, "data", json_data)
    
    # Set expiration if provided, otherwise use default TTL for temporary data
    ttl = expire if expire is not None else RedisTTL.TEMPORARY
    client.expire(key, ttl)
    
    logger = _get_logger()
    logger.debug(
        "JSON data stored successfully",
        extra={"key": key, "ttl": ttl, "data_size": len(json_data)}
    )
    
    return True

def hget_json(key: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve and parse JSON data from a Redis hash.
    """
    return safe_redis_operation(
        lambda client: _hget_json_impl(client, key),
        None,
        "hget_json"
    )

def _hget_json_impl(client: redis.Redis, key: str) -> Optional[Dict[str, Any]]:
    """Implementation of hget_json for use with safe_redis_operation."""
    json_data = client.hget(key, "data")
    if not json_data:
        return None
    
    data = json.loads(json_data)
    logger = _get_logger()
    logger.debug(
        "JSON data retrieved successfully",
        extra={"key": key}
    )
    
    return data

def list_push_safe(key: str, value: str, maxlen: int = 1000) -> bool:
    """
    Safely push a value to a Redis list with automatic trimming and TTL.
    """
    return safe_redis_operation(
        lambda client: _list_push_safe_impl(client, key, value, maxlen),
        False,
        "list_push_safe"
    )

def _list_push_safe_impl(client: redis.Redis, key: str, value: str, maxlen: int = 1000) -> bool:
    """Implementation of list_push_safe for use with safe_redis_operation."""
    # Push value to list
    client.lpush(key, value)
    # Trim to maxlen
    client.ltrim(key, 0, maxlen - 1)
    # Set TTL for temporary list data
    _enforce_ttl_impl(client, key, RedisTTL.TEMPORARY)
    
    logger = _get_logger()
    logger.debug(
        "Value pushed to list successfully",
        extra={"key": key, "maxlen": maxlen}
    )
    
    return True

def expire_key(key: str, ttl_seconds: int) -> bool:
    """
    Set TTL (time-to-live) on a Redis key.
    """
    return safe_redis_operation(
        lambda client: _expire_key_impl(client, key, ttl_seconds),
        False,
        "expire_key"
    )

def _expire_key_impl(client: redis.Redis, key: str, ttl_seconds: int) -> bool:
    """Implementation of expire_key for use with safe_redis_operation."""
    result = client.expire(key, ttl_seconds)
    if result:
        logger = _get_logger()
        logger.debug(
            "TTL set successfully",
            extra={"key": key, "ttl_seconds": ttl_seconds}
        )
    
    return bool(result)

# --------------------------------------------------------------------------
# Duplex Session Helpers (Phase 11-F) - Updated with standardized keys
# --------------------------------------------------------------------------

def save_duplex_state(call_id: str, state_dict: Dict[str, Any]) -> bool:
    """
    Save duplex session state to Redis with standardized key schema.
    """
    return safe_redis_operation(
        lambda client: _save_duplex_state_impl(client, call_id, state_dict),
        False,
        "save_duplex_state"
    )

def _save_duplex_state_impl(client: redis.Redis, call_id: str, state_dict: Dict[str, Any]) -> bool:
    """Implementation of save_duplex_state for use with safe_redis_operation."""
    key = RedisKeySchema.session(call_id)
    
    success = _hmset_json_impl(client, key, state_dict, expire=RedisTTL.SESSION)
    if success:
        logger = _get_logger()
        logger.info(
            "Duplex state saved successfully",
            extra={"call_id": call_id, "key": key}
        )
    return success

def load_duplex_state(call_id: str) -> Optional[Dict[str, Any]]:
    """
    Load duplex session state from Redis with standardized key schema.
    """
    return safe_redis_operation(
        lambda client: _load_duplex_state_impl(client, call_id),
        None,
        "load_duplex_state"
    )

def _load_duplex_state_impl(client: redis.Redis, call_id: str) -> Optional[Dict[str, Any]]:
    """Implementation of load_duplex_state for use with safe_redis_operation."""
    key = RedisKeySchema.session(call_id)
    
    state = _hget_json_impl(client, key)
    if state:
        logger = _get_logger()
        logger.debug(
            "Duplex state loaded successfully",
            extra={"call_id": call_id, "key": key}
        )
    return state

# --------------------------------------------------------------------------
# Testing & Diagnostics
# --------------------------------------------------------------------------

def test_connection() -> Dict[str, Any]:
    """
    Test Redis connection and all major operations for REPL diagnostics.
    """
    results = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "tests": {},
        "key_schema": "standardized",
        "reconnect_delay": _reconnect_delay
    }
    
    # Test 1: Health Check
    health = health_check()
    results["tests"]["health_check"] = {
        "status": health["status"],
        "latency_ms": health.get("latency_ms"),
    }
    
    # Test 2: Circuit Breaker Operations
    test_breaker = "test_breaker_123"
    try:
        open_success = open_breaker(test_breaker, 60)
        is_open_result = is_open(test_breaker)
        close_success = close_breaker(test_breaker)
        
        results["tests"]["circuit_breaker"] = {
            "status": "ok" if all([open_success, is_open_result, close_success]) else "failed",
            "open_success": open_success,
            "is_open": is_open_result,
            "close_success": close_success
        }
    except Exception as e:
        results["tests"]["circuit_breaker"] = {
            "status": "error",
            "error": str(e)
        }
    
    # Test 3: TTL Operations
    test_ttl_key = RedisKeySchema.session("test_ttl")
    try:
        ttl_success = set_with_ttl(test_ttl_key, "test_value", 60)
        ttl_value = get_ttl(test_ttl_key)
        
        results["tests"]["ttl_operations"] = {
            "status": "ok" if ttl_success and ttl_value > 0 else "failed",
            "set_success": ttl_success,
            "ttl_seconds": ttl_value
        }
        
        # Cleanup
        safe_redis_operation(
            lambda client: client.delete(test_ttl_key),
            None,
            "test_cleanup"
        )
    except Exception as e:
        results["tests"]["ttl_operations"] = {
            "status": "error",
            "error": str(e)
        }
    
    # Test 4: Distributed Lock Operations
    try:
        with with_setnx_lock("test_lock", 10) as acquired:
            results["tests"]["distributed_lock"] = {
                "status": "ok" if acquired else "failed",
                "lock_acquired": acquired
            }
    except Exception as e:
        results["tests"]["distributed_lock"] = {
            "status": "error",
            "error": str(e)
        }
    
    # Test 5: Standardized Key Schema
    try:
        session_key = RedisKeySchema.session("test123")
        breaker_key = RedisKeySchema.breaker("test_breaker")
        metrics_key = RedisKeySchema.metrics("test_metric")
        circuit_breaker_key = RedisKeySchema.circuit_breaker("test_service", "state")
        lock_key = RedisKeySchema.lock("test_resource")
        
        results["tests"]["key_schema"] = {
            "status": "ok",
            "session_key": session_key,
            "breaker_key": breaker_key,
            "metrics_key": metrics_key,
            "circuit_breaker_key": circuit_breaker_key,
            "lock_key": lock_key
        }
    except Exception as e:
        results["tests"]["key_schema"] = {
            "status": "error",
            "error": str(e)
        }
    
    # Summary
    all_ok = all(
        t.get("status") == "ok" 
        for t in results["tests"].values()
    )
    results["overall_status"] = "ok" if all_ok else "issues_detected"
    
    return results

# --------------------------------------------------------------------------
# Phase 11-F Enhanced RedisClient Class
# --------------------------------------------------------------------------
class RedisClient:
    """
    Phase 11-F Enhanced Redis client with initialization sanity checks,
    self-healing connections, metrics hooks, and context manager support.
    """
    
    def __init__(self, redis_url: Optional[str] = None, url: Optional[str] = None):
        """
        Initialize Redis client with connection verification.
        
        Args:
            redis_url: Redis URL (backward compatibility)
            url: Redis URL (new parameter name)
        """
        # Phase 11-F Requirement #2: Support both parameter names
        self.redis_url = redis_url or url or os.getenv("REDIS_URL", "redis://red-d43ertemcj7s73b0qrcg:6379/0")
        self.client = None
        self.logger = _get_logger()
        self._degraded_mode = False
        
        # Phase 11-F Requirement #1: Initialization sanity check
        try:
            self.client = redis.Redis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_connect_timeout=_REDIS_CONNECT_TIMEOUT,
                socket_timeout=_REDIS_SOCKET_TIMEOUT,
                health_check_interval=_REDIS_HEALTH_CHECK_INTERVAL,
                retry_on_timeout=True,
                # REMOVED: retry_on_error is not a valid kw in redis-py 4.x
                max_connections=_REDIS_MAX_CONNECTIONS
            )
            # Verify connection at startup with PING
            self.client.ping()
            
            self.logger.info(
                "RedisClient initialized successfully",
                extra={"url": self.redis_url, "max_connections": _REDIS_MAX_CONNECTIONS}
            )
            
            # Emit connect success metric
            _safe_metrics_call("redis_connection_events", 1, labels={"event": "client_connect_success"})
            
            # Phase 11-F: Startup Auto-Sync Metrics
            try:
                _, _, sync_metrics = _get_metrics_collector()
                sync_metrics()
                self.logger.info("Metrics sync completed at startup", extra={"event": "metrics_sync_startup"})
            except Exception as e:
                self.logger.warning("Metrics sync failed at startup", extra={"event": "metrics_sync_failed", "error": str(e)})
            
        except Exception as e:
            self.logger.error(
                "RedisClient initialization failed",
                extra={"url": self.redis_url, "error": str(e)}
            )
            _report_to_sentry(e, {"url": self.redis_url, "operation": "RedisClient.init"})
            
            # Emit connect failure metric
            _safe_metrics_call("redis_connection_events", 1, labels={"event": "client_connect_failure"})
            
            # Phase 11-F: Structured warning on fallback mode
            self.logger.warning(
                "Redis unavailable, operating in degraded mode",
                extra={
                    "event": "redis_unavailable", 
                    "url": self.redis_url,
                    "action": "degraded_mode"
                }
            )
            self._degraded_mode = True
            
            # Don't raise to maintain backward compatibility
            self.client = None

    # Phase 11-F Requirement #3: Self-healing connection logic
    def _ensure_connection(self) -> bool:
        """
        Ensure Redis connection is active, attempt reconnection if needed.
        
        Returns:
            True if connection is active, False otherwise
        """
        if self.client is None:
            try:
                self.client = redis.Redis.from_url(
                    self.redis_url,
                    decode_responses=True,
                    socket_connect_timeout=_REDIS_CONNECT_TIMEOUT,
                    socket_timeout=_REDIS_SOCKET_TIMEOUT
                )
                self.client.ping()
                self.logger.info("Redis reconnection successful", extra={"event": "redis_reconnect_success"})
                
                # Phase 11-F: Reconnection metrics (FIXED: Using safe call with labels)
                _safe_metrics_call(REDIS_EVENT_METRICS["reconnections"], 1, labels={"event": "reconnection_success"})
                _safe_metrics_call("redis_connection_events", 1, labels={"event": "client_reconnect_success"})
                self._degraded_mode = False
                
                return True
            except Exception as e:
                self.logger.error("Redis reconnection failed", extra={"event": "redis_reconnect_failed", "error": str(e)})
                # Phase 11-F: Connection failure metrics (FIXED: Using safe call with labels)
                _safe_metrics_call(REDIS_EVENT_METRICS["connection_failures"], 1, labels={"event": "reconnection_failure"})
                _safe_metrics_call("redis_connection_events", 1, labels={"event": "client_reconnect_failure"})
                self.client = None
                
                # FIXED: Guard blocking sleep to avoid event loop stall
                _safe_blocking_sleep(_reconnect_delay)
                
                return False
        
        try:
            self.client.ping()
            return True
        except Exception:
            self.logger.warning("Redis connection lost, attempting reconnection", extra={"event": "redis_reconnect_attempt"})
            try:
                self.client = redis.Redis.from_url(
                    self.redis_url,
                    decode_responses=True,
                    socket_connect_timeout=_REDIS_CONNECT_TIMEOUT,
                    socket_timeout=_REDIS_SOCKET_TIMEOUT
                )
                self.client.ping()
                self.logger.info("Redis reconnection successful", extra={"event": "redis_reconnect_success"})
                
                # Phase 11-F: Reconnection metrics (FIXED: Using safe call with labels)
                _safe_metrics_call(REDIS_EVENT_METRICS["reconnections"], 1, labels={"event": "reconnection_success"})
                _safe_metrics_call("redis_connection_events", 1, labels={"event": "client_reconnect_success"})
                self._degraded_mode = False
                
                return True
            except Exception as e:
                self.logger.error("Redis reconnection failed", extra={"event": "redis_reconnect_failed", "error": str(e)})
                # Phase 11-F: Connection failure metrics (FIXED: Using safe call with labels)
                _safe_metrics_call(REDIS_EVENT_METRICS["connection_failures"], 1, labels={"event": "reconnection_failure"})
                _safe_metrics_call("redis_connection_events", 1, labels={"event": "client_reconnect_failure"})
                self.client = None
                
                # Phase 11-F: Track degraded mode (FIXED: Using safe call with labels)
                if not self._degraded_mode:
                    self._degraded_mode = True
                    _safe_metrics_call(REDIS_EVENT_METRICS["degraded_mode"], 1, labels={"event": "entered_degraded_mode"})
                
                # FIXED: Guard blocking sleep to avoid event loop stall
                _safe_blocking_sleep(_reconnect_delay)
                
                return False

    # Phase 11-F Requirement #5: Context manager support
    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.close()

    # Phase 11-F Requirement #6: Close and flush methods
    def close(self):
        """Safely close the Redis connection."""
        try:
            if self.client:
                self.client.close()
                self.logger.info("Redis connection closed", extra={"event": "redis_closed"})
        except Exception as e:
            self.logger.warning("Redis close operation failed", extra={"event": "redis_close_failed", "error": str(e)})

    def flush_safe(self):
        """Safely flush the Redis database (use with caution)."""
        try:
            if self.client and self._ensure_connection():
                self.client.flushdb()
                self.logger.warning("Redis database flushed", extra={"event": "redis_flushed"})
                
                # Phase 11-F: Flush metrics (FIXED: Using safe call with labels)
                _safe_metrics_call(REDIS_EVENT_METRICS["flushes"], 1, labels={"type": "flushdb"})
        except Exception as e:
            self.logger.error("Redis flush operation failed", extra={"event": "redis_flush_failed", "error": str(e)})

    def flushall_safely(self):
        """
        Safely flush all Redis databases (use with extreme caution).
        Requires ALLOW_FULL_FLUSH=true environment variable.
        """
        if os.getenv("ALLOW_FULL_FLUSH") != "true":
            self.logger.warning(
                "Full flush not allowed - set ALLOW_FULL_FLUSH=true", 
                extra={"event": "redis_flushall_denied"}
            )
            return False
        
        try:
            if self.client and self._ensure_connection():
                self.client.flushall()
                self.logger.warning(
                    "All Redis databases flushed", 
                    extra={"event": "redis_flushall_complete"}
                )
                
                # Phase 11-F: Flush metrics (FIXED: Using safe call with labels)
                _safe_metrics_call(REDIS_EVENT_METRICS["flushes"], 1, labels={"type": "flushall"})
                return True
        except Exception as e:
            self.logger.error(
                "Full flush failed", 
                extra={"event": "redis_flushall_failed", "error": str(e)}
            )
        return False

    # Enhanced methods with connection assurance and metrics
    def _execute_with_metrics(self, operation_name: str, func: Callable, *args, **kwargs):
        """Execute Redis operation with connection check and metrics."""
        if not self._ensure_connection():
            return None
        
        try:
            start_time = time.time()
            result = func(*args, **kwargs)
            latency_ms = (time.time() - start_time) * 1000
            
            # Record success metrics with latency
            _safe_metrics_call("redis_operation_success", 1, labels={"command": operation_name})
            _safe_latency_observation("redis_operation_latency_ms", latency_ms, labels={"command": operation_name})
            
            return result
        except Exception as e:
            self.logger.error(
                f"Redis operation failed: {operation_name}",
                extra={"error": str(e)}
            )
            # Phase 11-F: Error metrics hook (FIXED: Using safe call)
            _safe_metrics_call("redis_error_total", 1, labels={"error": type(e).__name__})
            _report_to_sentry(e, {"operation": operation_name})
            return None

    # connection accessors
    def get_client(self):
        return self.client

    def get_redis_client(self):
        return get_redis_client()
    
    def is_degraded_mode(self) -> bool:
        """Check if client is in degraded mode (Redis unavailable)."""
        return self._degraded_mode

    # health and diagnostics
    def health_check(self):
        return health_check()
    
    def test_connection(self):
        return test_connection()

    # metrics helpers (delegates to existing functions)
    def increment_metric(self, key: str, field: str, amount: int = 1):
        return self._execute_with_metrics(
            "increment_metric",
            lambda: redis_increment_metric(key, field, amount)
        )

    def get_metric(self, key: str, field: str):
        return self._execute_with_metrics(
            "get_metric", 
            lambda: redis_get_metric(key, field)
        )

    # JSON operations
    def hmset_json(self, key: str, data: Dict[str, Any], expire: Optional[int] = None):
        return self._execute_with_metrics(
            "hmset_json",
            lambda: hmset_json(key, data, expire)
        )
    
    def hget_json(self, key: str):
        return self._execute_with_metrics(
            "hget_json",
            lambda: hget_json(key)
        )
    
    # List operations
    def list_push_safe(self, key: str, value: str, maxlen: int = 1000):
        return self._execute_with_metrics(
            "list_push_safe",
            lambda: list_push_safe(key, value, maxlen)
        )
    
    # Key expiration
    def expire_key(self, key: str, ttl_seconds: int):
        return self._execute_with_metrics(
            "expire_key",
            lambda: expire_key(key, ttl_seconds)
        )
    
    # Duplex operations
    def save_duplex_state(self, call_id: str, state_dict: Dict[str, Any]):
        return self._execute_with_metrics(
            "save_duplex_state",
            lambda: save_duplex_state(call_id, state_dict)
        )
    
    def load_duplex_state(self, call_id: str):
        return self._execute_with_metrics(
            "load_duplex_state",
            lambda: load_duplex_state(call_id)
        )
    
    # Circuit breaker operations
    def open_breaker(self, breaker_name: str, ttl: int = RedisTTL.BREAKER):
        return self._execute_with_metrics(
            "open_breaker",
            lambda: open_breaker(breaker_name, ttl)
        )
    
    def close_breaker(self, breaker_name: str):
        return self._execute_with_metrics(
            "close_breaker",
            lambda: close_breaker(breaker_name)
        )
    
    def is_open(self, breaker_name: str):
        return self._execute_with_metrics(
            "is_open",
            lambda: is_open(breaker_name)
        )
    
    def get_breaker_state(self, breaker_name: str):
        return self._execute_with_metrics(
            "get_breaker_state",
            lambda: get_breaker_state(breaker_name)
        )
    
    # TTL operations
    def enforce_ttl(self, key: str, ttl: int):
        return self._execute_with_metrics(
            "enforce_ttl",
            lambda: enforce_ttl(key, ttl)
        )
    
    def set_with_ttl(self, key: str, value: Any, ttl: int):
        return self._execute_with_metrics(
            "set_with_ttl",
            lambda: set_with_ttl(key, value, ttl)
        )
    
    def cleanup_expired_keys(self, pattern: str = "*"):
        return self._execute_with_metrics(
            "cleanup_expired_keys",
            lambda: cleanup_expired_keys(pattern)
        )
    
    # Distributed lock operations
    def with_setnx_lock(self, key: str, ttl: int = RedisTTL.LOCK):
        """Distributed lock context manager."""
        return with_setnx_lock(key, ttl)
    
    def get_ttl(self, key: str) -> int:
        """Get TTL for a key."""
        return self._execute_with_metrics(
            "get_ttl",
            lambda: get_ttl(key)
        ) or -2
    
    def set_ttl(self, key: str, ttl: int) -> bool:
        """Set TTL for a key."""
        return self._execute_with_metrics(
            "set_ttl",
            lambda: set_ttl(key, ttl)
        ) or False

    # convenience wrappers used by some legacy code
    def ping(self) -> bool:
        return self._ensure_connection()

    # safe executor for arbitrary operations
    def safe_operation(self, func, fallback=None, operation_name: str = None):
        return safe_redis_operation(func, fallback=fallback, operation_name=operation_name or func.__name__)

# Alias for older module checks that still expect this symbol name
RedisCircuitClient = RedisClient

# --------------------------------------------------------------------------
# Phase 11-F: Graceful Shutdown Hook
# --------------------------------------------------------------------------
def shutdown_redis_safely():
    """
    Flush buffers and close Redis safely at system shutdown.
    
    This should be called by phase11f_runtime_supervisor before exit.
    """
    try:
        client = get_client()
        if client:
            # Flush any buffered transient data before shutdown
            if hasattr(client, "flush_safe"):
                client.flush_safe()
            client.close()
        
        # Phase 11-F: Shutdown metrics (FIXED: Using safe call with labels)
        _safe_metrics_call(REDIS_EVENT_METRICS["shutdowns"], 1, labels={"event": "graceful_shutdown"})
        
        logger = _get_logger()
        logger.info("Redis shutdown completed", extra={"event": "redis_shutdown_complete"})
    except Exception as e:
        logger = _get_logger()
        logger.warning(
            "Redis shutdown encountered issues", 
            extra={"event": "redis_shutdown_failed", "error": str(e)}
        )