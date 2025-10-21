"""
redis_client.py — Phase 11-D Compliant
Centralized, telemetry-aware Redis client with self-healing connections.
"""

import os
import redis
import time
from typing import Optional


# Connection Management
_redis_client: Optional[redis.Redis] = None

_REDIS_CONNECT_TIMEOUT = 5
_REDIS_SOCKET_TIMEOUT = 5
_REDIS_HEALTH_CHECK_INTERVAL = 30


def _get_logger():
    """Lazy import logging utilities to break circular imports."""
    try:
        from logging_utils import log_event
        return log_event
    except Exception:
        def _noop_log(*a, **k): pass
        return _noop_log


def _connect() -> Optional[redis.Redis]:
    """Attempt to establish a Redis connection and perform a health check."""
    from config import Config
    url = Config.REDIS_URL
    try:
        client = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=_REDIS_CONNECT_TIMEOUT,
            socket_timeout=_REDIS_SOCKET_TIMEOUT,
            health_check_interval=_REDIS_HEALTH_CHECK_INTERVAL,
        )
        # Connectivity check
        client.ping()
        log_func = _get_logger()
        log_func(
            service="redis_client",
            event="redis_connected",
            status="ok",
            message=f"Connected to Redis at {url}",
        )
        return client
    except redis.exceptions.ConnectionError as e:
        log_func = _get_logger()
        log_func(
            service="redis_client",
            event="redis_connection_failed",
            status="warn",
            message=f"Cannot reach Redis at {url}",
            extra={"error": str(e)},
        )
        return None
    except Exception as e:
        log_func = _get_logger()
        log_func(
            service="redis_client",
            event="redis_init_error",
            status="error",
            message=f"Redis init failed for {url}",
            extra={"error": str(e)},
        )
        return None


def get_client() -> Optional[redis.Redis]:
    """
    Return the global Redis client instance.
    If disconnected, attempt a lazy reconnection.
    """
    global _redis_client
    if _redis_client is None:
        _redis_client = _connect()
    else:
        try:
            _redis_client.ping()
        except redis.exceptions.ConnectionError:
            log_func = _get_logger()
            log_func(
                service="redis_client",
                event="redis_reconnect_attempt",
                status="warn",
                message="Redis connection lost; attempting reconnect...",
            )
            _redis_client = _connect()
    return _redis_client


# Public Singleton
redis_client: Optional[redis.Redis] = get_client()

# Phase 11-D Compatibility Alias
# Ensures backward compatibility for modules importing get_redis_client
def get_redis_client() -> Optional[redis.Redis]:
    """Alias for get_client() to maintain import compatibility."""
    return get_client()

__all__ = ["get_client", "get_redis_client", "redis_client",
           "increment_metric", "get_metric", "health_check"]


def increment_metric(key: str, field: str, amount: int = 1) -> None:
    """
    Increment a Redis hash field safely.
    """
    client = get_client()
    if not client:
        return
    try:
        client.hincrby(key, field, amount)
    except Exception as e:
        log_func = _get_logger()
        log_func(
            service="redis_client",
            event="increment_metric_failed",
            status="warn",
            message=f"Failed to increment metric {key}:{field}",
            extra={"error": str(e)},
        )


def get_metric(key: str, field: str) -> int:
    """Retrieve a single Redis metric value (int) or 0 if missing."""
    client = get_client()
    if not client:
        return 0
    try:
        val = client.hget(key, field)
        return int(val) if val else 0
    except Exception as e:
        log_func = _get_logger()
        log_func(
            service="redis_client",
            event="get_metric_failed",
            status="warn",
            message=f"Failed to fetch metric {key}:{field}",
            extra={"error": str(e)},
        )
        return 0


def health_check() -> dict:
    """Return a health snapshot for diagnostics."""
    from config import Config
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
            log_func = _get_logger()
            log_func(
                service="redis_client",
                event="health_check_failed",
                status="error",
                message=str(e),
            )
    return {
        "status": status,
        "latency_ms": latency_ms,
        "url": Config.REDIS_URL,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }