"""
redis_client.py — Phase 11-C (Telemetry + Auto-Recoverable Redis Singleton)
---------------------------------------------------------------------------
Centralized, telemetry-aware Redis client for Sara AI Core.
Integrates with structured logging (logging_utils.log_event)
and provides self-healing connection management for all services.
"""

import os
import redis
import time
from typing import Optional
from logging_utils import log_event

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_REDIS_CONNECT_TIMEOUT = 5
_REDIS_SOCKET_TIMEOUT = 5
_REDIS_HEALTH_CHECK_INTERVAL = 30

# ---------------------------------------------------------------------------
# Connection Management
# ---------------------------------------------------------------------------

def get_redis_url() -> str:
    """Resolve Redis URL from environment or fallback."""
    return (
        os.getenv("REDIS_URL")
        or os.getenv("REDIS_HOST")
        or DEFAULT_REDIS_URL
    )

_redis_client: Optional[redis.Redis] = None


def _connect() -> Optional[redis.Redis]:
    """Attempt to establish a Redis connection and perform a health check."""
    url = get_redis_url()
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
        log_event(
            service="redis_client",
            event="redis_connected",
            status="ok",
            message=f"Connected to Redis at {url}",
        )
        return client
    except redis.exceptions.ConnectionError as e:
        log_event(
            service="redis_client",
            event="redis_connection_failed",
            status="warn",
            message=f"Cannot reach Redis at {url}",
            extra={"error": str(e)},
        )
        return None
    except Exception as e:
        log_event(
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
            log_event(
                service="redis_client",
                event="redis_reconnect_attempt",
                status="warn",
                message="Redis connection lost; attempting reconnect...",
            )
            _redis_client = _connect()
    return _redis_client


# ---------------------------------------------------------------------------
# Public Singleton
# ---------------------------------------------------------------------------

redis_client: Optional[redis.Redis] = get_client()


# ---------------------------------------------------------------------------
# Utility Helpers (Telemetry-Aware)
# ---------------------------------------------------------------------------

def increment_metric(key: str, field: str, amount: int = 1) -> None:
    """
    Increment a Redis hash field safely.
    Example:
        increment_metric("metrics:tts", "uploads")
    """
    client = get_client()
    if not client:
        return
    try:
        client.hincrby(key, field, amount)
    except Exception as e:
        log_event(
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
        log_event(
            service="redis_client",
            event="get_metric_failed",
            status="warn",
            message=f"Failed to fetch metric {key}:{field}",
            extra={"error": str(e)},
        )
        return 0


def health_check() -> dict:
    """Return a health snapshot for diagnostics."""
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
            log_event(
                service="redis_client",
                event="health_check_failed",
                status="error",
                message=str(e),
            )
    return {
        "status": status,
        "latency_ms": latency_ms,
        "url": get_redis_url(),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


# ---------------------------------------------------------------------------
# Smoke Test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    hc = health_check()
    print(f"[redis_client] Health: {hc}")
