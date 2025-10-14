"""
redis_client.py
Centralized Redis connection for Sara AI Core.
Used by app.py, tasks.py, and Celery workers for metrics and state tracking.
"""

import os
import redis

# ---------------------------------------------------------------------------
# Redis connection initialization
# ---------------------------------------------------------------------------

def get_redis_url() -> str:
    """Resolve Redis URL from environment or fallback."""
    return os.getenv("REDIS_URL") or os.getenv("REDIS_HOST") or "redis://localhost:6379/0"


try:
    redis_client = redis.Redis.from_url(
        get_redis_url(),
        decode_responses=True,
        socket_connect_timeout=5,   # seconds
        socket_timeout=5,           # seconds
        health_check_interval=30,   # improves long-lived connection stability
    )

    # Quick connectivity check (non-fatal)
    try:
        redis_client.ping()
        print("[redis_client] Connected successfully to Redis.")
    except redis.exceptions.ConnectionError:
        print("[redis_client] Warning: Unable to reach Redis — running in degraded mode.")

except Exception as e:
    print(f"[redis_client] Fatal error initializing Redis: {e}")
    redis_client = None

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def increment_metric(key: str, field: str, amount: int = 1):
    """
    Increment a Redis hash field safely.
    Example:
        increment_metric("metrics:tts", "uploads")
    """
    if not redis_client:
        return
    try:
        redis_client.hincrby(key, field, amount)
    except Exception as e:
        print(f"[redis_client] Failed to increment metric {key}:{field} — {e}")


def get_metric(key: str, field: str) -> int:
    """Retrieve a single Redis metric value (int) or 0 if missing."""
    if not redis_client:
        return 0
    try:
        value = redis_client.hget(key, field)
        return int(value) if value else 0
    except Exception as e:
        print(f"[redis_client] Failed to fetch metric {key}:{field} — {e}")
        return 0
