"""
metrics_collector.py — Phase 10M-D (Redis-backed persistence shim)

- Adds optional Redis-backed persistence for global counters (prometheus totals).
- Backwards-compatible: keeps in-memory counters and latency buckets as primary
  data sources when Redis is unavailable.
- New helpers:
  - increment_metric(metric_name, value=1)
  - get_metric_total(metric_name) -> int
  - export_prometheus() now reports redis-merged totals for counters
- Latency aggregation and snapshot behavior unchanged.
"""

from __future__ import annotations

import threading
import math
from collections import defaultdict, deque
from typing import Dict, Deque, List
import statistics
import time

from logging_utils import log_event

# Try to import the application redis client (optional)
try:
    # expected to expose `redis_client`
    from redis_client import redis_client as _redis_client  # type: ignore
except Exception:
    _redis_client = None

# Configuration
_DEFAULT_ROLLING_WINDOW = 1000  # keep up to 1000 samples per latency metric

# Internal storage (protected by _lock)
_lock = threading.Lock()
_counters: Dict[str, int] = defaultdict(int)
_latency_buckets: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=_DEFAULT_ROLLING_WINDOW))


# -------------------------
# Backward-compatible shim
# -------------------------
def init_redis_client(*args, **kwargs) -> None:
    """
    DEPRECATED no-op for Phase 10L compatibility.

    Previously this function attached a Redis client and started a background
    sync thread. In Phase 10M-D we support explicit Redis-backed helpers but
    this shim remains to avoid breaking callers.
    """
    try:
        log_event(
            service="metrics",
            event="init_redis_client_deprecated",
            status="info" if _redis_client else "warn",
            message=(
                "init_redis_client shim called. In Phase 10M-D metrics support optional "
                "Redis-backed totals via increment_metric/get_metric_total. "
                f"redis_client available={bool(_redis_client)}"
            ),
            extra={},
        )
    except Exception:
        pass


# -------------------------
# Redis-backed helpers
# -------------------------
REDIS_METRIC_PREFIX = "prometheus:metrics:"


def _redis_key(metric_name: str) -> str:
    return f"{REDIS_METRIC_PREFIX}{metric_name}"


def increment_metric(metric_name: str, value: int = 1) -> bool:
    """
    Increment a named counter both in-memory and (if available) in Redis.

    - Updates the in-memory `_counters` for fast local observation.
    - Attempts to update redis key `_redis_key(metric_name)` with INCRBY.
    - Returns True on success (in-memory always updated), False only if in-memory fails.
    """
    try:
        # update local counters under lock
        with _lock:
            _counters[metric_name] = _counters.get(metric_name, 0) + int(value)
        # best-effort Redis update
        if _redis_client:
            try:
                # use integer incrby if available
                _redis_client.incrby(_redis_key(metric_name), int(value))
            except Exception:
                # do not fail the overall operation on redis errors
                try:
                    log_event(
                        service="metrics",
                        event="redis_incr_failed",
                        status="warn",
                        message="Failed to incrby redis metric (best-effort).",
                        extra={"metric": metric_name, "value": value},
                    )
                except Exception:
                    pass
        return True
    except Exception as e:
        try:
            log_event(
                service="metrics",
                event="increment_metric_failed",
                status="error",
                message="Failed to increment metric.",
                extra={"metric": metric_name, "value": value, "error": str(e)},
            )
        except Exception:
            pass
        return False


def get_metric_total(metric_name: str) -> int:
    """
    Return the global total for a metric.

    - If Redis is available, prefer the Redis-stored total (safe-cast to int).
    - Otherwise, fall back to local in-memory counter.
    - Returns 0 on error.
    """
    try:
        redis_val = 0
        if _redis_client:
            try:
                raw = _redis_client.get(_redis_key(metric_name))
                redis_val = int(raw) if raw is not None else 0
            except Exception:
                # log and continue with local fallback
                try:
                    log_event(
                        service="metrics",
                        event="redis_get_failed",
                        status="warn",
                        message="Failed to read metric total from Redis (fallback to local).",
                        extra={"metric": metric_name},
                    )
                except Exception:
                    pass
                redis_val = 0
        with _lock:
            local_val = int(_counters.get(metric_name, 0))
        # If Redis has larger number, prefer it (aggregate/global); otherwise local
        return max(local_val, redis_val)
    except Exception:
        try:
            log_event(
                service="metrics",
                event="get_metric_total_failed",
                status="error",
                message="Failed to compute metric total",
                extra={"metric": metric_name},
            )
        except Exception:
            pass
        return 0


# -------------------------
# Counter API (kept for compatibility)
# -------------------------
def inc_metric(name: str, amount: int = 1) -> bool:
    """
    Backwards-compatible increment. Internally calls increment_metric to ensure
    Redis-backed persistence when available.
    """
    return increment_metric(name, amount)


def set_metric(name: str, value: int) -> bool:
    """Set a counter to a specific integer value (in-memory only)."""
    try:
        with _lock:
            _counters[name] = int(value)
        # best-effort: also set in Redis if available
        if _redis_client:
            try:
                _redis_client.set(_redis_key(name), int(value))
            except Exception:
                try:
                    log_event(
                        service="metrics",
                        event="redis_set_failed",
                        status="warn",
                        message="Failed to set metric in Redis (best-effort).",
                        extra={"metric": name, "value": value},
                    )
                except Exception:
                    pass
        return True
    except Exception as e:
        try:
            log_event(
                service="metrics",
                event="set_metric_failed",
                status="error",
                message="Failed to set metric",
                extra={"name": name, "value": value, "error": str(e)},
            )
        except Exception:
            pass
        return False


# -------------------------
# Latency API
# -------------------------
def observe_latency(name: str, value_ms: float) -> bool:
    """
    Record a latency observation (milliseconds) under metric `name`.
    Keeps a rolling window of recent samples to bound memory usage.
    """
    try:
        val = float(value_ms)
        with _lock:
            if name not in _latency_buckets:
                _latency_buckets[name] = deque(maxlen=_DEFAULT_ROLLING_WINDOW)
            _latency_buckets[name].append(val)
        return True
    except Exception as e:
        try:
            log_event(
                service="metrics",
                event="observe_latency_failed",
                status="error",
                message="Failed to observe latency",
                extra={"name": name, "value_ms": value_ms, "error": str(e)},
            )
        except Exception:
            pass
        return False


# -------------------------
# Snapshot helpers
# -------------------------
def _compute_latency_stats(samples: List[float]) -> Dict[str, float]:
    if not samples:
        return {"count": 0, "avg": 0.0, "min": 0.0, "max": 0.0, "p50": 0.0, "p95": 0.0, "sum": 0.0}

    count = len(samples)
    avg = statistics.mean(samples)
    mn = min(samples)
    mx = max(samples)

    def percentile(sorted_samples: List[float], p: float) -> float:
        if not sorted_samples:
            return 0.0
        k = (len(sorted_samples) - 1) * (p / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_samples[int(k)]
        d0 = sorted_samples[int(f)] * (c - k)
        d1 = sorted_samples[int(c)] * (k - f)
        return d0 + d1

    s_sorted = sorted(samples)
    p50 = percentile(s_sorted, 50.0)
    p95 = percentile(s_sorted, 95.0)
    total = float(sum(samples))

    return {
        "count": count,
        "avg": round(avg, 2),
        "min": round(mn, 2),
        "max": round(mx, 2),
        "p50": round(p50, 2),
        "p95": round(p95, 2),
        "sum": round(total, 2),
    }


def get_snapshot() -> Dict[str, object]:
    """
    Return a snapshot of counters and latency metrics (local in-memory view).
    Note: counters reflect local counters only; use export_prometheus() for
    Redis-merged global totals.
    """
    try:
        with _lock:
            counters_copy = dict(_counters)
            latencies_copy = {k: list(v) for k, v in _latency_buckets.items()}

        latencies_stats = {name: _compute_latency_stats(samples) for name, samples in latencies_copy.items()}

        snapshot = {
            "counters": counters_copy,
            "latencies": latencies_stats,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        return snapshot
    except Exception as e:
        try:
            log_event(
                service="metrics",
                event="get_snapshot_failed",
                status="error",
                message="Failed to get metrics snapshot",
                extra={"error": str(e)},
            )
        except Exception:
            pass
        return {"counters": {}, "latencies": {}, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


# -------------------------
# Prometheus export
# -------------------------
def export_prometheus() -> str:
    """
    Render metrics in Prometheus exposition format.
    For counters we prefer Redis-backed totals when available via get_metric_total.
    For latencies we export the current aggregated stats computed from in-memory samples.
    """
    try:
        snap = get_snapshot()  # local snapshot (counters are local)
        lines: List[str] = []

        # Build the set of metric names to export:
        # include local counters keys plus any keys that redis reports (best-effort)
        metric_names = set(snap.get("counters", {}).keys())

        # If Redis is available, attempt to list keys with the prefix to include them too.
        if _redis_client:
            try:
                # Note: using KEYS here is acceptable for small sets; if scale grows, replace with SCAN.
                for k in _redis_client.keys(f"{REDIS_METRIC_PREFIX}*"):
                    try:
                        # strip prefix
                        if isinstance(k, bytes):
                            k = k.decode("utf-8")
                        if k.startswith(REDIS_METRIC_PREFIX):
                            metric_names.add(k[len(REDIS_METRIC_PREFIX):])
                    except Exception:
                        continue
            except Exception:
                # If Redis key list fails, continue with local metrics only
                try:
                    log_event(
                        service="metrics",
                        event="redis_keys_failed",
                        status="warn",
                        message="Failed to list redis metric keys during export (best-effort).",
                        extra={},
                    )
                except Exception:
                    pass

        # Counters (use get_metric_total to return merged totals)
        for name in sorted(metric_names):
            total = get_metric_total(name)
            lines.append(f"# HELP {name} Total count for {name}")
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {int(total)}")

        # Latency aggregates
        latencies = snap.get("latencies", {})
        for base, stats in sorted(latencies.items()):
            lines.append(f"# HELP {base}_avg Average {base}")
            lines.append(f"# TYPE {base}_avg gauge")
            lines.append(f"{base}_avg {float(stats.get('avg', 0.0))}")

            lines.append(f"# HELP {base}_count Count of {base} observations")
            lines.append(f"# TYPE {base}_count gauge")
            lines.append(f"{base}_count {int(stats.get('count', 0))}")

            lines.append(f"# HELP {base}_p50 50th percentile of {base}")
            lines.append(f"# TYPE {base}_p50 gauge")
            lines.append(f"{base}_p50 {float(stats.get('p50', 0.0))}")

            lines.append(f"# HELP {base}_p95 95th percentile of {base}")
            lines.append(f"# TYPE {base}_p95 gauge")
            lines.append(f"{base}_p95 {float(stats.get('p95', 0.0))}")

            lines.append(f"# HELP {base}_min Minimum {base}")
            lines.append(f"# TYPE {base}_min gauge")
            lines.append(f"{base}_min {float(stats.get('min', 0.0))}")

            lines.append(f"# HELP {base}_max Maximum {base}")
            lines.append(f"# TYPE {base}_max gauge")
            lines.append(f"{base}_max {float(stats.get('max', 0.0))}")

        lines.append(f"# timestamp {snap.get('timestamp')}")
        return "\n".join(lines) + "\n"
    except Exception:
        try:
            log_event(
                service="metrics",
                event="export_prometheus_error",
                status="error",
                message="Failed to export metrics prometheus text",
            )
        except Exception:
            pass
        return "# metrics_export_error 1\n"


# -------------------------
# Utilities (for tests / dev)
# -------------------------
def reset_collector() -> None:
    """Reset all in-memory metrics (useful for tests)."""
    with _lock:
        _counters.clear()
        _latency_buckets.clear()


# -------------------------
# Convenience metric names (recommended)
# -------------------------
# - tts_requests_total
# - tts_cache_hits_total
# - tts_failures_total
# - tts_latency_ms
# - inference_latency_ms
# - r2_upload_latency_ms

# End of file
