"""
metrics_collector.py — Phase 10L (Prometheus-only)

Lightweight in-memory metrics collector with Prometheus text export.

Phase 10L changes:
- Redis integration removed (no background sync, no HSET/HINCR to Redis)
- init_redis_client remains as a backwards-compatible no-op shim that logs a warning.
- All metrics are kept in-memory and exported via export_prometheus().
- Thread-safe and fail-safe; uses logging_utils.log_event for errors.
"""

from __future__ import annotations

import threading
import math
from collections import defaultdict, deque
from typing import Dict, Deque, List
import statistics
import time

from logging_utils import log_event

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
    DEPRECATED no-op.

    Previously this function attached a Redis client and started a background
    sync thread. In Phase 10L we removed Redis synchronization entirely;
    this shim exists to avoid breaking callers that still call init_redis_client().
    It logs a warning that Redis-backed metrics are removed.
    """
    try:
        log_event(
            service="metrics",
            event="init_redis_client_deprecated",
            status="warn",
            message="Redis integration removed from metrics_collector (Phase 10L). "
                    "Metrics are now in-memory Prometheus-only.",
            extra={},
        )
    except Exception:
        # Best-effort only; do not raise
        pass


# -------------------------
# Counter API
# -------------------------
def inc_metric(name: str, amount: int = 1) -> bool:
    """
    Increment a named counter by `amount`. Returns True on success.
    In-memory only (Prometheus primary).
    """
    try:
        with _lock:
            _counters[name] += int(amount)
        return True
    except Exception as e:
        try:
            log_event(
                service="metrics",
                event="inc_metric_failed",
                status="error",
                message="Failed to increment metric",
                extra={"name": name, "amount": amount, "error": str(e)},
            )
        except Exception:
            pass
        return False


def set_metric(name: str, value: int) -> bool:
    """Set a counter to a specific integer value (in-memory only)."""
    try:
        with _lock:
            _counters[name] = int(value)
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
    Return a snapshot of counters and latency metrics.
    Example:
    {
      "counters": {"tts_requests_total": 10, ...},
      "latencies": {"tts_latency_ms": {...stats...}, ...},
      "timestamp": "..."
    }
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
    Produces HELP/TYPE comments for counters and latency aggregates.
    """
    try:
        snap = get_snapshot()
        lines: List[str] = []

        counters = snap.get("counters", {})
        latencies = snap.get("latencies", {})

        # Counters
        for k, v in sorted(counters.items()):
            name = k
            lines.append(f"# HELP {name} Total count for {name}")
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {int(v)}")

        # Latency aggregates
        for name, stats in sorted(latencies.items()):
            base = name
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
