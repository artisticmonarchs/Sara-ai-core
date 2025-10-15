"""
metrics_collector.py — Phase 11-A (Streaming Metrics Extension)

Includes:
- Phase 10M-D Redis-backed persistence shim (global counters)
- Phase 11-A streaming-specific Prometheus metrics (gauges + summaries)
"""

from __future__ import annotations

import threading
import math
from collections import defaultdict, deque
from typing import Dict, Deque, List
import statistics
import time

from logging_utils import log_event

# ------------------------------------------------------------------
# Phase 11-A: Streaming-specific Prometheus metrics
# ------------------------------------------------------------------
from prometheus_client import Gauge, Summary

# --- Streaming Metrics ---
tts_active_streams = Gauge(
    "tts_active_streams",
    "Current number of active TTS streams",
)

stream_latency_ms = Summary(
    "stream_latency_ms",
    "TTS stream completion latency (milliseconds)",
)

stream_bytes_out_total = Gauge(
    "stream_bytes_out_total",
    "Total number of audio bytes streamed out",
)

# Try to import the application redis client (optional)
try:
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
    """DEPRECATED no-op for Phase 10L compatibility."""
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
    """Increment a named counter both in-memory and (if available) in Redis."""
    try:
        with _lock:
            _counters[metric_name] = _counters.get(metric_name, 0) + int(value)
        if _redis_client:
            try:
                _redis_client.incrby(_redis_key(metric_name), int(value))
            except Exception:
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
    """Return the global total for a metric (prefers Redis-backed totals)."""
    try:
        redis_val = 0
        if _redis_client:
            try:
                raw = _redis_client.get(_redis_key(metric_name))
                redis_val = int(raw) if raw is not None else 0
            except Exception:
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
# Counter API (compatibility)
# -------------------------
def inc_metric(name: str, amount: int = 1) -> bool:
    """Backwards-compatible increment."""
    return increment_metric(name, amount)


def set_metric(name: str, value: int) -> bool:
    """Set a counter to a specific integer value (in-memory + Redis)."""
    try:
        with _lock:
            _counters[name] = int(value)
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
    """Record a latency observation (milliseconds) under metric `name`."""
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
    """Return a snapshot of counters, latency metrics, and streaming gauges."""
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

        # --- Phase 11-A: include streaming gauges in snapshot ---
        try:
            snapshot["streaming"] = {
                "tts_active_streams": tts_active_streams._value.get(),
                "stream_bytes_out_total": stream_bytes_out_total._value.get(),
            }
        except Exception:
            snapshot["streaming"] = {}

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
    """Render metrics in Prometheus exposition format."""
    try:
        snap = get_snapshot()
        lines: List[str] = []

        metric_names = set(snap.get("counters", {}).keys())
        if _redis_client:
            try:
                for k in _redis_client.keys(f"{REDIS_METRIC_PREFIX}*"):
                    if isinstance(k, bytes):
                        k = k.decode("utf-8")
                    if k.startswith(REDIS_METRIC_PREFIX):
                        metric_names.add(k[len(REDIS_METRIC_PREFIX):])
            except Exception:
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

        for name in sorted(metric_names):
            total = get_metric_total(name)
            lines.append(f"# HELP {name} Total count for {name}")
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {int(total)}")

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
# Utilities
# -------------------------
def reset_collector() -> None:
    """Reset all in-memory metrics (useful for tests)."""
    with _lock:
        _counters.clear()
        _latency_buckets.clear()


# -------------------------
# Recommended metric names
# -------------------------
# - tts_requests_total
# - tts_cache_hits_total
# - tts_failures_total
# - tts_latency_ms
# - inference_latency_ms
# - r2_upload_latency_ms
# - tts_active_streams
# - stream_latency_ms
# - stream_bytes_out_total

# End of file
