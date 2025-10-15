"""
metrics_collector.py — Phase 11-B (Prometheus Registry Merge)

Includes:
- Phase 10M-D Redis-backed persistence shim (global counters)
- Phase 11-A streaming-specific Prometheus metrics (gauges + summaries)
- Phase 11-B Prometheus REGISTRY merge in export_prometheus()
"""

from __future__ import annotations
import threading
import math
from collections import defaultdict, deque
from typing import Dict, Deque, List
import statistics
import time
import os
from logging_utils import log_event

# ------------------------------------------------------------------
# Phase 11-A: Streaming-specific Prometheus metrics
# ------------------------------------------------------------------
from prometheus_client import Gauge, Summary, generate_latest, REGISTRY

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

# ------------------------------------------------------------------
# Redis (optional legacy persistence shim)
# ------------------------------------------------------------------
try:
    from redis_client import redis_client as _redis_client  # type: ignore
except Exception:
    _redis_client = None

_DEFAULT_ROLLING_WINDOW = 1000  # samples per latency metric
_lock = threading.Lock()
_counters: Dict[str, int] = defaultdict(int)
_latency_buckets: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=_DEFAULT_ROLLING_WINDOW))

REDIS_METRIC_PREFIX = "prometheus:metrics:"

# Service name namespace for Redis keys. Allows per-service keys:
# prometheus:metrics:<service_name>:<metric_name>
SERVICE_NAME = os.getenv("SERVICE_NAME", "unknown_service")


def _redis_key(metric_name: str) -> str:
    """Return a Redis key namespaced with the service name."""
    return f"{REDIS_METRIC_PREFIX}{SERVICE_NAME}:{metric_name}"


# ------------------------------------------------------------------
# Backward-compatible shim
# ------------------------------------------------------------------
def init_redis_client(*args, **kwargs) -> None:
    """DEPRECATED no-op for Phase 10L compatibility."""
    try:
        log_event(
            service="metrics",
            event="init_redis_client_deprecated",
            status="info" if _redis_client else "warn",
            message=(
                "init_redis_client shim called. In Phase 10M-D metrics support optional "
                f"Redis-backed totals via increment_metric/get_metric_total. redis_client={bool(_redis_client)}"
            ),
        )
    except Exception:
        pass


# ------------------------------------------------------------------
# Redis-backed helpers
# ------------------------------------------------------------------
def increment_metric(metric_name: str, value: int = 1) -> bool:
    """Increment counter in memory + Redis (if available)."""
    try:
        with _lock:
            _counters[metric_name] += int(value)
        if _redis_client:
            try:
                _redis_client.incrby(_redis_key(metric_name), int(value))
            except Exception:
                log_event(
                    service="metrics",
                    event="redis_incr_failed",
                    status="warn",
                    message="Failed to incrby Redis metric (best-effort).",
                    extra={"metric": metric_name, "value": value},
                )
        return True
    except Exception as e:
        log_event(
            service="metrics",
            event="increment_metric_failed",
            status="error",
            message=str(e),
            extra={"metric": metric_name, "value": value},
        )
        return False


def get_metric_total(metric_name: str) -> int:
    """Return total (prefers Redis for this service's metric key)."""
    try:
        redis_val = 0
        if _redis_client:
            try:
                raw = _redis_client.get(_redis_key(metric_name))
                redis_val = int(raw) if raw else 0
            except Exception:
                log_event(
                    service="metrics",
                    event="redis_get_failed",
                    status="warn",
                    message="Failed to get Redis metric total.",
                    extra={"metric": metric_name},
                )
        with _lock:
            local_val = int(_counters.get(metric_name, 0))
        # prefer Redis value for this service if it exists, otherwise local
        return max(local_val, redis_val)
    except Exception:
        return 0


def inc_metric(name: str, amount: int = 1) -> bool:
    return increment_metric(name, amount)


def set_metric(name: str, value: int) -> bool:
    try:
        with _lock:
            _counters[name] = int(value)
        if _redis_client:
            try:
                _redis_client.set(_redis_key(name), int(value))
            except Exception:
                log_event(
                    service="metrics",
                    event="redis_set_failed",
                    status="warn",
                    message="Failed to set metric in Redis.",
                    extra={"metric": name, "value": value},
                )
        return True
    except Exception as e:
        log_event(
            service="metrics",
            event="set_metric_failed",
            status="error",
            message=str(e),
            extra={"name": name, "value": value},
        )
        return False


# ------------------------------------------------------------------
# Latency handling
# ------------------------------------------------------------------
def observe_latency(name: str, value_ms: float) -> bool:
    try:
        val = float(value_ms)
        with _lock:
            _latency_buckets[name].append(val)
        return True
    except Exception as e:
        log_event(
            service="metrics",
            event="observe_latency_failed",
            status="error",
            message=str(e),
            extra={"name": name, "value_ms": value_ms},
        )
        return False


# ------------------------------------------------------------------
# Snapshot
# ------------------------------------------------------------------
def _compute_latency_stats(samples: List[float]) -> Dict[str, float]:
    if not samples:
        return {"count": 0, "avg": 0.0, "min": 0.0, "max": 0.0, "p50": 0.0, "p95": 0.0, "sum": 0.0}
    count = len(samples)
    avg = statistics.mean(samples)
    s_sorted = sorted(samples)

    def pct(p: float) -> float:
        if not s_sorted:
            return 0.0
        k = (len(s_sorted) - 1) * (p / 100)
        f, c = math.floor(k), math.ceil(k)
        if f == c:
            return s_sorted[int(k)]
        return s_sorted[f] * (c - k) + s_sorted[c] * (k - f)

    return {
        "count": count,
        "avg": round(avg, 2),
        "min": round(min(s_sorted), 2),
        "max": round(max(s_sorted), 2),
        "p50": round(pct(50), 2),
        "p95": round(pct(95), 2),
        "sum": round(sum(samples), 2),
    }


def get_snapshot() -> Dict[str, object]:
    try:
        with _lock:
            counters_copy = dict(_counters)
            latencies_copy = {k: list(v) for k, v in _latency_buckets.items()}

        latencies_stats = {n: _compute_latency_stats(v) for n, v in latencies_copy.items()}

        snapshot = {
            "counters": counters_copy,
            "latencies": latencies_stats,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        # Phase 11-A streaming metrics snapshot (per-process registry values)
        snapshot["streaming"] = {
            "tts_active_streams": tts_active_streams._value.get(),
            "stream_bytes_out_total": stream_bytes_out_total._value.get(),
        }

        return snapshot
    except Exception as e:
        try:
            log_event(
                service="metrics",
                event="get_snapshot_failed",
                status="error",
                message=str(e),
            )
        except Exception:
            pass
        return {"counters": {}, "latencies": {}, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


# ------------------------------------------------------------------
# Prometheus export (Phase 11-B merged)
# ------------------------------------------------------------------
def export_prometheus() -> str:
    """Render metrics in Prometheus exposition format, merged with client registry.
    Also compute Redis global aggregates across services and append *_global metrics.
    """
    try:
        snap = get_snapshot()
        lines: List[str] = []

        metric_names = set(snap.get("counters", {}).keys())
        # Discover metric names from Redis keys (namespaced as prometheus:metrics:<service>:<metric>)
        if _redis_client:
            try:
                # Use scan_iter to avoid blocking the Redis server
                for k in _redis_client.scan_iter(f"{REDIS_METRIC_PREFIX}*"):
                    if isinstance(k, bytes):
                        k = k.decode("utf-8")
                    if k.startswith(REDIS_METRIC_PREFIX):
                        rest = k[len(REDIS_METRIC_PREFIX) :]  # <service>:<metric_name>
                        # Extract metric name after first colon (service_name:metric_name)
                        if ":" in rest:
                            _, metric = rest.split(":", 1)
                        else:
                            metric = rest
                        metric_names.add(metric)
            except Exception:
                log_event(
                    service="metrics",
                    event="redis_key_scan_failed",
                    status="warn",
                    message="Failed to scan redis metric keys (best-effort).",
                )

        # Manual counters (per-service totals using get_metric_total which reads service-specific key)
        for name in sorted(metric_names):
            total = get_metric_total(name)
            lines.append(f"# HELP {name} Total count for {name}")
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {int(total)}")

        # Manual latency summaries
        for base, stats in sorted(snap.get("latencies", {}).items()):
            lines.append(f"# HELP {base}_avg Average {base}")
            lines.append(f"# TYPE {base}_avg gauge")
            lines.append(f"{base}_avg {float(stats.get('avg', 0.0))}")
            lines.append(f"# HELP {base}_count Count of {base} observations")
            lines.append(f"# TYPE {base}_count gauge")
            lines.append(f"{base}_count {int(stats.get('count', 0))}")

        # Compute Redis global aggregates across services for each metric_name
        global_totals: Dict[str, int] = {}
        if _redis_client:
            try:
                for name in sorted(metric_names):
                    # pattern: prometheus:metrics:*:<metric_name>
                    pattern = f"{REDIS_METRIC_PREFIX}*:{name}"
                    try:
                        keys = []
                        for k in _redis_client.scan_iter(pattern):
                            keys.append(k)
                        if not keys:
                            continue
                        # Use mget to fetch values in bulk
                        vals = _redis_client.mget(keys)
                        s = 0
                        for v in vals:
                            try:
                                if v is None:
                                    continue
                                if isinstance(v, bytes):
                                    v = v.decode("utf-8")
                                s += int(v)
                            except Exception:
                                # skip unparsable values
                                continue
                        global_totals[name] = s
                    except Exception:
                        # per-name failure should not abort whole export
                        log_event(
                            service="metrics",
                            event="redis_mget_failed",
                            status="warn",
                            message=f"Failed to mget/aggregate keys for pattern {pattern}",
                            extra={"pattern": pattern},
                        )
                if global_totals:
                    # Append global values to lines as gauge metrics with *_global suffix
                    for name in sorted(global_totals.keys()):
                        gname = f"{name}_global"
                        gval = int(global_totals[name])
                        lines.append(f"# HELP {gname} Global total across all services for {name}")
                        lines.append(f"# TYPE {gname} gauge")
                        lines.append(f"{gname} {gval}")

                    # Log aggregation summary
                    try:
                        log_event(
                            service="metrics",
                            event="redis_global_aggregate",
                            status="info",
                            message=f"Aggregated {len(global_totals)} global metrics from Redis",
                            extra={"metrics": list(global_totals.keys())},
                        )
                    except Exception:
                        pass
            except Exception:
                log_event(
                    service="metrics",
                    event="redis_global_aggregate_failed",
                    status="warn",
                    message="Failed during global aggregation (best-effort).",
                )
        else:
            # Redis not available — log and continue
            try:
                log_event(
                    service="metrics",
                    event="redis_unavailable_for_global_agg",
                    status="warn",
                    message="Redis client not configured; skipping global aggregation.",
                )
            except Exception:
                pass

        lines.append(f"# timestamp {snap.get('timestamp')}")

        # ---- Phase 11-B addition: merge prometheus_client REGISTRY output ----
        prom_text = []
        try:
            prom_text.append(generate_latest(REGISTRY).decode("utf-8"))
        except Exception as e:
            log_event(
                service="metrics",
                event="prometheus_export_error",
                status="error",
                message=f"Failed to merge REGISTRY: {e}",
            )

        combined = "\n".join(lines) + "\n" + "\n".join(prom_text)
        return combined
    except Exception as e:
        log_event(
            service="metrics",
            event="export_prometheus_error",
            status="error",
            message=f"Failed to export metrics: {e}",
        )
        return "# metrics_export_error 1\n"


# ------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------
def reset_collector() -> None:
    with _lock:
        _counters.clear()
        _latency_buckets.clear()
