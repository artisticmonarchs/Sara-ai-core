"""
metrics_collector.py — Phase 11-D (Unified Registry + Redis persistence, fixed)

Key fixes (Phase 11-D):
 - get_metric_total() now returns authoritative Redis-summed global total when Redis is present.
 - export_prometheus() now exposes a single canonical metric per logical metric name:
     - uses a temporary CollectorRegistry and registers Gauges with the merged/global values.
     - exposes latency <name>_count and <name>_sum (sum in seconds) merged from Redis.
 - observe_latency persists counts & sums to Redis so global latencies are available.
 - Adds metric index (SADD) for discovery and TTL refresh on writes.
 - Adds rate-limited Redis error logging.
"""

from __future__ import annotations
import threading
import math
from collections import defaultdict, deque
from typing import Dict, Deque, List, Any, Optional, Tuple
import statistics
import time
import os
import json
import traceback

from logging_utils import log_event

# Shared REGISTRY import (we no longer use local REGISTRY for generate_latest here)
from core.metrics_registry import REGISTRY  # type: ignore

# Prometheus client imports for temp registry construction & exposition
from prometheus_client import CollectorRegistry, Gauge, generate_latest

# Redis client (centralized)
try:
    from redis_client import redis_client as _redis_client  # type: ignore
except Exception:
    _redis_client = None

# ------------------------------------------------------------------
# In-process counters & latency buckets (thread-safe)
# ------------------------------------------------------------------
_DEFAULT_ROLLING_WINDOW = 1000  # samples per latency metric
_lock = threading.Lock()
_counters: Dict[str, int] = defaultdict(int)
_latency_buckets: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=_DEFAULT_ROLLING_WINDOW))

# Redis naming / index
REDIS_METRIC_PREFIX = "prometheus:metrics:"  # prometheus:metrics:<service>:<metric>
LATENCY_SUBPREFIX = "latency"               # prometheus:metrics:<service>:latency:<name>:count|sum
REDIS_INDEX_KEY = "prometheus:metrics:index"  # set of metric names for quick discovery

SERVICE_NAME = os.getenv("SERVICE_NAME", "unknown_service")

# TTL for metric keys (days -> seconds)
REDIS_METRIC_TTL_DAYS = int(os.getenv("REDIS_METRIC_TTL_DAYS", "30"))
REDIS_METRIC_TTL = REDIS_METRIC_TTL_DAYS * 24 * 3600

# Snapshot key (kept for compatibility)
REDIS_METRIC_SNAPSHOT_KEY = os.getenv("REDIS_METRIC_SNAPSHOT_KEY", "global_metrics_snapshot")

# Simple rate-limited Redis error logging (per-error-type)
_redis_error_timestamps: Dict[str, float] = {}
_REDIS_LOG_RATE_LIMIT_SEC = int(os.getenv("REDIS_LOG_RATE_LIMIT_SEC", "60"))


def _rate_limited_redis_log(event: str, level: str = "warn", **kwargs):
    """Log Redis-related issues but rate-limit repeated messages per event."""
    now = time.time()
    last = _redis_error_timestamps.get(event, 0.0)
    if now - last > _REDIS_LOG_RATE_LIMIT_SEC:
        _redis_error_timestamps[event] = now
        try:
            log_event(service="metrics", event=event, status=level, **kwargs)
        except Exception:
            # fallback print to avoid silent failures
            try:
                print(f"[metrics][{event}] {kwargs}")
            except Exception:
                pass


def _redis_key(metric_name: str) -> str:
    """Return a Redis key namespaced with the service name (legacy per-service totals)."""
    return f"{REDIS_METRIC_PREFIX}{SERVICE_NAME}:{metric_name}"


def _redis_latency_keys(metric_name: str) -> Tuple[str, str]:
    """Return (count_key, sum_key) for a latency metric for this service."""
    # e.g. prometheus:metrics:<service>:latency:<name>:count
    base = f"{REDIS_METRIC_PREFIX}{SERVICE_NAME}:{LATENCY_SUBPREFIX}:{metric_name}"
    return (f"{base}:count", f"{base}:sum")


# ------------------------------------------------------------------
# Backwards-compatible console shim
# ------------------------------------------------------------------
def init_redis_client(*args, **kwargs) -> None:
    try:
        log_event(service="metrics", event="init_redis_client_deprecated",
                  status="info" if _redis_client else "warn",
                  message=f"init_redis_client called; redis_present={bool(_redis_client)}")
    except Exception:
        pass


# ------------------------------------------------------------------
# Counter helpers
# ------------------------------------------------------------------
def increment_metric(metric_name: str, value: int = 1) -> bool:
    """Increment local counter and write per-service Redis total (best-effort).
    Also ensure metric index contains the metric_name and key TTL refreshed.
    """
    try:
        with _lock:
            _counters[metric_name] += int(value)
    except Exception as e:
        _rate_limited_redis_log("increment_local_failed", "error", message=str(e), stack=traceback.format_exc())
        # continue to try Redis writes even if local fails

    if _redis_client:
        try:
            pipe = _redis_client.pipeline()
            key = _redis_key(metric_name)
            pipe.incrby(key, int(value))
            # refresh TTL & add to index
            pipe.expire(key, REDIS_METRIC_TTL)
            pipe.sadd(REDIS_INDEX_KEY, metric_name)
            pipe.execute()
        except Exception as e:
            _rate_limited_redis_log("redis_incr_failed", "warn",
                                    message="Failed to incrby Redis metric (best-effort)",
                                    extra={"metric": metric_name, "value": value, "error": str(e)})
    return True


def inc_metric(name: str, amount: int = 1) -> bool:
    return increment_metric(name, amount)


def set_metric(name: str, value: int) -> bool:
    """Overwrite local and per-service Redis metric (best-effort)."""
    try:
        with _lock:
            _counters[name] = int(value)
    except Exception as e:
        _rate_limited_redis_log("set_local_failed", "error", message=str(e), stack=traceback.format_exc())

    if _redis_client:
        try:
            pipe = _redis_client.pipeline()
            pipe.set(_redis_key(name), int(value))
            pipe.expire(_redis_key(name), REDIS_METRIC_TTL)
            pipe.sadd(REDIS_INDEX_KEY, name)
            pipe.execute()
        except Exception as e:
            _rate_limited_redis_log("redis_set_failed", "warn",
                                    message="Failed to set metric in Redis (best-effort)",
                                    extra={"metric": name, "value": value, "error": str(e)})
    return True


def get_metric_total(metric_name: str) -> int:
    """
    Return the authoritative global total for metric_name.
    If Redis is available, sum across all services: pattern prometheus:metrics:*:<metric_name>.
    Otherwise fall back to local in-memory counter.
    """
    if _redis_client:
        try:
            # Use SCAN to find keys matching pattern and mget them in bulk
            pattern = f"{REDIS_METRIC_PREFIX}*:{metric_name}"
            keys = []
            for k in _redis_client.scan_iter(match=pattern):
                keys.append(k)
            if not keys:
                # No Redis keys (maybe first time) -> fallback to local
                with _lock:
                    return int(_counters.get(metric_name, 0))
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
            return int(s)
        except Exception as e:
            _rate_limited_redis_log("redis_get_total_failed", "warn",
                                    message="Failed to sum Redis metric keys; falling back to local",
                                    extra={"metric": metric_name, "error": str(e)})
            with _lock:
                return int(_counters.get(metric_name, 0))
    else:
        with _lock:
            return int(_counters.get(metric_name, 0))


# ------------------------------------------------------------------
# Latency handling (local + persisted aggregates)
# ------------------------------------------------------------------
def observe_latency(name: str, value_ms: float) -> bool:
    """
    Record latency sample locally and persist aggregated count & sum to Redis (best-effort).
    Redis keys per service:
      prometheus:metrics:<service>:latency:<name>:count  (integer)
      prometheus:metrics:<service>:latency:<name>:sum    (float ms stored)
    TTL refreshed on writes.
    """
    try:
        val = float(value_ms)
        with _lock:
            _latency_buckets[name].append(val)
    except Exception as e:
        _rate_limited_redis_log("observe_local_failed", "error", message=str(e), stack=traceback.format_exc())
        # continue to persist to redis if possible

    if _redis_client:
        try:
            count_key, sum_key = _redis_latency_keys(name)
            pipe = _redis_client.pipeline()
            pipe.incrby(count_key, 1)
            # Use INCRBYFLOAT if available to add fractional ms; fallback to incrby with int(ms)
            try:
                pipe.incrbyfloat(sum_key, float(val))
            except Exception:
                # older redis-py may not have incrbyfloat; fallback to get+set approach
                cur = _redis_client.get(sum_key)
                curf = float(cur) if cur else 0.0
                pipe.set(sum_key, curf + float(val))
            pipe.expire(count_key, REDIS_METRIC_TTL)
            pipe.expire(sum_key, REDIS_METRIC_TTL)
            # add latency metric name to index for discovery
            pipe.sadd(REDIS_INDEX_KEY, f"{LATENCY_SUBPREFIX}:{name}")
            pipe.execute()
        except Exception as e:
            _rate_limited_redis_log("redis_latency_write_failed", "warn",
                                    message="Failed to persist latency aggregates (best-effort)",
                                    extra={"latency": name, "value_ms": value_ms, "error": str(e)})
    return True


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


def get_snapshot() -> Dict[str, Any]:
    try:
        with _lock:
            counters_copy = dict(_counters)
            latencies_copy = {k: list(v) for k, v in _latency_buckets.items()}

        latencies_stats = {n: _compute_latency_stats(v) for n, v in latencies_copy.items()}

        # Avoid touching private internals of Prometheus objects; just report None if not available.
        streaming_info: Dict[str, Any] = {}
        try:
            streaming_info["tts_active_streams"] = None
            streaming_info["stream_bytes_out_total"] = None
        except Exception:
            streaming_info = {}

        snapshot = {
            "counters": counters_copy,
            "latencies": latencies_stats,
            "streaming": streaming_info,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        return snapshot
    except Exception as e:
        try:
            log_event(service="metrics", event="get_snapshot_failed", status="error",
                      message=str(e), extra={"stack": traceback.format_exc()})
        except Exception:
            pass
        return {"counters": {}, "latencies": {}, "streaming": {}, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


# ------------------------------------------------------------------
# Prometheus export (single canonical exposition using temporary registry)
# ------------------------------------------------------------------
def export_prometheus() -> str:
    """
    Produce a consistent Prometheus exposition representing global totals.
    Approach:
      - Discover metric names via Redis index (preferred) or local counters.
      - For each metric name, compute the global total via Redis aggregation if available,
        otherwise use local counter.
      - For latency metrics, compute global count & sum from Redis (if available), expose <name>_count and <name>_sum (seconds).
      - Register these as Gauges in a temporary CollectorRegistry and return generate_latest(temp_registry).
    Note: We intentionally DO NOT append generate_latest(REGISTRY) to avoid duplicate/conflicting metrics.
    """
    try:
        temp_registry = CollectorRegistry()
        metric_names = set()

        # 1) Discover names from Redis index if available
        if _redis_client:
            try:
                members = _redis_client.smembers(REDIS_INDEX_KEY)
                if members:
                    for m in members:
                        if isinstance(m, bytes):
                            m = m.decode("utf-8")
                        # index may contain latency:<name> entries; normalize
                        if isinstance(m, str) and m.startswith(f"{LATENCY_SUBPREFIX}:"):
                            # latency entries stored as latency:<name>; ensure we track base name separately
                            metric_names.add(m)  # keep as latency:name sentinel
                        else:
                            metric_names.add(m)
            except Exception as e:
                _rate_limited_redis_log("redis_index_read_failed", "warn",
                                        message="Failed to read Redis metric index (falling back to local keys)",
                                        extra={"error": str(e)})
        # 2) If index empty, fallback to local counters keys
        with _lock:
            for k in _counters.keys():
                metric_names.add(k)

        # Build consolidated numeric metrics in the temp registry
        for entry in sorted(metric_names):
            # detect latency sentinel entries ("latency:<name>")
            if isinstance(entry, str) and entry.startswith(f"{LATENCY_SUBPREFIX}:"):
                # latency metric name
                _, latency_name = entry.split(":", 1)
                # Aggregate counts and sums across services from Redis if possible
                total_count = 0
                total_sum_ms = 0.0
                if _redis_client:
                    try:
                        # pattern: prometheus:metrics:*:latency:<name>:count  and ...:sum
                        count_pattern = f"{REDIS_METRIC_PREFIX}*:{LATENCY_SUBPREFIX}:{latency_name}:count"
                        sum_pattern = f"{REDIS_METRIC_PREFIX}*:{LATENCY_SUBPREFIX}:{latency_name}:sum"

                        # gather keys and mget
                        ckeys = [k for k in _redis_client.scan_iter(match=count_pattern)]
                        if ckeys:
                            cvals = _redis_client.mget(ckeys)
                            for v in cvals:
                                try:
                                    if v is None:
                                        continue
                                    if isinstance(v, bytes):
                                        v = v.decode("utf-8")
                                    total_count += int(float(v))
                                except Exception:
                                    continue
                        skeys = [k for k in _redis_client.scan_iter(match=sum_pattern)]
                        if skeys:
                            svals = _redis_client.mget(skeys)
                            for v in svals:
                                try:
                                    if v is None:
                                        continue
                                    if isinstance(v, bytes):
                                        v = v.decode("utf-8")
                                    total_sum_ms += float(v)
                                except Exception:
                                    continue
                    except Exception as e:
                        _rate_limited_redis_log("redis_latency_aggregate_failed", "warn",
                                                message="Failed to aggregate latency keys from Redis",
                                                extra={"latency": latency_name, "error": str(e)})
                        # fallback: use local latency samples aggregate
                        with _lock:
                            local_samples = list(_latency_buckets.get(latency_name, []))
                        local_stats = _compute_latency_stats(local_samples)
                        total_count = int(local_stats.get("count", 0))
                        total_sum_ms = float(local_stats.get("sum", 0.0))
                else:
                    # No Redis -> use local samples
                    with _lock:
                        local_samples = list(_latency_buckets.get(latency_name, []))
                    local_stats = _compute_latency_stats(local_samples)
                    total_count = int(local_stats.get("count", 0))
                    total_sum_ms = float(local_stats.get("sum", 0.0))

                # Expose count (integer) and sum (in seconds)
                try:
                    g_count = Gauge(f"{latency_name}_count", f"Count of {latency_name} observations (global)", registry=temp_registry)
                    g_count.set(int(total_count))
                    g_sum = Gauge(f"{latency_name}_sum", f"Sum of {latency_name} in seconds (global)", registry=temp_registry)
                    g_sum.set(float(total_sum_ms) / 1000.0)  # convert ms -> s
                except Exception as e:
                    _rate_limited_redis_log("temp_registry_latency_register_failed", "warn",
                                            message="Failed to register latency gauges in temp registry",
                                            extra={"latency": latency_name, "error": str(e)})
                continue

            # Regular (counter-like) metric
            metric = entry
            # Compute global total via Redis aggregation if available
            total = 0
            if _redis_client:
                try:
                    pattern = f"{REDIS_METRIC_PREFIX}*:{metric}"
                    keys = [k for k in _redis_client.scan_iter(match=pattern)]
                    if keys:
                        vals = _redis_client.mget(keys)
                        s = 0
                        for v in vals:
                            try:
                                if v is None:
                                    continue
                                if isinstance(v, bytes):
                                    v = v.decode("utf-8")
                                s += int(float(v))
                            except Exception:
                                continue
                        total = int(s)
                    else:
                        # no redis keys -> fallback to local counter
                        with _lock:
                            total = int(_counters.get(metric, 0))
                except Exception as e:
                    _rate_limited_redis_log("redis_metric_aggregate_failed", "warn",
                                            message="Failed to aggregate metric from Redis (fallback to local)",
                                            extra={"metric": metric, "error": str(e)})
                    with _lock:
                        total = int(_counters.get(metric, 0))
            else:
                with _lock:
                    total = int(_counters.get(metric, 0))

            # Register a Gauge with the canonical metric name and set the merged value
            try:
                g = Gauge(metric, f"Global total for {metric} (aggregated across services)", registry=temp_registry)
                g.set(int(total))
            except Exception as e:
                _rate_limited_redis_log("temp_registry_metric_register_failed", "warn",
                                        message="Failed to register metric in temp registry",
                                        extra={"metric": metric, "error": str(e)})
                continue

        # Also expose a timestamp of the snapshot for debugging
        try:
            ts_g = Gauge("_metrics_snapshot_timestamp_seconds", "Snapshot timestamp (epoch seconds)", registry=temp_registry)
            ts_g.set(float(time.time()))
        except Exception:
            pass

        # Return Prometheus text exposition of the temp registry
        try:
            text = generate_latest(temp_registry).decode("utf-8")
            return text
        except Exception as e:
            log_event(service="metrics", event="generate_latest_temp_failed", status="error",
                      message="Failed to generate exposition from temp registry", extra={"error": str(e), "stack": traceback.format_exc()})
            return "# metrics_export_error 1\n"
    except Exception as e:
        log_event(service="metrics", event="export_prometheus_unhandled", status="error",
                  message="Unhandled error in export_prometheus", extra={"error": str(e), "stack": traceback.format_exc()})
        return "# metrics_export_error 1\n"


# ------------------------------------------------------------------
# Redis snapshot push/pull helpers (compat)
# ------------------------------------------------------------------
def push_metrics_snapshot_to_redis(snapshot: dict) -> bool:
    """Store the latest metrics snapshot in Redis as JSON under REDIS_METRIC_SNAPSHOT_KEY."""
    if not _redis_client:
        _rate_limited_redis_log("redis_unavailable_snapshot_save", "warn", message="Redis not configured; snapshot not saved.")
        return False
    try:
        payload = json.dumps(snapshot)
        _redis_client.set(REDIS_METRIC_SNAPSHOT_KEY, payload)
        _redis_client.expire(REDIS_METRIC_SNAPSHOT_KEY, REDIS_METRIC_TTL)
        log_event(service="metrics", event="snapshot_saved", status="info", message="Metrics snapshot saved")
        return True
    except Exception as e:
        _rate_limited_redis_log("snapshot_save_failed", "error", message=str(e), stack=traceback.format_exc())
        return False


def pull_metrics_snapshot_from_redis() -> dict:
    """Retrieve the latest metrics snapshot from Redis. Returns empty dict if missing/error."""
    if not _redis_client:
        _rate_limited_redis_log("redis_unavailable_snapshot_load", "warn", message="Redis not configured; cannot load snapshot.")
        return {}
    try:
        raw = _redis_client.get(REDIS_METRIC_SNAPSHOT_KEY)
        if not raw:
            return {}
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        log_event(service="metrics", event="snapshot_loaded", status="info", message="Metrics snapshot loaded")
        return data
    except Exception as e:
        _rate_limited_redis_log("snapshot_load_failed", "warn", message=str(e), stack=traceback.format_exc())
        return {}


# ------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------
def reset_collector() -> None:
    with _lock:
        _counters.clear()
        _latency_buckets.clear()
