"""
metrics_collector.py — Phase 11-F (Unified Registry + Redis persistence, fixed)

Key fixes (Phase 11-F):
 - get_metric_total() now returns authoritative Redis-summed global total when Redis is present.
 - export_prometheus() now exposes a single canonical metric per logical metric name:
     - uses a temporary CollectorRegistry and registers Gauges with the merged/global values.
     - exposes latency <name>_count and <name>_sum (sum in seconds) merged from Redis.
 - observe_latency persists counts & sums to Redis so global latencies are available.
 - Adds metric index (SADD) for discovery and TTL refresh on writes.
 - Adds rate-limited Redis error logging.
 - Phase 11-F enhancements: structured logging, config integration, circuit breaker awareness,
   health metrics, fault-tolerant sync loop, and Prometheus endpoint compatibility.
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
import uuid
from contextlib import contextmanager

# --------------------------------------------------------------------------
# Phase 11-F Configuration Integration
# --------------------------------------------------------------------------
try:
    from config import Config
    config = Config
    
    def get_metrics_config():
        return {
            "enable_sync": getattr(config, "ENABLE_METRICS_SYNC", True),
            "sync_interval": getattr(config, "METRICS_SYNC_INTERVAL", 30),
            "snapshot_interval": getattr(config, "SNAPSHOT_INTERVAL", 60),
            "enable_persistence": getattr(config, "ENABLE_METRICS_PERSISTENCE", True),
            "enable_endpoint": getattr(config, "METRICS_ENDPOINT_ENABLED", True),
        }
except ImportError:
    # Fallback configuration for backward compatibility
    class FallbackConfig:
        ENABLE_METRICS_SYNC = os.getenv("ENABLE_METRICS_SYNC", "true").lower() == "true"
        METRICS_SYNC_INTERVAL = int(os.getenv("METRICS_SYNC_INTERVAL", "30"))
        SNAPSHOT_INTERVAL = int(os.getenv("SNAPSHOT_INTERVAL", "60"))
        ENABLE_METRICS_PERSISTENCE = os.getenv("ENABLE_METRICS_PERSISTENCE", "true").lower() == "true"
        METRICS_ENDPOINT_ENABLED = os.getenv("METRICS_ENDPOINT_ENABLED", "true").lower() == "true"
    
    config = FallbackConfig()
    
    def get_metrics_config():
        return {
            "enable_sync": config.ENABLE_METRICS_SYNC,
            "sync_interval": config.METRICS_SYNC_INTERVAL,
            "snapshot_interval": config.SNAPSHOT_INTERVAL,
            "enable_persistence": config.ENABLE_METRICS_PERSISTENCE,
            "enable_endpoint": config.METRICS_ENDPOINT_ENABLED,
        }

# Prometheus client imports for temp registry construction & exposition
from prometheus_client import CollectorRegistry, Gauge, generate_latest

# --------------------------------------------------------------------------
# Lazy Redis Client Shim to Avoid Circular Imports
# --------------------------------------------------------------------------
def _has_redis():
    """Check if Redis client is available without importing at module level."""
    try:
        import redis_client
        return True
    except Exception:
        return False

def _get_redis_utils():
    """Lazy import Redis utilities to break circular imports."""
    try:
        from redis_client import (
            safe_redis_operation, 
            increment_metric_redis, 
            get_metric_redis,
            set_key_redis,
            get_key_redis,
            save_snapshot,
            load_snapshot,
            get_circuit_breaker_status
        )
        return (safe_redis_operation, increment_metric_redis, get_metric_redis,
                set_key_redis, get_key_redis, save_snapshot, load_snapshot,
                get_circuit_breaker_status, True)
    except Exception:
        # Fallback implementations
        def safe_redis_operation(*args, **kwargs):
            def decorator(func):
                return func
            return decorator
        
        def increment_metric_redis(*args, **kwargs):
            pass
        
        def get_metric_redis(*args, **kwargs):
            return 0
        
        def set_key_redis(*args, **kwargs):
            return False
        
        def get_key_redis(*args, **kwargs):
            return None
        
        def save_snapshot(*args, **kwargs):
            return False
        
        def load_snapshot(*args, **kwargs):
            return None
        
        def get_circuit_breaker_status():
            return {"state": "DISABLED", "enabled": False}
        
        return (safe_redis_operation, increment_metric_redis, get_metric_redis,
                set_key_redis, get_key_redis, save_snapshot, load_snapshot,
                get_circuit_breaker_status, False)

# --------------------------------------------------------------------------
# Lazy Logging Shim to Avoid Circular Imports
# --------------------------------------------------------------------------
def _get_logger():
    """Lazy import logging utilities to break circular imports."""
    try:
        from logging_utils import log_event
        return log_event
    except Exception:
        def _noop_log(*a, **k): pass
        return _noop_log

# --------------------------------------------------------------------------
# Lazy service name resolver to avoid import-time race conditions
# --------------------------------------------------------------------------
def get_service_name() -> str:
    """
    Return the configured service name at runtime.
    Reads environment variables dynamically so it reflects the actual
    container/service name even if this module was imported early.
    """
    env_name = os.getenv("SERVICE_NAME")
    if env_name:
        return env_name

    try:
        return getattr(config, "SERVICE_NAME")
    except Exception:
        return "unknown_service"

# --------------------------------------------------------------------------
# Phase 11-F Health Metrics
# --------------------------------------------------------------------------
_health_metrics = {
    "metrics_collector_start_time": time.time(),
    "metrics_sync_success_total": 0,
    "metrics_sync_failure_total": 0,
    "metrics_persistence_success_total": 0,
    "metrics_persistence_failure_total": 0,
    "last_successful_sync": 0,
    "last_successful_persistence": 0,
}

# --------------------------------------------------------------------------
# In-process counters & latency buckets (thread-safe)
# --------------------------------------------------------------------------
_DEFAULT_ROLLING_WINDOW = 1000  # samples per latency metric
_lock = threading.RLock()  # Upgraded to RLock for Phase 11-F thread safety
_counters: Dict[str, int] = defaultdict(int)
_latency_buckets: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=_DEFAULT_ROLLING_WINDOW))

# Redis naming / index
REDIS_METRIC_PREFIX = "prometheus:metrics:"  # prometheus:metrics:<service>:<metric>
LATENCY_SUBPREFIX = "latency"               # prometheus:metrics:<service>:latency:<name>:count|sum
REDIS_INDEX_KEY = "prometheus:metrics:index"  # set of metric names for quick discovery

# TTL for metric keys (days -> seconds)
REDIS_METRIC_TTL_DAYS = int(os.getenv("REDIS_METRIC_TTL_DAYS", "30"))
REDIS_METRIC_TTL = REDIS_METRIC_TTL_DAYS * 24 * 3600

# Snapshot key (kept for compatibility)
REDIS_METRIC_SNAPSHOT_KEY = os.getenv("REDIS_METRIC_SNAPSHOT_KEY", "global_metrics_snapshot")

# Simple rate-limited Redis error logging (per-error-type)
_redis_error_timestamps: Dict[str, float] = {}
_REDIS_LOG_RATE_LIMIT_SEC = int(os.getenv("REDIS_LOG_RATE_LIMIT_SEC", "60"))

# Phase 11-F Sync Control
_sync_enabled = get_metrics_config()["enable_sync"]
_sync_interval = get_metrics_config()["sync_interval"]
_snapshot_interval = get_metrics_config()["snapshot_interval"]
_sync_thread: Optional[threading.Thread] = None
_sync_running = False
_sync_lock = threading.Lock()

# --------------------------------------------------------------------------
# Phase 11-F Structured Logging Helpers
# --------------------------------------------------------------------------
def _log_metric_event(event: str, status: str, message: str, **extra):
    """Structured logging for metric events with Phase 11-F context."""
    log_func = _get_logger()
    log_func(
        service="metrics_collector",
        event=event,
        status=status,
        message=message,
        extra={
            "service_name": get_service_name(),
            "sync_enabled": _sync_enabled,
            "sync_interval": _sync_interval,
            **extra
        }
    )

def _rate_limited_redis_log(event: str, level: str = "warn", **kwargs):
    """Log Redis-related issues but rate-limit repeated messages per event."""
    now = time.time()
    last = _redis_error_timestamps.get(event, 0.0)
    if now - last > _REDIS_LOG_RATE_LIMIT_SEC:
        _redis_error_timestamps[event] = now
        try:
            _log_metric_event(event, level, **kwargs)
        except Exception:
            # fallback print to avoid silent failures
            try:
                print(f"[metrics][{event}] {kwargs}")
            except Exception:
                pass

def _log_sync_activity(event: str, status: str, message: str, **extra):
    """Structured logging for sync activities."""
    _log_metric_event(
        f"sync_{event}",
        status,
        message,
        sync_interval=_sync_interval,
        snapshot_interval=_snapshot_interval,
        **extra
    )

# --------------------------------------------------------------------------
# Phase 11-F Fault-Tolerant Sync Loop
# --------------------------------------------------------------------------
def _start_sync_loop():
    """Start the background metrics sync loop."""
    global _sync_thread, _sync_running
    
    with _sync_lock:
        if _sync_thread and _sync_thread.is_alive():
            return
        
        _sync_running = True
        _sync_thread = threading.Thread(target=_sync_loop, daemon=True, name="MetricsSync")
        _sync_thread.start()
        _log_metric_event(
            "sync_loop_started",
            "info",
            "Background metrics sync loop started",
            sync_interval=_sync_interval,
            snapshot_interval=_snapshot_interval
        )

def _stop_sync_loop():
    """Stop the background metrics sync loop."""
    global _sync_running
    _sync_running = False
    if _sync_thread and _sync_thread.is_alive():
        _sync_thread.join(timeout=5.0)
    _log_metric_event("sync_loop_stopped", "info", "Background metrics sync loop stopped")

def _sync_loop():
    """Background loop for syncing metrics and snapshots."""
    last_sync = 0
    last_snapshot = 0
    
    while _sync_running:
        try:
            current_time = time.time()
            metrics_config = get_metrics_config()
            
            # Sync metrics to Redis if enabled and interval elapsed
            if metrics_config["enable_sync"] and (current_time - last_sync) >= metrics_config["sync_interval"]:
                if _sync_metrics_to_redis():
                    last_sync = current_time
                    _health_metrics["metrics_sync_success_total"] += 1
                    _health_metrics["last_successful_sync"] = current_time
                else:
                    _health_metrics["metrics_sync_failure_total"] += 1
            
            # Create snapshots if enabled and interval elapsed
            if metrics_config["enable_persistence"] and (current_time - last_snapshot) >= metrics_config["snapshot_interval"]:
                if _create_and_save_snapshot():
                    last_snapshot = current_time
                    _health_metrics["metrics_persistence_success_total"] += 1
                    _health_metrics["last_successful_persistence"] = current_time
                else:
                    _health_metrics["metrics_persistence_failure_total"] += 1
            
            # Update health metrics
            _health_metrics["metrics_collector_start_time"] = _health_metrics["metrics_collector_start_time"]  # Keep original start time
            
            time.sleep(5)  # Check every 5 seconds for better CPU usage
            
        except Exception as e:
            _log_sync_activity(
                "loop_error",
                "error",
                "Unhandled error in sync loop",
                error=str(e),
                stack_trace=traceback.format_exc()
            )
            time.sleep(5)  # Back off on errors

def _sync_metrics_to_redis() -> bool:
    """Sync current metrics to Redis with circuit breaker protection."""
    try:
        with _lock:
            counters_copy = dict(_counters)
            latencies_copy = {k: list(v) for k, v in _latency_buckets.items()}
        
        # Sync counters
        for metric_name, value in counters_copy.items():
            safe_redis_operation, _, _, _, _, _, _, get_circuit_breaker_status, _ = _get_redis_utils()
            safe_redis_operation("sync_counter")(lambda client: client.set(
                _redis_key(metric_name), 
                value, 
                ex=REDIS_METRIC_TTL
            ))
        
        # Sync latency aggregates
        for metric_name, samples in latencies_copy.items():
            if samples:
                count_key, sum_key = _redis_latency_keys(metric_name)
                total_sum = sum(samples)
                
                safe_redis_operation("sync_latency_count")(lambda client: client.set(
                    count_key, len(samples), ex=REDIS_METRIC_TTL
                ))
                safe_redis_operation("sync_latency_sum")(lambda client: client.set(
                    sum_key, total_sum, ex=REDIS_METRIC_TTL
                ))
        
        _log_sync_activity("metrics_synced", "info", "Metrics successfully synced to Redis")
        return True
        
    except Exception as e:
        _log_sync_activity(
            "metrics_sync_failed",
            "error",
            "Failed to sync metrics to Redis",
            error=str(e),
            circuit_breaker_state=_get_redis_utils()[-2]()["state"]  # get_circuit_breaker_status
        )
        return False

def _create_and_save_snapshot() -> bool:
    """Create and save a metrics snapshot with Phase 11-F persistence."""
    try:
        snapshot = get_snapshot()
        snapshot_key = f"metrics_snapshot:{get_service_name()}:{int(time.time())}"
        
        _, _, _, _, _, save_snapshot, _, _, has_redis = _get_redis_utils()
        success = save_snapshot(
            snapshot_key,
            snapshot,
            ttl=REDIS_METRIC_TTL
        )
        
        if success:
            _log_sync_activity(
                "snapshot_saved",
                "info",
                "Metrics snapshot saved successfully",
                snapshot_key=snapshot_key
            )
        elif not has_redis:
            # Don't log warnings if Redis is not available
            pass
        else:
            _rate_limited_redis_log(
                "snapshot_failed",
                "warn",
                message="Failed to save metrics snapshot",
                circuit_breaker_state=_get_redis_utils()[-2]()["state"]  # get_circuit_breaker_status
            )
        
        return success
        
    except Exception as e:
        _log_sync_activity(
            "snapshot_error",
            "error",
            "Error creating/saving metrics snapshot",
            error=str(e)
        )
        return False

# --------------------------------------------------------------------------
# Redis Key Helpers
# --------------------------------------------------------------------------
def _redis_key(metric_name: str) -> str:
    """Return a Redis key namespaced with the service name (legacy per-service totals)."""
    return f"{REDIS_METRIC_PREFIX}{get_service_name()}:{metric_name}"

def _redis_latency_keys(metric_name: str) -> Tuple[str, str]:
    """Return (count_key, sum_key) for a latency metric for this service."""
    # e.g. prometheus:metrics:<service>:latency:<name>:count
    base = f"{REDIS_METRIC_PREFIX}{get_service_name()}:{LATENCY_SUBPREFIX}:{metric_name}"
    return (f"{base}:count", f"{base}:sum")

# --------------------------------------------------------------------------
# Backwards-compatible console shim
# --------------------------------------------------------------------------
def init_redis_client(*args, **kwargs) -> None:
    try:
        _log_metric_event(
            "init_redis_client_deprecated",
            "info" if _has_redis() else "warn",
            "init_redis_client called",
            redis_present=_has_redis()
        )
    except Exception:
        pass

# --------------------------------------------------------------------------
# Counter helpers
# --------------------------------------------------------------------------
def increment_metric(metric_name: str, value: int = 1) -> bool:
    """Increment local counter and write per-service Redis total (best-effort).
    Also ensure metric index contains the metric_name and key TTL refreshed.
    """
    # 🧩 Skip self-metrics to avoid recursion
    if metric_name.startswith("metrics_collector_"):
        return True
        
    try:
        with _lock:
            _counters[metric_name] += int(value)
    except Exception as e:
        _rate_limited_redis_log("increment_local_failed", "error", message=str(e), stack=traceback.format_exc())
        # continue to try Redis writes even if local fails

    if _has_redis():
        try:
            # Use Phase 11-F safe Redis operation
            safe_redis_operation, _, _, _, _, _, _, _, _ = _get_redis_utils()
            success = safe_redis_operation("increment_metric_persist")(
                lambda client: client.incrby(_redis_key(metric_name), int(value))
            )
            
            if success is not None:
                # Refresh TTL & add to index using safe operations
                safe_redis_operation("refresh_metric_ttl")(
                    lambda client: client.expire(_redis_key(metric_name), REDIS_METRIC_TTL)
                )
                safe_redis_operation("add_metric_index")(
                    lambda client: client.sadd(REDIS_INDEX_KEY, metric_name)
                )
                
                _log_metric_event(
                    "metric_incremented",
                    "info",
                    "Metric incremented successfully",
                    metric_name=metric_name,
                    value=value,
                    new_total=_counters[metric_name]
                )
            else:
                _rate_limited_redis_log("redis_incr_failed", "warn",
                                        message="Failed to increment Redis metric (circuit breaker)",
                                        extra={"metric": metric_name, "value": value})
        except Exception as e:
            _rate_limited_redis_log("redis_incr_failed", "warn",
                                    message="Failed to incrby Redis metric (best-effort)",
                                    extra={"metric": metric_name, "value": value, "error": str(e)})
    return True

def inc_metric(name: str, amount: int = 1) -> bool:
    return increment_metric(name, amount)

def set_metric(name: str, value: int) -> bool:
    """Overwrite local and per-service Redis metric (best-effort)."""
    # 🧩 Skip self-metrics to avoid recursion
    if name.startswith("metrics_collector_"):
        return True
        
    try:
        with _lock:
            _counters[name] = int(value)
    except Exception as e:
        _rate_limited_redis_log("set_local_failed", "error", message=str(e), stack=traceback.format_exc())

    if _has_redis():
        try:
            safe_redis_operation, _, _, _, _, _, _, _, _ = _get_redis_utils()
            success = safe_redis_operation("set_metric_persist")(
                lambda client: client.set(_redis_key(name), int(value), ex=REDIS_METRIC_TTL)
            )
            
            if success:
                safe_redis_operation("add_metric_index")(
                    lambda client: client.sadd(REDIS_INDEX_KEY, name)
                )
                
                _log_metric_event(
                    "metric_set",
                    "info",
                    "Metric set successfully",
                    metric_name=name,
                    value=value
                )
            else:
                _rate_limited_redis_log("redis_set_failed", "warn",
                                        message="Failed to set metric in Redis (circuit breaker)",
                                        extra={"metric": name, "value": value})
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
    if _has_redis():
        try:
            # Use SCAN to find keys matching pattern and mget them in bulk
            pattern = f"{REDIS_METRIC_PREFIX}*:{metric_name}"
            
            def scan_and_sum(client):
                keys = []
                for k in client.scan_iter(match=pattern):
                    keys.append(k)
                if not keys:
                    return None
                vals = client.mget(keys)
                s = 0
                for v in vals:
                    try:
                        if v is None:
                            continue
                        if isinstance(v, bytes):
                            v = v.decode("utf-8")
                        s += int(v)
                    except Exception:
                        continue
                return s
            
            safe_redis_operation, _, _, _, _, _, _, _, _ = _get_redis_utils()
            result = safe_redis_operation("get_metric_total")(scan_and_sum)
            
            if result is not None:
                return int(result)
            else:
                # No Redis keys or circuit breaker blocked -> fallback to local
                with _lock:
                    return int(_counters.get(metric_name, 0))
                    
        except Exception as e:
            _rate_limited_redis_log("redis_get_total_failed", "warn",
                                    message="Failed to sum Redis metric keys; falling back to local",
                                    extra={"metric": metric_name, "error": str(e)})
            with _lock:
                return int(_counters.get(metric_name, 0))
    else:
        with _lock:
            return int(_counters.get(metric_name, 0))

# --------------------------------------------------------------------------
# Latency handling (local + persisted aggregates)
# --------------------------------------------------------------------------
def observe_latency(name: str, value_ms: float) -> bool:
    """
    Record latency sample locally and persist aggregated count & sum to Redis (best-effort).
    Redis keys per service:
      prometheus:metrics:<service>:latency:<name>:count  (integer)
      prometheus:metrics:<service>:latency:<name>:sum    (float ms stored)
    TTL refreshed on writes.
    """
    # 🧩 Skip self-metrics to avoid recursion
    if name.startswith("metrics_collector_"):
        return True
        
    try:
        val = float(value_ms)
        with _lock:
            _latency_buckets[name].append(val)
    except Exception as e:
        _rate_limited_redis_log("observe_local_failed", "error", message=str(e), stack=traceback.format_exc())
        # continue to persist to redis if possible

    if _has_redis():
        try:
            count_key, sum_key = _redis_latency_keys(name)
            
            def persist_latency(client):
                pipe = client.pipeline()
                pipe.incrby(count_key, 1)
                try:
                    pipe.incrbyfloat(sum_key, float(val))
                except Exception:
                    # older redis-py may not have incrbyfloat; fallback to get+set approach
                    cur = client.get(sum_key)
                    curf = float(cur) if cur else 0.0
                    pipe.set(sum_key, curf + float(val))
                pipe.expire(count_key, REDIS_METRIC_TTL)
                pipe.expire(sum_key, REDIS_METRIC_TTL)
                pipe.sadd(REDIS_INDEX_KEY, f"{LATENCY_SUBPREFIX}:{name}")
                return pipe.execute()
            
            safe_redis_operation, _, _, _, _, _, _, _, _ = _get_redis_utils()
            success = safe_redis_operation("persist_latency")(persist_latency)
            
            if success is None:
                _rate_limited_redis_log("redis_latency_write_failed", "warn",
                                        message="Failed to persist latency aggregates (circuit breaker)",
                                        extra={"latency": name, "value_ms": value_ms})
            else:
                _log_metric_event(
                    "latency_observed",
                    "info",
                    "Latency observed and persisted",
                    latency_name=name,
                    value_ms=value_ms
                )
                
        except Exception as e:
            _rate_limited_redis_log("redis_latency_write_failed", "warn",
                                    message="Failed to persist latency aggregates (best-effort)",
                                    extra={"latency": name, "value_ms": value_ms, "error": str(e)})
    return True

# --------------------------------------------------------------------------
# Snapshot
# --------------------------------------------------------------------------
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
    """Get comprehensive metrics snapshot with Phase 11-F health metrics."""
    try:
        with _lock:
            counters_copy = dict(_counters)
            latencies_copy = {k: list(v) for k, v in _latency_buckets.items()}

        latencies_stats = {n: _compute_latency_stats(v) for n, v in latencies_copy.items()}

        # Include Phase 11-F health metrics
        health_info = {
            "uptime_seconds": time.time() - _health_metrics["metrics_collector_start_time"],
            "sync_success_total": _health_metrics["metrics_sync_success_total"],
            "sync_failure_total": _health_metrics["metrics_sync_failure_total"],
            "persistence_success_total": _health_metrics["metrics_persistence_success_total"],
            "persistence_failure_total": _health_metrics["metrics_persistence_failure_total"],
            "last_successful_sync": _health_metrics["last_successful_sync"],
            "last_successful_persistence": _health_metrics["last_successful_persistence"],
            "circuit_breaker_state": _get_redis_utils()[-2]()["state"],  # get_circuit_breaker_status
        }

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
            "health": health_info,
            "config": get_metrics_config(),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        return snapshot
    except Exception as e:
        try:
            _log_metric_event("get_snapshot_failed", "error", message=str(e), stack=traceback.format_exc())
        except Exception:
            pass
        return {
            "counters": {}, 
            "latencies": {}, 
            "streaming": {}, 
            "health": {},
            "config": {},
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }

# --------------------------------------------------------------------------
# Prometheus export (single canonical exposition using temporary registry)
# --------------------------------------------------------------------------
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
        if _has_redis():
            try:
                def get_index_members(client):
                    return client.smembers(REDIS_INDEX_KEY)
                
                safe_redis_operation, _, _, _, _, _, _, _, _ = _get_redis_utils()
                members = safe_redis_operation("get_index_members")(get_index_members)
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
                if _has_redis():
                    try:
                        # pattern: prometheus:metrics:*:latency:<name>:count  and ...:sum
                        count_pattern = f"{REDIS_METRIC_PREFIX}*:{LATENCY_SUBPREFIX}:{latency_name}:count"
                        sum_pattern = f"{REDIS_METRIC_PREFIX}*:{LATENCY_SUBPREFIX}:{latency_name}:sum"

                        def aggregate_latency(client):
                            ckeys = [k for k in client.scan_iter(match=count_pattern)]
                            skeys = [k for k in client.scan_iter(match=sum_pattern)]
                            
                            cvals = client.mget(ckeys) if ckeys else []
                            svals = client.mget(skeys) if skeys else []
                            
                            count_total = 0
                            for v in cvals:
                                try:
                                    if v is None:
                                        continue
                                    if isinstance(v, bytes):
                                        v = v.decode("utf-8")
                                    count_total += int(float(v))
                                except Exception:
                                    continue
                            
                            sum_total = 0.0
                            for v in svals:
                                try:
                                    if v is None:
                                        continue
                                    if isinstance(v, bytes):
                                        v = v.decode("utf-8")
                                    sum_total += float(v)
                                except Exception:
                                    continue
                            
                            return count_total, sum_total
                        
                        safe_redis_operation, _, _, _, _, _, _, _, _ = _get_redis_utils()
                        result = safe_redis_operation("aggregate_latency")(aggregate_latency)
                        if result:
                            total_count, total_sum_ms = result
                        else:
                            # Circuit breaker blocked -> fallback to local
                            with _lock:
                                local_samples = list(_latency_buckets.get(latency_name, []))
                            local_stats = _compute_latency_stats(local_samples)
                            total_count = int(local_stats.get("count", 0))
                            total_sum_ms = float(local_stats.get("sum", 0.0))
                            
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
                    g_count = safe_register_metric(Gauge(f"{latency_name}_count", f"Count of {latency_name} observations (global)", registry=temp_registry), temp_registry)
                    g_count.set(int(total_count))
                    g_sum = safe_register_metric(Gauge(f"{latency_name}_sum", f"Sum of {latency_name} in seconds (global)", registry=temp_registry), temp_registry)
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
            if _has_redis():
                try:
                    def aggregate_metric(client):
                        pattern = f"{REDIS_METRIC_PREFIX}*:{metric}"
                        keys = [k for k in client.scan_iter(match=pattern)]
                        if not keys:
                            return None
                        vals = client.mget(keys)
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
                        return s
                    
                    safe_redis_operation, _, _, _, _, _, _, _, _ = _get_redis_utils()
                    result = safe_redis_operation("aggregate_metric")(aggregate_metric)
                    if result is not None:
                        total = int(result)
                    else:
                        # Circuit breaker blocked -> fallback to local
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
                g = safe_register_metric(Gauge(metric, f"Global total for {metric} (aggregated across services)", registry=temp_registry), temp_registry)
                g.set(int(total))
            except Exception as e:
                _rate_limited_redis_log("temp_registry_metric_register_failed", "warn",
                                        message="Failed to register metric in temp registry",
                                        extra={"metric": metric, "error": str(e)})
                continue

        # Also expose a timestamp of the snapshot for debugging
        try:
            ts_g = safe_register_metric(Gauge("_metrics_snapshot_timestamp_seconds", "Snapshot timestamp (epoch seconds)", registry=temp_registry), temp_registry)
            ts_g.set(float(time.time()))
        except Exception:
            pass

        # Return Prometheus text exposition of the temp registry
        try:
            text = generate_latest(temp_registry).decode("utf-8")
            return text
        except Exception as e:
            _log_metric_event("generate_latest_temp_failed", "error",
                      message="Failed to generate exposition from temp registry", 
                      extra={"error": str(e), "stack": traceback.format_exc()})
            return "# metrics_export_error 1\n"
    except Exception as e:
        _log_metric_event("export_prometheus_unhandled", "error",
                  message="Unhandled error in export_prometheus", 
                  extra={"error": str(e), "stack": traceback.format_exc()})
        return "# metrics_export_error 1\n"

# --------------------------------------------------------------------------
# Redis snapshot push/pull helpers (compat)
# --------------------------------------------------------------------------
def push_metrics_snapshot_to_redis(snapshot: dict) -> bool:
    """Store the latest metrics snapshot in Redis as JSON under REDIS_METRIC_SNAPSHOT_KEY."""
    if not _has_redis():
        _rate_limited_redis_log("redis_unavailable_snapshot_save", "warn", message="Redis not configured; snapshot not saved.")
        return False
    try:
        safe_redis_operation, _, _, _, _, _, _, get_circuit_breaker_status, _ = _get_redis_utils()
        success = safe_redis_operation("push_snapshot")(
            lambda client: client.set(REDIS_METRIC_SNAPSHOT_KEY, json.dumps(snapshot), ex=REDIS_METRIC_TTL)
        )
        
        if success:
            _log_metric_event("snapshot_saved", "info", "Metrics snapshot saved")
        else:
            _rate_limited_redis_log("snapshot_save_failed", "warn", 
                                    message="Failed to save snapshot (circuit breaker)",
                                    circuit_breaker_state=get_circuit_breaker_status()["state"])
        
        return bool(success)
    except Exception as e:
        _rate_limited_redis_log("snapshot_save_failed", "error", message=str(e), stack=traceback.format_exc())
        return False

def pull_metrics_snapshot_from_redis() -> dict:
    """Retrieve the latest metrics snapshot from Redis. Returns empty dict if missing/error."""
    if not _has_redis():
        _rate_limited_redis_log("redis_unavailable_snapshot_load", "warn", message="Redis not configured; cannot load snapshot.")
        return {}
    try:
        def load_snapshot_data(client):
            raw = client.get(REDIS_METRIC_SNAPSHOT_KEY)
            if not raw:
                return {}
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            return json.loads(raw)
        
        safe_redis_operation, _, _, _, _, _, _, get_circuit_breaker_status, _ = _get_redis_utils()
        data = safe_redis_operation("pull_snapshot")(load_snapshot_data)
        
        if data:
            _log_metric_event("snapshot_loaded", "info", "Metrics snapshot loaded")
        else:
            _rate_limited_redis_log("snapshot_load_failed", "warn", 
                                    message="Failed to load snapshot (circuit breaker or empty)",
                                    circuit_breaker_state=get_circuit_breaker_status()["state"])
        
        return data if data else {}
    except Exception as e:
        _rate_limited_redis_log("snapshot_load_failed", "warn", message=str(e), stack=traceback.format_exc())
        return {}

# --------------------------------------------------------------------------
# Phase 11-F Utility Functions for Duplicate Prevention and Safe Operations
# --------------------------------------------------------------------------
def safe_register_metric(metric, registry):
    """Avoid duplicate metric registration in the same registry."""
    try:
        registry.unregister(metric)
    except KeyError:
        pass
    return metric

@contextmanager
def redis_lock(lock_name, timeout=10):
    """Redis-based distributed lock for preventing concurrent operations."""
    if not _has_redis():
        yield False
        return
        
    token = str(uuid.uuid4())
    try:
        # Try to acquire the lock
        safe_redis_operation, _, _, _, _, _, _, _, _ = _get_redis_utils()
        acquired = safe_redis_operation("acquire_lock")(
            lambda client: client.set(lock_name, token, nx=True, ex=timeout)
        )
        
        if not acquired:
            yield False
            return
            
        try:
            yield True
        finally:
            # Release the lock, but only if we still own it
            def release_lock(client):
                current_token = client.get(lock_name)
                if current_token and current_token.decode('utf-8') == token:
                    client.delete(lock_name)
                    return True
                return False
                
            safe_redis_operation("release_lock")(release_lock)
            
    except Exception as e:
        _rate_limited_redis_log("redis_lock_error", "warn", 
                                message="Redis lock operation failed",
                                extra={"lock_name": lock_name, "error": str(e)})
        yield False
        return

def save_metrics_snapshot(snapshot):
    """Persist metrics snapshot safely (idempotent)."""
    if not _has_redis():
        _rate_limited_redis_log("redis_unavailable_snapshot_save", "warn", 
                                message="Redis not configured; snapshot not saved.")
        return False
        
    key = f"prometheus:registry_snapshot:{get_service_name()}"
    
    try:
        safe_redis_operation, _, _, _, _, _, _, _, _ = _get_redis_utils()
        # Check if snapshot is unchanged to avoid redundant writes
        existing = safe_redis_operation("check_existing_snapshot")(
            lambda client: client.get(key)
        )
        
        if existing:
            existing_data = json.loads(existing.decode('utf-8') if isinstance(existing, bytes) else existing)
            if existing_data == snapshot:
                _log_metric_event("snapshot_unchanged", "info", "Snapshot unchanged; skipping write.")
                return True
        
        # Save the new snapshot
        success = safe_redis_operation("save_metrics_snapshot")(
            lambda client: client.set(key, json.dumps(snapshot), ex=REDIS_METRIC_TTL)
        )
        
        if success:
            _log_metric_event("snapshot_saved", "info", f"Metrics snapshot updated for {get_service_name()}")
        else:
            _rate_limited_redis_log("snapshot_save_failed", "warn",
                                    message="Failed to save metrics snapshot (circuit breaker)")
        
        return bool(success)
        
    except Exception as e:
        _rate_limited_redis_log("snapshot_save_error", "error", 
                                message="Error saving metrics snapshot",
                                extra={"error": str(e), "stack": traceback.format_exc()})
        return False

def push_snapshot_from_collector():
    """
    Phase 11-F — Unified snapshot push handler.
    Used by external modules to trigger metrics persistence manually.
    """
    # 🧩 Skip self-metrics to avoid recursion
    # This function itself doesn't record metrics, but we add protection for completeness
    
    try:
        # Use Redis lock to prevent concurrent snapshot operations
        with redis_lock(f"metrics_lock:{get_service_name()}") as locked:
            if not locked:
                _log_metric_event("snapshot_skipped", "warn", 
                                 "Metrics collector lock already held; skipping snapshot push.")
                return {"status": "skipped"}
            
            # FIXED: Use get_snapshot() instead of non-existent generate_metrics_snapshot()
            snapshot = get_snapshot()
            success = save_metrics_snapshot(snapshot)
            
            if success:
                _log_metric_event("snapshot_pushed", "info", "Snapshot pushed successfully from collector")
                return {"status": "success", "snapshot": snapshot}
            else:
                _log_metric_event("snapshot_push_failed", "error", "Failed to push snapshot from collector")
                return {"status": "error", "error": "Failed to save snapshot"}
                
    except Exception as e:
        _log_metric_event("snapshot_push_error", "error",
                         f"Failed to push snapshot from collector: {e}",
                         extra={"stack": traceback.format_exc()})
        return {"status": "error", "error": str(e)}

# --------------------------------------------------------------------------
# Phase 11-F Initialization
# --------------------------------------------------------------------------
def initialize_metrics_collector():
    """Initialize the metrics collector with Phase 11-F features."""
    metrics_config = get_metrics_config()
    
    _log_metric_event(
        "initializing",
        "info",
        "Initializing metrics collector",
        sync_enabled=metrics_config["enable_sync"],
        persistence_enabled=metrics_config["enable_persistence"],
        endpoint_enabled=metrics_config["enable_endpoint"]
    )
    
    # Start sync loop if enabled
    if metrics_config["enable_sync"]:
        _start_sync_loop()
    
    _log_metric_event("initialized", "info", "Metrics collector initialized successfully")

# --------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------
def reset_collector() -> None:
    with _lock:
        _counters.clear()
        _latency_buckets.clear()
    _log_metric_event("collector_reset", "info", "Metrics collector reset")

def get_health_status() -> Dict[str, Any]:
    """Get health status for monitoring."""
    return {
        "uptime_seconds": time.time() - _health_metrics["metrics_collector_start_time"],
        "sync_success_total": _health_metrics["metrics_sync_success_total"],
        "sync_failure_total": _health_metrics["metrics_sync_failure_total"],
        "persistence_success_total": _health_metrics["metrics_persistence_success_total"],
        "persistence_failure_total": _health_metrics["metrics_persistence_failure_total"],
        "last_successful_sync": _health_metrics["last_successful_sync"],
        "last_successful_persistence": _health_metrics["last_successful_persistence"],
        "sync_loop_running": _sync_running,
        "circuit_breaker_state": _get_redis_utils()[-2]()["state"],  # get_circuit_breaker_status
        "service_name": get_service_name(),
    }

# Initialize on module import - moved to prevent circular imports
# initialize_metrics_collector() is intentionally NOT called at import-time to avoid import cycles.
# The application entrypoint (app.py) should call initialize_metrics_collector() after imports are complete.