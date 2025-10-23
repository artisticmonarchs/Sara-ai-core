"""
metrics_registry.py — Phase 11-D (Unified Metrics Registry)
------------------------------------------------------------
Purpose:
    Centralized Prometheus REGISTRY for all Sara AI services
    + Redis-backed snapshot persistence.

Responsibilities:
    • Define and export a unified REGISTRY used by all modules.
    • Provide Redis-based snapshot save/load for metrics continuity
      across service restarts.
    • Seamlessly integrate with Phase 11-D `metrics_collector.py`
      without mutating its behavior.
"""

from __future__ import annotations
import os
import json
import time
import threading
import traceback
from typing import Dict, Any, Optional
from prometheus_client import CollectorRegistry, Counter, Gauge
from logging_utils import get_json_logger, log_event, get_trace_id

# ------------------------------------------------------------------
# Phase 11-D: Configuration & Environment Awareness
# ------------------------------------------------------------------
try:
    from config import Config
    SERVICE_NAME = getattr(Config, "SERVICE_NAME", os.getenv("SERVICE_NAME", "sara-ai-core"))
except ImportError:
    SERVICE_NAME = os.getenv("SERVICE_NAME", "sara-ai-core")

METRICS_CONFIG = {
    "snapshot_interval": 30,
    "redis_key_prefix": "prometheus",
    "circuit_breaker_enabled": True
}

SNAPSHOT_INTERVAL = METRICS_CONFIG.get("snapshot_interval", 30)

# ------------------------------------------------------------------
# Phase 11-D: Unified Prometheus Registry
# ------------------------------------------------------------------
REGISTRY = CollectorRegistry(auto_describe=True)
logger = get_json_logger("sara-ai-core-metrics-registry")

# ------------------------------------------------------------------
# Phase 11-D: Registry Self-Monitoring Metrics
# ------------------------------------------------------------------
_registry_snapshot_push_total = Counter(
    'registry_snapshot_push_total',
    'Total number of snapshot push operations',
    registry=REGISTRY
)

_registry_snapshot_failures_total = Counter(
    'registry_snapshot_failures_total',
    'Total number of snapshot push failures',
    registry=REGISTRY
)

_registry_restore_failures_total = Counter(
    'registry_restore_failures_total',
    'Total number of snapshot restore failures',
    registry=REGISTRY
)

_registry_last_snapshot_timestamp = Gauge(
    'registry_last_snapshot_timestamp',
    'Timestamp of the last successful snapshot',
    registry=REGISTRY
)

# ------------------------------------------------------------------
# Redis Setup with Circuit Breaker Awareness
# ------------------------------------------------------------------
try:
    from redis_client import redis_client as _redis_client  # type: ignore
except Exception:
    _redis_client = None

REDIS_METRIC_SNAPSHOT_KEY = f"{METRICS_CONFIG.get('redis_key_prefix', 'prometheus')}:registry_snapshot"

# ------------------------------------------------------------------
# Phase 11-D: Circuit Breaker Support
# ------------------------------------------------------------------
def _is_redis_circuit_breaker_open() -> bool:
    """Check if Redis circuit breaker is open."""
    if not METRICS_CONFIG.get("circuit_breaker_enabled", True):
        return False
        
    try:
        from redis_client import get_redis_client
        client = get_redis_client()
        if not client:
            return False
            
        key = "circuit_breaker:redis:state"
        state = client.get(key)
        return state == b"open"
    except Exception:
        return False  # Proceed on circuit breaker check errors

def _log_rate_limited(event: str, message: str, **extra):
    """Rate-limited logging to prevent log flooding."""
    # Simple rate limiting - in production, you might want more sophisticated logic
    current_time = time.time()
    if not hasattr(_log_rate_limited, 'last_log_times'):
        _log_rate_limited.last_log_times = {}
    
    last_time = _log_rate_limited.last_log_times.get(event, 0)
    if current_time - last_time > 300:  # 5 minutes cooldown
        _log_rate_limited.last_log_times[event] = current_time
        log_event(
            service="metrics_registry",
            event=event,
            status="warn" if "unavailable" in event else "error",
            message=message,
            extra=extra
        )

# ------------------------------------------------------------------
# Snapshot Persistence with Circuit Breaker Awareness
# ------------------------------------------------------------------
def save_metrics_snapshot(snapshot: Dict[str, Any]) -> bool:
    """
    Persist a metrics snapshot to Redis.
    Called periodically or on service shutdown.
    """
    # Phase 11-D: Circuit breaker check
    if _is_redis_circuit_breaker_open():
        _log_rate_limited(
            "redis_circuit_breaker_open_snapshot",
            "Redis circuit breaker open - skipping snapshot save",
            service_name=SERVICE_NAME
        )
        return False

    if not _redis_client:
        _log_rate_limited(
            "redis_unavailable_for_snapshot_save",
            "Redis client not configured; snapshot not saved.",
            service_name=SERVICE_NAME
        )
        return False

    try:
        key = f"{REDIS_METRIC_SNAPSHOT_KEY}:{SERVICE_NAME}"
        payload = json.dumps(snapshot)
        _redis_client.set(key, payload)
        
        # Update self-monitoring metrics
        _registry_snapshot_push_total.inc()
        _registry_last_snapshot_timestamp.set_to_current_time()
        
        log_event(
            service="metrics_registry",
            event="snapshot_saved",
            status="info",
            message=f"Metrics snapshot saved to Redis under {key}",
            extra={"size_bytes": len(payload), "service_name": SERVICE_NAME},
        )
        return True
    except Exception as e:
        _registry_snapshot_failures_total.inc()
        log_event(
            service="metrics_registry",
            event="snapshot_save_failed",
            status="error",
            message="Failed to save metrics snapshot to Redis",
            extra={
                "error": str(e), 
                "stack": traceback.format_exc(),
                "service_name": SERVICE_NAME
            },
        )
        return False


def load_metrics_snapshot() -> Dict[str, Any]:
    """
    Retrieve last saved metrics snapshot from Redis.
    Returns empty dict if unavailable or error occurs.
    """
    # Phase 11-D: Circuit breaker check
    if _is_redis_circuit_breaker_open():
        _log_rate_limited(
            "redis_circuit_breaker_open_restore",
            "Redis circuit breaker open - skipping snapshot load",
            service_name=SERVICE_NAME
        )
        return {}

    if not _redis_client:
        _log_rate_limited(
            "redis_unavailable_for_snapshot_load",
            "Redis client not configured; cannot load snapshot.",
            service_name=SERVICE_NAME
        )
        return {}

    try:
        key = f"{REDIS_METRIC_SNAPSHOT_KEY}:{SERVICE_NAME}"
        raw = _redis_client.get(key)
        if not raw:
            log_event(
                service="metrics_registry",
                event="snapshot_not_found",
                status="warn",
                message=f"No snapshot found for {key}",
                extra={"service_name": SERVICE_NAME}
            )
            return {}
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        log_event(
            service="metrics_registry",
            event="snapshot_loaded",
            status="info",
            message=f"Metrics snapshot loaded from Redis ({key})",
            extra={
                "keys": list(data.keys()) if isinstance(data, dict) else "n/a",
                "service_name": SERVICE_NAME
            },
        )
        return data
    except Exception as e:
        _registry_restore_failures_total.inc()
        log_event(
            service="metrics_registry",
            event="snapshot_load_failed",
            status="error",
            message="Failed to load metrics snapshot from Redis",
            extra={
                "error": str(e), 
                "stack": traceback.format_exc(),
                "service_name": SERVICE_NAME
            },
        )
        return {}


# ------------------------------------------------------------------
# Integration Utilities
# ------------------------------------------------------------------
def push_snapshot_from_collector(get_snapshot_func) -> bool:
    """
    Utility for app/services to push metrics_collector snapshot to Redis.
    get_snapshot_func should be metrics_collector.get_snapshot.
    """
    try:
        snap = get_snapshot_func()
        if not isinstance(snap, dict):
            raise ValueError("Invalid snapshot type returned by get_snapshot_func()")

        snap["persisted_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        snap["service_name"] = SERVICE_NAME
        ok = save_metrics_snapshot(snap)
        if ok:
            log_event(
                service="metrics_registry",
                event="push_snapshot_success",
                status="info",
                message="Snapshot successfully pushed from collector to Redis.",
                extra={
                    "snapshot_keys": list(snap.keys()),
                    "service_name": SERVICE_NAME
                },
            )
        return ok
    except Exception as e:
        _registry_snapshot_failures_total.inc()
        log_event(
            service="metrics_registry",
            event="push_snapshot_failed",
            status="error",
            message="Failed to push snapshot from collector",
            extra={
                "error": str(e), 
                "stack": traceback.format_exc(),
                "service_name": SERVICE_NAME
            },
        )
        return False


def restore_snapshot_to_collector(collector_module) -> bool:
    """
    Load persisted snapshot from Redis and restore to a given collector module.
    collector_module must expose set_metric(name, value) and observe_latency(name, value_ms).
    """
    try:
        snap = load_metrics_snapshot()
        if not snap:
            log_event(
                service="metrics_registry",
                event="snapshot_restore_skipped",
                status="warn",
                message="No metrics snapshot found in Redis; nothing to restore.",
                extra={"service_name": SERVICE_NAME}
            )
            return False

        counters = snap.get("counters", {})
        latencies = snap.get("latencies", {})

        # Restore counters
        restored_counters = 0
        for name, val in counters.items():
            try:
                collector_module.set_metric(name, int(val))
                restored_counters += 1
            except Exception:
                continue

        # Restore latency averages (simple approximation)
        restored_latencies = 0
        for name, stats in latencies.items():
            try:
                avg = float(stats.get("avg", 0.0))
                if avg > 0:
                    collector_module.observe_latency(name, avg)
                    restored_latencies += 1
            except Exception:
                continue

        log_event(
            service="metrics_registry",
            event="snapshot_restored",
            status="info",
            message=f"Restored {restored_counters} counters, {restored_latencies} latency metrics.",
            extra={
                "counters": restored_counters, 
                "latencies": restored_latencies,
                "service_name": SERVICE_NAME
            },
        )
        return True
    except Exception as e:
        _registry_restore_failures_total.inc()
        log_event(
            service="metrics_registry",
            event="restore_snapshot_failed",
            status="error",
            message="Failed to restore metrics snapshot to collector",
            extra={
                "error": str(e), 
                "stack": traceback.format_exc(),
                "service_name": SERVICE_NAME
            },
        )
        return False

# ------------------------------------------------------------------
# Phase 11-D: Background Sync Thread
# ------------------------------------------------------------------
_sync_thread: Optional[threading.Thread] = None
_sync_running = False

def _background_sync_worker(collector_module, interval: int = 30):
    """Background worker for periodic metrics sync."""
    while _sync_running:
        try:
            time.sleep(interval)
            if collector_module and hasattr(collector_module, 'get_snapshot'):
                push_snapshot_from_collector(collector_module.get_snapshot)
        except Exception as e:
            log_event(
                service="metrics_registry",
                event="background_sync_error",
                status="error",
                message="Background sync worker error",
                extra={"error": str(e), "service_name": SERVICE_NAME}
            )

def start_background_sync(collector_module, interval: int = None):
    """
    Start background metrics sync thread.
    """
    global _sync_thread, _sync_running
    
    if _sync_running:
        log_event(
            service="metrics_registry",
            event="background_sync_already_running",
            status="warn",
            message="Background sync already running",
            extra={"service_name": SERVICE_NAME}
        )
        return
    
    sync_interval = interval or SNAPSHOT_INTERVAL
    _sync_running = True
    _sync_thread = threading.Thread(
        target=_background_sync_worker,
        args=(collector_module, sync_interval),
        daemon=True,
        name="metrics-background-sync"
    )
    _sync_thread.start()
    
    log_event(
        service="metrics_registry",
        event="background_sync_started",
        status="info",
        message="Background metrics sync started",
        extra={"interval": sync_interval, "service_name": SERVICE_NAME}
    )

def stop_background_sync():
    """Stop background metrics sync thread."""
    global _sync_running
    _sync_running = False
    
    log_event(
        service="metrics_registry",
        event="background_sync_stopped",
        status="info",
        message="Background metrics sync stopped",
        extra={"service_name": SERVICE_NAME}
    )

# ------------------------------------------------------------------
# Phase 11-D: Metric Registration Helper
# ------------------------------------------------------------------
def register_metric(metric):
    """
    Safely register a metric with the unified registry.
    """
    try:
        REGISTRY.register(metric)
        return True
    except Exception as e:
        log_event(
            service="metrics_registry",
            event="metric_registration_failed",
            status="error",
            message="Failed to register metric with registry",
            extra={"error": str(e), "service_name": SERVICE_NAME}
        )
        return False

# ------------------------------------------------------------------
# Phase 11-D: Temporary Collector for Global Exposition
# ------------------------------------------------------------------
def export_prometheus() -> str:
    """
    Export merged metrics from registry in Prometheus format.
    Creates a temporary registry to avoid double exposition.
    """
    try:
        from prometheus_client import generate_latest
        return generate_latest(REGISTRY).decode('utf-8')
    except Exception as e:
        log_event(
            service="metrics_registry",
            event="prometheus_export_failed",
            status="error",
            message="Failed to export Prometheus metrics",
            extra={"error": str(e), "service_name": SERVICE_NAME}
        )
        return "# metrics_export_error 1\n"