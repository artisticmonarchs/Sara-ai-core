"""
global_metrics_store.py — Phase 11-D
----------------------------------------
Background synchronization service that keeps a persisted view of metrics
in Redis and helps restore across restarts.

Responsibilities:
 - On startup: attempt to restore the last saved snapshot from Redis into metrics_collector.
 - Periodically (default 30s): call metrics_collector.get_snapshot() and generate_latest(REGISTRY)
   and persist the combined snapshot to Redis using core.metrics_registry.save_metrics_snapshot
   and/or core.metrics_registry.push_snapshot_from_collector.
 - Expose start_background_sync(interval_sec=30) and stop_background_sync().
 - Robust logging via logging_utils.log_event and best-effort behavior if Redis is absent.
"""

from __future__ import annotations
import threading
import time
import json
import traceback
import copy  # ← ADD: For deep copy in get_global_snapshot()
from typing import Optional, Dict, Any, Callable
from prometheus_client import Gauge, Counter

from logging_utils import log_event, get_trace_id

# ------------------------------------------------------------------
# Phase 11-D: Configuration Management - FIXED
# ------------------------------------------------------------------
try:
    from config import get_metrics_config, Config
    METRICS_CONFIG = get_metrics_config()
    SERVICE_NAME = getattr(Config, "SERVICE_NAME", "unknown_service")
except ImportError:
    # Fallback configuration without direct os.environ access
    METRICS_CONFIG = {
        "snapshot_interval": 30,  # Fixed default, no os.environ access
        "circuit_breaker_enabled": True
    }
    SERVICE_NAME = "unknown_service"

SNAPSHOT_INTERVAL = METRICS_CONFIG.get("snapshot_interval", 30)

# ------------------------------------------------------------------
# Phase 11-D: Unified Global Snapshot with Thread Safety
# ------------------------------------------------------------------
GLOBAL_METRICS_SNAPSHOT = {
    "counters": {},
    "latencies": {},
    "gauges": {},
    "last_sync": None,
    "service_name": SERVICE_NAME
}

_snapshot_lock = threading.Lock()

# ------------------------------------------------------------------
# Phase 11-D: Self-Monitoring Metrics
# ------------------------------------------------------------------
try:
    from metrics_registry import REGISTRY, register_metric
except Exception:
    REGISTRY = None
    def register_metric(metric):  # type: ignore
        return False

# Register self-monitoring metrics
if REGISTRY:
    _global_metrics_last_sync_timestamp = Gauge(
        'global_metrics_last_sync_timestamp',
        'Timestamp of the last successful global metrics sync',
        registry=REGISTRY
    )
    
    _global_metrics_total_merges = Counter(
        'global_metrics_total_merges',
        'Total number of metric merge operations',
        registry=REGISTRY
    )
    
    _global_metrics_merge_failures_total = Counter(
        'global_metrics_merge_failures_total',
        'Total number of metric merge failures',
        registry=REGISTRY
    )
    
    _global_metrics_redis_sync_failures_total = Counter(
        'global_metrics_redis_sync_failures_total',
        'Total number of Redis sync failures',
        registry=REGISTRY
    )

# ------------------------------------------------------------------
# Phase 11-D: Safe Metric Operations (No-op if metrics not registered)
# ------------------------------------------------------------------
def _safe_inc(metric):
    """Safely increment a metric (no-op if metric doesn't exist)."""
    try:
        if metric is not None:
            metric.inc()
    except Exception:
        # Never bubble metric operations
        pass

def _safe_set_time(gauge):
    """Safely set gauge to current time (no-op if gauge doesn't exist)."""
    try:
        if gauge is not None:
            gauge.set_to_current_time()
    except Exception:
        # Never bubble metric operations
        pass

# Local imports (import-time safe)
try:
    from metrics_registry import (
        save_metrics_snapshot,
        load_metrics_snapshot,
        push_snapshot_from_collector,
        restore_snapshot_to_collector,
    )
except Exception:
    # If metrics_registry missing, we still want file to import without hard failure
    # Re-raise later if user actually calls start; but for safety assign placeholders
    def save_metrics_snapshot(x):  # type: ignore
        raise RuntimeError("metrics_registry unavailable")

    def load_metrics_snapshot():  # type: ignore
        return {}

    def push_snapshot_from_collector(f):  # type: ignore
        raise RuntimeError("metrics_registry unavailable")

    def restore_snapshot_to_collector(m):  # type: ignore
        raise RuntimeError("metrics_registry unavailable")

try:
    import metrics_collector as metrics_collector  # type: ignore
    from metrics_collector import get_snapshot  # type: ignore
except Exception:
    metrics_collector = None
    def get_snapshot():  # type: ignore
        return {}

try:
    from prometheus_client import generate_latest
except Exception:
    def generate_latest(x):  # placeholder
        return b""

# ------------------------------------------------------------------
# Phase 11-D: Redis Circuit Breaker Support - FIXED
# ------------------------------------------------------------------
def _is_redis_circuit_breaker_open() -> bool:
    """Check if Redis circuit breaker is open."""
    if not METRICS_CONFIG.get("circuit_breaker_enabled", True):
        return False
        
    try:
        # CORRECTED: Use get_client() instead of get_redis_client()
        from redis_client import get_client
        client = get_client()
        if not client:
            return False
            
        key = "circuit_breaker:redis:state"
        state = client.get(key)
        return state == b"open"
    except Exception:
        return False  # Proceed on circuit breaker check errors

# Thread control
_sync_thread: Optional[threading.Thread] = None
_stop_event: Optional[threading.Event] = None

# ------------------------------------------------------------------
# Phase 11-D: Snapshot Management with Thread Safety
# ------------------------------------------------------------------
def get_global_snapshot() -> dict:
    """Return the current in-memory global snapshot."""
    with _snapshot_lock:
        return copy.deepcopy(GLOBAL_METRICS_SNAPSHOT)  # ← FIX: Use deepcopy instead of shallow copy

def _update_global_snapshot(new_snapshot: Dict[str, Any]) -> None:
    """Update the global snapshot with thread safety."""
    with _snapshot_lock:
        try:
            _safe_inc(globals().get('_global_metrics_total_merges'))  # ← FIX: Safe metric increment
            
            # Merge counters (sum values)
            if "counters" in new_snapshot:
                for key, value in new_snapshot["counters"].items():
                    if key in GLOBAL_METRICS_SNAPSHOT["counters"]:
                        GLOBAL_METRICS_SNAPSHOT["counters"][key] += value
                    else:
                        GLOBAL_METRICS_SNAPSHOT["counters"][key] = value
            
            # Update gauges (replace values)
            if "gauges" in new_snapshot:
                GLOBAL_METRICS_SNAPSHOT["gauges"].update(new_snapshot["gauges"])
            
            # Merge latency stats (weighted average if applicable)
            if "latencies" in new_snapshot:
                for key, new_stats in new_snapshot["latencies"].items():
                    if key in GLOBAL_METRICS_SNAPSHOT["latencies"]:
                        # Simple merge - in production you might want weighted average
                        old_stats = GLOBAL_METRICS_SNAPSHOT["latencies"][key]
                        # Add boundary checks for min/max
                        old_min = old_stats.get("min", float('inf'))
                        new_min = new_stats.get("min", float('inf'))
                        old_max = old_stats.get("max", 0)
                        new_max = new_stats.get("max", 0)
                        
                        # Handle infinity cases
                        if old_min == float('inf'):
                            old_min = 0
                        if new_min == float('inf'):
                            new_min = 0
                        
                        merged_stats = {
                            "avg": (old_stats.get("avg", 0) + new_stats.get("avg", 0)) / 2,
                            "count": old_stats.get("count", 0) + new_stats.get("count", 0),
                            "min": min(old_min, new_min),
                            "max": max(old_max, new_max)
                        }
                        GLOBAL_METRICS_SNAPSHOT["latencies"][key] = merged_stats
                    else:
                        GLOBAL_METRICS_SNAPSHOT["latencies"][key] = new_stats
            
            GLOBAL_METRICS_SNAPSHOT["last_sync"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            
        except Exception as e:
            _safe_inc(globals().get('_global_metrics_merge_failures_total'))  # ← FIX: Safe metric increment
            log_event(
                service="global_metrics_store",
                event="snapshot_merge_failed",
                status="error",
                message="Failed to merge metrics snapshot",
                extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()}
            )

def _build_combined_snapshot() -> Dict[str, Any]:
    """
    Compose a combined snapshot object using:
      - metrics_collector.get_snapshot() (structured counters/latencies)
      - textual registry export (generate_latest(REGISTRY).decode())
    Returns a dict ready for JSON serialization.
    """
    try:
        coll_snap = {}
        try:
            coll_snap = get_snapshot()
            # Update global snapshot with new data
            _update_global_snapshot(coll_snap)
        except Exception as e:
            log_event(
                service="global_metrics_store",
                event="get_snapshot_failed",
                status="warn",
                message="metrics_collector.get_snapshot() failed (best-effort)",
                extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()},
            )

        registry_text = None
        try:
            if REGISTRY is not None:
                registry_text = generate_latest(REGISTRY).decode("utf-8")
        except Exception as e:
            log_event(
                service="global_metrics_store",
                event="generate_latest_failed",
                status="warn",
                message="generate_latest(REGISTRY) failed",
                extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()},
            )

        payload = {
            "schema": "v1",  # ← FIX: Add schema version for future-proofing
            "schema_version": "phase_11d_v1",  # Add explicit schema version
            "service": SERVICE_NAME,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "collector_snapshot": coll_snap,
            "registry_text": registry_text,
            "global_snapshot": get_global_snapshot()
        }
        return payload
    except Exception as e:
        log_event(
            service="global_metrics_store",
            event="build_snapshot_error",
            status="error",
            message="Failed to build combined snapshot",
            extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()},
        )
        return {
            "schema": "v1",
            "schema_version": "phase_11d_v1",
            "collector_snapshot": {}, 
            "registry_text": None, 
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }


def _sync_once() -> bool:
    """
    Perform a single sync operation:
      - build combined snapshot
      - push snapshot using push_snapshot_from_collector (if available)
      - save raw combined snapshot via save_metrics_snapshot
    Returns True on success (any best-effort portion), False otherwise.
    """
    # Phase 11-D: Circuit breaker check
    if _is_redis_circuit_breaker_open():
        log_event(
            service="global_metrics_store",
            event="redis_circuit_breaker_open",
            status="warn",
            message="Skipped Redis operation due to circuit breaker open",
            extra={"trace_id": get_trace_id()}
        )
        return False

    try:
        snap = _build_combined_snapshot()

        # Try to push via convenience helper (this will call metrics_collector.get_snapshot internally).
        pushed = False
        try:
            # Use push_snapshot_from_collector if available (it expects metrics_collector.get_snapshot)
            if callable(globals().get('push_snapshot_from_collector')):  # ← FIX: Guard helper call
                push_snapshot_from_collector(get_snapshot)
                pushed = True
                log_event(service="global_metrics_store", event="pushed_via_helper", status="info",
                          message="Pushed snapshot via push_snapshot_from_collector",
                          extra={"trace_id": get_trace_id()})
            else:
                log_event(service="global_metrics_store", event="push_helper_missing", status="debug",
                          message="push_snapshot_from_collector helper not available; skipping",
                          extra={"trace_id": get_trace_id()})
        except Exception as e:
            _safe_inc(globals().get('_global_metrics_redis_sync_failures_total'))  # ← FIX: Safe metric increment
            log_event(service="global_metrics_store", event="push_helper_failed", status="warn",
                      message="push_snapshot_from_collector failed (best-effort)",
                      extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()})

        # Always attempt to save the combined payload to a Redis key via save_metrics_snapshot
        saved = False
        try:
            save_metrics_snapshot(snap)
            saved = True
            _safe_set_time(globals().get('_global_metrics_last_sync_timestamp'))  # ← FIX: Safe metric update
            log_event(service="global_metrics_store", event="saved_snapshot", status="info",
                      message="Saved combined snapshot via save_metrics_snapshot",
                      extra={"trace_id": get_trace_id()})
        except Exception as e:
            _safe_inc(globals().get('_global_metrics_redis_sync_failures_total'))  # ← FIX: Safe metric increment
            log_event(service="global_metrics_store", event="save_snapshot_failed", status="warn",
                      message="save_metrics_snapshot failed (best-effort)",
                      extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()})

        return pushed or saved
    except Exception as e:
        _safe_inc(globals().get('_global_metrics_redis_sync_failures_total'))  # ← FIX: Safe metric increment
        log_event(service="global_metrics_store", event="sync_failed", status="error",
                  message="Unhandled error during _sync_once", 
                  extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()})
        return False


def _restore_once() -> bool:
    """
    Attempt a best-effort restore from Redis:
      - load a raw snapshot via load_metrics_snapshot()
      - if snapshot present, call restore_snapshot_to_collector(metrics_collector)
    """
    # Phase 11-D: Circuit breaker check
    if _is_redis_circuit_breaker_open():
        log_event(
            service="global_metrics_store",
            event="redis_circuit_breaker_open_restore",
            status="warn",
            message="Skipped Redis restore due to circuit breaker open",
            extra={"trace_id": get_trace_id()}
        )
        return False

    try:
        try:
            snap = load_metrics_snapshot() or {}
        except Exception as e:
            log_event(service="global_metrics_store", event="load_snapshot_failed", status="warn",
                      message="load_metrics_snapshot failed",
                      extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()})
            snap = {}

        restored = False
        if snap:
            try:
                # Primary restore path: let metrics_registry.restore_snapshot_to_collector do the work
                restore_snapshot_to_collector(metrics_collector)
                restored = True
                log_event(service="global_metrics_store", event="restored_snapshot", status="info",
                          message="Restored snapshot via restore_snapshot_to_collector",
                          extra={"trace_id": get_trace_id()})
            except Exception as e:
                log_event(service="global_metrics_store", event="restore_failed", status="warn",
                          message="restore_snapshot_to_collector failed (best-effort)",
                          extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()})
        else:
            log_event(service="global_metrics_store", event="no_snapshot_found", status="info",
                      message="No snapshot found at startup",
                      extra={"trace_id": get_trace_id()})
        return restored
    except Exception as e:
        log_event(service="global_metrics_store", event="restore_unhandled_error", status="error",
                  message="Unhandled error during _restore_once", 
                  extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()})
        return False


def _sync_loop(interval_sec: int) -> None:
    """
    Background thread target — runs until stop_event is set.
    Performs restore on first iteration, then periodically syncs.
    """
    log_event(
        service="global_metrics_store",
        event="sync_thread_start",
        status="info",
        message="Global metrics sync thread starting",
        extra={"interval_sec": interval_sec, "service_name": SERVICE_NAME, "trace_id": get_trace_id()}
    )
    
    # First restore attempt
    try:
        _restore_once()
    except Exception:
        pass

    while _stop_event is not None and not _stop_event.is_set():  # ← FIX: Defensive _stop_event check
        # Log cycle start for monitoring
        log_event(
            service="global_metrics_store",
            event="sync_cycle_start",
            status="debug", 
            message="Sync cycle starting",
            extra={"trace_id": get_trace_id()}
        )
        
        try:
            ok = _sync_once()
            if not ok:
                # log at info as we expect occasional failures (Redis unavailable)
                log_event(service="global_metrics_store", event="sync_cycle_noop", status="info",
                          message="Sync cycle completed with no-op (best-effort)",
                          extra={"trace_id": get_trace_id()})
        except Exception as e:
            log_event(service="global_metrics_store", event="sync_cycle_error", status="warn",
                      message="Exception during sync cycle", 
                      extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()})
        # wait but allow timely shutdown
        if _stop_event:  # ← FIX: Additional defensive check
            _stop_event.wait(interval_sec)

    log_event(service="global_metrics_store", event="sync_thread_stop", status="info",
              message="Global metrics sync thread stopping",
              extra={"trace_id": get_trace_id()})


def start_background_sync(service_name: str = None, interval_sec: int = None) -> None:
    """
    Start the background sync thread (idempotent).
    Should be called once at service startup (app.py, streaming_server.py, tasks.py).
    """
    global _sync_thread, _stop_event, SERVICE_NAME  # ← FIX: Include SERVICE_NAME in global declaration
    
    # Reset SERVICE_NAME if provided - FIXED: No direct os.environ access
    if service_name:
        SERVICE_NAME = service_name
    # Otherwise keep the SERVICE_NAME from config initialization
        
    if _sync_thread and _sync_thread.is_alive():
        log_event(service="global_metrics_store", event="sync_already_running", status="info",
                  message="Background metrics sync already running",
                  extra={"trace_id": get_trace_id()})
        return

    # Enforce minimum interval
    sync_interval = max(5, interval_sec or SNAPSHOT_INTERVAL)
    _stop_event = threading.Event()
    _sync_thread = threading.Thread(
        target=_sync_loop, 
        args=(sync_interval,), 
        daemon=True, 
        name=f"global-metrics-sync-{SERVICE_NAME}"
    )
    _sync_thread.start()
    
    log_event(
        service="global_metrics_store",
        event="background_sync_started",
        status="info",
        message="Global metrics sync started",
        extra={"interval": sync_interval, "service_name": SERVICE_NAME, "trace_id": get_trace_id()}
    )


def stop_background_sync(timeout_sec: Optional[int] = 5) -> None:
    """Stop the background sync thread gracefully (best-effort)."""
    global _sync_thread, _stop_event
    if not _sync_thread:
        return
    try:
        if _stop_event:
            _stop_event.set()
        # Only join if thread is alive
        if _sync_thread.is_alive():
            _sync_thread.join(timeout=timeout_sec)
    except Exception as e:
        log_event(service="global_metrics_store", event="stop_sync_failed", status="warn",
                  message="Failed to stop sync thread cleanly", 
                  extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()})
    finally:
        _sync_thread = None
        _stop_event = None

# ------------------------------------------------------------------
# Phase 11-D: Export Functionality
# ------------------------------------------------------------------
def export_prometheus_snapshot() -> str:
    """Return Prometheus text exposition for the global registry."""
    try:
        from metrics_registry import export_prometheus
        result = export_prometheus()
        return result if isinstance(result, str) else result.decode('utf-8', errors='replace')
    except Exception as e:
        log_event(
            service="global_metrics_store",
            event="prometheus_export_failed",
            status="error",
            message="Failed to export Prometheus snapshot",
            extra={"error": str(e), "trace_id": get_trace_id()}
        )
        return "# metrics_export_error 1\n"