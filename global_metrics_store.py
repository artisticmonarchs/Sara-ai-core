"""
core/global_metrics_store.py — Phase 11-D
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
from typing import Optional, Dict, Any, Callable

from logging_utils import log_event

# Local imports (import-time safe)
try:
    from core.metrics_registry import (
        save_metrics_snapshot,
        load_metrics_snapshot,
        push_snapshot_from_collector,
        restore_snapshot_to_collector,
        REGISTRY,
    )
except Exception:
    # If metrics_registry missing, we still want file to import without hard failure
    REGISTRY = None
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
    import core.metrics_collector as metrics_collector  # type: ignore
    from core.metrics_collector import get_snapshot  # type: ignore
except Exception:
    metrics_collector = None
    def get_snapshot():  # type: ignore
        return {}

try:
    from prometheus_client import generate_latest
except Exception:
    def generate_latest(x):  # placeholder
        return b""

# Thread control
_sync_thread: Optional[threading.Thread] = None
_stop_event: Optional[threading.Event] = None

# Default sync interval seconds
DEFAULT_SYNC_INTERVAL = int(float(__import__("os").environ.get("METRICS_SYNC_INTERVAL", 30)))


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
        except Exception as e:
            log_event(
                service="global_metrics_store",
                event="get_snapshot_failed",
                status="warn",
                message="metrics_collector.get_snapshot() failed (best-effort)",
                extra={"error": str(e), "stack": traceback.format_exc()},
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
                extra={"error": str(e), "stack": traceback.format_exc()},
            )

        payload = {
            "service": __import__("os").environ.get("SERVICE_NAME", "unknown_service"),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "collector_snapshot": coll_snap,
            "registry_text": registry_text,
        }
        return payload
    except Exception as e:
        log_event(
            service="global_metrics_store",
            event="build_snapshot_error",
            status="error",
            message="Failed to build combined snapshot",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        return {"collector_snapshot": {}, "registry_text": None, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


def _sync_once() -> bool:
    """
    Perform a single sync operation:
      - build combined snapshot
      - push snapshot using push_snapshot_from_collector (if available)
      - save raw combined snapshot via save_metrics_snapshot
    Returns True on success (any best-effort portion), False otherwise.
    """
    try:
        snap = _build_combined_snapshot()

        # Try to push via convenience helper (this will call metrics_collector.get_snapshot internally).
        pushed = False
        try:
            # Use push_snapshot_from_collector if available (it expects metrics_collector.get_snapshot)
            push_snapshot_from_collector(get_snapshot)
            pushed = True
            log_event(service="global_metrics_store", event="pushed_via_helper", status="info",
                      message="Pushed snapshot via push_snapshot_from_collector")
        except Exception as e:
            log_event(service="global_metrics_store", event="push_helper_failed", status="warn",
                      message="push_snapshot_from_collector failed (best-effort)",
                      extra={"error": str(e), "stack": traceback.format_exc()})

        # Always attempt to save the combined payload to a Redis key via save_metrics_snapshot
        saved = False
        try:
            save_metrics_snapshot(snap)
            saved = True
            log_event(service="global_metrics_store", event="saved_snapshot", status="info",
                      message="Saved combined snapshot via save_metrics_snapshot")
        except Exception as e:
            log_event(service="global_metrics_store", event="save_snapshot_failed", status="warn",
                      message="save_metrics_snapshot failed (best-effort)",
                      extra={"error": str(e), "stack": traceback.format_exc()})

        return pushed or saved
    except Exception as e:
        log_event(service="global_metrics_store", event="sync_failed", status="error",
                  message="Unhandled error during _sync_once", extra={"error": str(e), "stack": traceback.format_exc()})
        return False


def _restore_once() -> bool:
    """
    Attempt a best-effort restore from Redis:
      - load a raw snapshot via load_metrics_snapshot()
      - if snapshot present, call restore_snapshot_to_collector(metrics_collector)
    """
    try:
        try:
            snap = load_metrics_snapshot() or {}
        except Exception as e:
            log_event(service="global_metrics_store", event="load_snapshot_failed", status="warn",
                      message="load_metrics_snapshot failed",
                      extra={"error": str(e), "stack": traceback.format_exc()})
            snap = {}

        restored = False
        if snap:
            try:
                # Primary restore path: let metrics_registry.restore_snapshot_to_collector do the work
                restore_snapshot_to_collector(metrics_collector)
                restored = True
                log_event(service="global_metrics_store", event="restored_snapshot", status="info",
                          message="Restored snapshot via restore_snapshot_to_collector")
            except Exception as e:
                log_event(service="global_metrics_store", event="restore_failed", status="warn",
                          message="restore_snapshot_to_collector failed (best-effort)",
                          extra={"error": str(e), "stack": traceback.format_exc()})
        else:
            log_event(service="global_metrics_store", event="no_snapshot_found", status="info",
                      message="No snapshot found at startup")
        return restored
    except Exception as e:
        log_event(service="global_metrics_store", event="restore_unhandled_error", status="error",
                  message="Unhandled error during _restore_once", extra={"error": str(e), "stack": traceback.format_exc()})
        return False


def _sync_loop(interval_sec: int):
    """
    Background thread target — runs until stop_event is set.
    Performs restore on first iteration, then periodically syncs.
    """
    log_event(service="global_metrics_store", event="sync_thread_start", status="info",
              message="Global metrics sync thread starting", extra={"interval_sec": interval_sec})
    # First restore attempt
    try:
        _restore_once()
    except Exception:
        pass

    while _stop_event is not None and not _stop_event.is_set():
        try:
            ok = _sync_once()
            if not ok:
                # log at info as we expect occasional failures (Redis unavailable)
                log_event(service="global_metrics_store", event="sync_cycle_noop", status="info",
                          message="Sync cycle completed with no-op (best-effort)")
        except Exception as e:
            log_event(service="global_metrics_store", event="sync_cycle_error", status="warn",
                      message="Exception during sync cycle", extra={"error": str(e), "stack": traceback.format_exc()})
        # wait but allow timely shutdown
        _stop_event.wait(interval_sec)

    log_event(service="global_metrics_store", event="sync_thread_stop", status="info",
              message="Global metrics sync thread stopping")


def start_background_sync(interval_sec: int = DEFAULT_SYNC_INTERVAL) -> None:
    """
    Start the background sync thread (idempotent).
    Should be called once at service startup (app.py, streaming_server.py, tasks.py).
    """
    global _sync_thread, _stop_event
    if _sync_thread and _sync_thread.is_alive():
        log_event(service="global_metrics_store", event="sync_already_running", status="info",
                  message="Background metrics sync already running")
        return

    _stop_event = threading.Event()
    _sync_thread = threading.Thread(target=_sync_loop, args=(interval_sec,), daemon=True, name="global-metrics-sync")
    _sync_thread.start()


def stop_background_sync(timeout_sec: Optional[int] = 5) -> None:
    """Stop the background sync thread gracefully (best-effort)."""
    global _sync_thread, _stop_event
    if not _sync_thread:
        return
    try:
        if _stop_event:
            _stop_event.set()
        _sync_thread.join(timeout=timeout_sec)
    except Exception as e:
        log_event(service="global_metrics_store", event="stop_sync_failed", status="warn",
                  message="Failed to stop sync thread cleanly", extra={"error": str(e), "stack": traceback.format_exc()})
    finally:
        _sync_thread = None
        _stop_event = None
