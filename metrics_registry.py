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
    • Seamlessly integrate with Phase 11-B `metrics_collector.py`
      without mutating its behavior.
"""

from __future__ import annotations
import os
import json
import time
from typing import Dict, Any
from prometheus_client import CollectorRegistry
from logging_utils import log_event

# ------------------------------------------------------------------
# Phase 11-D: Unified Prometheus Registry
# ------------------------------------------------------------------
REGISTRY = CollectorRegistry(auto_describe=True)

# ------------------------------------------------------------------
# Redis Setup
# ------------------------------------------------------------------
try:
    from redis_client import redis_client as _redis_client  # type: ignore
except Exception:
    _redis_client = None

REDIS_METRIC_SNAPSHOT_KEY = "prometheus:registry_snapshot"
SERVICE_NAME = os.getenv("SERVICE_NAME", "unknown_service")

# ------------------------------------------------------------------
# Snapshot Persistence
# ------------------------------------------------------------------
def save_metrics_snapshot(snapshot: Dict[str, Any]) -> bool:
    """
    Persist a metrics snapshot to Redis.
    Called periodically or on service shutdown.
    """
    if not _redis_client:
        log_event(
            service="metrics_registry",
            event="redis_unavailable_for_snapshot_save",
            status="warn",
            message="Redis client not configured; snapshot not saved.",
        )
        return False

    try:
        key = f"{REDIS_METRIC_SNAPSHOT_KEY}:{SERVICE_NAME}"
        payload = json.dumps(snapshot)
        _redis_client.set(key, payload)
        log_event(
            service="metrics_registry",
            event="snapshot_saved",
            status="info",
            message=f"Metrics snapshot saved to Redis under {key}",
            extra={"size_bytes": len(payload)},
        )
        return True
    except Exception as e:
        log_event(
            service="metrics_registry",
            event="snapshot_save_failed",
            status="error",
            message=str(e),
        )
        return False


def load_metrics_snapshot() -> Dict[str, Any]:
    """
    Retrieve last saved metrics snapshot from Redis.
    Returns empty dict if unavailable or error occurs.
    """
    if not _redis_client:
        log_event(
            service="metrics_registry",
            event="redis_unavailable_for_snapshot_load",
            status="warn",
            message="Redis client not configured; cannot load snapshot.",
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
            extra={"keys": list(data.keys()) if isinstance(data, dict) else "n/a"},
        )
        return data
    except Exception as e:
        log_event(
            service="metrics_registry",
            event="snapshot_load_failed",
            status="error",
            message=str(e),
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
            raise ValueError("Invalid snapshot type")
        snap["persisted_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return save_metrics_snapshot(snap)
    except Exception as e:
        log_event(
            service="metrics_registry",
            event="push_snapshot_failed",
            status="error",
            message=str(e),
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
            return False

        counters = snap.get("counters", {})
        latencies = snap.get("latencies", {})

        # Restore counters
        for name, val in counters.items():
            try:
                collector_module.set_metric(name, int(val))
            except Exception:
                pass

        # Restore latency samples (only summary statistics if raw not present)
        for name, stats in latencies.items():
            try:
                avg = float(stats.get("avg", 0.0))
                if avg > 0:
                    collector_module.observe_latency(name, avg)
            except Exception:
                pass

        log_event(
            service="metrics_registry",
            event="snapshot_restored",
            status="info",
            message=f"Restored {len(counters)} counters, {len(latencies)} latency metrics.",
        )
        return True
    except Exception as e:
        log_event(
            service="metrics_registry",
            event="restore_snapshot_failed",
            status="error",
            message=str(e),
        )
        return False
