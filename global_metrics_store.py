"""
global_metrics_store.py — Phase 11-F Enhanced
----------------------------------------
Enhanced metrics synchronization service with multi-service aggregation,
metric publishing helpers, and robust Prometheus exposition.

New Features:
✅ Aggregate metrics from all services into Redis
✅ Helper publish_metric(name, value, labels) 
✅ Expose /metrics endpoint to Prometheus
✅ Handle per-service TTL + counter resets
✅ Add logging + Sentry for metric push failures

Responsibilities:
 - Multi-service metrics aggregation with service-specific TTL
 - Simplified metric publishing API
 - Prometheus exposition endpoint
 - Counter reset detection and handling
 - Enhanced error tracking with Sentry integration
"""

from __future__ import annotations
import threading
import time
import json
import traceback
import copy
from typing import Optional, Dict, Any, Callable, List, Union
from datetime import datetime, timedelta

# Enhanced imports for new functionality
try:
    import sentry_sdk
    from sentry_sdk import capture_exception, capture_message
    SENTRY_AVAILABLE = True
except ImportError:
    SENTRY_AVAILABLE = False
    def capture_exception(*args, **kwargs): pass
    def capture_message(*args, **kwargs): pass

# ────────────────────────────────────────────────────────────────
# Phase 11-F: Enhanced Metrics Registry with Service Support
# ────────────────────────────────────────────────────────────────

try:
    from prometheus_client import REGISTRY as registry, Counter, Gauge, Histogram, generate_latest
except ImportError:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
    registry = CollectorRegistry()

# Avoid duplicate registration of metrics
def safe_counter(name: str, description: str, labels: List[str] = None):
    """Safely register a Counter metric or reuse existing one."""
    existing = [m for m in registry._names_to_collectors.keys() if m == name]
    if existing:
        return registry._names_to_collectors[name]
    return Counter(name, description, labelnames=labels or [], registry=registry)

def safe_gauge(name: str, description: str, labels: List[str] = None):
    existing = [m for m in registry._names_to_collectors.keys() if m == name]
    if existing:
        return registry._names_to_collectors[name]
    return Gauge(name, description, labelnames=labels or [], registry=registry)

def safe_histogram(name: str, description: str, labels: List[str] = None):
    existing = [m for m in registry._names_to_collectors.keys() if m == name]
    if existing:
        return registry._names_to_collectors[name]
    return Histogram(name, description, labelnames=labels or [], registry=registry)

# ────────────────────────────────────────────────────────────────
# Phase 11-F: Global Metrics (Enhanced)
# ────────────────────────────────────────────────────────────────

# Clean up any duplicate metrics
for name in list(registry._names_to_collectors.keys()):
    if name.startswith("global_metrics_"):
        try:
            registry.unregister(registry._names_to_collectors[name])
        except Exception:
            pass

# Enhanced global metrics with service labels
_global_metrics_total_merges = safe_counter(
    "global_metrics_total_merges",
    "Total merges performed across metrics snapshots",
    labels=["service"]
)

_global_metrics_merge_failures_total = safe_counter(
    "global_metrics_merge_failures_total",
    "Total number of metric merge failures",
    labels=["service", "error_type"]
)

_global_metrics_redis_sync_failures_total = safe_counter(
    "global_metrics_redis_sync_failures_total", 
    "Total number of Redis sync failures",
    labels=["service", "operation"]
)

_global_metrics_last_sync_timestamp = safe_gauge(
    "global_metrics_last_sync_timestamp",
    "Last successful metrics sync timestamp (UNIX epoch seconds)",
    labels=["service"]
)

# New metrics for Phase 11-F
_global_metrics_publish_total = safe_counter(
    "global_metrics_publish_total",
    "Total metrics published via publish_metric",
    labels=["service", "metric_type"]
)

_global_metrics_ttl_expirations_total = safe_counter(
    "global_metrics_ttl_expirations_total",
    "Total metrics expired due to TTL",
    labels=["service"]
)

_global_metrics_counter_resets_total = safe_counter(
    "global_metrics_counter_resets_total",
    "Total counter reset events detected",
    labels=["service", "metric_name"]
)

_global_metrics_service_health = safe_gauge(
    "global_metrics_service_health",
    "Service health status (1=healthy, 0=unhealthy)",
    labels=["service", "service_type"]
)

from logging_utils import log_event, get_trace_id

# ------------------------------------------------------------------
# Phase 11-F: Enhanced Configuration Management
# ------------------------------------------------------------------
try:
    from config import get_metrics_config, Config
    METRICS_CONFIG = get_metrics_config()
    SERVICE_NAME = getattr(Config, "SERVICE_NAME", "unknown_service")
except ImportError:
    METRICS_CONFIG = {
        "snapshot_interval": 30,
        "circuit_breaker_enabled": True,
        "service_ttl_seconds": 300,  # 5 minutes default TTL
        "counter_reset_threshold": 0.8,  # 80% decrease triggers reset detection
        "max_metrics_per_service": 1000
    }
    SERVICE_NAME = "unknown_service"

SNAPSHOT_INTERVAL = METRICS_CONFIG.get("snapshot_interval", 30)
SERVICE_TTL_SECONDS = METRICS_CONFIG.get("service_ttl_seconds", 300)
COUNTER_RESET_THRESHOLD = METRICS_CONFIG.get("counter_reset_threshold", 0.8)
MAX_METRICS_PER_SERVICE = METRICS_CONFIG.get("max_metrics_per_service", 1000)

# ------------------------------------------------------------------
# Phase 11-F: Enhanced Global Snapshot with Service TTL Support
# ------------------------------------------------------------------
GLOBAL_METRICS_SNAPSHOT = {
    "counters": {},
    "latencies": {}, 
    "gauges": {},
    "last_sync": None,
    "service_name": SERVICE_NAME,
    "services": {}  # New: Track all services and their last update
}

_snapshot_lock = threading.Lock()

# Service registry for TTL management
_SERVICE_REGISTRY = {}
_SERVICE_REGISTRY_LOCK = threading.Lock()

# ------------------------------------------------------------------
# Phase 11-F: Enhanced Metric Operations
# ------------------------------------------------------------------
def _safe_inc(metric, labels=None):
    """Safely increment a metric with labels."""
    try:
        if metric is not None:
            if labels:
                metric.labels(**labels).inc()
            else:
                metric.inc()
    except Exception:
        pass

def _safe_set(metric, value, labels=None):
    """Safely set a metric value with labels."""
    try:
        if metric is not None:
            if labels:
                metric.labels(**labels).set(value)
            else:
                metric.set(value)
    except Exception:
        pass

def _safe_set_time(gauge, labels=None):
    """Safely set gauge to current time with labels."""
    try:
        if gauge is not None:
            if labels:
                gauge.labels(**labels).set_to_current_time()
            else:
                gauge.set_to_current_time()
    except Exception:
        pass

def _safe_observe(histogram, value, labels=None):
    """Safely observe histogram value with labels."""
    try:
        if histogram is not None:
            if labels:
                histogram.labels(**labels).observe(value)
            else:
                histogram.observe(value)
    except Exception:
        pass

# ------------------------------------------------------------------
# Phase 11-F: Service TTL Management
# ------------------------------------------------------------------
def update_service_heartbeat(service_name: str, service_type: str = "unknown"):
    """Update service heartbeat and health status."""
    with _SERVICE_REGISTRY_LOCK:
        _SERVICE_REGISTRY[service_name] = {
            "last_heartbeat": time.time(),
            "service_type": service_type,
            "status": "healthy"
        }
    
    # Update health metric
    _safe_set(_global_metrics_service_health, 1, {
        "service": service_name,
        "service_type": service_type
    })

def check_service_ttl() -> List[str]:
    """Check for expired services and return list of expired service names."""
    expired_services = []
    current_time = time.time()
    
    with _SERVICE_REGISTRY_LOCK:
        for service_name, service_info in list(_SERVICE_REGISTRY.items()):
            if current_time - service_info["last_heartbeat"] > SERVICE_TTL_SECONDS:
                expired_services.append(service_name)
                # Mark as unhealthy
                _safe_set(_global_metrics_service_health, 0, {
                    "service": service_name,
                    "service_type": service_info["service_type"]
                })
                _safe_inc(_global_metrics_ttl_expirations_total, {
                    "service": service_name
                })
                
                # Log and capture in Sentry
                log_event(
                    service="global_metrics_store",
                    event="service_ttl_expired",
                    status="warning",
                    message=f"Service TTL expired: {service_name}",
                    extra={
                        "service_name": service_name,
                        "service_type": service_info["service_type"],
                        "last_heartbeat": service_info["last_heartbeat"],
                        "ttl_seconds": SERVICE_TTL_SECONDS,
                        "trace_id": get_trace_id()
                    }
                )
                
                if SENTRY_AVAILABLE:
                    capture_message(
                        f"Service TTL expired: {service_name}",
                        level="warning"
                    )
    
    return expired_services

# ------------------------------------------------------------------
# Phase 11-F: Counter Reset Detection
# ------------------------------------------------------------------
def detect_counter_reset(service_name: str, metric_name: str, current_value: float, previous_value: float) -> bool:
    """Detect if a counter has reset (decreased significantly)."""
    if previous_value is None or current_value >= previous_value:
        return False
    
    decrease_ratio = (previous_value - current_value) / previous_value if previous_value > 0 else 1.0
    
    if decrease_ratio > COUNTER_RESET_THRESHOLD:
        _safe_inc(_global_metrics_counter_resets_total, {
            "service": service_name,
            "metric_name": metric_name
        })
        
        log_event(
            service="global_metrics_store",
            event="counter_reset_detected",
            status="info",
            message=f"Counter reset detected: {metric_name}",
            extra={
                "service_name": service_name,
                "metric_name": metric_name,
                "previous_value": previous_value,
                "current_value": current_value,
                "decrease_ratio": decrease_ratio,
                "trace_id": get_trace_id()
            }
        )
        
        return True
    
    return False

# ------------------------------------------------------------------
# Phase 11-F: Enhanced Snapshot Management
# ------------------------------------------------------------------
def get_global_snapshot() -> dict:
    """Return the current in-memory global snapshot with service info."""
    with _snapshot_lock:
        snapshot = copy.deepcopy(GLOBAL_METRICS_SNAPSHOT)
        snapshot["services"] = copy.deepcopy(_SERVICE_REGISTRY)
        return snapshot

def _update_global_snapshot(new_snapshot: Dict[str, Any]) -> None:
    """Update the global snapshot with enhanced service support."""
    with _snapshot_lock:
        try:
            service_name = new_snapshot.get("service_name", "unknown")
            
            # Update service heartbeat
            update_service_heartbeat(service_name, new_snapshot.get("service_type", "unknown"))
            
            _safe_inc(_global_metrics_total_merges, {"service": service_name})
            
            # Initialize service structure if not exists
            if service_name not in GLOBAL_METRICS_SNAPSHOT["counters"]:
                GLOBAL_METRICS_SNAPSHOT["counters"][service_name] = {}
            if service_name not in GLOBAL_METRICS_SNAPSHOT["gauges"]:
                GLOBAL_METRICS_SNAPSHOT["gauges"][service_name] = {}
            if service_name not in GLOBAL_METRICS_SNAPSHOT["latencies"]:
                GLOBAL_METRICS_SNAPSHOT["latencies"][service_name] = {}
            
            # Merge counters with reset detection
            if "counters" in new_snapshot:
                for key, value in new_snapshot["counters"].items():
                    previous_value = GLOBAL_METRICS_SNAPSHOT["counters"][service_name].get(key)
                    if detect_counter_reset(service_name, key, value, previous_value):
                        # Reset detected, use current value
                        GLOBAL_METRICS_SNAPSHOT["counters"][service_name][key] = value
                    else:
                        # Normal merge
                        if key in GLOBAL_METRICS_SNAPSHOT["counters"][service_name]:
                            GLOBAL_METRICS_SNAPSHOT["counters"][service_name][key] += value
                        else:
                            GLOBAL_METRICS_SNAPSHOT["counters"][service_name][key] = value
            
            # Update gauges (replace values)
            if "gauges" in new_snapshot:
                GLOBAL_METRICS_SNAPSHOT["gauges"][service_name].update(new_snapshot["gauges"])
            
            # Merge latency stats
            if "latencies" in new_snapshot:
                for key, new_stats in new_snapshot["latencies"].items():
                    if key in GLOBAL_METRICS_SNAPSHOT["latencies"][service_name]:
                        old_stats = GLOBAL_METRICS_SNAPSHOT["latencies"][service_name][key]
                        old_min = old_stats.get("min", float('inf'))
                        new_min = new_stats.get("min", float('inf'))
                        old_max = old_stats.get("max", 0)
                        new_max = new_stats.get("max", 0)
                        
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
                        GLOBAL_METRICS_SNAPSHOT["latencies"][service_name][key] = merged_stats
                    else:
                        GLOBAL_METRICS_SNAPSHOT["latencies"][service_name][key] = new_stats
            
            GLOBAL_METRICS_SNAPSHOT["last_sync"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            
        except Exception as e:
            error_type = type(e).__name__
            _safe_inc(_global_metrics_merge_failures_total, {
                "service": service_name,
                "error_type": error_type
            })
            
            log_event(
                service="global_metrics_store",
                event="snapshot_merge_failed",
                status="error",
                message="Failed to merge metrics snapshot",
                extra={
                    "error": str(e),
                    "error_type": error_type,
                    "stack": traceback.format_exc(),
                    "trace_id": get_trace_id()
                }
            )
            
            if SENTRY_AVAILABLE:
                capture_exception(e)

# ------------------------------------------------------------------
# Phase 11-F: Enhanced Metric Publishing Helper
# ------------------------------------------------------------------
def publish_metric(name: str, value: float, labels: Dict[str, str] = None, metric_type: str = "gauge") -> bool:
    """
    Publish a metric to the global metrics store.
    
    Args:
        name: Metric name
        value: Metric value
        labels: Dictionary of labels
        metric_type: Type of metric ('counter', 'gauge', 'histogram')
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        service_name = labels.get("service", SERVICE_NAME) if labels else SERVICE_NAME
        labels = labels or {}
        
        # Update service heartbeat
        update_service_heartbeat(service_name, labels.get("service_type", "unknown"))
        
        # Track metric publication
        _safe_inc(_global_metrics_publish_total, {
            "service": service_name,
            "metric_type": metric_type
        })
        
        # Store in appropriate snapshot category
        with _snapshot_lock:
            if metric_type == "counter":
                if service_name not in GLOBAL_METRICS_SNAPSHOT["counters"]:
                    GLOBAL_METRICS_SNAPSHOT["counters"][service_name] = {}
                
                metric_key = f"{name}_{'_'.join(f'{k}_{v}' for k, v in sorted(labels.items()))}"
                previous_value = GLOBAL_METRICS_SNAPSHOT["counters"][service_name].get(metric_key, 0)
                
                if detect_counter_reset(service_name, name, value, previous_value):
                    GLOBAL_METRICS_SNAPSHOT["counters"][service_name][metric_key] = value
                else:
                    GLOBAL_METRICS_SNAPSHOT["counters"][service_name][metric_key] = previous_value + value
                    
            elif metric_type == "gauge":
                if service_name not in GLOBAL_METRICS_SNAPSHOT["gauges"]:
                    GLOBAL_METRICS_SNAPSHOT["gauges"][service_name] = {}
                
                metric_key = f"{name}_{'_'.join(f'{k}_{v}' for k, v in sorted(labels.items()))}"
                GLOBAL_METRICS_SNAPSHOT["gauges"][service_name][metric_key] = value
                
            elif metric_type == "histogram":
                if service_name not in GLOBAL_METRICS_SNAPSHOT["latencies"]:
                    GLOBAL_METRICS_SNAPSHOT["latencies"][service_name] = {}
                
                if name not in GLOBAL_METRICS_SNAPSHOT["latencies"][service_name]:
                    GLOBAL_METRICS_SNAPSHOT["latencies"][service_name][name] = {
                        "avg": value,
                        "count": 1,
                        "min": value,
                        "max": value
                    }
                else:
                    stats = GLOBAL_METRICS_SNAPSHOT["latencies"][service_name][name]
                    stats["avg"] = (stats["avg"] * stats["count"] + value) / (stats["count"] + 1)
                    stats["count"] += 1
                    stats["min"] = min(stats["min"], value)
                    stats["max"] = max(stats["max"], value)
        
        log_event(
            service="global_metrics_store",
            event="metric_published",
            status="debug",
            message=f"Metric published: {name}",
            extra={
                "metric_name": name,
                "value": value,
                "metric_type": metric_type,
                "labels": labels,
                "service_name": service_name,
                "trace_id": get_trace_id()
            }
        )
        
        return True
        
    except Exception as e:
        error_type = type(e).__name__
        log_event(
            service="global_metrics_store",
            event="metric_publish_failed",
            status="error",
            message=f"Failed to publish metric: {name}",
            extra={
                "error": str(e),
                "error_type": error_type,
                "metric_name": name,
                "value": value,
                "metric_type": metric_type,
                "labels": labels,
                "trace_id": get_trace_id()
            }
        )
        
        if SENTRY_AVAILABLE:
            capture_exception(e)
        
        return False

# ------------------------------------------------------------------
# Phase 11-F: Enhanced Combined Snapshot Builder
# ------------------------------------------------------------------
def _build_combined_snapshot() -> Dict[str, Any]:
    """Build enhanced combined snapshot with service aggregation."""
    try:
        # Check for expired services
        expired_services = check_service_ttl()
        
        # Clean up expired services from snapshot
        with _snapshot_lock:
            for service_name in expired_services:
                for category in ["counters", "gauges", "latencies"]:
                    if service_name in GLOBAL_METRICS_SNAPSHOT[category]:
                        del GLOBAL_METRICS_SNAPSHOT[category][service_name]
        
        coll_snap = {}
        try:
            from metrics_collector import get_snapshot
            coll_snap = get_snapshot()
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
            registry_text = generate_latest(registry).decode("utf-8")
        except Exception as e:
            log_event(
                service="global_metrics_store",
                event="generate_latest_failed",
                status="warn",
                message="generate_latest(REGISTRY) failed",
                extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()},
            )

        payload = {
            "schema": "v2",
            "schema_version": "phase_11f_v1",
            "service": SERVICE_NAME,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "collector_snapshot": coll_snap,
            "registry_text": registry_text,
            "global_snapshot": get_global_snapshot(),
            "service_registry": _SERVICE_REGISTRY,
            "expired_services": expired_services
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
        if SENTRY_AVAILABLE:
            capture_exception(e)
        return {
            "schema": "v2",
            "schema_version": "phase_11f_v1",
            "collector_snapshot": {},
            "registry_text": None,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "service_registry": {},
            "expired_services": []
        }

# ------------------------------------------------------------------
# Phase 11-F: Prometheus Metrics Endpoint
# ------------------------------------------------------------------
def get_prometheus_metrics() -> str:
    """
    Generate Prometheus metrics exposition format.
    This can be exposed via a /metrics endpoint.
    """
    try:
        # Generate standard registry metrics
        metrics_output = generate_latest(registry).decode('utf-8')
        
        # Add custom aggregated metrics from global snapshot
        custom_metrics = []
        snapshot = get_global_snapshot()
        
        # Export counters
        for service_name, counters in snapshot.get("counters", {}).items():
            for counter_name, value in counters.items():
                custom_metrics.append(f'sara_custom_{counter_name}{{service="{service_name}"}} {float(value)}')
        
        # Export gauges  
        for service_name, gauges in snapshot.get("gauges", {}).items():
            for gauge_name, value in gauges.items():
                custom_metrics.append(f'sara_custom_{gauge_name}{{service="{service_name}"}} {float(value)}')
        
        if custom_metrics:
            metrics_output += "\n" + "\n".join(custom_metrics)
        
        return metrics_output
        
    except Exception as e:
        log_event(
            service="global_metrics_store",
            event="prometheus_export_failed",
            status="error",
            message="Failed to generate Prometheus metrics",
            extra={"error": str(e), "trace_id": get_trace_id()}
        )
        if SENTRY_AVAILABLE:
            capture_exception(e)
        return "# metrics_export_error 1\n"

# ------------------------------------------------------------------
# Phase 11-F: Existing Redis Integration (Enhanced)
# ------------------------------------------------------------------
def _is_redis_circuit_breaker_open() -> bool:
    """Check if Redis circuit breaker is open."""
    if not METRICS_CONFIG.get("circuit_breaker_enabled", True):
        return False
        
    try:
        from redis_client import get_client
        client = get_client()
        if not client:
            return False
            
        key = "circuit_breaker:redis:state"
        state = client.get(key)
        return state == b"open"
    except Exception as e:
        log_event(
            service="global_metrics_store",
            event="circuit_breaker_check_failed",
            status="warn",
            message="Circuit breaker check failed",
            extra={"error": str(e), "trace_id": get_trace_id()}
        )
        return False

# Local imports (maintain existing functionality)
try:
    from metrics_registry import (
        save_metrics_snapshot,
        load_metrics_snapshot,
        push_snapshot_from_collector,
        restore_snapshot_to_collector,
    )
except Exception:
    def save_metrics_snapshot(x): 
        log_event(
            service="global_metrics_store",
            event="save_metrics_snapshot_unavailable",
            status="error",
            message="metrics_registry.save_metrics_snapshot unavailable"
        )
        return False
    def load_metrics_snapshot(): 
        log_event(
            service="global_metrics_store",
            event="load_metrics_snapshot_unavailable", 
            status="warn",
            message="metrics_registry.load_metrics_snapshot unavailable"
        )
        return {}
    def push_snapshot_from_collector(f): 
        log_event(
            service="global_metrics_store",
            event="push_snapshot_unavailable",
            status="warn",
            message="metrics_registry.push_snapshot_from_collector unavailable"
        )
        return False
    def restore_snapshot_to_collector(m): 
        log_event(
            service="global_metrics_store",
            event="restore_snapshot_unavailable",
            status="warn",
            message="metrics_registry.restore_snapshot_to_collector unavailable"
        )
        return False

try:
    import metrics_collector as metrics_collector
    from metrics_collector import get_snapshot
except Exception:
    metrics_collector = None
    def get_snapshot(): 
        log_event(
            service="global_metrics_store",
            event="get_snapshot_unavailable",
            status="warn",
            message="metrics_collector.get_snapshot unavailable"
        )
        return {}

# ------------------------------------------------------------------
# Phase 11-F: Enhanced Sync Operations
# ------------------------------------------------------------------
def _sync_once() -> bool:
    """Enhanced sync operation with better error handling and Sentry integration."""
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

        # Try to push via convenience helper
        pushed = False
        try:
            if callable(globals().get('push_snapshot_from_collector')):
                push_snapshot_from_collector(get_snapshot)
                pushed = True
                log_event(
                    service="global_metrics_store",
                    event="pushed_via_helper", 
                    status="info",
                    message="Pushed snapshot via push_snapshot_from_collector",
                    extra={"trace_id": get_trace_id()}
                )
        except Exception as e:
            _safe_inc(_global_metrics_redis_sync_failures_total, {
                "service": SERVICE_NAME,
                "operation": "push_helper"
            })
            log_event(
                service="global_metrics_store",
                event="push_helper_failed",
                status="warn", 
                message="push_snapshot_from_collector failed (best-effort)",
                extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()}
            )
            if SENTRY_AVAILABLE:
                capture_exception(e)

        # Save combined payload to Redis
        saved = False
        try:
            save_metrics_snapshot(snap)
            saved = True
            _safe_set_time(_global_metrics_last_sync_timestamp, {"service": SERVICE_NAME})
            log_event(
                service="global_metrics_store", 
                event="saved_snapshot",
                status="info",
                message="Saved combined snapshot via save_metrics_snapshot",
                extra={"trace_id": get_trace_id()}
            )
        except Exception as e:
            _safe_inc(_global_metrics_redis_sync_failures_total, {
                "service": SERVICE_NAME, 
                "operation": "save_snapshot"
            })
            log_event(
                service="global_metrics_store",
                event="save_snapshot_failed",
                status="warn",
                message="save_metrics_snapshot failed (best-effort)",
                extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()}
            )
            if SENTRY_AVAILABLE:
                capture_exception(e)

        return pushed or saved
        
    except Exception as e:
        _safe_inc(_global_metrics_redis_sync_failures_total, {
            "service": SERVICE_NAME,
            "operation": "sync_once"
        })
        log_event(
            service="global_metrics_store",
            event="sync_failed",
            status="error",
            message="Unhandled error during _sync_once", 
            extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()}
        )
        if SENTRY_AVAILABLE:
            capture_exception(e)
        return False

# ------------------------------------------------------------------
# Phase 11-F: Maintain Existing Background Sync Functionality
# ------------------------------------------------------------------
_sync_thread: Optional[threading.Thread] = None
_stop_event: Optional[threading.Event] = None

def _sync_loop(interval_sec: int) -> None:
    """Background thread target with enhanced logging."""
    log_event(
        service="global_metrics_store",
        event="sync_thread_start",
        status="info",
        message="Global metrics sync thread starting",
        extra={"interval_sec": interval_sec, "service_name": SERVICE_NAME, "trace_id": get_trace_id()}
    )
    
    # First restore attempt
    try:
        from metrics_registry import restore_snapshot_to_collector
        restore_snapshot_to_collector(metrics_collector)
    except Exception as e:
        log_event(
            service="global_metrics_store",
            event="initial_restore_failed",
            status="warn",
            message="Initial restore failed",
            extra={"error": str(e), "trace_id": get_trace_id()}
        )

    while _stop_event is not None and not _stop_event.is_set():
        try:
            ok = _sync_once()
            if not ok:
                log_event(
                    service="global_metrics_store",
                    event="sync_cycle_noop", 
                    status="info",
                    message="Sync cycle completed with no-op (best-effort)",
                    extra={"trace_id": get_trace_id()}
                )
        except Exception as e:
            log_event(
                service="global_metrics_store",
                event="sync_cycle_error",
                status="warn",
                message="Exception during sync cycle", 
                extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()}
            )
            if SENTRY_AVAILABLE:
                capture_exception(e)
                
        if _stop_event:
            _stop_event.wait(interval_sec)

    log_event(
        service="global_metrics_store",
        event="sync_thread_stop",
        status="info",
        message="Global metrics sync thread stopping",
        extra={"trace_id": get_trace_id()}
    )

def start_background_sync(service_name: str = None, interval_sec: int = None) -> None:
    """Start the background sync thread (idempotent)."""
    global _sync_thread, _stop_event, SERVICE_NAME
    
    if service_name:
        SERVICE_NAME = service_name
        
    if _sync_thread and _sync_thread.is_alive():
        log_event(
            service="global_metrics_store",
            event="sync_already_running", 
            status="info",
            message="Background metrics sync already running",
            extra={"trace_id": get_trace_id()}
        )
        return

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
    """Stop the background sync thread gracefully."""
    global _sync_thread, _stop_event
    if not _sync_thread:
        return
    try:
        if _stop_event:
            _stop_event.set()
        if _sync_thread.is_alive():
            _sync_thread.join(timeout=timeout_sec)
    except Exception as e:
        log_event(
            service="global_metrics_store",
            event="stop_sync_failed",
            status="warn",
            message="Failed to stop sync thread cleanly", 
            extra={"error": str(e), "stack": traceback.format_exc(), "trace_id": get_trace_id()}
        )
    finally:
        _sync_thread = None
        _stop_event = None

# ------------------------------------------------------------------
# Phase 11-F: Export Enhanced Functionality
# ------------------------------------------------------------------
def metrics_safe_sync():
    """Safe sync for external use."""
    try:
        return _sync_once()
    except Exception as e:
        log_event(
            service="global_metrics_store",
            event="safe_sync_failed",
            status="error",
            message="metrics_safe_sync failed",
            extra={"error": str(e), "trace_id": get_trace_id()}
        )
        return False

# Export the key new functionality
__all__ = [
    'publish_metric',
    'get_prometheus_metrics', 
    'update_service_heartbeat',
    'check_service_ttl',
    'get_global_snapshot',
    'start_background_sync',
    'stop_background_sync',
    'metrics_safe_sync'
]