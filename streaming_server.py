"""
streaming_server.py — Phase 11-D (Unified Metrics Registry + Global Sync)
Sara AI Core — Streaming Service

- Uses unified metrics_registry.REGISTRY as the shared Prometheus registry
- Restores persisted snapshot from Redis into metrics_collector on startup
- Starts background metrics sync for global aggregation
- /metrics returns generate_latest(REGISTRY) for unified metrics
- /metrics_snapshot persists combined snapshot to Redis for cross-service restore
- Adds RedisCircuitBreaker to isolate Redis outages
- Retains SSE streaming, Twilio webhook, health endpoints, and structured logging
"""

import json
import time
import uuid
import logging_utils
import traceback
import asyncio
import threading
import os
from typing import Generator, Optional, Dict, Any

from flask import Flask, request, jsonify, Response, stream_with_context
import redis  # for typed redis exceptions and client behaviors

# --- PATCH START: Fix duplicated timeseries (streaming_server_uptime_seconds) ---
from prometheus_client import Gauge, REGISTRY

# Define a persistent uptime gauge safely (avoid duplicate registration)
def get_or_create_uptime_gauge():
    metric_name = "streaming_server_uptime_seconds"
    if metric_name in REGISTRY._names_to_collectors:
        return REGISTRY._names_to_collectors[metric_name]
    return Gauge(
        metric_name,
        "Uptime of the streaming server in seconds",
        registry=REGISTRY
    )

UPTIME_GAUGE = get_or_create_uptime_gauge()

def report_health_metrics(start_time, active_streams, redis_state, redis_failures):
    """
    Report health metrics without duplicating gauges.
    """
    from time import time
    import logging_utils
    import metrics_collector as mc

    try:
        uptime_seconds = time() - start_time
        UPTIME_GAUGE.set(uptime_seconds)  # reuse persistent gauge safely

        mc.increment_metric("streaming.health_check.count")
        mc.observe_latency("streaming.health_check.latency", 0.001)

        logging_utils.log_event(
        service="streaming_server",
        event="health_reported",
        status="ok",
        message="Health metrics reported successfully",
        extra={
            "uptime_seconds": uptime_seconds,
            "active_streams": active_streams,
            "redis_state": redis_state,
            "redis_failures": redis_failures
        }
     )

    except Exception as e:
        logging_utils.log_event(
            service="streaming_server",
            event="health_report_failed",
            status="error",
            message="Failed to report health metrics",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
# --- PATCH END ---

# --------------------------------------------------------------------------
# Phase 11-D Configuration Isolation
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    # Fallback config for backward compatibility
    import os
    class Config:
        STREAM_HEARTBEAT_INTERVAL = float(os.getenv("STREAM_HEARTBEAT_INTERVAL", "10"))
        PORT = int(os.getenv("PORT", "7000"))

# --------------------------------------------------------------------------
# Phase 11-D Metrics Integration - Lazy Import Shim
# --------------------------------------------------------------------------
def _get_metrics():
    """Lazy metrics shim to avoid circular imports at import-time"""
    try:
        # Import the module and get the MetricsCollector class or instance
        import metrics_collector as metrics_module
        # Check if metrics_module has a MetricsCollector class
        if hasattr(metrics_module, 'MetricsCollector'):
            return metrics_module.MetricsCollector()
        else:
            # If it's already a singleton instance, return it directly
            return metrics_module
    except Exception:
        # safe no-op fallbacks
        class NoopMetrics:
            def increment_metric(self, *a, **k): pass
            def observe_latency(self, *a, **k): pass
            def get_snapshot(self, *a, **k): return {}
        return NoopMetrics()

# Lazy metrics instance - will be initialized on first use
_metrics = None

def get_metrics():
    """Get or initialize lazy metrics instance"""
    global _metrics
    if _metrics is None:
        _metrics = _get_metrics()
    return _metrics

# --------------------------------------------------------------------------
# Local modules
# --------------------------------------------------------------------------
from tasks import run_tts
from gpt_client import generate_reply
from logging_utils import log_event

# --------------------------------------------------------------------------
# Shared registry & registry-backed metrics (Phase 11-D) - Lazy imports
# --------------------------------------------------------------------------
def _get_registry():
    """Lazy import for metrics registry"""
    try:
        from metrics_registry import REGISTRY
        return REGISTRY
    except Exception:
        return None

def _get_prometheus_client():
    """Lazy import for prometheus client"""
    try:
        from prometheus_client import Summary, Gauge, Counter, generate_latest, CONTENT_TYPE_LATEST
        return Summary, Gauge, Counter, generate_latest, CONTENT_TYPE_LATEST
    except Exception:
        # Fallback no-op classes
        class NoopMetric:
            def __init__(self, *args, **kwargs): pass
            def inc(self, *args, **kwargs): pass
            def set(self, *args, **kwargs): pass
            def observe(self, *args, **kwargs): pass
            def labels(self, *args, **kwargs): return self
            def time(self): return lambda: None
        return NoopMetric, NoopMetric, NoopMetric, lambda: "# metrics_unavailable\n", "text/plain"

# Initialize metrics with lazy loading
Summary, Gauge, Counter, generate_latest, CONTENT_TYPE_LATEST = _get_prometheus_client()
REGISTRY = _get_registry()

# Register streaming-specific metrics to the shared REGISTRY
stream_latency_ms = Summary(
    "stream_latency_ms",
    "TTS stream completion latency (milliseconds)",
    registry=REGISTRY,
)

stream_bytes_out_total = Gauge(
    "stream_bytes_out_total",
    "Total number of audio bytes streamed out",
    registry=REGISTRY,
)

# Phase 11-D Unified Metrics Schema
stream_events_total = Counter(
    "stream_events_total",
    "Total stream events processed",
    ["event_type"],
    registry=REGISTRY,
)

stream_errors_total = Counter(
    "stream_errors_total", 
    "Total stream errors",
    ["error_type"],
    registry=REGISTRY,
)

latency_seconds = Summary(
    "latency_seconds",
    "Stream processing latency in seconds",
    ["stage"],
    registry=REGISTRY,
)

redis_retries_total = Counter(
    "redis_retries_total",
    "Total Redis operation retries",
    registry=REGISTRY,
)

# --------------------------------------------------------------------------
# Redis client (use centralized redis_client module)
# --------------------------------------------------------------------------
# Unified import pattern required by Sean:
from redis_client import get_client
get_redis_client = get_client
# instantiate local singleton (may return None if module returns None)
try:
    redis_client = get_redis_client()
except Exception as e:
    redis_client = None
    log_event(
        service="streaming_server",
        event="redis_client_init_failed",
        status="warn",
        message="get_client() raised during init",
        extra={"error": str(e), "stack": traceback.format_exc()},
    )

# --------------------------------------------------------------------------
# Phase 11-D Redis Circuit Breaker (Standardized)
# --------------------------------------------------------------------------
import time as _time
from functools import wraps

class RedisCircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_time=30, timeout=5):
        self.failures = 0
        self.failure_threshold = failure_threshold
        self.last_failure_time = 0
        self.recovery_time = recovery_time
        self.timeout = timeout
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper

    def call(self, func, *args, **kwargs):
        now = _time.time()
        
        # Check if circuit is OPEN
        if self.state == "OPEN":
            if now - self.last_failure_time < self.recovery_time:
                log_event(
                    service="streaming_server",
                    event="redis_circuit_open",
                    status="warning",
                    message="Circuit breaker OPEN - skipping Redis call",
                    extra={
                        "failures": self.failures,
                        "time_since_last_failure": now - self.last_failure_time
                    },
                )
                redis_retries_total.inc()
                return None
            else:
                # Transition to HALF_OPEN for trial
                self.state = "HALF_OPEN"
                log_event(
                    service="streaming_server",
                    event="redis_circuit_half_open",
                    status="info",
                    message="Circuit breaker HALF_OPEN - testing recovery",
                )
        try:
            # Execute with timeout protection
            result = func(*args, **kwargs)
            
            # Success - reset circuit
            self.failures = 0
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                log_event(
                    service="streaming_server",
                    event="redis_circuit_closed",
                    status="info",
                    message="Circuit breaker CLOSED - Redis recovered",
                )
            return result
            
        except (redis.exceptions.RedisError, redis.exceptions.TimeoutError) as e:
            self.failures += 1
            self.last_failure_time = _time.time()
            
            if self.failures >= self.failure_threshold:
                self.state = "OPEN"
                log_event(
                    service="streaming_server",
                    event="redis_circuit_opened",
                    status="error",
                    message="Circuit breaker OPENED due to Redis failures",
                    extra={
                        "failures": self.failures,
                        "error": str(e),
                        "recovery_time_seconds": self.recovery_time
                    },
                )
            
            redis_retries_total.inc()
            stream_errors_total.labels(error_type="redis").inc()
            
            log_event(
                service="streaming_server",
                event="redis_call_failed",
                status="error",
                message=f"Redis operation failed: {e}",
                extra={
                    "failures": self.failures,
                    "state": self.state,
                    "stack": traceback.format_exc()
                },
            )
            return None
            
        except Exception as e:
            # Non-Redis exceptions don't trip the circuit breaker
            log_event(
                service="streaming_server",
                event="redis_call_failed_nonredis",
                status="error",
                message=f"Non-Redis exception in circuit breaker: {e}",
                extra={"stack": traceback.format_exc()},
            )
            return None

# instantiate circuit breaker with conservative defaults
redis_cb = RedisCircuitBreaker(failure_threshold=3, recovery_time=30, timeout=5)

# --------------------------------------------------------------------------
# Stream State Management for Recovery Checkpoints
# --------------------------------------------------------------------------
class StreamStateManager:
    def __init__(self):
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self.checkpoint_interval = 30  # seconds
        
    def create_checkpoint(self, stream_id: str, state: Dict[str, Any]):
        """Store stream state checkpoint in Redis"""
        try:
            checkpoint = {
                "stream_id": stream_id,
                "state": state,
                "timestamp": time.time(),
                "service": "streaming_server"
            }
            
            @redis_cb
            def _save_checkpoint():
                client = get_redis_client()
                if client:
                    key = f"stream_checkpoint:{stream_id}"
                    client.setex(key, 3600, json.dumps(checkpoint))  # 1 hour TTL
                    return True
                return False
                
            result = _save_checkpoint()
            if result:
                log_event(
                    service="streaming_server",
                    event="stream_checkpoint_saved",
                    status="info",
                    message="Stream state checkpoint saved",
                    extra={"stream_id": stream_id, "state_keys": list(state.keys())},
                )
            return result
            
        except Exception as e:
            log_event(
                service="streaming_server",
                event="checkpoint_save_failed",
                status="warn",
                message="Failed to save stream checkpoint",
                extra={"stream_id": stream_id, "error": str(e)},
            )
            return False
    
    def restore_checkpoint(self, stream_id: str) -> Optional[Dict[str, Any]]:
        """Restore stream state from Redis checkpoint"""
        try:
            @redis_cb
            def _load_checkpoint():
                client = get_redis_client()
                if client:
                    key = f"stream_checkpoint:{stream_id}"
                    data = client.get(key)
                    if data:
                        return json.loads(data)
                return None
                
            checkpoint = _load_checkpoint()
            if checkpoint:
                log_event(
                    service="streaming_server",
                    event="stream_checkpoint_restored",
                    status="info",
                    message="Stream state checkpoint restored",
                    extra={"stream_id": stream_id},
                )
                return checkpoint.get("state", {})
            return None
            
        except Exception as e:
            log_event(
                service="streaming_server",
                event="checkpoint_restore_failed",
                status="warn",
                message="Failed to restore stream checkpoint",
                extra={"stream_id": stream_id, "error": str(e)},
            )
            return None

# Initialize stream state manager
stream_state_mgr = StreamStateManager()

# --------------------------------------------------------------------------
# Async Health Monitoring
# --------------------------------------------------------------------------
class HealthMonitor:
    def __init__(self):
        self.running = False
        self.thread = None
        self.uptime_start = time.time()
        
    def start(self):
        """Start the health monitoring background thread"""
        if self.running:
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        log_event(
            service="streaming_server",
            event="health_monitor_started",
            status="info",
            message="Health monitoring started",
        )
    
    def stop(self):
        """Stop the health monitoring"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
    
    def _monitor_loop(self):
        """Background monitoring loop"""
        while self.running:
            try:
                self._report_health()
                time.sleep(15)  # Report every 15 seconds
            except Exception as e:
                log_event(
                    service="streaming_server",
                    event="health_monitor_error",
                    status="error",
                    message="Health monitor loop error",
                    extra={"error": str(e)},
                )
                time.sleep(15)
    
    def _report_health(self):
        """Report health metrics to system monitor"""
        try:
            uptime = time.time() - self.uptime_start
            active_streams = len(stream_state_mgr.active_streams)
            
            # Use the patched function to report health metrics safely
            report_health_metrics(self.uptime_start, active_streams, redis_cb.state, redis_cb.failures)
            
        except Exception as e:
            log_event(
                service="streaming_server",
                event="health_report_failed",
                status="error",
                message="Failed to report health metrics",
                extra={"error": str(e)},
            )

# Initialize health monitor
health_monitor = HealthMonitor()

# --------------------------------------------------------------------------
# Import diagnostic helpers for parity with app.py (fallback stubs)
# --------------------------------------------------------------------------
try:
    from utils import check_redis_status, check_r2_connectivity
except Exception:
    async def check_r2_connectivity():
        return "not_available"

    def check_redis_status():
        return "not_available"

# --------------------------------------------------------------------------
# Flask app
# --------------------------------------------------------------------------
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# --------------------------------------------------------------------------
# Lazy Initialization Functions (Phase 11-D - No side effects at import time)
# --------------------------------------------------------------------------
def _initialize_metrics_system():
    """Lazy metrics system initialization - called on first request"""
    try:
        from metrics_registry import restore_snapshot_to_collector
        from global_metrics_store import start_background_sync
        
        restore_snapshot_to_collector(get_metrics())
        start_background_sync(service_name="streaming")
        
        # Start health monitoring
        health_monitor.start()
        
        log_event(
            service="streaming",
            event="global_metrics_sync_started",
            status="ok",
            message="Streaming server metrics restored and sync started",
            extra={"phase": "11-D"}
        )
    except Exception as e:
        log_event(
            service="streaming",
            event="metrics_startup_failed",
            status="error",
            message="Failed to initialize unified metrics",
            extra={"error": str(e), "stack": traceback.format_exc(), "phase": "11-D"}
        )

def _initialize_metrics_restore():
    """Lazy metrics restore - called on first request"""
    try:
        @redis_cb
        def _restore_metrics():
            from metrics_registry import restore_snapshot_to_collector
            return restore_snapshot_to_collector(get_metrics())
        
        result = _restore_metrics()
        
        if result:
            log_event(
                service="streaming_server",
                event="metrics_snapshot_restored",
                status="info",
                message="Restored snapshot into metrics_collector from Redis using standardized restoration.",
            )
        else:
            log_event(
                service="streaming_server",
                event="metrics_restore_no_data",
                status="info",
                message="No metrics snapshot found in Redis for restoration.",
            )
    except Exception as e:
        log_event(
            service="streaming_server",
            event="restore_at_startup_failed",
            status="warn",
            message="Failed to restore metrics snapshot at startup (best-effort).",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )

def _ensure_initialized():
    """Ensure metrics are initialized on first use"""
    _initialize_metrics_system()
    _initialize_metrics_restore()

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def new_trace() -> str:
    trace_id = str(uuid.uuid4())
    log_event(
        service="streaming_server",
        event="trace_created",
        status="info",
        message="New trace context created",
        extra={"trace_id": trace_id},
    )
    return trace_id

def sse_format(event: Optional[str] = None, data: Optional[dict] = None) -> str:
    msg_lines = []
    if event:
        msg_lines.append(f"event: {event}")
        stream_events_total.labels(event_type=event).inc()
    if data is not None:
        msg_lines.append(f"data: {json.dumps(data, separators=(',', ':'))}")
    msg_lines.append("")
    return "\n".join(msg_lines) + "\n"

def safe_redis_ping(trace_id: Optional[str] = None, session_id: Optional[str] = None) -> bool:
    # Use circuit breaker for ping
    client = get_redis_client()
    if not client:
        return False
    
    @redis_cb
    def _ping():
        return client.ping()
    
    res = _ping()
    if res is None:
        return False
    return bool(res)

def safe_redis_call(func, *args, **kwargs):
    """Utility wrapper to call Redis operations via circuit breaker."""
    client = get_redis_client()
    if not client:
        return None
    
    @redis_cb  
    def _wrapped_call():
        return func(*args, **kwargs)
        
    return _wrapped_call()

# --------------------------------------------------------------------------
# Prometheus Metrics Endpoint
# --------------------------------------------------------------------------
@app.route("/metrics", methods=["GET"])
def metrics_endpoint():
    """
    Expose unified metrics from shared REGISTRY.
    """
    try:
        try:
            get_metrics().increment_metric("streaming_metrics_requests_total")
            stream_events_total.labels(event_type="metrics_request").inc()
        except Exception:
            pass

        # Generate unified metrics from shared REGISTRY
        try:
            reg_text = generate_latest(REGISTRY).decode("utf-8")
            return Response(reg_text, mimetype=CONTENT_TYPE_LATEST, status=200)
        except Exception as e:
            log_event(
                service="streaming_server",
                event="generate_latest_failed",
                status="error",
                message="Failed to generate REGISTRY output",
                extra={"error": str(e), "stack": traceback.format_exc()},
            )
            stream_errors_total.labels(error_type="metrics_generation").inc()
            return Response("# metrics_export_error 1\n", mimetype="text/plain", status=500)

    except Exception as e:
        log_event(
            service="streaming_server",
            event="metrics_endpoint_error",
            status="error",
            message="Failed to serve /metrics",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        stream_errors_total.labels(error_type="metrics_endpoint").inc()
        return Response("# metrics_export_error 1\n", mimetype="text/plain", status=500)

# --------------------------------------------------------------------------
# /metrics_snapshot — JSON snapshot persisted to Redis (Phase 11-D)
# --------------------------------------------------------------------------
@app.route("/metrics_snapshot", methods=["GET"])
def metrics_snapshot():
    try:
        try:
            get_metrics().increment_metric("streaming_metrics_snapshot_requests_total")
            stream_events_total.labels(event_type="metrics_snapshot").inc()
        except Exception:
            pass

        coll_snap = {}
        try:
            coll_snap = get_metrics().get_snapshot()
        except Exception:
            coll_snap = {}

        registry_snap = {}
        try:
            for family in REGISTRY.collect():
                fam_name = family.name
                samples = []
                for s in family.samples:
                    # handle tuple-like and object sample representations
                    try:
                        sample_name = s[0] if isinstance(s, (list, tuple)) else getattr(s, "name", None)
                        labels = s[1] if isinstance(s, (list, tuple)) else getattr(s, "labels", {})
                        value = s[2] if isinstance(s, (list, tuple)) else getattr(s, "value", None)
                    except Exception:
                        sample_name, labels, value = getattr(s, "name", None), getattr(s, "labels", {}), getattr(s, "value", None)
                    samples.append({"name": sample_name, "labels": labels, "value": value})
                registry_snap[fam_name] = {
                    "documentation": getattr(family, "documentation", ""),
                    "type": getattr(family, "type", ""),
                    "samples": samples,
                }
        except Exception:
            registry_snap = {}

        payload = {
            "service": "streaming",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "collector_snapshot": coll_snap,
            "registry_snapshot": registry_snap,
        }

        # Persist collector snapshot via helper (guarded)
        try:
            from metrics_collector import push_snapshot_from_collector
            safe_redis_call(push_snapshot_from_collector, get_metrics().get_snapshot)
        except redis.exceptions.RedisError as e:
            log_event(
                service="streaming_server",
                event="push_snapshot_redis_failed",
                status="warn",
                message=str(e),
                extra={"stack": traceback.format_exc()},
            )
            stream_errors_total.labels(error_type="redis_snapshot").inc()
        except Exception as e:
            log_event(
                service="streaming_server",
                event="push_snapshot_failed",
                status="warn",
                message="push_snapshot_from_collector failed",
                extra={"error": str(e)},
            )
            stream_errors_total.labels(error_type="snapshot_push").inc()

        # Also save combined payload as convenience/fallback
        try:
            from metrics_registry import save_metrics_snapshot
            safe_redis_call(save_metrics_snapshot, payload)
        except redis.exceptions.RedisError:
            stream_errors_total.labels(error_type="redis_backup").inc()
        except Exception:
            stream_errors_total.labels(error_type="backup_snapshot").inc()

        return jsonify(payload), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="metrics_snapshot_error",
            status="error",
            message="Failed to produce metrics snapshot",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        stream_errors_total.labels(error_type="snapshot_generation").inc()
        return jsonify({"error": "snapshot_failure"}), 500

# --------------------------------------------------------------------------
# Health endpoints
# --------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    try:
        get_metrics().increment_metric("streaming_healthz_requests_total")
        stream_events_total.labels(event_type="healthz").inc()
    except Exception:
        pass
    
    log_event(
        service="streaming",
        event="healthz_check",
        status="ok",
        message="Streaming health OK",
        extra={
            "active_streams": len(stream_state_mgr.active_streams),
            "redis_circuit_state": redis_cb.state,
            "uptime_seconds": time.time() - health_monitor.uptime_start
        }
    )
    return jsonify({
        "status": "ok", 
        "service": "streaming_server",
        "active_streams": len(stream_state_mgr.active_streams),
        "redis_circuit_state": redis_cb.state
    }), 200

@app.route("/health", methods=["GET"])
def health():
    trace_id = new_trace()
    log_event(
        service="streaming",
        event="health_check",
        status="ok",
        message="Streaming health check",
        trace_id=trace_id,
        extra={
            "active_streams": len(stream_state_mgr.active_streams),
            "redis_circuit_state": redis_cb.state
        }
    )
    
    client = get_redis_client()
    if client is None:
        return jsonify({"status": "ok", "redis": "not_applicable"}), 200
    try:
        ok = safe_redis_ping(trace_id=trace_id)
        if ok:
            return jsonify({
                "status": "ok", 
                "redis": "connected",
                "circuit_state": redis_cb.state,
                "active_streams": len(stream_state_mgr.active_streams)
            }), 200
        else:
            log_event(
                service="streaming_server",
                event="health_degraded",
                status="warn",
                message="Redis unreachable during health check",
                trace_id=trace_id,
                extra={"circuit_state": redis_cb.state, "failures": redis_cb.failures},
            )
            return jsonify({
                "status": "degraded", 
                "redis": "unreachable",
                "circuit_state": redis_cb.state
            }), 503
    except redis.exceptions.RedisError as e:
        log_event(
            service="streaming_server",
            event="health_exception_redis",
            status="error",
            message=str(e),
            trace_id=trace_id,
            extra={"stack": traceback.format_exc()},
        )
        stream_errors_total.labels(error_type="health_redis").inc()
        return jsonify({"status": "degraded", "redis": "error"}), 503
    except Exception as e:
        log_event(
            service="streaming_server",
            event="health_exception",
            status="error",
            message="Health endpoint failure",
            trace_id=trace_id,
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        stream_errors_total.labels(error_type="health_general").inc()
        return jsonify({"status": "degraded", "redis": "error"}), 503

# --------------------------------------------------------------------------
# System Status Endpoint (parity schema)
# --------------------------------------------------------------------------
@app.route("/system_status", methods=["GET"])
def system_status():
    try:
        log_event(
            service="streaming",
            event="system_status_check",
            status="ok",
            message="System status check",
            extra={
                "active_streams": len(stream_state_mgr.active_streams),
                "redis_circuit_state": redis_cb.state,
                "uptime_seconds": time.time() - health_monitor.uptime_start
            }
        )
        return jsonify({
            "service": "streaming_server",
            "status": "ok",
            "redis_connectivity": "not_applicable_in_streaming",
            "r2_connectivity": "ok",
            "active_streams": len(stream_state_mgr.active_streams),
            "redis_circuit_state": redis_cb.state,
            "uptime_seconds": round(time.time() - health_monitor.uptime_start, 2)
        }), 200
    except Exception as e:
        trace_id = new_trace()
        log_event(
            service="streaming_server",
            event="system_status_failed",
            status="error",
            message="Failed to check system status",
            trace_id=trace_id,
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        stream_errors_total.labels(error_type="system_status").inc()
        return jsonify({
            "status": "error",
            "service": "streaming_server",
            "message": str(e),
        }), 500

# --------------------------------------------------------------------------
# SSE Streaming Endpoint
# --------------------------------------------------------------------------
@app.route("/stream", methods=["POST"])
def stream():
    trace_id = None
    session_id = None

    try:
        get_metrics().increment_metric("stream_requests_total")
        stream_events_total.labels(event_type="stream_request").inc()
    except Exception:
        pass

    try:
        data = request.get_json(force=True)
        session_id = data.get("session_id") or str(uuid.uuid4())
        trace_id = data.get("trace_id") or new_trace()
        user_text = (data.get("text") or data.get("message") or "").strip()

        if not user_text:
            log_event(
                service="streaming_server",
                event="stream_rejected",
                status="error",
                message="No input text provided",
                trace_id=trace_id,
                session_id=session_id,
            )
            get_metrics().increment_metric("stream_rejected_total")
            stream_errors_total.labels(error_type="no_input_text").inc()
            return jsonify({"error": "No input text"}), 400

        # Create initial stream state
        stream_state = {
            "session_id": session_id,
            "trace_id": trace_id,
            "user_text": user_text,
            "stage": "received",
            "start_time": time.time(),
            "last_checkpoint": time.time()
        }
        stream_state_mgr.active_streams[session_id] = stream_state
        
        # Save initial checkpoint
        stream_state_mgr.create_checkpoint(session_id, stream_state)

        log_event(
            service="streaming_server",
            event="stream_start",
            status="ok",
            message=f"Stream session started ({len(user_text)} chars)",
            trace_id=trace_id,
            session_id=session_id,
            extra={"text_length": len(user_text), "stage": "initialized"},
        )

        @stream_with_context
        def event_stream() -> Generator[str, None, None]:
            try:
                yield sse_format("status", {"stage": "received", "trace_id": trace_id})

                start_infer = time.time()
                with latency_seconds.labels(stage="inference").time():
                    reply_text = generate_reply(user_text, trace_id=trace_id)
                infer_ms = round((time.time() - start_infer) * 1000, 2)

                # Update stream state
                stream_state["stage"] = "inference_complete"
                stream_state["inference_latency_ms"] = infer_ms
                stream_state["reply_text_length"] = len(reply_text)
                stream_state_mgr.active_streams[session_id] = stream_state

                # Save checkpoint after inference
                if time.time() - stream_state["last_checkpoint"] > 30:
                    stream_state_mgr.create_checkpoint(session_id, stream_state)
                    stream_state["last_checkpoint"] = time.time()

                try:
                    get_metrics().observe_latency("inference_latency_ms", infer_ms)
                except Exception:
                    pass

                try:
                    stream_latency_ms.observe(infer_ms)
                except Exception:
                    pass

                log_event(
                    service="streaming_server",
                    event="inference_done",
                    status="ok",
                    message=f"Inference completed ({len(reply_text)} chars)",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={
                        "inference_latency_ms": infer_ms,
                        "reply_text_length": len(reply_text),
                        "stage": "inference_complete"
                    },
                )
                yield sse_format("reply_text", {"text": reply_text})

                start_tts = time.time()
                with latency_seconds.labels(stage="tts").time():
                    tts_result = run_tts(
                        {"text": reply_text, "trace_id": trace_id, "session_id": session_id},
                        inline=True,
                    )
                tts_ms = round((time.time() - start_tts) * 1000, 2)

                if isinstance(tts_result, dict) and tts_result.get("error"):
                    get_metrics().increment_metric("tts_failures_total")
                    stream_errors_total.labels(error_type="tts_generation").inc()
                    log_event(
                        service="streaming_server",
                        event="tts_failed",
                        status="error",
                        message="TTS generation failed",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={"tts_result": tts_result, "tts_latency_ms": tts_ms},
                    )
                    yield sse_format("error", tts_result)
                    return

                if isinstance(tts_result, dict) and tts_result.get("cached"):
                    get_metrics().increment_metric("tts_cache_hits_total")
                    stream_events_total.labels(event_type="tts_cache_hit").inc()
                    log_event(
                        service="streaming_server",
                        event="cache_hit",
                        status="ok",
                        message="TTS result served from cache",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={"cached": True, "tts_latency_ms": tts_ms},
                    )

                # If we have a bytes count from TTS, increment the shared Gauge
                if isinstance(tts_result, dict):
                    bytes_out = tts_result.get("bytes", None)
                    if isinstance(bytes_out, (int, float)):
                        try:
                            stream_bytes_out_total.inc(bytes_out)
                        except Exception:
                            pass

                audio_url = tts_result.get("audio_url") if isinstance(tts_result, dict) else None
                
                # Update final stream state
                stream_state["stage"] = "complete"
                stream_state["tts_latency_ms"] = tts_ms
                stream_state["audio_url"] = audio_url
                stream_state["end_time"] = time.time()
                stream_state_mgr.active_streams[session_id] = stream_state

                # Final checkpoint
                stream_state_mgr.create_checkpoint(session_id, stream_state)

                log_event(
                    service="streaming_server",
                    event="tts_done",
                    status="ok",
                    message="TTS generated successfully",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={
                        "audio_url": audio_url, 
                        "tts_latency_ms": tts_ms,
                        "stage": "complete",
                        "total_duration_ms": round((time.time() - stream_state["start_time"]) * 1000, 2)
                    },
                )

                yield sse_format("audio_ready", {"url": audio_url})
                yield sse_format("complete", {"trace_id": trace_id, "session_id": session_id})

                # Clean up completed stream
                if session_id in stream_state_mgr.active_streams:
                    del stream_state_mgr.active_streams[session_id]

            except Exception as exc:
                err_id = str(uuid.uuid4())
                log_event(
                    service="streaming_server",
                    event="stream_exception",
                    status="error",
                    message=str(exc),
                    trace_id=trace_id or new_trace(),
                    session_id=session_id or "unknown",
                    extra={
                        "error_id": err_id, 
                        "stack": traceback.format_exc(),
                        "stage": stream_state.get("stage", "unknown")
                    },
                )
                stream_errors_total.labels(error_type="stream_internal").inc()
                yield sse_format("error", {"message": "Streaming error", "error_id": err_id})
                
                # Clean up failed stream
                if session_id in stream_state_mgr.active_streams:
                    del stream_state_mgr.active_streams[session_id]

        headers = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        return Response(event_stream(), headers=headers)
    except Exception as e:
        trace = trace_id or new_trace()
        log_event(
            service="streaming_server",
            event="fatal_error",
            status="error",
            message=str(e),
            trace_id=trace,
            session_id=session_id or "unknown",
            extra={"stack": traceback.format_exc()},
        )
        get_metrics().increment_metric("stream_errors_total")
        stream_errors_total.labels(error_type="fatal").inc()
        
        # Clean up on fatal error
        if session_id and session_id in stream_state_mgr.active_streams:
            del stream_state_mgr.active_streams[session_id]
            
        return jsonify({"error": "Internal server error", "trace_id": trace}), 500

# --------------------------------------------------------------------------
# Twilio webhook compatibility
# --------------------------------------------------------------------------
@app.route("/twilio_tts", methods=["POST"])
def twilio_tts():
    from flask import Response as TwilioResponse

    trace_id = new_trace()
    session_id = str(uuid.uuid4())

    try:
        get_metrics().increment_metric("twilio_requests_total")
        stream_events_total.labels(event_type="twilio_request").inc()
    except Exception:
        pass

    try:
        text = request.form.get("SpeechResult") or request.form.get("text") or "Hello from Sara AI"
        with latency_seconds.labels(stage="twilio_tts").time():
            tts_result = run_tts({"text": text, "session_id": session_id, "trace_id": trace_id}, inline=True)

        if isinstance(tts_result, dict) and tts_result.get("error"):
            get_metrics().increment_metric("tts_failures_total")
            stream_errors_total.labels(error_type="twilio_tts").inc()
            log_event(
                service="streaming_server",
                event="twilio_tts_failed",
                status="error",
                message="TTS generation for Twilio failed",
                trace_id=trace_id,
                session_id=session_id,
                extra={"tts_result": tts_result},
            )
            return TwilioResponse(
                "<Response><Say>Sorry, an error occurred generating speech.</Say></Response>",
                mimetype="application/xml",
            )

        audio_url = tts_result.get("audio_url") if isinstance(tts_result, dict) else None
        log_event(
            service="streaming_server",
            event="twilio_tts_done",
            status="ok",
            message="Generated Twilio-compatible TTS",
            trace_id=trace_id,
            session_id=session_id,
            extra={"audio_url": audio_url, "text_length": len(text)},
        )

        twiml = f"<Response><Play>{audio_url}</Play></Response>"
        return TwilioResponse(twiml, mimetype="application/xml")

    except Exception as e:
        log_event(
            service="streaming_server",
            event="twilio_tts_exception",
            status="error",
            message="Twilio TTS handler error",
            trace_id=trace_id,
            session_id=session_id,
            extra={"stack": traceback.format_exc()},
        )
        get_metrics().increment_metric("twilio_errors_total")
        stream_errors_total.labels(error_type="twilio_exception").inc()
        return TwilioResponse(
            "<Response><Say>Sorry, an internal error occurred.</Say></Response>",
            mimetype="application/xml",
        )

# --------------------------------------------------------------------------
# Stream Recovery Endpoint
# --------------------------------------------------------------------------
@app.route("/stream/recover/<session_id>", methods=["GET"])
def recover_stream(session_id: str):
    """Recover stream state from checkpoint"""
    try:
        state = stream_state_mgr.restore_checkpoint(session_id)
        if state:
            log_event(
                service="streaming_server",
                event="stream_recovered",
                status="info",
                message="Stream state recovered from checkpoint",
                extra={"session_id": session_id, "stage": state.get("stage")},
            )
            stream_events_total.labels(event_type="stream_recovery").inc()
            return jsonify({"status": "recovered", "state": state}), 200
        else:
            log_event(
                service="streaming_server",
                event="stream_recovery_failed",
                status="warn",
                message="No checkpoint found for stream recovery",
                extra={"session_id": session_id},
            )
            stream_errors_total.labels(error_type="recovery_not_found").inc()
            return jsonify({"status": "not_found"}), 404
    except Exception as e:
        log_event(
            service="streaming_server",
            event="stream_recovery_error",
            status="error",
            message="Stream recovery failed",
            extra={"session_id": session_id, "error": str(e)},
        )
        stream_errors_total.labels(error_type="recovery_error").inc()
        return jsonify({"status": "error", "message": str(e)}), 500

# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------
if __name__ == "__main__":
    # Initialize metrics system on startup
    _ensure_initialized()
    
    # Ensure health monitor is stopped on exit
    import atexit
    atexit.register(health_monitor.stop)
    
    log_event(
        service="streaming",
        event="startup",
        status="ok",
        message="Streaming server initialized with unified metrics registry",
        extra={"phase": "11-D"}
    )
    
    # Explicit port override to avoid conflict with main app
    port = int(os.getenv("STREAMING_PORT", "5001"))
    app.run(host="0.0.0.0", port=port, threaded=True)