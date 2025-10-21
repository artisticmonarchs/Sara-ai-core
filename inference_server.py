"""
inference_server.py — Phase 11-D Compliant
Inference server with trace propagation, centralized config, and unified metrics.
"""

import time
import json
import traceback
from typing import Tuple, Optional

from flask import Flask, request, jsonify
from werkzeug.exceptions import HTTPException

# Phase 11-D: Use centralized config only
from config import Config

# Phase 11-D: Defer metrics imports to avoid circular dependencies
def _get_metrics():
    """Lazy import metrics to avoid circular imports."""
    try:
        from metrics_collector import increment_metric, observe_latency
        return increment_metric, observe_latency
    except ImportError:
        # Safe no-op fallbacks
        def _noop_inc(*args, **kwargs): pass
        def _noop_obs(*args, **kwargs): pass
        return _noop_inc, _noop_obs

# Initialize lazy metrics
increment_metric, observe_latency = _get_metrics()

# Phase 11-D: Import other dependencies
from logging_utils import log_event, get_trace_id
from sentry_utils import init_sentry, capture_exception_safe
from celery_app import celery
from redis_client import get_client, safe_redis_operation

# ------------------------------------------------------------------
# Phase 11-D: Application Initialization (No side effects at import)
# ------------------------------------------------------------------
app = Flask(__name__)
SERVICE_NAME = "inference_server"

# Redis client for lightweight metrics (lazy initialization)
_redis_client = None

def get_redis_client():
    """Lazy Redis client initialization."""
    global _redis_client
    if _redis_client is None:
        _redis_client = get_client()
    return _redis_client

def initialize_inference_server():
    """Initialize inference server components - call this at startup."""
    # Initialize Sentry
    init_sentry()
    
    # Phase 11-D: Start background sync only when explicitly initialized
    try:
        from global_metrics_store import start_background_sync
        start_background_sync(service_name=SERVICE_NAME)
        log_event(
            service=SERVICE_NAME,
            event="global_metrics_sync_started",
            status="info",
            message="Background global metrics sync started for inference_server",
            trace_id=get_trace_id()
        )
    except Exception as e:
        log_event(
            service=SERVICE_NAME, 
            event="global_metrics_sync_failed", 
            status="error",
            message="Failed to start background metrics sync",
            trace_id=get_trace_id(),
            extra={"error": str(e), "stack": traceback.format_exc()}
        )

# ------------------------------------------------------------------
# Phase 11-D: Circuit Breaker Support
# ------------------------------------------------------------------
def _is_circuit_breaker_open(service: str = "inference") -> bool:
    """Check if circuit breaker is open for the specified service."""
    try:
        client = get_redis_client()
        if not client:
            return False
            
        key = f"circuit_breaker:{service}:state"
        state = safe_redis_operation(lambda: client.get(key))
        if state is None:
            return False
        # normalize bytes/str
        if isinstance(state, bytes):
            state = state.decode("utf-8")
        return str(state).lower() == "open"
    except Exception:
        return False  # Proceed on circuit breaker check errors

def validate_payload(payload: dict) -> Tuple[bool, Optional[str]]:
    """Validate basic payload schema."""
    if not isinstance(payload, dict):
        return False, "Invalid JSON format"
    if "input" not in payload and "text" not in payload:
        return False, "Missing required field: 'input' or 'text'"
    content = payload.get("input") or payload.get("text")
    if not isinstance(content, (str, list, dict)):
        return False, "'input' / 'text' must be string, list, or dict"
    if "model" in payload and not isinstance(payload["model"], str):
        return False, "'model' must be a string if provided"
    return True, None

def _record_metrics(latency_ms: float, source: str = "unknown"):
    """Record lightweight metrics with comprehensive Phase 11-D coverage."""
    try:
        # convert ms -> seconds for Prometheus histograms
        latency_s = float(latency_ms) / 1000.0 if latency_ms is not None else 0.0
        try:
            increment_metric("inference_requests_total")
        except Exception:
            pass
        try:
            increment_metric(f"inference_requests_by_source_total:{source}")
        except Exception:
            # fallback if metrics API doesn't accept colon-suffixed names
            try:
                increment_metric(f"inference_requests_{source}_total")
            except Exception:
                pass
        try:
            # prefer seconds naming
            observe_latency("inference_latency_seconds", latency_s)
        except Exception:
            # last resort: legacy name (if used elsewhere)
            try:
                observe_latency("inference_dispatch_latency_ms", latency_ms)
            except Exception:
                pass
    except Exception as e:
        log_event(
            service=SERVICE_NAME,
            event="metrics_record_failed",
            status="warn",
            message="Failed to record metrics",
            trace_id=get_trace_id(),
            extra={"error": str(e)}
        )

def _get_cached_result(session_id: str, input_hash: str) -> Optional[dict]:
    """Get cached inference result with circuit breaker awareness."""
    if _is_circuit_breaker_open("redis"):
        log_event(
            service=SERVICE_NAME,
            event="redis_circuit_breaker_open",
            status="warn",
            message="Redis circuit breaker open; skipping cache lookup",
            trace_id=get_trace_id()
        )
        increment_metric("inference_circuit_breaker_hits_total")
        return None
    
    redis_client = get_redis_client()
    if not redis_client:
        return None
        
    try:
        cache_key = f"inference_cache:{session_id}:{input_hash}"
        cached_raw = safe_redis_operation(lambda: redis_client.get(cache_key))
        if not cached_raw:
            return None
        # decode if bytes
        if isinstance(cached_raw, bytes):
            try:
                cached_raw = cached_raw.decode("utf-8")
            except Exception:
                pass
        try:
            cached = json.loads(cached_raw)
            increment_metric("inference_cache_hits_total")
            log_event(
                service=SERVICE_NAME,
                event="inference_cache_hit",
                status="info",
                message="Cache served successfully",
                trace_id=get_trace_id(),
                extra={"session_id": session_id}
            )
            return cached
        except Exception:
            # fallback - return None
            return None
    except Exception as e:
        log_event(
            service=SERVICE_NAME,
            event="cache_lookup_failed",
            status="warn",
            message="Cache lookup failed",
            trace_id=get_trace_id(),
            extra={"error": str(e)}
        )
        return None

def _set_cached_result(session_id: str, input_hash: str, result: dict, ttl: int = 3600):
    """Set cached inference result with circuit breaker awareness."""
    if _is_circuit_breaker_open("redis"):
        return
        
    redis_client = get_redis_client()
    if not redis_client:
        return
        
    try:
        cache_key = f"inference_cache:{session_id}:{input_hash}"
        safe_redis_operation(
            lambda: redis_client.setex(cache_key, ttl, json.dumps(result))
        )
        log_event(
            service=SERVICE_NAME,
            event="inference_cache_set",
            status="info",
            message="Result cached successfully",
            trace_id=get_trace_id(),
            extra={"session_id": session_id, "ttl": ttl}
        )
    except Exception as e:
        log_event(
            service=SERVICE_NAME,
            event="cache_set_failed",
            status="warn",
            message="Cache set failed",
            trace_id=get_trace_id(),
            extra={"error": str(e)}
        )

@app.route("/infer", methods=["POST"])
def infer():
    """Accept inference requests with trace propagation."""
    start_ts = time.time()
    incoming = request.get_json(silent=True) or {}

    # prefer provided trace_id; otherwise generate one
    trace_id = incoming.get("trace_id") or get_trace_id()
    source = incoming.get("source", "unknown")

    log_event(
        service=SERVICE_NAME,
        event="inference_start",
        status="info",
        message="Inference request received",
        trace_id=trace_id,
        extra={"source": source},
    )

    # Phase 11-D: Circuit breaker check
    if _is_circuit_breaker_open("inference"):
        increment_metric("inference_circuit_breaker_hits_total")
        log_event(
            service=SERVICE_NAME,
            event="circuit_breaker_open",
            status="warn",
            message="Inference circuit breaker active",
            trace_id=trace_id,
        )
        return jsonify({"error": "service_unavailable", "trace_id": trace_id}), 503

    is_valid, error = validate_payload(incoming)
    if not is_valid:
        increment_metric("inference_failures_total")
        log_event(
            service=SERVICE_NAME,
            event="infer_request_invalid",
            status="error",
            message=error,
            trace_id=trace_id,
        )
        return jsonify({"error": error, "trace_id": trace_id}), 400

    # Build canonical payload to send to Celery
    content = incoming.get("input") or incoming.get("text")
    canonical = {
        "trace_id": trace_id,
        "source": source,
        "session_id": incoming.get("session_id"),
        "input": content,
        "model": incoming.get("model", "gpt-4"),
        "meta": incoming.get("meta", {}),
        "received_at": start_ts,
    }

    try:
        # Check cache if session_id provided
        if incoming.get("session_id") and isinstance(content, str):
            import hashlib
            input_hash = hashlib.sha256(content.encode()).hexdigest()
            cached_result = _get_cached_result(incoming["session_id"], input_hash)
            if cached_result:
                dispatch_latency_ms = (time.time() - start_ts) * 1000.0
                _record_metrics(dispatch_latency_ms, source=source)
                return jsonify({"result": cached_result, "cached": True, "trace_id": trace_id}), 200

        # non-blocking dispatch; tasks should honor trace_id in payload
        task = celery.send_task("tasks.run_inference", args=[canonical])
        dispatch_latency_ms = (time.time() - start_ts) * 1000.0

        # metrics (best-effort)
        _record_metrics(dispatch_latency_ms, source=source)

        log_event(
            service=SERVICE_NAME,
            event="task_dispatched",
            status="info",
            message="Inference task dispatched to Celery",
            trace_id=trace_id,
            extra={"task_id": task.id, "dispatch_latency_ms": round(dispatch_latency_ms, 2), "source": source},
        )
        return jsonify({"task_id": task.id, "trace_id": trace_id}), 202

    except Exception as e:
        increment_metric("inference_failures_total")
        capture_exception_safe(e, {"component": "inference_server", "stage": "dispatch", "trace_id": trace_id})
        err = traceback.format_exc()
        log_event(
            service=SERVICE_NAME,
            event="task_dispatch_failed",
            status="error",
            message=str(e),
            trace_id=trace_id,
            extra={"traceback": err},
        )
        return jsonify({"error": "Task dispatch failed", "trace_id": trace_id}), 503

@app.route("/task-status/<task_id>", methods=["GET"])
def get_task_status(task_id):
    """Poll Celery task status."""
    trace_id = get_trace_id()
    try:
        task_result = celery.AsyncResult(task_id)
        state = task_result.state or "UNKNOWN"

        if state == "PENDING":
            response = {"status": "pending"}
        elif state == "SUCCESS":
            response = {"status": "success", "result": task_result.result}
        elif state == "FAILURE":
            response = {"status": "failure", "error": str(task_result.info or "unknown failure")}
        else:
            response = {"status": state}

        log_event(
            service=SERVICE_NAME,
            event="task_status_checked",
            status="info",
            message=f"Checked status for task {task_id}",
            trace_id=trace_id,
            extra={"state": state},
        )
        return jsonify(response), 200

    except Exception as e:
        capture_exception_safe(e, {"component": "inference_server", "stage": "status_check", "trace_id": trace_id})
        log_event(
            service=SERVICE_NAME,
            event="task_status_error",
            status="error",
            message=f"Task status check failed: {e}",
            trace_id=trace_id,
        )
        return jsonify({"error": "Failed to fetch task status", "trace_id": trace_id}), 503

@app.route("/health", methods=["GET"])
def health():
    """Redis/Celery health check."""
    trace_id = get_trace_id()
    try:
        # lightweight broker check — inspect ping
        broker_ok = False
        try:
            insp = celery.control.inspect(timeout=1)
            ping_res = insp.ping()  # may return None if no workers; handle gracefully
            broker_ok = ping_res is not None
        except Exception as e:
            log_event(
                service=SERVICE_NAME,
                event="broker_unavailable",
                status="error",
                message=f"Broker connection failed: {e}",
                trace_id=trace_id,
            )
            return jsonify({"service": SERVICE_NAME, "status": "unhealthy", "trace_id": trace_id}), 503

        # Redis ping for metrics (non-fatal)
        redis_ok = False
        redis_client = get_redis_client()
        if redis_client:
            try:
                pong = safe_redis_operation(lambda: redis_client.ping())
                redis_ok = bool(pong)
            except Exception:
                redis_ok = False

        # Circuit breaker status
        inference_breaker_open = _is_circuit_breaker_open("inference")
        redis_breaker_open = _is_circuit_breaker_open("redis")

        log_event(
            service=SERVICE_NAME,
            event="health_check",
            status="info",
            message="Service healthy (Celery broker connected)",
            trace_id=trace_id,
            extra={
                "redis_metrics": "ok" if redis_ok else "unavailable",
                "inference_circuit_breaker": "open" if inference_breaker_open else "closed",
                "redis_circuit_breaker": "open" if redis_breaker_open else "closed"
            },
        )
        return jsonify({
            "service": SERVICE_NAME, 
            "status": "healthy", 
            "redis_metrics": redis_ok,
            "inference_circuit_breaker": "open" if inference_breaker_open else "closed",
            "redis_circuit_breaker": "open" if redis_breaker_open else "closed",
            "trace_id": trace_id
        }), 200

    except Exception as e:
        capture_exception_safe(e, {"component": "inference_server", "stage": "health_check", "trace_id": trace_id})
        log_event(
            service=SERVICE_NAME,
            event="health_check_failed",
            status="error",
            message=str(e),
            trace_id=trace_id,
        )
        return jsonify({"service": SERVICE_NAME, "status": "unhealthy", "trace_id": trace_id}), 503

# Enhanced health check endpoint
@app.route("/health/detailed", methods=["GET"])
def health_detailed():
    """Detailed health check with component status."""
    trace_id = get_trace_id()
    
    components = {}
    
    # Celery health
    try:
        insp = celery.control.inspect(timeout=1)
        ping_res = insp.ping()
        broker_ok = ping_res is not None
        components["celery"] = {"status": "healthy", "details": "Broker connected"} if broker_ok else {"status": "unhealthy", "details": "No broker response"}
    except Exception as e:
        components["celery"] = {"status": "unhealthy", "details": str(e)}
    
    # Redis health
    redis_ok = False
    redis_client = get_redis_client()
    if redis_client:
        try:
            pong = safe_redis_operation(lambda: redis_client.ping())
            redis_ok = bool(pong)
            components["redis"] = {"status": "healthy", "details": "Connection successful"}
        except Exception as e:
            components["redis"] = {"status": "unhealthy", "details": str(e)}
    else:
        components["redis"] = {"status": "unavailable", "details": "Client not initialized"}
    
    # Circuit breaker status
    components["circuit_breakers"] = {
        "inference": "open" if _is_circuit_breaker_open("inference") else "closed",
        "redis": "open" if _is_circuit_breaker_open("redis") else "closed"
    }
    
    # Sentry status (if available)
    try:
        from sentry_utils import get_sentry_status
        components["sentry"] = get_sentry_status()
    except Exception:
        components["sentry"] = {"status": "unknown", "details": "Sentry utils unavailable"}
    
    all_healthy = all(
        comp.get("status") in ("healthy", "closed") 
        for comp in components.values() 
        if isinstance(comp, dict)
    )
    
    status_code = 200 if all_healthy else 503
    overall_status = "healthy" if all_healthy else "degraded"
    
    log_event(
        service=SERVICE_NAME,
        event="detailed_health_check",
        status=overall_status,
        message=f"Detailed health check: {overall_status}",
        trace_id=trace_id,
        extra={"components": components}
    )
    
    return jsonify({
        "service": SERVICE_NAME,
        "status": overall_status,
        "components": components,
        "trace_id": trace_id
    }), status_code

# Global error handling
@app.errorhandler(Exception)
def handle_exception(e):
    trace_id = get_trace_id()
    status_code = 500
    if isinstance(e, HTTPException):
        status_code = e.code
    
    capture_exception_safe(e, {"component": "inference_server", "stage": "request_handling", "trace_id": trace_id})
    log_event(
        service=SERVICE_NAME,
        event="unhandled_exception",
        status="error",
        message=str(e),
        trace_id=trace_id,
    )
    return jsonify({"error": str(e), "trace_id": trace_id}), status_code

# Local debug entry point
if __name__ == "__main__":
    # Initialize the server before running
    initialize_inference_server()
    
    log_event(
        service=SERVICE_NAME,
        event="startup",
        status="info",
        message=f"Starting {SERVICE_NAME} on port {getattr(Config, 'INFERENCE_PORT', 7000)}",
        trace_id=get_trace_id()
    )
    app.run(host="0.0.0.0", port=int(getattr(Config, 'INFERENCE_PORT', 7000)))