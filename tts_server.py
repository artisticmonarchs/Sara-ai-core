"""
tts_server.py — Phase 11-D Deepgram Integration (Production Standardization)
Handles text-to-speech requests for Sara AI Core with full observability.
"""

import io
import time
import json
from flask import Flask, request, jsonify, send_file

# Phase 11-D Standardized Imports
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
from redis_client import get_client, safe_redis_operation
from sentry_utils import init_sentry, capture_exception_safe
from celery_app import celery

# --------------------------------------------------------------------------
# Initialization (No side effects at import)
# --------------------------------------------------------------------------
app = Flask(__name__)
SERVICE_NAME = "tts_server"

# Redis client for lightweight metrics (lazy initialization)
_redis_client = None

def get_redis_client():
    """Lazy Redis client initialization."""
    global _redis_client
    if _redis_client is None:
        _redis_client = get_client()
    return _redis_client

def initialize_tts_server():
    """Initialize TTS server components - call this at startup."""
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
            message="Background global metrics sync started for tts_server",
            trace_id=get_trace_id()
        )
    except Exception as e:
        log_event(
            service=SERVICE_NAME, 
            event="global_metrics_sync_failed", 
            status="error",
            message="Failed to start background metrics sync",
            trace_id=get_trace_id(),
            extra={"error": str(e)}
        )

# --------------------------------------------------------------------------
# Circuit Breaker Implementation (Canonical Pattern)
# --------------------------------------------------------------------------
def _is_circuit_breaker_open(service: str = "tts") -> bool:
    """Canonical circuit breaker check used across Phase 11-D services."""
    try:
        client = get_redis_client()
        if not client:
            return False
        key = f"circuit_breaker:{service}:state"
        state = safe_redis_operation(lambda: client.get(key))
        if state is None:
            return False
        if isinstance(state, bytes):
            try:
                state = state.decode("utf-8")
            except Exception:
                pass
        return str(state).lower() == "open"
    except Exception:
        # Best-effort: do not block on checker errors
        return False

# --------------------------------------------------------------------------
# Validation & Utility Functions
# --------------------------------------------------------------------------
def validate_payload(payload):
    """Validate TTS payload according to Phase 11-D schema."""
    if not isinstance(payload, dict):
        return False, "Invalid JSON payload"
    if "text" not in payload:
        return False, "Missing required field 'text'"
    if not isinstance(payload["text"], str):
        return False, "Field 'text' must be string"
    if "voice" in payload and not isinstance(payload["voice"], str):
        return False, "Field 'voice' must be string"
    return True, None

def _record_metrics(event_type: str, status: str, latency_ms: float = None, trace_id: str = None):
    """Record standardized metrics for TTS operations (seconds for latency)."""
    try:
        # Counters (best-effort)
        try:
            increment_metric(f"tts_{event_type}_{status}_total")
        except Exception:
            # Some metric backends vary; ignore failures
            pass

        # Histogram / latency: convert ms -> seconds for Prometheus
        if latency_ms is not None:
            try:
                latency_s = float(latency_ms) / 1000.0
                observe_latency(f"tts_{event_type}_latency_seconds", latency_s)
            except Exception:
                # fallback to legacy ms metric if needed
                try:
                    observe_latency(f"tts_{event_type}_latency_ms", float(latency_ms))
                except Exception:
                    pass
    except Exception as e:
        log_event(
            service="tts_server",
            event="metrics_record_failed",
            status="warn",
            message="Failed to record tts metrics",
            trace_id=trace_id or get_trace_id(),
            extra={"error": str(e)}
        )

# --------------------------------------------------------------------------
# Error Handlers
# --------------------------------------------------------------------------
@app.errorhandler(Exception)
def handle_exception(e):
    """Global exception handler for Phase 11-D standardization."""
    trace_id = get_trace_id()
    capture_exception_safe(e, {"service": "tts_server", "trace_id": trace_id})
    
    log_event(
        service="tts_server",
        event="unhandled_exception",
        status="error",
        message=f"Unhandled exception: {str(e)}",
        trace_id=trace_id,
    )
    
    return jsonify({
        "error": "Internal server error",
        "trace_id": trace_id
    }), 500

# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.route("/tts", methods=["POST"])
def tts():
    """
    Handle TTS (text-to-speech) generation requests.
    Dispatches Deepgram TTS via Celery worker with Phase 11-D observability.
    """
    start_time = time.time()
    data = request.get_json(silent=True) or {}
    trace_id = data.get("trace_id") or get_trace_id()
    
    # Circuit breaker check
    if _is_circuit_breaker_open("tts"):
        try:
            increment_metric("tts_circuit_breaker_hits_total")
        except Exception:
            pass
        log_event(
            service="tts_server",
            event="circuit_breaker_blocked",
            status="warn",
            message="TTS request blocked by open circuit breaker",
            trace_id=trace_id,
        )
        return jsonify({
            "error": "Service temporarily unavailable",
            "trace_id": trace_id
        }), 503

    # Payload validation
    is_valid, error_msg = validate_payload(data)
    if not is_valid:
        _record_metrics("requests", "invalid", trace_id=trace_id)
        log_event(
            service="tts_server",
            event="invalid_payload",
            status="error",
            message=f"Payload validation failed: {error_msg}",
            extra={"payload": data},
            trace_id=trace_id,
        )
        return jsonify({"error": error_msg, "trace_id": trace_id}), 400

    text = data.get("text")
    voice = data.get("voice", "aura-2-asteria-en")

    try:
        # Check Redis circuit breaker
        if _is_circuit_breaker_open("redis"):
            log_event(
                service="tts_server",
                event="redis_circuit_breaker_open",
                status="warn",
                message="Redis circuit breaker open, proceeding without cache",
                trace_id=trace_id,
            )

        task_data = {
            "text": text, 
            "voice": voice, 
            "trace_id": trace_id,
            "timestamp": start_time
        }
        
        # Dispatch Celery task
        task_result = celery.send_task("sara_ai.tasks.tts.run_tts", args=[task_data])
        
        dispatch_latency = round((time.time() - start_time) * 1000, 2)
        _record_metrics("dispatch", "success", dispatch_latency, trace_id)
        
        log_event(
            service="tts_server",
            event="tts_dispatched",
            status="info",
            message=f"Deepgram TTS task dispatched (len={len(text)})",
            extra={
                "task_id": task_result.id,
                "voice": voice,
                "dispatch_latency_ms": dispatch_latency
            },
            trace_id=trace_id,
        )

        return jsonify({
            "status": "queued",
            "task_id": task_result.id,
            "trace_id": trace_id
        }), 202

    except Exception as e:
        latency_ms = round((time.time() - start_time) * 1000, 2)
        _record_metrics("dispatch", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": "tts_server", "trace_id": trace_id})
        
        log_event(
            service="tts_server",
            event="dispatch_failed",
            status="error",
            message=f"TTS dispatch failed: {str(e)}",
            extra={"payload": data, "latency_ms": latency_ms},
            trace_id=trace_id,
        )
        return jsonify({
            "error": "Failed to dispatch TTS task",
            "trace_id": trace_id
        }), 500


@app.route("/tts/direct", methods=["POST"])
def direct_tts():
    """
    Direct Deepgram synthesis (sync) — for testing without Celery.
    Enhanced with Phase 11-D observability.
    """
    start_time = time.time()
    data = request.get_json(silent=True) or {}
    trace_id = data.get("trace_id") or get_trace_id()
    
    # Circuit breaker check
    if _is_circuit_breaker_open("tts"):
        try:
            increment_metric("tts_circuit_breaker_hits_total")
        except Exception:
            pass
        log_event(
            service="tts_server",
            event="direct_tts_circuit_breaker_blocked",
            status="warn",
            message="Direct TTS request blocked by open circuit breaker",
            trace_id=trace_id,
        )
        return jsonify({
            "error": "Service temporarily unavailable",
            "trace_id": trace_id
        }), 503

    # Payload validation
    is_valid, error_msg = validate_payload(data)
    if not is_valid:
        _record_metrics("direct_requests", "invalid", trace_id=trace_id)
        return jsonify({"error": error_msg, "trace_id": trace_id}), 400

    text = data.get("text")
    voice = data.get("voice", "aura-2-asteria-en")

    try:
        from deepgram import DeepgramClient, SpeakOptions
        
        dg_client = DeepgramClient(Config.DEEPGRAM_API_KEY)
        speak_opts = SpeakOptions(model=voice)
        response = dg_client.speak.v("1").stream({"text": text}, speak_opts)

        audio_bytes = io.BytesIO()
        for chunk in response.stream:
            audio_bytes.write(chunk)

        if audio_bytes.tell() == 0:
            raise RuntimeError("Deepgram stream returned no audio data.")

        audio_bytes.seek(0)
        duration = round(time.time() - start_time, 2)
        latency_ms = duration * 1000.0

        _record_metrics("synthesis", "success", latency_ms, trace_id)
        try:
            increment_metric("tts_requests_total")
        except Exception:
            pass
        
        log_event(
            service="tts_server",
            event="direct_tts_complete",
            status="info",
            message=f"Direct audio generated ({len(text)} chars, {duration}s)",
            extra={
                "text_length": len(text),
                "synthesis_latency_ms": latency_ms,
                "voice": voice
            },
            trace_id=trace_id,
        )

        return send_file(
            io.BytesIO(audio_bytes.getvalue()),
            mimetype="audio/wav",
            as_attachment=True,
            download_name="sara_tts.wav"
        )

    except Exception as e:
        latency_ms = round((time.time() - start_time) * 1000, 2)
        _record_metrics("synthesis", "failure", latency_ms, trace_id)
        try:
            increment_metric("tts_failures_total")
        except Exception:
            pass
        capture_exception_safe(e, {"service": "tts_server", "trace_id": trace_id})
        
        log_event(
            service="tts_server",
            event="direct_tts_failed",
            status="error",
            message=str(e),
            extra={"latency_ms": latency_ms},
            trace_id=trace_id,
        )
        return jsonify({
            "error": str(e),
            "trace_id": trace_id
        }), 500


@app.route("/task-status/<task_id>", methods=["GET"])
def task_status(task_id):
    """
    Check the status of a Celery TTS task with Phase 11-D safety.
    """
    trace_id = get_trace_id()
    
    try:
        # Check Redis circuit breaker
        if _is_circuit_breaker_open("redis"):
            log_event(
                service="tts_server",
                event="status_check_redis_breaker",
                status="warn",
                message="Redis circuit breaker open during status check",
                extra={"task_id": task_id},
                trace_id=trace_id,
            )
            return jsonify({
                "error": "Cache service temporarily unavailable",
                "trace_id": trace_id
            }), 503

        # Direct Celery AsyncResult (safe)
        try:
            async_result = celery.AsyncResult(task_id)
        except Exception as e:
            capture_exception_safe(e, {"service": "tts_server", "trace_id": trace_id, "task_id": task_id})
            log_event(
                service="tts_server",
                event="task_status_error",
                status="error",
                message="Failed to create Celery AsyncResult",
                extra={"task_id": task_id, "error": str(e)},
                trace_id=trace_id
            )
            return jsonify({"error": "Failed to retrieve task status", "trace_id": trace_id}), 500

        # Derive status/result
        status = getattr(async_result, "status", "UNKNOWN")
        response = {"task_id": task_id, "status": status, "trace_id": trace_id}
        
        if status == "SUCCESS":
            response["result"] = async_result.result
            try:
                async_result.forget()
            except Exception:
                pass

        log_event(
            service="tts_server",
            event="task_status_checked",
            status="info",
            message=f"Task status retrieved: {status}",
            extra={"task_id": task_id, "status": status},
            trace_id=trace_id,
        )
        
        return jsonify(response)

    except Exception as e:
        capture_exception_safe(e, {"service": "tts_server", "trace_id": trace_id})
        log_event(
            service="tts_server",
            event="status_check_failed",
            status="error",
            message=f"Failed to check task status: {str(e)}",
            extra={"task_id": task_id},
            trace_id=trace_id,
        )
        return jsonify({
            "error": "Failed to retrieve task status",
            "trace_id": trace_id
        }), 500


@app.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint with Phase 11-D circuit breaker status.
    """
    trace_id = get_trace_id()
    
    try:
        # Check Redis connectivity with safety wrapper
        redis_client = get_redis_client()
        redis_health = safe_redis_operation(
            lambda: redis_client.ping() if redis_client else False,
            fallback=False,
            operation_name="health_check_ping"
        )
        redis_status = "healthy" if redis_health else "unhealthy"
        
    except Exception as e:
        redis_status = "unhealthy"
        safe_url = Config.REDIS_URL.split("@")[-1] if "@" in Config.REDIS_URL else Config.REDIS_URL
        log_event(
            service="tts_server",
            event="health_check_failed",
            status="error",
            message=f"Redis connection failed at {safe_url}: {str(e)}",
            trace_id=trace_id,
        )

    # Check circuit breaker states
    tts_breaker_open = _is_circuit_breaker_open("tts")
    redis_breaker_open = _is_circuit_breaker_open("redis")
    
    overall_status = "healthy"
    if not redis_health or tts_breaker_open or redis_breaker_open:
        overall_status = "degraded"

    return jsonify({
        "service": "tts_server",
        "status": overall_status,
        "redis": redis_status,
        "circuit_breakers": {
            "tts": "open" if tts_breaker_open else "closed",
            "redis": "open" if redis_breaker_open else "closed"
        },
        "trace_id": trace_id
    })


@app.route("/health/detailed", methods=["GET"])
def detailed_health_check():
    """
    Detailed health check with Sentry status and comprehensive system checks.
    """
    from sentry_utils import get_sentry_status
    trace_id = get_trace_id()
    
    try:
        # Redis health
        redis_client = get_redis_client()
        redis_ok = safe_redis_operation(
            lambda: redis_client.ping() if redis_client else False,
            fallback=False,
            operation_name="detailed_health_ping"
        )
        
        # Celery broker check using proper inspect.ping
        try:
            insp = celery.control.inspect(timeout=1)
            insp_result = insp.ping()
            celery_ok = bool(insp_result)
        except Exception:
            celery_ok = False
        
        # Circuit breaker states
        tts_breaker_open = _is_circuit_breaker_open("tts")
        redis_breaker_open = _is_circuit_breaker_open("redis")
        
        # Sentry status
        sentry_status = get_sentry_status()

        overall_status = "healthy" if celery_ok and redis_ok and not (tts_breaker_open or redis_breaker_open) else "degraded"

        log_event(
            service="tts_server",
            event="health_check",
            status="info" if overall_status == "healthy" else "warn",
            message="Health check completed",
            trace_id=trace_id,
            extra={
                "celery_ok": celery_ok,
                "redis_ok": redis_ok,
                "tts_breaker_open": tts_breaker_open,
                "redis_breaker_open": redis_breaker_open
            }
        )

        return jsonify({
            "service": "tts_server",
            "status": overall_status,
            "trace_id": trace_id,
            "components": {
                "redis": {
                    "status": "healthy" if redis_ok else "unhealthy",
                    "circuit_breaker": "open" if redis_breaker_open else "closed"
                },
                "celery_broker": {
                    "status": "healthy" if celery_ok else "unhealthy"
                },
                "sentry": sentry_status,
                "circuit_breakers": {
                    "tts": "open" if tts_breaker_open else "closed",
                    "redis": "open" if redis_breaker_open else "closed"
                }
            }
        })

    except Exception as e:
        capture_exception_safe(e, {"service": "tts_server", "health_check": "detailed"})
        return jsonify({
            "service": "tts_server",
            "status": "unhealthy",
            "trace_id": trace_id,
            "error": str(e)
        }), 500


# --------------------------------------------------------------------------
# Entrypoint (Render will use Gunicorn)
# --------------------------------------------------------------------------
if __name__ == "__main__":
    # Initialize the server before running
    initialize_tts_server()
    
    port = getattr(Config, 'TTS_PORT', 7001)
    
    log_event(
        service="tts_server",
        event="startup",
        status="info",
        message=f"Starting TTS server on port {port} with Phase 11-D standardization",
        extra={"port": port, "phase": "11-D"}
    )
    
    app.run(host="0.0.0.0", port=port)