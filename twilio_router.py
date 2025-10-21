"""
twilio_router.py — Sara AI Core (Phase 11-D)
Central Twilio routing layer with Observability, Circuit Breaker, and Metrics
"""
__phase__ = "11-D"
__service__ = "twilio_router"
__schema_version__ = "phase_11d_v1"

import time
import traceback
from flask import Flask, request, Response, jsonify, Blueprint

# Phase 11-D: Lazy imports to avoid import-time side effects
def _get_twilio_client():
    try:
        from twilio.rest import Client
        return Client
    except Exception:
        return None

def _get_twilio_exceptions():
    try:
        from twilio.base.exceptions import TwilioRestException
        return TwilioRestException
    except Exception:
        return Exception

# Phase 11-D Observability imports with lazy loading
def _get_observability_modules():
    try:
        from logging_utils import log_event, get_trace_id
        from metrics_collector import increment_metric, observe_latency
        from redis_client import get_redis_client, safe_redis_operation
        from sentry_utils import capture_exception_safe
        from config import Config
        return log_event, get_trace_id, increment_metric, observe_latency, get_redis_client, safe_redis_operation, capture_exception_safe, Config
    except Exception as e:
        # Fallback for missing Phase 11-D modules
        def get_trace_id(): return "unknown-trace"
        def capture_exception_safe(*args, **kwargs): pass
        def increment_metric(*args, **kwargs): pass
        def observe_latency(*args, **kwargs): pass
        def get_redis_client(): return None
        def safe_redis_operation(operation, fallback=None, operation_name=None): 
            try: 
                return operation() 
            except: 
                return fallback
        class Config:
            pass
        def log_event(*args, **kwargs): 
            # Fallback logging
            print(f"[{kwargs.get('service', __service__)}] {kwargs.get('event', 'unknown')}: {kwargs.get('message', '')}")
        return log_event, get_trace_id, increment_metric, observe_latency, get_redis_client, safe_redis_operation, capture_exception_safe, Config

# Initialize observability modules
log_event, get_trace_id, increment_metric, observe_latency, get_redis_client, safe_redis_operation, capture_exception_safe, Config = _get_observability_modules()

# --------------------------------------------------------------------------
# Configuration (Phase 11-D: Only use Config, no direct env access)
# --------------------------------------------------------------------------
def _get_config_values():
    """Get all configuration values from Config with fallbacks"""
    return {
        "TWILIO_ACCOUNT_SID": getattr(Config, "TWILIO_ACCOUNT_SID", None),
        "TWILIO_AUTH_TOKEN": getattr(Config, "TWILIO_AUTH_TOKEN", None),
        "PUBLIC_AUDIO_BASE": getattr(Config, "PUBLIC_AUDIO_BASE", "https://your-domain.com/audio").rstrip("/"),
        "TWILIO_ROUTER_PORT": getattr(Config, "TWILIO_ROUTER_PORT", 8001)
    }

CONFIG = _get_config_values()

# Phase 11-D: Redis client initialization deferred to avoid import-time connections
redis_client = None
def _get_redis_client_safe():
    """Lazy Redis client initialization"""
    global redis_client
    if redis_client is not None:
        return redis_client
    
    try:
        redis_client = get_redis_client()
        if redis_client is None:
            # Fallback only if absolutely necessary
            try:
                from redis import Redis
                redis_url = getattr(Config, "REDIS_URL", "redis://localhost:6379/0")
                redis_client = Redis.from_url(redis_url, decode_responses=True)
            except Exception:
                redis_client = None
    except Exception:
        redis_client = None
    
    return redis_client

# Twilio client - initialize as None, will be set by helper
twilio_client = None

# Phase 11-D: Convert to Blueprint for better isolation
twilio_router_bp = Blueprint("twilio_router", __name__)

# --------------------------------------------------------------------------
# Phase 11-D Twilio Client Initialization Helper
# --------------------------------------------------------------------------
def _init_twilio_client():
    global twilio_client
    if twilio_client:
        return twilio_client
    
    if not CONFIG["TWILIO_ACCOUNT_SID"] or not CONFIG["TWILIO_AUTH_TOKEN"]:
        _structured_log("twilio_credentials_missing", level="warning", message="Twilio creds missing")
        return None
    
    try:
        TwilioClient = _get_twilio_client()
        if TwilioClient is None:
            _structured_log("twilio_library_missing", level="error", message="Twilio library not available")
            return None
        
        twilio_client = TwilioClient(CONFIG["TWILIO_ACCOUNT_SID"], CONFIG["TWILIO_AUTH_TOKEN"])
        return twilio_client
    except Exception as e:
        capture_exception_safe(e, {"service": __service__, "msg": "Twilio init failed"})
        _structured_log("twilio_init_failed", level="error", message=str(e))
        twilio_client = None
        return None

# --------------------------------------------------------------------------
# Phase 11-D Circuit Breaker
# --------------------------------------------------------------------------
def _is_circuit_breaker_open(service: str = "twilio_client") -> bool:
    """Check if circuit breaker is open for Twilio operations"""
    try:
        r = _get_redis_client_safe()
        if not r:
            return False
        state = safe_redis_operation(
            lambda: r.get(f"circuit_breaker:{service}:state"), 
            fallback=None,
            operation_name="get_circuit_breaker_state"
        )
        if not state:
            return False
        if isinstance(state, bytes):
            state = state.decode("utf-8")
        return state.lower() == "open"
    except Exception:
        return False

# --------------------------------------------------------------------------
# Phase 11-D Structured Logging Wrapper
# --------------------------------------------------------------------------
def _structured_log(event: str, level: str = "info", message: str = None, trace_id: str = None, **extra):
    """Structured logging wrapper with Phase 11-D schema"""
    log_event(
        service=__service__,
        event=event,
        status=level,
        message=message or event,
        trace_id=trace_id or get_trace_id(),
        extra={**extra, "schema_version": __schema_version__}
    )

# --------------------------------------------------------------------------
# Phase 11-D Metrics Recording
# --------------------------------------------------------------------------
def _record_metrics(event_type: str, status: str, latency_ms: float = None, trace_id: str = None):
    """Record metrics for router operations"""
    try:
        increment_metric(f"twilio_router_{event_type}_{status}_total")
        if latency_ms is not None:
            observe_latency(f"twilio_router_{event_type}_latency_seconds", latency_ms / 1000.0)
    except Exception:
        pass

# --------------------------------------------------------------------------
# Playback Endpoint (Updated with Phase 11-D Observability)
# --------------------------------------------------------------------------
@twilio_router_bp.route("/twilio/playback", methods=["POST"])
def playback():
    """
    Endpoint to update a Twilio call with <Play> of generated TTS audio.
    Expects JSON payload: { "session_id": str, "trace_id": str }
    """
    start_time = time.time()
    payload = request.get_json(silent=True) or {}
    session_id = payload.get("session_id")
    trace_id = payload.get("trace_id") or get_trace_id()

    # Phase 11-D: Circuit breaker check
    if _is_circuit_breaker_open("twilio_client"):
        _structured_log("circuit_breaker_blocked", level="warning", 
                      message="Playback blocked by circuit breaker", trace_id=trace_id)
        try:
            increment_metric("twilio_router_circuit_breaker_hits_total")
        except Exception:
            pass
        return jsonify({"error": "circuit_breaker_open", "trace_id": trace_id}), 503

    if not session_id or not trace_id:
        _structured_log("playback_missing_fields", level="error",
                      message=f"Missing session_id or trace_id in request: {payload}",
                      trace_id=trace_id, session_id=session_id)
        return jsonify({"error": "Missing session_id or trace_id", "trace_id": trace_id}), 400

    # Sanitize session_id for URL safety
    import os
    safe_session_id = os.path.basename(session_id) if session_id else None
    if not safe_session_id or safe_session_id != session_id:
        _structured_log("playback_invalid_session_id", level="error",
                      message=f"Invalid session_id: {session_id}", trace_id=trace_id)
        return jsonify({"error": "Invalid session_id", "trace_id": trace_id}), 400

    try:
        # Phase 11-D: Safe Redis operation with bytes decoding
        redis_client_instance = _get_redis_client_safe()
        call_sid = safe_redis_operation(
            lambda: redis_client_instance.get(f"twilio_call:{safe_session_id}") if redis_client_instance else None,
            fallback=None,
            operation_name="get_call_sid"
        )
        
        # Decode bytes if needed
        if isinstance(call_sid, bytes):
            try:
                call_sid = call_sid.decode("utf-8")
            except Exception:
                call_sid = str(call_sid)
        
        if not call_sid:
            _structured_log("playback_missing_call_sid", level="error",
                          message=f"No call SID found for session {safe_session_id}",
                          trace_id=trace_id, session_id=safe_session_id)
            return jsonify({"error": "Call SID not found", "trace_id": trace_id}), 404

        # Construct safe audio URL
        audio_url = f"{CONFIG['PUBLIC_AUDIO_BASE']}/{safe_session_id}/{trace_id}.wav"

        # Build TwiML to play audio
        twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Play>{audio_url}</Play>
</Response>"""

        # Initialize Twilio client and update call
        client = _init_twilio_client()
        if client is None:
            _structured_log("twilio_client_unavailable", level="error",
                          message="Twilio client not initialized", trace_id=trace_id)
            try:
                increment_metric("twilio_router_client_unavailable_total")
            except Exception:
                pass
            return jsonify({"error": "Twilio client unavailable", "trace_id": trace_id}), 500

        # Update Twilio call with explicit timeout
        client.calls(call_sid).update(twiml=twiml, timeout=5)

        # Phase 11-D: Record success metrics and structured log
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("playback", "success", latency_ms, trace_id)
        _structured_log("twilio_playback_update", level="info",
                      message=f"Updated Twilio call {call_sid} with audio {audio_url}",
                      trace_id=trace_id, session_id=safe_session_id, call_sid=call_sid, 
                      audio_url=audio_url, latency_ms=latency_ms)

        return jsonify({
            "status": "success", 
            "audio_url": audio_url, 
            "trace_id": trace_id,
            "call_sid": call_sid
        }), 200

    except Exception as e:
        TwilioRestException = _get_twilio_exceptions()
        # Phase 11-D: Exception handling
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("playback", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id, "session_id": safe_session_id})
        
        # Check if it's a Twilio-specific exception
        if isinstance(e, TwilioRestException):
            _structured_log("twilio_playback_twilio_error", level="error",
                          message=str(e), trace_id=trace_id, session_id=safe_session_id,
                          traceback=traceback.format_exc(), latency_ms=latency_ms)
            return jsonify({"error": f"Twilio API error: {str(e)}", "trace_id": trace_id}), 502
        else:
            _structured_log("twilio_playback_error", level="error",
                          message=str(e), trace_id=trace_id, session_id=safe_session_id,
                          traceback=traceback.format_exc(), latency_ms=latency_ms)
            return jsonify({"error": str(e), "trace_id": trace_id}), 500

# --------------------------------------------------------------------------
# Health Check (Updated with Phase 11-D Observability)
# --------------------------------------------------------------------------
@twilio_router_bp.route("/health/twilio_router", methods=["GET"])
def health_check():
    """Phase 11-D health check endpoint with Redis ping + circuit breaker state"""
    start_time = time.time()
    trace_id = get_trace_id()
    
    try:
        # Phase 11-D: Safe Redis ping and circuit breaker check
        redis_client_instance = _get_redis_client_safe()
        redis_ok = safe_redis_operation(
            lambda: redis_client_instance.ping() if redis_client_instance else False, 
            fallback=False,
            operation_name="ping_redis"
        )
        breaker_open = _is_circuit_breaker_open("twilio_client")
        status = "healthy" if redis_ok and not breaker_open else "degraded"
        
        # Record metrics
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "success", latency_ms, trace_id)
        
        _structured_log("health_check", level="info", 
                      message=f"Health check completed: {status}",
                      trace_id=trace_id, redis_ok=redis_ok, breaker_open=breaker_open)
        
        # Return appropriate HTTP status code
        http_code = 200 if status == "healthy" else 503
        
        return jsonify({
            "service": __service__,
            "status": status,
            "redis_ok": redis_ok,
            "breaker_open": breaker_open,
            "phase": __phase__,
            "schema_version": __schema_version__,
            "trace_id": trace_id
        }), http_code
        
    except Exception as e:
        # Phase 11-D: Health check failure handling
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id})
        _structured_log("health_check_failed", level="error",
                      message=str(e), trace_id=trace_id, 
                      traceback=traceback.format_exc())
        
        return jsonify({
            "service": __service__,
            "status": "unhealthy",
            "error": str(e),
            "phase": __phase__,
            "schema_version": __schema_version__,
            "trace_id": trace_id
        }), 503

# --------------------------------------------------------------------------
# Legacy Health Check (Preserved for backward compatibility)
# --------------------------------------------------------------------------
@twilio_router_bp.route("/health", methods=["GET"])
def health_legacy():
    """Legacy health check endpoint for backward compatibility"""
    trace_id = get_trace_id()
    _structured_log("health_legacy_called", level="info", 
                  message="Legacy health endpoint called", trace_id=trace_id)
    return jsonify({"service": __service__, "status": "healthy"}), 200

# --------------------------------------------------------------------------
# Local Debug Entry (Preserved)
# --------------------------------------------------------------------------
if __name__ == "__main__":
    import os
    app = Flask(__name__)
    app.register_blueprint(twilio_router_bp)
    app.run(host="0.0.0.0", port=int(CONFIG["TWILIO_ROUTER_PORT"]))

# --------------------------------------------------------------------------
# Exports
# --------------------------------------------------------------------------
__all__ = [
    "twilio_router_bp", 
    "playback", 
    "health_check",
    "health_legacy"
]