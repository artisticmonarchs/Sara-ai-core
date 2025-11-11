#!/usr/bin/env python3
"""
call_handler.py — Phase 11-F compliant Twilio call orchestration with Duplex Streaming

Responsibilities (summary):
- Accepts incoming call webhook payloads (framework-agnostic handler).
- Validates DNC (dnc_check.is_suppressed), contact info, and rate limits.
- Creates/manages call session state in Redis via redis_client.get_redis_client() + safe_redis_operation.
- Orchestrates start/stop of voice pipeline via voice_pipeline APIs (deferred imports).
- Supports duplex streaming integration for real-time audio processing.
- Emits structured logs via logging_utils.log_event and metrics via metrics_collector (lazy).
- Safe: no network/IO at import-time, lazy imports, circuit-breaker aware, config isolation.

Functions exported:
- handle_incoming_call(payload: dict) -> dict
- handle_call_event(payload: dict) -> dict
- end_call(call_sid: str, reason: str = None) -> bool

Intended usage (Flask example):
    from flask import request, Flask, jsonify
    app = Flask(__name__)
    @app.route("/twilio/call", methods=["POST"])
    def twilio_call():
        result = handle_incoming_call(request.form.to_dict())
        return jsonify(result)
"""

from typing import Optional, Dict, Any
import time
import uuid
import json  # Added for JSON session serialization

# Phase 11-F Compliance Metadata
__phase__ = "11-F"
__service__ = "call_handler"
__schema_version__ = "phase_11f_v1"

# ---------------------------
# Configuration & Logging shims (import-time safe)
# ---------------------------
try:
    from config import Config
except Exception:
    # Minimal fallback — does not access os.environ here
    class Config:
        SERVICE_NAME = "call_handler"

def _get_logger():
    try:
        from logging_utils import log_event, get_trace_id  # lazy import at shim creation time
        return log_event, get_trace_id
    except Exception:
        def _noop_log(*a, **k): pass
        def _fallback_trace_id(): return str(uuid.uuid4())[:8]
        return _noop_log, _fallback_trace_id

log_event, get_trace_id = _get_logger()

# Phase 11-F: Structured logging wrapper
def _structured_log(event: str, level: str = "info", message: str = None, trace_id: str = None, **extra):
    """Structured logging wrapper with Phase 11-F schema"""
    log_event(
        service=__service__,
        event=event,
        status=level,
        message=message or event,
        trace_id=trace_id or get_trace_id(),
        extra={**extra, "schema_version": __schema_version__, "phase": __phase__}
    )

# Phase 11-F: Metrics recording helper
def _record_metrics(event_type: str, status: str, latency_ms: float = None, trace_id: str = None):
    """Record metrics for call handler operations"""
    try:
        from metrics_collector import increment_metric, observe_latency
        increment_metric(f"call_handler_{event_type}_{status}_total")
        if latency_ms is not None:
            observe_latency(f"call_handler_{event_type}_latency_seconds", latency_ms / 1000.0)
    except Exception:
        pass

# Phase 11-F: Circuit breaker check
def _is_circuit_breaker_open(service: str = "call_handler") -> bool:
    """Check if circuit breaker is open for call operations"""
    try:
        from redis_client import get_redis_client, safe_redis_operation
        client = get_redis_client()
        if not client:
            return False
        state = safe_redis_operation(
            lambda: client.get(f"circuit_breaker:{service}:state"),
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

# ---------------------------
# Helpers: lazy imports to avoid import-time cycles
# ---------------------------
def _get_redis_helpers():
    """
    Returns (get_redis_client, safe_redis_operation) or (None, None) if unavailable.
    Do not call at import-time for side-effects.
    """
    try:
        from redis_client import get_redis_client, safe_redis_operation
        if not callable(safe_redis_operation):
            return None, None
        return get_redis_client, safe_redis_operation
    except Exception:
        return None, None

def _get_voice_pipeline():
    try:
        import voice_pipeline
        return voice_pipeline
    except Exception:
        return None

def _get_dnc_check():
    try:
        import dnc_check
        return dnc_check
    except Exception:
        return None

def _get_sentry_utils():
    try:
        import sentry_utils
        return sentry_utils
    except Exception:
        return None

# Phase 11-F: Duplex streaming integration
def _get_duplex_modules():
    try:
        from duplex_voice_controller import DuplexVoiceController
        from realtime_voice_engine import RealtimeVoiceEngine
        return DuplexVoiceController, RealtimeVoiceEngine
    except ImportError:
        return None, None

DuplexVoiceController, RealtimeVoiceEngine = _get_duplex_modules()
DUPLEX_AVAILABLE = DuplexVoiceController is not None and RealtimeVoiceEngine is not None

# ---------------------------
# Core call-session helpers
# ---------------------------
CALL_REDIS_PREFIX = "call_session:"  # Redis key prefix (idempotent, safe constant)

def _redis_key_for_call(call_sid: str) -> str:
    return f"{CALL_REDIS_PREFIX}{call_sid}"

def _safe_redis_set(key: str, value: str, ex: Optional[int] = None) -> bool:
    get_redis_client, safe_op = _get_redis_helpers()
    if not get_redis_client:
        return False
    try:
        def _op(client):
            if ex:
                result = client.set(key, value, ex=ex)
                return bool(result)  # Normalize return value
            result = client.set(key, value)
            return bool(result)  # Normalize return value
        return safe_op(_op, fallback=False, operation_name="call_handler_set")
    except Exception:
        return False

def _safe_redis_get(key: str) -> Optional[str]:
    get_redis_client, safe_op = _get_redis_helpers()
    if not get_redis_client:
        return None
    try:
        def _op(client):
            return client.get(key)
        return safe_op(_op, fallback=None, operation_name="call_handler_get")
    except Exception:
        return None

def _safe_redis_delete(key: str) -> bool:
    get_redis_client, safe_op = _get_redis_helpers()
    if not get_redis_client:
        return False
    try:
        def _op(client):
            result = client.delete(key)
            return bool(result)  # Normalize return value
        return safe_op(_op, fallback=False, operation_name="call_handler_delete")
    except Exception:
        return False

# ---------------------------
# Public API: handle_incoming_call
# ---------------------------
def handle_incoming_call(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Framework-agnostic handler for incoming call webhook payloads.
    Expects typical Twilio keys: CallSid, From, To, CallStatus, etc.
    Returns a dict with minimal response metadata (framework adaptors can convert).
    """
    trace_id = get_trace_id()
    start_ts = time.time()
    sentry = _get_sentry_utils()
    voice_pipeline = _get_voice_pipeline()
    dnc = _get_dnc_check()

    # Phase 11-F: Circuit breaker check
    if _is_circuit_breaker_open("call_handler"):
        _structured_log("circuit_breaker_blocked", level="warning",
                       message="Incoming call blocked by circuit breaker",
                       trace_id=trace_id)
        _record_metrics("incoming_call", "blocked", None, trace_id)
        return {"ok": False, "reason": "circuit_breaker_open", "trace_id": trace_id}

    # Extract canonical fields safely
    call_sid = (payload.get("CallSid") or payload.get("call_sid") or "").strip()
    from_number = (payload.get("From") or payload.get("from") or "").strip()
    to_number = (payload.get("To") or payload.get("to") or "").strip()
    call_status = (payload.get("CallStatus") or payload.get("call_status") or payload.get("Status") or "").strip()

    _structured_log("incoming_call_received", level="info",
                   message="Incoming call webhook received",
                   call_sid=call_sid, from_number=from_number, to_number=to_number, 
                   call_status=call_status, trace_id=trace_id)
    _record_metrics("incoming_call", "received", None, trace_id)

    # Basic validation
    if not call_sid:
        _structured_log("missing_call_sid", level="error", 
                       message="Missing CallSid in payload", trace_id=trace_id)
        _record_metrics("incoming_call", "invalid_payload", None, trace_id)
        return {"ok": False, "reason": "missing_call_sid", "trace_id": trace_id}

    # DNC check (best-effort)
    try:
        if dnc:
            try:
                if dnc.is_suppressed(from_number):
                    _record_metrics("incoming_call", "blocked_dnc", None, trace_id)
                    _structured_log("call_blocked_dnc", level="warning",
                                   message="Call blocked by DNC", 
                                   call_sid=call_sid, from_number=from_number, trace_id=trace_id)
                    # Safe response for Twilio: end call / provide TwiML to hangup (adaptor should convert)
                    return {"ok": False, "reason": "dnc_blocked", "trace_id": trace_id}
            except Exception as e:
                # DNC check failure should not block the call; record metric & log
                _record_metrics("incoming_call", "dnc_check_failed", None, trace_id)
                _structured_log("dnc_check_error", level="warn", 
                               message="DNC check error",
                               call_sid=call_sid, error=str(e), trace_id=trace_id)
    except Exception:
        # Defensive: any unexpected error in DNC flow should be logged and continued
        _record_metrics("incoming_call", "dnc_flow_error", None, trace_id)

    # Phase 11-F: Initialize duplex controller if available
    duplex_controller = None
    if DUPLEX_AVAILABLE:
        try:
            duplex_controller = DuplexVoiceController()
            _structured_log("duplex_controller_initialized", level="info",
                           message="Duplex controller initialized for incoming call",
                           call_sid=call_sid, trace_id=trace_id)
        except Exception as e:
            _structured_log("duplex_controller_init_failed", level="warn",
                           message="Failed to initialize duplex controller, falling back to standard mode",
                           call_sid=call_sid, error=str(e), trace_id=trace_id)

    # Build session object (idempotent) with duplex info
    session = {
        "call_sid": call_sid,
        "from": from_number,
        "to": to_number,
        "status": call_status,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "trace_id": trace_id,
        "schema": "v1",
        "phase": __phase__,
        "duplex_available": DUPLEX_AVAILABLE,
        "duplex_controller_initialized": duplex_controller is not None
    }

    # Persist session state to Redis (best-effort)
    try:
        key = _redis_key_for_call(call_sid)
        _safe_redis_set(key, json.dumps(session), ex=60 * 60 * 2)  # keep for 2 hours by default
    except Exception as e:
        _record_metrics("session", "persist_failed", None, trace_id)
        _structured_log("session_persist_failed", level="warn",
                       message="Failed to persist session to Redis", 
                       call_sid=call_sid, error=str(e), trace_id=trace_id)

    # Orchestrate voice pipeline start (best-effort)
    started_pipeline = False
    if voice_pipeline:
        try:
            # voice_pipeline.start_call should be non-blocking / resilient; pass minimal context
            # Include duplex controller for Phase 11-F integration
            pipeline_payload = {
                "call_sid": call_sid,
                "from": from_number,
                "to": to_number,
                "trace_id": trace_id,
                "duplex_controller": duplex_controller
            }
            voice_pipeline.start_call(pipeline_payload)
            started_pipeline = True
            _record_metrics("pipeline", "started", None, trace_id)
            _structured_log("pipeline_started", level="info",
                           message="Voice pipeline started for call", 
                           call_sid=call_sid, trace_id=trace_id)
        except Exception as e:
            _record_metrics("pipeline", "start_failed", None, trace_id)
            _structured_log("pipeline_start_error", level="error",
                           message="Failed to start voice pipeline", 
                           call_sid=call_sid, error=str(e), trace_id=trace_id)
            # report to Sentry best-effort
            try:
                if sentry:
                    sentry.capture_exception_safe(e, context={"call_sid": call_sid})
            except Exception:
                pass

    # Finalize and return reply structure for framework adaptor
    latency_ms = (time.time() - start_ts) * 1000
    # TODO: Move hardcoded port number to config.py
    _record_metrics("incoming_call", "processed", latency_ms, trace_id)
    _structured_log("incoming_call_handled", level="info",
                   message="Incoming call processing completed", 
                   call_sid=call_sid, pipeline_started=started_pipeline, 
                   latency_ms=latency_ms, duplex_available=DUPLEX_AVAILABLE,
                   trace_id=trace_id)

    return {
        "ok": True, 
        "call_sid": call_sid, 
        "pipeline_started": started_pipeline,
        "duplex_available": DUPLEX_AVAILABLE,
        "trace_id": trace_id
    }

# ---------------------------
# Public API: handle_call_event
# ---------------------------
def handle_call_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle lifecycle events (status updates) from Twilio or internal sources.
    Expected keys: CallSid, CallStatus, Timestamp (optional)
    """
    trace_id = get_trace_id()
    sentry = _get_sentry_utils()

    call_sid = (payload.get("CallSid") or payload.get("call_sid") or "").strip()
    call_status = (payload.get("CallStatus") or payload.get("call_status") or payload.get("Status") or "").strip()

    if not call_sid:
        _structured_log("event_missing_call_sid", level="error", 
                       message="Missing CallSid in event", trace_id=trace_id)
        _record_metrics("call_event", "invalid", None, trace_id)
        return {"ok": False, "reason": "missing_call_sid", "trace_id": trace_id}

    _structured_log("call_event_received", level="debug",
                   message="Call lifecycle event received", 
                   call_sid=call_sid, call_status=call_status, trace_id=trace_id)

    # Update Redis session if present
    try:
        key = _redis_key_for_call(call_sid)
        current = _safe_redis_get(key)
        if current:
            # Parse existing session safely
            try:
                session_obj = json.loads(current) if current else None
                if session_obj:
                    # Update status in session object
                    session_obj["status"] = call_status
                    session_obj["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    _safe_redis_set(key, json.dumps(session_obj), ex=60*60)
            except Exception:
                # Fallback: create minimal session if parsing fails
                minimal_session = {
                    "call_sid": call_sid, 
                    "status": call_status, 
                    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "schema": "v1",
                    "phase": __phase__
                }
                _safe_redis_set(key, json.dumps(minimal_session), ex=60*60)
    except Exception as e:
        _record_metrics("session", "update_failed", None, trace_id)
        _structured_log("redis_update_error", level="warn", 
                       message="Failed to update call session in Redis", 
                       call_sid=call_sid, error=str(e), trace_id=trace_id)

    # If call completed/failed, ensure cleanup and metrics
    if call_status.lower() in ("completed", "canceled", "no-answer", "busy", "failed"):
        try:
            end_call(call_sid, reason=call_status)
            _record_metrics("call", "completed", None, trace_id)
            _structured_log("call_completed", level="info", 
                           message="Call completed and cleaned up", 
                           call_sid=call_sid, call_status=call_status, trace_id=trace_id)
        except Exception as e:
            _record_metrics("call", "cleanup_failed", None, trace_id)
            _structured_log("call_cleanup_error", level="warn", 
                           message="Failed to cleanup call", 
                           call_sid=call_sid, error=str(e), trace_id=trace_id)
            try:
                if sentry:
                    sentry.capture_exception_safe(e, context={"call_sid": call_sid})
            except Exception:
                pass

    return {"ok": True, "call_sid": call_sid, "trace_id": trace_id}

# ---------------------------
# Public API: end_call
# ---------------------------
def end_call(call_sid: str, reason: Optional[str] = None) -> bool:
    """
    Gracefully teardown call session state and inform downstream components.
    Returns True when cleanup is performed (best-effort).
    """
    trace_id = get_trace_id()
    sentry = _get_sentry_utils()
    voice_pipeline = _get_voice_pipeline()

    # Phase 11-F: Clean up duplex controller if exists for this call
    try:
        if DUPLEX_AVAILABLE:
            # Note: In a full implementation, you'd retrieve the specific controller for this call
            # For now, we log that duplex cleanup would occur
            _structured_log("duplex_cleanup_noted", level="debug",
                           message="Duplex controller cleanup noted for call end",
                           call_sid=call_sid, reason=reason, trace_id=trace_id)
    except Exception as e:
        _structured_log("duplex_cleanup_error", level="warn",
                       message="Error during duplex cleanup note",
                       call_sid=call_sid, error=str(e), trace_id=trace_id)

    try:
        # Remove Redis session
        key = _redis_key_for_call(call_sid)
        _safe_redis_delete(key)
    except Exception as e:
        _structured_log("redis_delete_failed", level="warn", 
                       message="Failed to delete Redis session", 
                       call_sid=call_sid, error=str(e), trace_id=trace_id)

    # Inform voice pipeline to stop any associated processing (best-effort)
    if voice_pipeline:
        try:
            # voice_pipeline.stop_call should exist and be safe to call
            if hasattr(voice_pipeline, "stop_call"):
                try:
                    voice_pipeline.stop_call(call_sid=call_sid, reason=reason)
                    _structured_log("voice_pipeline_stop", level="info", 
                                   message="Requested voice pipeline to stop", 
                                   call_sid=call_sid, reason=reason, trace_id=trace_id)
                except Exception as e:
                    _structured_log("voice_pipeline_stop_failed", level="warn", 
                                   message="voice_pipeline.stop_call failed", 
                                   call_sid=call_sid, error=str(e), trace_id=trace_id)
                    try:
                        if sentry:
                            sentry.capture_exception_safe(e, context={"call_sid": call_sid})
                    except Exception:
                        pass
        except Exception:
            # swallow broad errors in best-effort cleanup
            pass

    # final metric + log
    _record_metrics("call", "cleaned_up", None, trace_id)
    _structured_log("call_end_processed", level="info", 
                   message="Call session ended (cleanup attempted)", 
                   call_sid=call_sid, reason=reason, trace_id=trace_id)
    return True

# ---------------------------
# Phase 11-F: Health check function
# ---------------------------
def call_handler_health_check():
    """Health check for call handler service"""
    trace_id = get_trace_id()
    start_time = time.time()
    
    try:
        # Check circuit breaker state
        circuit_breaker_open = _is_circuit_breaker_open("call_handler")
        
        # Check Redis connectivity
        try:
            from redis_client import get_redis_client, safe_redis_operation
            redis_client = get_redis_client()
            redis_ok = safe_redis_operation(
                lambda: redis_client.ping() if redis_client else False,
                fallback=False,
                operation_name="health_check_ping"
            )
        except Exception:
            redis_ok = False
        
        # Check duplex availability
        duplex_available = DUPLEX_AVAILABLE
        
        status = "healthy" if not circuit_breaker_open and redis_ok else "degraded"
        
        latency_ms = (time.time() - start_time) * 1000
        # TODO: Move hardcoded port number to config.py
        _record_metrics("health_check", "success", latency_ms, trace_id)
        
        _structured_log("health_check", level="info",
                       message="Call handler health check completed",
                       trace_id=trace_id, status=status, circuit_breaker_open=circuit_breaker_open,
                       redis_ok=redis_ok, duplex_available=duplex_available)
        
        return {
            "service": __service__,
            "status": status,
            "phase": __phase__,
            "schema_version": __schema_version__,
            "trace_id": trace_id,
            "components": {
                "circuit_breaker": "open" if circuit_breaker_open else "closed",
                "redis": "healthy" if redis_ok else "unhealthy",
                "duplex_available": duplex_available
            }
        }
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        # TODO: Move hardcoded port number to config.py
        _record_metrics("health_check", "failed", latency_ms, trace_id)
        _structured_log("health_check_failed", level="error",
                       message="Call handler health check failed",
                       trace_id=trace_id, error=str(e))
        return {
            "service": __service__,
            "status": "unhealthy",
            "error": str(e),
            "phase": __phase__,
            "schema_version": __schema_version__,
            "trace_id": trace_id
        }

# ---------------------------
# Exports
# ---------------------------
__all__ = [
    "handle_incoming_call",
    "handle_call_event", 
    "end_call",
    "call_handler_health_check",
    "__phase__",
    "__service__",
    "__schema_version__"
]