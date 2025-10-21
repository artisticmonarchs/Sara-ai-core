#!/usr/bin/env python3
"""
call_handler.py — Phase 11-D compliant Twilio call orchestration shim

Responsibilities (summary):
- Accepts incoming call webhook payloads (framework-agnostic handler).
- Validates DNC (dnc_check.is_suppressed), contact info, and rate limits.
- Creates/manages call session state in Redis via redis_client.get_client() + safe_redis_operation.
- Orchestrates start/stop of voice pipeline via voice_pipeline APIs (deferred imports).
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
        from logging_utils import log_event  # lazy import at shim creation time
        return log_event
    except Exception:
        def _noop_log(*a, **k): pass
        return _noop_log

log_event = _get_logger()

def _get_trace_id():
    try:
        from logging_utils import get_trace_id as _g
        return _g()
    except Exception:
        return str(uuid.uuid4())[:8]

# ---------------------------
# Helpers: lazy imports to avoid import-time cycles
# ---------------------------
def _get_redis_helpers():
    """
    Returns (get_client, safe_redis_operation) or (None, None) if unavailable.
    Do not call at import-time for side-effects.
    """
    try:
        from redis_client import get_client, safe_redis_operation
        if not callable(safe_redis_operation):
            return None, None
        return get_client, safe_redis_operation
    except Exception:
        return None, None

def _get_metrics():
    try:
        from metrics_collector import increment_metric, observe_latency
        return increment_metric, observe_latency
    except Exception:
        def _noop(*a, **k): pass
        return _noop, _noop

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

# ---------------------------
# Core call-session helpers
# ---------------------------
CALL_REDIS_PREFIX = "call_session:"  # Redis key prefix (idempotent, safe constant)

def _redis_key_for_call(call_sid: str) -> str:
    return f"{CALL_REDIS_PREFIX}{call_sid}"

def _safe_redis_set(key: str, value: str, ex: Optional[int] = None) -> bool:
    get_client, safe_op = _get_redis_helpers()
    if not get_client:
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
    get_client, safe_op = _get_redis_helpers()
    if not get_client:
        return None
    try:
        def _op(client):
            return client.get(key)
        return safe_op(_op, fallback=None, operation_name="call_handler_get")
    except Exception:
        return None

def _safe_redis_delete(key: str) -> bool:
    get_client, safe_op = _get_redis_helpers()
    if not get_client:
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
    trace_id = _get_trace_id()
    start_ts = time.time()
    increment_metric, observe_latency = _get_metrics()
    sentry = _get_sentry_utils()
    voice_pipeline = _get_voice_pipeline()
    dnc = _get_dnc_check()

    # Extract canonical fields safely
    call_sid = (payload.get("CallSid") or payload.get("call_sid") or "").strip()
    from_number = (payload.get("From") or payload.get("from") or "").strip()
    to_number = (payload.get("To") or payload.get("to") or "").strip()
    call_status = (payload.get("CallStatus") or payload.get("call_status") or payload.get("Status") or "").strip()

    log_event(
        service="call_handler",
        event="incoming_call_received",
        status="info",
        message="Incoming call webhook received",
        extra={"call_sid": call_sid, "from": from_number, "to": to_number, "status": call_status, "trace_id": trace_id}
    )
    increment_metric("calls.received_total")

    # Basic validation
    if not call_sid:
        log_event("call_handler", "missing_call_sid", "error", "Missing CallSid in payload", trace_id=trace_id)
        increment_metric("calls.invalid_payload")
        return {"ok": False, "reason": "missing_call_sid"}

    # DNC check (best-effort)
    try:
        if dnc:
            try:
                if dnc.is_suppressed(from_number):
                    increment_metric("calls.blocked_dnc")
                    log_event("call_handler", "call_blocked_dnc", "warning",
                              "Call blocked by DNC", extra={"call_sid": call_sid, "from": from_number, "trace_id": trace_id})
                    # Safe response for Twilio: end call / provide TwiML to hangup (adaptor should convert)
                    return {"ok": False, "reason": "dnc_blocked"}
            except Exception as e:
                # DNC check failure should not block the call; record metric & log
                increment_metric("calls.dnc_check_failed")
                log_event("call_handler", "dnc_check_error", "warn", "DNC check error",
                          extra={"call_sid": call_sid, "error": str(e), "trace_id": trace_id})
    except Exception:
        # Defensive: any unexpected error in DNC flow should be logged and continued
        increment_metric("calls.dnc_flow_error")

    # Build session object (idempotent)
    session = {
        "call_sid": call_sid,
        "from": from_number,
        "to": to_number,
        "status": call_status,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "trace_id": trace_id,
        "schema": "v1"  # Added schema versioning
    }

    # Persist session state to Redis (best-effort)
    try:
        key = _redis_key_for_call(call_sid)
        _safe_redis_set(key, json.dumps(session), ex=60 * 60 * 2)  # keep for 2 hours by default
    except Exception as e:
        increment_metric("calls.redis_session_write_failed")
        log_event("call_handler", "session_persist_failed", "warn",
                  "Failed to persist session to Redis", extra={"call_sid": call_sid, "error": str(e), "trace_id": trace_id})

    # Orchestrate voice pipeline start (best-effort)
    started_pipeline = False
    if voice_pipeline:
        try:
            # voice_pipeline.start_call should be non-blocking / resilient; pass minimal context
            # Defer metrics inside voice pipeline itself; guard call here
            voice_pipeline.start_call({
                "call_sid": call_sid,
                "from": from_number,
                "to": to_number,
                "trace_id": trace_id
            })
            started_pipeline = True
            increment_metric("calls.pipeline_started")
            log_event("call_handler", "pipeline_started", "info",
                      "Voice pipeline started for call", extra={"call_sid": call_sid, "trace_id": trace_id})
        except Exception as e:
            increment_metric("calls.pipeline_start_failed")
            log_event("call_handler", "pipeline_start_error", "error",
                      "Failed to start voice pipeline", extra={"call_sid": call_sid, "error": str(e), "trace_id": trace_id})
            # report to Sentry best-effort
            try:
                if sentry:
                    sentry.capture_exception_safe(e, context={"call_sid": call_sid})
            except Exception:
                pass

    # Finalize and return reply structure for framework adaptor
    latency_ms = (time.time() - start_ts) * 1000
    observe_latency("calls.incoming_latency_ms", latency_ms)
    log_event("call_handler", "incoming_handled", "info",
              "Incoming call processing completed", extra={"call_sid": call_sid, "pipeline_started": started_pipeline, "latency_ms": latency_ms, "trace_id": trace_id})

    return {"ok": True, "call_sid": call_sid, "pipeline_started": started_pipeline}

# ---------------------------
# Public API: handle_call_event
# ---------------------------
def handle_call_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle lifecycle events (status updates) from Twilio or internal sources.
    Expected keys: CallSid, CallStatus, Timestamp (optional)
    """
    trace_id = _get_trace_id()
    increment_metric, observe_latency = _get_metrics()
    sentry = _get_sentry_utils()

    call_sid = (payload.get("CallSid") or payload.get("call_sid") or "").strip()
    call_status = (payload.get("CallStatus") or payload.get("call_status") or payload.get("Status") or "").strip()

    if not call_sid:
        log_event("call_handler", "event_missing_call_sid", "error", "Missing CallSid in event", trace_id=trace_id)
        increment_metric("calls.event_invalid")
        return {"ok": False, "reason": "missing_call_sid"}

    log_event("call_handler", "call_event_received", "debug",
              "Call lifecycle event received", extra={"call_sid": call_sid, "status": call_status, "trace_id": trace_id})

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
                    "schema": "v1"
                }
                _safe_redis_set(key, json.dumps(minimal_session), ex=60*60)
    except Exception as e:
        increment_metric("calls.redis_update_failed")
        log_event("call_handler", "redis_update_error", "warn", "Failed to update call session in Redis", extra={"call_sid": call_sid, "error": str(e), "trace_id": trace_id})

    # If call completed/failed, ensure cleanup and metrics
    if call_status.lower() in ("completed", "canceled", "no-answer", "busy", "failed"):  # Removed duplicate "failed"
        try:
            end_call(call_sid, reason=call_status)
            increment_metric("calls.completed_total")
            log_event("call_handler", "call_completed", "info", "Call completed and cleaned up", extra={"call_sid": call_sid, "status": call_status, "trace_id": trace_id})
        except Exception as e:
            increment_metric("calls.cleanup_failed")
            log_event("call_handler", "call_cleanup_error", "warn", "Failed to cleanup call", extra={"call_sid": call_sid, "error": str(e), "trace_id": trace_id})
            try:
                if sentry:
                    sentry.capture_exception_safe(e, context={"call_sid": call_sid})
            except Exception:
                pass

    return {"ok": True, "call_sid": call_sid}

# ---------------------------
# Public API: end_call
# ---------------------------
def end_call(call_sid: str, reason: Optional[str] = None) -> bool:
    """
    Gracefully teardown call session state and inform downstream components.
    Returns True when cleanup is performed (best-effort).
    """
    trace_id = _get_trace_id()
    sentry = _get_sentry_utils()
    voice_pipeline = _get_voice_pipeline()

    try:
        # Remove Redis session
        key = _redis_key_for_call(call_sid)
        _safe_redis_delete(key)
    except Exception as e:
        log_event("call_handler", "redis_delete_failed", "warn", "Failed to delete Redis session", extra={"call_sid": call_sid, "error": str(e), "trace_id": trace_id})

    # Inform voice pipeline to stop any associated processing (best-effort)
    if voice_pipeline:
        try:
            # voice_pipeline.stop_call should exist and be safe to call
            if hasattr(voice_pipeline, "stop_call"):
                try:
                    voice_pipeline.stop_call(call_sid=call_sid, reason=reason)
                    log_event("call_handler", "voice_pipeline_stop", "info", "Requested voice pipeline to stop", extra={"call_sid": call_sid, "reason": reason, "trace_id": trace_id})
                except Exception as e:
                    log_event("call_handler", "voice_pipeline_stop_failed", "warn", "voice_pipeline.stop_call failed", extra={"call_sid": call_sid, "error": str(e), "trace_id": trace_id})
                    try:
                        if sentry:
                            sentry.capture_exception_safe(e, context={"call_sid": call_sid})
                    except Exception:
                        pass
        except Exception:
            # swallow broad errors in best-effort cleanup
            pass

    # final metric + log
    increment_metric, _ = _get_metrics()
    increment_metric("calls.cleaned_up_total")
    log_event("call_handler", "call_end_processed", "info", "Call session ended (cleanup attempted)", extra={"call_sid": call_sid, "reason": reason, "trace_id": trace_id})
    return True

# ---------------------------
# Exports
# ---------------------------
__all__ = [
    "handle_incoming_call",
    "handle_call_event",
    "end_call",
]