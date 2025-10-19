"""
twilio_client.py — Sara AI Core (Phase 11-D)
Unified Twilio REST Client with Observability, Circuit Breaker, and Structured Metrics
"""
__phase__ = "11-D"
__service__ = "twilio_client"
__schema_version__ = "phase_11d_v1"

"""
Features:
- Place outbound Twilio calls via REST API (optional if Twilio SDK installed).
- Provide TwiML endpoints: /twilio/answer, /twilio/events and dynamic /twilio/twiml/play for mid-call <Play>.
- Upload TTS audio bytes to Render static storage (/public/audio/) and return public URL via RENDER_EXTERNAL_HOST.
- Dispatch Celery tasks for TTS, inference, and event logging. Configurable task names.
- Redis-backed call state for traceable call lifecycle.
- Structured logging with trace_id via logging_utils.log_event.
- E.164 basic phone validation (no external libs).
- No AWS/S3/CDN references — Render-only storage.
"""

import os
import re
import json
import uuid
import traceback
import time
from typing import Optional, Dict, Any

from flask import Blueprint, request, Response, jsonify

# Celery app and optional task callables
from celery_app import celery
try:
    from tasks import run_tts, run_inference
except Exception:
    run_tts = None
    run_inference = None

from logging_utils import log_event, get_trace_id

# Phase 11-D Observability imports
try:
    from sentry_utils import init_sentry, capture_exception_safe
    from metrics_collector import increment_metric, observe_latency
    from global_metrics_store import start_background_sync
    from redis_client import get_redis_client, safe_redis_operation
    from config import Config
except Exception as e:
    # Fallback for missing Phase 11-D modules
    def capture_exception_safe(*args, **kwargs): pass
    def increment_metric(*args, **kwargs): pass
    def observe_latency(*args, **kwargs): pass
    def start_background_sync(*args, **kwargs): pass
    def get_redis_client(): return None
    def safe_redis_operation(operation, fallback=None): 
        try: return operation() 
        except: return fallback
    class Config:
        TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
        TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")

# Initialize Phase 11-D observability backbone
init_sentry()
start_background_sync(service_name=__service__)

# Twilio SDK optional
try:
    from twilio.rest import Client as TwilioClient
    from twilio.twiml.voice_response import VoiceResponse, Start, Stream, Play, Say
    from twilio.base.exceptions import TwilioRestException
    TWILIO_SDK_AVAILABLE = True
except Exception:
    TWILIO_SDK_AVAILABLE = False
    TwilioRestException = Exception  # fallback

# --------------------------------------------------------------------------
# Configuration (env-driven)
# --------------------------------------------------------------------------
SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "twilio_client")
SARA_ENV = os.getenv("SARA_ENV", "development")

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER", "")
TWILIO_ANSWER_URL = os.getenv("TWILIO_ANSWER_URL", "")  # public endpoint Twilio fetches for TwiML on answer
TWILIO_MEDIA_WS_URL = os.getenv("TWILIO_MEDIA_WS_URL", "")  # optional WebSocket media stream
TWILIO_DYNAMIC_TWIML_URL = os.getenv("TWILIO_DYNAMIC_TWIML_URL", "")  # optional override for dynamic twiml play

# Celery task names (make sure they match Phase 8 celery task definitions)
TTS_TASK_NAME = os.getenv("TTS_TASK_NAME", "run_tts")
INFERENCE_TASK_NAME = os.getenv("INFERENCE_TASK_NAME", "run_inference")
EVENT_TASK_NAME = os.getenv("EVENT_TASK_NAME", "celery_tasks.log_twilio_event")

# Render static host used to build public audio URLs
RENDER_EXTERNAL_HOST = os.getenv("RENDER_EXTERNAL_HOST", "")  # e.g. "sara-ai.example.com"
PUBLIC_AUDIO_DIR = os.getenv("PUBLIC_AUDIO_DIR", "public/audio")
CALL_STATE_TTL = int(os.getenv("CALL_STATE_TTL", 60 * 60 * 4))  # seconds (4 hours)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# --------------------------------------------------------------------------
# Phase 11-D Circuit Breaker
# --------------------------------------------------------------------------
def _is_circuit_breaker_open(service: str = "twilio_client") -> bool:
    """Check if circuit breaker is open for Twilio operations"""
    try:
        client = get_redis_client()
        if not client:
            return False
        state = safe_redis_operation(lambda: client.get(f"circuit_breaker:{service}:state"))
        if not state:
            return False
        if isinstance(state, bytes):
            state = state.decode("utf-8")
        return state.lower() == "open"
    except Exception:
        return False

# --------------------------------------------------------------------------
# Phase 11-D Metrics and Observability
# --------------------------------------------------------------------------
def _record_metrics(event_type: str, status: str, latency_ms: float = None, trace_id: str = None):
    """Record metrics for Twilio operations"""
    try:
        increment_metric(f"twilio_client_{event_type}_{status}_total")
        if latency_ms is not None:
            observe_latency(f"twilio_client_{event_type}_latency_seconds", latency_ms / 1000.0)
    except Exception:
        pass

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
# Clients & helpers
# --------------------------------------------------------------------------
_twilio_client = None

def _init_twilio_client():
    global _twilio_client
    if _twilio_client is not None:
        return _twilio_client
    if not TWILIO_SDK_AVAILABLE:
        _structured_log("twilio_sdk_missing", level="ERROR",
                      message="twilio SDK not installed; Twilio operations disabled")
        return None
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN):
        _structured_log("twilio_credentials_missing", level="ERROR",
                      message="Twilio credentials missing in environment")
        return None
    _twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    return _twilio_client

# Phase 11-D: Updated Redis client to use safe operations
_redis_client = None

def _get_redis():
    global _redis_client
    if _redis_client is None:
        try:
            # Try Phase 11-D Redis client first, fallback to direct connection
            _redis_client = get_redis_client()
            if _redis_client is None:
                import redis
                _redis_client = redis.Redis.from_url(REDIS_URL, socket_connect_timeout=3)
        except Exception as e:
            _structured_log("redis_init_failed", level="ERROR",
                          message=str(e), extra={"redis_url_preview": REDIS_URL.split("//")[-1]})
            _redis_client = None
    return _redis_client

# Basic E.164 check (conservative)
_e164_re = re.compile(r"^\+\d{7,15}$")

def _validate_e164(number: str) -> bool:
    if not number or not isinstance(number, str):
        return False
    return bool(_e164_re.fullmatch(number))

# Ensure public audio directory exists (Render container has writable FS for build-time; Render static needs uploaded files at deploy time).
def _ensure_public_audio_dir():
    try:
        os.makedirs(PUBLIC_AUDIO_DIR, exist_ok=True)
        return True
    except Exception as e:
        _structured_log("public_audio_dir_failed", level="ERROR", message=str(e))
        return False

# Save bytes to Render static path and return public URL built from RENDER_EXTERNAL_HOST
def upload_bytes_to_public_storage(bytes_obj: bytes, filename: str) -> Optional[str]:
    """
    Render-only implementation: write to PUBLIC_AUDIO_DIR and return a public URL using RENDER_EXTERNAL_HOST.
    NOTE: Ensure Render serves the `public/` directory at the root of the site (configure static files on your Render service).
    """
    trace_id = get_trace_id()
    if not _ensure_public_audio_dir():
        return None

    safe_filename = os.path.basename(filename)
    file_path = os.path.join(PUBLIC_AUDIO_DIR, safe_filename)
    try:
        with open(file_path, "wb") as f:
            f.write(bytes_obj)
        if not RENDER_EXTERNAL_HOST:
            _structured_log("render_external_host_missing", level="ERROR",
                          message="RENDER_EXTERNAL_HOST not set; cannot build public audio URL", trace_id=trace_id)
            return None
        public_url = f"https://{RENDER_EXTERNAL_HOST.rstrip('/')}/{PUBLIC_AUDIO_DIR.rstrip('/')}/{safe_filename}"
        _structured_log("audio_uploaded", message="Audio saved to public storage",
                      trace_id=trace_id, extra={"path": file_path, "public_url": public_url})
        return public_url
    except Exception as e:
        _structured_log("audio_upload_failed", level="ERROR",
                      message=str(e), trace_id=trace_id, extra={"file_path": file_path, "traceback": traceback.format_exc()})
        return None

def get_public_audio_url(local_path: Optional[str] = None, audio_bytes: Optional[bytes] = None, filename: Optional[str] = None) -> Optional[str]:
    """
    Resolve/generate a public HTTPS URL for Twilio to fetch.
    - If local_path is already an http(s) URL, return it.
    - If audio_bytes provided, upload to Render static and return URL.
    - If local_path is a local file path, upload and return URL.
    """
    if local_path and isinstance(local_path, str) and local_path.startswith(("http://", "https://")):
        return local_path

    if audio_bytes and filename:
        return upload_bytes_to_public_storage(audio_bytes, filename)

    if local_path and os.path.exists(local_path):
        try:
            with open(local_path, "rb") as f:
                data = f.read()
            name = filename or os.path.basename(local_path)
            return upload_bytes_to_public_storage(data, name)
        except Exception as e:
            _structured_log("get_public_audio_failed", level="ERROR",
                          message=str(e), extra={"local_path": local_path})
            return None
    return None

# --------------------------------------------------------------------------
# Redis call-state helpers (Updated with Phase 11-D safe operations)
# --------------------------------------------------------------------------
def _store_call_state(call_sid: str, trace_id: str, state: Dict[str, Any]) -> None:
    r = _get_redis()
    if not r:
        return
    try:
        key = f"call:{call_sid}"
        payload = {"trace_id": trace_id, **state}
        safe_redis_operation(lambda: r.set(key, json.dumps(payload), ex=CALL_STATE_TTL))
    except Exception as e:
        _structured_log("redis_set_failed", level="WARNING", message=str(e))

def _get_call_state(call_sid: str) -> Optional[Dict[str, Any]]:
    r = _get_redis()
    if not r:
        return None
    try:
        key = f"call:{call_sid}"
        raw = safe_redis_operation(lambda: r.get(key))
        if not raw:
            return None
        return json.loads(raw)
    except Exception as e:
        _structured_log("redis_get_failed", level="WARNING", message=str(e))
        return None

def _delete_call_state(call_sid: str) -> None:
    r = _get_redis()
    if not r:
        return
    try:
        key = f"call:{call_sid}"
        safe_redis_operation(lambda: r.delete(key))
    except Exception as e:
        _structured_log("redis_delete_failed", level="WARNING", message=str(e))

# --------------------------------------------------------------------------
# Task dispatch wrappers (safe, honor configured names)
# --------------------------------------------------------------------------
def _dispatch_tts(payload: Dict[str, Any]) -> Dict[str, Any]:
    trace_id = payload.get("trace_id") or get_trace_id()
    try:
        if run_tts is not None:
            task = run_tts.apply_async(args=[payload])
        else:
            task = celery.send_task(TTS_TASK_NAME, args=[payload], kwargs={"trace_id": trace_id})
        _structured_log("tts_task_dispatched", message="TTS task dispatched",
                      trace_id=trace_id, extra={"task_id": getattr(task, "id", None)})
        return {"task_id": getattr(task, "id", None), "trace_id": trace_id}
    except Exception as e:
        _structured_log("tts_dispatch_failed", level="ERROR", message=str(e),
                      trace_id=trace_id, extra={"traceback": traceback.format_exc()})
        raise

def _dispatch_inference(payload: Dict[str, Any]) -> Dict[str, Any]:
    trace_id = payload.get("trace_id") or get_trace_id()
    try:
        if run_inference is not None:
            task = run_inference.apply_async(args=[payload])
        else:
            task = celery.send_task(INFERENCE_TASK_NAME, args=[payload], kwargs={"trace_id": trace_id})
        _structured_log("inference_task_dispatched", message="Inference task dispatched",
                      trace_id=trace_id, extra={"task_id": getattr(task, "id", None)})
        return {"task_id": getattr(task, "id", None), "trace_id": trace_id}
    except Exception as e:
        _structured_log("inference_dispatch_failed", level="ERROR", message=str(e),
                      trace_id=trace_id, extra={"traceback": traceback.format_exc()})
        raise

# --------------------------------------------------------------------------
# Twilio client init (optional)
# --------------------------------------------------------------------------
def _twilio_client_init_safe():
    client = _init_twilio_client()
    if client is None:
        _structured_log("twilio_init_unavailable", level="WARNING",
                      message="Twilio client unavailable for REST operations")
    return client

# --------------------------------------------------------------------------
# Outbound call orchestration (Updated with Phase 11-D observability)
# --------------------------------------------------------------------------
def place_outbound_call(to_number: str, from_number: Optional[str] = None,
                        initial_payload: Optional[Dict[str, Any]] = None,
                        trace_id: Optional[str] = None, pre_generate_tts: bool = False) -> Dict[str, Any]:
    """
    Place an outbound call via Twilio REST API (if available).
    Returns {ok: bool, call_sid, trace_id, error?}
    """
    trace_id = trace_id or get_trace_id()
    from_number = from_number or TWILIO_PHONE_NUMBER
    payload = initial_payload or {}

    # Phase 11-D: Circuit breaker check
    if _is_circuit_breaker_open("twilio_client"):
        _structured_log("circuit_breaker_blocked", level="WARNING", 
                      message="Call blocked by circuit breaker", trace_id=trace_id)
        increment_metric("twilio_client_circuit_breaker_hits_total")
        return {"ok": False, "error": "circuit_breaker_open", "trace_id": trace_id}

    if not _validate_e164(to_number):
        _structured_log("invalid_phone_number", level="ERROR",
                      message=f"Invalid E.164: {to_number}", trace_id=trace_id)
        return {"ok": False, "error": "invalid_phone_number", "trace_id": trace_id}

    _structured_log("call_initiation_requested", message=f"Placing outbound call to {to_number}",
                  trace_id=trace_id, extra={"from": from_number})

    client = _twilio_client_init_safe()
    if client is None:
        return {"ok": False, "error": "twilio_client_unavailable", "trace_id": trace_id}

    start_time = time.time()
    try:
        call = client.calls.create(
            to=to_number,
            from_=from_number,
            url=TWILIO_ANSWER_URL or None,
            timeout=int(os.getenv("TWILIO_CALL_TIMEOUT", "60"))
        )
        call_sid = getattr(call, "sid", None)
        
        # Phase 11-D: Record metrics and structured log
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("place_call", "success", latency_ms, trace_id)
        _structured_log("call_placed", message="Call placed", trace_id=trace_id,
                      extra={"call_sid": call_sid, "latency_ms": latency_ms})

        _store_call_state(call_sid, trace_id, {"status": "placed", "to": to_number, "from": from_number})

        if pre_generate_tts:
            try:
                _dispatch_tts({**payload, "trace_id": trace_id})
            except Exception:
                # helper logged details
                pass

        return {"ok": True, "call_sid": call_sid, "trace_id": trace_id}
    except TwilioRestException as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("place_call", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id})
        _structured_log("call_place_twilio_error", level="ERROR",
                      message=str(e), trace_id=trace_id, extra={"traceback": traceback.format_exc(), "latency_ms": latency_ms})
        return {"ok": False, "error": "twilio_api_error", "detail": str(e), "trace_id": trace_id}
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("place_call", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id})
        _structured_log("call_place_error", level="ERROR",
                      message=str(e), trace_id=trace_id, extra={"traceback": traceback.format_exc(), "latency_ms": latency_ms})
        return {"ok": False, "error": "call_place_failed", "trace_id": trace_id}

# --------------------------------------------------------------------------
# TwiML helpers and Flask blueprint
# --------------------------------------------------------------------------
def make_answer_twiml(trace_id: str, play_audio_url: Optional[str] = None, start_media_stream: bool = False) -> str:
    vr = VoiceResponse()
    if start_media_stream and TWILIO_MEDIA_WS_URL:
        try:
            start = Start()
            stream = Stream(url=TWILIO_MEDIA_WS_URL)
            start.append(stream)
            vr.append(start)
        except Exception as e:
            _structured_log("twiml_stream_error", level="ERROR", message=str(e), trace_id=trace_id)
    if play_audio_url:
        vr.play(play_audio_url)
    else:
        vr.say("Connecting you now. Please hold.", voice="alice")
    return str(vr)

def make_play_twiml(audio_url: str) -> str:
    vr = VoiceResponse()
    vr.play(audio_url)
    return str(vr)

twilio_bp = Blueprint("twilio_client", __name__, url_prefix="/twilio")

@twilio_bp.route("/answer", methods=["GET", "POST"])
def twilio_answer():
    form = request.values.to_dict()
    call_sid = form.get("CallSid")
    from_number = form.get("From")
    to_number = form.get("To")
    trace_id = form.get("trace_id") or get_trace_id()

    call_state = _get_call_state(call_sid) if call_sid else None
    start_stream = bool(TWILIO_MEDIA_WS_URL)
    play_url = None
    if call_state and call_state.get("last_audio"):
        play_url = get_public_audio_url(local_path=call_state.get("last_audio"))

    twiml = make_answer_twiml(trace_id=trace_id, play_audio_url=play_url, start_media_stream=start_stream)

    if call_sid:
        _store_call_state(call_sid, trace_id, {"status": "answered", "last_audio": call_state.get("last_audio") if call_state else None})

    _structured_log("answer_provided", message="Provided TwiML on answer",
                  trace_id=trace_id, extra={"call_sid": call_sid, "to": to_number, "from": from_number})
    return Response(twiml, mimetype="application/xml")

@twilio_bp.route("/events", methods=["POST"])
def twilio_events():
    form = request.values.to_dict()
    call_sid = form.get("CallSid") or form.get("callSid")
    call_status = form.get("CallStatus") or form.get("CallStatus")
    trace_id = form.get("trace_id") or get_trace_id()

    _structured_log("twilio_event_received", message=f"Event {call_status}",
                  trace_id=trace_id, extra={"call_sid": call_sid, "payload": form})

    if call_sid:
        _store_call_state(call_sid, trace_id, {"status": call_status})

    # Dispatch external event processing task
    try:
        celery.send_task(EVENT_TASK_NAME, args=[form], kwargs={"trace_id": trace_id})
    except Exception:
        try:
            _dispatch_inference({"event": "twilio_event", "payload": form, "trace_id": trace_id})
        except Exception:
            _structured_log("event_dispatch_failed", level="ERROR",
                          message="Failed to dispatch twilio event processing task", trace_id=trace_id,
                          extra={"traceback": traceback.format_exc()})

    if call_status in ("completed", "canceled", "failed", "no-answer", "busy"):
        if call_sid:
            _delete_call_state(call_sid)

    return ("", 204)

@twilio_bp.route("/twiml/play", methods=["GET"])
def twiml_play():
    audio_url = request.args.get("audio_url")
    if not audio_url or not audio_url.startswith(("http://", "https://")):
        _structured_log("twiml_play_invalid", level="WARNING", message="Missing/invalid audio_url")
        return jsonify({"error": "invalid audio_url"}), 400
    twiml = make_play_twiml(audio_url)
    return Response(twiml, mimetype="application/xml")

# --------------------------------------------------------------------------
# Play audio on call helper (dynamic TwiML update) - Updated with Phase 11-D observability
# --------------------------------------------------------------------------
def play_audio_on_call(call_sid: str, local_audio_path: Optional[str] = None,
                       audio_bytes: Optional[bytes] = None, filename: Optional[str] = None,
                       trace_id: Optional[str] = None) -> Dict[str, Any]:
    trace_id = trace_id or get_trace_id()
    
    # Phase 11-D: Circuit breaker check
    if _is_circuit_breaker_open("twilio_client"):
        _structured_log("circuit_breaker_blocked", level="WARNING", 
                      message="Play audio blocked by circuit breaker", trace_id=trace_id)
        increment_metric("twilio_client_circuit_breaker_hits_total")
        return {"ok": False, "error": "circuit_breaker_open", "trace_id": trace_id}

    public_url = get_public_audio_url(local_path=local_audio_path, audio_bytes=audio_bytes, filename=filename)
    if not public_url:
        _structured_log("play_audio_no_public_url", level="ERROR",
                      message="No public audio URL available", trace_id=trace_id)
        return {"ok": False, "error": "no_public_url", "trace_id": trace_id}

    client = _twilio_client_init_safe()
    if client is None:
        return {"ok": False, "error": "twilio_client_unavailable", "trace_id": trace_id}

    start_time = time.time()
    try:
        dynamic_url = TWILIO_DYNAMIC_TWIML_URL or (os.getenv("PUBLIC_BASE_URL", "").rstrip("/") + f"/twilio/twiml/play?audio_url={public_url}")
        if not dynamic_url:
            _structured_log("no_dynamic_twiml_config", level="ERROR",
                          message="No dynamic TwiML URL configured", trace_id=trace_id)
            return {"ok": False, "error": "no_dynamic_twiml", "trace_id": trace_id}

        call = client.calls(call_sid).update(url=dynamic_url)
        
        # Phase 11-D: Record metrics and structured log
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("play_audio", "success", latency_ms, trace_id)
        _structured_log("play_audio_invoked", message="Instructed Twilio to play audio",
                      trace_id=trace_id, extra={"call_sid": call_sid, "audio_url": public_url, "latency_ms": latency_ms})
        
        _store_call_state(call_sid, trace_id, {"last_audio": public_url})
        return {"ok": True, "call_sid": call_sid, "trace_id": trace_id}
    except TwilioRestException as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("play_audio", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id})
        _structured_log("play_audio_twilio_error", level="ERROR",
                      message=str(e), trace_id=trace_id, extra={"traceback": traceback.format_exc(), "latency_ms": latency_ms})
        return {"ok": False, "error": "twilio_api_error", "trace_id": trace_id}
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("play_audio", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id})
        _structured_log("play_audio_error", level="ERROR",
                      message=str(e), trace_id=trace_id, extra={"traceback": traceback.format_exc(), "latency_ms": latency_ms})
        return {"ok": False, "error": "play_failed", "trace_id": trace_id}

# --------------------------------------------------------------------------
# Phase 11-D Health Check Endpoint
# --------------------------------------------------------------------------
@twilio_bp.route("/health/twilio", methods=["GET"])
def health_check():
    """Phase 11-D health check endpoint with Redis ping + circuit breaker state"""
    trace_id = get_trace_id()
    start_time = time.time()
    try:
        redis_ok = safe_redis_operation(lambda: _get_redis().ping() if _get_redis() else False, fallback=False)
        breaker_open = _is_circuit_breaker_open("twilio_client")
        status = "healthy" if redis_ok and not breaker_open else "degraded"
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "success", latency_ms, trace_id)
        
        return {
            "service": __service__,
            "status": status,
            "redis_ok": redis_ok,
            "breaker_open": breaker_open,
            "phase": __phase__,
            "schema_version": __schema_version__,
            "trace_id": trace_id
        }, 200
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id})
        return {
            "service": __service__,
            "status": "unhealthy",
            "error": str(e),
            "phase": __phase__,
            "schema_version": __schema_version__,
            "trace_id": trace_id
        }, 500

# --------------------------------------------------------------------------
# Exports
# --------------------------------------------------------------------------
__all__ = [
    "place_outbound_call",
    "play_audio_on_call",
    "twilio_bp",
    "twiml_play",
    "twilio_answer",
    "twilio_events",
    "health_check",  # Phase 11-D addition
]

# End of file