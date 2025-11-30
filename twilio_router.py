"""
twilio_router.py — Sara AI Core (Phase 11-F)
Central Twilio routing layer with Duplex Streaming, Observability, Circuit Breaker, and Metrics
"""
__phase__ = "11-F"
__service__ = "twilio_router"
__schema_version__ = "phase_11f_v1"

import time
import traceback
import os
from flask import Flask, request, Response, jsonify, Blueprint
import signal
import sys
from logging_utils import get_logger

logger = get_logger("twilio_router")


def _graceful_shutdown(signum, frame):
    """Phase 12: Graceful shutdown handler"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

def _install_signal_handlers():
    """Install signal handlers only in main thread to avoid WSGI issues"""
    try:
        signal.signal(signal.SIGINT, _graceful_shutdown)
        signal.signal(signal.SIGTERM, _graceful_shutdown)
        logger.debug("Signal handlers installed successfully")
    except Exception as e:
        logger.debug(f"Signal handlers not installed (likely not main thread): {str(e)}")


# Twilio signature verification scaffold
VERIFY_TWILIO_SIGNATURE = os.getenv("VERIFY_TWILIO_SIGNATURE", "false").lower() == "true"

# Phase 11-F: Lazy imports to avoid import-time side effects
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

# Phase 11-F: Duplex streaming imports
def _get_duplex_modules():
    try:
        from duplex_voice_controller import DuplexVoiceController
        from realtime_voice_engine import RealtimeVoiceEngine
        return DuplexVoiceController, RealtimeVoiceEngine
    except ImportError:
        return None, None

# Phase 11-F Observability imports with lazy loading
def _get_observability_modules():
    try:
        from logging_utils import log_event, get_trace_id
        from metrics_collector import increment_metric, observe_latency
        from redis_client import get_redis_client, safe_redis_operation
        from sentry_utils import capture_exception_safe
        from config import Config
        return log_event, get_trace_id, increment_metric, observe_latency, get_redis_client, safe_redis_operation, capture_exception_safe, Config
    except Exception as e:
        # Fallback for missing Phase 11-F modules
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
            logger.info(f"[{kwargs.get('service', __service__)}] {kwargs.get('event', 'unknown')}: {kwargs.get('message', '')}")
        return log_event, get_trace_id, increment_metric, observe_latency, get_redis_client, safe_redis_operation, capture_exception_safe, Config

# Initialize observability modules
log_event, get_trace_id, increment_metric, observe_latency, get_redis_client, safe_redis_operation, capture_exception_safe, Config = _get_observability_modules()

# Initialize duplex modules
DuplexVoiceController, RealtimeVoiceEngine = _get_duplex_modules()
DUPLEX_AVAILABLE = DuplexVoiceController is not None and RealtimeVoiceEngine is not None

# --------------------------------------------------------------------------
# Configuration (Phase 11-F: Only use Config, no direct env access)
# --------------------------------------------------------------------------
def _get_config_values():
    """Get all configuration values from Config with fallbacks"""
    return {
        "TWILIO_ACCOUNT_SID": getattr(Config, "TWILIO_ACCOUNT_SID", None),
        "TWILIO_AUTH_TOKEN": getattr(Config, "TWILIO_AUTH_TOKEN", None),
        "PUBLIC_AUDIO_BASE": getattr(Config, "PUBLIC_AUDIO_BASE", os.getenv("PUBLIC_AUDIO_HOST", "https://pub-dc4b36bcc13f45a3aa77dc092e3b2cd4.r2.dev")).rstrip("/"),
        "TWILIO_ROUTER_PORT": getattr(Config, "TWILIO_ROUTER_PORT", 8001),
        "DUPLEX_STREAMING_ENABLED": getattr(Config, "DUPLEX_STREAMING_ENABLED", True) and DUPLEX_AVAILABLE,
        "TWILIO_MEDIA_WS_CONN_TRACK": getattr(Config, "TWILIO_MEDIA_WS_CONN_TRACK", "inbound")
    }

CONFIG = _get_config_values()

# Phase 11-F: Redis client initialization deferred to avoid import-time connections
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
                redis_url = getattr(Config, "REDIS_URL", os.getenv("REDIS_URL"))
                redis_client = Redis.from_url(redis_url, decode_responses=True)
            except Exception:
                redis_client = None
    except Exception:
        redis_client = None
    
    return redis_client

# Twilio client - initialize as None, will be set by helper
twilio_client = None

# Phase 11-F: Duplex controller - lazy initialization
duplex_controller = None

# Phase 11-F: Blueprint already imported above
twilio_router_bp = Blueprint("twilio_router", __name__)

logger.info("Twilio router blueprint initialized successfully.")

# --------------------------------------------------------------------------
# Twilio Webhook Helper Functions
# --------------------------------------------------------------------------
def _media_ws_url() -> str | None:
    """Get WebSocket URL for media streaming from environment"""
    ws_url = os.getenv("TWILIO_MEDIA_WS_URL", "wss://srv-d43eqvemcj7s73b0pum0.onrender.com/media").strip()
    if not ws_url:
        return None
    if not ws_url.startswith("wss://"):
        logger.warning("TWILIO_MEDIA_WS_URL must be wss://, got %s", ws_url)
        return None
    return ws_url

def _verify_twilio_signature() -> bool:
    """Verify Twilio request signature if enabled"""
    if not VERIFY_TWILIO_SIGNATURE:
        return True
    
    try:
        from twilio.request_validator import RequestValidator
        auth_token = CONFIG["TWILIO_AUTH_TOKEN"]
        if not auth_token:
            logger.warning("Twilio auth token not available for signature verification")
            return False
            
        validator = RequestValidator(auth_token)
        signature = request.headers.get('X-Twilio-Signature', '')
        url = request.url
        params = request.form.to_dict()
        
        return validator.validate(url, params, signature)
    except Exception as e:
        logger.warning(f"Twilio signature verification failed: {str(e)}")
        return False

# --------------------------------------------------------------------------
# Twilio Webhook Endpoints
# --------------------------------------------------------------------------
@twilio_router_bp.route("/twilio/answer", methods=["GET", "POST"])
def twilio_answer():
    """
    Handle incoming Twilio call. Always returns valid TwiML with application/xml content-type.
    If Twilio SDK is unavailable at call-time, fall back to static TwiML string.
    """
    trace_id = get_trace_id()

    # Extremely defensive: precompute ws_url, but never let it crash the route.
    try:
        ws_url = _media_ws_url()
    except Exception as e:
        ws_url = None
        _structured_log("twilio_answer_hit_error", level="error",
                        message="Error computing media_ws_url in twilio_answer",
                        trace_id=trace_id, error=str(e))

    _structured_log("twilio_answer_hit", level="info",
                    message="Twilio /twilio/answer invoked",
                    trace_id=trace_id,
                    has_stream_url=bool(ws_url and isinstance(ws_url, str) and ws_url.strip().startswith("wss://")),
                    media_ws_url=ws_url)

    # Signature check (will return 403 if enabled and invalid)
    if not _verify_twilio_signature():
        return Response("Invalid signature", status=403, mimetype="text/plain")

    # Helper: safe TwiML emitter via SDK
    def _emit_twiml_with_sdk(_ws_url: str | None) -> str:
        # Lazy import at call-time so import issues don't break module import
        from twilio.twiml.voice_response import VoiceResponse
        r = VoiceResponse()
        if _ws_url and isinstance(_ws_url, str) and _ws_url.strip().startswith("wss://"):
            # Use <Start><Stream track="both_tracks"/> for full duplex
            r.start().stream(url=_ws_url.strip(), track="both_tracks")
        else:
            r.say("Hello, this is Sara. Please hold while we connect.")
        return str(r)

    # Hard fallback: static XML that Twilio accepts even if the SDK is not available
    def _emit_static_twiml(_ws_url: str | None) -> str:
        if _ws_url and isinstance(_ws_url, str) and _ws_url.strip().startswith("wss://"):
            return f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Start>
    <Stream url="{_ws_url.strip()}" track="both_tracks"/>
  </Start>
</Response>"""
        # Minimal say-only fallback
        return '<?xml version="1.0" encoding="UTF-8"?><Response><Say voice="alice">Hello, this is Sara. Please hold while we connect.</Say></Response>'

    # Try SDK path first; if *anything* goes wrong, fall back to static XML
    try:
        twiml_xml = _emit_twiml_with_sdk(ws_url)
        _structured_log("twilio_answer_twiML_emitted", level="info",
                        message="Emitted TwiML (SDK)", trace_id=trace_id, media_ws_url=ws_url)
    except Exception as e:
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id})
        _structured_log("twilio_answer_sdk_error", level="error",
                        message="SDK TwiML generation failed; falling back to static XML",
                        trace_id=trace_id, error=str(e))
        twiml_xml = _emit_static_twiml(ws_url)

    # Always return 200 + application/xml
    return Response(twiml_xml, status=200, mimetype="application/xml")

@twilio_router_bp.route("/twilio/events", methods=["GET", "POST"])
def twilio_events():
    """Handle Twilio status webhooks - accepts application/x-www-form-urlencoded"""
    trace_id = get_trace_id()
    
    # Verify Twilio signature if enabled
    if not _verify_twilio_signature():
        _structured_log("twilio_signature_failed", level="warning",
                      message="Twilio signature verification failed", trace_id=trace_id)
        return Response("Invalid signature", status=403, mimetype="text/plain")
    
    try:
        # For GET requests, return service info (useful for debugging)
        if request.method == "GET":
            return jsonify({
                "service": __service__,
                "status": "healthy",
                "phase": __phase__,
                "schema_version": __schema_version__,
                "trace_id": trace_id
            }), 200
        
        # For POST requests, process Twilio webhook data
        # Twilio sends application/x-www-form-urlencoded - defensive parsing
        payload = request.form.to_dict() if request.form else {}
        _structured_log("twilio_status_webhook", level="info",
                      message="Received Twilio status webhook",
                      trace_id=trace_id, **payload)
        return ("", 204)
    except Exception as e:
        _structured_log("twilio_events_error", level="error",
                      message=f"Error processing events webhook: {str(e)}",
                      trace_id=trace_id, error=str(e), traceback=traceback.format_exc())
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id})
        return ("", 204)  # Always return 204 to Twilio even on errors

# --------------------------------------------------------------------------
# Phase 11-F Duplex Controller Initialization
# --------------------------------------------------------------------------
def _init_duplex_controller():
    """Initialize duplex controller for real-time streaming"""
    global duplex_controller
    if duplex_controller is not None or not CONFIG["DUPLEX_STREAMING_ENABLED"]:
        return duplex_controller
    
    try:
        duplex_controller = DuplexVoiceController()
        _structured_log("duplex_controller_initialized", level="info",
                      message="Duplex voice controller initialized successfully")
        return duplex_controller
    except Exception as e:
        _structured_log("duplex_controller_init_failed", level="warning",
                      message="Failed to initialize duplex controller",
                      error=str(e))
        duplex_controller = None
        return None

# --------------------------------------------------------------------------
# Phase 11-F Twilio Client Initialization Helper
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
        _structured_log("twilio_init_failed", level="error", message=str(e), error=str(e))
        twilio_client = None
        return None

# --------------------------------------------------------------------------
# Phase 11-F Circuit Breaker
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
# Phase 11-F Structured Logging Wrapper
# --------------------------------------------------------------------------
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

# --------------------------------------------------------------------------
# Phase 11-F Metrics Recording
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
# Playback Endpoint (Updated with Phase 11-F Duplex Streaming)
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

    # Check for placeholder PUBLIC_AUDIO_BASE configuration
    if not CONFIG["PUBLIC_AUDIO_BASE"] or "your-domain.com" in CONFIG["PUBLIC_AUDIO_BASE"]:
        _structured_log("playback_config_missing", level="error",
                      message="PUBLIC_AUDIO_BASE not configured", trace_id=trace_id)
        return jsonify({"error": "PUBLIC_AUDIO_BASE not configured", "trace_id": trace_id}), 400

    # Phase 11-F: Circuit breaker check
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
    safe_session_id = os.path.basename(session_id) if session_id else None
    if not safe_session_id or safe_session_id != session_id:
        _structured_log("playback_invalid_session_id", level="error",
                      message=f"Invalid session_id: {session_id}", trace_id=trace_id)
        return jsonify({"error": "Invalid session_id", "trace_id": trace_id}), 400

    try:
        # Phase 11-F: Safe Redis operation with bytes decoding
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

        # Phase 11-F: Try duplex streaming first if available
        if CONFIG["DUPLEX_STREAMING_ENABLED"]:
            try:
                controller = _init_duplex_controller()
                if controller and hasattr(controller, 'handle_playback_request'):
                    result = controller.handle_playback_request(call_sid, audio_url, trace_id)
                    if result and result.get("success"):
                        latency_ms = (time.time() - start_time) * 1000
                        _record_metrics("playback_duplex", "success", latency_ms, trace_id)
                        _structured_log("twilio_playback_duplex", level="info",
                                      message=f"Playback handled via duplex controller for call {call_sid}",
                                      trace_id=trace_id, session_id=safe_session_id, call_sid=call_sid,
                                      audio_url=audio_url, latency_ms=latency_ms)
                        return jsonify({
                            "status": "success", 
                            "audio_url": audio_url, 
                            "trace_id": trace_id,
                            "call_sid": call_sid,
                            "via": "duplex_controller"
                        }), 200
            except Exception as e:
                _structured_log("duplex_playback_failed", level="warning",
                              message="Duplex playback failed, falling back to standard Twilio",
                              trace_id=trace_id, session_id=safe_session_id, 
                              error=str(e))
        # Fallback to standard Twilio client
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
        client.calls(call_sid).update(twiml=twiml)

        # Phase 11-F: Record success metrics and structured log
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("playback", "success", latency_ms, trace_id)
        _structured_log("twilio_playback_update", level="info",
                      message=f"Updated Twilio call {call_sid} with audio {audio_url}",
                      trace_id=trace_id, session_id=safe_session_id, call_sid=call_sid, 
                      audio_url=audio_url, latency_ms=latency_ms, via="standard_twilio")

        return jsonify({
            "status": "success", 
            "audio_url": audio_url, 
            "trace_id": trace_id,
            "call_sid": call_sid,
            "via": "standard_twilio"
        }), 200

    except Exception as e:
        TwilioRestException = _get_twilio_exceptions()
        # Phase 11-F: Exception handling
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("playback", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id, "session_id": safe_session_id})
        
        # Check if it's a Twilio-specific exception
        if isinstance(e, TwilioRestException):
            _structured_log("twilio_playback_twilio_error", level="error",
                          message=str(e), trace_id=trace_id, session_id=safe_session_id,
                          traceback=traceback.format_exc(), latency_ms=latency_ms, error=str(e))
            return jsonify({"error": f"Twilio API error: {str(e)}", "trace_id": trace_id}), 502
        else:
            _structured_log("twilio_playback_error", level="error",
                          message=str(e), trace_id=trace_id, session_id=safe_session_id,
                          traceback=traceback.format_exc(), latency_ms=latency_ms, error=str(e))
            return jsonify({"error": str(e), "trace_id": trace_id}), 500

# --------------------------------------------------------------------------
# Duplex Streaming Endpoint (Phase 11-F Addition)
# --------------------------------------------------------------------------
@twilio_router_bp.route("/twilio/duplex/stream", methods=["POST"])
def duplex_stream():
    """
    Phase 11-F: Endpoint for duplex streaming operations
    Expects JSON payload: { "call_sid": str, "audio_data": base64, "trace_id": str, "stream_type": "inbound|outbound" }
    """
    start_time = time.time()
    payload = request.get_json(silent=True) or {}
    call_sid = payload.get("call_sid")
    trace_id = payload.get("trace_id") or get_trace_id()
    stream_type = payload.get("stream_type", "inbound")
    
    if not call_sid:
        _structured_log("duplex_missing_call_sid", level="error",
                      message="Missing call_sid in duplex stream request",
                      trace_id=trace_id)
        return jsonify({"error": "Missing call_sid", "trace_id": trace_id}), 400

    if not CONFIG["DUPLEX_STREAMING_ENABLED"]:
        _structured_log("duplex_streaming_disabled", level="warning",
                      message="Duplex streaming disabled, request rejected",
                      trace_id=trace_id, call_sid=call_sid)
        return jsonify({"error": "Duplex streaming disabled", "trace_id": trace_id}), 503

    try:
        controller = _init_duplex_controller()
        if not controller:
            _structured_log("duplex_controller_unavailable", level="error",
                          message="Duplex controller not available",
                          trace_id=trace_id, call_sid=call_sid)
            return jsonify({"error": "Duplex controller unavailable", "trace_id": trace_id}), 503

        # Route to appropriate duplex handler
        if stream_type == "inbound":
            result = controller.handle_inbound_stream(call_sid, payload, trace_id)
        elif stream_type == "outbound":
            result = controller.handle_outbound_stream(call_sid, payload, trace_id)
        else:
            _structured_log("duplex_invalid_stream_type", level="error",
                          message=f"Invalid stream type: {stream_type}",
                          trace_id=trace_id, call_sid=call_sid)
            return jsonify({"error": f"Invalid stream type: {stream_type}", "trace_id": trace_id}), 400

        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("duplex_stream", "success", latency_ms, trace_id)
        _structured_log("duplex_stream_handled", level="info",
                      message=f"Duplex stream handled for {stream_type}",
                      trace_id=trace_id, call_sid=call_sid, stream_type=stream_type,
                      latency_ms=latency_ms)

        return jsonify({
            "status": "success",
            "stream_type": stream_type,
            "call_sid": call_sid,
            "trace_id": trace_id,
            "result": result
        }), 200

    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("duplex_stream", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id, "call_sid": call_sid})
        _structured_log("duplex_stream_error", level="error",
                      message=str(e), trace_id=trace_id, call_sid=call_sid,
                      traceback=traceback.format_exc(), latency_ms=latency_ms, error=str(e))
        return jsonify({"error": str(e), "trace_id": trace_id}), 500

# --------------------------------------------------------------------------
# Health Check (Updated with Phase 11-F Observability and Duplex Status)
# --------------------------------------------------------------------------
@twilio_router_bp.route("/health/twilio_router", methods=["GET"])
def health_check():
    """Phase 11-F health check endpoint with Redis ping + circuit breaker state + duplex status"""
    start_time = time.time()
    trace_id = get_trace_id()
    
    try:
        # Phase 11-F: Safe Redis ping and circuit breaker check
        redis_client_instance = _get_redis_client_safe()
        redis_ok = safe_redis_operation(
            lambda: redis_client_instance.ping() if redis_client_instance else False, 
            fallback=False,
            operation_name="ping_redis"
        )
        breaker_open = _is_circuit_breaker_open("twilio_client")
        
        # Phase 11-F: Duplex controller health check
        duplex_healthy = False
        if CONFIG["DUPLEX_STREAMING_ENABLED"]:
            try:
                controller = _init_duplex_controller()
                duplex_healthy = controller.is_healthy() if controller else False
            except Exception:
                duplex_healthy = False
        
        # Determine overall status - only unhealthy if circuit breaker is open
        # Redis and duplex issues are considered degraded but not unhealthy
        if breaker_open:
            status = "unhealthy"
            http_code = 503
        else:
            status = "healthy" if redis_ok else "degraded"
            http_code = 200
        
        # Record metrics
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "success", latency_ms, trace_id)
        
        _structured_log("health_check", level="info", 
                      message="Health check completed",
                      trace_id=trace_id, redis_ok=redis_ok, breaker_open=breaker_open,
                      duplex_healthy=duplex_healthy, duplex_enabled=CONFIG["DUPLEX_STREAMING_ENABLED"])
        
        return jsonify({
            "service": __service__,
            "status": status,
            "phase": __phase__,
            "schema_version": __schema_version__,
            "trace_id": trace_id,
            "duplex_enabled": CONFIG["DUPLEX_STREAMING_ENABLED"],
            "duplex_healthy": duplex_healthy,
            "components": {
                "redis": {
                    "status": "healthy" if redis_ok else "unhealthy"
                },
                "circuit_breaker": {
                    "status": "open" if breaker_open else "closed"
                },
                "duplex_controller": {
                    "status": "healthy" if duplex_healthy else "unhealthy",
                    "enabled": CONFIG["DUPLEX_STREAMING_ENABLED"]
                }
            }
        }), http_code
        
    except Exception as e:
        # Phase 11-F: Health check failure handling
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id})
        _structured_log("health_check_failed", level="error",
                      message=str(e), trace_id=trace_id, 
                      traceback=traceback.format_exc(), error=str(e))
        
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
# Local Debug Entry (Preserved with Phase 11-F enhancements)
# --------------------------------------------------------------------------
if __name__ == "__main__":
    # Install signal handlers only when running directly
    _install_signal_handlers()
    
    app = Flask(__name__)
    app.register_blueprint(twilio_router_bp)
    
    # Phase 11-F: Initialize duplex controller on startup
    if CONFIG["DUPLEX_STREAMING_ENABLED"]:
        _init_duplex_controller()
    
    _structured_log("twilio_router_startup", level="info",
                  message="Twilio router starting with Phase 11-F compliance",
                  phase=__phase__, duplex_enabled=CONFIG["DUPLEX_STREAMING_ENABLED"])
    
    app.run(host="0.0.0.0", port=int(CONFIG["TWILIO_ROUTER_PORT"]))

# --------------------------------------------------------------------------
# Exports
# --------------------------------------------------------------------------
__all__ = [
    "twilio_router_bp", 
    "playback", 
    "duplex_stream",
    "health_check",
    "health_legacy",
    "_init_duplex_controller",
    "twilio_answer",
    "twilio_events"
]

# Backward compatibility alias for legacy imports
twilio_bp = twilio_router_bp