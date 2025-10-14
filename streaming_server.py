"""
streaming_server.py — Production-grade SSE streaming endpoint for Sara AI

Improvements included:
- /healthz endpoint (required by health checks)
- /health endpoint (detailed Redis check)
- robust Redis ping with graceful degradation
- structured logging via logging_utils.log_event
- SSE responses with recommended headers (no buffering)
- safer error handling and trace propagation
- small performance-friendly tweaks for Gunicorn/containers
"""

import os
import json
import time
import uuid
import logging
from typing import Generator, Optional

from flask import Flask, request, jsonify, Response, stream_with_context
from redis import Redis, RedisError

from tasks import run_tts
from gpt_client import generate_reply
from logging_utils import log_event

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "")
STREAM_HEARTBEAT_INTERVAL = float(os.getenv("STREAM_HEARTBEAT_INTERVAL", "10"))

logger = logging.getLogger("streaming_server")
logger.setLevel(os.getenv("STREAMING_LOG_LEVEL", "INFO"))

# Redis client (safe init)
try:
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
except Exception:
    redis_client = None
    logger.exception("Failed to initialize Redis client at startup")

# Utility helpers ----------------------------------------------------------
def new_trace() -> str:
    return str(uuid.uuid4())

def sse_format(event: Optional[str] = None, data: Optional[dict] = None) -> str:
    """Formats a Server-Sent Events message."""
    msg_lines = []
    if event:
        msg_lines.append(f"event: {event}")
    if data is not None:
        # data must be a string per SSE spec; encode JSON compactly
        msg_lines.append(f"data: {json.dumps(data, separators=(',', ':'))}")
    msg_lines.append("")  # blank line terminator
    return "\n".join(msg_lines) + "\n"

def safe_redis_ping() -> bool:
    if not redis_client:
        return False
    try:
        return redis_client.ping()
    except RedisError:
        return False
    except Exception:
        logger.exception("Unexpected exception during Redis ping")
        return False

# Health endpoints ---------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    """Lightweight healthcheck for orchestrator/load balancer (fast)."""
    return jsonify({"status": "ok", "service": "streaming_server"}), 200

@app.route("/health", methods=["GET"])
def health():
    """Detailed health (checks Redis connectivity)."""
    try:
        ok = safe_redis_ping()
        if ok:
            return jsonify({"status": "ok", "redis": "connected"}), 200
        else:
            return jsonify({"status": "degraded", "redis": "unreachable"}), 503
    except Exception:
        logger.exception("Health endpoint failure")
        return jsonify({"status": "degraded", "redis": "error"}), 503

# SSE Streaming Endpoint ---------------------------------------------------
@app.route("/stream", methods=["POST"])
def stream():
    """
    Accepts JSON payload:
      { "session_id": "...", "trace_id": "...", "text": "..." }

    Returns an SSE stream with events:
      - status (received)
      - reply_text
      - audio_ready
      - complete
      - error
    """
    trace_id = None
    session_id = None

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
            return jsonify({"error": "No input text"}), 400

        log_event(
            service="streaming_server",
            event="stream_start",
            status="ok",
            message=f"Stream session started ({len(user_text)} chars)",
            trace_id=trace_id,
            session_id=session_id,
        )

        @stream_with_context
        def event_stream() -> Generator[str, None, None]:
            """
            Generator sends SSE events. Keep it synchronous (blocking) for simplicity,
            but protect and log exceptions so client receives a final error event.
            """
            try:
                # Acknowledge receipt quickly
                yield sse_format("status", {"stage": "received", "trace_id": trace_id})

                # Inference
                start_infer = time.time()
                reply_text = generate_reply(user_text, trace_id=trace_id)
                infer_ms = round((time.time() - start_infer) * 1000, 2)

                log_event(
                    service="streaming_server",
                    event="inference_done",
                    status="ok",
                    message=f"Inference completed ({len(reply_text)} chars)",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={"latency_ms": infer_ms},
                )
                yield sse_format("reply_text", {"text": reply_text})

                # TTS generation (synchronous, uses run_tts inline)
                start_tts = time.time()
                tts_result = run_tts(
                    {"text": reply_text, "trace_id": trace_id, "session_id": session_id},
                    inline=True,
                )

                if isinstance(tts_result, dict) and tts_result.get("error_code"):
                    # Return error to client and log
                    log_event(
                        service="streaming_server",
                        event="tts_failed",
                        status="error",
                        message="TTS generation failed",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={"tts_result": tts_result},
                    )
                    yield sse_format("error", tts_result)
                    return

                audio_url = tts_result.get("audio_url") if isinstance(tts_result, dict) else None
                tts_ms = round((time.time() - start_tts) * 1000, 2)

                log_event(
                    service="streaming_server",
                    event="tts_done",
                    status="ok",
                    message="TTS generated successfully",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={"audio_url": audio_url, "tts_latency_ms": tts_ms},
                )

                yield sse_format("audio_ready", {"url": audio_url})

                # Complete
                yield sse_format("complete", {"trace_id": trace_id, "session_id": session_id})

            except Exception as exc:
                err_id = str(uuid.uuid4())
                logger.exception("Unhandled exception inside streaming event_stream")
                log_event(
                    service="streaming_server",
                    event="stream_exception",
                    status="error",
                    message=str(exc),
                    trace_id=trace_id or new_trace(),
                    session_id=session_id or "unknown",
                    extra={"error_id": err_id},
                )
                yield sse_format("error", {"message": "Streaming error", "error_id": err_id})

        # Important SSE headers to avoid buffering by proxies/load balancers
        headers = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            # Nginx / some proxies will buffer; this header disables it where supported
            "X-Accel-Buffering": "no",
        }

        return Response(event_stream(), headers=headers)

    except Exception as e:
        # fatal route-level error
        logger.exception("Stream route fatal error")
        trace = trace_id or new_trace()
        log_event(
            service="streaming_server",
            event="fatal_error",
            status="error",
            message=str(e),
            trace_id=trace,
            session_id=session_id or "unknown",
        )
        return jsonify({"error": "Internal server error", "trace_id": trace}), 500

# Twilio webhook compatibility ----------------------------------------------
@app.route("/twilio_tts", methods=["POST"])
def twilio_tts():
    """
    Generate TTS for Twilio webhook calls and return TwiML <Play>.
    Kept simple and synchronous for Twilio usage.
    """
    from flask import Response as TwilioResponse

    trace_id = new_trace()
    session_id = str(uuid.uuid4())

    try:
        text = request.form.get("SpeechResult") or request.form.get("text") or "Hello from Sara AI"

        tts_result = run_tts({"text": text, "session_id": session_id, "trace_id": trace_id}, inline=True)
        if isinstance(tts_result, dict) and tts_result.get("error_code"):
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
            extra={"audio_url": audio_url},
        )

        twiml = f"<Response><Play>{audio_url}</Play></Response>"
        return TwilioResponse(twiml, mimetype="application/xml")

    except Exception:
        logger.exception("Twilio TTS handler error")
        return TwilioResponse(
            "<Response><Say>Sorry, an internal error occurred.</Say></Response>",
            mimetype="application/xml",
        )

# Entrypoint ----------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 7000))
    # When run directly (not via Gunicorn) enable threaded mode for concurrent SSE clients
    app.run(host="0.0.0.0", port=port, threaded=True)
