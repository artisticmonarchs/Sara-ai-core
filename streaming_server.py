import os
import json
import time
import uuid
import logging
from flask import Flask, request, jsonify, Response
from redis import Redis
from tasks import run_tts
from gpt_client import generate_reply
from logging_utils import log_event

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
app = Flask(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "")
STREAM_HEARTBEAT_INTERVAL = float(os.getenv("STREAM_HEARTBEAT_INTERVAL", "10"))

redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Utility
# --------------------------------------------------------------------------
def new_trace():
    return str(uuid.uuid4())

def sse_format(event=None, data=None):
    """Formats data for Server-Sent Events (SSE)."""
    msg = ""
    if event:
        msg += f"event: {event}\n"
    if data:
        msg += f"data: {json.dumps(data)}\n"
    msg += "\n"
    return msg

# --------------------------------------------------------------------------
# SSE Streaming Endpoint
# --------------------------------------------------------------------------
@app.route("/stream", methods=["POST"])
def stream():
    """
    Primary conversation endpoint for Sara AI voice or chat clients.
    Accepts JSON payload, performs GPT inference, runs TTS, and streams progress.
    """
    try:
        data = request.get_json(force=True)
        session_id = data.get("session_id") or str(uuid.uuid4())
        trace_id = data.get("trace_id") or new_trace()
        user_text = data.get("text") or data.get("message") or ""
        if not user_text.strip():
            return jsonify({"error": "No input text"}), 400

        log_event(
            service="streaming_server",
            event="stream_start",
            status="ok",
            message=f"Stream session started with {len(user_text)} chars",
            trace_id=trace_id,
            session_id=session_id,
        )

        def event_stream():
            """Generator yielding SSE messages."""
            try:
                # 1️⃣ Step: Acknowledge
                yield sse_format("status", {"stage": "received", "trace_id": trace_id})

                # 2️⃣ Step: Generate GPT reply
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

                # 3️⃣ Step: Generate TTS Audio (R2 upload)
                start_tts = time.time()
                tts_result = run_tts(
                    {"text": reply_text, "trace_id": trace_id, "session_id": session_id},
                    inline=True,
                )
                if "error_code" in tts_result:
                    yield sse_format("error", tts_result)
                    return

                audio_url = tts_result["audio_url"]
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

                # 4️⃣ Step: Completion
                yield sse_format("complete", {"trace_id": trace_id, "session_id": session_id})

            except Exception as e:
                err_id = str(uuid.uuid4())
                logger.exception("Stream exception")
                log_event(
                    service="streaming_server",
                    event="stream_exception",
                    status="error",
                    message=str(e),
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={"error_id": err_id},
                )
                yield sse_format("error", {"message": "Streaming error", "error_id": err_id})

        return Response(event_stream(), mimetype="text/event-stream")

    except Exception as e:
        trace_id = str(uuid.uuid4())
        logger.exception("Stream route fatal error")
        log_event(
            service="streaming_server",
            event="fatal_error",
            status="error",
            message=str(e),
            trace_id=trace_id,
        )
        return jsonify({"error": "Internal server error", "trace_id": trace_id}), 500

# --------------------------------------------------------------------------
# Twilio-compatible webhook (optional)
# --------------------------------------------------------------------------
@app.route("/twilio_tts", methods=["POST"])
def twilio_tts():
    """
    Webhook endpoint for Twilio calls — generates speech and returns TwiML.
    """
    from flask import Response as TwilioResponse

    text = request.form.get("SpeechResult") or request.form.get("text") or "Hello from Sara AI"
    session_id = str(uuid.uuid4())
    trace_id = new_trace()

    tts_result = run_tts({"text": text, "session_id": session_id, "trace_id": trace_id}, inline=True)
    if "error_code" in tts_result:
        return TwilioResponse(
            f"<Response><Say>Sorry, an error occurred generating speech.</Say></Response>",
            mimetype="application/xml",
        )

    audio_url = tts_result["audio_url"]
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

# --------------------------------------------------------------------------
# Healthcheck
# --------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    try:
        redis_client.ping()
        return jsonify({"status": "ok", "redis": "connected"})
    except Exception:
        return jsonify({"status": "degraded", "redis": "unreachable"}), 503

# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 7000))
    app.run(host="0.0.0.0", port=port, threaded=True)
