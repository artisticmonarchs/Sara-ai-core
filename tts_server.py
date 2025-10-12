"""
tts_server.py — Phase 9 Deepgram Integration (Production Refinements)
Handles text-to-speech requests for Sara AI Core (Render-compatible).
"""

import os
import io
import redis
import time
from flask import Flask, request, jsonify, send_file
from celery import Celery
from deepgram import DeepgramClient, SpeakOptions
from logging_utils import log_event
from sentry_utils import init_sentry

# --------------------------------------------------------------------------
# Initialization
# --------------------------------------------------------------------------
init_sentry()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DG_API_KEY = os.getenv("DEEPGRAM_API_KEY")
dg_client = DeepgramClient(DG_API_KEY)

app = Flask(__name__)

# Celery configuration
celery = Celery("tts_server", broker=REDIS_URL, backend=REDIS_URL)

# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.route("/tts", methods=["POST"])
def tts():
    """
    Handle TTS (text-to-speech) generation requests.
    Dispatches Deepgram TTS via Celery worker.
    """
    data = request.get_json(silent=True) or {}
    text = data.get("text")
    voice = data.get("voice", "aura-2-asteria-en")
    trace_id = data.get("trace_id")

    if not text or not isinstance(text, str):
        log_event(
            service="tts_server",
            event="invalid_payload",
            status="error",
            message="Missing or invalid 'text' field",
            extra={"payload": data},
        )
        return jsonify({"error": "Missing required field 'text'"}), 400

    try:
        task_data = {"text": text, "voice": voice, "trace_id": trace_id}
        task_result = celery.send_task("sara_ai.tasks.tts.run_tts", args=[task_data])

        log_event(
            service="tts_server",
            event="tts_dispatched",
            status="ok",
            message=f"Deepgram TTS task dispatched (len={len(text)})",
            extra={"task_id": task_result.id, "voice": voice},
            trace_id=trace_id,
        )

        return jsonify({
            "status": "queued",
            "task_id": task_result.id,
            "trace_id": trace_id or task_result.id
        }), 202

    except Exception as e:
        log_event(
            service="tts_server",
            event="dispatch_failed",
            status="error",
            message=f"TTS dispatch failed: {str(e)}",
            extra={"payload": data},
            trace_id=trace_id,
        )
        return jsonify({"error": "Failed to dispatch TTS task"}), 500


@app.route("/tts/direct", methods=["POST"])
def direct_tts():
    """
    Direct Deepgram synthesis (sync) — for testing without Celery.
    """
    data = request.get_json(silent=True) or {}
    text = data.get("text")
    voice = data.get("voice", "aura-2-asteria-en")

    if not text:
        return jsonify({"error": "Missing 'text'"}), 400

    start_time = time.time()

    try:
        speak_opts = SpeakOptions(model=voice)
        response = dg_client.speak.v("1").stream({"text": text}, speak_opts)

        audio_bytes = io.BytesIO()
        for chunk in response.stream:
            audio_bytes.write(chunk)

        if audio_bytes.tell() == 0:
            raise RuntimeError("Deepgram stream returned no audio data.")

        duration = round(time.time() - start_time, 2)
        log_event(
            service="tts_server",
            event="direct_tts_complete",
            status="ok",
            message=f"Audio generated ({len(text)} chars, {duration}s)",
        )

        return send_file(
            io.BytesIO(audio_bytes.getvalue()),
            mimetype="audio/wav",
            as_attachment=True,
            download_name="sara_tts.wav"
        )

    except Exception as e:
        log_event(
            service="tts_server",
            event="direct_tts_failed",
            status="error",
            message=str(e),
        )
        return jsonify({"error": str(e)}), 500


@app.route("/task-status/<task_id>", methods=["GET"])
def task_status(task_id):
    """
    Check the status of a Celery TTS task.
    """
    try:
        result = celery.AsyncResult(task_id)
        status = result.status
        response = {"task_id": task_id, "status": status}

        if status == "SUCCESS":
            response["result"] = result.result
            # Optional cleanup to save Redis memory
            result.forget()

        return jsonify(response)

    except Exception as e:
        log_event(
            service="tts_server",
            event="status_check_failed",
            status="error",
            message=f"Failed to check task status: {str(e)}",
            extra={"task_id": task_id},
        )
        return jsonify({"error": "Failed to retrieve task status"}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint to confirm TTS server & Redis broker availability.
    """
    try:
        r = redis.Redis.from_url(REDIS_URL)
        r.ping()
        redis_status = "healthy"
    except redis.RedisError as e:
        redis_status = "unhealthy"
        safe_url = REDIS_URL.split("@")[-1] if "@" in REDIS_URL else REDIS_URL
        log_event(
            service="tts_server",
            event="health_check_failed",
            status="error",
            message=f"Redis connection failed at {safe_url}: {str(e)}",
        )

    return jsonify({
        "service": "tts_server",
        "redis": redis_status,
        "status": "healthy" if redis_status == "healthy" else "degraded"
    })


# --------------------------------------------------------------------------
# Entrypoint (Render will use Gunicorn)
# --------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("TTS_PORT", 6000))
    log_event(
        service="tts_server",
        event="startup",
        status="ok",
        message=f"TTS server listening on port {port}",
    )
    app.run(host="0.0.0.0", port=port)
