import os
import json
import uuid
import time
from flask import Flask, request, jsonify
from tasks import run_tts
from logging_utils import log_event

app = Flask(__name__)

PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ENV = os.getenv("ENV", "development")

@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "status": "ok",
        "service": "Sara AI Core",
        "env": ENV,
        "public_audio_host": PUBLIC_AUDIO_HOST,
        "message": "TTS & inference pipeline online"
    })

@app.route("/tts_test", methods=["POST"])
def tts_test():
    """
    Simple endpoint for testing the TTS task pipeline.
    Accepts JSON {"text": "..."} and returns audio_url.
    """
    data = request.get_json(force=True)
    text = data.get("text", "").strip()
    if not text:
        return jsonify({"error": "No text provided"}), 400

    session_id = str(uuid.uuid4())
    trace_id = str(uuid.uuid4())

    payload = {"text": text, "session_id": session_id, "trace_id": trace_id}
    log_event(
        service="app",
        event="tts_test_start",
        status="ok",
        message=f"Received TTS test request for {len(text)} chars",
        trace_id=trace_id,
        session_id=session_id,
    )

    start = time.time()
    try:
        result = run_tts(payload, inline=True)
    except Exception as e:
        log_event(
            service="app",
            event="tts_test_exception",
            status="error",
            message=str(e),
            trace_id=trace_id,
            session_id=session_id,
        )
        return jsonify({"error": "TTS execution failed", "trace_id": trace_id}), 500

    latency = round((time.time() - start) * 1000, 2)

    if "error_code" in result:
        return jsonify(result), 500

    return jsonify({
        "trace_id": result["trace_id"],
        "session_id": result["session_id"],
        "audio_url": result["audio_url"],
        "latency_ms": latency,
        "status": "ok"
    })

@app.errorhandler(Exception)
def handle_exception(e):
    trace_id = str(uuid.uuid4())
    log_event(
        service="app",
        event="unhandled_exception",
        status="error",
        message=str(e),
        trace_id=trace_id,
    )
    return jsonify({"error": "Internal server error", "trace_id": trace_id}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
