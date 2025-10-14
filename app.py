"""
app.py — Sara AI Core (Phase 10H, R2-integrated)
Handles HTTP endpoints for health checks, test TTS, and async task orchestration.
"""

import os
import json
import uuid
import time
from flask import Flask, request, jsonify
from redis import Redis
from logging_utils import log_event
from sara_ai.tasks import run_tts

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
app = Flask(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "")
ENV = os.getenv("ENV", "development")

redis_client = Redis.from_url(REDIS_URL, decode_responses=True)


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.route("/")
def root():
    return jsonify({
        "service": "Sara AI Core",
        "status": "ok",
        "env": ENV,
        "public_audio_host": PUBLIC_AUDIO_HOST,
    })


@app.route("/health")
def health():
    try:
        redis_client.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return jsonify({
        "service": "Sara AI Core",
        "status": "healthy",
        "redis": redis_ok,
    })


@app.route("/tts_test", methods=["POST", "GET"])
def tts_test():
    """
    Test endpoint to generate TTS audio and return the R2 public URL.
    Usage:
        GET  /tts_test?text=Hello
        POST /tts_test  { "text": "Hello" }
    """
    if request.method == "GET":
        text = request.args.get("text", "")
    else:
        data = request.get_json(silent=True) or {}
        text = data.get("text", "")

    if not text.strip():
        return jsonify({"error": "No text provided"}), 400

    session_id = str(uuid.uuid4())
    trace_id = str(uuid.uuid4())

    log_event(
        service="app",
        event="tts_test_start",
        status="ok",
        message=f"Received /tts_test request ({len(text)} chars)",
        trace_id=trace_id,
        session_id=session_id,
    )

    # Run inline for simplicity — avoids Celery queue lag in testing
    start = time.time()
    result = run_tts({"text": text, "session_id": session_id, "trace_id": trace_id}, inline=True)
    latency = round((time.time() - start) * 1000, 2)

    if "error_code" in result:
        return jsonify({
            "trace_id": trace_id,
            "session_id": session_id,
            "status": "error",
            "error": result["error_message"],
        }), 500

    log_event(
        service="app",
        event="tts_test_done",
        status="ok",
        message=f"TTS test done in {latency} ms",
        trace_id=trace_id,
        session_id=session_id,
        extra={"audio_url": result.get("audio_url")},
    )

    return jsonify({
        "trace_id": trace_id,
        "session_id": session_id,
        "audio_url": result.get("audio_url"),
        "latency_ms": latency,
        "status": "ok",
    })


@app.route("/metrics")
def metrics():
    """Expose basic TTS counters from Redis."""
    try:
        stats = redis_client.hgetall("metrics:tts")
    except Exception:
        stats = {}
    return jsonify({
        "service": "Sara AI Core",
        "metrics": stats,
    })


# --------------------------------------------------------------------------
# Error handlers
# --------------------------------------------------------------------------
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def internal_error(e):
    return jsonify({"error": "Internal server error"}), 500


# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
