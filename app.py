"""
app.py — Production Flask Entry Point for Sara AI Core (Phase 10H)
-----------------------------------------------------------------
Features:
- Core API for Sara AI inference and TTS orchestration
- Integrated with Deepgram + Cloudflare R2 via tasks.py
- Celery-backed async processing
- Health and metrics endpoints for platform checks
- Structured logging to Redis via log_event
"""

import os
import json
import traceback
from flask import Flask, request, jsonify
from redis import Redis
from tasks import run_tts, run_inference
from logging_utils import log_event

# --------------------------------------------------------------------------
# Flask App Setup
# --------------------------------------------------------------------------
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = Redis.from_url(REDIS_URL, decode_responses=True)


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    """Root endpoint for info and quick status."""
    return jsonify({
        "service": "Sara AI Core",
        "status": "online",
        "version": "1.0.0",
        "endpoints": [
            "/healthz",
            "/health",
            "/metrics",
            "/tts_test",
            "/run_inference"
        ],
    }), 200


@app.route("/healthz", methods=["GET"])
def healthz():
    """Render-compatible health check."""
    try:
        redis_client.ping()
        return jsonify({"status": "ok", "redis": "connected"}), 200
    except Exception:
        return jsonify({"status": "degraded", "redis": "unreachable"}), 503


@app.route("/health", methods=["GET"])
def health_alias():
    """Alias for Render health checks (maps to /healthz)."""
    return healthz()


@app.route("/metrics", methods=["GET"])
def metrics():
    """Lightweight endpoint for uptime and monitoring."""
    try:
        total_tts = redis_client.hget("metrics:tts", "files_generated") or 0
        total_failures = redis_client.hget("metrics:tts", "failures") or 0
        return jsonify({
            "service": "Sara AI Core",
            "tts_generated": int(total_tts),
            "tts_failures": int(total_failures),
            "status": "ok",
        }), 200
    except Exception:
        return jsonify({"status": "error", "message": "Failed to fetch metrics"}), 500


@app.route("/tts_test", methods=["POST"])
def tts_test():
    """Inline test for TTS generation (Deepgram + R2)."""
    try:
        payload = request.get_json(force=True)
        log_event(service="api", event="tts_test_received", status="ok", message="Received TTS test request")

        result = run_tts(payload, inline=True)
        return jsonify(result), 200
    except Exception:
        err_msg = traceback.format_exc()
        log_event(service="api", event="tts_test_error", status="error", message=err_msg)
        return jsonify({"error": "TTS test failed", "details": err_msg}), 500


@app.route("/run_inference", methods=["POST"])
def inference():
    """Main entrypoint for AI inference + TTS pipeline."""
    try:
        payload = request.get_json(force=True)
        log_event(service="api", event="inference_received", status="ok", message="Received inference request")

        task = run_inference.apply_async(kwargs={"payload": payload})
        return jsonify({"task_id": task.id, "status": "queued"}), 202
    except Exception:
        err_msg = traceback.format_exc()
        log_event(service="api", event="inference_error", status="error", message=err_msg)
        return jsonify({"error": "Inference enqueue failed", "details": err_msg}), 500


# --------------------------------------------------------------------------
# Launch Entrypoint
# --------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
