"""
app.py — Production Flask Entry Point for Sara AI Core
------------------------------------------------------
Features:
- Flask API endpoints for health, TTS testing, and task orchestration
- Celery-backed async processing
- Integration with Deepgram TTS and Cloudflare R2 via tasks.py
- Logging and Redis metrics
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
    """Root endpoint for quick info."""
    return jsonify({
        "service": "Sara AI Core",
        "status": "online",
        "version": "1.0.0",
        "endpoints": ["/healthz", "/tts_test", "/run_inference"]
    }), 200


@app.route("/healthz", methods=["GET"])
def healthz():
    """Health check endpoint (used by Render/Fly/Kubernetes)."""
    return jsonify({"status": "healthy", "service": "Sara AI Core"}), 200


@app.route("/tts_test", methods=["POST"])
def tts_test():
    """Inline test for TTS pipeline."""
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
# Launch
# --------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
