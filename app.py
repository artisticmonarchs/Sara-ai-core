"""
app.py — Production Flask Entry Point for Sara AI Core (Phase 10H)
-----------------------------------------------------------------
Core responsibilities:
- Acts as API gateway for Sara AI Core orchestration (Inference + TTS)
- Exposes REST endpoints for health checks, metrics, and testing
- Integrated with Deepgram and Cloudflare R2 through tasks.py
- Uses Celery for async inference/TTS workloads
- Logs structured events to Redis (via logging_utils.log_event)
- Safe, production-ready error handling and JSON responses
"""

import os
import json
import traceback
from flask import Flask, request, jsonify
from redis import Redis, RedisError
from tasks import run_tts, run_inference
from logging_utils import log_event

# -----------------------------------------------------------------------------
# Flask App Setup
# -----------------------------------------------------------------------------
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# Redis connection (non-blocking dependency)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
try:
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
except Exception:
    redis_client = None
    log_event(service="api", event="redis_init_failed", status="warn", message="Redis init failed")

# -----------------------------------------------------------------------------
# Utility Helpers
# -----------------------------------------------------------------------------
def safe_redis_ping() -> bool:
    """Safely check Redis connectivity without breaking app startup."""
    if not redis_client:
        return False
    try:
        return redis_client.ping()
    except RedisError:
        return False
    except Exception:
        return False


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    """Root informational endpoint."""
    return jsonify({
        "service": "Sara AI Core",
        "version": "1.0.0",
        "status": "online",
        "endpoints": [
            "/healthz",
            "/health",
            "/metrics",
            "/tts_test",
            "/run_inference",
        ],
    }), 200


# -----------------------------------------------------------------------------
# Health Endpoints
# -----------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    """
    Lightweight health check used by Render and orchestrators.
    Must not depend on Redis or external services.
    """
    return jsonify({"status": "ok", "service": "sara-ai-core-app"}), 200


@app.route("/health", methods=["GET"])
def health():
    """
    Detailed health check with dependency validation.
    Safe to return degraded if Redis or subservice is unavailable.
    """
    try:
        if safe_redis_ping():
            return jsonify({"status": "ok", "redis": "connected"}), 200
        else:
            return jsonify({"status": "degraded", "redis": "unreachable"}), 503
    except Exception:
        return jsonify({"status": "degraded", "redis": "error"}), 503


# -----------------------------------------------------------------------------
# Metrics Endpoint
# -----------------------------------------------------------------------------
@app.route("/metrics", methods=["GET"])
def metrics():
    """Collect lightweight operational metrics from Redis."""
    try:
        total_tts = int(redis_client.hget("metrics:tts", "files_generated") or 0)
        total_failures = int(redis_client.hget("metrics:tts", "failures") or 0)
        return jsonify({
            "service": "Sara AI Core",
            "status": "ok",
            "tts_generated": total_tts,
            "tts_failures": total_failures,
        }), 200
    except Exception as e:
        log_event(service="api", event="metrics_fetch_error", status="error", message=str(e))
        return jsonify({"status": "error", "message": "Failed to fetch metrics"}), 500


# -----------------------------------------------------------------------------
# Inline TTS Testing
# -----------------------------------------------------------------------------
@app.route("/tts_test", methods=["POST"])
def tts_test():
    """Direct Deepgram TTS test (used by internal tools)."""
    try:
        payload = request.get_json(force=True)
        if not payload or "text" not in payload:
            return jsonify({"error": "Missing 'text' in payload"}), 400

        log_event(service="api", event="tts_test_received", status="ok", message="Received TTS test")

        result = run_tts(payload, inline=True)
        if isinstance(result, dict) and result.get("error_code"):
            log_event(service="api", event="tts_test_failed", status="error", message=str(result))
            return jsonify({"error": "TTS generation failed", "details": result}), 500

        return jsonify(result), 200
    except Exception:
        err_msg = traceback.format_exc()
        log_event(service="api", event="tts_test_exception", status="error", message=err_msg)
        return jsonify({"error": "TTS test failed", "details": err_msg}), 500


# -----------------------------------------------------------------------------
# Inference + TTS Pipeline
# -----------------------------------------------------------------------------
@app.route("/run_inference", methods=["POST"])
def inference():
    """
    Asynchronous inference pipeline:
    - Triggers GPT inference and optional TTS via Celery task.
    - Returns queued task_id for tracking.
    """
    try:
        payload = request.get_json(force=True)
        if not payload or "prompt" not in payload:
            return jsonify({"error": "Missing 'prompt' in payload"}), 400

        log_event(service="api", event="inference_received", status="ok", message="Received inference request")

        task = run_inference.apply_async(kwargs={"payload": payload})
        return jsonify({"task_id": task.id, "status": "queued"}), 202
    except Exception:
        err_msg = traceback.format_exc()
        log_event(service="api", event="inference_error", status="error", message=err_msg)
        return jsonify({"error": "Inference enqueue failed", "details": err_msg}), 500


# -----------------------------------------------------------------------------
# Launch Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
