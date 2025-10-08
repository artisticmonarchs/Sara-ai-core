"""
streaming_server.py — Phase 5B (Render + Redis Resilient + Healthz)

Twilio / WebSocket Gateway for Sara AI Core.
Routes incoming Twilio Media Streams and dispatches Celery tasks.
"""

import os
import logging
import redis
from flask import Flask, request, jsonify
from sara_ai.logging_utils import log_event
from sara_ai.tasks import run_inference, run_tts

# --------------------------------------------------------
# Initialization
# --------------------------------------------------------
app = Flask(__name__)
PORT = int(os.getenv("PORT", 10000))

# Initialize Redis client
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

if ".internal" in REDIS_URL:
    fallback = os.getenv("REDIS_PUBLIC_URL")
    if fallback:
        REDIS_URL = fallback

redis_client = redis.Redis.from_url(REDIS_URL)

# Verify Redis connectivity on startup
try:
    redis_client.ping()
    log_event(
        service="streaming_server",
        event="startup",
        status="ok",
        message=f"Redis client initialized and reachable at {REDIS_URL}",
        details={"port": PORT},
    )
except Exception as e:
    log_event(
        service="streaming_server",
        event="startup",
        status="error",
        message=f"Redis initialization failed: {e}",
        details={"port": PORT},
    )

# --------------------------------------------------------
# Healthcheck endpoint
# --------------------------------------------------------
@app.route("/health", methods=["GET"])
@app.route("/healthz", methods=["GET"])  # Added for Render
def health():
    try:
        redis_client.ping()
        return jsonify({"status": "ok", "redis": "connected"}), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="health_check",
            status="error",
            details={"error": str(e)},
        )
        return jsonify({"status": "error", "redis": "unreachable"}), 500


# --------------------------------------------------------
# Twilio stream ingest endpoint
# --------------------------------------------------------
@app.route("/stream", methods=["POST"])
def stream_event():
    try:
        data = request.get_json(force=True)
        stream_type = data.get("type", "inference")

        if stream_type == "tts":
            run_tts.delay(data)
            task_type = "tts"
        else:
            run_inference.delay(data)
            task_type = "inference"

        log_event(
            service="streaming_server",
            event="enqueue_task",
            status="ok",
            details={"task": task_type},
        )
        return jsonify({"status": "queued", "task": task_type}), 202

    except Exception as e:
        log_event(
            service="streaming_server",
            event="enqueue_task",
            status="error",
            details={"error": str(e)},
        )
        return jsonify({"status": "error", "message": str(e)}), 500


# --------------------------------------------------------
# Entrypoint
# --------------------------------------------------------
if __name__ == "__main__":
    log_event(service="streaming_server", event="startup_banner", status="ok", message=f"Streaming Server running on port {PORT}")
    app.run(host="0.0.0.0", port=PORT)
