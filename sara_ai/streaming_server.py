# sara_ai/streaming_server.py
"""
Streaming Server — Phase 5B (Render Deployment + Redis Integration)

This module exposes a Flask app. In production we expect to run it using
Gunicorn (so the server will bind to $PORT). For local/dev runs, the
__main__ block will also start Flask's dev server on PORT if executed directly.
"""

import os
import uuid
import redis
from flask import Flask, request, jsonify
from sara_ai.logging_utils import log_event

app = Flask(__name__)

# Read Redis and PORT from environment (Render provides connection via REDIS_URL)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
# Do not assume a default port that conflicts with Render. Use 10000 for local dev only.
try:
    PORT = int(os.environ.get("PORT", os.environ.get("PORT_OVERRIDE", "10000")))
except Exception:
    PORT = 10000

SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "sara-ai-core-streaming")

# Initialize Redis client
redis_client = redis.Redis.from_url(REDIS_URL, socket_connect_timeout=5)

# Startup Redis check & structured log
try:
    redis_client.ping()
    log_event(
        service="streaming_server",
        event="startup",
        status="ok",
        message=f"Connected to Redis: {REDIS_URL}",
        extra={"service": SERVICE_NAME, "port": PORT},
    )
except Exception as e:
    # Log an error but continue -- the health endpoint will report redis unreachable.
    log_event(
        service="streaming_server",
        event="startup",
        status="error",
        message=f"Redis init failed: {str(e)}",
        extra={"service": SERVICE_NAME, "port": PORT},
    )


@app.route("/health", methods=["GET"])
@app.route("/healthz", methods=["GET"])
def health():
    try:
        redis_client.ping()
        return jsonify({"status": "ok", "redis": "connected"}), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="health_check",
            status="error",
            extra={"error": str(e)},
        )
        return jsonify({"status": "error", "redis": "unreachable"}), 500


@app.route("/stream", methods=["POST"])
def handle_stream():
    data = request.get_json(silent=True) or {}
    trace_id = str(uuid.uuid4())[:8]
    task_type = data.get("task", "unknown")

    log_event(
        service="streaming_server",
        event="request_received",
        status="ok",
        extra={"task": task_type, "trace_id": trace_id},
    )
    try:
        # TODO: enqueue to Celery queue here (run_inference / run_tts) when ready
        log_event(
            service="streaming_server",
            event="task_dispatched",
            status="ok",
            extra={"task": task_type, "trace_id": trace_id},
        )
        return jsonify({"status": "ok", "trace_id": trace_id}), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="task_dispatched",
            status="error",
            extra={"error": str(e), "trace_id": trace_id},
        )
        return jsonify({"status": "error", "trace_id": trace_id}), 500


if __name__ == "__main__":
    # Local development runner. In prod use Gunicorn to bind $PORT.
    log_event(
        service="streaming_server",
        event="startup_complete",
        status="ok",
        message=f"Starting Flask dev server on port {PORT}",
        extra={"service": SERVICE_NAME},
    )
    app.run(host="0.0.0.0", port=PORT)
