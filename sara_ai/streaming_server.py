# sara_ai/streaming_server.py
"""
Streaming Server — Phase 5B (Render Deployment + Redis Integration)
"""

import os
import uuid
import redis
from flask import Flask, request, jsonify
from sara_ai.logging_utils import log_event

# --------------------------------------------------------
# Initialization
# --------------------------------------------------------
app = Flask(__name__)

# Environment + Redis setup
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PORT = int(os.getenv("PORT", 10000))
SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "sara-ai-core-streaming")

redis_client = redis.Redis.from_url(REDIS_URL)

# Startup check
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
    log_event(
        service="streaming_server",
        event="startup",
        status="error",
        message=f"Redis init failed: {str(e)}",
        extra={"service": SERVICE_NAME, "port": PORT},
    )

# --------------------------------------------------------
# Healthcheck (for Render)
# --------------------------------------------------------
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

# --------------------------------------------------------
# Stream Ingest Endpoint (Twilio or WebSocket proxy)
# --------------------------------------------------------
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

    # Simulated processing — replace later with Celery dispatch
    try:
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

# --------------------------------------------------------
# Entrypoint
# --------------------------------------------------------
if __name__ == "__main__":
    log_event(
        service="streaming_server",
        event="startup_complete",
        status="ok",
        message=f"Starting Flask on port {PORT}",
        extra={"service": SERVICE_NAME},
    )
    app.run(host="0.0.0.0", port=PORT)
