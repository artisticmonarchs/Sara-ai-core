# sara_ai/streaming_server.py
"""
Streaming Server — Phase 5B (Render + Redis Resilient)

Twilio / WebSocket Gateway for Sara AI Core.
Routes incoming events and dispatches Celery tasks.
"""

import os
import uuid
import redis
from flask import Flask, request, jsonify
from sara_ai.logging_utils import log_event
from sara_ai.tasks import run_inference, run_tts

app = Flask(__name__)

# --------------------------------------------------------
# Redis Setup
# --------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Optional fallback for Render public hostname (rarely used)
if ".internal" in REDIS_URL:
    fallback = os.getenv("REDIS_PUBLIC_URL")
    if fallback:
        REDIS_URL = fallback

redis_client = redis.Redis.from_url(REDIS_URL)

# Startup logs
log_event(
    service="streaming_server",
    event="startup",
    status="ok",
    message=f"Redis client initialized with URL: {REDIS_URL}",
)

# Test Redis connectivity
try:
    redis_client.ping()
    log_event(service="streaming_server", event="redis_ping", status="ok")
except Exception as e:
    log_event(
        service="streaming_server",
        event="redis_ping",
        status="error",
        details={"error": str(e)},
    )

# --------------------------------------------------------
# Stream Handler
# --------------------------------------------------------
@app.route("/stream", methods=["POST"])
def handle_stream():
    data = request.get_json(silent=True) or {}
    trace_id = str(uuid.uuid4())[:8]
    task_type = data.get("task", "inference")

    log_event(
        service="streaming_server",
        event="request_received",
        status="ok",
        details={"task": task_type, "trace_id": trace_id},
    )

    try:
        if task_type == "tts":
            run_tts.delay(data)
        else:
            run_inference.delay(data)

        log_event(
            service="streaming_server",
            event="enqueue_task",
            status="ok",
            details={"task": task_type, "trace_id": trace_id},
        )
        return jsonify({"status": "queued", "task": task_type, "trace_id": trace_id}), 202

    except Exception as e:
        log_event(
            service="streaming_server",
            event="enqueue_task",
            status="error",
            details={"error": str(e), "trace_id": trace_id},
        )
        return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500


# --------------------------------------------------------
# Entrypoint
# --------------------------------------------------------
if __name__ == "__main__":
    PORT = int(os.getenv("PORT", 5002))
    log_event(service="streaming_server", event="startup_main", status="ok")
    app.run(host="0.0.0.0", port=PORT)
