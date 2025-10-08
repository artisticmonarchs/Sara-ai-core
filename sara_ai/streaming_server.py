import os
import json
import uuid
import redis
from flask import Flask, request, jsonify
from sara_ai.logging_utils import log_event

app = Flask(__name__)

# --- Redis Client Setup ---
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(redis_url)

# Startup log
log_event(
    service="streaming_server",
    event="startup",
    status="ok",
    message=f"Streaming server initialized with Redis URL: {redis_url}",
)

# Optional Redis connectivity check
try:
    redis_client.ping()
    log_event(service="streaming_server", event="redis_ping", status="ok")
except Exception as e:
    log_event(
        service="streaming_server",
        event="redis_ping",
        status="error",
        extra={"error": str(e)},
    )


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
        # Placeholder for Celery task dispatch or WebSocket handling
        log_event(
            service="streaming_server",
            event="task_dispatch",
            status="ok",
            extra={"task": task_type, "trace_id": trace_id},
        )
        return jsonify({"status": "ok", "trace_id": trace_id}), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="task_dispatch",
            status="error",
            extra={"error": str(e), "trace_id": trace_id},
        )
        return jsonify({"status": "error", "error": str(e), "trace_id": trace_id}), 500


@app.route("/healthz", methods=["GET"])
def healthz():
    """Health check endpoint for Render."""
    try:
        redis_client.ping()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    env_name = os.getenv("RENDER_SERVICE_NAME", "sara-ai-core-streaming")
    log_event(
        service="streaming_server",
        event="startup_complete",
        status="ok",
        message="Streaming server is running.",
        extra={"port": port, "env": env_name},
    )
    app.run(host="0.0.0.0", port=port)
