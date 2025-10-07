"""
streaming_server.py — Phase 5B Final (Render-Ready)

Twilio / WebSocket Gateway for Sara AI Core.
Routes incoming Twilio Media Streams and dispatches Celery tasks.
"""

import os
import asyncio
import logging
from flask import Flask, request, jsonify
from sara_ai.logging_utils import log_event
from sara_ai.tasks import run_inference, run_tts
from sara_ai.celery_app import redis_client

# --------------------------------------------------------
# Initialization
# --------------------------------------------------------
app = Flask(__name__)
PORT = int(os.getenv("PORT", 5001))

# Startup banner
log_event(
    service="streaming_server",
    event="startup",
    status="ok",
    details={"port": PORT},
)

# --------------------------------------------------------
# Healthcheck endpoint
# --------------------------------------------------------
@app.route("/health", methods=["GET"])
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
    log_event(service="streaming_server", event="startup_main", status="ok")
    app.run(host="0.0.0.0", port=PORT)
