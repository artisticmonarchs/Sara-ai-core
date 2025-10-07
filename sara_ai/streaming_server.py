"""
streaming_server.py — Phase 5B Production Build
Twilio Media Stream Gateway for Sara AI.
Handles real-time audio input/output and integrates with Redis + Celery.
"""

import os
import asyncio
import logging
import traceback
from flask import Flask, request, jsonify
from sara_ai.tasks import process_event
from sara_ai.logging_utils import log_event

app = Flask(__name__)
PORT = int(os.getenv("PORT", 5000))
logger = logging.getLogger("streaming_server")

@app.route("/", methods=["GET"])
def index():
    return jsonify({"service": "Sara AI Streaming Gateway", "status": "running"})

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "streaming_server"})

@app.route("/stream", methods=["POST"])
def handle_stream():
    try:
        data = request.json or {}
        trace_id = log_event(
            service="streaming_server",
            event="request_received",
            status="ok",
            message="Incoming Twilio media stream",
        )
        task = process_event.delay(data)
        log_event(
            service="streaming_server",
            event="task_enqueued",
            status="ok",
            message=f"Task enqueued with ID: {task.id}",
            trace_id=trace_id,
        )
        return jsonify({"status": "submitted", "task_id": task.id, "trace_id": trace_id})
    except Exception as e:
        log_event(
            service="streaming_server",
            event="request_error",
            status="error",
            message=str(e),
        )
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    log_event(
        service="streaming_server",
        event="startup",
        status="ok",
        message=f"Streaming server started on port {PORT}",
    )
    app.run(host="0.0.0.0", port=PORT)
