# --------------------------------------------------------
# app.py — Sara AI Core (Ready-to-Deploy)
# --------------------------------------------------------

import logging
from flask import Flask, jsonify, request
from celery.result import AsyncResult
from tasks import run_inference, run_tts
from logging_utils import log_event

app = Flask(__name__)
logger = logging.getLogger("flask_app")

# -------------------------
# 1️⃣ Root & Health Endpoints
# -------------------------

@app.route("/", methods=["GET"])
def index():
    """Root route for sanity check"""
    return jsonify({"service": "Sara AI Core API", "status": "running"})


@app.route("/health", methods=["GET"])
def health():
    """Deep health check for uptime monitors"""
    log_event(
        service="flask_app",
        event="healthcheck",
        status="ok",
        message="Health endpoint hit",
    )
    return jsonify({"status": "healthy", "service": "flask_app"})


# -------------------------
# 2️⃣ Submit Tasks Endpoints
# -------------------------

@app.route("/inference", methods=["POST"])
def inference():
    """Submit text for LLM inference through Celery"""
    data = request.json or {}
    task = run_inference.delay(data)
    trace_msg = f"Inference task {task.id} submitted to Celery."
    log_event(
        service="flask_app",
        event="task_enqueue",
        status="ok",
        message=trace_msg,
    )
    return jsonify({"task_id": task.id, "status": "submitted"})


@app.route("/tts", methods=["POST"])
def tts():
    """Submit text for text-to-speech processing through Celery"""
    data = request.json or {}
    task = run_tts.delay(data)
    trace_msg = f"TTS task {task.id} submitted to Celery."
    log_event(
        service="flask_app",
        event="task_enqueue",
        status="ok",
        message=trace_msg,
    )
    return jsonify({"task_id": task.id, "status": "submitted"})


# -------------------------
# 3️⃣ Get Task Results Endpoint
# -------------------------

@app.route("/task-result/<task_id>", methods=["GET"])
def task_result(task_id):
    """Fetch result of a Celery task by task ID"""
    # Check both inference and TTS queues
    for celery_task in (run_inference, run_tts):
        task = AsyncResult(task_id, app=celery_task._get_current_app())
        if task.state != "PENDING":
            if task.state == "FAILURE":
                return jsonify({"task_id": task_id, "status": "failure", "error": str(task.result)})
            return jsonify({"task_id": task_id, "status": task.state.lower(), "result": task.result})

    # If task ID not found
    return jsonify({"task_id": task_id, "status": "not_found"}), 404


# -------------------------
# 4️⃣ Main Entry (for local testing)
# -------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
