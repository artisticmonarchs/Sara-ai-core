# --------------------------------------------------------
# app.py — Sara AI Core (Ready-to-Deploy, Celery Result Fixed)
# --------------------------------------------------------

import logging
from flask import Flask, jsonify, request
from tasks import run_inference, run_tts  # Your Celery tasks
from celery_app import celery                # Your Celery app instance
from celery.result import AsyncResult
from logging_utils import log_event         # Optional: your logging utils

app = Flask(__name__)
logger = logging.getLogger("flask_app")


# -------------------------------
# Basic Health & Root Endpoints
# -------------------------------

@app.route("/", methods=["GET"])
def index():
    return jsonify({"service": "Sara AI Core API", "status": "running"})


@app.route("/health", methods=["GET"])
def health():
    log_event(
        service="flask_app",
        event="healthcheck",
        status="ok",
        message="Health endpoint hit",
    )
    return jsonify({"service": "flask_app", "status": "healthy"})


# -------------------------------
# Submit Celery Tasks
# -------------------------------

@app.route("/inference", methods=["POST"])
def inference():
    data = request.json or {}
    task = run_inference.delay(data)
    trace_msg = f"Inference task {task.id} submitted to Celery."
    log_event(service="flask_app", event="task_enqueue", status="ok", message=trace_msg)
    return jsonify({"task_id": task.id, "status": "submitted"})


@app.route("/tts", methods=["POST"])
def tts():
    data = request.json or {}
    task = run_tts.delay(data)
    trace_msg = f"TTS task {task.id} submitted to Celery."
    log_event(service="flask_app", event="task_enqueue", status="ok", message=trace_msg)
    return jsonify({"task_id": task.id, "status": "submitted"})


# -------------------------------
# Fetch Celery Task Results
# -------------------------------

@app.route("/task-result/<task_id>", methods=["GET"])
def task_result(task_id):
    task = AsyncResult(task_id, app=celery)  # Pass the Celery app instance here

    if task.state == "PENDING":
        return jsonify({"status": "pending", "task_id": task_id})
    elif task.state == "FAILURE":
        return jsonify({"status": "failure", "task_id": task_id, "error": str(task.result)})
    else:  # SUCCESS
        return jsonify({"status": "success", "task_id": task_id, "result": task.result})


# -------------------------------
# Main Entry (Optional)
# -------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
