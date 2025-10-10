# --------------------------------------------------------
# app.py — Sara AI Core (Ready-to-Deploy)
# --------------------------------------------------------

import logging
from flask import Flask, jsonify, request
from tasks import run_inference, run_tts
from logging_utils import log_event
from celery.result import AsyncResult
from celery_app import celery

app = Flask(__name__)
logger = logging.getLogger("flask_app")

@app.route("/", methods=["GET"])
def index():
    """Root route for sanity check"""
    return jsonify({"service": "Sara AI Core API", "status": "running"})

@app.route("/health", methods=["GET"])
def health():
    """Health check for uptime monitors"""
    log_event(service="flask_app", event="healthcheck", status="ok", message="Health endpoint hit")
    return jsonify({"status": "healthy", "service": "flask_app"})

@app.route("/inference", methods=["POST"])
def inference():
    """Submit text for LLM inference through Celery"""
    data = request.json or {}
    task = run_inference.delay(data)
    log_event(service="flask_app", event="task_enqueue", status="ok", message=f"Inference task {task.id} submitted to Celery.")
    return jsonify({"task_id": task.id, "status": "submitted"})

@app.route("/tts", methods=["POST"])
def tts():
    """Submit text for text-to-speech processing through Celery"""
    data = request.json or {}
    task = run_tts.delay(data)
    log_event(service="flask_app", event="task_enqueue", status="ok", message=f"TTS task {task.id} submitted to Celery.")
    return jsonify({"task_id": task.id, "status": "submitted"})

@app.route("/task-result/<task_id>", methods=["GET"])
def task_result(task_id):
    """Fetch Celery task result safely"""
    task = AsyncResult(task_id, app=celery)
    if task.state == "PENDING":
        return jsonify({"task_id": task_id, "status": "pending"})
    elif task.state == "FAILURE":
        return jsonify({"task_id": task_id, "status": "failure", "error": str(task.result)})
    elif task.state == "SUCCESS":
        return jsonify({"task_id": task_id, "status": "success", "result": task.result})
    else:
        return jsonify({"task_id": task_id, "status": task.state})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
