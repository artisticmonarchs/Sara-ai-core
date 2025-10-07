import logging
from flask import Flask, jsonify, request
from sara_ai.tasks import process_event, run_inference, run_tts
from sara_ai.logging_utils import log_event

app = Flask(__name__)
logger = logging.getLogger("flask_app")

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

@app.route("/process", methods=["POST"])
def process():
    """Generic event processor route"""
    data = request.json or {}
    task = process_event.delay(data)
    trace_msg = f"Process task {task.id} submitted to Celery."
    log_event(
        service="flask_app",
        event="task_enqueue",
        status="ok",
        message=trace_msg,
    )
    return jsonify({"task_id": task.id, "status": "submitted"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
