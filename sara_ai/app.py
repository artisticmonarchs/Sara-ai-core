import logging
from flask import Flask, jsonify, request
from sara_ai.tasks import example_task  # or run_inference / run_tts once merged
from sara_ai.logging_utils import log_event

app = Flask(__name__)
logger = logging.getLogger("flask_app")

@app.route("/", methods=["GET"])
def index():
    return jsonify({"service": "Sara AI Flask API", "status": "running"})

@app.route("/health", methods=["GET"])
def health():
    log_event(service="flask_app", event="healthcheck", status="ok", message="Health endpoint hit")
    return jsonify({"status": "healthy", "service": "flask_app"})

@app.route("/inference", methods=["POST"])
def inference():
    data = request.json or {}
    task = example_task.delay(data)
    trace_msg = f"Task {task.id} submitted to Celery."
    log_event(service="flask_app", event="task_enqueue", status="ok", message=trace_msg)
    return jsonify({"task_id": task.id, "status": "submitted"})
