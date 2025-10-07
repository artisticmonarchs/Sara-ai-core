import logging
from flask import Flask, jsonify, request
from sara_ai.tasks import run_inference, run_tts
from sara_ai.logging_utils import log_event

app = Flask(__name__)
logger = logging.getLogger("flask_app")


# ------------------------------------------------------------
# Startup Log
# ------------------------------------------------------------
log_event(
    service="flask_app",
    event="startup",
    message="Sara AI Flask API service started successfully.",
)


# ------------------------------------------------------------
# Routes
# ------------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    return jsonify({"service": "Sara AI Flask API", "status": "running"})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "flask_app"})


@app.route("/inference", methods=["POST"])
def inference():
    data = request.json or {}
    task = run_inference.delay(data)
    log_event(
        service="flask_app",
        event="enqueue_task",
        message=f"Inference task queued",
        extra={"task_id": task.id},
    )
    return jsonify({"task_id": task.id, "status": "submitted"})


@app.route("/tts", methods=["POST"])
def tts():
    data = request.json or {}
    task = run_tts.delay(data)
    log_event(
        service="flask_app",
        event="enqueue_task",
        message=f"TTS task queued",
        extra={"task_id": task.id},
    )
    return jsonify({"task_id": task.id, "status": "submitted"})


# ------------------------------------------------------------
# Main Entry Point (for local debug)
# ------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
