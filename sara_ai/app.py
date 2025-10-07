import logging
from flask import Flask, jsonify, request
from sara_ai.tasks import process_event

app = Flask(__name__)
logger = logging.getLogger("flask_app")

@app.route("/", methods=["GET"])
def index():
    return jsonify({"service": "Sara AI Flask API", "status": "running"})

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "flask_app"})

@app.route("/inference", methods=["POST"])
def inference():
    data = request.json or {}
    task = process_event.delay(data)
    return jsonify({"task_id": task.id, "status": "submitted"})
