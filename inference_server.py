"""
inference_server.py — Phase 6 Ready (Flattened Structure)
Handles lightweight inference requests for Sara AI Core.
"""

import os
from flask import Flask, request, jsonify
from logging_utils import log_event          # ✅ fixed import
from sentry_utils import init_sentry         # ✅ fixed import

# Initialize Sentry
init_sentry()

app = Flask(__name__)


@app.route("/infer", methods=["POST"])
def infer():
    """Handle incoming inference requests."""
    payload = request.json
    log_event(
        service="inference_server",
        event="infer_request",
        status="ok",
        message="Received inference request",
        extra={"payload": payload},
    )
    return jsonify({"result": "This is a dummy inference response"})


if __name__ == "__main__":
    port = int(os.getenv("INFERENCE_PORT", 7000))
    log_event(
        service="inference_server",
        event="startup",
        status="ok",
        message=f"Listening on port {port}",
    )
    app.run(host="0.0.0.0", port=port)
