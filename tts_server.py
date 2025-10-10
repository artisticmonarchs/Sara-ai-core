"""
tts_server.py — Phase 6 Ready (Flattened Structure)
Handles text-to-speech request endpoints for Sara AI Core.
"""

from flask import Flask, request, jsonify
from logging_utils import log_event       # ✅ fixed import
from sentry_utils import init_sentry      # ✅ fixed import

app = Flask(__name__)

# Observability setup
init_sentry()
trace_id = log_event(
    service="tts_server",
    event="startup",
    status="ok",
    message="TTS server starting"
)


@app.route("/tts", methods=["POST"])
def tts():
    """Handle incoming TTS requests."""
    data = request.json or {}
    log_event(
        service="tts_server",
        event="request_received",
        status="ok",
        message=f"TTS request {data}",
        trace_id=trace_id,
    )
    return jsonify({"status": "ok", "trace_id": trace_id})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000)
