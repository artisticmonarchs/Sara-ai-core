from flask import Flask, request, jsonify
from sara_ai.logging_utils import log_event
from sara_ai.sentry_utils import init_sentry

app = Flask(__name__)

# Observability
init_sentry()
trace_id = log_event(service="tts_server", event="startup", status="ok", message="TTS server starting")

@app.route("/tts", methods=["POST"])
def tts():
    data = request.json
    log_event(service="tts_server", event="request_received", status="ok", message=f"TTS request {data}", trace_id=trace_id)
    return jsonify({"status": "ok", "trace_id": trace_id})
