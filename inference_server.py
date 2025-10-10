import os
from flask import Flask, request, jsonify
from sara_ai.logging_utils import log_event
from sara_ai.sentry_utils import init_sentry

# Initialize Sentry
init_sentry()

app = Flask(__name__)

@app.route("/infer", methods=["POST"])
def infer():
    payload = request.json
    log_event(service="inference_server", event="infer_request", status="ok", message="Received inference request")
    return jsonify({"result": "This is a dummy inference response"})

if __name__ == "__main__":
    port = int(os.getenv("INFERENCE_PORT", 7000))
    log_event(service="inference_server", event="startup", status="ok", message=f"Listening on {port}")
    app.run(host="0.0.0.0", port=port)
