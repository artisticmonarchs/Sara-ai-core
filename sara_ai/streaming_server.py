import os
import asyncio
from flask import Flask, request, jsonify
from sara_ai.logging_utils import log_event
from sara_ai.tasks import run_inference, run_tts

app = Flask(__name__)
PORT = int(os.getenv("PORT", 8002))

# ---------------------------------------------------------------------
# ✅ Startup banner
# ---------------------------------------------------------------------
try:
    log_event(
        service="streaming_server",
        event="startup",
        status="ok",
        message="Streaming server initialized and ready.",
        extra={
            "port": PORT,
            "env": os.getenv("RENDER_SERVICE_NAME", "local"),
        },
    )
except Exception as e:
    print(f"⚠️ Startup log_event failed: {e}")

# ---------------------------------------------------------------------
# ✅ Healthcheck endpoint
# ---------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "streaming_server"}), 200

# ---------------------------------------------------------------------
# ✅ Inference endpoint
# ---------------------------------------------------------------------
@app.route("/inference", methods=["POST"])
async def inference():
    data = request.get_json(force=True)
    trace_id = data.get("trace_id")
    log_event(
        service="streaming_server",
        event="inference_request",
        status="received",
        message="Inference request received.",
        extra={"trace_id": trace_id},
    )

    try:
        result = await asyncio.to_thread(run_inference, data)
        log_event(
            service="streaming_server",
            event="inference_response",
            status="success",
            message="Inference completed successfully.",
            extra={"trace_id": trace_id},
        )
        return jsonify(result), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="inference_error",
            status="error",
            message=str(e),
            extra={"trace_id": trace_id},
        )
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------------------------
# ✅ TTS endpoint
# ---------------------------------------------------------------------
@app.route("/tts", methods=["POST"])
async def tts():
    data = request.get_json(force=True)
    trace_id = data.get("trace_id")
    log_event(
        service="streaming_server",
        event="tts_request",
        status="received",
        message="TTS request received.",
        extra={"trace_id": trace_id},
    )

    try:
        result = await asyncio.to_thread(run_tts, data)
        log_event(
            service="streaming_server",
            event="tts_response",
            status="success",
            message="TTS generation completed successfully.",
            extra={"trace_id": trace_id},
        )
        return jsonify(result), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="tts_error",
            status="error",
            message=str(e),
            extra={"trace_id": trace_id},
        )
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
