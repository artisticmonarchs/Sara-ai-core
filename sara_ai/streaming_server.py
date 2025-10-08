import os
import asyncio
import traceback
from flask import Flask, request, jsonify
import redis

from sara_ai.logging_utils import log_event
from sara_ai.tasks import run_inference, run_tts

# -------------------------------------------------------------------
# Flask App Setup
# -------------------------------------------------------------------
app = Flask(__name__)

# -------------------------------------------------------------------
# Environment + Redis Initialization
# -------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT = int(os.getenv("PORT", 5003))

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

try:
    redis_client.ping()
    log_event(
        service="streaming_server",
        event="startup",
        status="ok",
        message=f"Redis client initialized and reachable at {REDIS_URL}",
    )
except Exception as e:
    log_event(
        service="streaming_server",
        event="startup_error",
        status="failed",
        message="Redis connection failed during startup",
        error=str(e),
        stacktrace=traceback.format_exc(),
    )
    raise SystemExit(1)

# -------------------------------------------------------------------
# Routes
# -------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health_check():
    try:
        redis_client.ping()
        return jsonify({"status": "healthy", "redis": "connected"}), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="healthcheck_error",
            status="failed",
            message="Redis healthcheck failed",
            error=str(e),
        )
        return jsonify({"status": "unhealthy", "error": str(e)}), 500


@app.route("/inference", methods=["POST"])
async def inference():
    try:
        data = request.json or {}
        task_id = run_inference.delay(data)
        log_event(
            service="streaming_server",
            event="enqueue_inference",
            status="queued",
            message=f"Inference task enqueued ({task_id})",
        )
        return jsonify({"status": "queued", "task_id": str(task_id)}), 202
    except Exception as e:
        log_event(
            service="streaming_server",
            event="inference_enqueue_error",
            status="failed",
            message="Error enqueueing inference task",
            error=str(e),
            stacktrace=traceback.format_exc(),
        )
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/tts", methods=["POST"])
async def tts():
    try:
        data = request.json or {}
        task_id = run_tts.delay(data)
        log_event(
            service="streaming_server",
            event="enqueue_tts",
            status="queued",
            message=f"TTS task enqueued ({task_id})",
        )
        return jsonify({"status": "queued", "task_id": str(task_id)}), 202
    except Exception as e:
        log_event(
            service="streaming_server",
            event="tts_enqueue_error",
            status="failed",
            message="Error enqueueing TTS task",
            error=str(e),
            stacktrace=traceback.format_exc(),
        )
        return jsonify({"status": "error", "error": str(e)}), 500


# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------
if __name__ == "__main__":
    log_event(
        service="streaming_server",
        event="startup_banner",
        status="ok",
        message=f"Streaming Server running on port {PORT}",
    )
    app.run(host="0.0.0.0", port=PORT)
