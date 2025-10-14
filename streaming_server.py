"""
streaming_server.py — Phase 10H (R2-integrated)

Enhancements:
- Retains full Phase 8 structured logging, Redis health checks, async Celery dispatch
- TTS + inference tasks now generate and return Cloudflare R2 public URLs (no local file usage)
- Simplified /stream endpoint for async orchestration between services (Render-compatible)
- All logs enriched with trace_id, session_id, and R2 public URLs for debugging
"""

import os
import time
import traceback
from typing import Optional
from flask import Flask, request, jsonify
from celery.result import AsyncResult
import redis
from logging_utils import log_event, get_trace_id
from celery_app import celery
from tasks import run_inference, run_tts

# --------------------------------------------------------------------------
# Config (env-driven)
# --------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PORT = int(os.getenv("PORT", 10000))
SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "sara-ai-core-streaming")
SARA_ENV = os.getenv("SARA_ENV", "development")
PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "")
REDIS_RETRY_MAX = int(os.getenv("REDIS_RETRY_MAX", 5))
REDIS_RETRY_DELAY = float(os.getenv("REDIS_RETRY_DELAY", 1.0))  # seconds

# --------------------------------------------------------------------------
# App setup
# --------------------------------------------------------------------------
app = Flask(__name__)

def _render_stdout_mode():
    if SARA_ENV.lower() == "production":
        import logging
        root = logging.getLogger()
        stream_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        if not stream_handlers:
            stream = logging.StreamHandler()
            stream.setFormatter(logging.Formatter("%(message)s"))
            root.handlers = [stream]
        else:
            root.handlers = stream_handlers

_render_stdout_mode()

# --------------------------------------------------------------------------
# Redis client with retry/backoff helper
# --------------------------------------------------------------------------
def make_redis_client(url: str):
    return redis.Redis.from_url(url, socket_connect_timeout=5, socket_timeout=5)

redis_client = make_redis_client(REDIS_URL)

def redis_ping_with_retries(max_retries: int = REDIS_RETRY_MAX, delay: float = REDIS_RETRY_DELAY) -> bool:
    attempt = 0
    while attempt <= max_retries:
        try:
            if redis_client.ping():
                log_event(
                    service="streaming_server",
                    event="redis_ping",
                    message=f"Redis reachable (attempt {attempt})",
                    trace_id=get_trace_id(),
                    extra={"redis_url_preview": REDIS_URL.split('//')[-1], "attempt": attempt},
                )
                return True
        except Exception as e:
            log_event(
                service="streaming_server",
                event="redis_ping_failed",
                level="WARNING",
                message=f"Redis ping failed on attempt {attempt}: {e}",
                trace_id=get_trace_id(),
                extra={"attempt": attempt, "error": str(e)},
            )
        attempt += 1
        time.sleep(delay * attempt)
    log_event(
        service="streaming_server",
        event="redis_unreachable",
        level="ERROR",
        message=f"Redis unreachable after {max_retries} attempts.",
        trace_id=get_trace_id(),
        extra={"redis_url_preview": REDIS_URL.split('//')[-1]},
    )
    return False


# --------------------------------------------------------------------------
# Startup health log
# --------------------------------------------------------------------------
if not redis_ping_with_retries():
    log_event(
        service="streaming_server",
        event="startup",
        level="ERROR",
        message="Redis connection could not be established during startup.",
        trace_id=get_trace_id(),
        extra={"service": SERVICE_NAME, "port": PORT, "redis_url": REDIS_URL},
    )
else:
    log_event(
        service="streaming_server",
        event="startup",
        message="Connected to Redis at startup",
        trace_id=get_trace_id(),
        extra={"service": SERVICE_NAME, "port": PORT, "redis_url_preview": REDIS_URL.split('//')[-1]},
    )


# --------------------------------------------------------------------------
# Task dispatcher
# --------------------------------------------------------------------------
def dispatch_task(task_type: str, payload: dict) -> dict:
    trace_id = payload.get("trace_id") or get_trace_id()
    session_id = payload.get("session_id")

    try:
        if task_type == "tts":
            task = run_tts.apply_async(args=[payload])
        elif task_type == "inference":
            task = run_inference.apply_async(args=[payload])
        else:
            task_name = os.getenv("STREAMING_GENERIC_TASK", "streaming.process")
            task = celery.send_task(task_name, args=[payload], kwargs={})
        log_event(
            service="streaming_server",
            event="task_dispatched",
            message=f"Dispatched {task_type} task",
            trace_id=trace_id,
            session_id=session_id,
            extra={"task_type": task_type, "task_id": getattr(task, "id", None)},
        )
        return {"task_id": getattr(task, "id", None), "trace_id": trace_id}
    except Exception as e:
        log_event(
            service="streaming_server",
            event="task_dispatch_error",
            level="ERROR",
            message=f"Failed to dispatch task {task_type}: {e}",
            trace_id=trace_id,
            session_id=session_id,
            extra={"task_type": task_type, "error": str(e), "traceback": traceback.format_exc()},
        )
        raise


# --------------------------------------------------------------------------
# Health endpoints
# --------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
@app.route("/healthz", methods=["GET"])
def health():
    ok = redis_ping_with_retries(max_retries=1, delay=0.5)
    if ok:
        return jsonify({"status": "ok", "redis": "connected", "r2_host": PUBLIC_AUDIO_HOST}), 200
    else:
        return jsonify({"status": "error", "redis": "unreachable"}), 500


# --------------------------------------------------------------------------
# /stream — orchestrates TTS or inference tasks
# --------------------------------------------------------------------------
@app.route("/stream", methods=["POST"])
def handle_stream():
    payload = request.get_json(silent=True) or {}
    trace_id = payload.get("trace_id") or get_trace_id()
    task_type = payload.get("task", "unknown")

    log_event(
        service="streaming_server",
        event="stream_request_received",
        message=f"Received stream request for {task_type}",
        trace_id=trace_id,
        extra={"task_type": task_type},
    )

    if not redis_ping_with_retries(max_retries=1, delay=0.5):
        log_event(
            service="streaming_server",
            event="stream_rejected_redis_unavailable",
            level="ERROR",
            message="Rejecting stream request because Redis is unreachable",
            trace_id=trace_id,
        )
        return jsonify({"status": "error", "reason": "redis_unreachable", "trace_id": trace_id}), 503

    if not isinstance(payload, dict) or "task" not in payload:
        return jsonify({"status": "error", "reason": "invalid_payload", "trace_id": trace_id}), 400

    try:
        dispatch_info = dispatch_task(task_type, payload)
        log_event(
            service="streaming_server",
            event="stream_accepted",
            message="Stream request accepted and dispatched",
            trace_id=trace_id,
            extra={"task_type": task_type, "task_id": dispatch_info.get("task_id")},
        )
        return jsonify({
            "status": "accepted",
            "trace_id": dispatch_info.get("trace_id"),
            "task_id": dispatch_info.get("task_id"),
            "r2_host": PUBLIC_AUDIO_HOST,
        }), 202
    except Exception as e:
        log_event(
            service="streaming_server",
            event="stream_dispatch_failed",
            level="ERROR",
            message=f"Failed to dispatch stream task: {e}",
            trace_id=trace_id,
            extra={"traceback": traceback.format_exc()},
        )
        return jsonify({"status": "error", "reason": "dispatch_failed", "trace_id": trace_id}), 500


# --------------------------------------------------------------------------
# Task status polling (for Twilio or front-end)
# --------------------------------------------------------------------------
@app.route("/task-status/<task_id>", methods=["GET"])
def task_status(task_id: str):
    try:
        result = AsyncResult(task_id, app=celery)
        status = result.state
        payload = {"task_id": task_id, "status": status}
        if status == "PENDING":
            payload["info"] = "pending"
        elif status == "FAILURE":
            payload["info"] = str(result.result)
        else:
            payload["result"] = result.result
            if isinstance(result.result, dict) and "audio_url" in result.result:
                payload["audio_url"] = result.result["audio_url"]

        log_event(
            service="streaming_server",
            event="task_status_checked",
            message=f"Checked status for {task_id}",
            trace_id=get_trace_id(),
            extra={"task_id": task_id, "status": status},
        )
        return jsonify(payload), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="task_status_error",
            level="ERROR",
            message=f"Error checking task status: {e}",
            trace_id=get_trace_id(),
            extra={"task_id": task_id, "error": str(e), "traceback": traceback.format_exc()},
        )
        return jsonify({"status": "error", "reason": "status_check_failed"}), 500


# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------
if __name__ == "__main__":
    log_event(
        service="streaming_server",
        event="startup_complete",
        message=f"Starting streaming server on port {PORT}",
        trace_id=get_trace_id(),
        extra={"service": SERVICE_NAME, "r2_host": PUBLIC_AUDIO_HOST},
    )
    app.run(host="0.0.0.0", port=PORT)
