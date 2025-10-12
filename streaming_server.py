"""
streaming_server.py — Phase 8 Render Merge Draft

Upgraded from Phase 7:
- Phase 8 structured logging + trace propagation (log_event)
- Async task dispatch to Celery (non-blocking)
- Robust Redis health checks & retry logic
- Environment-driven configuration (PORT, REDIS_URL, SERVICE_NAME)
- Render-compatible stdout-only logging mode (when SARA_ENV=production)
- Placeholders/hooks for future streaming/TTS modules
"""

import os
import time
import uuid
import traceback
from typing import Optional

from flask import Flask, request, jsonify
from celery.result import AsyncResult

import redis

from logging_utils import log_event, get_trace_id
from celery_app import celery
# Optional: import known tasks. If future streaming-specific tasks exist, wire them here.
from tasks import run_inference, run_tts

# --------------------------------------------------------------------------
# Config (env-driven)
# --------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PORT = int(os.getenv("PORT", 10000))
SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "sara-ai-core-streaming")
SARA_ENV = os.getenv("SARA_ENV", "development")
REDIS_RETRY_MAX = int(os.getenv("REDIS_RETRY_MAX", 5))
REDIS_RETRY_DELAY = float(os.getenv("REDIS_RETRY_DELAY", 1.0))  # seconds

# --------------------------------------------------------------------------
# App setup
# --------------------------------------------------------------------------
app = Flask(__name__)


def _render_stdout_mode():
    """
    If running in production/Render, prefer stdout-only logging to integrate with Render logs.
    This function adjusts the root logger handlers to preserve existing behavior elsewhere,
    but ensures this service's output appears on stdout for platform ingestion.
    """
    if SARA_ENV.lower() == "production":
        import logging
        root = logging.getLogger()
        # Keep only StreamHandlers (stdout)
        stream_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        if not stream_handlers:
            # Add a basic stream handler if none present
            stream = logging.StreamHandler()
            stream.setFormatter(logging.Formatter("%(message)s"))
            root.handlers = [stream]
        else:
            root.handlers = stream_handlers


# Apply Render stdout-only logging if requested
_render_stdout_mode()

# --------------------------------------------------------------------------
# Redis client with retry/backoff helper
# --------------------------------------------------------------------------
def make_redis_client(url: str):
    return redis.Redis.from_url(url, socket_connect_timeout=5, socket_timeout=5)


redis_client = make_redis_client(REDIS_URL)


def redis_ping_with_retries(max_retries: int = REDIS_RETRY_MAX, delay: float = REDIS_RETRY_DELAY) -> bool:
    """
    Try to ping Redis with simple retry/backoff. Returns True if successful, False otherwise.
    All failures are logged via log_event.
    """
    attempt = 0
    while attempt <= max_retries:
        try:
            if redis_client.ping():
                log_event(
                    service="streaming_server",
                    event="redis_ping",
                    message=f"Redis reachable (attempt {attempt})",
                    trace_id=get_trace_id(),
                    extra={"redis_url_preview": REDIS_URL.split("//")[-1], "attempt": attempt},
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
        time.sleep(delay * attempt)  # simple backoff
    log_event(
        service="streaming_server",
        event="redis_unreachable",
        level="ERROR",
        message=f"Redis unreachable after {max_retries} attempts.",
        trace_id=get_trace_id(),
        extra={"redis_url_preview": REDIS_URL.split("//")[-1]},
    )
    return False


# Startup health check (non-blocking)
if not redis_ping_with_retries():
    # Keep startup but mark in logs; in Render this will appear in service logs.
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
# Helpers for task dispatching
# --------------------------------------------------------------------------
def dispatch_task(task_type: str, payload: dict) -> dict:
    """
    Dispatch to appropriate Celery task based on task_type.
    Returns a dict with task metadata including task_id.
    Non-blocking: does not wait for result.
    """
    trace_id = payload.get("trace_id") or get_trace_id()

    try:
        if task_type == "tts":
            task = run_tts.apply_async(args=[payload])
        elif task_type == "inference":
            task = run_inference.apply_async(args=[payload])
        else:
            # Future-proof: send a generic task name to Celery if implemented
            task_name = os.getenv("STREAMING_GENERIC_TASK", "streaming.process")
            # send_task allows dispatch of tasks not imported locally
            task = celery.send_task(task_name, args=[payload], kwargs={})
        log_event(
            service="streaming_server",
            event="task_dispatched",
            message=f"Dispatched {task_type} task",
            trace_id=trace_id,
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
            extra={"task_type": task_type, "error": str(e), "traceback": traceback.format_exc()},
        )
        raise


# --------------------------------------------------------------------------
# Health endpoints
# --------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
@app.route("/healthz", methods=["GET"])
def health():
    """
    Health check endpoint that verifies Redis connectivity.
    Returns 200 when Redis reachable; 500 otherwise.
    """
    ok = redis_ping_with_retries(max_retries=1, delay=0.5)
    if ok:
        return jsonify({"status": "ok", "redis": "connected"}), 200
    else:
        return jsonify({"status": "error", "redis": "unreachable"}), 500


# --------------------------------------------------------------------------
# Streaming endpoint (non-blocking)
# --------------------------------------------------------------------------
@app.route("/stream", methods=["POST"])
def handle_stream():
    """
    Accepts streaming requests, validates payload, enqueues a Celery task and returns trace_id + task_id.
    Does not perform long-running work in the Flask thread.
    """
    payload = request.get_json(silent=True) or {}
    trace_id = payload.get("trace_id") or get_trace_id()
    task_type = payload.get("task", "unknown")

    # Log receipt
    log_event(
        service="streaming_server",
        event="stream_request_received",
        message="Received stream request",
        trace_id=trace_id,
        extra={"task_type": task_type},
    )

    # Redis sanity check before dispatch
    if not redis_ping_with_retries(max_retries=1, delay=0.5):
        # immediate failure response — Redis must be available in Render-first mode
        log_event(
            service="streaming_server",
            event="stream_rejected_redis_unavailable",
            level="ERROR",
            message="Rejecting stream request because Redis is unreachable",
            trace_id=trace_id,
            extra={"task_type": task_type},
        )
        return jsonify({"status": "error", "reason": "redis_unreachable", "trace_id": trace_id}), 503

    # Validate minimal payload
    if not isinstance(payload, dict) or "task" not in payload:
        log_event(
            service="streaming_server",
            event="stream_invalid_payload",
            level="WARNING",
            message="Invalid stream payload",
            trace_id=trace_id,
            extra={"payload_preview": str(payload)[:200]},
        )
        return jsonify({"status": "error", "reason": "invalid_payload", "trace_id": trace_id}), 400

    try:
        dispatch_info = dispatch_task(task_type, payload)
        # Log the dispatch success
        log_event(
            service="streaming_server",
            event="stream_accepted",
            message="Stream request accepted and dispatched",
            trace_id=trace_id,
            extra={"task_type": task_type, "task_id": dispatch_info.get("task_id")},
        )
        return jsonify({"status": "accepted", "trace_id": dispatch_info.get("trace_id"), "task_id": dispatch_info.get("task_id")}), 202

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
# Optional: Task result polling endpoint (non-blocking for UI/QA)
# --------------------------------------------------------------------------
@app.route("/task-status/<task_id>", methods=["GET"])
def task_status(task_id: str):
    """
    Light-weight endpoint to check Celery task status using the configured backend.
    In Render-first mode, this relies on the Celery result backend being reachable.
    """
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
# Entrypoint note: Render should run this service via Gunicorn or similar.
# Keep __main__ for local debugging only, but Phase 8 enforces Render-first operation.
# --------------------------------------------------------------------------
if __name__ == "__main__":
    log_event(
        service="streaming_server",
        event="startup_complete",
        message=f"Starting streaming server on port {PORT}",
        trace_id=get_trace_id(),
        extra={"service": SERVICE_NAME},
    )
    # Use Flask's built-in server only for rare local debugging (not for Render)
    app.run(host="0.0.0.0", port=PORT)
