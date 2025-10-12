"""
inference_server.py — Phase 10B (Trace-propagation + Metrics + Source-aware)
Upgraded from Phase 8:
- Uses the central celery app (sara_ai.celery_app.celery) for consistent broker/backend
- Propagates trace_id from caller (streaming_ws / tests) into Celery payloads
- Accepts `source` in payload (e.g. "twilio", "seo_test")
- Records lightweight Redis metrics (counts + latency buckets)
- Keeps structured logging via log_event() and Sentry integration
- Render-safe / env-driven configuration
"""

import os
import time
import json
import traceback
from typing import Tuple, Optional

from flask import Flask, request, jsonify
from werkzeug.exceptions import HTTPException

from logging_utils import log_event, get_trace_id
from sentry_utils import init_sentry
from celery.result import AsyncResult

# Use the shared Celery instance so tasks route consistently across services
from celery_app import celery

# Redis for metrics storage
import redis

# --------------------------------------------------------------------------
# Initialization & Config
# --------------------------------------------------------------------------
init_sentry()

app = Flask(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
INFERENCE_PORT = int(os.getenv("INFERENCE_PORT", 7000))
SARA_ENV = os.getenv("SARA_ENV", "development")
SERVICE_NAME = "inference_server"

# Metrics keys/namespaces
INFERENCE_METRICS_HASH = "metrics:inference"
LATENCY_BUCKET_PREFIX = "metrics:inference:latency_ms"  # optional detailed buckets

# Redis client (used only for lightweight metrics; safe if unavailable)
redis_client = None
try:
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    # quick ping to ensure availability (non-fatal)
    try:
        redis_client.ping()
        log_event(service=SERVICE_NAME, event="redis_metrics_ready", message="Redis metrics client connected")
    except Exception:
        log_event(service=SERVICE_NAME, event="redis_metrics_unavailable", level="WARNING",
                  message="Redis for metrics not reachable; metrics will be skipped")
        redis_client = None
except Exception:
    redis_client = None

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def validate_payload(payload: dict) -> Tuple[bool, Optional[str]]:
    """
    Validate basic payload schema.
    Accepts both voice transcripts and structured inputs for SEO testing.
    """
    if not isinstance(payload, dict):
        return False, "Invalid JSON format"
    # We accept either 'input' or 'text' as the primary content carrier
    if "input" not in payload and "text" not in payload:
        return False, "Missing required field: 'input' or 'text'"
    content = payload.get("input") or payload.get("text")
    if not isinstance(content, (str, list, dict)):
        return False, "'input' / 'text' must be string, list, or dict"
    if "model" in payload and not isinstance(payload["model"], str):
        return False, "'model' must be a string if provided"
    return True, None

def _record_metrics(latency_ms: float, source: str = "unknown"):
    """
    Record lightweight metrics in Redis:
    - increment counts
    - increment total latency (ms) as integer
    - optionally increment latency bucket keys
    This is best-effort and non-fatal if Redis not available.
    """
    try:
        if not redis_client:
            return
        # increment total requests
        redis_client.hincrby(INFERENCE_METRICS_HASH, "requests_total", 1)
        # increment per-source counters
        redis_client.hincrby(f"{INFERENCE_METRICS_HASH}:by_source", source, 1)
        # increment cumulative latency (store as integer ms)
        redis_client.hincrby(INFERENCE_METRICS_HASH, "latency_ms_total", int(round(latency_ms)))
        # bucket (example buckets: 0-100,100-300,300-1000,1000+)
        bucket = "gt1000"
        if latency_ms <= 100:
            bucket = "0_100"
        elif latency_ms <= 300:
            bucket = "101_300"
        elif latency_ms <= 1000:
            bucket = "301_1000"
        redis_client.hincrby(f"{LATENCY_BUCKET_PREFIX}", bucket, 1)
    except Exception:
        # non-fatal; best-effort
        log_event(service=SERVICE_NAME, event="metrics_record_failed", level="WARNING",
                  message="Failed to record metrics to Redis (non-fatal)")

# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.route("/infer", methods=["POST"])
def infer():
    """
    Accept inference requests (voice transcripts, SEO test payloads, or direct API calls).
    This endpoint:
      - preserves or creates a trace_id
      - validates payload
      - dispatches a Celery run_inference task with trace propagation
      - records lightweight latency metrics (task dispatch time)
    Payload accepted shapes:
    {
      "text": "transcript or prompt text",
      "input": ... (alternate field),
      "model": "gpt-5-mini",
      "source": "twilio" | "seo_test" | "web",
      "trace_id": "..." (optional),
      "session_id": "..." (optional)
    }
    """
    start_ts = time.time()
    incoming = request.get_json(silent=True) or {}

    # prefer provided trace_id; otherwise generate one
    trace_id = incoming.get("trace_id") or get_trace_id()
    source = incoming.get("source", "unknown")

    log_event(
        service=SERVICE_NAME,
        event="infer_request_received",
        status="ok",
        message="Inference request received",
        trace_id=trace_id,
        extra={"payload_preview": str(incoming)[:200], "source": source},
    )

    is_valid, error = validate_payload(incoming)
    if not is_valid:
        log_event(
            service=SERVICE_NAME,
            event="infer_request_invalid",
            status="error",
            message=error,
            trace_id=trace_id,
            extra={"payload_preview": str(incoming)[:200]},
        )
        return jsonify({"error": error, "trace_id": trace_id}), 400

    # Build canonical payload to send to Celery
    content = incoming.get("input") or incoming.get("text")
    canonical = {
        "trace_id": trace_id,
        "source": source,
        "session_id": incoming.get("session_id"),
        "input": content,
        "model": incoming.get("model", "gpt-5-mini"),
        "meta": incoming.get("meta", {}),
        "received_at": start_ts,
    }

    try:
        # non-blocking dispatch; tasks should honor trace_id in payload
        task = celery.send_task("run_inference", args=[canonical])
        dispatch_latency_ms = (time.time() - start_ts) * 1000.0

        # metrics (best-effort)
        _record_metrics(dispatch_latency_ms, source=source)

        log_event(
            service=SERVICE_NAME,
            event="task_dispatched",
            status="ok",
            message="Inference task dispatched to Celery",
            trace_id=trace_id,
            extra={"task_id": task.id, "dispatch_latency_ms": round(dispatch_latency_ms, 2), "source": source},
        )
        return jsonify({"task_id": task.id, "trace_id": trace_id}), 202

    except Exception as e:
        err = traceback.format_exc()
        log_event(
            service=SERVICE_NAME,
            event="task_dispatch_failed",
            status="error",
            message=str(e),
            trace_id=trace_id,
            extra={"traceback": err},
        )
        return jsonify({"error": "Task dispatch failed", "trace_id": trace_id}), 503


@app.route("/task-status/<task_id>", methods=["GET"])
def get_task_status(task_id):
    """Poll Celery task status (with safe backend handling)."""
    trace_id = get_trace_id()
    try:
        task_result = celery.AsyncResult(task_id)
        state = task_result.state or "UNKNOWN"

        if state == "PENDING":
            response = {"status": "pending"}
        elif state == "SUCCESS":
            response = {"status": "success", "result": task_result.result}
        elif state == "FAILURE":
            response = {"status": "failure", "error": str(task_result.info or "unknown failure")}
        else:
            response = {"status": state}

        log_event(
            service=SERVICE_NAME,
            event="task_status_checked",
            status="ok",
            message=f"Checked status for task {task_id}",
            trace_id=trace_id,
            extra={"state": state},
        )
        return jsonify(response), 200

    except Exception as e:
        log_event(
            service=SERVICE_NAME,
            event="task_status_error",
            status="error",
            message=f"Task status check failed: {e}",
            trace_id=trace_id,
        )
        return jsonify({"error": "Failed to fetch task status", "trace_id": trace_id}), 503


@app.route("/health", methods=["GET"])
def health():
    """Redis/Celery health check."""
    trace_id = get_trace_id()
    try:
        # quick broker check
        try:
            conn = celery.connection()
            conn.connect()
            conn.release()
        except Exception as e:
            log_event(
                service=SERVICE_NAME,
                event="broker_unavailable",
                status="error",
                message=f"Broker connection failed: {e}",
                trace_id=trace_id,
            )
            return jsonify({"service": SERVICE_NAME, "status": "unhealthy"}), 503

        # Redis ping for metrics (non-fatal)
        redis_ok = True
        if redis_client:
            try:
                redis_client.ping()
            except Exception:
                redis_ok = False

        log_event(
            service=SERVICE_NAME,
            event="health_check",
            status="ok",
            message="Service healthy (Celery broker connected)",
            trace_id=trace_id,
            extra={"redis_metrics": "ok" if redis_ok else "unavailable"},
        )
        return jsonify({"service": SERVICE_NAME, "status": "healthy", "redis_metrics": redis_ok}), 200

    except Exception as e:
        log_event(
            service=SERVICE_NAME,
            event="health_check_failed",
            status="error",
            message=str(e),
            trace_id=trace_id,
        )
        return jsonify({"service": SERVICE_NAME, "status": "unhealthy"}), 503


# --------------------------------------------------------------------------
# Global error handling
# --------------------------------------------------------------------------
@app.errorhandler(Exception)
def handle_exception(e):
    trace_id = get_trace_id()
    status_code = 500
    if isinstance(e, HTTPException):
        status_code = e.code
    log_event(
        service=SERVICE_NAME,
        event="unhandled_exception",
        status="error",
        message=str(e),
        trace_id=trace_id,
    )
    return jsonify({"error": str(e), "trace_id": trace_id}), status_code


# --------------------------------------------------------------------------
# Local debug entry point (Render uses Gunicorn)
# --------------------------------------------------------------------------
if __name__ == "__main__":
    log_event(
        service=SERVICE_NAME,
        event="startup",
        status="ok",
        message=f"Starting {SERVICE_NAME} on port {INFERENCE_PORT} ({SARA_ENV})",
    )
    app.run(host="0.0.0.0", port=INFERENCE_PORT)
