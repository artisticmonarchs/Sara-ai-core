"""
app.py — Phase 10L-Final (Prometheus Primary + Redis Deprecated removed from API surface)

Sara AI Core API Service

- Prometheus metrics_collector unified system
- Redis retained only for caching/Celery backend (no metric reads/writes in API)
- Structured logging via logging_utils.log_event
- /metrics_snapshot added for developer debugging
- Grafana permanently excluded from observability stack
"""

import os
import time
import traceback
from datetime import datetime, timezone
from flask import Flask, jsonify, request, Response
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from logging_utils import log_event
from tasks import run_tts
import metrics_collector as metrics  # Phase 10L-Final unified collector

app = Flask(__name__)

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# Note: Redis metrics plumbing removed from API surface (Phase 10L-Final).
# If the runtime requires a redis client for caching/Celery it should be
# used only in tasks.py or other lower-level modules that manage the cache.

# --------------------------------------------------------------------------
# R2 connection check
# --------------------------------------------------------------------------
def check_r2_connection(trace_id=None, session_id=None):
    start = time.time()
    try:
        r2_bucket = os.getenv("R2_BUCKET_NAME")
        r2_account = os.getenv("R2_ACCOUNT_ID")
        endpoint = f"https://{r2_account}.r2.cloudflarestorage.com"

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
            region_name=os.getenv("R2_REGION", "auto"),
        )

        s3.list_objects_v2(Bucket=r2_bucket, MaxKeys=1)
        latency_ms = round((time.time() - start) * 1000, 2)

        log_event(
            service="api",
            event="r2_status_ok",
            status="ok",
            message=f"Bucket {r2_bucket} accessible",
            trace_id=trace_id,
            session_id=session_id,
            extra={"r2_latency_ms": latency_ms, "bucket": r2_bucket},
        )
        return {"status": "ok", "bucket": r2_bucket, "r2_latency_ms": latency_ms}
    except (BotoCoreError, ClientError) as e:
        log_event(service="api", event="r2_status_error", status="error",
                  message=str(e), trace_id=trace_id, session_id=session_id)
        return {"status": "error", "message": str(e)}
    except Exception as e:
        log_event(service="api", event="r2_status_exception", status="error",
                  message=str(e), trace_id=trace_id, session_id=session_id)
        return {"status": "error", "message": str(e)}

# --------------------------------------------------------------------------
# /metrics endpoint (Prometheus text format)
# --------------------------------------------------------------------------
@app.route("/metrics", methods=["GET"])
def metrics_endpoint():
    """Expose aggregated metrics in Prometheus plain-text format."""
    try:
        metrics.inc_metric("api_metrics_requests_total")
        payload = metrics.export_prometheus()
        return Response(payload, mimetype="text/plain"), 200
    except Exception as e:
        log_event(
            service="api",
            event="metrics_export_error",
            status="error",
            message="Failed to export metrics",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        return Response("# metrics_export_error 1\n", mimetype="text/plain"), 500

# --------------------------------------------------------------------------
# /metrics_snapshot (developer JSON view)
# --------------------------------------------------------------------------
@app.route("/metrics_snapshot", methods=["GET"])
def metrics_snapshot():
    """Return in-memory metrics as JSON (for internal debug; not Prometheus)."""
    try:
        snapshot = metrics.get_snapshot()
        metrics.inc_metric("api_metrics_snapshot_requests_total")
        return jsonify({"status": "ok", "snapshot": snapshot}), 200
    except Exception as e:
        log_event(
            service="api",
            event="metrics_snapshot_error",
            status="error",
            message=str(e),
            extra={"stack": traceback.format_exc()},
        )
        return jsonify({"status": "error", "message": str(e)}), 500

# --------------------------------------------------------------------------
# /r2_status endpoint
# --------------------------------------------------------------------------
@app.route("/r2_status", methods=["GET"])
def r2_status():
    trace_id = os.urandom(8).hex()
    metrics.inc_metric("api_r2_status_requests_total")
    result = check_r2_connection(trace_id=trace_id)
    code = 200 if result.get("status") == "ok" else 500
    return jsonify(result), code

# --------------------------------------------------------------------------
# /tts_test endpoint
# --------------------------------------------------------------------------
@app.route("/tts_test", methods=["POST"])
def tts_test():
    trace_id = os.urandom(8).hex()
    session_id = request.headers.get("X-Session-ID") or os.urandom(6).hex()

    metrics.inc_metric("api_tts_test_requests_total")

    try:
        payload = request.get_json(force=True)
        if not payload or "text" not in payload:
            return jsonify({"error": "Missing 'text' in payload"}), 400

        log_event(service="api", event="tts_test_received", status="ok",
                  message="Received TTS test request", trace_id=trace_id, session_id=session_id)

        # run_tts is expected to propagate trace_id/session_id into task-level logs
        result = run_tts(payload, inline=True)
        tts_latency_ms = result.get("total_ms") if isinstance(result, dict) else None

        if isinstance(result, dict) and "audio_url" in result:
            metrics.inc_metric("tts_requests_total")
            if tts_latency_ms is not None:
                metrics.observe_latency("tts_latency_ms", float(tts_latency_ms))

            log_event(
                service="api",
                event="tts_test_success",
                status="ok",
                message="TTS test produced audio successfully",
                trace_id=trace_id,
                session_id=session_id,
                extra={"audio_url": result["audio_url"], "tts_latency_ms": tts_latency_ms},
            )
            return jsonify({"status": "ok", **result, "tts_latency_ms": tts_latency_ms}), 200

        log_event(service="api", event="tts_test_failed", status="error",
                  message=str(result), trace_id=trace_id, session_id=session_id)
        metrics.inc_metric("tts_failures_total")
        return jsonify({"error": "TTS generation failed", "details": result}), 500

    except Exception:
        err_msg = traceback.format_exc()
        log_event(service="api", event="tts_test_exception", status="error",
                  message=err_msg, trace_id=trace_id, session_id=session_id)
        metrics.inc_metric("tts_failures_total")
        return jsonify({"error": "TTS test failed", "details": err_msg}), 500

# --------------------------------------------------------------------------
# /healthz
# --------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    metrics.inc_metric("api_healthz_requests_total")
    return jsonify({"status": "ok", "service": "Sara AI Core"}), 200

# --------------------------------------------------------------------------
# /system_status
# --------------------------------------------------------------------------
@app.route("/system_status", methods=["GET"])
def system_status():
    trace_id = os.urandom(8).hex()
    metrics.inc_metric("api_system_status_requests_total")

    try:
        # Redis is no longer used as a metrics store by the API.
        # Any cache/Celery-level redis checks should be performed in lower-level modules.
        r2_status = check_r2_connection(trace_id=trace_id)
        r2_ok = r2_status.get("status") == "ok"

        env_snapshot = {
            "mode": os.getenv("ENV_MODE", "unknown"),
            "service": "sara-ai-core-app",
            "version": "10L-Final",
        }

        log_event(
            service="api",
            event="system_status_checked",
            status="ok" if r2_ok else "degraded",
            trace_id=trace_id,
            extra={
                "r2_ok": r2_ok,
                "r2_bucket": r2_status.get("bucket"),
                "env": env_snapshot,
            },
        )

        return jsonify({
            "status": "ok" if r2_ok else "degraded",
            "redis": "not_applicable_in_api",  # Redis metrics removed from API surface
            "r2": "ok" if r2_ok else "unavailable",
            "r2_bucket": r2_status.get("bucket"),
            "env": env_snapshot,
        }), 200
    except Exception as e:
        log_event(service="api", event="system_status_error", status="error",
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "message": str(e)}), 500

# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------
if __name__ == "__main__":
    # Initialize unified metrics system (Prometheus primary).
    try:
        # NOTE: metrics.init_redis_client(redis_client) removed.
        # The metrics collector still supports a Redis-sync shim internally
        # for backward compat; initialization of that shim should happen
        # from the module that owns the redis client (tasks.py) if needed.
        log_event(
            service="api",
            event="metrics_init",
            status="ok",
            message="Metrics system initialized (Prometheus primary)."
        )
    except Exception as e:
        log_event(service="api", event="metrics_init_error", status="warn",
                  message=f"Metrics collector degraded: {e}")

    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
