"""
app.py — Phase 11-D (Unified Prometheus Registry + Redis Snapshot)
Sara AI Core API Service

- Uses a centralized Prometheus REGISTRY (metrics_registry.REGISTRY)
- Persists metrics_collector snapshots to Redis via metrics_registry helpers
- Restores persisted metrics on startup and starts the global metrics sync engine
- Keeps existing endpoints and behavior (R2 checks, TTS, health, logging)
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

# Use the centralized metrics_collector module (so we can restore into it)
import metrics_collector as metrics  # type: ignore
from metrics_collector import (
    increment_metric,
    export_prometheus,
    observe_latency,
    get_snapshot,
)
# Phase 11-D: unified registry + snapshot persistence helpers & registry
from metrics_registry import REGISTRY, push_snapshot_from_collector, save_metrics_snapshot, restore_snapshot_to_collector  # type: ignore
from prometheus_client import generate_latest

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# --------------------------------------------------------------------------
# Restore persisted metrics into metrics_collector (best-effort)
# --------------------------------------------------------------------------
try:
    try:
        restored_ok = restore_snapshot_to_collector(metrics)
        if restored_ok:
            log_event(service="api", event="metrics_restored_on_startup", status="info",
                      message="Restored persisted metrics into metrics_collector at startup")
    except Exception as e:
        log_event(service="api", event="metrics_restore_failed", status="warn",
                  message="Failed to restore metrics snapshot at startup",
                  extra={"error": str(e), "stack": traceback.format_exc()})
except Exception:
    # Fail silently if metrics_registry not available (maintain backward compatibility)
    pass

# --------------------------------------------------------------------------
# Start background global metrics sync (Phase 11-D) — best-effort, non-blocking
# --------------------------------------------------------------------------
try:
    from global_metrics_store import start_background_sync  # type: ignore
    try:
        start_background_sync(service_name="app")
        log_event(service="api", event="global_metrics_sync_started", status="ok",
                  message="Background global metrics sync started for app service")
    except Exception as e:
        log_event(service="api", event="global_metrics_sync_failed", status="error",
                  message="Failed to start background metrics sync",
                  extra={"error": str(e), "stack": traceback.format_exc()})
except Exception:
    # If global_metrics_store not present, continue — metrics still work locally
    log_event(service="api", event="global_metrics_sync_unavailable", status="warn",
              message="global_metrics_store not available; global sync disabled")


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        try:
            increment_metric("api_metrics_requests_total")
        except Exception:
            pass

        # export_prometheus now returns a canonical global exposition (Phase 11-D)
        payload = export_prometheus()
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
# /metrics_snapshot (developer JSON view) + persist to Redis (Phase 11-D)
# --------------------------------------------------------------------------
@app.route("/metrics_snapshot", methods=["GET"])
def metrics_snapshot():
    """
    Return an enriched JSON snapshot:
      - metrics_collector.get_snapshot() (counters + latencies)
      - textual REGISTRY output (generate_latest(REGISTRY))
    Persist the snapshot to Redis via metrics_registry.push_snapshot_from_collector
    and save a combined payload via save_metrics_snapshot for cross-service restore.
    """
    try:
        try:
            increment_metric("api_metrics_snapshot_requests_total")
        except Exception:
            pass

        # Collector snapshot (counters + latencies) — best-effort
        collector_snap = {}
        try:
            collector_snap = get_snapshot()
        except Exception as e:
            log_event(
                service="api",
                event="collector_snapshot_failed",
                status="warn",
                message="metrics_collector.get_snapshot() failed",
                extra={"error": str(e)},
            )

        # REGISTRY textual dump for debugging (prometheus text format)
        registry_text = ""
        try:
            registry_text = generate_latest(REGISTRY).decode("utf-8")
        except Exception as e:
            log_event(
                service="api",
                event="registry_generate_failed",
                status="warn",
                message="Failed to generate REGISTRY text output",
                extra={"error": str(e)},
            )

        # Combined snapshot payload
        payload = {
            "service": "sara-ai-core-app",
            "timestamp": _now_iso(),
            "collector_snapshot": collector_snap,
            "registry_text": registry_text,
        }

        # Persist using helper that expects metrics_collector.get_snapshot
        try:
            pushed = push_snapshot_from_collector(get_snapshot)
            if not pushed:
                log_event(
                    service="api",
                    event="push_snapshot_warn",
                    status="warn",
                    message="push_snapshot_from_collector reported False (not saved).",
                )
        except Exception as e:
            log_event(
                service="api",
                event="push_snapshot_failed",
                status="error",
                message="Failed to push snapshot from collector to Redis",
                extra={"error": str(e), "stack": traceback.format_exc()},
            )

        # Also save the combined payload as a convenience (fallback)
        try:
            save_metrics_snapshot(payload)
        except Exception:
            # save_metrics_snapshot logs internally; don't escalate here
            pass

        return jsonify({"status": "ok", "snapshot": payload}), 200
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
    try:
        increment_metric("api_r2_status_requests_total")
    except Exception:
        pass
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

    try:
        increment_metric("api_tts_test_requests_total")
    except Exception:
        pass

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
            try:
                increment_metric("tts_requests_total")
            except Exception:
                pass

            if tts_latency_ms is not None:
                try:
                    observe_latency("tts_latency_ms", float(tts_latency_ms))
                except Exception:
                    pass

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
        try:
            increment_metric("tts_failures_total")
        except Exception:
            pass
        return jsonify({"error": "TTS generation failed", "details": result}), 500

    except Exception:
        err_msg = traceback.format_exc()
        log_event(service="api", event="tts_test_exception", status="error",
                  message=err_msg, trace_id=trace_id, session_id=session_id)
        try:
            increment_metric("tts_failures_total")
        except Exception:
            pass
        return jsonify({"error": "TTS test failed", "details": err_msg}), 500


# --------------------------------------------------------------------------
# /healthz
# --------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    try:
        increment_metric("api_healthz_requests_total")
    except Exception:
        pass
    return jsonify({"status": "ok", "service": "Sara AI Core"}), 200


# --------------------------------------------------------------------------
# /system_status
# --------------------------------------------------------------------------
@app.route("/system_status", methods=["GET"])
def system_status():
    trace_id = os.urandom(8).hex()
    try:
        increment_metric("api_system_status_requests_total")
    except Exception:
        pass

    try:
        r2_status = check_r2_connection(trace_id=trace_id)
        r2_ok = r2_status.get("status") == "ok"

        env_snapshot = {
            "mode": os.getenv("ENV_MODE", "unknown"),
            "service": "sara-ai-core-app",
            "version": "11-D",
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
            "redis": "not_applicable_in_api",
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
        log_event(
            service="api",
            event="metrics_init",
            status="ok",
            message="Unified metrics registry ready (metrics_registry.REGISTRY)."
        )
    except Exception as e:
        log_event(service="api", event="metrics_init_error", status="warn",
                  message=f"Metrics collector degraded: {e}")

    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
