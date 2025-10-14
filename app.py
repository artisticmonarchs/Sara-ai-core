# --- Existing imports remain unchanged ---
import os
import traceback
import boto3
from flask import Flask, jsonify, request
from botocore.exceptions import BotoCoreError, ClientError

from logging_utils import log_event
from redis_client import redis_client
from tasks import run_tts

app = Flask(__name__)

# -----------------------------------------------------------------------------
# Temporary: R2 diagnostic helper (for Phase 10I validation only)
# -----------------------------------------------------------------------------
@app.route("/r2_status", methods=["GET"])
def r2_status():
    """
    Verify Cloudflare R2 configuration and bucket accessibility.
    Safe for manual inspection during Phase 10I.
    """
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

        # Simple list to confirm connectivity
        s3.list_objects_v2(Bucket=r2_bucket, MaxKeys=1)
        log_event(service="api", event="r2_status_ok", status="ok", message=f"Bucket {r2_bucket} accessible")
        return jsonify({"status": "ok", "bucket": r2_bucket}), 200

    except (BotoCoreError, ClientError) as e:
        log_event(service="api", event="r2_status_error", status="error", message=str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

    except Exception as e:
        log_event(service="api", event="r2_status_exception", status="error", message=str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


# -----------------------------------------------------------------------------
# Metrics route aligned with new Redis schema (Phase 10I)
# -----------------------------------------------------------------------------
@app.route("/metrics", methods=["GET"])
def metrics():
    """Collect lightweight operational metrics from Redis."""
    try:
        metrics_data = {
            "files_generated": int(redis_client.hget("metrics:tts", "files_generated") or 0),
            "uploads": int(redis_client.hget("metrics:tts", "uploads") or 0),
            "bytes_uploaded": int(redis_client.hget("metrics:tts", "bytes_uploaded") or 0),
        }
        log_event(service="api", event="metrics_fetch_ok", status="ok", extra=metrics_data)
        return jsonify({"service": "Sara AI Core", "status": "ok", "metrics": metrics_data}), 200

    except Exception as e:
        log_event(service="api", event="metrics_fetch_error", status="error", message=str(e))
        return jsonify({"status": "error", "message": "Failed to fetch metrics"}), 500


# -----------------------------------------------------------------------------
# Enhanced /tts_test for explicit audio_url visibility (Phase 10I)
# -----------------------------------------------------------------------------
@app.route("/tts_test", methods=["POST"])
def tts_test():
    """Direct Deepgram TTS test (used by internal tools)."""
    try:
        payload = request.get_json(force=True)
        if not payload or "text" not in payload:
            return jsonify({"error": "Missing 'text' in payload"}), 400

        log_event(service="api", event="tts_test_received", status="ok", message="Received TTS test request")

        result = run_tts(payload, inline=True)

        # Add explicit success log and ensure audio_url is visible
        if isinstance(result, dict) and "audio_url" in result:
            log_event(
                service="api",
                event="tts_test_success",
                status="ok",
                message="TTS test produced audio successfully",
                extra={"audio_url": result["audio_url"]},
            )
            return jsonify({"status": "ok", **result}), 200

        log_event(service="api", event="tts_test_failed", status="error", message=str(result))
        return jsonify({"error": "TTS generation failed", "details": result}), 500

    except Exception:
        err_msg = traceback.format_exc()
        log_event(service="api", event="tts_test_exception", status="error", message=err_msg)
        return jsonify({"error": "TTS test failed", "details": err_msg}), 500


# -----------------------------------------------------------------------------
# Healthcheck endpoint (unchanged, but left here for completeness)
# -----------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    """Lightweight health probe."""
    return jsonify({"status": "ok", "service": "Sara AI Core"}), 200

# -----------------------------------------------------------------------------
# Diagnostic Endpoints (Phase 10J)
# -----------------------------------------------------------------------------

import time
from r2_client import check_r2_connection  # Ensure this helper exists in your R2 util

@app.route("/redis_status", methods=["GET"])
def redis_status():
    """Check Redis latency and basic metrics."""
    if not redis_client:
        return jsonify({"status": "error", "message": "Redis not initialized"}), 500

    try:
        start = time.time()
        pong = redis_client.ping()
        latency_ms = round((time.time() - start) * 1000, 2)

        metrics = {
            "files_generated": int(redis_client.hget("metrics:tts", "files_generated") or 0),
            "uploads": int(redis_client.hget("metrics:tts", "uploads") or 0),
            "bytes_uploaded": int(redis_client.hget("metrics:tts", "bytes_uploaded") or 0),
        }

        status = "ok" if pong else "degraded"
        return jsonify({"status": status, "latency_ms": latency_ms, "metrics": metrics}), 200
    except Exception as e:
        log_event(service="api", event="redis_status_error", status="error", message=str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/system_status", methods=["GET"])
def system_status():
    """Comprehensive system-level diagnostics."""
    try:
        redis_ok = safe_redis_ping()
        r2_ok = False
        r2_bucket = None
        try:
            r2_status = check_r2_connection()
            if isinstance(r2_status, dict):
                r2_ok = r2_status.get("status") == "ok"
                r2_bucket = r2_status.get("bucket")
        except Exception:
            r2_ok = False

        env_snapshot = {
            "mode": os.getenv("ENV_MODE", "unknown"),
            "service": "sara-ai-core-app",
            "version": "1.0.0",
        }

        return jsonify({
            "status": "ok" if redis_ok and r2_ok else "degraded",
            "redis": "connected" if redis_ok else "unreachable",
            "r2": "ok" if r2_ok else "unavailable",
            "r2_bucket": r2_bucket,
            "env": env_snapshot,
        }), 200
    except Exception as e:
        log_event(service="api", event="system_status_error", status="error", message=str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
