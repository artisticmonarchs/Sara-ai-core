"""
streaming_server.py — Phase 11-C (Streaming Diagnostics & Global Metrics Parity)
Sara AI Core — Streaming Service

- Integrates global Prometheus export via metrics_collector.export_prometheus()
- Adds /metrics and /metrics_snapshot endpoints
- Adds /system_status endpoint (Phase 11-C) for Redis + R2 connectivity
- Retains structured logging, SSE streaming, and Twilio webhook endpoints
"""

import os
import json
import time
import uuid
import traceback
from typing import Generator, Optional

from flask import Flask, request, jsonify, Response, stream_with_context
from redis import Redis, RedisError

from tasks import run_tts
from gpt_client import generate_reply
from logging_utils import log_event
from metrics_collector import (
    increment_metric,
    observe_latency,
    export_prometheus,
    get_snapshot,
)
from prometheus_client import CONTENT_TYPE_LATEST

# Phase 11-C: Import diagnostic helpers for parity with app.py
from core.utils import check_redis_status, check_r2_connectivity

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_HEARTBEAT_INTERVAL = float(os.getenv("STREAM_HEARTBEAT_INTERVAL", "10"))

# Redis client (safe init)
try:
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
except Exception as e:
    redis_client = None
    log_event(
        service="streaming_server",
        event="redis_init_failed",
        status="error",
        message="Failed to initialize Redis client at startup",
        extra={"error": str(e)},
    )

# --------------------------------------------------------------------------
# Utility helpers
# --------------------------------------------------------------------------
def new_trace() -> str:
    return str(uuid.uuid4())


def sse_format(event: Optional[str] = None, data: Optional[dict] = None) -> str:
    """Formats a Server-Sent Events message."""
    msg_lines = []
    if event:
        msg_lines.append(f"event: {event}")
    if data is not None:
        msg_lines.append(f"data: {json.dumps(data, separators=(',', ':'))}")
    msg_lines.append("")  # blank line terminator
    return "\n".join(msg_lines) + "\n"


def safe_redis_ping(trace_id: Optional[str] = None, session_id: Optional[str] = None) -> bool:
    if not redis_client:
        return False
    try:
        return redis_client.ping()
    except RedisError:
        return False
    except Exception as e:
        log_event(
            service="streaming_server",
            event="redis_ping_exception",
            status="error",
            message="Unexpected exception during Redis ping",
            trace_id=trace_id,
            session_id=session_id,
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        return False

# --------------------------------------------------------------------------
# Prometheus Metrics Endpoints
# --------------------------------------------------------------------------
@app.route("/metrics", methods=["GET"])
def metrics_endpoint():
    """Expose Prometheus metrics via the unified exporter."""
    try:
        increment_metric("streaming_metrics_requests_total")
        payload = export_prometheus()
        return Response(payload, mimetype=CONTENT_TYPE_LATEST), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="metrics_export_error",
            status="error",
            message="Failed to export Prometheus metrics",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        return Response("# metrics_export_error 1\n", mimetype="text/plain"), 500


@app.route("/metrics_snapshot", methods=["GET"])
def metrics_snapshot():
    """Expose live JSON snapshot of key metrics (for API validation)."""
    try:
        snapshot = get_snapshot()
        return jsonify(snapshot), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="metrics_snapshot_error",
            status="error",
            message="Failed to generate metrics snapshot",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        return jsonify({"error": "snapshot_failure"}), 500

# --------------------------------------------------------------------------
# Health endpoints
# --------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    """Lightweight healthcheck for orchestrator/load balancer (fast)."""
    try:
        increment_metric("streaming_healthz_requests_total")
    except Exception:
        pass
    return jsonify({"status": "ok", "service": "streaming_server"}), 200


@app.route("/health", methods=["GET"])
def health():
    """Detailed health (checks Redis connectivity)."""
    trace_id = new_trace()
    try:
        ok = safe_redis_ping(trace_id=trace_id)
        if ok:
            return jsonify({"status": "ok", "redis": "connected"}), 200
        else:
            log_event(
                service="streaming_server",
                event="health_degraded",
                status="warn",
                message="Redis unreachable during health check",
                trace_id=trace_id,
            )
            return jsonify({"status": "degraded", "redis": "unreachable"}), 503
    except Exception as e:
        log_event(
            service="streaming_server",
            event="health_exception",
            status="error",
            message="Health endpoint failure",
            trace_id=trace_id,
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        return jsonify({"status": "degraded", "redis": "error"}), 503

# --------------------------------------------------------------------------
# Phase 11-C: System Status Endpoint
# --------------------------------------------------------------------------
@app.route("/system_status", methods=["GET"])
async def system_status():
    """Returns JSON status of Redis and R2 connectivity for streaming service."""
    try:
        redis_status = "not_applicable_in_streaming"
        r2_status = await check_r2_connectivity()
        return jsonify({
            "status": "ok",
            "service": "streaming",
            "redis_status": redis_status,
            "r2_status": r2_status,
        }), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="system_status_failed",
            status="error",
            message="Failed to check system status",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        return jsonify({
            "status": "error",
            "service": "streaming",
            "message": str(e),
        }), 500

# --------------------------------------------------------------------------
# SSE Streaming Endpoint
# --------------------------------------------------------------------------
@app.route("/stream", methods=["POST"])
def stream():
    trace_id = None
    session_id = None

    try:
        increment_metric("stream_requests_total")
    except Exception:
        pass

    try:
        data = request.get_json(force=True)
        session_id = data.get("session_id") or str(uuid.uuid4())
        trace_id = data.get("trace_id") or new_trace()
        user_text = (data.get("text") or data.get("message") or "").strip()

        if not user_text:
            log_event(
                service="streaming_server",
                event="stream_rejected",
                status="error",
                message="No input text provided",
                trace_id=trace_id,
                session_id=session_id,
            )
            increment_metric("stream_rejected_total")
            return jsonify({"error": "No input text"}), 400

        log_event(
            service="streaming_server",
            event="stream_start",
            status="ok",
            message=f"Stream session started ({len(user_text)} chars)",
            trace_id=trace_id,
            session_id=session_id,
        )

        @stream_with_context
        def event_stream() -> Generator[str, None, None]:
            try:
                yield sse_format("status", {"stage": "received", "trace_id": trace_id})

                # Inference
                start_infer = time.time()
                reply_text = generate_reply(user_text, trace_id=trace_id)
                infer_ms = round((time.time() - start_infer) * 1000, 2)
                try:
                    observe_latency("inference_latency_ms", infer_ms)
                except Exception:
                    pass

                log_event(
                    service="streaming_server",
                    event="inference_done",
                    status="ok",
                    message=f"Inference completed ({len(reply_text)} chars)",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={"inference_latency_ms": infer_ms},
                )
                yield sse_format("reply_text", {"text": reply_text})

                # TTS generation
                start_tts = time.time()
                tts_result = run_tts(
                    {"text": reply_text, "trace_id": trace_id, "session_id": session_id},
                    inline=True,
                )
                tts_ms = round((time.time() - start_tts) * 1000, 2)

                if isinstance(tts_result, dict) and tts_result.get("error"):
                    increment_metric("tts_failures_total")
                    log_event(
                        service="streaming_server",
                        event="tts_failed",
                        status="error",
                        message="TTS generation failed",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={"tts_result": tts_result, "tts_latency_ms": tts_ms},
                    )
                    yield sse_format("error", tts_result)
                    return

                if isinstance(tts_result, dict) and tts_result.get("cached"):
                    increment_metric("tts_cache_hits_total")
                    log_event(
                        service="streaming_server",
                        event="cache_hit",
                        status="ok",
                        message="TTS result served from cache",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={"cached": True, "tts_latency_ms": tts_ms},
                    )

                audio_url = tts_result.get("audio_url") if isinstance(tts_result, dict) else None
                log_event(
                    service="streaming_server",
                    event="tts_done",
                    status="ok",
                    message="TTS generated successfully",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={"audio_url": audio_url, "tts_latency_ms": tts_ms},
                )

                yield sse_format("audio_ready", {"url": audio_url})
                yield sse_format("complete", {"trace_id": trace_id, "session_id": session_id})

            except Exception as exc:
                err_id = str(uuid.uuid4())
                log_event(
                    service="streaming_server",
                    event="stream_exception",
                    status="error",
                    message=str(exc),
                    trace_id=trace_id or new_trace(),
                    session_id=session_id or "unknown",
                    extra={"error_id": err_id, "stack": traceback.format_exc()},
                )
                yield sse_format("error", {"message": "Streaming error", "error_id": err_id})

        headers = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        return Response(event_stream(), headers=headers)

    except Exception as e:
        trace = trace_id or new_trace()
        log_event(
            service="streaming_server",
            event="fatal_error",
            status="error",
            message=str(e),
            trace_id=trace,
            session_id=session_id or "unknown",
            extra={"stack": traceback.format_exc()},
        )
        increment_metric("stream_errors_total")
        return jsonify({"error": "Internal server error", "trace_id": trace}), 500

# --------------------------------------------------------------------------
# Twilio webhook compatibility
# --------------------------------------------------------------------------
@app.route("/twilio_tts", methods=["POST"])
def twilio_tts():
    """Generate TTS for Twilio webhook calls and return TwiML <Play>."""
    from flask import Response as TwilioResponse

    trace_id = new_trace()
    session_id = str(uuid.uuid4())

    try:
        increment_metric("twilio_requests_total")
    except Exception:
        pass

    try:
        text = request.form.get("SpeechResult") or request.form.get("text") or "Hello from Sara AI"
        tts_result = run_tts({"text": text, "session_id": session_id, "trace_id": trace_id}, inline=True)

        if isinstance(tts_result, dict) and tts_result.get("error"):
            increment_metric("tts_failures_total")
            log_event(
                service="streaming_server",
                event="twilio_tts_failed",
                status="error",
                message="TTS generation for Twilio failed",
                trace_id=trace_id,
                session_id=session_id,
                extra={"tts_result": tts_result},
            )
            return TwilioResponse(
                "<Response><Say>Sorry, an error occurred generating speech.</Say></Response>",
                mimetype="application/xml",
            )

        audio_url = tts_result.get("audio_url") if isinstance(tts_result, dict) else None
        log_event(
            service="streaming_server",
            event="twilio_tts_done",
            status="ok",
            message="Generated Twilio-compatible TTS",
            trace_id=trace_id,
            session_id=session_id,
            extra={"audio_url": audio_url},
        )

        twiml = f"<Response><Play>{audio_url}</Play></Response>"
        return TwilioResponse(twiml, mimetype="application/xml")

    except Exception as e:
        log_event(
            service="streaming_server",
            event="twilio_tts_exception",
            status="error",
            message="Twilio TTS handler error",
            trace_id=trace_id,
            session_id=session_id,
            extra={"stack": traceback.format_exc()},
        )
        increment_metric("twilio_errors_total")
        return TwilioResponse(
            "<Response><Say>Sorry, an internal error occurred.</Say></Response>",
            mimetype="application/xml",
        )

# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 7000))
    app.run(host="0.0.0.0", port=port, threaded=True)
