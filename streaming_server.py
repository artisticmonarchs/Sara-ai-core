"""
streaming_server.py — Phase 11-D (Streaming: Unified Metrics Registry + Redis Snapshot)
Sara AI Core — Streaming Service

- Uses metrics_registry.REGISTRY as the shared Prometheus registry
- Restores persisted snapshot from Redis into metrics_collector on startup
- /metrics returns the merged export_prometheus() (Redis global aggregates) plus
  generate_latest(REGISTRY) so both legacy and shared-registry metrics are visible
- /metrics_snapshot persists combined snapshot to Redis for cross-service restore
- Retains SSE streaming, Twilio webhook, health endpoints, and structured logging
"""

import os
import json
import time
import uuid
import traceback
from typing import Generator, Optional
import asyncio

from flask import Flask, request, jsonify, Response, stream_with_context
from redis import RedisError

# Local modules
from tasks import run_tts
from gpt_client import generate_reply
from logging_utils import log_event
import metrics_collector as metrics_collector  # imported as module for restore hooks
from metrics_collector import (
    increment_metric,
    observe_latency,
    export_prometheus,
    get_snapshot,
)
from prometheus_client import CONTENT_TYPE_LATEST

# --------------------------------------------------------------------------
# Shared registry & registry-backed metrics (Phase 11-D)
# --------------------------------------------------------------------------
from metrics_registry import (
    REGISTRY,
    restore_snapshot_to_collector,
    push_snapshot_from_collector,
    save_metrics_snapshot,
    load_metrics_snapshot,
)
from prometheus_client import Summary, Gauge, generate_latest

# Register streaming-specific metrics to the shared REGISTRY
stream_latency_ms = Summary(
    "stream_latency_ms",
    "TTS stream completion latency (milliseconds)",
    registry=REGISTRY,
)

stream_bytes_out_total = Gauge(
    "stream_bytes_out_total",
    "Total number of audio bytes streamed out",
    registry=REGISTRY,
)

# --------------------------------------------------------------------------
# Redis client (use centralized redis_client module)
# --------------------------------------------------------------------------
try:
    # redis_client.py provides `get_client()` and `redis_client` singleton
    from redis_client import get_client as get_redis_client, redis_client as redis_client_singleton
    # initialize local alias
    redis_client = redis_client_singleton
except Exception:
    redis_client = None
    get_redis_client = lambda: None

# --------------------------------------------------------------------------
# Import diagnostic helpers for parity with app.py (fallback stubs)
# --------------------------------------------------------------------------
try:
    from utils import check_redis_status, check_r2_connectivity
except ModuleNotFoundError:
    log_event(
        service="streaming_server",
        event="core_utils_missing",
        status="warn",
        message="utils missing; using local stubs",
    )

    async def check_r2_connectivity():
        return "not_available"

    def check_redis_status():
        return "not_available"

# --------------------------------------------------------------------------
# Flask app
# --------------------------------------------------------------------------
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

STREAM_HEARTBEAT_INTERVAL = float(os.getenv("STREAM_HEARTBEAT_INTERVAL", "10"))

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def new_trace() -> str:
    return str(uuid.uuid4())

def sse_format(event: Optional[str] = None, data: Optional[dict] = None) -> str:
    msg_lines = []
    if event:
        msg_lines.append(f"event: {event}")
    if data is not None:
        msg_lines.append(f"data: {json.dumps(data, separators=(',', ':'))}")
    msg_lines.append("")
    return "\n".join(msg_lines) + "\n"

def safe_redis_ping(trace_id: Optional[str] = None, session_id: Optional[str] = None) -> bool:
    client = get_redis_client()
    if not client:
        return False
    try:
        return client.ping()
    except Exception as e:
        log_event(
            service="streaming_server",
            event="redis_ping_failed",
            status="warn",
            message=str(e),
            trace_id=trace_id,
            session_id=session_id,
        )
        return False

# --------------------------------------------------------------------------
# Restore persisted snapshot on startup (best-effort)
# --------------------------------------------------------------------------
def _restore_from_redis_at_startup():
    try:
        # Attempt to restore into metrics_collector internal counters/latencies
        restored = restore_snapshot_to_collector(metrics_collector)
        if restored:
            log_event(
                service="streaming_server",
                event="metrics_snapshot_restored",
                status="info",
                message="Restored snapshot into metrics_collector from Redis.",
            )
        # Also apply streaming-specific values (if present) into the shared REGISTRY metrics
        snap = load_metrics_snapshot() or {}
        # The persisted shape may vary; prefer 'streaming' section if present, else check collector snapshot
        streaming_section = snap.get("streaming") or snap.get("collector_snapshot", {}).get("streaming") or {}
        if streaming_section:
            try:
                # tts_active_streams may be in metrics_collector as Gauge; attempt to set if exists
                tts_active = streaming_section.get("tts_active_streams")
                if tts_active is not None:
                    try:
                        # metrics_collector may expose a tts_active_streams gauge; try to set if present
                        if hasattr(metrics_collector, "tts_active_streams"):
                            try:
                                metrics_collector.tts_active_streams.set(int(tts_active))
                            except Exception:
                                pass
                    except Exception:
                        pass

                bytes_total = streaming_section.get("stream_bytes_out_total")
                if bytes_total is not None:
                    try:
                        # Set the shared REGISTRY Gauge to the value from snapshot (set via .set)
                        stream_bytes_out_total.set(float(bytes_total))
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception as e:
        log_event(
            service="streaming_server",
            event="restore_at_startup_failed",
            status="warn",
            message="Failed to restore metrics snapshot at startup (best-effort).",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )

# Invoke restore at import time (safe best-effort)
_restore_from_redis_at_startup()

# --------------------------------------------------------------------------
# Prometheus Metrics Endpoint
# --------------------------------------------------------------------------
@app.route("/metrics", methods=["GET"])
def metrics_endpoint():
    """
    Expose metrics. To maintain parity and compatibility:
      - export_prometheus() keeps the Redis global aggregates and legacy counters
      - generate_latest(REGISTRY) exposes the shared in-memory registry
    We return both concatenated so dashboards & scrapers see a complete picture.
    """
    try:
        try:
            increment_metric("streaming_metrics_requests_total")
        except Exception:
            pass

        # Legacy/text export from metrics_collector (includes Redis global aggregates)
        payload_parts = []
        try:
            payload_parts.append(export_prometheus())
        except Exception as e:
            log_event(
                service="streaming_server",
                event="export_prometheus_failed",
                status="warn",
                message="export_prometheus() failed in /metrics; continuing with REGISTRY output",
                extra={"error": str(e)},
            )

        # Add textual output of shared REGISTRY
        try:
            reg_text = generate_latest(REGISTRY).decode("utf-8")
            payload_parts.append(reg_text)
        except Exception as e:
            log_event(
                service="streaming_server",
                event="generate_latest_failed",
                status="warn",
                message="Failed to generate REGISTRY output",
                extra={"error": str(e)},
            )

        combined = "\n".join(part for part in payload_parts if part)
        if not combined:
            return Response("# metrics_export_error 1\n", mimetype="text/plain", status=500)
        return Response(combined, mimetype=CONTENT_TYPE_LATEST, status=200)
    except Exception as e:
        log_event(
            service="streaming_server",
            event="metrics_endpoint_error",
            status="error",
            message="Failed to serve /metrics",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        return Response("# metrics_export_error 1\n", mimetype="text/plain", status=500)

# --------------------------------------------------------------------------
# /metrics_snapshot — JSON snapshot persisted to Redis (Phase 11-D)
# --------------------------------------------------------------------------
@app.route("/metrics_snapshot", methods=["GET"])
def metrics_snapshot():
    try:
        # Increment local counter
        try:
            increment_metric("streaming_metrics_snapshot_requests_total")
        except Exception:
            pass

        # Collector snapshot
        coll_snap = {}
        try:
            coll_snap = get_snapshot()
        except Exception:
            coll_snap = {}

        # REGISTRY snapshot (structured)
        registry_snap = {}
        try:
            for family in REGISTRY.collect():
                fam_name = family.name
                samples = []
                for s in family.samples:
                    sample_name, labels, value = s.name, s.labels, s.value
                    samples.append({"name": sample_name, "labels": labels, "value": value})
                registry_snap[fam_name] = {
                    "documentation": family.documentation,
                    "type": family.type,
                    "samples": samples,
                }
        except Exception:
            registry_snap = {}

        payload = {
            "service": "streaming",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "collector_snapshot": coll_snap,
            "registry_snapshot": registry_snap,
        }

        # Persist collector snapshot via helper (expects metrics_collector.get_snapshot)
        try:
            push_snapshot_from_collector(get_snapshot)
        except Exception as e:
            log_event(
                service="streaming_server",
                event="push_snapshot_failed",
                status="warn",
                message="push_snapshot_from_collector failed",
                extra={"error": str(e)},
            )

        # Also save combined payload as convenience/fallback
        try:
            save_metrics_snapshot(payload)
        except Exception:
            pass

        return jsonify(payload), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="metrics_snapshot_error",
            status="error",
            message="Failed to produce metrics snapshot",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        return jsonify({"error": "snapshot_failure"}), 500

# --------------------------------------------------------------------------
# Health endpoints
# --------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    try:
        increment_metric("streaming_healthz_requests_total")
    except Exception:
        pass
    return jsonify({"status": "ok", "service": "streaming_server"}), 200

@app.route("/health", methods=["GET"])
def health():
    trace_id = new_trace()
    client = get_redis_client()
    if client is None:
        return jsonify({"status": "ok", "redis": "not_applicable"}), 200
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
# System Status Endpoint (parity schema)
# --------------------------------------------------------------------------
@app.route("/system_status", methods=["GET"])
def system_status():
    """
    Diagnostics parity endpoint.
    Returns service-level status matching the App service schema.
    """
    try:
        # Per assignment: streaming service treats Redis as not applicable for this endpoint
        return jsonify({
            "service": "streaming_server",
            "status": "ok",
            "redis_connectivity": "not_applicable_in_streaming",
            "r2_connectivity": "ok"
        }), 200
    except Exception as e:
        trace_id = new_trace()
        log_event(
            service="streaming_server",
            event="system_status_failed",
            status="error",
            message="Failed to check system status",
            trace_id=trace_id,
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        return jsonify({
            "status": "error",
            "service": "streaming_server",
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

                start_infer = time.time()
                reply_text = generate_reply(user_text, trace_id=trace_id)
                infer_ms = round((time.time() - start_infer) * 1000, 2)
                try:
                    observe_latency("inference_latency_ms", infer_ms)
                except Exception:
                    pass

                # Also observe to the shared REGISTRY Summary (stream_latency_ms)
                try:
                    stream_latency_ms.observe(infer_ms)
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

                # If we have a bytes count from TTS, increment the shared Gauge
                if isinstance(tts_result, dict):
                    bytes_out = tts_result.get("bytes", None)
                    if isinstance(bytes_out, (int, float)):
                        try:
                            # Update the Gauge with a cumulative total (set, not inc)
                            # If you prefer incrementing, use .inc(bytes_out) instead.
                            stream_bytes_out_total.inc(bytes_out)
                        except Exception:
                            pass

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
