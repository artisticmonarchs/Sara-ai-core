"""
streaming_ws.py — Sara AI Core (Phase 11-D)
Real-time Twilio → Deepgram ASR Bridge with Full Observability
"""

import os
import io
import json
import base64
import asyncio
import websockets
import uuid
import time
from flask import Flask, request
from flask_sock import Sock

# Phase 11-D Canonical Imports
from config import Config
from logging_utils import log_event, get_trace_id
from sentry_utils import init_sentry, capture_exception_safe
from redis_client import get_redis_client, safe_redis_operation
from metrics_collector import increment_metric, observe_latency
from global_metrics_store import start_background_sync
from celery_app import celery

# -------------------------------------------------------------------
# Compliance Metadata
# -------------------------------------------------------------------
__phase__ = "11-D"
__service__ = "streaming_ws"
__schema_version__ = "phase_11d_v1"

# -------------------------------------------------------------------
# Initialization
# -------------------------------------------------------------------
init_sentry()
start_background_sync(service_name=__service__)

app = Flask(__name__)
sock = Sock(app)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
DEEPGRAM_API_KEY = getattr(Config, "DEEPGRAM_API_KEY", os.getenv("DEEPGRAM_API_KEY"))
DG_LISTEN_MODEL = getattr(Config, "DEEPGRAM_LISTEN_MODEL", "nova-3")

# Twilio 8 kHz PCM stream parameters
DEEPGRAM_WS_BASE = (
    f"wss://api.deepgram.com/v1/listen?"
    f"model={DG_LISTEN_MODEL}&encoding=linear16&sample_rate=8000"
)

# -------------------------------------------------------------------
# Circuit Breaker Implementation (Canonical Pattern)
# -------------------------------------------------------------------
def _is_circuit_breaker_open(service: str = "streaming_ws") -> bool:
    """Canonical circuit breaker check used across Phase 11-D services."""
    try:
        client = get_redis_client()
        if not client:
            return False
        key = f"circuit_breaker:{service}:state"
        state = safe_redis_operation(lambda: client.get(key))
        
        # Record circuit breaker check metric
        try:
            increment_metric("streaming_ws_circuit_breaker_checks_total")
        except Exception:
            pass
            
        if state is None:
            return False
        if isinstance(state, bytes):
            try:
                state = state.decode("utf-8")
            except Exception:
                pass
        return str(state).lower() == "open"
    except Exception:
        # Best-effort: do not block on checker errors
        return False

# -------------------------------------------------------------------
# Metrics Recording Helper
# -------------------------------------------------------------------
def _record_metrics(event_type: str, status: str, latency_ms: float = None, trace_id: str = None):
    """Record standardized metrics for WebSocket operations (seconds for latency)."""
    try:
        # Counters (best-effort)
        try:
            increment_metric(f"streaming_ws_{event_type}_{status}_total")
        except Exception:
            pass

        # Histogram / latency: convert ms -> seconds for Prometheus
        if latency_ms is not None:
            try:
                latency_s = float(latency_ms) / 1000.0
                observe_latency(f"streaming_ws_{event_type}_latency_seconds", latency_s)
                # Add aggregate total latency view
                if event_type != "total":
                    observe_latency("streaming_ws_total_latency_seconds", latency_s)
            except Exception:
                # fallback to legacy ms metric if needed
                try:
                    observe_latency(f"streaming_ws_{event_type}_latency_ms", float(latency_ms))
                except Exception:
                    pass
    except Exception as e:
        log_event(
            service=__service__,
            event="metrics_record_failed",
            status="warn",
            message="Failed to record streaming WebSocket metrics",
            trace_id=trace_id or get_trace_id(),
            extra={"error": str(e), "schema_version": __schema_version__}
        )

# -------------------------------------------------------------------
# Structured Logging Helper
# -------------------------------------------------------------------
def _structured_log(event: str, level: str = "info", message: Optional[str] = None,
                    trace_id: Optional[str] = None, session_id: Optional[str] = None, **extra):
    log_event(service=__service__, event=event, status=level, message=message or event,
              trace_id=trace_id or get_trace_id(), 
              extra={**({"session_id": session_id} if session_id else {}), **extra, "schema_version": __schema_version__})

# -------------------------------------------------------------------
# Utility Functions
# -------------------------------------------------------------------
def _resolve_trace_id() -> str:
    """Resolve or generate trace ID from request context."""
    return get_trace_id() or str(uuid.uuid4())

async def enqueue_inference(transcript: str, trace_id: str, session_id: str):
    """Dispatch transcript to Celery for Sara inference with safety wrappers."""
    start_time = time.time()
    
    # Circuit breaker check
    if _is_circuit_breaker_open("inference"):
        try:
            increment_metric("streaming_ws_circuit_breaker_hits_total")
        except Exception:
            pass
        _structured_log("inference_circuit_breaker_blocked", level="warn",
                        message="Inference dispatch blocked by circuit breaker", 
                        trace_id=trace_id, session_id=session_id)
        return

    payload = {
        "trace_id": trace_id,
        "text": transcript,
        "session_id": session_id,
        "source": "twilio",
        "timestamp": time.time(),
        "schema_version": __schema_version__
    }
    
    try:
        # Use safe Redis operation for session mapping
        call_sid = safe_redis_operation(
            lambda: get_redis_client().get(f"twilio_call:{session_id}"),
            fallback=None,
            operation_name="get_call_sid"
        )
        if call_sid:
            payload["call_sid"] = call_sid.decode('utf-8') if isinstance(call_sid, bytes) else call_sid

        celery.send_task(
            "sara_ai.tasks.streaming_ws.run_inference", 
            args=[payload],
            kwargs={"trace_id": trace_id},
            queue=getattr(Config, "CELERY_STREAMING_QUEUE", "streaming_ws")
        )
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("inference_dispatch", "success", latency_ms, trace_id)
        
        _structured_log(
            event="inference_enqueued",
            message=f"Transcript dispatched ({len(transcript)} chars)",
            trace_id=trace_id,
            session_id=session_id,
            extra={"transcript_length": len(transcript)}
        )
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("inference_dispatch", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id, "session_id": session_id})
        _structured_log(
            event="inference_dispatch_failed",
            level="error",
            message=f"Inference dispatch failed: {str(e)}",
            trace_id=trace_id,
            session_id=session_id,
            extra={"traceback": str(e)}
        )

# -------------------------------------------------------------------
# WebSocket Route with Full Observability
# -------------------------------------------------------------------
@sock.route("/media-stream/ws")
async def media_stream(ws):
    """Handle incoming Twilio Media Stream → Deepgram ASR pipeline with Phase 11-D observability."""
    trace_id = _resolve_trace_id()
    session_id = str(uuid.uuid4())
    deepgram_ws = None
    deepgram_reader_task = None
    connection_start_time = time.time()

    # Circuit breaker check at connection start
    if _is_circuit_breaker_open(__service__):
        try:
            increment_metric("streaming_ws_circuit_breaker_hits_total")
        except Exception:
            pass
        _structured_log("connection_circuit_breaker_blocked", level="warn",
                        message="WebSocket connection blocked by circuit breaker", 
                        trace_id=trace_id, session_id=session_id)
        await ws.close(1008, "Service temporarily unavailable")
        return

    # Connection metrics
    try:
        increment_metric("streaming_ws_connections_total")
        increment_metric("streaming_ws_active_connections_total")
    except Exception:
        pass

    try:
        _structured_log(
            event="stream_opened",
            message="New Twilio media session established",
            trace_id=trace_id,
            session_id=session_id,
            extra={"remote_addr": request.remote_addr}
        )

        # Receive initial 'connected' event from Twilio
        message = await ws.recv()
        try:
            increment_metric("streaming_ws_messages_in_total")
        except Exception:
            pass
            
        data = json.loads(message)
        if data.get("event") == "connected":
            call_sid = data.get("call_sid")
            if call_sid:
                # Store session_id -> call_sid mapping in Redis with safety wrapper
                safe_redis_operation(
                    lambda: get_redis_client().set(f"twilio_call:{session_id}", call_sid, ex=3600),
                    fallback=None,
                    operation_name="store_call_sid"
                )
                _structured_log(
                    event="twilio_call_sid_stored",
                    message=f"Stored Call SID {call_sid} for session {session_id}",
                    trace_id=trace_id,
                    session_id=session_id,
                )

        # Connect to Deepgram live endpoint
        deepgram_connect_start = time.time()
        try:
            deepgram_ws = await websockets.connect(
                DEEPGRAM_WS_BASE,
                extra_headers={"Authorization": f"Token {DEEPGRAM_API_KEY}"},
                ping_interval=20,
                ping_timeout=20,
            )
            dg_latency_ms = (time.time() - deepgram_connect_start) * 1000
            _record_metrics("deepgram_connect", "success", dg_latency_ms, trace_id)
        except Exception as e:
            dg_latency_ms = (time.time() - deepgram_connect_start) * 1000
            _record_metrics("deepgram_connect", "failure", dg_latency_ms, trace_id)
            capture_exception_safe(e, {"service": __service__, "trace_id": trace_id, "session_id": session_id})
            raise

        async def deepgram_reader():
            """Listen for transcript messages from Deepgram with safety wrappers"""
            try:
                async for msg in deepgram_ws:
                    try:
                        increment_metric("streaming_ws_messages_out_total")
                    except Exception:
                        pass
                        
                    data = json.loads(msg)
                    if "channel" in data and "alternatives" in data["channel"]:
                        alt = data["channel"]["alternatives"][0]
                        transcript = alt.get("transcript", "").strip()
                        if transcript and data.get("is_final"):
                            try:
                                increment_metric("streaming_ws_transcripts_final_total")
                            except Exception:
                                pass
                            await enqueue_inference(transcript, trace_id, session_id)
            except asyncio.CancelledError:
                _structured_log(
                    event="deepgram_reader_cancelled",
                    level="info", 
                    message="Deepgram reader task cancelled",
                    trace_id=trace_id,
                    session_id=session_id
                )
            except Exception as e:
                capture_exception_safe(e, {"service": __service__, "trace_id": trace_id, "session_id": session_id})
                _structured_log(
                    event="deepgram_reader_error",
                    level="error",
                    message=f"Deepgram reader error: {str(e)}",
                    trace_id=trace_id,
                    session_id=session_id,
                )

        deepgram_reader_task = asyncio.create_task(deepgram_reader())

        # Main loop for Twilio events with comprehensive observability
        async for message in ws:
            message_start_time = time.time()
            try:
                try:
                    increment_metric("streaming_ws_messages_in_total")
                except Exception:
                    pass
                    
                data = json.loads(message)
                event_type = data.get("event", "")

                if event_type == "start":
                    _structured_log(
                        event="stream_start",
                        message="Streaming started",
                        trace_id=trace_id,
                        session_id=session_id,
                    )

                elif event_type == "media":
                    audio_payload = data.get("media", {}).get("payload")
                    if not audio_payload:
                        continue

                    # Decode µ-law → PCM16LE bytes
                    try:
                        pcm_bytes = base64.b64decode(audio_payload)
                        await deepgram_ws.send(pcm_bytes)
                        await asyncio.sleep(0.02)  # backpressure throttle (~20 ms)
                        
                        latency_ms = (time.time() - message_start_time) * 1000
                        _record_metrics("media_processing", "success", latency_ms, trace_id)
                        
                    except Exception as e:
                        latency_ms = (time.time() - message_start_time) * 1000
                        _record_metrics("media_processing", "failure", latency_ms, trace_id)
                        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id, "session_id": session_id})
                        _structured_log(
                            event="media_processing_error",
                            level="error",
                            message=f"Media processing error: {str(e)}",
                            trace_id=trace_id,
                            session_id=session_id,
                        )

                elif event_type == "stop":
                    _structured_log(
                        event="stream_stop",
                        message="Twilio stream stop event",
                        trace_id=trace_id,
                        session_id=session_id,
                    )
                    if deepgram_reader_task:
                        deepgram_reader_task.cancel()
                    break

                # Record successful message processing
                latency_ms = (time.time() - message_start_time) * 1000
                _record_metrics("message_processing", "success", latency_ms, trace_id)

            except Exception as e:
                latency_ms = (time.time() - message_start_time) * 1000
                _record_metrics("message_processing", "failure", latency_ms, trace_id)
                capture_exception_safe(e, {"service": __service__, "trace_id": trace_id, "session_id": session_id})
                _structured_log(
                    event="media_stream_error",
                    level="error",
                    message=f"Media stream processing error: {str(e)}",
                    trace_id=trace_id,
                    session_id=session_id,
                )

    except Exception as e:
        connection_latency_ms = (time.time() - connection_start_time) * 1000
        _record_metrics("connection", "failure", connection_latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "trace_id": trace_id, "session_id": session_id})
        _structured_log(
            event="connection_error",
            level="error",
            message=f"Connection error: {str(e)}",
            trace_id=trace_id,
            session_id=session_id,
        )

    finally:
        # Clean shutdown with observability
        cleanup_start = time.time()
        try:
            if deepgram_reader_task and not deepgram_reader_task.done():
                deepgram_reader_task.cancel()
                try:
                    await asyncio.wait_for(deepgram_reader_task, timeout=2.0)
                except asyncio.TimeoutError:
                    _structured_log(
                        event="cleanup_timeout",
                        level="warn",
                        message="Deepgram reader task cleanup timeout",
                        trace_id=trace_id,
                        session_id=session_id
                    )
                    
            if deepgram_ws:
                await deepgram_ws.close()
                
            # Clean up Redis session mapping
            safe_redis_operation(
                lambda: get_redis_client().delete(f"twilio_call:{session_id}"),
                fallback=None,
                operation_name="cleanup_call_sid"
            )
            
            cleanup_latency_ms = (time.time() - cleanup_start) * 1000
            _record_metrics("cleanup", "success", cleanup_latency_ms, trace_id)
            
        except Exception as e:
            cleanup_latency_ms = (time.time() - cleanup_start) * 1000
            _record_metrics("cleanup", "failure", cleanup_latency_ms, trace_id)
            capture_exception_safe(e, {"service": __service__, "trace_id": trace_id, "session_id": session_id})
            _structured_log(
                event="cleanup_error",
                level="error",
                message=f"Cleanup error: {str(e)}",
                trace_id=trace_id,
                session_id=session_id,
            )

        finally:
            # Always decrement active connections
            try:
                increment_metric("streaming_ws_disconnects_total")
                increment_metric("streaming_ws_active_connections_total", -1)  # Decrement
            except Exception:
                pass

        connection_latency_ms = (time.time() - connection_start_time) * 1000
        _record_metrics("connection", "success", connection_latency_ms, trace_id)
        
        _structured_log(
            event="stream_closed",
            message="Session ended cleanly",
            trace_id=trace_id,
            session_id=session_id,
            extra={"connection_duration_ms": connection_latency_ms}
        )

# -------------------------------------------------------------------
# Health Check Endpoint
# -------------------------------------------------------------------
@app.route("/health/ws", methods=["GET"])
def health_check():
    """Health check endpoint for WebSocket streaming service."""
    trace_id = get_trace_id()
    start_time = time.time()
    
    try:
        # Record health check metric
        try:
            increment_metric("streaming_ws_health_checks_total")
        except Exception:
            pass
        
        # Redis health
        redis_ok = safe_redis_operation(
            lambda: get_redis_client().ping(),
            fallback=False,
            operation_name="health_check_ping"
        )
        
        # Circuit breaker states
        ws_breaker_open = _is_circuit_breaker_open("streaming_ws")
        redis_breaker_open = _is_circuit_breaker_open("redis")
        inference_breaker_open = _is_circuit_breaker_open("inference")
        
        # Get active connections count (approximate)
        try:
            active_conns = safe_redis_operation(
                lambda: int(get_redis_client().get("streaming_ws_active_connections_total") or 0),
                fallback=0,
                operation_name="get_active_connections"
            )
        except Exception:
            active_conns = 0

        overall_status = "healthy" if redis_ok and not any([ws_breaker_open, redis_breaker_open]) else "degraded"

        _structured_log(
            event="health_check",
            level="info" if overall_status == "healthy" else "warn",
            message="WebSocket health check completed", 
            trace_id=trace_id,
            extra={
                "redis_ok": redis_ok,
                "active_connections": active_conns,
                "ws_breaker_open": ws_breaker_open,
                "redis_breaker_open": redis_breaker_open,
                "inference_breaker_open": inference_breaker_open
            }
        )

        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "success", latency_ms, trace_id)
        
        return {
            "service": __service__,
            "status": overall_status,
            "trace_id": trace_id,
            "schema_version": __schema_version__,
            "components": {
                "redis": {
                    "status": "healthy" if redis_ok else "unhealthy",
                    "circuit_breaker": "open" if redis_breaker_open else "closed"
                },
                "circuit_breakers": {
                    "streaming_ws": "open" if ws_breaker_open else "closed",
                    "inference": "open" if inference_breaker_open else "closed",
                    "redis": "open" if redis_breaker_open else "closed"
                },
                "connections": {
                    "active": active_conns,
                    "status": "healthy" if active_conns < 1000 else "degraded"  # Reasonable limit
                }
            }
        }, 200 if overall_status == "healthy" else 503

    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": __service__, "health_check": "ws"})
        return {
            "service": __service__, 
            "status": "unhealthy",
            "trace_id": trace_id,
            "schema_version": __schema_version__,
            "error": str(e)
        }, 500

# -------------------------------------------------------------------
# Startup Logging
# -------------------------------------------------------------------
_structured_log(
    event="startup_init",
    level="info",
    message="WebSocket streaming service initialized with Phase 11-D compliance",
    extra={"phase": __phase__, "schema_version": __schema_version__}
)

# -------------------------------------------------------------------
# Local debug entry point
# -------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("STREAMING_WS_PORT", 8000))
    _structured_log(
        event="service_start",
        level="info", 
        message=f"Starting WebSocket streaming service on port {port}",
        extra={"port": port}
    )
    app.run(host="0.0.0.0", port=port)