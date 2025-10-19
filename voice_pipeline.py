"""voice_pipeline.py — Phase 11-D
Production-grade voice orchestration with Prometheus metrics, unified Redis, and structured observability.
"""

from __future__ import annotations
import json
import time
import traceback
from typing import Optional, Dict, Any, Callable
import threading

from celery_app import celery
from logging_utils import get_json_logger, log_event, get_trace_id

# Phase 11-D canonical imports
from config import Config
from redis_client import get_redis_client, safe_redis_operation
from metrics_collector import increment_metric, observe_latency
from sentry_utils import init_sentry, capture_exception_safe
from global_metrics_store import start_background_sync

# --------------------------------------------------------------------------
# Compliance Metadata
# --------------------------------------------------------------------------
__phase__ = "11-D"
__schema_version__ = "phase_11d_v1"
__service__ = "voice_pipeline"

# --------------------------------------------------------------------------
# Initialization
# --------------------------------------------------------------------------
init_sentry()
start_background_sync(service_name="voice_pipeline")

logger = get_json_logger("sara-ai-core-voice")

# Startup logging
log_event(
    service=__service__,
    event="startup_init", 
    status="info",
    message="Voice pipeline initialized with Phase 11-D compliance",
    extra={"phase": __phase__, "schema_version": __schema_version__}
)

# Optional imports (ASR / Twilio)
try:
    from asr_service import process_audio_buffer
except Exception:
    process_audio_buffer = None

try:
    import twilio_client
except Exception:
    twilio_client = None

# --------------------------------------------------------------------------
# Centralized configuration (from config.settings)
# --------------------------------------------------------------------------
SERVICE_NAME = __service__
SARA_ENV = getattr(Config, "SARA_ENV", "development")

CALL_STATE_TTL = int(getattr(Config, "CALL_STATE_TTL", 60 * 60 * 4))
PARTIAL_THROTTLE_SECONDS = float(getattr(Config, "PARTIAL_THROTTLE_SECONDS", 1.5))
PARTIAL_THROTTLE_PREFIX = "partial_throttle:"

INFERENCE_TASK_NAME = getattr(Config, "INFERENCE_TASK_NAME", "sara_ai.tasks.voice_pipeline.run_inference")
EVENT_TASK_NAME = getattr(Config, "EVENT_TASK_NAME", "sara_ai.tasks.voice_pipeline.dispatch_event")
CELERY_VOICE_QUEUE = getattr(Config, "CELERY_VOICE_QUEUE", "voice_pipeline")

# --------------------------------------------------------------------------
# Circuit Breaker Implementation (Canonical Pattern)
# --------------------------------------------------------------------------
def _is_circuit_breaker_open(service: str = "voice_pipeline") -> bool:
    """Canonical circuit breaker check used across Phase 11-D services."""
    try:
        client = get_redis_client()
        if not client:
            return False
        key = f"circuit_breaker:{service}:state"
        state = safe_redis_operation(lambda: client.get(key))
        
        # Record circuit breaker check metric
        try:
            increment_metric("voice_pipeline_circuit_breaker_checks_total")
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

# --------------------------------------------------------------------------
# Metrics Recording Helper
# --------------------------------------------------------------------------
def _record_metrics(event_type: str, status: str, latency_ms: float = None, trace_id: str = None):
    """Record standardized metrics for voice pipeline operations (seconds for latency)."""
    try:
        # Counters (best-effort)
        try:
            increment_metric(f"voice_pipeline_{event_type}_{status}_total")
        except Exception:
            pass

        # Histogram / latency: convert ms -> seconds for Prometheus
        if latency_ms is not None:
            try:
                latency_s = float(latency_ms) / 1000.0
                observe_latency(f"voice_pipeline_{event_type}_latency_seconds", latency_s)
                # Add aggregate total latency view
                if event_type != "total":
                    observe_latency("voice_pipeline_total_latency_seconds", latency_s)
            except Exception:
                # fallback to legacy ms metric if needed
                try:
                    observe_latency(f"voice_pipeline_{event_type}_latency_ms", float(latency_ms))
                except Exception:
                    pass
    except Exception as e:
        log_event(
            service=SERVICE_NAME,
            event="metrics_record_failed",
            status="warn",
            message="Failed to record voice pipeline metrics",
            trace_id=trace_id or get_trace_id(),
            extra={"error": str(e), "schema_version": __schema_version__}
        )

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _resolve_trace_id(payload: Optional[Dict[str, Any]] = None, override: Optional[str] = None) -> str:
    if override:
        return override
    if payload and payload.get("trace_id"):
        return payload["trace_id"]
    return get_trace_id()

def _redis_key(call_sid: str) -> str:
    return f"call:{call_sid}"

def _partial_throttle_key(call_sid: str) -> str:
    return f"{PARTIAL_THROTTLE_PREFIX}{call_sid}"

def _structured_log(event: str, level: str = "info", message: Optional[str] = None,
                    trace_id: Optional[str] = None, call_sid: Optional[str] = None, **extra):
    log_event(service=SERVICE_NAME, event=event, status=level, message=message or event,
              trace_id=trace_id or get_trace_id(), 
              extra={**({"call_sid": call_sid} if call_sid else {}), **extra, "schema_version": __schema_version__})

def _get_r_client() -> Optional[Any]:
    """
    Return a fresh redis client for each call, safely handling exceptions.
    No global mutable state - consistent with Phase 11-D stateless design.
    """
    try:
        rr = get_redis_client()
        if not rr:
            return None
            
        # Some redis clients lazily connect; try ping if possible
        if hasattr(rr, "ping"):
            rr.ping()
        # Record success metric
        try:
            increment_metric("voice_pipeline_redis_success_total")
        except Exception:
            pass
        return rr
    except Exception as e:
        # Record failure metric
        try:
            increment_metric("voice_pipeline_redis_failures_total")
        except Exception:
            pass
        _structured_log("redis_unavailable", level="warn",
                        message=f"Redis unavailable: {e}", trace_id=get_trace_id())
        return None

# --------------------------------------------------------------------------
# Session state helpers (merge semantics)
# --------------------------------------------------------------------------
def _store_call_state(call_sid: str, trace_id: str, state: Dict[str, Any]) -> None:
    rr = _get_r_client()
    if not rr:
        _structured_log("redis_unavailable_store", level="warn",
                        message="Redis unavailable during store", trace_id=trace_id, call_sid=call_sid)
        # record pipeline-level error metric
        try:
            increment_metric("voice_pipeline_errors_total")
        except Exception:
            pass
        return
    
    start_time = time.time()
    try:
        key = _redis_key(call_sid)
        raw = safe_redis_operation(lambda: rr.get(key), fallback=b"{}", operation_name="get_call_state")
        if raw is None:
            raw = b"{}"
        
        # Safe decoding
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except Exception:
                raw = "{}"
        
        try:
            existing = json.loads(raw)
        except Exception:
            existing = {}
        
        merged = {**existing, **state, "trace_id": trace_id, "schema_version": __schema_version__}
        safe_redis_operation(
            lambda: rr.set(key, json.dumps(merged), ex=CALL_STATE_TTL),
            fallback=None,
            operation_name="set_call_state"
        )
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("store_state", "success", latency_ms, trace_id)
        _structured_log("call_state_merged", message="Merged call state stored", trace_id=trace_id, call_sid=call_sid)
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("store_state", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        _structured_log("redis_set_failed", level="error", message=str(e),
                        trace_id=trace_id, call_sid=call_sid, extra={"traceback": traceback.format_exc()})

def _get_call_state(call_sid: str) -> Optional[Dict[str, Any]]:
    rr = _get_r_client()
    if not rr:
        _structured_log("redis_unavailable_get", level="warn",
                        message="Redis unavailable during get", trace_id=get_trace_id(), call_sid=call_sid)
        return None
    
    start_time = time.time()
    try:
        data = safe_redis_operation(
            lambda: rr.get(_redis_key(call_sid)), 
            fallback=None,
            operation_name="get_call_state"
        )
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("get_state", "success", latency_ms)
        
        if not data:
            return None
            
        # Safe decoding
        if isinstance(data, bytes):
            try:
                data = data.decode("utf-8")
            except Exception:
                return None
                
        try:
            return json.loads(data)
        except Exception:
            return None
            
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("get_state", "failure", latency_ms)
        capture_exception_safe(e, {"service": SERVICE_NAME, "call_sid": call_sid})
        _structured_log("redis_get_failed", level="warn", message=str(e),
                        trace_id=get_trace_id(), call_sid=call_sid, extra={"traceback": traceback.format_exc()})
        return None

def get_session_metadata(call_sid: str):
    return _get_call_state(call_sid)

def clear_session(call_sid: str):
    rr = _get_r_client()
    trace_id = get_trace_id()
    if not rr:
        _structured_log("redis_unavailable_clear", level="warn",
                        message="Redis unavailable during clear", trace_id=trace_id, call_sid=call_sid)
        return
    
    start_time = time.time()
    try:
        safe_redis_operation(
            lambda: rr.delete(_redis_key(call_sid)),
            fallback=None,
            operation_name="clear_session"
        )
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("clear_session", "success", latency_ms, trace_id)
        _structured_log("clear_session", message="Session cleared", trace_id=trace_id, call_sid=call_sid)
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("clear_session", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        _structured_log("redis_delete_failed", level="warn", message=str(e),
                        trace_id=trace_id, call_sid=call_sid, extra={"traceback": traceback.format_exc()})

# --------------------------------------------------------------------------
# Partial throttle & fallback guard
# --------------------------------------------------------------------------
_throttle_lock = threading.RLock()  # Changed to RLock for reentrant safety

def _should_dispatch_partial(call_sid: str) -> bool:
    """
    Returns True if we SHOULD dispatch (i.e. not throttled), or if Redis is unavailable
    we allow dispatch but count fallback occurrences.
    """
    with _throttle_lock:
        # Circuit breaker check
        if _is_circuit_breaker_open("redis"):
            try:
                increment_metric("voice_pipeline_circuit_breaker_hits_total")
            except Exception:
                pass
            _structured_log("redis_circuit_breaker_open", level="warn",
                            message="Redis circuit breaker open - allowing dispatch", call_sid=call_sid, trace_id=get_trace_id())
            return True

        rr = _get_r_client()
        if not rr:
            # Redis unavailable - allow dispatch but record metric
            try:
                increment_metric("voice_pipeline_redis_failures_total")
            except Exception:
                pass
            _structured_log("partial_throttle_no_redis", level="warn",
                            message="Redis unavailable - allowing dispatch", call_sid=call_sid, trace_id=get_trace_id())
            return True

        start_time = time.time()
        try:
            # Use a quick NX set for throttle (atomic)
            key = _partial_throttle_key(call_sid)
            was_set = safe_redis_operation(
                lambda: rr.set(key, "1", nx=True, ex=int(PARTIAL_THROTTLE_SECONDS)),
                fallback=False,
                operation_name="partial_throttle_set"
            )
            
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("throttle_check", "success", latency_ms)
            
            if not was_set:
                # Throttled
                try:
                    increment_metric("voice_pipeline_partial_throttled_total")
                except Exception:
                    pass
                _structured_log("partial_throttled", message="Partial dispatch throttled", trace_id=get_trace_id(), call_sid=call_sid)
                return False
            return True
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("throttle_check", "failure", latency_ms)
            capture_exception_safe(e, {"service": SERVICE_NAME, "call_sid": call_sid})
            _structured_log("partial_throttle_error", level="warn",
                            message=str(e), trace_id=get_trace_id(), call_sid=call_sid,
                            extra={"traceback": traceback.format_exc()})
            return True

# --------------------------------------------------------------------------
# Twilio wrapper with retries & backoff
# --------------------------------------------------------------------------
def _twilio_with_retries(func: Callable, *args, max_attempts: int = 3, trace_id: Optional[str] = None, call_sid: Optional[str] = None, **kwargs):
    trace_id = trace_id or get_trace_id()
    last_exc = None
    start_time = time.time()
    
    for attempt in range(1, max_attempts + 1):
        try:
            result = func(*args, **kwargs)
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("twilio_operation", "success", latency_ms, trace_id)
            return result
        except Exception as e:
            last_exc = e
            _structured_log("twilio_call_failed", level="warn",
                            message=f"Twilio call failed (attempt {attempt}) - {e}",
                            trace_id=trace_id, call_sid=call_sid,
                            extra={"attempt": attempt, "traceback": traceback.format_exc()})
            # short exponential backoff
            time.sleep(0.2 * attempt)
    
    # All retries failed
    latency_ms = (time.time() - start_time) * 1000
    _record_metrics("twilio_operation", "failure", latency_ms, trace_id)
    try:
        increment_metric("voice_pipeline_errors_total")
    except Exception:
        pass
    
    # Capture final exception
    if last_exc:
        capture_exception_safe(last_exc, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        
    return None

# --------------------------------------------------------------------------
# Internal handlers for ASR results
# --------------------------------------------------------------------------
def _handle_partial_result(call_sid: str, text: str, payload: dict, trace_id: str):
    _store_call_state(call_sid, trace_id,
                      {"last_partial": text, "last_partial_ts": int(time.time())})
    _structured_log("partial_stored", message="Stored partial transcript", trace_id=trace_id, call_sid=call_sid, preview=text[:120])

    if not _should_dispatch_partial(call_sid):
        try:
            increment_metric("voice_pipeline_partial_throttled_total")
        except Exception:
            pass
        _structured_log("partial_throttled", message="Partial dispatch throttled", trace_id=trace_id, call_sid=call_sid)
        return

    start_time = time.time()
    try:
        if twilio_client and hasattr(twilio_client, "update_partial_transcript"):
            res = _twilio_with_retries(twilio_client.update_partial_transcript, call_sid, text,
                                       payload=payload, trace_id=trace_id, call_sid=call_sid)
            if res is None:
                _structured_log("partial_dispatch_twilio_failed", level="warn",
                                message="Twilio partial dispatch failed after retries", trace_id=trace_id, call_sid=call_sid)
            else:
                _structured_log("partial_dispatched_via_twilio", message="Partial dispatched via Twilio client", trace_id=trace_id, call_sid=call_sid, extra={"result": res})
        else:
            celery.send_task(
                EVENT_TASK_NAME,
                args=[{"type": "partial_transcript", "callSid": call_sid, "text": text}],
                kwargs={"trace_id": trace_id},
                queue=CELERY_VOICE_QUEUE
            )
            _structured_log("partial_event_sent_fallback", message="Partial event sent via Celery fallback", trace_id=trace_id, call_sid=call_sid)
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("partial_dispatch", "success", latency_ms, trace_id)
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("partial_dispatch", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        _structured_log("partial_dispatch_failed", level="error", message=str(e), trace_id=trace_id, call_sid=call_sid,
                        extra={"traceback": traceback.format_exc()})

def _handle_final_result(call_sid: str, text: str, payload: dict, trace_id: str):
    start_time = time.time()
    try:
        process_final_transcript(call_sid, text, payload=payload, trace_id=trace_id, trigger_inference=True)
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("final_processing", "success", latency_ms, trace_id)
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("final_processing", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        _structured_log("final_process_failed", level="error", message=str(e), trace_id=trace_id, call_sid=call_sid, extra={"traceback": traceback.format_exc()})

# --------------------------------------------------------------------------
# Audio chunk processing
# --------------------------------------------------------------------------
def process_audio_chunk(call_sid: str, audio_chunk: bytes,
                        payload: Optional[Dict[str, Any]] = None,
                        trace_id: Optional[str] = None) -> None:
    trace = _resolve_trace_id(payload, trace_id)
    
    # Circuit breaker check
    if _is_circuit_breaker_open("voice_pipeline"):
        try:
            increment_metric("voice_pipeline_circuit_breaker_hits_total")
        except Exception:
            pass
        _structured_log("circuit_breaker_blocked", level="warn",
                        message="Voice pipeline request blocked by open circuit breaker", trace_id=trace, call_sid=call_sid)
        return

    # metrics: count chunk
    try:
        increment_metric("voice_pipeline_chunks_total")
    except Exception:
        pass

    _structured_log("audio_chunk_received", message="Audio chunk received", trace_id=trace,
                  call_sid=call_sid, extra={"bytes": len(audio_chunk)})

    _store_call_state(call_sid, trace, {"last_chunk_received_at": int(time.time())})

    if not process_audio_buffer:
        _structured_log("asr_missing", level="error", message="ASR backend unavailable", trace_id=trace, call_sid=call_sid)
        try:
            increment_metric("voice_pipeline_asr_failures_total")
        except Exception:
            pass
        return

    start_time = time.time()
    try:
        # Safe ASR result unpacking
        try:
            consumed, result = process_audio_buffer(call_sid, bytearray(audio_chunk))
        except (ValueError, TypeError) as e:
            try:
                increment_metric("voice_pipeline_asr_invalid_output_total")
            except Exception:
                pass
            _structured_log("asr_invalid_output", level="error", 
                          message="ASR returned invalid output format", trace_id=trace, call_sid=call_sid,
                          extra={"error": str(e), "traceback": traceback.format_exc()})
            return
            
        duration = time.time() - start_time
        latency_ms = duration * 1000
        
        try:
            observe_latency("voice_pipeline_asr_latency_seconds", duration)
        except Exception:
            pass

        if consumed:
            _structured_log("asr_consumed", message="ASR consumed bytes", trace_id=trace, call_sid=call_sid, extra={"consumed": consumed})

        if not result:
            _structured_log("asr_no_result", message="No transcript for chunk", trace_id=trace, call_sid=call_sid)
            return

        rtype = result.get("type")
        text = result.get("text", "")
        if rtype == "partial":
            _handle_partial_result(call_sid, text, payload or {}, trace)
        elif rtype == "final":
            _handle_final_result(call_sid, text, payload or {}, trace)
        else:
            _structured_log("asr_unknown_type", level="warn", message="ASR returned unknown type", trace_id=trace, call_sid=call_sid, extra={"result": result})
            
        _record_metrics("chunk_processing", "success", latency_ms, trace)
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("chunk_processing", "failure", latency_ms, trace)
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace, "call_sid": call_sid})
        _structured_log("asr_processing_failed", level="error", message=str(e), trace_id=trace, call_sid=call_sid, extra={"traceback": traceback.format_exc()})

# --------------------------------------------------------------------------
# Final transcript handling
# --------------------------------------------------------------------------
def process_final_transcript(call_sid: str, final_text: str,
                             payload: Optional[Dict[str, Any]] = None,
                             trace_id: Optional[str] = None,
                             trigger_inference: bool = True) -> Dict[str, Any]:
    resolved = _resolve_trace_id(payload, trace_id)
    start_time = time.time()
    
    try:
        _store_call_state(call_sid, resolved,
                          {"last_transcript": final_text,
                           "last_transcript_ts": int(time.time()),
                           "status": "transcribed"})
        _structured_log("final_stored", message="Final transcript stored", trace_id=resolved, call_sid=call_sid, preview=final_text[:150])

        dispatch_success = False
        if twilio_client and hasattr(twilio_client, "update_final_transcript"):
            res = _twilio_with_retries(twilio_client.update_final_transcript, call_sid, final_text,
                                       payload=payload, trace_id=resolved, call_sid=call_sid)
            if res is None:
                _structured_log("final_dispatched_twilio_failed", level="warn",
                                message="Twilio final dispatch failed after retries", trace_id=resolved, call_sid=call_sid)
                # fallback to celery event
            else:
                _structured_log("final_dispatched_via_twilio_client", message="Final dispatched via Twilio client",
                                trace_id=resolved, call_sid=call_sid, extra={"result": res})
                dispatch_success = True
                info = {"ok": True, "via": "twilio_client"}

        if not dispatch_success:
            # fallback or no twilio_client
            celery.send_task(
                EVENT_TASK_NAME,
                args=[{"type": "final_transcript", "callSid": call_sid, "text": final_text}],
                kwargs={"trace_id": resolved},
                queue=CELERY_VOICE_QUEUE
            )
            info = {"ok": True, "via": "celery_event"}
            _structured_log("final_event_sent_fallback", message="Final event sent via Celery fallback", trace_id=resolved, call_sid=call_sid)

        if trigger_inference:
            dispatch_info = _dispatch_inference_final(final_text, call_sid, resolved)
            info["inference"] = dispatch_info
            
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("final_processing", "success", latency_ms, resolved)
        return info
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("final_processing", "failure", latency_ms, resolved)
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": resolved, "call_sid": call_sid})
        _structured_log("final_store_failed", level="error", message=str(e), trace_id=resolved, call_sid=call_sid, extra={"traceback": traceback.format_exc()})
        return {"ok": False, "error": str(e)}

def _dispatch_inference_final(final_text: str, call_sid: str, trace_id: str) -> Dict[str, Any]:
    start_time = time.time()
    try:
        if twilio_client and hasattr(twilio_client, "_dispatch_inference_task"):
            res = _twilio_with_retries(twilio_client._dispatch_inference_task, final_text, call_sid, trace_id=trace_id, call_sid=call_sid)
            if res is None:
                _structured_log("inference_dispatch_twilio_failed", level="warn", message="Twilio inference dispatch failed", trace_id=trace_id, call_sid=call_sid)
                return {"ok": False, "error": "twilio_dispatch_failed"}
            _structured_log("inference_dispatched_via_twilio_client", message="Inference dispatched via Twilio client", trace_id=trace_id, call_sid=call_sid)
            try:
                increment_metric("voice_pipeline_inference_dispatched_total")
            except Exception:
                pass
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("inference_dispatch", "success", latency_ms, trace_id)
            return {"ok": True, "meta": res}
        else:
            payload = {"trace_id": trace_id, "call_sid": call_sid, "transcript": final_text, "metadata": {}, "schema_version": __schema_version__}
            task = celery.send_task(
                INFERENCE_TASK_NAME, 
                args=[payload], 
                kwargs={"trace_id": trace_id},
                queue=CELERY_VOICE_QUEUE
            )
            try:
                increment_metric("voice_pipeline_inference_dispatched_total")
            except Exception:
                pass
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("inference_dispatch", "success", latency_ms, trace_id)
            return {"ok": True, "task_id": getattr(task, "id", None)}
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("inference_dispatch", "failure", latency_ms, trace_id)
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        _structured_log("inference_dispatch_error", level="error", message=str(e), trace_id=trace_id, call_sid=call_sid, extra={"traceback": traceback.format_exc()})
        return {"ok": False, "error": str(e)}

# --------------------------------------------------------------------------
# Test hooks (preserved) — instrumented to emit metrics
# --------------------------------------------------------------------------
def _test_simulate_chunk_flow():
    try:
        increment_metric("voice_pipeline_test_runs_total")
    except Exception:
        pass
        
    test_call = "SIM-TEST-CALL-123"
    trace = get_trace_id()
    _structured_log("test_start", message="Simulating chunk flow", trace_id=trace, call_sid=test_call)
    # Fake ASR stub output
    global process_audio_buffer

    def fake_asr(call_sid, buf):
        if len(buf) < 4000:
            return len(buf), {"type": "partial", "text": f"partial-{len(buf)}"}
        return len(buf), {"type": "final", "text": "This is the final transcript."}

    process_audio_buffer = fake_asr
    for size in [1000, 1200, 1500, 5000]:
        try:
            process_audio_chunk(test_call, b"x" * size, trace_id=trace)
        except Exception as e:
            _structured_log("test_chunk_error", level="error", message=str(e), trace_id=trace, call_sid=test_call, extra={"traceback": traceback.format_exc()})
        time.sleep(0.2)
    _structured_log("test_done", message="Simulation complete", trace_id=trace, call_sid=test_call)

def _test_simulate_final_flow():
    try:
        increment_metric("voice_pipeline_test_runs_total")
    except Exception:
        pass
        
    call = "SIM-FINAL-456"
    trace = get_trace_id()
    res = process_final_transcript(call, "Simulated final transcript.", trace_id=trace, trigger_inference=False)
    _structured_log("test_final_result", message="Final flow result", trace_id=trace, call_sid=call, extra={"result": res})

__all__ = [
    "process_audio_chunk", "process_final_transcript",
    "_store_call_state", "_get_call_state", "get_session_metadata",
    "clear_session", "_test_simulate_chunk_flow", "_test_simulate_final_flow"
]

# --------------------------------------------------------------------------
# Health check endpoint (if this module runs as a service)
# --------------------------------------------------------------------------
if __name__ == "__main__":
    from flask import Flask, jsonify
    app = Flask(__name__)
    
    @app.route("/health", methods=["GET"])
    def health_check():
        """Health check endpoint for voice pipeline service."""
        trace_id = get_trace_id()
        start_time = time.time()
        
        try:
            # Record health check metric
            try:
                increment_metric("voice_pipeline_health_checks_total")
            except Exception:
                pass
            
            # Redis health
            redis_ok = safe_redis_operation(
                lambda: get_redis_client().ping(),
                fallback=False,
                operation_name="health_check_ping"
            )
            
            # Circuit breaker states
            pipeline_breaker_open = _is_circuit_breaker_open("voice_pipeline")
            redis_breaker_open = _is_circuit_breaker_open("redis")
            
            # Celery broker check
            try:
                insp = celery.control.inspect(timeout=1)
                insp_result = insp.ping()
                celery_ok = bool(insp_result)
            except Exception:
                celery_ok = False

            overall_status = "healthy" if all([redis_ok, celery_ok]) and not any([pipeline_breaker_open, redis_breaker_open]) else "degraded"

            _structured_log(
                "health_check", 
                level="info" if overall_status == "healthy" else "warn",
                message="Health check completed", 
                trace_id=trace_id,
                extra={
                    "redis_ok": redis_ok,
                    "celery_ok": celery_ok,
                    "pipeline_breaker_open": pipeline_breaker_open,
                    "redis_breaker_open": redis_breaker_open
                }
            )

            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("health_check", "success", latency_ms, trace_id)
            
            return jsonify({
                "service": "voice_pipeline",
                "status": overall_status,
                "trace_id": trace_id,
                "schema_version": __schema_version__,
                "components": {
                    "redis": {
                        "status": "healthy" if redis_ok else "unhealthy",
                        "circuit_breaker": "open" if redis_breaker_open else "closed"
                    },
                    "celery_broker": {
                        "status": "healthy" if celery_ok else "unhealthy"
                    },
                    "circuit_breakers": {
                        "voice_pipeline": "open" if pipeline_breaker_open else "closed",
                        "redis": "open" if redis_breaker_open else "closed"
                    }
                }
            })

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("health_check", "failure", latency_ms, trace_id)
            capture_exception_safe(e, {"service": "voice_pipeline", "health_check": "main"})
            return jsonify({
                "service": "voice_pipeline", 
                "status": "unhealthy",
                "trace_id": trace_id,
                "schema_version": __schema_version__,
                "error": str(e)
            }), 500

    # Startup logging
    log_event(
        service=SERVICE_NAME,
        event="startup",
        status="info", 
        message="Voice pipeline service starting with Phase 11-D standardization",
        extra={"phase": __phase__, "schema_version": __schema_version__}
    )
    
    port = int(getattr(Config, "VOICE_PIPELINE_PORT", 7000))
    app.run(host="0.0.0.0", port=port)