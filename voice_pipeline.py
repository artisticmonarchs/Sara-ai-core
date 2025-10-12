"""
voice_pipeline.py — Phase 8-R1
Orchestrates live audio → ASR → transcripts → Twilio client → Celery tasks.
"""

from __future__ import annotations
import os, json, time, traceback
from typing import Optional, Dict, Any
import redis
from celery_app import celery
from logging_utils import log_event, get_trace_id

# Optional imports
try:
    from asr_service import process_audio_buffer
except Exception:
    process_audio_buffer = None
try:
    import twilio_client
except Exception:
    twilio_client = None

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "voice_pipeline")
SARA_ENV = os.getenv("SARA_ENV", "development")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CALL_STATE_TTL = int(os.getenv("CALL_STATE_TTL", 60 * 60 * 4))

PARTIAL_THROTTLE_SECONDS = float(os.getenv("PARTIAL_THROTTLE_SECONDS", "1.5"))
PARTIAL_THROTTLE_PREFIX = "partial_throttle:"

INFERENCE_TASK_NAME = os.getenv("INFERENCE_TASK_NAME", "tasks.run_inference")
EVENT_TASK_NAME = os.getenv("EVENT_TASK_NAME", "tasks.dispatch_event")

_redis_client: Optional[redis.Redis] = None
_fallback_counter = 0  # track Redis unavailability streaks

# --------------------------------------------------------------------------
# Redis init with lightweight retry
# --------------------------------------------------------------------------
def _get_redis() -> Optional[redis.Redis]:
    global _redis_client
    if _redis_client:
        return _redis_client
    for attempt in range(2):
        try:
            _redis_client = redis.Redis.from_url(REDIS_URL, socket_connect_timeout=3)
            _redis_client.ping()
            log_event(service=SERVICE_NAME, event="redis_client_initialized",
                      message=f"Redis connected (attempt {attempt+1})")
            return _redis_client
        except Exception as e:
            time.sleep(0.5)
            if attempt == 1:
                log_event(service=SERVICE_NAME, event="redis_init_failed",
                          level="ERROR", message=str(e),
                          extra={"redis_preview": REDIS_URL.split('//')[-1]})
    return None

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _resolve_trace_id(payload: Optional[Dict[str, Any]] = None, override: Optional[str] = None) -> str:
    if override:
        return override
    if payload and payload.get("trace_id"):
        return payload["trace_id"]
    return get_trace_id()

def _redis_key(call_sid: str) -> str: return f"call:{call_sid}"
def _partial_throttle_key(call_sid: str) -> str: return f"{PARTIAL_THROTTLE_PREFIX}{call_sid}"

# --------------------------------------------------------------------------
# Session state helpers (merge semantics)
# --------------------------------------------------------------------------
def _store_call_state(call_sid: str, trace_id: str, state: Dict[str, Any]) -> None:
    r = _get_redis()
    if not r:
        log_event(service=SERVICE_NAME, event="redis_unavailable_store",
                  level="WARNING", message="Redis unavailable during store",
                  trace_id=trace_id, extra={"call_sid": call_sid})
        return
    try:
        key = _redis_key(call_sid)
        existing = json.loads(r.get(key) or "{}")
        merged = {**existing, **state, "trace_id": trace_id}
        r.set(key, json.dumps(merged), ex=CALL_STATE_TTL)
        log_event(service=SERVICE_NAME, event="call_state_merged",
                  message="Merged call state stored", trace_id=trace_id,
                  extra={"call_sid": call_sid})
    except Exception as e:
        log_event(service=SERVICE_NAME, event="redis_set_failed", level="ERROR",
                  message=str(e), trace_id=trace_id,
                  extra={"call_sid": call_sid, "traceback": traceback.format_exc()})

def _get_call_state(call_sid: str) -> Optional[Dict[str, Any]]:
    r = _get_redis()
    if not r: return None
    try:
        data = r.get(_redis_key(call_sid))
        return json.loads(data) if data else None
    except Exception as e:
        log_event(service=SERVICE_NAME, event="redis_get_failed",
                  level="WARNING", message=str(e), extra={"call_sid": call_sid})
        return None

def get_session_metadata(call_sid: str): return _get_call_state(call_sid)

def clear_session(call_sid: str):
    r = _get_redis()
    trace_id = get_trace_id()
    if not r:
        log_event(service=SERVICE_NAME, event="redis_unavailable_clear",
                  level="WARNING", message="Redis unavailable during clear",
                  trace_id=trace_id, extra={"call_sid": call_sid})
        return
    try:
        r.delete(_redis_key(call_sid))
        log_event(service=SERVICE_NAME, event="clear_session",
                  message="Session cleared", trace_id=trace_id,
                  extra={"call_sid": call_sid})
    except Exception as e:
        log_event(service=SERVICE_NAME, event="redis_delete_failed",
                  level="WARNING", message=str(e), trace_id=trace_id,
                  extra={"call_sid": call_sid})

# --------------------------------------------------------------------------
# Partial throttle & fallback guard
# --------------------------------------------------------------------------
def _should_dispatch_partial(call_sid: str) -> bool:
    global _fallback_counter
    r = _get_redis()
    if not r:
        _fallback_counter += 1
        log_event(service=SERVICE_NAME, event="partial_throttle_no_redis",
                  level="WARNING", message=f"Redis unavailable (x{_fallback_counter}) - allowing dispatch",
                  extra={"call_sid": call_sid})
        return True
    try:
        was_set = r.set(_partial_throttle_key(call_sid), "1", nx=True,
                        ex=int(PARTIAL_THROTTLE_SECONDS))
        _fallback_counter = 0
        return bool(was_set)
    except Exception as e:
        _fallback_counter += 1
        log_event(service=SERVICE_NAME, event="partial_throttle_error",
                  level="WARNING", message=str(e),
                  extra={"call_sid": call_sid, "fallback_counter": _fallback_counter})
        return True

# --------------------------------------------------------------------------
# Internal handlers for ASR results
# --------------------------------------------------------------------------
def _handle_partial_result(call_sid: str, text: str, payload: dict, trace_id: str):
    _store_call_state(call_sid, trace_id,
                      {"last_partial": text, "last_partial_ts": int(time.time())})
    log_event(service=SERVICE_NAME, event="partial_stored",
              message="Stored partial transcript", trace_id=trace_id,
              extra={"call_sid": call_sid, "preview": text[:120]})

    if not _should_dispatch_partial(call_sid):
        log_event(service=SERVICE_NAME, event="partial_throttled",
                  message="Partial dispatch throttled", trace_id=trace_id,
                  extra={"call_sid": call_sid})
        return

    try:
        if twilio_client and hasattr(twilio_client, "update_partial_transcript"):
            twilio_client.update_partial_transcript(call_sid, text,
                                                    payload=payload, trace_id=trace_id)
        else:
            celery.send_task(EVENT_TASK_NAME,
                             args=[{"type": "partial_transcript",
                                    "callSid": call_sid, "text": text}],
                             kwargs={"trace_id": trace_id})
            log_event(service=SERVICE_NAME, event="partial_event_sent_fallback",
                      message="Partial event sent via Celery fallback",
                      trace_id=trace_id, extra={"call_sid": call_sid})
    except Exception as e:
        log_event(service=SERVICE_NAME, event="partial_dispatch_failed",
                  level="WARNING", message=str(e),
                  trace_id=trace_id,
                  extra={"call_sid": call_sid, "traceback": traceback.format_exc()})

def _handle_final_result(call_sid: str, text: str, payload: dict, trace_id: str):
    try:
        process_final_transcript(call_sid, text, payload=payload,
                                 trace_id=trace_id, trigger_inference=True)
    except Exception as e:
        log_event(service=SERVICE_NAME, event="final_process_failed",
                  level="ERROR", message=str(e), trace_id=trace_id,
                  extra={"call_sid": call_sid, "traceback": traceback.format_exc()})

# --------------------------------------------------------------------------
# Audio chunk processing
# --------------------------------------------------------------------------
def process_audio_chunk(call_sid: str, audio_chunk: bytes,
                        payload: Optional[Dict[str, Any]] = None,
                        trace_id: Optional[str] = None) -> None:
    trace = _resolve_trace_id(payload, trace_id)
    log_event(service=SERVICE_NAME, event="audio_chunk_received",
              message="Audio chunk received", trace_id=trace,
              extra={"call_sid": call_sid, "bytes": len(audio_chunk)})

    _store_call_state(call_sid, trace, {"last_chunk_received_at": int(time.time())})

    if not process_audio_buffer:
        log_event(service=SERVICE_NAME, event="asr_missing",
                  level="ERROR", message="ASR backend unavailable",
                  trace_id=trace)
        return

    try:
        consumed, result = process_audio_buffer(call_sid, bytearray(audio_chunk))
        if consumed:
            log_event(service=SERVICE_NAME, event="asr_consumed",
                      message="ASR consumed bytes", trace_id=trace,
                      extra={"call_sid": call_sid, "consumed": consumed})

        if not result:
            log_event(service=SERVICE_NAME, event="asr_no_result",
                      message="No transcript for chunk", trace_id=trace,
                      extra={"call_sid": call_sid})
            return

        rtype = result.get("type")
        text = result.get("text", "")
        if rtype == "partial":
            _handle_partial_result(call_sid, text, payload or {}, trace)
        elif rtype == "final":
            _handle_final_result(call_sid, text, payload or {}, trace)
        else:
            log_event(service=SERVICE_NAME, event="asr_unknown_type",
                      level="WARNING", message="ASR returned unknown type",
                      trace_id=trace, extra={"result": result})
    except Exception as e:
        log_event(service=SERVICE_NAME, event="asr_processing_failed",
                  level="ERROR", message=str(e), trace_id=trace,
                  extra={"call_sid": call_sid, "traceback": traceback.format_exc()})

# --------------------------------------------------------------------------
# Final transcript handling (same as before)
# --------------------------------------------------------------------------
def process_final_transcript(call_sid: str, final_text: str,
                             payload: Optional[Dict[str, Any]] = None,
                             trace_id: Optional[str] = None,
                             trigger_inference: bool = True) -> Dict[str, Any]:
    resolved = _resolve_trace_id(payload, trace_id)
    try:
        _store_call_state(call_sid, resolved,
                          {"last_transcript": final_text,
                           "last_transcript_ts": int(time.time()),
                           "status": "transcribed"})
        log_event(service=SERVICE_NAME, event="final_stored",
                  message="Final transcript stored", trace_id=resolved,
                  extra={"call_sid": call_sid, "preview": final_text[:150]})

        if twilio_client and hasattr(twilio_client, "update_final_transcript"):
            res = twilio_client.update_final_transcript(call_sid, final_text,
                                                        payload=payload,
                                                        trace_id=resolved,
                                                        auto_dispatch_inference=trigger_inference)
            log_event(service=SERVICE_NAME, event="final_dispatched_via_twilio_client",
                      message="Final dispatched via Twilio client",
                      trace_id=resolved, extra={"result": res})
            return {"ok": True, "via": "twilio_client"}
        else:
            celery.send_task(EVENT_TASK_NAME,
                             args=[{"type": "final_transcript",
                                    "callSid": call_sid, "text": final_text}],
                             kwargs={"trace_id": resolved})
            info = {"ok": True, "via": "celery_event"}
            if trigger_inference:
                info["inference"] = _dispatch_inference_final(final_text, call_sid, resolved)
            return info
    except Exception as e:
        log_event(service=SERVICE_NAME, event="final_store_failed",
                  level="ERROR", message=str(e), trace_id=resolved,
                  extra={"call_sid": call_sid, "traceback": traceback.format_exc()})
        return {"ok": False, "error": str(e)}

def _dispatch_inference_final(final_text: str, call_sid: str, trace_id: str) -> Dict[str, Any]:
    try:
        if twilio_client and hasattr(twilio_client, "_dispatch_inference_task"):
            res = twilio_client._dispatch_inference_task(final_text, call_sid, trace_id=trace_id)
            log_event(service=SERVICE_NAME, event="inference_dispatched_via_twilio_client",
                      message="Inference dispatched via Twilio client",
                      trace_id=trace_id, extra={"call_sid": call_sid})
            return {"ok": True, "meta": res}
        else:
            payload = {"trace_id": trace_id, "call_sid": call_sid,
                       "transcript": final_text, "metadata": {}}
            task = celery.send_task(INFERENCE_TASK_NAME, args=[payload], kwargs={"trace_id": trace_id})
            return {"ok": True, "task_id": getattr(task, "id", None)}
    except Exception as e:
        log_event(service=SERVICE_NAME, event="inference_dispatch_error",
                  level="ERROR", message=str(e), trace_id=trace_id,
                  extra={"call_sid": call_sid, "traceback": traceback.format_exc()})
        return {"ok": False, "error": str(e)}

# --------------------------------------------------------------------------
# Test hooks
# --------------------------------------------------------------------------
def _test_simulate_chunk_flow():
    test_call = "SIM-TEST-CALL-123"
    trace = get_trace_id()
    log_event(service=SERVICE_NAME, event="test_start", message="Simulating chunk flow", trace_id=trace)
    # Fake ASR stub output
    global process_audio_buffer
    def fake_asr(call_sid, buf):
        if len(buf) < 4000:
            return len(buf), {"type": "partial", "text": f"partial-{len(buf)}"}
        return len(buf), {"type": "final", "text": "This is the final transcript."}
    process_audio_buffer = fake_asr
    for size in [1000, 1200, 1500, 5000]:
        process_audio_chunk(test_call, b"x" * size, trace_id=trace)
        time.sleep(0.2)
    log_event(service=SERVICE_NAME, event="test_done", message="Simulation complete", trace_id=trace)

def _test_simulate_final_flow():
    call = "SIM-FINAL-456"
    trace = get_trace_id()
    res = process_final_transcript(call, "Simulated final transcript.", trace_id=trace, trigger_inference=False)
    log_event(service=SERVICE_NAME, event="test_final_result", message="Final flow result", trace_id=trace, extra={"result": res})

__all__ = [
    "process_audio_chunk", "process_final_transcript",
    "_store_call_state", "_get_call_state", "get_session_metadata",
    "clear_session", "_test_simulate_chunk_flow", "_test_simulate_final_flow"
]
# End of file
