# asr_service.py
# Phase 11-D — Voice Pipeline (ASR Integration, Post-Review Revision)
# ------------------------------------------------------------------
# Implements live and file-based ASR with unified Redis, metrics integration,
# and structured observability — Phase 11-D compliant.

import os
import io
import traceback
import time
from typing import Optional

import speech_recognition as sr

from logging_utils import log_event, get_trace_id
from twilio_client import (
    update_partial_transcript,
    update_final_transcript,
    get_trace_id as twilio_trace_id,
)
from tasks import run_inference

# --------------------------------------------------------------------------
# Phase 11-D Configuration Integration
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    # Fallback configuration for backward compatibility
    class Config:
        REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        SARA_ENV = os.getenv("SARA_ENV", "development")
        PARTIAL_THROTTLE_SECONDS = float(os.getenv("PARTIAL_THROTTLE_SECONDS", "1.5"))
        ASR_ENGINE = os.getenv("ASR_ENGINE", "speech_recognition")

# --------------------------------------------------------------------------
# Phase 11-D Redis Integration
# --------------------------------------------------------------------------
try:
    from redis_client import get_client
except ImportError:
    # Fallback Redis client
    def get_client():
        return None

# --------------------------------------------------------------------------
# Phase 11-D Metrics Integration
# --------------------------------------------------------------------------
try:
    from metrics_collector import increment_metric, observe_latency
except ImportError:
    # Fallback metrics functions
    def increment_metric(metric_name: str, value: int = 1):
        pass
    
    def observe_latency(metric_name: str, latency_ms: float):
        pass

# --------------------------------------------------------------------------
# Phase 11-D Metric Constants
# --------------------------------------------------------------------------
ASR_PARTIAL_METRIC = "asr_partial_chunks_total"
ASR_FINAL_METRIC = "asr_final_chunks_total" 
ASR_FAILURE_METRIC = "asr_failures_total"
ASR_LATENCY_METRIC = "asr_processing_latency_ms"

# --------------------------------------------------------------------------
# Internal Utilities
# --------------------------------------------------------------------------

def _safe_get_redis():
    """Return Redis client if available; log warning if unreachable."""
    try:
        r = get_client()
        if r:
            r.ping()  # Test connection
        return r
    except Exception as e:
        log_event(
            service="asr_service",
            event="redis_unavailable",
            status="warning",
            message="Redis client unavailable",
            error=str(e),
            schema_version="phase_11d_v1",
            service_version="11d"
        )
        return None


def throttle_partial_dispatch(call_sid: str) -> bool:
    """
    Enforces throttling for partial transcripts using Redis.
    Returns True if dispatch is allowed, False if within throttle window.
    """
    redis_client = _safe_get_redis()
    if not redis_client:
        return True  # allow dispatch if Redis is down

    key = f"partial_throttle:{call_sid}"
    if redis_client.exists(key):
        # 🔹 Added: debug log for skipped chunk
        log_event(
            service="asr_service",
            event="partial_throttle_skipped",
            status="debug",
            message="Partial transcript throttled",
            call_sid=call_sid,
            schema_version="phase_11d_v1",
            service_version="11d"
        )
        return False

    try:
        redis_client.setex(key, int(Config.PARTIAL_THROTTLE_SECONDS), "1")
    except Exception as e:
        log_event(
            service="asr_service",
            event="redis_throttle_error",
            status="warning",
            message="Redis throttle error",
            error=str(e),
            schema_version="phase_11d_v1",
            service_version="11d"
        )
    return True


def _convert_audio_to_text(audio_bytes: bytes) -> Optional[str]:
    """Convert raw audio bytes to text using SpeechRecognition."""
    start_time = time.time()
    recognizer = sr.Recognizer()
    try:
        with sr.AudioFile(io.BytesIO(audio_bytes)) as source:
            audio = recognizer.record(source)
        text = recognizer.recognize_google(audio)
        
        # Phase 11-D: Record latency
        latency_ms = (time.time() - start_time) * 1000
        observe_latency(ASR_LATENCY_METRIC, latency_ms)
        
        return text
    except Exception as e:
        # Phase 11-D: Increment failure metric
        increment_metric(ASR_FAILURE_METRIC)
        
        log_event(
            service="asr_service",
            event="asr_conversion_error",
            status="error",
            message="ASR conversion failed",
            error=str(e),
            traceback=traceback.format_exc(),
            schema_version="phase_11d_v1",
            service_version="11d"
        )
        return None


def _store_partial_state(call_sid: str, partial_text: str, trace_id: str):
    """Store last partial transcript in Redis (non-critical)."""
    redis_client = _safe_get_redis()
    if not redis_client:
        return
    try:
        redis_client.setex(f"call:{call_sid}:partial", 14400, partial_text)
    except Exception as e:
        log_event(
            service="asr_service",
            event="redis_partial_store_error",
            status="warning",
            message="Failed to store partial transcript",
            error=str(e),
            schema_version="phase_11d_v1",
            service_version="11d"
        )


def _store_final_state(call_sid: str, final_text: str, trace_id: str):
    """Store final transcript in Redis (non-critical)."""
    redis_client = _safe_get_redis()
    if not redis_client:
        return
    try:
        redis_client.setex(f"call:{call_sid}:final", 14400, final_text)
    except Exception as e:
        log_event(
            service="asr_service",
            event="redis_final_store_error",
            status="warning",
            message="Failed to store final transcript",
            error=str(e),
            schema_version="phase_11d_v1",
            service_version="11d"
        )

# --------------------------------------------------------------------------
# Core API Functions
# --------------------------------------------------------------------------

def process_audio_stream(call_sid: str, audio_chunk: bytes, trace_id: Optional[str] = None):
    """Process an incoming audio chunk for partial recognition."""
    trace_id = trace_id or twilio_trace_id()
    if not throttle_partial_dispatch(call_sid):
        return

    log_event(
        service="asr_service",
        event="partial_chunk_received",
        status="ok",
        message="Partial audio chunk received",
        call_sid=call_sid,
        trace_id=trace_id,
        size=len(audio_chunk),
        schema_version="phase_11d_v1",
        service_version="11d"
    )

    text = _convert_audio_to_text(audio_chunk)
    if not text:
        return

    try:
        update_partial_transcript(call_sid, text, {"chunk_size": len(audio_chunk)}, trace_id)
        _store_partial_state(call_sid, text, trace_id)
        
        # Phase 11-D: Increment partial chunk metric
        increment_metric(ASR_PARTIAL_METRIC)
        
        # 🔹 Truncate logged text to first 200 chars for readability
        log_event(
            service="asr_service",
            event="partial_transcript_processed",
            status="ok",
            message="Partial transcript processed successfully",
            call_sid=call_sid,
            trace_id=trace_id,
            partial_text=text[:200],
            schema_version="phase_11d_v1",
            service_version="11d"
        )
    except Exception as e:
        # Phase 11-D: Increment failure metric
        increment_metric(ASR_FAILURE_METRIC)
        
        log_event(
            service="asr_service",
            event="partial_update_failed",
            status="error",
            message="Partial transcript update failed",
            error=str(e),
            trace_id=trace_id,
            schema_version="phase_11d_v1",
            service_version="11d"
        )


def process_final_audio(call_sid: str, audio_data: bytes, trace_id: Optional[str] = None):
    """Process final audio at end-of-call or upload."""
    trace_id = trace_id or twilio_trace_id()
    log_event(
        service="asr_service",
        event="final_audio_received",
        status="ok",
        message="Final audio received",
        call_sid=call_sid,
        trace_id=trace_id,
        size=len(audio_data),
        schema_version="phase_11d_v1",
        service_version="11d"
    )

    text = _convert_audio_to_text(audio_data)
    if not text:
        log_event(
            service="asr_service",
            event="final_asr_conversion_failed",
            status="error",
            message="Final ASR conversion failed",
            call_sid=call_sid,
            trace_id=trace_id,
            schema_version="phase_11d_v1",
            service_version="11d"
        )
        return

    try:
        # 🔹 Pass auto_dispatch_inference=False to avoid duplicate Celery task
        update_final_transcript(
            call_sid,
            text,
            {"length": len(audio_data)},
            trace_id,
            auto_dispatch_inference=False,
        )
        _store_final_state(call_sid, text, trace_id)

        # Phase 11-D: Increment final chunk metric
        increment_metric(ASR_FINAL_METRIC)

        log_event(
            service="asr_service",
            event="final_transcript_processed",
            status="ok",
            message="Final transcript processed successfully",
            call_sid=call_sid,
            trace_id=trace_id,
            final_text=text[:200],
            schema_version="phase_11d_v1",
            service_version="11d"
        )

        # Single non-blocking inference dispatch (no duplication)
        run_inference.delay({"text": text, "call_sid": call_sid, "trace_id": trace_id})

    except Exception as e:
        # Phase 11-D: Increment failure metric
        increment_metric(ASR_FAILURE_METRIC)
        
        log_event(
            service="asr_service",
            event="final_update_failed",
            status="error",
            message="Final transcript update failed",
            error=str(e),
            trace_id=trace_id,
            schema_version="phase_11d_v1",
            service_version="11d"
        )

# --------------------------------------------------------------------------
# Phase 11-D Health Monitoring
# --------------------------------------------------------------------------
def health_check() -> dict:
    """Return ASR + Redis health snapshot for diagnostics."""
    redis_status = "ok"
    client = get_client()
    try:
        if client:
            client.ping()
        else:
            redis_status = "unavailable"
    except Exception:
        redis_status = "error"
    
    health_info = {
        "service": "asr_service",
        "status": "ok",
        "redis_status": redis_status,
        "asr_engine": Config.ASR_ENGINE,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    
    # Log health status
    log_event(
        service="asr_service",
        event="health_check_completed",
        status="ok" if redis_status == "ok" else "warning",
        message="ASR service health check completed",
        extra=health_info,
        schema_version="phase_11d_v1",
        service_version="11d"
    )
    
    return health_info

# --------------------------------------------------------------------------
# CLI Debug Utility
# --------------------------------------------------------------------------

if __name__ == "__main__":
    """
    Local debug utility for file-based ASR testing:
      $ python asr_service.py <audio_file.wav> <call_sid>
    """
    import sys
    if len(sys.argv) < 3:
        print("Usage: python asr_service.py <audio_file.wav> <call_sid>")
        sys.exit(1)

    file_path = sys.argv[1]
    call_sid = sys.argv[2]
    with open(file_path, "rb") as f:
        audio_bytes = f.read()

    process_final_audio(call_sid, audio_bytes)
    print(f"[ASR] Processed final audio for call_sid={call_sid}")