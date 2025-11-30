# asr_service.py
# Phase 11-D — Voice Pipeline (ASR Integration, Post-Review Revision)
# ------------------------------------------------------------------
# Implements live and file-based ASR with unified Redis, metrics integration,
# and structured observability — Phase 11-D compliant.

import os
import io
import traceback
import time
import signal
import sys
import logging
from typing import Optional

import speech_recognition as sr

from logging_utils import log_event
from tasks import run_inference

# --------------------------------------------------------------------------
# Safe Twilio imports with fallback stubs only if import fails
# --------------------------------------------------------------------------
try:
    from twilio_client import (
        update_partial_transcript,
        update_final_transcript,
        get_trace_id as twilio_trace_id,
    )
except Exception:
    logging.getLogger(__name__).warning("twilio_client import failed; using fallback stubs")
    def update_partial_transcript(*args, **kwargs):
        logging.getLogger(__name__).warning("Fallback: update_partial_transcript() called but not implemented.")
    def update_final_transcript(*args, **kwargs):
        logging.getLogger(__name__).warning("Fallback: update_final_transcript() called but not implemented.")
    def twilio_trace_id():
        return "twilio-fallback-trace"

# --------------------------------------------------------------------------
# Phase 11-D Configuration Integration
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    # Fallback configuration for backward compatibility
    class Config:
        REDIS_URL = os.getenv("REDIS_URL", "redis://red-d43ertemcj7s73b0qrcg:6379/0")
        SARA_ENV = os.getenv("SARA_ENV", "production")
        PARTIAL_THROTTLE_SECONDS = float(os.getenv("PARTIAL_THROTTLE_SECONDS", "1.5"))
        ASR_ENGINE = os.getenv("ASR_ENGINE", "speech_recognition")

# --------------------------------------------------------------------------
# Phase 11-D Redis Integration
# --------------------------------------------------------------------------
try:
    from redis_client import get_redis_client as get_client
except ImportError:
    # Fallback Redis client
    def get_client():
        return None

# --------------------------------------------------------------------------
# Phase 11-D Metrics Integration (Lazy Load)
# --------------------------------------------------------------------------
# Lazy metrics shim to avoid circular imports at import-time
def _get_metrics():
    try:
        from metrics_collector import increment_metric as _inc, observe_latency as _obs
        return _inc, _obs
    except Exception:
        # safe no-op fallbacks
        def _noop_inc(*a, **k): pass
        def _noop_obs(*a, **k): pass
        return _noop_inc, _noop_obs

# --------------------------------------------------------------------------
# Phase 11-D Metric Constants
# --------------------------------------------------------------------------
ASR_PARTIAL_METRIC = "asr_partial_chunks_total"
ASR_FINAL_METRIC = "asr_final_chunks_total" 
ASR_FAILURE_METRIC = "asr_failures_total"
ASR_LATENCY_METRIC = "asr_processing_latency_ms"

# --------------------------------------------------------------------------
# Logger Setup
# --------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Signal Handlers
# --------------------------------------------------------------------------
def _graceful_shutdown(signum, frame):
    """Phase 12: Graceful shutdown handler"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    try:
        # Flush metrics and resources if available
        try:
            from global_metrics_store import flush_now
            flush_now()
            logger.info("Metrics flushed during shutdown")
        except Exception:
            pass

        try:
            from conversation_state_manager import flush_all_sessions_to_redis
            flush_all_sessions_to_redis()
            logger.info("Conversation states flushed during shutdown")
        except Exception:
            pass

        try:
            from r2_client import close as r2_close
            r2_close()
            logger.info("R2 client closed during shutdown")
        except Exception:
            pass
    except Exception:
        logger.exception("Error during graceful shutdown cleanup")
    finally:
        sys.exit(0)

signal.signal(signal.SIGINT, _graceful_shutdown)
signal.signal(signal.SIGTERM, _graceful_shutdown)

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
        # Try BytesIO first for efficiency
        with sr.AudioFile(io.BytesIO(audio_bytes)) as source:
            audio = recognizer.record(source)
        text = recognizer.recognize_google(audio)
        
        # Phase 11-D: Record latency (lazy metrics)
        latency_ms = (time.time() - start_time) * 1000
        _inc, _obs = _get_metrics()
        _obs(ASR_LATENCY_METRIC, latency_ms)
        
        return text
    except Exception as e:
        # Fallback to temporary file if BytesIO fails
        try:
            from tempfile import NamedTemporaryFile
            with NamedTemporaryFile(suffix=".wav", delete=True) as tmp:
                tmp.write(audio_bytes)
                tmp.flush()
                with sr.AudioFile(tmp.name) as source:
                    audio = recognizer.record(source)
            text = recognizer.recognize_google(audio)
            
            latency_ms = (time.time() - start_time) * 1000
            _inc, _obs = _get_metrics()
            _obs(ASR_LATENCY_METRIC, latency_ms)
            
            return text
        except Exception as fallback_error:
            # Phase 11-D: Increment failure metric (lazy metrics)
            _inc, _obs = _get_metrics()
            _inc(ASR_FAILURE_METRIC)
            
            log_event(
                service="asr_service",
                event="asr_conversion_error",
                status="error",
                message="ASR conversion failed with both BytesIO and tempfile",
                error=f"BytesIO: {str(e)}, Tempfile: {str(fallback_error)}",
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
        
        # Phase 11-D: Increment partial chunk metric (lazy metrics)
        _inc, _obs = _get_metrics()
        _inc(ASR_PARTIAL_METRIC)
        
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
        # Phase 11-D: Increment failure metric (lazy metrics)
        _inc, _obs = _get_metrics()
        _inc(ASR_FAILURE_METRIC)
        
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

        # Phase 11-D: Increment final chunk metric (lazy metrics)
        _inc, _obs = _get_metrics()
        _inc(ASR_FINAL_METRIC)

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
        # Phase 11-D: Increment failure metric (lazy metrics)
        _inc, _obs = _get_metrics()
        _inc(ASR_FAILURE_METRIC)
        
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
        logger.error("Usage: python asr_service.py <audio_file.wav> <call_sid>")
        sys.exit(1)

    file_path = sys.argv[1]
    call_sid = sys.argv[2]
    with open(file_path, "rb") as f:
        audio_bytes = f.read()

    process_final_audio(call_sid, audio_bytes)
    logger.info(f"Processed final audio for call_sid={call_sid}")