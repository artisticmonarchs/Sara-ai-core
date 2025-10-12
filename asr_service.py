# asr_service.py
# Phase 8B — Voice Pipeline (ASR Integration, Post-Review Revision)
# ------------------------------------------------------------------
# Implements live and file-based ASR, Redis integration, Twilio client state
# updates, and Celery dispatch — refined per Mike’s feedback.

import os
import io
import traceback
from typing import Optional

import speech_recognition as sr

from logging_utils import log_event, get_trace_id
from twilio_client import (
    _get_redis,
    update_partial_transcript,
    update_final_transcript,
    get_trace_id as twilio_trace_id,
)
from tasks import run_inference

# --------------------------------------------------------------------------
# Environment Configuration
# --------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SARA_ENV = os.getenv("SARA_ENV", "development")
PARTIAL_THROTTLE_SECONDS = float(os.getenv("PARTIAL_THROTTLE_SECONDS", "1.5"))
ASR_ENGINE = os.getenv("ASR_ENGINE", "speech_recognition")

# --------------------------------------------------------------------------
# Internal Utilities
# --------------------------------------------------------------------------

def _safe_get_redis():
    """Return Redis client if available; log warning if unreachable."""
    try:
        r = _get_redis()
        r.ping()  # keep for now—redundant but safe
        return r
    except Exception as e:
        log_event(
            service="asr_service",
            event="redis_unavailable",
            level="WARNING",
            error=str(e),
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
            level="DEBUG",
            call_sid=call_sid,
        )
        return False

    try:
        redis_client.setex(key, int(PARTIAL_THROTTLE_SECONDS), "1")
    except Exception as e:
        log_event(
            service="asr_service",
            event="redis_throttle_error",
            level="WARNING",
            error=str(e),
        )
    return True


def _convert_audio_to_text(audio_bytes: bytes) -> Optional[str]:
    """Convert raw audio bytes to text using SpeechRecognition."""
    recognizer = sr.Recognizer()
    try:
        with sr.AudioFile(io.BytesIO(audio_bytes)) as source:
            audio = recognizer.record(source)
        return recognizer.recognize_google(audio)
    except Exception as e:
        log_event(
            service="asr_service",
            event="asr_conversion_error",
            level="ERROR",
            error=str(e),
            traceback=traceback.format_exc(),
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
            level="WARNING",
            error=str(e),
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
            level="WARNING",
            error=str(e),
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
        call_sid=call_sid,
        trace_id=trace_id,
        size=len(audio_chunk),
    )

    text = _convert_audio_to_text(audio_chunk)
    if not text:
        return

    try:
        update_partial_transcript(call_sid, text, {"chunk_size": len(audio_chunk)}, trace_id)
        _store_partial_state(call_sid, text, trace_id)
        # 🔹 Truncate logged text to first 200 chars for readability
        log_event(
            service="asr_service",
            event="partial_transcript_processed",
            call_sid=call_sid,
            trace_id=trace_id,
            partial_text=text[:200],
        )
    except Exception as e:
        log_event(
            service="asr_service",
            event="partial_update_failed",
            level="ERROR",
            error=str(e),
            trace_id=trace_id,
        )


def process_final_audio(call_sid: str, audio_data: bytes, trace_id: Optional[str] = None):
    """Process final audio at end-of-call or upload."""
    trace_id = trace_id or twilio_trace_id()
    log_event(
        service="asr_service",
        event="final_audio_received",
        call_sid=call_sid,
        trace_id=trace_id,
        size=len(audio_data),
    )

    text = _convert_audio_to_text(audio_data)
    if not text:
        log_event(
            service="asr_service",
            event="final_asr_conversion_failed",
            level="ERROR",
            call_sid=call_sid,
            trace_id=trace_id,
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

        log_event(
            service="asr_service",
            event="final_transcript_processed",
            call_sid=call_sid,
            trace_id=trace_id,
            final_text=text[:200],
        )

        # Single non-blocking inference dispatch (no duplication)
        run_inference.delay({"text": text, "call_sid": call_sid, "trace_id": trace_id})

    except Exception as e:
        log_event(
            service="asr_service",
            event="final_update_failed",
            level="ERROR",
            error=str(e),
            trace_id=trace_id,
        )

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
