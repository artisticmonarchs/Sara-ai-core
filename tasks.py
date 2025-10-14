"""
sara_ai/tasks.py — Robust TTS pipeline (Phase 10G / 10H-ready)

Changes & features:
- The Celery task remains registered as "sara_ai.tasks.run_tts" so existing
  enqueueing works unchanged.
- A proxy object `run_tts` is exported which:
    - preserves `.delay(...)` / `.apply_async(...)` for async enqueueing
    - supports direct synchronous calls: run_tts(payload, inline=True)
- Core TTS logic is factored into `perform_tts_core(payload)` so both
  the Celery task and the inline path reuse the same code.
- Keep existing Deepgram REST flow and atomic file writes.
- Public URL behavior continues to be controlled by env vars.
"""

import os
import io
import time
import uuid
import json
import traceback
import logging
import requests

from celery_app import celery
from logging_utils import log_event
from redis import Redis
from gpt_client import generate_reply

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DG_API_KEY = os.getenv("DEEPGRAM_API_KEY")
DG_SPEAK_MODEL = os.getenv("DEEPGRAM_SPEAK_MODEL", "aura-2-asteria-en")
PUBLIC_AUDIO_PATH = os.getenv("PUBLIC_AUDIO_PATH", "public/audio")
PUBLIC_AUDIO_BASE_URL = os.getenv("PUBLIC_AUDIO_BASE_URL", "/audio")
PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "")  # e.g., https://sara-ai-core-app.onrender.com
MAX_TTS_TEXT_LEN = int(os.getenv("MAX_TTS_TEXT_LEN", "2000"))
AUDIO_TMP_SUFFIX = ".tmp"

# Celery retry defaults (env-overridable)
CELERY_RETRY_MAX = int(os.getenv("CELERY_RETRY_MAX", "5"))
CELERY_RETRY_BACKOFF_MAX = int(os.getenv("CELERY_RETRY_BACKOFF_MAX", "600"))

# --------------------------------------------------------------------------
# Clients
# --------------------------------------------------------------------------
redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Startup guard
# --------------------------------------------------------------------------
if not DG_API_KEY:
    log_event(
        service="tasks",
        event="config_warning",
        status="warn",
        message="DEEPGRAM_API_KEY missing — TTS will fail until configured.",
        trace_id=str(uuid.uuid4()),
    )
    logger.warning("DEEPGRAM_API_KEY not set. TTS will fail until configured.")

# --------------------------------------------------------------------------
# Helpers (same as before, factored)
# --------------------------------------------------------------------------
def get_trace():
    return str(uuid.uuid4())


def normalize_payload(payload):
    """Ensure Celery payload is a dict (unwrap lists/tuples, parse JSON strings)."""
    if isinstance(payload, (list, tuple)) and payload:
        payload = payload[0]

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {"text": str(payload)}

    if not isinstance(payload, dict):
        log_event(
            service="tasks",
            event="payload_warning",
            status="warn",
            message="Non-dict payload received, defaulting to empty dict",
            trace_id=get_trace(),
        )
        payload = {}

    return payload


def extract_text(payload):
    """Flexible recursive text extractor for nested payloads."""
    if not payload:
        return ""
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        for key in ("sara_text", "text", "message", "input"):
            v = payload.get(key)
            if isinstance(v, str) and v.strip():
                return v
        # deep dive into nested structures
        for v in payload.values():
            if isinstance(v, (dict, list, tuple, str)):
                nested = extract_text(v)
                if nested:
                    return nested
    if isinstance(payload, (list, tuple)):
        for item in payload:
            nested = extract_text(item)
            if nested:
                return nested
    return ""


def make_public_url(session_id: str, trace_id: str) -> str:
    """Return absolute public URL if PUBLIC_AUDIO_HOST provided, else base path."""
    rel = f"{PUBLIC_AUDIO_BASE_URL.rstrip('/')}/{session_id}/{trace_id}.wav"
    if PUBLIC_AUDIO_HOST:
        host = PUBLIC_AUDIO_HOST.rstrip("/")
        return f"{host}{rel}"
    return rel


def save_audio_file_atomic(session_id: str, trace_id: str, audio_bytes: bytes) -> str:
    """Write audio file atomically (tmp -> rename). Returns final path."""
    folder = os.path.join(PUBLIC_AUDIO_PATH, session_id)
    os.makedirs(folder, exist_ok=True)
    final_path = os.path.join(folder, f"{trace_id}.wav")
    tmp_path = final_path + AUDIO_TMP_SUFFIX
    with open(tmp_path, "wb") as f:
        f.write(audio_bytes)
    os.replace(tmp_path, final_path)  # atomic across same filesystem
    return final_path


def tts_error_response(code: str, message: str, trace_id: str, session_id: str):
    """Standardized error payload and logging/metrics increment."""
    try:
        redis_client.hincrby("metrics:tts", "failures", 1)
    except Exception:
        logger.exception("Failed to increment tts failure metric")

    log_event(
        service="tasks",
        event="tts_error",
        status="error",
        message=message,
        trace_id=trace_id,
        session_id=session_id,
        extra={"code": code},
    )
    return {"error_code": code, "error_message": message, "trace_id": trace_id, "session_id": session_id}


def deepgram_tts_rest(text: str) -> bytes:
    """Call Deepgram REST TTS endpoint with timeout and error handling."""

    # TODO: if you already have a Deepgram helper, replace this implementation
    # with your existing call. This implementation uses REST speak endpoint.
    if not DG_API_KEY:
        raise RuntimeError("Missing Deepgram API key")

    url = f"https://api.deepgram.com/v1/speak?model={DG_SPEAK_MODEL}&encoding=linear16&container=wav"
    headers = {
        "Authorization": f"Token {DG_API_KEY}",
        "Content-Type": "text/plain",
    }

    try:
        response = requests.post(url, headers=headers, data=text.encode("utf-8"), timeout=15)
    except requests.Timeout as te:
        logger.exception("Deepgram timeout")
        raise RuntimeError("Deepgram API timeout after 15s") from te
    except requests.RequestException as re:
        logger.exception("Deepgram request failed")
        raise RuntimeError("Deepgram request failed") from re

    if response.status_code != 200:
        logger.error("Deepgram responded non-200: %s - %s", response.status_code, response.text)
        raise RuntimeError(f"Deepgram TTS failed: {response.status_code} - {response.text}")

    return response.content

# --------------------------------------------------------------------------
# Core TTS implementation (factored out so both inline and the Celery task reuse it)
# --------------------------------------------------------------------------
def perform_tts_core(payload):
    """
    Core synchronous TTS logic.
    Accepts payload dict-like. Returns dict with audio_url or error payload.
    """
    # Diagnostic raw payload logging
    try:
        log_event(
            service="tasks",
            event="tts_debug_raw_payload",
            status="ok",
            message=f"RAW PAYLOAD RECEIVED: type={type(payload)} value={repr(payload)[:500]}",
        )
    except Exception:
        logger.exception("Failed to emit tts_debug_raw_payload")

    payload = normalize_payload(payload)
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    text = extract_text(payload)

    if text and len(text) > MAX_TTS_TEXT_LEN:
        original_len = len(text)
        text = text[:MAX_TTS_TEXT_LEN]
        log_event(
            service="tasks",
            event="tts_text_truncated",
            status="warn",
            message=f"Truncated text from {original_len} to {len(text)} chars",
            trace_id=trace_id,
            session_id=session_id,
            extra={"original_len": original_len, "trimmed_to": len(text)},
        )

    log_event(
        service="tasks",
        event="tts_start",
        status="ok",
        message=f"TTS task started for {len(text)} chars",
        trace_id=trace_id,
        session_id=session_id,
    )

    if not text:
        return tts_error_response("NO_TEXT", "No text provided for TTS", trace_id, session_id)

    start_time = time.time()
    try:
        audio_bytes = deepgram_tts_rest(text)
        audio_path = save_audio_file_atomic(session_id, trace_id, audio_bytes)
        duration = round(time.time() - start_time, 2)
        public_url = make_public_url(session_id, trace_id)

        # Success metric
        try:
            redis_client.hincrby("metrics:tts", "files_generated", 1)
        except Exception:
            logger.exception("Failed to increment tts files_generated metric")

        log_event(
            service="tasks",
            event="tts_done",
            status="ok",
            message=f"TTS audio generated ({len(text)} chars, {duration}s)",
            trace_id=trace_id,
            session_id=session_id,
            extra={"audio_path": audio_path, "duration_s": duration, "public_url": public_url},
        )

        return {"trace_id": trace_id, "session_id": session_id, "audio_url": public_url, "audio_path": audio_path}

    except Exception as e:
        # Log and increment failure metric & return standardized error payload
        err_msg = traceback.format_exc()
        try:
            redis_client.hincrby("metrics:tts", "failures", 1)
        except Exception:
            logger.exception("Failed to increment tts failures metric")

        log_event(
            service="tasks",
            event="tts_exception",
            status="error",
            message=err_msg,
            trace_id=trace_id,
            session_id=session_id,
            extra={"exception": str(e)},
        )

        return tts_error_response("TTS_FAILURE", str(e), trace_id, session_id)

# --------------------------------------------------------------------------
# Celery task wrapper (keeps the legacy task name)
# --------------------------------------------------------------------------
@celery.task(
    name="sara_ai.tasks.run_tts",
    bind=True,
    autoretry_for=(requests.RequestException, RuntimeError),
    retry_backoff=True,
    retry_backoff_max=CELERY_RETRY_BACKOFF_MAX,
    retry_kwargs={"max_retries": CELERY_RETRY_MAX},
)
def _run_tts_task(self, payload=None):
    """
    This is the Celery task entrypoint. It simply calls the core synchronous
    implementation so both codepaths stay identical.
    """
    # Celery will handle retries per the decorator settings.
    return perform_tts_core(payload)

# --------------------------------------------------------------------------
# Proxy object: preserves .delay(...) behavior for existing code while allowing
# inline synchronous calls run_tts(payload, inline=True)
# --------------------------------------------------------------------------
class RunTTSProxy:
    def __init__(self, celery_task):
        self._task = celery_task

    def __call__(self, payload=None, inline=False):
        """
        If inline=True, run synchronously and return the result dict.
        If inline=False, enqueue to Celery (returns AsyncResult).
        """
        if inline:
            return perform_tts_core(payload)
        # Default async enqueue via kwargs to avoid arg-wrapping inconsistencies
        return self._task.apply_async(kwargs={"payload": payload})

    # preserve .delay for compatibility
    def delay(self, payload=None):
        return self._task.apply_async(kwargs={"payload": payload})

    # preserve apply_async for compatibility
    def apply_async(self, *args, **kwargs):
        return self._task.apply_async(*args, **kwargs)

# instantiate proxy and export as run_tts
run_tts = RunTTSProxy(_run_tts_task)

# --------------------------------------------------------------------------
# Inference task (left unchanged, still sends tasks by name)
# --------------------------------------------------------------------------
@celery.task(name="sara_ai.tasks.run_inference", bind=True)
def run_inference(self, payload):
    payload = normalize_payload(payload)

    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    transcript = extract_text(payload)

    trace_id = log_event(
        service="tasks",
        event="inference_start",
        status="ok",
        message=f"Received transcript ({len(transcript)} chars)",
        trace_id=trace_id,
        session_id=session_id,
    )

    start_time = time.time()
    try:
        reply_text = generate_reply(transcript, trace_id=trace_id)

        # Enqueue TTS using kwargs to avoid args-wrapping across brokers/serializers
        celery.send_task(
            "sara_ai.tasks.run_tts",
            kwargs={"payload": {"text": reply_text, "trace_id": trace_id, "session_id": session_id}},
        )

        latency_ms = round((time.time() - start_time) * 1000, 2)
        log_event(
            service="tasks",
            event="inference_done",
            status="ok",
            message=f"Inference completed ({len(reply_text)} chars, {latency_ms}ms)",
            trace_id=trace_id,
            session_id=session_id,
            extra={"latency_ms": latency_ms},
        )

        return {"trace_id": trace_id, "session_id": session_id, "reply": reply_text}

    except Exception:
        err_msg = traceback.format_exc()
        log_event(
            service="tasks",
            event="inference_error",
            status="error",
            message=err_msg,
            trace_id=trace_id,
            session_id=session_id,
        )
        raise
