"""
tasks.py — Phase 10H (R2 Integrated) Production Version

Features:
- Generates TTS audio via Deepgram
- Uploads directly to Cloudflare R2 using boto3 (S3-compatible)
- Returns public R2 URLs instead of local file paths
- Maintains Celery task and inline synchronous execution paths
- Logs events, errors, and metrics to Redis and log_event
"""

import os
import io
import time
import uuid
import json
import traceback
import logging
import requests
import boto3
from redis import Redis
from celery_app import celery
from gpt_client import generate_reply
from logging_utils import log_event

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DG_API_KEY = os.getenv("DEEPGRAM_API_KEY")
DG_SPEAK_MODEL = os.getenv("DEEPGRAM_SPEAK_MODEL", "aura-2-asteria-en")
PUBLIC_AUDIO_BASE_URL = os.getenv("PUBLIC_AUDIO_BASE_URL", "/audio")
PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "")
MAX_TTS_TEXT_LEN = int(os.getenv("MAX_TTS_TEXT_LEN", "2000"))

# R2 config
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_REGION = os.getenv("R2_REGION", "auto")

# Retry settings
CELERY_RETRY_MAX = int(os.getenv("CELERY_RETRY_MAX", "5"))
CELERY_RETRY_BACKOFF_MAX = int(os.getenv("CELERY_RETRY_BACKOFF_MAX", "600"))

# --------------------------------------------------------------------------
# Clients
# --------------------------------------------------------------------------
redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
logger = logging.getLogger(__name__)

# S3-compatible boto3 client for R2
s3_client = boto3.client(
    "s3",
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name=R2_REGION,
)

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def get_trace():
    return str(uuid.uuid4())

def normalize_payload(payload):
    """Ensure Celery payload is a dict."""
    if isinstance(payload, (list, tuple)) and payload:
        payload = payload[0]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {"text": str(payload)}
    if not isinstance(payload, dict):
        payload = {}
    return payload

def extract_text(payload):
    """Flexible recursive text extractor."""
    if not payload:
        return ""
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        for key in ("sara_text", "text", "message", "input"):
            v = payload.get(key)
            if isinstance(v, str) and v.strip():
                return v
        for v in payload.values():
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
    """Constructs public R2 URL for the uploaded file."""
    filename = f"{session_id}/{trace_id}.wav"
    return f"{PUBLIC_AUDIO_HOST.rstrip('/')}/{filename}"

def upload_to_r2(session_id: str, trace_id: str, audio_bytes: bytes):
    """Upload audio to R2 via boto3."""
    key = f"{session_id}/{trace_id}.wav"
    s3_client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=key,
        Body=audio_bytes,
        ContentType="audio/wav",
    )
    return make_public_url(session_id, trace_id)

def tts_error_response(code: str, message: str, trace_id: str, session_id: str):
    """Standardized error response and metric logging."""
    try:
        redis_client.hincrby("metrics:tts", "failures", 1)
    except Exception:
        logger.exception("Failed to increment TTS failure metric")

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

# --------------------------------------------------------------------------
# Deepgram TTS
# --------------------------------------------------------------------------
def deepgram_tts_rest(text: str) -> bytes:
    """Call Deepgram REST API to generate TTS audio."""
    if not DG_API_KEY:
        raise RuntimeError("Missing Deepgram API key")

    url = f"https://api.deepgram.com/v1/speak?model={DG_SPEAK_MODEL}&encoding=linear16&container=wav"
    headers = {"Authorization": f"Token {DG_API_KEY}", "Content-Type": "text/plain"}

    try:
        response = requests.post(url, headers=headers, data=text.encode("utf-8"), timeout=20)
    except requests.Timeout as e:
        raise RuntimeError("Deepgram TTS timeout") from e
    except requests.RequestException as e:
        raise RuntimeError("Deepgram TTS request failed") from e

    if response.status_code != 200:
        raise RuntimeError(f"Deepgram returned {response.status_code}: {response.text}")

    return response.content

# --------------------------------------------------------------------------
# Core TTS Logic
# --------------------------------------------------------------------------
def perform_tts_core(payload):
    payload = normalize_payload(payload)
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    text = extract_text(payload)

    if not text:
        return tts_error_response("NO_TEXT", "No text provided for TTS", trace_id, session_id)

    if len(text) > MAX_TTS_TEXT_LEN:
        text = text[:MAX_TTS_TEXT_LEN]
        log_event(
            service="tasks",
            event="tts_text_truncated",
            status="warn",
            message=f"TTS text truncated to {MAX_TTS_TEXT_LEN} chars",
            trace_id=trace_id,
            session_id=session_id,
        )

    log_event(
        service="tasks",
        event="tts_start",
        status="ok",
        message="TTS generation started",
        trace_id=trace_id,
        session_id=session_id,
    )

    try:
        start_time = time.time()
        audio_bytes = deepgram_tts_rest(text)
        public_url = upload_to_r2(session_id, trace_id, audio_bytes)
        duration = round(time.time() - start_time, 2)

        redis_client.hincrby("metrics:tts", "files_generated", 1)

        log_event(
            service="tasks",
            event="tts_done",
            status="ok",
            message=f"TTS completed ({len(text)} chars, {duration}s)",
            trace_id=trace_id,
            session_id=session_id,
            extra={"audio_url": public_url},
        )

        return {"trace_id": trace_id, "session_id": session_id, "audio_url": public_url}

    except Exception as e:
        err = traceback.format_exc()
        log_event(
            service="tasks",
            event="tts_exception",
            status="error",
            message=err,
            trace_id=trace_id,
            session_id=session_id,
        )
        return tts_error_response("TTS_FAILURE", str(e), trace_id, session_id)

# --------------------------------------------------------------------------
# Celery Task (async)
# --------------------------------------------------------------------------
@celery.task(
    name="tasks.run_tts",
    bind=True,
    autoretry_for=(requests.RequestException, RuntimeError),
    retry_backoff=True,
    retry_backoff_max=CELERY_RETRY_BACKOFF_MAX,
    retry_kwargs={"max_retries": CELERY_RETRY_MAX},
)
def _run_tts_task(self, payload=None):
    """Celery entrypoint for TTS generation."""
    return perform_tts_core(payload)

# --------------------------------------------------------------------------
# Proxy to allow both async and inline execution
# --------------------------------------------------------------------------
class RunTTSProxy:
    def __init__(self, celery_task):
        self._task = celery_task

    def __call__(self, payload=None, inline=False):
        if inline:
            return perform_tts_core(payload)
        return self._task.apply_async(kwargs={"payload": payload})

    def delay(self, payload=None):
        return self._task.apply_async(kwargs={"payload": payload})

    def apply_async(self, *args, **kwargs):
        return self._task.apply_async(*args, **kwargs)

run_tts = RunTTSProxy(_run_tts_task)

# --------------------------------------------------------------------------
# Inference Task (for GPT + TTS pipeline)
# --------------------------------------------------------------------------
@celery.task(name="tasks.run_inference", bind=True)
def run_inference(self, payload):
    payload = normalize_payload(payload)
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    transcript = extract_text(payload)

    log_event(
        service="tasks",
        event="inference_start",
        status="ok",
        message=f"Received transcript ({len(transcript)} chars)",
        trace_id=trace_id,
        session_id=session_id,
    )

    try:
        start_time = time.time()
        reply_text = generate_reply(transcript, trace_id=trace_id)
        latency_ms = round((time.time() - start_time) * 1000, 2)

        run_tts.delay({"text": reply_text, "trace_id": trace_id, "session_id": session_id})

        log_event(
            service="tasks",
            event="inference_done",
            status="ok",
            message="Inference complete, TTS enqueued",
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
