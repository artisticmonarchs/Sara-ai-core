"""
tasks.py — Phase 10I (R2 Validation & Observability Upgrade)

Key updates:
- PUBLIC_AUDIO_HOST treated as full base path (no bucket) by default
- Optional PUBLIC_AUDIO_INCLUDE_BUCKET env to include /<bucket>/ segment
- upload_to_r2() logs upload_start / upload_success / upload_failure
- Tracks upload duration, size_bytes, Redis metrics (uploads, bytes_uploaded)
- make_public_url() simplified with robust join handling
- Added R2UploadError subclass for clearer Celery retry filtering
"""

import os
import io
import time
import uuid
import json
import traceback
import logging
from typing import Any, Dict

import requests
import boto3
import botocore.exceptions
from redis import Redis

from celery_app import celery
from gpt_client import generate_reply
from logging_utils import log_event

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
LOG = logging.getLogger("tasks")
LOG.setLevel(os.getenv("TASKS_LOG_LEVEL", "INFO"))

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DG_API_KEY = os.getenv("DEEPGRAM_API_KEY")
DG_SPEAK_MODEL = os.getenv("DEEPGRAM_SPEAK_MODEL", "aura-2-asteria-en")
MAX_TTS_TEXT_LEN = int(os.getenv("MAX_TTS_TEXT_LEN", "2000"))

# R2 config
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_REGION = os.getenv("R2_REGION", "auto")

PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "").strip().rstrip("/")
PUBLIC_AUDIO_INCLUDE_BUCKET = os.getenv("PUBLIC_AUDIO_INCLUDE_BUCKET", "false").lower() in ("1", "true", "yes")
PUBLIC_URL_EXPIRES = int(os.getenv("PUBLIC_URL_EXPIRES", "3600"))

# Retry settings
CELERY_RETRY_MAX = int(os.getenv("CELERY_RETRY_MAX", "5"))
CELERY_RETRY_BACKOFF_MAX = int(os.getenv("CELERY_RETRY_BACKOFF_MAX", "600"))

# --------------------------------------------------------------------------
# Redis & boto3 clients
# --------------------------------------------------------------------------
try:
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
except Exception:
    redis_client = None
    LOG.exception("Failed to initialize Redis client at import time")

_s3_client = None
def _get_s3_client():
    """Lazily create a boto3 S3 client for Cloudflare R2."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    if not (R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_BUCKET_NAME):
        LOG.warning("R2 configuration incomplete; R2 uploads disabled.")
        return None

    try:
        endpoint = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        _s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name=R2_REGION,
        )
        return _s3_client
    except Exception:
        LOG.exception("Failed to create R2 boto3 client")
        return None

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
class R2UploadError(RuntimeError):
    """Raised when an R2 upload fails."""

def get_trace() -> str:
    return str(uuid.uuid4())

def _safe_redis_hincr(key: str, field: str, amount: int = 1):
    if not redis_client:
        return
    try:
        redis_client.hincrby(key, field, amount)
    except Exception:
        LOG.exception("Failed to increment redis metric %s.%s", key, field)

def normalize_payload(payload) -> Dict[str, Any]:
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

def extract_text(payload) -> str:
    if not payload:
        return ""
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        for key in ("sara_text", "text", "message", "input"):
            v = payload.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
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

def make_public_url(bucket: str, key: str) -> str:
    """
    Return public URL for uploaded R2 object.
    - If PUBLIC_AUDIO_HOST is set → use it as base (no bucket unless env flag true)
    - Else → generate presigned URL
    """
    if PUBLIC_AUDIO_HOST:
        if PUBLIC_AUDIO_INCLUDE_BUCKET:
            return f"{PUBLIC_AUDIO_HOST.rstrip('/')}/{bucket}/{key.lstrip('/')}"
        return f"{PUBLIC_AUDIO_HOST.rstrip('/')}/{key.lstrip('/')}"

    s3 = _get_s3_client()
    if s3:
        try:
            return s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=PUBLIC_URL_EXPIRES,
            )
        except Exception:
            LOG.exception("Failed to generate presigned URL for %s/%s", bucket, key)
    LOG.warning("PUBLIC_AUDIO_HOST unset and presign failed; returning key path only.")
    return f"/{key}"

# --------------------------------------------------------------------------
# Upload to R2
# --------------------------------------------------------------------------
def upload_to_r2(session_id: str, trace_id: str, audio_bytes: bytes) -> str:
    """Upload bytes to R2 and return a public URL."""
    s3 = _get_s3_client()
    key = f"{session_id}/{trace_id}.wav"
    size_bytes = len(audio_bytes or b"")
    start = time.time()

    log_event(
        service="tasks",
        event="upload_start",
        status="ok",
        message="Uploading audio to R2",
        trace_id=trace_id,
        session_id=session_id,
        extra={"key": key, "size_bytes": size_bytes},
    )

    if not s3:
        raise R2UploadError("R2 client unavailable")

    try:
        s3.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=audio_bytes,
            ContentType="audio/wav",
        )
        duration_ms = round((time.time() - start) * 1000, 2)
        url = make_public_url(R2_BUCKET_NAME, key)

        _safe_redis_hincr("metrics:tts", "uploads", 1)
        _safe_redis_hincr("metrics:tts", "bytes_uploaded", size_bytes)

        log_event(
            service="tasks",
            event="upload_success",
            status="ok",
            message="R2 upload complete",
            trace_id=trace_id,
            session_id=session_id,
            extra={"key": key, "url": url, "duration_ms": duration_ms, "size_bytes": size_bytes},
        )
        return url
    except botocore.exceptions.BotoCoreError as e:
        log_event(
            service="tasks",
            event="upload_failure",
            status="error",
            message="BotoCore error during R2 upload",
            trace_id=trace_id,
            session_id=session_id,
            extra={"error": str(e)},
        )
        raise R2UploadError("BotoCoreError during R2 upload") from e
    except Exception as e:
        log_event(
            service="tasks",
            event="upload_failure",
            status="error",
            message="Unexpected error during R2 upload",
            trace_id=trace_id,
            session_id=session_id,
            extra={"error": str(e)},
        )
        raise R2UploadError("Unexpected R2 upload error") from e

# --------------------------------------------------------------------------
# Deepgram TTS
# --------------------------------------------------------------------------
def deepgram_tts_rest(text: str, timeout: int = 30) -> bytes:
    if not DG_API_KEY:
        raise RuntimeError("Missing Deepgram API key")
    url = f"https://api.deepgram.com/v1/speak?model={DG_SPEAK_MODEL}&encoding=linear16&container=wav"
    headers = {"Authorization": f"Token {DG_API_KEY}", "Content-Type": "text/plain"}
    resp = requests.post(url, headers=headers, data=text.encode("utf-8"), timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"Deepgram returned {resp.status_code}: {resp.text[:200]}")
    return resp.content

# --------------------------------------------------------------------------
# Core TTS logic
# --------------------------------------------------------------------------
def perform_tts_core(payload: Any, raise_on_error: bool = True) -> Dict[str, Any]:
    payload = normalize_payload(payload)
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    text = extract_text(payload)

    if not text:
        return {"error": "No text provided", "trace_id": trace_id}

    if len(text) > MAX_TTS_TEXT_LEN:
        text = text[:MAX_TTS_TEXT_LEN]
        log_event(service="tasks", event="tts_text_truncated", status="warn",
                  message=f"TTS text truncated to {MAX_TTS_TEXT_LEN} chars",
                  trace_id=trace_id, session_id=session_id)

    log_event(service="tasks", event="tts_start", status="ok",
              message="TTS started", trace_id=trace_id, session_id=session_id)

    try:
        t0 = time.time()
        audio_bytes = deepgram_tts_rest(text)
        upload_url = upload_to_r2(session_id, trace_id, audio_bytes)
        total_ms = round((time.time() - t0) * 1000, 2)

        _safe_redis_hincr("metrics:tts", "files_generated", 1)

        log_event(
            service="tasks",
            event="tts_done",
            status="ok",
            message="TTS completed successfully",
            trace_id=trace_id,
            session_id=session_id,
            extra={"chars": len(text), "total_ms": total_ms, "audio_url": upload_url},
        )
        return {"trace_id": trace_id, "session_id": session_id, "audio_url": upload_url, "total_ms": total_ms}
    except Exception as e:
        log_event(
            service="tasks",
            event="tts_error",
            status="error",
            message=str(e),
            trace_id=trace_id,
            session_id=session_id,
        )
        if raise_on_error:
            raise
        return {"error": str(e), "trace_id": trace_id}

# --------------------------------------------------------------------------
# Celery Task & Proxy
# --------------------------------------------------------------------------
@celery.task(
    name="tasks.run_tts",
    bind=True,
    autoretry_for=(requests.RequestException, RuntimeError, botocore.exceptions.BotoCoreError, R2UploadError),
    retry_backoff=True,
    retry_backoff_max=CELERY_RETRY_BACKOFF_MAX,
    retry_kwargs={"max_retries": CELERY_RETRY_MAX},
)
def _run_tts_task(self, payload=None):
    return perform_tts_core(payload, raise_on_error=True)

class RunTTSProxy:
    def __init__(self, celery_task):
        self._task = celery_task
    def __call__(self, payload=None, inline: bool = False):
        if inline:
            return perform_tts_core(payload, raise_on_error=False)
        return self._task.apply_async(kwargs={"payload": payload})
    def delay(self, payload=None):
        return self._task.apply_async(kwargs={"payload": payload})
    def apply_async(self, *args, **kwargs):
        return self._task.apply_async(*args, **kwargs)

run_tts = RunTTSProxy(_run_tts_task)

# --------------------------------------------------------------------------
# GPT + TTS inference
# --------------------------------------------------------------------------
@celery.task(name="tasks.run_inference", bind=True)
def run_inference(self, payload):
    payload = normalize_payload(payload)
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    transcript = extract_text(payload)

    log_event(service="tasks", event="inference_start", status="ok",
              message=f"Received transcript ({len(transcript)} chars)",
              trace_id=trace_id, session_id=session_id)

    try:
        t0 = time.time()
        reply_text = generate_reply(transcript, trace_id=trace_id)
        latency_ms = round((time.time() - t0) * 1000, 2)
        run_tts.delay({"text": reply_text, "trace_id": trace_id, "session_id": session_id})
        log_event(service="tasks", event="inference_done", status="ok",
                  message="Inference complete, TTS enqueued",
                  trace_id=trace_id, session_id=session_id,
                  extra={"latency_ms": latency_ms})
        return {"trace_id": trace_id, "session_id": session_id, "reply": reply_text}
    except Exception:
        err_msg = traceback.format_exc()
        log_event(service="tasks", event="inference_error", status="error",
                  message=err_msg, trace_id=trace_id, session_id=session_id)
        raise
