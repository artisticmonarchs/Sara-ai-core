"""
tasks.py — Phase 10H (R2 Integrated) Production Version (Hardened)

Key changes:
- Lazy R2 (boto3) client factory with env validation
- Public URL fallback to presigned URL generation when PUBLIC_AUDIO_HOST unset
- Boto/botocore exceptions included in Celery retry policy
- upload_to_r2 raises on failure (so Celery retries can engage)
- perform_tts_core supports raise_on_error flag: inline callers can get dicts, Celery can raise
- Guarded Redis metric increments and improved structured logging via log_event
"""

import os
import io
import time
import uuid
import json
import traceback
import logging
from typing import Optional, Dict, Any

import requests
import boto3
import botocore.exceptions
from redis import Redis, RedisError

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
PUBLIC_AUDIO_BASE_URL = os.getenv("PUBLIC_AUDIO_BASE_URL", "/audio")
PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "").strip().rstrip("/")
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
PUBLIC_URL_EXPIRES = int(os.getenv("PUBLIC_URL_EXPIRES", "3600"))

# --------------------------------------------------------------------------
# Clients
# --------------------------------------------------------------------------
try:
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
except Exception:
    redis_client = None
    LOG.exception("Failed to initialize Redis client at import time")

# Lazy s3 client factory ----------------------------------------------------
_s3_client = None
def _get_s3_client():
    """
    Lazily create a boto3 S3 client for Cloudflare R2.
    Returns None if credentials/config are missing.
    """
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    if not (R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_BUCKET_NAME):
        LOG.warning("R2 configuration incomplete; R2 uploads disabled (R2_ACCOUNT_ID/ACCESS_KEY/SECRET/Bucket required).")
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
def get_trace() -> str:
    return str(uuid.uuid4())

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

def _safe_redis_hincr(key: str, field: str, amount: int = 1):
    if not redis_client:
        return
    try:
        redis_client.hincrby(key, field, amount)
    except Exception:
        LOG.exception("Failed to increment redis metric %s.%s", key, field)

def make_public_url(bucket: str, key: str) -> str:
    """
    Return public URL for uploaded R2 object.
    Priority:
     1) PUBLIC_AUDIO_HOST if configured (assumes public hosting)
     2) Generate presigned URL via R2 boto3 client if available
     3) Fallback to relative PUBLIC_AUDIO_BASE_URL path
    """
    if PUBLIC_AUDIO_HOST:
        return f"{PUBLIC_AUDIO_HOST}/{bucket}/{key}".replace("//", "/").replace(":/", "://")

    s3 = _get_s3_client()
    if s3:
        try:
            url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=PUBLIC_URL_EXPIRES,
            )
            return url
        except Exception:
            LOG.exception("Failed to generate presigned URL for %s/%s", bucket, key)

    LOG.warning("PUBLIC_AUDIO_HOST unset and presign failed; returning relative path for %s/%s", bucket, key)
    return f"{PUBLIC_AUDIO_BASE_URL}/{bucket}/{key}"

def upload_to_r2(session_id: str, trace_id: str, audio_bytes: bytes) -> str:
    """
    Upload bytes to R2 and return a public URL.
    Raises RuntimeError on failure (so Celery can retry).
    """
    s3 = _get_s3_client()
    key = f"{session_id}/{trace_id}.wav"
    if not s3:
        # If no R2 configured, raise so caller can handle fallback
        raise RuntimeError("R2 not configured (missing credentials or bucket)")

    try:
        s3.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=audio_bytes,
            ContentType="audio/wav",
        )
        return make_public_url(R2_BUCKET_NAME, key)
    except botocore.exceptions.BotoCoreError as e:
        LOG.exception("BotoCore/Boto3 error during R2 upload")
        raise RuntimeError("R2 upload failed") from e
    except Exception as e:
        LOG.exception("Unexpected error during R2 upload")
        raise RuntimeError("R2 upload failed") from e

def tts_error_response(code: str, message: str, trace_id: str, session_id: str) -> Dict[str, Any]:
    _safe_redis_hincr("metrics:tts", "failures", 1)
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
# Deepgram TTS (REST)
# --------------------------------------------------------------------------
def deepgram_tts_rest(text: str, timeout: int = 30) -> bytes:
    if not DG_API_KEY:
        raise RuntimeError("Missing Deepgram API key")
    url = f"https://api.deepgram.com/v1/speak?model={DG_SPEAK_MODEL}&encoding=linear16&container=wav"
    headers = {"Authorization": f"Token {DG_API_KEY}", "Content-Type": "text/plain"}
    try:
        resp = requests.post(url, headers=headers, data=text.encode("utf-8"), timeout=timeout)
    except requests.Timeout as e:
        LOG.exception("Deepgram request timeout")
        raise requests.RequestException("Deepgram TTS timeout") from e
    except requests.RequestException as e:
        LOG.exception("Deepgram request failed")
        raise

    if resp.status_code != 200:
        LOG.error("Deepgram returned non-200: %s %s", resp.status_code, resp.text[:1000])
        raise RuntimeError(f"Deepgram returned {resp.status_code}")
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

    log_event(service="tasks", event="tts_start", status="ok", message="TTS started", trace_id=trace_id, session_id=session_id)

    try:
        start_time = time.time()
        audio_bytes = deepgram_tts_rest(text)
        upload_url = upload_to_r2(session_id, trace_id, audio_bytes)
        duration = round(time.time() - start_time, 2)

        _safe_redis_hincr("metrics:tts", "files_generated", 1)

        log_event(
            service="tasks",
            event="tts_done",
            status="ok",
            message=f"TTS completed ({len(text)} chars, {duration}s)",
            trace_id=trace_id,
            session_id=session_id,
            extra={"audio_url": upload_url},
        )

        return {"trace_id": trace_id, "session_id": session_id, "audio_url": upload_url}

    except requests.RequestException as e:
        LOG.exception("Deepgram transient error")
        # treat as transient - raise for Celery retry
        if raise_on_error:
            raise
        return tts_error_response("DEEPGRAM_ERROR", str(e), trace_id, session_id)
    except RuntimeError as e:
        LOG.exception("Runtime error during TTS")
        if raise_on_error:
            raise
        return tts_error_response("TTS_RUNTIME", str(e), trace_id, session_id)
    except botocore.exceptions.BotoCoreError as e:
        LOG.exception("R2/Boto3 error during TTS")
        if raise_on_error:
            raise RuntimeError("R2 upload failed") from e
        return tts_error_response("R2_ERROR", str(e), trace_id, session_id)
    except Exception as e:
        LOG.exception("Unhandled exception in perform_tts_core")
        if raise_on_error:
            raise
        return tts_error_response("UNKNOWN", str(e), trace_id, session_id)

# --------------------------------------------------------------------------
# Celery Task (async) - retries on network and R2 errors
# --------------------------------------------------------------------------
@celery.task(
    name="tasks.run_tts",
    bind=True,
    autoretry_for=(requests.RequestException, RuntimeError, botocore.exceptions.BotoCoreError),
    retry_backoff=True,
    retry_backoff_max=CELERY_RETRY_BACKOFF_MAX,
    retry_kwargs={"max_retries": CELERY_RETRY_MAX},
)
def _run_tts_task(self, payload=None):
    # Celery task should raise on transient errors so retry policy kicks in.
    return perform_tts_core(payload, raise_on_error=True)

# --------------------------------------------------------------------------
# Proxy wrapper - inline or async usage
# --------------------------------------------------------------------------
class RunTTSProxy:
    def __init__(self, celery_task):
        self._task = celery_task

    def __call__(self, payload=None, inline: bool = False):
        if inline:
            # Inline callers expect a dict (not raise) so use raise_on_error=False
            return perform_tts_core(payload, raise_on_error=False)
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

    log_event(service="tasks", event="inference_start", status="ok",
              message=f"Received transcript ({len(transcript)} chars)", trace_id=trace_id, session_id=session_id)

    try:
        start_time = time.time()
        reply_text = generate_reply(transcript, trace_id=trace_id)
        latency_ms = round((time.time() - start_time) * 1000, 2)

        # Enqueue TTS and do not wait
        run_tts.delay({"text": reply_text, "trace_id": trace_id, "session_id": session_id})

        log_event(service="tasks", event="inference_done", status="ok",
                  message="Inference complete, TTS enqueued", trace_id=trace_id, session_id=session_id,
                  extra={"latency_ms": latency_ms})

        return {"trace_id": trace_id, "session_id": session_id, "reply": reply_text}

    except Exception:
        err_msg = traceback.format_exc()
        log_event(service="tasks", event="inference_error", status="error", message=err_msg,
                  trace_id=trace_id, session_id=session_id)
        # Re-raise so Celery marks the task failed (or configure retries if desired)
        raise
