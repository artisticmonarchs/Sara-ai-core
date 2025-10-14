"""
sara_ai/tasks.py — Phase 10H (Cloudflare R2 Integration)
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
from botocore.client import Config

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

# Cloudflare R2
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_REGION = os.getenv("R2_REGION", "auto")

PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "")  # e.g., https://pub-xxxx.r2.dev
MAX_TTS_TEXT_LEN = int(os.getenv("MAX_TTS_TEXT_LEN", "2000"))

CELERY_RETRY_MAX = int(os.getenv("CELERY_RETRY_MAX", "5"))
CELERY_RETRY_BACKOFF_MAX = int(os.getenv("CELERY_RETRY_BACKOFF_MAX", "600"))

redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Boto3 R2 client
# --------------------------------------------------------------------------
def get_r2_client():
    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME]):
        raise RuntimeError("Missing R2 configuration in environment variables")
    endpoint_url = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name=R2_REGION,
        config=Config(signature_version="s3v4"),
    )

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def get_trace():
    return str(uuid.uuid4())


def normalize_payload(payload):
    if isinstance(payload, (list, tuple)) and payload:
        payload = payload[0]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {"text": str(payload)}
    if not isinstance(payload, dict):
        log_event(service="tasks", event="payload_warning", status="warn", message="Non-dict payload received", trace_id=get_trace())
        payload = {}
    return payload


def extract_text(payload):
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
    """Constructs public R2 URL (PUBLIC_AUDIO_HOST/session_id/trace_id.wav)."""
    return f"{PUBLIC_AUDIO_HOST.rstrip('/')}/{session_id}/{trace_id}.wav"


def upload_to_r2(session_id: str, trace_id: str, audio_bytes: bytes) -> str:
    """Uploads audio bytes to Cloudflare R2 and returns public URL."""
    key = f"{session_id}/{trace_id}.wav"
    r2 = get_r2_client()
    try:
        r2.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=io.BytesIO(audio_bytes),
            ContentType="audio/wav",
            ACL="public-read",
        )
        log_event(
            service="tasks",
            event="r2_upload_success",
            status="ok",
            message=f"Uploaded {key} to R2 bucket {R2_BUCKET_NAME}",
            trace_id=trace_id,
            session_id=session_id,
        )
        return make_public_url(session_id, trace_id)
    except Exception as e:
        logger.exception("R2 upload failed")
        log_event(
            service="tasks",
            event="r2_upload_error",
            status="error",
            message=str(e),
            trace_id=trace_id,
            session_id=session_id,
        )
        raise RuntimeError(f"R2 upload failed: {e}")

# --------------------------------------------------------------------------
# Deepgram TTS
# --------------------------------------------------------------------------
def deepgram_tts_rest(text: str) -> bytes:
    if not DG_API_KEY:
        raise RuntimeError("Missing Deepgram API key")
    url = f"https://api.deepgram.com/v1/speak?model={DG_SPEAK_MODEL}&encoding=linear16&container=wav"
    headers = {"Authorization": f"Token {DG_API_KEY}", "Content-Type": "text/plain"}
    resp = requests.post(url, headers=headers, data=text.encode("utf-8"), timeout=15)
    if resp.status_code != 200:
        raise RuntimeError(f"Deepgram TTS failed: {resp.status_code} - {resp.text}")
    return resp.content

# --------------------------------------------------------------------------
# Core TTS logic
# --------------------------------------------------------------------------
def perform_tts_core(payload):
    payload = normalize_payload(payload)
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    text = extract_text(payload)

    if not text:
        return {"error_code": "NO_TEXT", "error_message": "No text provided", "trace_id": trace_id, "session_id": session_id}

    if len(text) > MAX_TTS_TEXT_LEN:
        text = text[:MAX_TTS_TEXT_LEN]
        log_event(service="tasks", event="tts_text_truncated", status="warn", message="Text truncated", trace_id=trace_id, session_id=session_id)

    try:
        log_event(service="tasks", event="tts_start", status="ok", message=f"TTS start ({len(text)} chars)", trace_id=trace_id, session_id=session_id)
        start = time.time()
        audio_bytes = deepgram_tts_rest(text)
        public_url = upload_to_r2(session_id, trace_id, audio_bytes)
        duration = round(time.time() - start, 2)

        redis_client.hincrby("metrics:tts", "files_generated", 1)
        log_event(
            service="tasks",
            event="tts_done",
            status="ok",
            message=f"TTS done ({duration}s)",
            trace_id=trace_id,
            session_id=session_id,
            extra={"public_url": public_url},
        )
        return {"trace_id": trace_id, "session_id": session_id, "audio_url": public_url}

    except Exception as e:
        logger.exception("TTS failed")
        redis_client.hincrby("metrics:tts", "failures", 1)
        log_event(service="tasks", event="tts_error", status="error", message=str(e), trace_id=trace_id, session_id=session_id)
        return {"error_code": "TTS_FAILURE", "error_message": str(e), "trace_id": trace_id, "session_id": session_id}

# --------------------------------------------------------------------------
# Celery tasks
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
    return perform_tts_core(payload)


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
# Inference task (unchanged)
# --------------------------------------------------------------------------
@celery.task(name="sara_ai.tasks.run_inference", bind=True)
def run_inference(self, payload):
    payload = normalize_payload(payload)
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    transcript = extract_text(payload)

    log_event(service="tasks", event="inference_start", status="ok", message=f"Received transcript ({len(transcript)} chars)", trace_id=trace_id, session_id=session_id)

    start = time.time()
    try:
        reply_text = generate_reply(transcript, trace_id=trace_id)
        celery.send_task("sara_ai.tasks.run_tts", kwargs={"payload": {"text": reply_text, "trace_id": trace_id, "session_id": session_id}})
        latency_ms = round((time.time() - start) * 1000, 2)
        log_event(service="tasks", event="inference_done", status="ok", message=f"Inference completed ({latency_ms}ms)", trace_id=trace_id, session_id=session_id)
        return {"trace_id": trace_id, "session_id": session_id, "reply": reply_text}
    except Exception:
        err = traceback.format_exc()
        log_event(service="tasks", event="inference_error", status="error", message=err, trace_id=trace_id, session_id=session_id)
        raise
