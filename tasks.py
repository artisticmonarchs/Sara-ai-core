"""
tasks.py — Phase 11-D (Unified Metrics Registry + Redis-persistent metrics + Global Sync)

- Uses metrics_collector for all metric operations (shared REGISTRY).
- Restores persisted metrics from Redis on startup via metrics_registry.
- Starts background metrics sync (global_metrics_store.start_background_sync).
- Redis is used for TTS caching only (metrics are persisted via metrics_collector).
- Preserves all Deepgram TTS, R2 upload, and Celery behavior.
- Structured logging via logging_utils.log_event.
"""

from __future__ import annotations
import time
import uuid
import json
import traceback
import hashlib
from typing import Any, Dict, Optional, Tuple

import requests
import boto3
import botocore.exceptions

from celery_app import celery, safe_task_wrapper  # ← ADD safe_task_wrapper import
from gpt_client import generate_reply
from logging_utils import log_event

# --------------------------------------------------------------------------
# Phase 11-D Configuration Isolation
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    # Fallback config for backward compatibility
    import os
    class Config:
        REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        DG_API_KEY = os.getenv("DEEPGRAM_API_KEY")
        DG_SPEAK_MODEL = os.getenv("DEEPGRAM_SPEAK_MODEL", "aura-2-asteria-en")
        MAX_TTS_TEXT_LEN = int(os.getenv("MAX_TTS_TEXT_LEN", "2000"))
        R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
        R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
        R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
        R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
        R2_REGION = os.getenv("R2_REGION", "auto")
        PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "").strip().rstrip("/")
        PUBLIC_AUDIO_INCLUDE_BUCKET = os.getenv("PUBLIC_AUDIO_INCLUDE_BUCKET", "false").lower() in ("1", "true", "yes")
        PUBLIC_URL_EXPIRES = int(os.getenv("PUBLIC_URL_EXPIRES", "3600"))
        CELERY_RETRY_MAX = int(os.getenv("CELERY_RETRY_MAX", "5"))
        CELERY_RETRY_BACKOFF_MAX = int(os.getenv("CELERY_RETRY_BACKOFF_MAX", "600"))

# --------------------------------------------------------------------------
# Phase 11-D Metrics Integration - Lazy Import Shim
# --------------------------------------------------------------------------
def _get_metrics():
    """Lazy metrics shim to avoid circular imports at import-time"""
    try:
        from metrics_collector import increment_metric, observe_latency
        return increment_metric, observe_latency
    except Exception:
        # safe no-op fallbacks
        def _noop_inc(*a, **k): pass
        def _noop_obs(*a, **k): pass
        return _noop_inc, _noop_obs

# Lazy metrics functions - will be initialized on first use
_inc_metric = None
_obs_latency = None

def get_metrics():
    """Get or initialize lazy metrics functions"""
    global _inc_metric, _obs_latency
    if _inc_metric is None or _obs_latency is None:
        _inc_metric, _obs_latency = _get_metrics()
    return _inc_metric, _obs_latency

# --------------------------------------------------------------------------
# Centralized Redis client (simplified) - Lazy Load
# --------------------------------------------------------------------------
def _redis() -> Optional[object]:
    try:
        from redis_client import get_client
        return get_client()
    except Exception:
        return None

# --------------------------------------------------------------------------
# Lazy Initialization Functions (Phase 11-D - No side effects at import time)
# --------------------------------------------------------------------------
def _initialize_metrics_restore():
    """Lazy metrics restore - called on first task execution"""
    try:
        from metrics_registry import restore_snapshot_to_collector  # type: ignore
        try:
            inc_metric, obs_latency = get_metrics()
            restored_ok = restore_snapshot_to_collector(inc_metric, obs_latency)
            if restored_ok:
                log_event(service="tasks", event="metrics_restored_on_startup", status="info",
                          message="Restored persisted metrics into metrics_collector at worker startup")
        except Exception as e:
            log_event(service="tasks", event="metrics_restore_failed", status="warn",
                      message="Failed to restore metrics snapshot at worker startup",
                      extra={"error": str(e), "stack": traceback.format_exc()})
    except Exception:
        pass

def _initialize_metrics_sync():
    """Lazy metrics sync startup - called on first task execution"""
    try:
        from global_metrics_store import start_background_sync  # type: ignore
        start_background_sync(service_name="tasks")
        log_event(service="tasks", event="global_metrics_sync_started", status="ok",
                  message="Background global metrics sync started for tasks service")
    except Exception as e:
        log_event(service="tasks", event="global_metrics_sync_failed", status="error",
                  message="Failed to start background metrics sync",
                  extra={"error": str(e), "stack": traceback.format_exc()})

def _ensure_initialized():
    """Ensure metrics are initialized on first use"""
    # These will only run once due to module-level caching
    _initialize_metrics_restore()
    _initialize_metrics_sync()

# --------------------------------------------------------------------------
# R2 / S3 client setup (using centralized client)
# --------------------------------------------------------------------------
_s3_client = None

def _get_s3_client():
    """Lazily create a boto3 S3 client using centralized R2 client."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    try:
        from r2_client import get_r2_client
        _s3_client = get_r2_client()
        if _s3_client:
            log_event(service="tasks", event="r2_client_retrieved", status="ok",
                      message="Got centralized R2 client via r2_client.py")
        else:
            log_event(service="tasks", event="r2_client_unavailable", status="warn",
                      message="Central R2 client not available")
        return _s3_client
    except Exception as e:
        log_event(service="tasks", event="r2_client_failed", status="error",
                  message="Failed to get R2 client from r2_client.py",
                  extra={"error": str(e), "stack": traceback.format_exc()})
        return None

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
class R2UploadError(RuntimeError):
    pass

def get_trace() -> str:
    return str(uuid.uuid4())

# --------------------------------------------------------------------------
# Redis-based TTS Cache
# --------------------------------------------------------------------------
def _tts_cache_key(session_id: str, text: str) -> str:
    text_hash = hashlib.sha1((text or "").encode("utf-8")).hexdigest()
    return f"tts_cache:{session_id}:{text_hash}"

def get_tts_cache(session_id: str, text: str) -> Optional[str]:
    client = _redis()
    if not client:
        return None
    key = _tts_cache_key(session_id, text)
    try:
        val = client.get(key)
        if isinstance(val, bytes):
            return val.decode("utf-8")
        return val
    except Exception as e:
        log_event(service="tasks", event="tts_cache_get_failed", status="error",
                  message="Failed to get TTS cache",
                  extra={"key": key, "error": str(e), "stack": traceback.format_exc()})
        return None

def set_tts_cache(session_id: str, text: str, audio_url: str, ttl: int = 300):
    client = _redis()
    if not client:
        log_event(service="tasks", event="tts_cache_set_skipped", status="warn",
                  message="Redis not available for TTS cache")
        return
    key = _tts_cache_key(session_id, text)
    try:
        client.setex(key, ttl, audio_url)
    except Exception as e:
        log_event(service="tasks", event="tts_cache_set_failed", status="error",
                  message="Failed to set TTS cache",
                  extra={"key": key, "error": str(e), "stack": traceback.format_exc()})

# --------------------------------------------------------------------------
# Payload utilities
# --------------------------------------------------------------------------
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
    if Config.PUBLIC_AUDIO_HOST:
        if Config.PUBLIC_AUDIO_INCLUDE_BUCKET:
            return f"{Config.PUBLIC_AUDIO_HOST.rstrip('/')}/{bucket}/{key.lstrip('/')}"
        return f"{Config.PUBLIC_AUDIO_HOST.rstrip('/')}/{key.lstrip('/')}"
    s3 = _get_s3_client()
    if s3:
        try:
            return s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=Config.PUBLIC_URL_EXPIRES,
            )
        except Exception as e:
            log_event(service="tasks", event="presign_failed", status="error",
                      message="Failed to presign URL",
                      extra={"bucket": bucket, "key": key, "error": str(e)})
    log_event(service="tasks", event="presign_unavailable", status="warn",
              message="No PUBLIC_AUDIO_HOST; presign failed")
    return f"/{key}"

# --------------------------------------------------------------------------
# Upload to R2
# --------------------------------------------------------------------------
def upload_to_r2(session_id: str, trace_id: str, audio_bytes: bytes) -> Tuple[str, float]:
    s3 = _get_s3_client()
    key = f"{session_id}/{trace_id}.wav"
    size_bytes = len(audio_bytes or b"")
    start = time.time()

    log_event(service="tasks", event="upload_start", status="ok",
              message="Uploading to R2",
              trace_id=trace_id, session_id=session_id,
              extra={"key": key, "size_bytes": size_bytes})

    if not s3:
        inc_metric, obs_latency = get_metrics()
        inc_metric("tts_failures_total")
        log_event(service="tasks", event="upload_unavailable", status="error",
                  message="R2 client unavailable",
                  trace_id=trace_id, session_id=session_id)
        raise R2UploadError("R2 client unavailable")

    try:
        s3.put_object(
            Bucket=Config.R2_BUCKET_NAME,
            Key=key,
            Body=audio_bytes,
            ContentType="audio/wav",
        )
        duration_ms = round((time.time() - start) * 1000, 2)
        url = make_public_url(Config.R2_BUCKET_NAME, key)
        inc_metric, obs_latency = get_metrics()
        inc_metric("tts_uploads_total")
        obs_latency("r2_upload_ms", duration_ms)

        log_event(service="tasks", event="upload_success", status="ok",
                  message="Upload complete",
                  trace_id=trace_id, session_id=session_id,
                  extra={"url": url, "r2_upload_ms": duration_ms, "size_bytes": size_bytes})
        return url, duration_ms
    except Exception as e:
        inc_metric, obs_latency = get_metrics()
        inc_metric("tts_failures_total")
        log_event(service="tasks", event="upload_failure", status="error",
                  message="R2 upload failed",
                  trace_id=trace_id, session_id=session_id,
                  extra={"error": str(e), "stack": traceback.format_exc()})
        raise R2UploadError(str(e)) from e

# --------------------------------------------------------------------------
# Deepgram TTS
# --------------------------------------------------------------------------
def deepgram_tts_rest(text: str, timeout: int = 30) -> Tuple[bytes, float]:
    if not Config.DG_API_KEY:
        log_event(service="tasks", event="deepgram_missing_key", status="error",
                  message="Missing Deepgram API key")
        raise RuntimeError("Missing Deepgram API key")

    url = f"https://api.deepgram.com/v1/speak?model={Config.DG_SPEAK_MODEL}&encoding=linear16&container=wav"
    headers = {"Authorization": f"Token {Config.DG_API_KEY}", "Content-Type": "text/plain"}

    start = time.time()
    for attempt in range(2):
        try:
            resp = requests.post(url, headers=headers, data=text.encode("utf-8"), timeout=timeout)
            if resp.status_code == 200:
                latency_ms = round((time.time() - start) * 1000, 2)
                if attempt > 0:
                    log_event(service="tasks", event="deepgram_retry_success", status="ok",
                              message="Deepgram succeeded on retry",
                              extra={"attempt": attempt + 1, "deepgram_latency_ms": latency_ms})
                return resp.content, latency_ms
            time.sleep(0.5)
        except requests.RequestException as e:
            if attempt == 1:
                inc_metric, obs_latency = get_metrics()
                inc_metric("tts_failures_total")
                log_event(service="tasks", event="deepgram_failed", status="error",
                          message="Deepgram failed after retries",
                          extra={"error": str(e), "stack": traceback.format_exc()})
                raise RuntimeError(f"Deepgram failed after retries: {e}") from e
            time.sleep(0.5)

    inc_metric, obs_latency = get_metrics()
    inc_metric("tts_failures_total")
    raise RuntimeError("Deepgram TTS failed with no successful response")

# --------------------------------------------------------------------------
# Core TTS logic
# --------------------------------------------------------------------------
def perform_tts_core(payload: Any, raise_on_error: bool = True) -> Dict[str, Any]:
    # Ensure metrics are initialized on first use
    _ensure_initialized()
    
    payload = normalize_payload(payload)
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    text = extract_text(payload)

    inc_metric, obs_latency = get_metrics()
    inc_metric("tts_requests_total")

    if not text:
        log_event(service="tasks", event="tts_no_text", status="error",
                  message="No text provided", trace_id=trace_id, session_id=session_id)
        inc_metric("tts_failures_total")
        return {"error": "No text provided", "trace_id": trace_id}

    if len(text) > Config.MAX_TTS_TEXT_LEN:
        log_event(service="tasks", event="tts_text_truncated", status="warn",
                  message=f"TTS text truncated to {Config.MAX_TTS_TEXT_LEN} chars",
                  trace_id=trace_id, session_id=session_id,
                  extra={"original_length": len(text)})
        text = text[:Config.MAX_TTS_TEXT_LEN]

    cached_url = get_tts_cache(session_id, text)
    if cached_url:
        inc_metric("tts_cache_hits_total")
        log_event(service="tasks", event="cache_hit", status="ok",
                  message="Cache hit for TTS",
                  trace_id=trace_id, session_id=session_id)
        return {"audio_url": cached_url, "cached": True,
                "trace_id": trace_id, "session_id": session_id}

    tts_start = time.time()
    log_event(service="tasks", event="tts_start", status="ok",
              message="TTS start", trace_id=trace_id, session_id=session_id)

    try:
        audio_bytes, deepgram_latency_ms = deepgram_tts_rest(text)
        obs_latency("deepgram_latency_ms", deepgram_latency_ms)

        upload_url, r2_upload_ms = upload_to_r2(session_id, trace_id, audio_bytes)
        obs_latency("r2_upload_ms", r2_upload_ms)

        total_ms = round((time.time() - tts_start) * 1000, 2)
        obs_latency("tts_latency_ms", total_ms)
        inc_metric("tts_requests_completed_total")

        try:
            set_tts_cache(session_id, text, upload_url)
        except Exception as e:
            log_event(service="tasks", event="cache_store_error", status="error",
                      message="Cache store failed",
                      trace_id=trace_id, session_id=session_id,
                      extra={"error": str(e)})

        log_event(service="tasks", event="tts_done", status="ok",
                  message="TTS completed",
                  trace_id=trace_id, session_id=session_id,
                  extra={"tts_latency_ms": total_ms, "audio_url": upload_url})
        return {"trace_id": trace_id, "session_id": session_id,
                "audio_url": upload_url, "total_ms": total_ms}

    except Exception as e:
        inc_metric("tts_failures_total")
        log_event(service="tasks", event="tts_error", status="error",
                  message=str(e),
                  trace_id=trace_id, session_id=session_id,
                  extra={"stack": traceback.format_exc()})
        if raise_on_error:
            raise
        return {"error": str(e), "trace_id": trace_id}

# --------------------------------------------------------------------------
# Celery Task & Proxy
# --------------------------------------------------------------------------
@celery.task(
    name="tasks.run_tts",
    bind=True,
    autoretry_for=(requests.RequestException, RuntimeError,
                   botocore.exceptions.BotoCoreError, R2UploadError),
    retry_backoff=True,
    retry_backoff_max=Config.CELERY_RETRY_BACKOFF_MAX,
    retry_kwargs={"max_retries": Config.CELERY_RETRY_MAX},
)
@safe_task_wrapper  # ← ADD safe_task_wrapper decorator
def _run_tts_task(self, payload=None):
    # Ensure metrics are initialized on first use
    _ensure_initialized()
    
    # ADD circuit breaker awareness and task-specific metrics
    inc_metric, obs_latency = get_metrics()
    inc_metric("task_run_tts_started_total")
    
    # Check Redis availability before proceeding
    redis_client = _redis()
    if not redis_client:
        log_event(service="tasks", event="redis_unavailable_at_task_start", status="warn",
                  message="Redis unavailable at task start - proceeding without cache")
    
    result = perform_tts_core(payload, raise_on_error=True)
    inc_metric("task_run_tts_completed_total")
    return result

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
# GPT + TTS Inference
# --------------------------------------------------------------------------
@celery.task(name="tasks.run_inference", bind=True)
@safe_task_wrapper  # ← ADD safe_task_wrapper decorator
def run_inference(self, payload):
    # Ensure metrics are initialized on first use
    _ensure_initialized()
    
    # ADD task-specific metrics and circuit breaker awareness
    inc_metric, obs_latency = get_metrics()
    inc_metric("task_run_inference_started_total")
    
    payload = normalize_payload(payload)
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    transcript = extract_text(payload)

    log_event(service="tasks", event="inference_start", status="ok",
              message="Inference received",
              trace_id=trace_id, session_id=session_id)

    try:
        t0 = time.time()
        reply_text = generate_reply(transcript, trace_id=trace_id)
        latency_ms = round((time.time() - t0) * 1000, 2)
        obs_latency("inference_latency_ms", latency_ms)

        run_tts.delay({"text": reply_text, "trace_id": trace_id, "session_id": session_id})
        log_event(service="tasks", event="inference_done", status="ok",
                  message="Inference complete, TTS enqueued",
                  trace_id=trace_id, session_id=session_id,
                  extra={"latency_ms": latency_ms})
        
        inc_metric("task_run_inference_completed_total")
        return {"reply": reply_text, "latency_ms": latency_ms, "trace_id": trace_id}
    except Exception as e:
        inc_metric("inference_failures_total")
        inc_metric("task_run_inference_failed_total")  # ← ADD task-specific failure metric
        log_event(service="tasks", event="inference_error", status="error",
                  message="Inference failed",
                  trace_id=trace_id, session_id=session_id,
                  extra={"error": str(e), "stack": traceback.format_exc()})
        return {"error": str(e), "trace_id": trace_id}