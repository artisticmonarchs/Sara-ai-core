"""
tasks.py — Phase 12 (Celery Task Resilience)
Sara AI Core Celery tasks with full fault tolerance, idempotency, and unified resilience patterns.
"""

from __future__ import annotations
import time
import uuid
import json
import traceback
import hashlib
import os
from typing import Any, Dict, Optional, Tuple
from functools import wraps

import requests
# Removed unused imports: boto3, botocore.exceptions

# Phase 12: Use shared_task with proper resilience decorators
from celery import shared_task

from gpt_client import generate_reply_sync
from logging_utils import log_event

# --------------------------------------------------------------------------
# Phase 12: Configuration for Celery resilience
# --------------------------------------------------------------------------
try:
    from config import config
except ImportError:
    # Fallback config for backward compatibility
    import os
    class config:
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
        CELERY_MAX_RETRIES = int(os.getenv("CELERY_MAX_RETRIES", "3"))
        CELERY_RETRY_BACKOFF_MAX = int(os.getenv("CELERY_RETRY_BACKOFF_MAX", "600"))
        CELERY_SOFT_TIME_LIMIT = int(os.getenv("CELERY_SOFT_TIME_LIMIT", "300"))

# Phase 12: Safe configuration access with fallbacks
CELERY_TASK_MAX_RETRIES = getattr(config, "CELERY_TASK_MAX_RETRIES", getattr(config, "CELERY_MAX_RETRIES", 3))
CELERY_RETRY_BACKOFF_MAX = getattr(config, "CELERY_RETRY_BACKOFF_MAX", 600)
CELERY_SOFT_TIME_LIMIT = getattr(config, "CELERY_SOFT_TIME_LIMIT", 300)

# Bucket name compatibility (env may use R2_BUCKET or R2_BUCKET_NAME)
R2_BUCKET_NAME = getattr(config, "R2_BUCKET_NAME", None) or getattr(config, "R2_BUCKET", None)

# Phase 12: Transient error detection
class TransientError(Exception):
    """Base class for transient errors that should trigger retries"""
    pass

class ConfigurationError(Exception):
    """Non-transient configuration errors that should not be retried"""
    pass

# --------------------------------------------------------------------------
# Phase 12: Metrics Integration - Lazy Import Shim
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
        from redis_client import get_redis_client
        return get_redis_client()
    except Exception:
        return None

# --- Task lock helpers ---
_TASK_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""

def _task_lock_keys(task_id: str) -> tuple[str, str]:
    # lock key and "done" key
    return f"task:{task_id}:lock", f"task:{task_id}:done"

def _new_task_token() -> str:
    return f"{os.getpid()}-{int(time.time()*1000)}"

def _acquire_task_lock(rc, task_id: str, ttl: int = 600) -> Optional[str]:
    if not rc:
        return _new_task_token()  # best-effort if Redis down
    lock_key, _ = _task_lock_keys(task_id)
    token = _new_task_token()
    # Use safe wrapper to avoid hard failures and emit metrics
    from redis_client import safe_redis_operation
    ok = safe_redis_operation(
        op=lambda client: client.set(lock_key, token, nx=True, ex=ttl),
        fallback=False,
        operation_name="task_acquire_lock",
    )
    return token if ok else None

def _release_task_lock(rc, task_id: str, token: Optional[str]) -> bool:
    if not rc or not token:
        return False
    lock_key, _ = _task_lock_keys(task_id)
    from redis_client import safe_redis_operation
    res = safe_redis_operation(
        op=lambda client: client.eval(_TASK_RELEASE_LUA, 1, lock_key, token),
        fallback=0,
        operation_name="task_release_lock",
    )
    return bool(res)

def _is_task_done(rc, task_id: str) -> bool:
    if not rc:
        return False
    from redis_client import safe_redis_operation
    _, done_key = _task_lock_keys(task_id)
    exists = safe_redis_operation(
        op=lambda client: client.exists(done_key),
        fallback=0,
        operation_name="task_done_exists",
    )
    return bool(exists)

def _mark_task_done(rc, task_id: str, ttl: int = 3600):
    if not rc:
        return
    from redis_client import safe_redis_operation
    _, done_key = _task_lock_keys(task_id)
    safe_redis_operation(
        op=lambda client: client.setex(done_key, ttl, "1"),
        fallback=None,
        operation_name="task_mark_done",
    )

def _mark_task_failed(rc, task_id: str, ttl: int = 600):
    """Mark task as failed to prevent immediate re-enqueue storms"""
    if not rc:
        return
    from redis_client import safe_redis_operation
    _, done_key = _task_lock_keys(task_id)
    safe_redis_operation(
        op=lambda client: client.setex(done_key, ttl, "failed"),
        fallback=None,
        operation_name="task_mark_failed",
    )

# --------------------------------------------------------------------------
# Phase 12: @resilient_task Decorator
# --------------------------------------------------------------------------
def resilient_task(func=None, **decorator_kwargs):
    """
    Decorator for Celery tasks with full resilience patterns:
    - Structured logging on start/success/error
    - Metrics collection (counters, latency)
    - Idempotency enforcement via Redis SETNX locks
    - Automatic retry with backoff and jitter
    """
    def decorator(task_func):
        # compute a stable name once
        task_name_full = f"{task_func.__module__}.{task_func.__name__}"
        
        # Use the safe configuration variable
        max_retries = CELERY_TASK_MAX_RETRIES

        # Remove @wraps(task_func) to avoid AttributeError with Celery proxy
        @shared_task(
            bind=True,
            acks_late=True,
            autoretry_for=(Exception,),
            retry_backoff=True,
            retry_backoff_max=CELERY_RETRY_BACKOFF_MAX,
            retry_jitter=True,
            max_retries=max_retries,
            soft_time_limit=CELERY_SOFT_TIME_LIMIT,
            name=task_name_full,                               # added
            **decorator_kwargs
        )
        def wrapper(self, *args, **kwargs):
            # Ensure metrics are initialized
            _ensure_initialized()
            inc_metric, obs_latency = get_metrics()
            
            task_name = task_func.__name__
            task_id = self.request.id
            trace_id = kwargs.get('trace_id') or str(uuid.uuid4())
            
            # Structured log on start
            log_event(
                service="tasks",
                event="task_started",
                status="info",
                message=f"Task {task_name} started",
                trace_id=trace_id,
                extra={
                    "task_id": task_id,
                    "task_name": task_name,
                    "args": args,
                    "kwargs": {k: v for k, v in kwargs.items() if k != 'trace_id'}
                }
            )
            inc_metric("task_started_total", labels={"task_name": task_name})
            
            start_time = time.time()
            
            rc = _redis()
            side_effecting = not (task_name.startswith('get_') or task_name.startswith('find_'))
            token = None

            if side_effecting:
                # Check if already completed
                if _is_task_done(rc, task_id):
                    log_event(
                        service="tasks",
                        event="task_already_completed",
                        status="info",
                        message="Task already completed successfully - skipping",
                        trace_id=trace_id,
                        extra={"task_id": task_id, "task_name": task_name}
                    )
                    inc_metric("task_skipped_total", labels={"task_name": task_name, "reason": "already_completed"})
                    return {"status": "skipped", "reason": "already_completed"}
                
                # Acquire execution lock
                token = _acquire_task_lock(rc, task_id, ttl=600)
                if not token:
                    log_event(
                        service="tasks",
                        event="task_in_progress",
                        status="info",
                        message="Task already in progress - skipping",
                        trace_id=trace_id,
                        extra={"task_id": task_id, "task_name": task_name}
                    )
                    inc_metric("task_skipped_total", labels={"task_name": task_name, "reason": "in_progress"})
                    return {"status": "skipped", "reason": "in_progress"}
            
            try:
                # Execute the task
                result = task_func(self, *args, **kwargs)
                latency_ms = round((time.time() - start_time) * 1000, 2)
                
                # Mark successful completion for idempotency
                if side_effecting:
                    _mark_task_done(rc, task_id)
                
                # Structured log on success
                log_event(
                    service="tasks",
                    event="task_completed",
                    status="success",
                    message=f"Task {task_name} completed successfully",
                    trace_id=trace_id,
                    extra={
                        "task_id": task_id,
                        "task_name": task_name,
                        "latency_ms": latency_ms
                    }
                )
                inc_metric("task_completed_total", labels={"task_name": task_name})
                obs_latency("task_latency_ms", latency_ms, labels={"task_name": task_name})
                
                return result
                
            except Exception as e:
                latency_ms = round((time.time() - start_time) * 1000, 2)
                
                # Release lock on failure (allow retries)
                if side_effecting:
                    _release_task_lock(rc, task_id, token)
                    token = None  # Prevent double release in finally block
                    # Mark as failed for non-transient errors after all retries
                    if not isinstance(e, TransientError) and self.request.retries >= self.max_retries:
                        _mark_task_failed(rc, task_id)
                
                # Structured log on error
                log_event(
                    service="tasks",
                    event="task_failed",
                    status="error",
                    message=f"Task {task_name} failed",
                    trace_id=trace_id,
                    extra={
                        "task_id": task_id,
                        "task_name": task_name,
                        "error": str(e),
                        "latency_ms": latency_ms,
                        "stack": traceback.format_exc()
                    }
                )
                error_type = "transient" if isinstance(e, TransientError) else "fatal"
                inc_metric("task_failed_total", labels={"task_name": task_name, "error_type": type(e).__name__, "type": error_type})
                obs_latency("task_latency_ms", latency_ms, labels={"task_name": task_name})
                
                # Re-raise for Celery's autoretry mechanism
                raise
            finally:
                # unified: only release when we actually acquired
                if token:
                    _release_task_lock(rc, task_id, token)
        
        return wrapper
    
    if func is None:
        return decorator
    return decorator(func)

# --------------------------------------------------------------------------
# Lazy Initialization Functions (Phase 12 - No side effects at import time)
# --------------------------------------------------------------------------
_INIT_DONE = False

def _ensure_initialized():
    """Ensure metrics are initialized on first use (idempotent)"""
    global _INIT_DONE
    if _INIT_DONE:
        return
    _initialize_metrics_restore()
    _initialize_metrics_sync()
    _INIT_DONE = True

def _initialize_metrics_restore():
    """Lazy metrics restore - called on first task execution"""
    try:
        from metrics_registry import restore_snapshot_to_collector  # type: ignore
        try:
            inc_metric, obs_latency = get_metrics()
            restored_ok = restore_snapshot_to_collector(inc_metric, obs_latency)
            if restored_ok:
                log_event(
                    service="tasks", 
                    event="metrics_restored_on_startup", 
                    status="info",
                    message="Restored persisted metrics into metrics_collector at worker startup"
                )
        except Exception as e:
            log_event(
                service="tasks", 
                event="metrics_restore_failed", 
                status="warn",
                message="Failed to restore metrics snapshot at worker startup",
                extra={"error": str(e), "stack": traceback.format_exc()}
            )
    except Exception:
        pass

def _initialize_metrics_sync():
    """Lazy metrics sync startup - called on first task execution"""
    try:
        from global_metrics_store import start_background_sync  # type: ignore
        start_background_sync(service_name="tasks")
        log_event(
            service="tasks",
            event="global_metrics_sync_started",
            status="ok",
            message="Background global metrics sync started for tasks service"
        )
    except Exception as e:
        log_event(
            service="tasks",
            event="global_metrics_sync_failed",
            status="error",
            message="Failed to start background metrics sync",
            extra={"error": str(e), "stack": traceback.format_exc()}
        )

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
            log_event(
                service="tasks", 
                event="r2_client_retrieved", 
                status="ok",
                message="Got centralized R2 client via r2_client.py"
            )
        else:
            log_event(
                service="tasks", 
                event="r2_client_unavailable", 
                status="warn",
                message="Central R2 client not available"
            )
        return _s3_client
    except Exception as e:
        log_event(
            service="tasks", 
            event="r2_client_failed", 
            status="error",
            message="Failed to get R2 client from r2_client.py",
            extra={"error": str(e), "stack": traceback.format_exc()}
        )
        return None

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
class R2UploadError(TransientError):
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
        from redis_client import safe_redis_operation
        val = safe_redis_operation(
            op=lambda client: client.get(key),
            fallback=None,
            operation_name="tts_cache_get",
        )
        if isinstance(val, bytes):
            return val.decode("utf-8")
        return val
    except Exception as e:
        log_event(
            service="tasks",
            event="tts_cache_get_failed",
            status="error",
            message="Failed to get TTS cache",
            extra={"key": key, "error": str(e), "stack": traceback.format_exc()}
        )
        return None

def set_tts_cache(session_id: str, text: str, audio_url: str, ttl: int = 300):
    client = _redis()
    if not client:
        log_event(
            service="tasks", 
            event="tts_cache_set_skipped", 
            status="warn",
            message="Redis not available for TTS cache"
        )
        return
    key = _tts_cache_key(session_id, text)
    try:
        from redis_client import safe_redis_operation
        safe_redis_operation(
            op=lambda client: client.setex(key, ttl, audio_url),
            fallback=None,
            operation_name="tts_cache_set",
        )
    except Exception as e:
        log_event(
            service="tasks", 
            event="tts_cache_set_failed", 
            status="error",
            message="Failed to set TTS cache",
            extra={"key": key, "error": str(e), "stack": traceback.format_exc()}
        )

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
    if config.PUBLIC_AUDIO_HOST:
        if config.PUBLIC_AUDIO_INCLUDE_BUCKET:
            return f"{config.PUBLIC_AUDIO_HOST.rstrip('/')}/{bucket}/{key.lstrip('/')}"
        return f"{config.PUBLIC_AUDIO_HOST.rstrip('/')}/{key.lstrip('/')}"
    s3 = _get_s3_client()
    if s3:
        try:
            return s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=config.PUBLIC_URL_EXPIRES,
            )
        except Exception as e:
            log_event(
                service="tasks", 
                event="presign_failed", 
                status="error",
                message="Failed to presign URL",
                extra={"bucket": bucket, "key": key, "error": str(e)}
            )
    # Return explicit error instead of relative path
    log_event(
        service="tasks", 
        event="presign_unavailable", 
        status="error",
        message="Cannot generate public URL: PUBLIC_AUDIO_HOST not set and presign failed"
    )
    raise ConfigurationError("Cannot generate public URL: configuration missing")

# --------------------------------------------------------------------------
# Upload to R2 with circuit breaker integration
# --------------------------------------------------------------------------
def upload_to_r2(session_id: str, trace_id: str, audio_bytes: bytes) -> Tuple[str, float]:
    # Check circuit breaker first
    try:
        from circuit_breaker import get_circuit_breaker
        r2_circuit_breaker = get_circuit_breaker("r2")
        if r2_circuit_breaker.is_open():
            raise TransientError("R2 circuit breaker is open")
    except ImportError:
        # Proceed without circuit breaker if not available
        pass

    s3 = _get_s3_client()
    key = f"{session_id}/{trace_id}.wav"
    size_bytes = len(audio_bytes or b"")
    start = time.time()

    log_event(
        service="tasks", 
        event="upload_start", 
        status="ok",
        message="Uploading to R2",
        trace_id=trace_id, 
        session_id=session_id,
        extra={"key": key, "size_bytes": size_bytes}
    )

    if not s3:
        inc_metric, obs_latency = get_metrics()
        inc_metric("tts_failures_total", labels={"reason": "client_unavailable"})
        log_event(
            service="tasks", 
            event="upload_unavailable", 
            status="error",
            message="R2 client unavailable",
            trace_id=trace_id, 
            session_id=session_id
        )
        raise R2UploadError("R2 client unavailable")

    try:
        s3.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=audio_bytes,
            ContentType="audio/wav",  # Already correct - kept for consistency
        )
        duration_ms = round((time.time() - start) * 1000, 2)
        url = make_public_url(R2_BUCKET_NAME, key)
        inc_metric, obs_latency = get_metrics()
        inc_metric("tts_uploads_total", labels={"op": "r2_put"})
        obs_latency("r2_upload_ms", duration_ms, labels={"op": "r2_put"})

        log_event(
            service="tasks", 
            event="upload_success", 
            status="ok",
            message="Upload complete",
            trace_id=trace_id, 
            session_id=session_id,
            extra={"url": url, "r2_upload_ms": duration_ms, "size_bytes": size_bytes}
        )
        return url, duration_ms
    except Exception as e:
        inc_metric, obs_latency = get_metrics()
        inc_metric("tts_failures_total", labels={"reason": f"exc_{type(e).__name__}"})  # Fixed: Added exception class name
        log_event(
            service="tasks", 
            event="upload_failure", 
            status="error",
            message="R2 upload failed",
            trace_id=trace_id, 
            session_id=session_id,
            extra={"error": str(e), "stack": traceback.format_exc()}
        )
        raise R2UploadError(str(e)) from e

# --------------------------------------------------------------------------
# Deepgram TTS - Updated to use external_api wrapper
# --------------------------------------------------------------------------
def deepgram_tts_rest(text: str, timeout: int = 30) -> Tuple[bytes, float]:
    if not config.DG_API_KEY:
        log_event(
            service="tasks", 
            event="deepgram_missing_key", 
            status="error",
            message="Missing Deepgram API key"
        )
        raise ConfigurationError("Missing Deepgram API key")  # Non-transient

    url = f"https://api.deepgram.com/v1/speak?model={config.DG_SPEAK_MODEL}&encoding=linear16&container=wav"
    headers = {"Authorization": f"Token {config.DG_API_KEY}", "Content-Type": "text/plain"}

    start = time.time()
    
    # Use external_api wrapper for Deepgram calls
    from external_api import external_api_call
    
    def make_deepgram_request():
        """Encapsulate Deepgram API call for external_api wrapper"""
        return requests.post(url, headers=headers, data=text.encode("utf-8"), timeout=timeout)
    
    try:
        resp = external_api_call("deepgram", make_deepgram_request)
        if resp.status_code == 200:
            latency_ms = round((time.time() - start) * 1000, 2)
            return resp.content, latency_ms
        elif 400 <= resp.status_code < 500:
            # Client errors are non-transient
            inc_metric, obs_latency = get_metrics()
            inc_metric("tts_failures_total", labels={"reason": f"client_error_{resp.status_code}"})
            log_event(
                service="tasks", 
                event="deepgram_client_error", 
                status="error",
                message="Deepgram client error",
                extra={"status_code": resp.status_code, "response": resp.text[:200]}
            )
            raise ConfigurationError(f"Deepgram client error: {resp.status_code}")
        else:
            # Server errors are transient
            inc_metric, obs_latency = get_metrics()
            inc_metric("tts_failures_total", labels={"reason": f"server_error_{resp.status_code}"})
            log_event(
                service="tasks", 
                event="deepgram_server_error", 
                status="error",
                message="Deepgram server error",
                extra={"status_code": resp.status_code, "response": resp.text[:200]}
            )
            raise TransientError(f"Deepgram server error: {resp.status_code}")
            
    except Exception as e:
        inc_metric, obs_latency = get_metrics()
        inc_metric("tts_failures_total", labels={"reason": f"exc_{type(e).__name__}"})  # Fixed: Added exception class name
        log_event(
            service="tasks", 
            event="deepgram_failed", 
            status="error",
            message="Deepgram failed after retries",
            extra={"error": str(e), "stack": traceback.format_exc()}
        )
        if isinstance(e, (requests.Timeout, requests.ConnectionError)):
            raise TransientError(f"Deepgram network error: {e}") from e
        else:
            raise

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
        log_event(
            service="tasks", 
            event="tts_no_text", 
            status="error",
            message="No text provided", 
            trace_id=trace_id, 
            session_id=session_id
        )
        inc_metric("tts_failures_total", labels={"reason": "no_text"})
        return {"error": "No text provided", "trace_id": trace_id}

    if len(text) > config.MAX_TTS_TEXT_LEN:
        log_event(
            service="tasks", 
            event="tts_text_truncated", 
            status="warn",
            message=f"TTS text truncated to {config.MAX_TTS_TEXT_LEN} chars",
            trace_id=trace_id, 
            session_id=session_id,
            extra={"original_length": len(text)}
        )
        text = text[:config.MAX_TTS_TEXT_LEN]

    cached_url = get_tts_cache(session_id, text)
    if cached_url:
        inc_metric("tts_cache_hits_total")
        log_event(
            service="tasks", 
            event="cache_hit", 
            status="ok",
            message="Cache hit for TTS",
            trace_id=trace_id, 
            session_id=session_id
        )
        return {"audio_url": cached_url, "cached": True,
                "trace_id": trace_id, "session_id": session_id}

    tts_start = time.time()
    log_event(
        service="tasks", 
        event="tts_start", 
        status="ok",
        message="TTS start", 
        trace_id=trace_id, 
        session_id=session_id
    )

    try:
        audio_bytes, deepgram_latency_ms = deepgram_tts_rest(text)
        obs_latency("deepgram_latency_ms", deepgram_latency_ms, labels={"provider": "deepgram"})

        upload_url, r2_upload_ms = upload_to_r2(session_id, trace_id, audio_bytes)
        obs_latency("r2_upload_ms", r2_upload_ms, labels={"op": "r2_put"})

        total_ms = round((time.time() - tts_start) * 1000, 2)
        obs_latency("tts_latency_ms", total_ms, labels={"op": "full_tts"})
        inc_metric("tts_requests_completed_total")

        try:
            set_tts_cache(session_id, text, upload_url)
        except Exception as e:
            log_event(
                service="tasks", 
                event="cache_store_error", 
                status="error",
                message="Cache store failed",
                trace_id=trace_id, 
                session_id=session_id,
                extra={"error": str(e)}
            )

        log_event(
            service="tasks", 
            event="tts_done", 
            status="ok",
            message="TTS completed",
            trace_id=trace_id, 
            session_id=session_id,
            extra={"tts_latency_ms": total_ms, "audio_url": upload_url}
        )
        return {"trace_id": trace_id, "session_id": session_id,
                "audio_url": upload_url, "total_ms": total_ms}

    except Exception as e:
        inc_metric("tts_failures_total", labels={"reason": f"exc_{type(e).__name__}"})  # Fixed: Added exception class name
        log_event(
            service="tasks", 
            event="tts_error", 
            status="error",
            message=str(e),
            trace_id=trace_id, 
            session_id=session_id,
            extra={"stack": traceback.format_exc()}
        )
        if raise_on_error:
            raise
        return {"error": str(e), "trace_id": trace_id}

# --------------------------------------------------------------------------
# Phase 12: Celery Task with Resilience Decorators
# --------------------------------------------------------------------------
@resilient_task
def _run_tts_task(self, payload=None):
    """
    Phase 12: TTS task with full resilience patterns
    """
    # Check Redis availability before proceeding
    redis_client = _redis()
    if not redis_client:
        log_event(
            service="tasks", 
            event="redis_unavailable_at_task_start", 
            status="warn",
            message="Redis unavailable at task start - proceeding without cache"
        )
    
    result = perform_tts_core(payload, raise_on_error=True)
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
# Phase 12: GPT + TTS Inference Task with Resilience
# --------------------------------------------------------------------------
@resilient_task
def run_inference(self, payload):
    """
    Phase 12: Inference task with full resilience patterns
    """
    payload = normalize_payload(payload)
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    transcript = extract_text(payload)

    log_event(
        service="tasks", 
        event="inference_start", 
        status="ok",
        message="Inference received",
        trace_id=trace_id, 
        session_id=session_id
    )

    inc_metric, obs_latency = get_metrics()  # Fixed: Initialize metrics before try block
    try:
        t0 = time.time()
        reply_text = generate_reply_sync(transcript, trace_id=trace_id)
        latency_ms = round((time.time() - t0) * 1000, 2)
        obs_latency("inference_latency_ms", latency_ms, labels={"task": self.name})

        run_tts.delay({"text": reply_text, "trace_id": trace_id, "session_id": session_id})
        log_event(
            service="tasks", 
            event="inference_done", 
            status="ok",
            message="Inference complete, TTS enqueued",
            trace_id=trace_id, 
            session_id=session_id,
            extra={"latency_ms": latency_ms}
        )
        
        return {"reply": reply_text, "latency_ms": latency_ms, "trace_id": trace_id}
    except TransientError as e:
        inc_metric("inference_failures_total", labels={"task": self.name, "type": "transient"})
        raise  # let Celery retry
    except Exception as e:
        inc_metric("inference_failures_total", labels={"task": self.name, "type": "fatal"})
        raise