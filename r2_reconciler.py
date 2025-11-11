"""
r2_reconciler.py — Phase 12 (TTS Upload Reconciliation)
Scheduled background job to reconcile pending TTS uploads with retry logic and metrics.
"""

import time
import traceback
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple

# --------------------------------------------------------------------------
# Configuration and Imports
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    import os
    class Config:
        R2_RECONCILE_INTERVAL_MIN = int(os.getenv("R2_RECONCILE_INTERVAL_MIN", "5"))
        R2_RECONCILE_LOCK_TIMEOUT = int(os.getenv("R2_RECONCILE_LOCK_TIMEOUT", "300"))
        R2_RECONCILE_STALE_HOURS = int(os.getenv("R2_RECONCILE_STALE_HOURS", "24"))
        R2_RECONCILE_MAX_RETRIES = int(os.getenv("R2_RECONCILE_MAX_RETRIES", "3"))
        R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "tts-cache")
        R2_RECONCILE_MAX_KEYS_PER_CYCLE = int(os.getenv("R2_RECONCILE_MAX_KEYS_PER_CYCLE", "1000"))

# Redis client
try:
    from redis_client import get_redis_client, safe_redis_operation
except ImportError:
    def get_redis_client():
        return None
    def safe_redis_operation(op, fallback=None, operation_name:str="r2_reconciler_op"):
        try:
            return op()
        except Exception:
            return fallback

# R2 client
try:
    from r2_client import get_r2_client, upload_to_r2
except ImportError:
    def get_r2_client():
        return None
    def upload_to_r2(*args, **kwargs):
        raise Exception("R2 client not available")

# Metrics integration
try:
    import metrics_collector as metrics
except ImportError:
    class metrics:
        @staticmethod
        def increment_metric(name, labels=None, amount=1): pass
        @staticmethod
        def observe_latency(name, value_ms, labels=None): pass
        @staticmethod
        def set_gauge(name, value, labels=None): pass

# Structured logging
try:
    from logging_utils import get_logger, log_event
    logger = get_logger("r2_reconciler")
except ImportError:
    import logging
    logger = logging.getLogger("r2_reconciler")
    def log_event(*, service, event, status, message, trace_id=None, extra=None):  # fallback
        level = logging.INFO if status in ("ok","info","success") else logging.ERROR
        logger.log(level, f"{event}: {message}", extra=extra or {})

# Celery integration and resilient_task decorator
try:
    from tasks import resilient_task
except ImportError:
    # Fallback if tasks module is not available
    def resilient_task(func=None, **kwargs):
        def decorator(f):
            return f
        return decorator if func is None else decorator(func)

# Circuit breaker integration
try:
    from circuit_breaker import get_circuit_breaker
    r2_circuit_breaker = get_circuit_breaker("r2")
except ImportError:
    # Fallback circuit breaker
    class DummyCircuitBreaker:
        def is_open(self):
            return False
        def record_success(self):
            pass
        def record_failure(self):
            pass
        def get_state(self):
            return "closed"
        def get_remaining_cooldown(self):
            return 0
    r2_circuit_breaker = DummyCircuitBreaker()

# --------------------------------------------------------------------------
# Atomic Lock Helpers
# --------------------------------------------------------------------------

# --- Atomic lock helpers (tokened) ---
_LOCK_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""

def _new_lock_token() -> str:
    # PID + time for minimal uniqueness; ok for single Redis deployment
    return f"{os.getpid()}-{int(time.time()*1000)}"

def acquire_lock_atomic(client, lock_key: str, ttl_seconds: int) -> Optional[str]:
    """
    SET {key} {token} NX EX {ttl}; returns token if acquired else None.
    """
    token = _new_lock_token()
    ok = safe_redis_operation(
        op=lambda: client.set(lock_key, token, nx=True, ex=ttl_seconds),
        fallback=False,
        operation_name="r2_reconciler_acquire_lock",
    )
    return token if ok else None

def release_lock_atomic(client, lock_key: str, token: str) -> bool:
    """
    Release lock only if token matches. Uses Lua to be atomic.
    """
    res = safe_redis_operation(
        op=lambda: client.eval(_LOCK_RELEASE_LUA, 1, lock_key, token),
        fallback=0,
        operation_name="r2_reconciler_release_lock",
    )
    return bool(res)

# --------------------------------------------------------------------------
# R2 Reconciler Class
# --------------------------------------------------------------------------

class R2Reconciler:
    """
    Scheduled background job to reconcile pending TTS uploads.
    Runs every 5-10 minutes to retry failed uploads and clean up stale entries.
    """
    
    def __init__(self):
        self.redis_client = get_redis_client()
        self.r2_client = get_r2_client()
        self.redis_available = self.redis_client is not None
        self.r2_available = self.r2_client is not None
        
        logger.info("R2Reconciler initialized", extra={
            "redis_available": self.redis_available,
            "r2_available": self.r2_available,
            "reconcile_interval_min": Config.R2_RECONCILE_INTERVAL_MIN,
            "lock_timeout_sec": Config.R2_RECONCILE_LOCK_TIMEOUT,
            "stale_hours": Config.R2_RECONCILE_STALE_HOURS,
            "max_keys_per_cycle": Config.R2_RECONCILE_MAX_KEYS_PER_CYCLE,
            "service": "r2_reconciler"
        })
    
    def _atomic_lock_operation(self, lock_key: str, timeout_sec: int) -> Tuple[bool, str]:
        """
        Atomic lock acquisition with token for safety.
        
        Returns:
            Tuple[bool, str]: (success, token)
        """
        if not self.redis_client:
            return True, "no_redis"
        
        token = f"{os.getpid()}-{int(time.time())}"
        acquired = safe_redis_operation(
            op=lambda: self.redis_client.set(lock_key, token, nx=True, ex=timeout_sec),
            fallback=False,
            operation_name="r2_reconciler_atomic_lock_op",
        )
        return bool(acquired), token if acquired else ""
    
    def _atomic_lock_release(self, lock_key: str, expected_token: str) -> bool:
        """
        Safely release lock only if token matches.
        """
        if not self.redis_client:
            return True
        
        # Prefer atomic Lua release used elsewhere
        return release_lock_atomic(self.redis_client, lock_key, expected_token)
    
    def acquire_lock(self) -> Tuple[bool, str]:
        """
        Acquire distributed lock using Redis SET with NX/EX for atomicity.
        
        Returns:
            Tuple[bool, str]: (success, token)
        """
        if not self.redis_client:
            logger.warning("Redis client not available, cannot acquire lock", extra={
                "service": "r2_reconciler"
            })
            return True, "no_redis"  # Proceed without lock if Redis unavailable
        
        lock_key = "r2:reconcile:lock"
        token = acquire_lock_atomic(self.redis_client, lock_key, Config.R2_RECONCILE_LOCK_TIMEOUT)
        
        if token:
            logger.debug("Reconciler lock acquired", extra={
                "lock_key": lock_key,
                "timeout_sec": Config.R2_RECONCILE_LOCK_TIMEOUT,
                "service": "r2_reconciler"
            })
            return True, token
        else:
            logger.debug("Reconciler lock already held by another worker", extra={
                "lock_key": lock_key,
                "service": "r2_reconciler"
            })
            return False, ""
    
    def release_lock(self, token: str):
        """Release the distributed lock safely with token check."""
        if not self.redis_client or token == "no_redis":
            return
        
        lock_key = "r2:reconcile:lock"
        try:
            if release_lock_atomic(self.redis_client, lock_key, token):
                logger.debug("Reconciler lock released", extra={
                    "lock_key": lock_key,
                    "service": "r2_reconciler"
                })
            else:
                logger.warning("Failed to release lock - token mismatch or lock expired", extra={
                    "lock_key": lock_key,
                    "service": "r2_reconciler"
                })
        except Exception as e:
            logger.error("Error releasing reconciler lock", extra={
                "error": str(e),
                "service": "r2_reconciler"
            })
    
    def acquire_object_lock(self, content_hash: str, ttl_seconds: int = 300) -> Optional[str]:
        """
        Acquire per-object lock using Redis SETNX with token for safety.
        
        Returns:
            str: token if acquired, None if not
        """
        if not self.redis_client:
            return "no_redis"
            
        lock_key = f"r2:object_lock:{content_hash}"
        token = acquire_lock_atomic(self.redis_client, lock_key, ttl_seconds)
        
        if token:
            logger.debug("Object lock acquired", extra={
                "lock_key": lock_key,
                "content_hash": content_hash[:16],
                "service": "r2_reconciler"
            })
        else:
            logger.debug("Object lock already held", extra={
                "lock_key": lock_key,
                "content_hash": content_hash[:16],
                "service": "r2_reconciler"
            })
            
        return token
    
    def release_object_lock(self, content_hash: str, token: str):
        """Release per-object lock safely with token check."""
        if not self.redis_client or token == "no_redis":
            return
            
        lock_key = f"r2:object_lock:{content_hash}"
        try:
            if release_lock_atomic(self.redis_client, lock_key, token):
                logger.debug("Object lock released", extra={
                    "lock_key": lock_key,
                    "content_hash": content_hash[:16],
                    "service": "r2_reconciler"
                })
        except Exception as e:
            logger.error("Error releasing object lock", extra={
                "content_hash": content_hash[:16],
                "error": str(e),
                "service": "r2_reconciler"
            })
    
    def find_pending_uploads(self) -> List[Dict[str, Any]]:
        """
        Scan Redis for TTS keys with upload_status == "pending" using SCAN to avoid blocking.
        
        Returns:
            List of pending upload entries with their metadata
        """
        if not self.redis_client:
            logger.warning("Redis client not available, cannot find pending uploads", extra={
                "service": "r2_reconciler"
            })
            return []
        
        pending_uploads = []
        processed_count = 0
        
        try:
            cursor = 0
            pattern = "tts:*"
            
            # Use SCAN iteratively with count limit
            while True:
                try:
                    cursor, keys = safe_redis_operation(
                        op=lambda: self.redis_client.scan(cursor=cursor, match=pattern, count=100),
                        fallback=(0, []),
                        operation_name="r2_reconciler_scan",
                    )
                    
                    for key in keys:
                        if processed_count >= Config.R2_RECONCILE_MAX_KEYS_PER_CYCLE:
                            logger.info("Reached max keys per cycle limit", extra={
                                "max_keys": Config.R2_RECONCILE_MAX_KEYS_PER_CYCLE,
                                "service": "r2_reconciler"
                            })
                            break
                            
                        try:
                            if isinstance(key, bytes):
                                key = key.decode('utf-8')
                            
                            # Get upload status
                            upload_status = safe_redis_operation(
                                op=lambda: self.redis_client.hget(key, "upload_status"),
                                fallback=None,
                                operation_name="r2_reconciler_hget_status",
                            )
                            if isinstance(upload_status, bytes):
                                upload_status = upload_status.decode('utf-8')
                            
                            if upload_status == "pending":
                                # Get all fields for this upload
                                upload_data = safe_redis_operation(
                                    op=lambda: self.redis_client.hgetall(key),
                                    fallback={},
                                    operation_name="r2_reconciler_hgetall",
                                )
                                decoded_data = {}
                                for k, v in upload_data.items():
                                    if isinstance(k, bytes):
                                        k = k.decode('utf-8')
                                    if isinstance(v, bytes):
                                        v = v.decode('utf-8')
                                    decoded_data[k] = v
                                
                                # Extract content hash from key (tts:{content_hash})
                                content_hash = key.split(":")[1] if ":" in key else key
                                
                                pending_upload = {
                                    "redis_key": key,
                                    "content_hash": content_hash,
                                    "upload_data": decoded_data,
                                    "created_at": decoded_data.get("created_at"),
                                    "r2_key": decoded_data.get("r2_key"),
                                    "retry_count": int(decoded_data.get("retry_count", "0"))
                                }
                                
                                pending_uploads.append(pending_upload)
                                processed_count += 1
                                
                        except Exception as e:
                            logger.error("Error processing Redis key", extra={
                                "key": key,
                                "error": str(e),
                                "service": "r2_reconciler"
                            })
                            continue
                    
                    # Break conditions: no more keys or reached limit
                    if cursor == 0 or processed_count >= Config.R2_RECONCILE_MAX_KEYS_PER_CYCLE:
                        break
                        
                except Exception as e:
                    logger.error("Error during SCAN iteration", extra={
                        "cursor": cursor,
                        "error": str(e),
                        "service": "r2_reconciler"
                    })
                    break
            
            logger.info("Found pending uploads via SCAN", extra={
                "pending_count": len(pending_uploads),
                "processed_count": processed_count,
                "max_keys_per_cycle": Config.R2_RECONCILE_MAX_KEYS_PER_CYCLE,
                "service": "r2_reconciler"
            })
            
            # Emit scanned metric
            metrics.increment_metric("r2_reconcile_scanned_total", amount=len(pending_uploads))
            metrics.set_gauge("r2_reconcile_batch_processed_total", value=processed_count)
            
            return pending_uploads
            
        except Exception as e:
            logger.error("Error finding pending uploads", extra={
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "r2_reconciler"
            })
            return []
    
    def is_stale_upload(self, upload_data: Dict[str, Any]) -> bool:
        """
        Check if an upload is stale (older than configured threshold) with proper timezone handling.
        
        Args:
            upload_data: Upload metadata from Redis
            
        Returns:
            bool: True if upload is stale and should be cleaned up
        """
        try:
            created_at_str = upload_data.get("created_at")
            if not created_at_str:
                return True
            
            # Parse ISO format timestamp with timezone awareness
            if created_at_str.endswith('Z'):
                created_at = datetime.fromisoformat(created_at_str[:-1] + '+00:00')
            else:
                created_at = datetime.fromisoformat(created_at_str)
            
            # Use timezone-aware now for comparison
            now = datetime.now(timezone.utc)
            if created_at.tzinfo is None:
                # If created_at is naive, assume UTC
                created_at = created_at.replace(tzinfo=timezone.utc)
            
            stale_threshold = timedelta(hours=Config.R2_RECONCILE_STALE_HOURS)
            is_stale = now - created_at > stale_threshold
            
            if is_stale:
                logger.debug("Found stale upload", extra={
                    "content_hash": upload_data.get("content_hash", "unknown")[:16],
                    "created_at": created_at_str,
                    "age_hours": (now - created_at).total_seconds() / 3600,
                    "stale_threshold_hours": Config.R2_RECONCILE_STALE_HOURS,
                    "service": "r2_reconciler"
                })
            
            return is_stale
            
        except Exception as e:
            logger.error("Error checking upload staleness", extra={
                "upload_data": upload_data,
                "error": str(e),
                "service": "r2_reconciler"
            })
            return False  # Don't clean up on parsing errors
    
    def get_audio_data_for_retry(self, content_hash: str) -> Optional[bytes]:
        """
        Retrieve audio data for retry attempt from Redis blob storage.
        
        Args:
            content_hash: Content hash identifying the TTS audio
            
        Returns:
            Optional[bytes]: Audio data bytes or None if unavailable
        """
        if not self.redis_client:
            return None
            
        try:
            blob_key = f"tts:blob:{content_hash}"
            
            # Use the existing client; wrapper handles failures
            audio_data = safe_redis_operation(
                op=lambda: self.redis_client.get(blob_key),
                fallback=None,
                operation_name="r2_reconciler_blob_get",
            )
            if isinstance(audio_data, str):  # decoded due to decode_responses=True -> bad for blobs
                logger.error("Audio data corrupted - string instead of bytes", extra={
                    "content_hash": content_hash[:16],
                    "blob_key": blob_key,
                    "service": "r2_reconciler"
                })
                audio_data = None  # refuse to use potentially mangled bytes
            
            if audio_data:
                logger.debug("Retrieved audio data from blob storage", extra={
                    "content_hash": content_hash[:16],
                    "blob_key": blob_key,
                    "data_size": len(audio_data),
                    "service": "r2_reconciler"
                })
                return audio_data
            else:
                logger.warning("Audio data not found in blob storage", extra={
                    "content_hash": content_hash[:16],
                    "blob_key": blob_key,
                    "service": "r2_reconciler"
                })
                return None
                
        except Exception as e:
            logger.error("Error retrieving audio data from blob storage", extra={
                "content_hash": content_hash[:16],
                "error": str(e),
                "service": "r2_reconciler"
            })
            return None
    
    # Removed local retry wrapper; centralized safe_redis_operation is used instead.
    
    def retry_upload(self, pending_upload: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Retry a pending upload to R2 with proper error handling and circuit breaker.
        
        Args:
            pending_upload: Pending upload metadata
            
        Returns:
            Tuple[bool, str]: (success, message)
        """
        content_hash = pending_upload["content_hash"]
        r2_key = pending_upload.get("r2_key")
        retry_count = pending_upload.get("retry_count", 0)
        trace_id = f"reconcile_retry_{content_hash[:16]}_{int(time.time())}"
        
        logger.info("Retrying upload", extra={
            "content_hash": content_hash[:16],
            "r2_key": r2_key,
            "retry_count": retry_count,
            "trace_id": trace_id,
            "service": "r2_reconciler"
        })
        
        # Check retry limit
        if retry_count >= Config.R2_RECONCILE_MAX_RETRIES:
            logger.warning("Max retries exceeded for upload", extra={
                "content_hash": content_hash[:16],
                "retry_count": retry_count,
                "max_retries": Config.R2_RECONCILE_MAX_RETRIES,
                "trace_id": trace_id,
                "service": "r2_reconciler"
            })
            metrics.increment_metric("r2_reconcile_failed_total", labels={"reason": "max_retries_exceeded"})
            return False, "max_retries_exceeded"
        
        # Acquire per-object lock for idempotency
        lock_token = self.acquire_object_lock(content_hash)
        if not lock_token:
            logger.info("Object lock already held, skipping upload", extra={
                "content_hash": content_hash[:16],
                "trace_id": trace_id,
                "service": "r2_reconciler"
            })
            metrics.increment_metric("r2_reconcile_requeued_total", labels={"reason": "object_locked"})
            return False, "object_locked"
        
        try:
            # Check circuit breaker state before R2 call
            breaker_state = r2_circuit_breaker.get_state()
            if r2_circuit_breaker.is_open():
                cooldown_remaining = r2_circuit_breaker.get_remaining_cooldown()
                logger.warning("Circuit breaker is open, skipping R2 call", extra={
                    "content_hash": content_hash[:16],
                    "trace_id": trace_id,
                    "breaker_state": breaker_state,
                    "cooldown_remaining_sec": cooldown_remaining,
                    "service": "r2_reconciler"
                })
                metrics.increment_metric("r2_reconcile_failed_total", labels={"reason": "circuit_breaker_open"})
                metrics.set_gauge("r2_circuit_state", value=1 if breaker_state == "open" else 0)  # 1=open, 0=closed
                return False, "circuit_breaker_open"
            
            # Log breaker state for visibility
            metrics.set_gauge("r2_circuit_state", value=1 if breaker_state == "open" else 0)
            if breaker_state == "half_open":
                logger.info("Circuit breaker is half-open, attempting request", extra={
                    "content_hash": content_hash[:16],
                    "service": "r2_reconciler"
                })
            
            # Get audio data for retry
            audio_data = self.get_audio_data_for_retry(content_hash)
            if not audio_data:
                logger.error("Cannot retry upload: audio data unavailable", extra={
                    "content_hash": content_hash[:16],
                    "trace_id": trace_id,
                    "service": "r2_reconciler"
                })
                metrics.increment_metric("r2_reconcile_failed_total", labels={"reason": "audio_data_unavailable"})
                return False, "audio_data_unavailable"
            
            # Get R2 bucket
            try:
                from config import Config
                bucket = Config.R2_BUCKET_NAME
            except Exception:
                bucket = os.getenv("R2_BUCKET_NAME")
            
            if not bucket:
                logger.error("R2_BUCKET_NAME not configured", extra={
                    "content_hash": content_hash[:16],
                    "trace_id": trace_id,
                    "service": "r2_reconciler"
                })
                metrics.increment_metric("r2_reconcile_failed_total", labels={"reason": "bucket_not_configured"})
                return False, "bucket_not_configured"
            
            # Generate R2 key if not present
            if not r2_key:
                hash_prefix = content_hash[:4]
                r2_key = f"tts-cache/{hash_prefix}/{content_hash}.audio"
            
            # Attempt upload with error handling
            try:
                success, upload_metadata = upload_to_r2(
                    self.r2_client,
                    bucket,
                    r2_key,
                    audio_data,
                    content_type="audio/wav",  # Fixed: Changed from "audio/mpeg" to "audio/wav" to match producer
                    trace_id=trace_id
                )
            except Exception as e:
                logger.error("Upload operation failed with exception", extra={
                    "content_hash": content_hash[:16],
                    "error": str(e),
                    "service": "r2_reconciler"
                })
                success = False
                upload_metadata = {"error": str(e)}
            
            if success:
                # Mark upload as complete
                self._update_upload_status(content_hash, "complete", r2_key, retry_count + 1)
                metrics.increment_metric("r2_reconcile_success_total")
                metrics.increment_metric("r2_reconcile_uploaded_total")
                r2_circuit_breaker.record_success()
                
                logger.info("Upload retry successful", extra={
                    "event": "upload_retry_success",
                    "content_hash": content_hash[:16],
                    "r2_key": r2_key,
                    "retry_count": retry_count + 1,
                    "trace_id": trace_id,
                    "service": "r2_reconciler"
                })
                return True, "success"
            else:
                # Update retry count but keep as pending
                self._update_upload_status(content_hash, "pending", r2_key, retry_count + 1)
                metrics.increment_metric("r2_reconcile_failed_total", labels={"reason": "upload_failed"})
                r2_circuit_breaker.record_failure()
                
                logger.error("Upload retry failed", extra={
                    "event": "upload_retry_failed",
                    "content_hash": content_hash[:16],
                    "r2_key": r2_key,
                    "retry_count": retry_count + 1,
                    "trace_id": trace_id,
                    "upload_metadata": str(upload_metadata),
                    "service": "r2_reconciler"
                })
                return False, "upload_failed"
                
        except Exception as e:
            # Update retry count but keep as pending
            self._update_upload_status(content_hash, "pending", r2_key, retry_count + 1)
            metrics.increment_metric("r2_reconcile_failed_total", labels={"reason": f"exc_{type(e).__name__}"})  # Fixed: Added exception class name
            r2_circuit_breaker.record_failure()
            
            logger.error("Upload retry exception", extra={
                "event": "upload_retry_exception",
                "content_hash": content_hash[:16],
                "r2_key": r2_key,
                "retry_count": retry_count + 1,
                "trace_id": trace_id,
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "r2_reconciler"
            })
            return False, f"exception: {str(e)}"
        finally:
            # Always release the object lock
            self.release_object_lock(content_hash, lock_token)
    
    def _update_upload_status(self, content_hash: str, status: str, r2_key: str = None, retry_count: int = None):
        """
        Update upload status in Redis with consistent timestamp formatting.
        
        Args:
            content_hash: Content hash identifying the upload
            status: New upload status
            r2_key: R2 object key (optional)
            retry_count: Number of retry attempts (optional)
        """
        if not self.redis_client:
            return
        
        try:
            redis_key = f"tts:{content_hash}"
            mapping = {
                "upload_status": status,
                "updated_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')  # Consistent format
            }
            
            if r2_key:
                mapping["r2_key"] = r2_key
            
            if retry_count is not None:
                mapping["retry_count"] = str(retry_count)
            
            safe_redis_operation(
                op=lambda: self.redis_client.hset(redis_key, mapping=mapping),
                fallback=None,
                operation_name="r2_reconciler_hset_update_status",
            )
            
        except Exception as e:
            logger.error("Error updating upload status", extra={
                "content_hash": content_hash[:16],
                "status": status,
                "error": str(e),
                "service": "r2_reconciler"
            })
    
    def cleanup_stale_uploads(self, pending_uploads: List[Dict[str, Any]]) -> int:
        """
        Clean up stale pending uploads (older than configured threshold) with proper lock handling.
        
        Args:
            pending_uploads: List of pending upload entries
            
        Returns:
            int: Number of stale entries cleaned up
        """
        if not self.redis_client:
            return 0
        
        cleaned_count = 0
        
        for upload in pending_uploads:
            if not self.is_stale_upload(upload["upload_data"]):
                continue
                
            # Acquire object lock before cleanup
            lock_token = self.acquire_object_lock(upload["content_hash"], ttl_seconds=60)
            if not lock_token:
                continue
                
            try:
                # Double-check staleness after acquiring lock
                if self.is_stale_upload(upload["upload_data"]):
                    # Remove the stale upload tracking
                    safe_redis_operation(
                        op=lambda: self.redis_client.delete(upload["redis_key"]),
                        fallback=0,
                        operation_name="r2_reconciler_delete_upload_key",
                    )
                    
                    # Clean up associated blob key to prevent storage leaks
                    blob_key = f"tts:blob:{upload['content_hash']}"
                    try:
                        safe_redis_operation(
                            op=lambda: self.redis_client.delete(blob_key),
                            fallback=0,
                            operation_name="r2_reconciler_delete_blob_key",
                        )
                        logger.debug("Cleaned up blob storage for stale upload", extra={
                            "content_hash": upload["content_hash"][:16],
                            "blob_key": blob_key,
                            "service": "r2_reconciler"
                        })
                    except Exception:
                        logger.warning("Failed to clean up blob storage for stale upload", extra={
                            "content_hash": upload["content_hash"][:16],
                            "blob_key": blob_key,
                            "service": "r2_reconciler"
                        })
                    
                    cleaned_count += 1
                    
                    logger.info("Cleaned up stale upload", extra={
                        "content_hash": upload["content_hash"][:16],
                        "redis_key": upload["redis_key"],
                        "created_at": upload["upload_data"].get("created_at"),
                        "service": "r2_reconciler"
                    })
                    
            except Exception as e:
                logger.error("Error cleaning up stale upload", extra={
                    "content_hash": upload["content_hash"][:16],
                    "redis_key": upload["redis_key"],
                    "error": str(e),
                    "service": "r2_reconciler"
                })
            finally:
                # Always release the lock
                self.release_object_lock(upload["content_hash"], lock_token)
        
        if cleaned_count > 0:
            logger.info("Stale upload cleanup completed", extra={
                "cleaned_count": cleaned_count,
                "service": "r2_reconciler"
            })
        
        # Emit cleaned metric
        metrics.increment_metric("r2_reconcile_cleaned_total", amount=cleaned_count)
        
        return cleaned_count
    
    def run_reconciliation(self) -> Dict[str, Any]:
        """
        Run the complete reconciliation process.
        
        Returns:
            Dictionary with reconciliation results and metrics
        """
        start_time = time.time()
        results = {
            "success": False,
            "lock_acquired": False,
            "pending_found": 0,
            "retry_success": 0,
            "retry_failure": 0,
            "stale_cleaned": 0,
            "errors": 0,
            "duration_sec": 0
        }
        
        lock_token = ""
        
        try:
            # Structured start event
            log_event(
                service="r2_reconciler",
                event="reconcile_start",
                status="info",
                message="Starting reconciliation cycle",
                extra={"pid": os.getpid()},
            )
            
            # Acquire distributed lock
            lock_acquired, lock_token = self.acquire_lock()
            results["lock_acquired"] = lock_acquired
            
            if not lock_acquired:
                logger.info("Skipping reconciliation - lock held by another worker", extra={
                    "service": "r2_reconciler"
                })
                results["success"] = True  # Not an error, just skipped
                return results
            
            # Increment reconciliation runs metric
            metrics.increment_metric("r2_reconcile_runs_total")
            
            # Find pending uploads
            pending_uploads = self.find_pending_uploads()
            results["pending_found"] = len(pending_uploads)
            metrics.set_gauge("r2_reconcile_pending_count", len(pending_uploads))  # Fixed: Consistent metric naming
            
            logger.info("Starting reconciliation cycle", extra={
                "pending_count": len(pending_uploads),
                "service": "r2_reconciler"
            })
            
            # Process each pending upload
            for upload in pending_uploads:
                try:
                    # Check if upload is stale
                    if self.is_stale_upload(upload["upload_data"]):
                        continue  # Will be handled by cleanup
                    
                    # Retry the upload
                    success, message = self.retry_upload(upload)
                    
                    if success:
                        results["retry_success"] += 1
                    else:
                        results["retry_failure"] += 1
                        if message != "object_locked":  # Don't count locked objects as requeued failures
                            metrics.increment_metric("r2_reconcile_requeued_total", labels={"reason": message})
                        
                except Exception as e:
                    results["errors"] += 1
                    logger.error("Error processing pending upload", extra={
                        "content_hash": upload["content_hash"][:16],
                        "error": str(e),
                        "stack": traceback.format_exc(),
                        "service": "r2_reconciler"
                    })
            
            # Clean up stale uploads
            results["stale_cleaned"] = self.cleanup_stale_uploads(pending_uploads)
            
            # Calculate duration
            results["duration_sec"] = round(time.time() - start_time, 2)
            results["success"] = True
            
            log_event(
                service="r2_reconciler",
                event="reconcile_completed",
                status="success",
                message="Reconciliation cycle completed",
                extra=results,
            )
            
            return results
            
        except Exception as e:
            results["errors"] += 1
            results["duration_sec"] = round(time.time() - start_time, 2)
            
            log_event(
                service="r2_reconciler",
                event="reconcile_failed",
                status="error",
                message="Reconciliation cycle failed",
                extra={**results, "error": str(e), "stack": traceback.format_exc()},
            )
            
            return results
            
        finally:
            # Always release the lock
            if lock_token:
                self.release_lock(lock_token)

# --------------------------------------------------------------------------
# Celery Task Definitions
# --------------------------------------------------------------------------

@resilient_task
def run_r2_reconciliation(self):
    """
    Celery task to run R2 reconciliation.
    Can be scheduled with Celery Beat for periodic execution.
    """
    reconciler = R2Reconciler()
    results = reconciler.run_reconciliation()
    return results

@resilient_task
def retry_r2_upload(self, content_hash: str, r2_key: str, metadata: Dict[str, Any] = None, trace_id: str = None):
    """
    Celery task to retry a specific R2 upload.
    Called from TTS cache manager when uploads fail.
    
    Args:
        content_hash: Content hash identifying the upload
        r2_key: R2 object key
        metadata: Additional metadata for the upload
        trace_id: Trace ID for logging
    """
    trace_id = trace_id or f"celery_retry_{content_hash[:16]}_{int(time.time())}"
    
    logger.info("Celery retry task started", extra={
        "content_hash": content_hash[:16],
        "r2_key": r2_key,
        "trace_id": trace_id,
        "service": "r2_reconciler"
    })
    
    reconciler = R2Reconciler()
    
    # Create pending upload structure for the retry method
    pending_upload = {
        "content_hash": content_hash,
        "r2_key": r2_key,
        "upload_data": metadata or {},
        "retry_count": 0
    }
    
    success, message = reconciler.retry_upload(pending_upload)
    
    return {
        "success": success,
        "message": message,
        "content_hash": content_hash,
        "r2_key": r2_key,
        "trace_id": trace_id
    }

# --------------------------------------------------------------------------
# Global Reconciler Instance
# --------------------------------------------------------------------------

_reconciler: Optional[R2Reconciler] = None

def get_r2_reconciler() -> R2Reconciler:
    """Get or create global R2 reconciler instance."""
    global _reconciler
    if _reconciler is None:
        _reconciler = R2Reconciler()
    return _reconciler

# --------------------------------------------------------------------------
# Helper Functions
# --------------------------------------------------------------------------

def run_reconciliation() -> Dict[str, Any]:
    """Helper function to run reconciliation using global reconciler."""
    reconciler = get_r2_reconciler()
    return reconciler.run_reconciliation()

def get_reconciliation_stats() -> Dict[str, Any]:
    """Get reconciliation statistics and status."""
    reconciler = get_r2_reconciler()
    
    # Get pending count
    pending_uploads = reconciler.find_pending_uploads()
    
    return {
        "pending_uploads": len(pending_uploads),
        "redis_available": reconciler.redis_available,
        "r2_available": reconciler.r2_available,
        "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    }

# --------------------------------------------------------------------------
# Main execution for testing
# --------------------------------------------------------------------------

if __name__ == "__main__":
    # For testing purposes
    reconciler = R2Reconciler()
    results = reconciler.run_reconciliation()
    logger.info("Reconciliation results", extra={"results": results})