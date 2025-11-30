"""
tts_cache_manager.py — Phase 12 (Celery Task Resilience)
Manage cached TTS audio snippets with full fault tolerance, idempotency, and unified resilience patterns.
"""

import hashlib
import json
import time
import os
import base64  # needed for Redis decode in _get_from_redis
from typing import Optional, Dict, Any, Tuple, List
import traceback
from datetime import datetime

# --------------------------------------------------------------------------
# Phase 12: Configuration and Imports with Resilience Patterns
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    class Config:
        TTS_CACHE_TTL_DAYS = int(os.getenv("TTS_CACHE_TTL_DAYS", "30"))
        TTS_CACHE_REDIS_PREFIX = os.getenv("TTS_CACHE_REDIS_PREFIX", "tts_cache")
        TTS_CACHE_R2_PREFIX = os.getenv("TTS_CACHE_R2_PREFIX", "tts-cache")
        TTS_CACHE_MAX_SIZE_MB = int(os.getenv("TTS_CACHE_MAX_SIZE_MB", "1024"))
        TTS_CACHE_CLEANUP_THRESHOLD = float(os.getenv("TTS_CACHE_CLEANUP_THRESHOLD", "0.8"))
        TTS_CACHE_RETRY_ATTEMPTS = int(os.getenv("TTS_CACHE_RETRY_ATTEMPTS", "2"))
        CELERY_MAX_RETRIES = int(os.getenv("CELERY_MAX_RETRIES", "3"))
        R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "sara-ai-audio")

# Phase 12: Transient error detection
class TransientError(Exception):
    """Base class for transient errors that should trigger retries"""
    pass

# Phase 12: Redis client compliance
try:
    from redis_client import get_redis_client, safe_redis_operation
except ImportError:
    def get_redis_client():
        import redis
        redis_url = os.getenv("REDIS_URL", "redis://red-d43ertemcj7s73b0qrcg:6379")
        return redis.from_url(redis_url)
    
    def safe_redis_operation(operation, operation_name="unknown", trace_id=None, fallback=None):
        try:
            return operation()
        except Exception as e:
            logger.error("Redis operation failed", extra={
                "operation_name": operation_name,
                "trace_id": trace_id,
                "error": str(e),
                "service": "tts_cache_manager"
            })
            return fallback

try:
    from r2_client import get_r2_client, upload_to_r2
except ImportError:
    def get_r2_client():
        import boto3
        return boto3.client('s3',
            endpoint_url=os.getenv("R2_ENDPOINT_URL", "https://6970388a67efe7f4ca9feed97b7838b6.r2.cloudflarestorage.com"),
            aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID", ""),
            aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY", ""),
            region_name='auto'
        )
    
    def upload_to_r2(client, bucket, key, data, content_type='audio/mpeg', trace_id=None):
        try:
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
                ContentType=content_type
            )
            return True, {"key": key, "size": len(data)}
        except Exception as e:
            logger.error("R2 upload failed", extra={
                "bucket": bucket,
                "key": key,
                "trace_id": trace_id,
                "error": str(e),
                "service": "tts_cache_manager"
            })
            return False, None

# Phase 12: Metrics integration
try:
    import metrics_collector as metrics
except ImportError:
    class metrics:
        @staticmethod
        def increment_metric(*args, **kwargs): pass
        @staticmethod
        def observe_latency(*args, **kwargs): pass
        @staticmethod
        def set_gauge(*args, **kwargs): pass

# Phase 12: Structured logging
try:
    from logging_utils import get_logger
    logger = get_logger("tts_cache_manager")
except ImportError:
    import logging
    logger = logging.getLogger("tts_cache_manager")

# Phase 12: External API wrapper
try:
    from core.utils.external_api import external_api_call
except ImportError:
    def external_api_call(service, operation, *args, trace_id=None, **kwargs):
        return operation(*args, **kwargs)

# Phase 12: Celery integration for retry tasks
try:
    from celery_app import celery
except ImportError:
    celery = None

# Phase 12: Circuit breaker check
def _is_circuit_breaker_open(service: str = "tts_cache") -> bool:
    """Check if circuit breaker is open for TTS cache operations"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            return False
        
        return safe_redis_operation(
            lambda client: (
                (state := client.get(f"circuit_breaker:{service}:state")) and
                (state.decode("utf-8") if isinstance(state, bytes) else state).lower() == "open"
            ),
            operation_name="circuit_breaker_get",
            trace_id=None,
            fallback=False
        )
    except Exception:
        return False

# --------------------------------------------------------------------------
# Cache Entry Model
# --------------------------------------------------------------------------

class TTSCacheEntry:
    """Represents a cached TTS audio entry with Phase 12 resilience"""
    
    def __init__(self, content_hash: str, audio_data: bytes, metadata: Dict[str, Any]):
        self.content_hash = content_hash
        self.audio_data = audio_data
        self.metadata = metadata
        self.created_at = time.time()
        self.last_accessed = time.time()
        self.access_count = 1
        self.size_bytes = len(audio_data)
        
        # Set default metadata
        self.text = metadata.get("text", "")
        self.voice_settings = metadata.get("voice_settings", {})
        self.provider = metadata.get("provider", "unknown")
        self.content_type = metadata.get("content_type", "audio/mpeg")
        self.duration_ms = metadata.get("duration_ms", 0)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "content_hash": self.content_hash,
            "created_at": self.created_at,
            "last_accessed": self.last_accessed,
            "access_count": self.access_count,
            "size_bytes": self.size_bytes,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], audio_data: bytes) -> 'TTSCacheEntry':
        """Create from dictionary and audio data"""
        entry = cls(
            content_hash=data["content_hash"],
            audio_data=audio_data,
            metadata=data["metadata"]
        )
        entry.created_at = data["created_at"]
        entry.last_accessed = data["last_accessed"]
        entry.access_count = data["access_count"]
        entry.size_bytes = data["size_bytes"]
        return entry
    
    def mark_accessed(self):
        """Update access tracking"""
        self.last_accessed = time.time()
        self.access_count += 1
    
    def get_redis_key(self) -> str:
        """Get Redis key for this entry"""
        return f"{Config.TTS_CACHE_REDIS_PREFIX}:{self.content_hash}"
    
    def get_r2_key(self) -> str:
        """Get R2 object key for this entry"""
        # Use hash partitioning to avoid too many files in one directory
        hash_prefix = self.content_hash[:4]
        return f"{Config.TTS_CACHE_R2_PREFIX}/{hash_prefix}/{self.content_hash}.audio"
    
    def get_metadata_key(self) -> str:
        """Get Redis key for metadata only"""
        return f"{Config.TTS_CACHE_REDIS_PREFIX}:meta:{self.content_hash}"
    
    def is_expired(self) -> bool:
        """Check if entry has expired"""
        ttl_seconds = Config.TTS_CACHE_TTL_DAYS * 24 * 60 * 60
        return (time.time() - self.created_at) > ttl_seconds
    
    def get_age_days(self) -> float:
        """Get age of entry in days"""
        return (time.time() - self.created_at) / (24 * 60 * 60)

# --------------------------------------------------------------------------
# TTS Cache Manager with Phase 12 Resilience
# --------------------------------------------------------------------------

class TTSCacheManager:
    """
    Phase 12: Manages cached TTS audio snippets with Redis and R2 storage and full resilience patterns.
    Provides fast lookup for common phrases to minimize TTS latency.
    """
    
    def __init__(self):
        self.redis_client = get_redis_client()
        self.r2_client = get_r2_client()
        self.redis_available = self.redis_client is not None
        self.r2_available = self.r2_client is not None
        
        # Cache statistics
        self.hit_count = 0
        self.miss_count = 0
        self.total_lookup_time = 0.0
        
        logger.info("TTSCacheManager initialized", extra={
            "redis_available": self.redis_available,
            "r2_available": self.r2_available,
            "ttl_days": Config.TTS_CACHE_TTL_DAYS,
            "max_size_mb": Config.TTS_CACHE_MAX_SIZE_MB,
            "service": "tts_cache_manager"
        })
    
    def compute_content_hash(self, text: str, voice_settings: Dict[str, Any] = None, params: Dict[str, Any] = None) -> str:
        """
        Compute content hash for normalized text and voice settings with Phase 12 resilience.
        Ensures same hash for identical content regardless of formatting.
        Keying: Cache key = hash(text + voice_id + params)
        """
        try:
            # Normalize text (lowercase, strip whitespace, etc.)
            normalized_text = self._normalize_text(text)
            
            # Normalize voice settings and params
            normalized_settings = self._normalize_voice_settings(voice_settings or {})
            normalized_params = self._normalize_params(params or {})
            
            # Create hashable content - include text, voice_id, and params
            content_to_hash = {
                "text": normalized_text,
                "voice_settings": normalized_settings,
                "params": normalized_params
            }
            
            # Compute SHA-256 hash
            content_json = json.dumps(content_to_hash, sort_keys=True, separators=(',', ':'))
            content_hash = hashlib.sha256(content_json.encode('utf-8')).hexdigest()
            
            return content_hash
            
        except Exception as e:
            logger.error("Error computing content hash", extra={
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "tts_cache_manager"
            })
            # Fallback: hash the raw text with params
            fallback_content = text + str(voice_settings or {}) + str(params or {})
            return hashlib.sha256(fallback_content.encode('utf-8')).hexdigest()
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text for consistent hashing"""
        # Remove SSML tags if present but preserve content
        if text.strip().startswith('<speak>') and text.strip().endswith('</speak>'):
            # Basic SSML stripping - in production, use proper XML parser
            text = text.replace('<speak>', '').replace('</speak>', '')
            # Remove other SSML tags but keep text content
            import re
            text = re.sub(r'<[^>]+>', '', text)
        
        # Normalize whitespace and case
        normalized = ' '.join(text.split()).lower().strip()
        return normalized
    
    def _normalize_voice_settings(self, voice_settings: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize voice settings for consistent hashing"""
        normalized = {}
        
        # Include only relevant settings that affect TTS output
        relevant_keys = {
            'voice', 'model', 'language', 'speed', 'pitch', 
            'volume', 'emotion', 'style', 'provider', 'voice_id'
        }
        
        for key, value in voice_settings.items():
            if key.lower() in relevant_keys:
                # Convert to string for consistent hashing
                if isinstance(value, (int, float)):
                    normalized[key] = f"{value:.6f}"
                else:
                    normalized[key] = str(value).lower()
        
        return dict(sorted(normalized.items()))  # Sort for consistent ordering

    def _normalize_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize additional parameters for consistent hashing"""
        normalized = {}
        
        # Include parameters that affect TTS output
        relevant_keys = {
            'sample_rate', 'format', 'bitrate', 'channels'
        }
        
        for key, value in params.items():
            if key.lower() in relevant_keys:
                # Convert to string for consistent hashing
                if isinstance(value, (int, float)):
                    normalized[key] = f"{value:.6f}"
                else:
                    normalized[key] = str(value).lower()
        
        return dict(sorted(normalized.items()))  # Sort for consistent ordering
    
    async def get_cached_audio(self, text: str, voice_settings: Dict[str, Any] = None, params: Dict[str, Any] = None) -> Optional[bytes]:
        """
        Check cache for existing TTS audio with Phase 12 resilience.
        Returns: Audio bytes if cache hit, None if miss
        """
        start_time = time.time()
        
        # Phase 12: Circuit breaker check
        if _is_circuit_breaker_open("tts_cache"):
            logger.warning("TTS cache access blocked by circuit breaker", extra={
                "service": "tts_cache_manager"
            })
            metrics.increment_metric("tts_cache_circuit_breaker_hits_total")
            return None
        
        try:
            content_hash = self.compute_content_hash(text, voice_settings, params)
            trace_id = f"get_{content_hash[:8]}"
            
            # Try Redis first (fastest)
            audio_data = await self._get_from_redis(content_hash, trace_id)
            if audio_data:
                self._record_hit(start_time, content_hash, "redis")
                return audio_data
            
            # Try R2 if Redis miss
            audio_data = await self._get_from_r2(content_hash)
            if audio_data:
                # Cache in Redis for future fast access using SETNX to prevent thundering herd
                await self._store_in_redis_with_dedupe(content_hash, audio_data, text, voice_settings, params, trace_id=trace_id)
                self._record_hit(start_time, content_hash, "r2")
                return audio_data
            
            # Cache miss
            self._record_miss(start_time, content_hash)
            return None
            
        except Exception as e:
            latency_seconds = (time.time() - start_time)
            metrics.increment_metric("tts_cache_errors_total")
            logger.error("Error getting cached audio", extra={
                "content_hash": content_hash[:16] if 'content_hash' in locals() else "unknown",
                "error": str(e),
                "latency_seconds": latency_seconds,
                "stack": traceback.format_exc(),
                "service": "tts_cache_manager"
            })
            return None
    
    async def cache_audio(self, text: str, audio_data: bytes, 
                         voice_settings: Dict[str, Any] = None,
                         params: Dict[str, Any] = None,
                         provider: str = "unknown") -> Tuple[bool, Optional[str]]:
        """
        Cache TTS audio data for future use with Phase 12 resilience.
        Returns: (success: bool, r2_key: Optional[str])
        """
        # Phase 12: Circuit breaker check
        if _is_circuit_breaker_open("tts_cache"):
            logger.warning("TTS cache storage blocked by circuit breaker", extra={
                "service": "tts_cache_manager"
            })
            metrics.increment_metric("tts_cache_circuit_breaker_hits_total")
            return False, None
        
        try:
            if not audio_data or len(audio_data) == 0:
                logger.warning("Attempted to cache empty audio data", extra={
                    "service": "tts_cache_manager"
                })
                return False, None
            
            content_hash = self.compute_content_hash(text, voice_settings, params)
            trace_id = f"cache_{content_hash[:8]}"
            
            # Prepare metadata
            metadata = {
                "text": text,
                "voice_settings": voice_settings or {},
                "params": params or {},
                "provider": provider,
                "content_type": "audio/mpeg",  # Adjust based on actual format
                "duration_ms": self._estimate_duration_ms(audio_data),
                "size_bytes": len(audio_data)
            }
            
            # Store in both Redis and R2 for redundancy with deduplication
            redis_success = await self._store_in_redis_with_dedupe(content_hash, audio_data, text, voice_settings, params, metadata, trace_id=trace_id)
            r2_success, r2_key = await self._store_in_r2_with_upload_tracking(content_hash, audio_data, metadata)
            
            # Consider successful if at least one storage succeeded
            success = redis_success or r2_success
            
            if success:
                metrics.increment_metric("tts_cache_writes_total")
                logger.debug("TTS audio cached successfully", extra={
                    "content_hash": content_hash[:16],
                    "size_bytes": len(audio_data),
                    "redis_success": redis_success,
                    "r2_success": r2_success,
                    "r2_key": r2_key,
                    "service": "tts_cache_manager"
                })
            else:
                metrics.increment_metric("tts_cache_store_errors_total")
                logger.error("Failed to cache TTS audio", extra={
                    "content_hash": content_hash[:16],
                    "service": "tts_cache_manager"
                })
            
            return success, r2_key
            
        except Exception as e:
            metrics.increment_metric("tts_cache_store_errors_total")
            logger.error("Error caching audio", extra={
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "tts_cache_manager"
            })
            return False, None

    async def _store_in_redis_with_dedupe(self, content_hash: str, audio_data: bytes, 
                                        text: str, voice_settings: Dict[str, Any],
                                        params: Dict[str, Any] = None,
                                        metadata: Dict[str, Any] = None,
                                        trace_id: str = None) -> bool:
        """
        Store audio data in Redis with SETNX deduplication to prevent thundering herd.
        """
        if not self.redis_client:
            return False
        
        try:
            redis_key = f"{Config.TTS_CACHE_REDIS_PREFIX}:{content_hash}"
            meta_key = f"{Config.TTS_CACHE_REDIS_PREFIX}:meta:{content_hash}"
            
            # Encode audio data as base64 for Redis
            audio_data_b64 = base64.b64encode(audio_data).decode('utf-8')
            
            # Prepare metadata
            if not metadata:
                metadata = {
                    "text": text,
                    "voice_settings": voice_settings or {},
                    "params": params or {},
                    "size_bytes": len(audio_data),
                    "created_at": time.time()
                }
            
            meta_data = {
                "content_hash": content_hash,
                "created_at": time.time(),
                "last_accessed": time.time(),
                "access_count": 1,
                "size_bytes": len(audio_data),
                "metadata": metadata
            }
            
            # Store with TTL and deduplication via SET with NX and EX
            ttl_seconds = Config.TTS_CACHE_TTL_DAYS * 24 * 60 * 60
            
            return safe_redis_operation(
                lambda client: (
                    (set_success := client.set(redis_key, audio_data_b64, ex=ttl_seconds, nx=True))
                    and client.setex(meta_key, ttl_seconds, json.dumps(meta_data))
                    and (logger.debug(
                        "SET(NX,EX) cache write successful",
                        extra={"content_hash": content_hash[:16], "service": "tts_cache_manager"}
                    ) or True)
                ) or (
                    (logger.debug(
                        "SET(NX,EX) cache write skipped - key exists",
                        extra={"content_hash": content_hash[:16], "service": "tts_cache_manager"}
                    ) or True)
                    and (client.get(redis_key) is not None)
                ),
                operation_name="redis_setnx_dedupe",
                trace_id=trace_id,
                fallback=False
            )
            
        except Exception as e:
            logger.debug("Redis cache store with dedupe failed", extra={
                "content_hash": content_hash[:16],
                "error": str(e),
                "service": "tts_cache_manager"
            })
            return False

    async def _store_in_r2_with_upload_tracking(self, content_hash: str, audio_data: bytes, 
                                              metadata: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Store audio data in R2 with upload state tracking and retry mechanisms.
        Idempotent R2 upload: If object exists, return existing URL/key; uploads retried safely; never duplicate keys.
        Returns: (success: bool, r2_key: Optional[str])
        """
        if not self.r2_client:
            return False, None

        trace_id = f"tts_upload_{content_hash[:16]}_{int(time.time())}"
        r2_key = self._get_r2_key_from_hash(content_hash)

        try:
            # Check if already uploaded to prevent duplicates
            if self._is_r2_upload_complete(content_hash, trace_id):
                logger.debug("R2 upload already complete, skipping", extra={
                    "content_hash": content_hash[:16],
                    "r2_key": r2_key,
                    "trace_id": trace_id,
                    "service": "tts_cache_manager"
                })
                return True, r2_key

            # Check if object already exists in R2 (idempotency check)
            r2_exists = await self._r2_object_exists(r2_key)
            if r2_exists:
                logger.debug("R2 object already exists, marking as complete", extra={
                    "content_hash": content_hash[:16],
                    "r2_key": r2_key,
                    "trace_id": trace_id,
                    "service": "tts_cache_manager"
                })
                self._set_upload_status(content_hash, "complete", r2_key, trace_id)
                return True, r2_key

            # Mark upload as pending in Redis
            self._set_upload_status(content_hash, "pending", r2_key, trace_id)

            # Get R2 bucket from config
            try:
                from config import Config
                bucket = Config.R2_BUCKET_NAME
            except Exception:
                bucket = os.getenv("R2_BUCKET_NAME", "sara-ai-audio")

            if not bucket:
                logger.error("R2_BUCKET_NAME not configured", extra={
                    "content_hash": content_hash[:16],
                    "trace_id": trace_id,
                    "service": "tts_cache_manager"
                })
                return False, None

            # Upload to R2
            success, upload_metadata = upload_to_r2(
                self.r2_client, 
                bucket, 
                r2_key, 
                audio_data,
                content_type=metadata.get('content_type', 'audio/mpeg'),
                trace_id=trace_id
            )

            if success:
                # Mark upload as complete
                self._set_upload_status(content_hash, "complete", r2_key, trace_id)
                metrics.increment_metric("tts.cache.upload_success.count")
                
                logger.info("TTS audio uploaded to R2 successfully", extra={
                    "event": "tts_upload",
                    "status": "complete",
                    "key": r2_key,
                    "trace_id": trace_id,
                    "content_hash": content_hash[:16],
                    "size_bytes": len(audio_data),
                    "service": "tts_cache_manager"
                })
                return True, r2_key
            else:
                # Mark upload as pending for retry
                self._set_upload_status(content_hash, "pending", r2_key, trace_id)
                metrics.increment_metric("tts.cache.upload_failure.count")
                
                # Enqueue retry task
                self._enqueue_retry_task(content_hash, audio_data, metadata, r2_key, trace_id)
                
                logger.error("TTS audio upload failed, enqueued for retry", extra={
                    "event": "tts_upload", 
                    "status": "failed",
                    "key": r2_key,
                    "trace_id": trace_id,
                    "content_hash": content_hash[:16],
                    "service": "tts_cache_manager"
                })
                return False, r2_key

        except Exception as e:
            # Mark upload as pending for retry on any exception
            self._set_upload_status(content_hash, "pending", r2_key, trace_id)
            metrics.increment_metric("tts.cache.upload_failure.count")
            
            # Enqueue retry task
            self._enqueue_retry_task(content_hash, audio_data, metadata, r2_key, trace_id)
            
            logger.error("TTS audio upload exception, enqueued for retry", extra={
                "event": "tts_upload",
                "status": "failed", 
                "key": r2_key,
                "trace_id": trace_id,
                "content_hash": content_hash[:16],
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "tts_cache_manager"
            })
            return False, r2_key

    async def _r2_object_exists(self, r2_key: str) -> bool:
        """Check if R2 object already exists for idempotent upload"""
        if not self.r2_client:
            return False
            
        try:
            from config import Config
            bucket = Config.R2_BUCKET_NAME
            
            if not bucket:
                logger.error("R2_BUCKET_NAME not configured", extra={
                    "r2_key": r2_key,
                    "service": "tts_cache_manager"
                })
                return False

            # Try to get object metadata
            self.r2_client.head_object(
                Bucket=bucket,
                Key=r2_key
            )
            return True
        except Exception as e:
            # Only treat 404/NoSuchKey as "not exists"
            if "404" in str(e) or "NoSuchKey" in str(e) or "Not Found" in str(e):
                return False
            # For other errors, log and return False (safe fallback)
            logger.error("Error checking R2 object existence", extra={
                "r2_key": r2_key,
                "error": str(e),
                "service": "tts_cache_manager"
            })
            return False

    def _is_r2_upload_complete(self, content_hash: str, trace_id: str = None) -> bool:
        """Check if R2 upload is already complete to prevent duplicates."""
        if not self.redis_client:
            return False
            
        try:
            return safe_redis_operation(
                lambda client: (
                    (upload_status := client.hget(f"{Config.TTS_CACHE_REDIS_PREFIX}:upload:{content_hash}", "upload_status")) and
                    (upload_status.decode("utf-8") if isinstance(upload_status, bytes) else upload_status) == "complete"
                ),
                operation_name="r2_upload_status_check",
                trace_id=trace_id,
                fallback=False
            )
        except Exception as e:
            logger.debug("Error checking R2 upload status", extra={
                "content_hash": content_hash[:16],
                "error": str(e),
                "service": "tts_cache_manager"
            })
            return False

    def _set_upload_status(self, content_hash: str, status: str, r2_key: str = None, trace_id: str = None):
        """Set upload status in Redis with TTL and proper namespace."""
        if not self.redis_client:
            return
            
        try:
            def _op(client):
                redis_key = f"{Config.TTS_CACHE_REDIS_PREFIX}:upload:{content_hash}"
                mapping = {
                    "upload_status": status,
                    "updated_at": datetime.utcnow().isoformat(),
                }
                if status == "pending":
                    mapping["created_at"] = datetime.utcnow().isoformat()
                    metrics.increment_metric("tts.cache.upload_pending.count")
                if status == "complete" and r2_key:
                    mapping["r2_key"] = r2_key
                client.hset(redis_key, mapping=mapping)
                client.expire(redis_key, 3 * 24 * 60 * 60)
                return True

            safe_redis_operation(
                _op,
                operation_name="set_upload_status",
                trace_id=trace_id,
                fallback=False
            )
            
        except Exception as e:
            logger.error("Error setting upload status", extra={
                "content_hash": content_hash[:16],
                "status": status,
                "error": str(e),
                "service": "tts_cache_manager"
            })

    def _enqueue_retry_task(self, content_hash: str, audio_data: bytes, 
                           metadata: Dict[str, Any], r2_key: str, trace_id: str):
        """Enqueue Celery task for retrying failed uploads."""
        if not celery:
            logger.warning("Celery app not available, cannot enqueue retry task", extra={
                "content_hash": content_hash[:16],
                "trace_id": trace_id,
                "service": "tts_cache_manager"
            })
            return
            
        try:
            # For retry tasks, we might store the file path or regenerate
            # For now, we'll pass the content hash and let the retry task regenerate
            celery.send_task(
                "retry_r2_upload", 
                args=[content_hash, r2_key, metadata],
                kwargs={"trace_id": trace_id}
            )
            
            logger.info("Retry task enqueued for failed upload", extra={
                "content_hash": content_hash[:16],
                "r2_key": r2_key,
                "trace_id": trace_id,
                "service": "tts_cache_manager"
            })
            
        except Exception as e:
            logger.error("Failed to enqueue retry task", extra={
                "content_hash": content_hash[:16],
                "r2_key": r2_key,
                "trace_id": trace_id,
                "error": str(e),
                "service": "tts_cache_manager"
            })
    
    async def _get_from_redis(self, content_hash: str, trace_id: str = None) -> Optional[bytes]:
        """Get cached audio from Redis with Phase 12 resilience"""
        if not self.redis_client:
            return None

        try:
            def op(client):
                value = client.get(f"{Config.TTS_CACHE_REDIS_PREFIX}:{content_hash}")
                if not value:
                    return None
                # decode base64 → bytes
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                audio_bytes = base64.b64decode(value)
                # touch metadata (access count / last_accessed) using same client
                self._update_redis_metadata_sync(
                    client, f"{Config.TTS_CACHE_REDIS_PREFIX}:meta:{content_hash}"
                )
                return audio_bytes

            return safe_redis_operation(
                op,
                operation_name="redis_get_audio",
                trace_id=trace_id,
                fallback=None,
            )
        except Exception as e:
            logger.debug("Redis cache get failed", extra={
                "content_hash": content_hash[:16],
                "error": str(e),
                "service": "tts_cache_manager"
            })
            return None

    def _update_redis_metadata_sync(self, client, meta_key: str):
        """Update access metadata in Redis (sync version for use in safe_redis_operation)"""
        try:
            existing_meta = client.get(meta_key)
            if existing_meta:
                if isinstance(existing_meta, bytes):
                    existing_meta = existing_meta.decode('utf-8')
                meta_data = json.loads(existing_meta)
                meta_data["last_accessed"] = time.time()
                meta_data["access_count"] = meta_data.get("access_count", 0) + 1
                
                # Update with same TTL
                ttl = client.ttl(meta_key)
                if ttl > 0:
                    client.setex(meta_key, ttl, json.dumps(meta_data))
                    
        except Exception as e:
            logger.debug("Failed to update Redis metadata", extra={
                "error": str(e),
                "service": "tts_cache_manager"
            })
    
    async def _update_redis_metadata(self, meta_key: str, trace_id: str = None):
        """Update access metadata in Redis with Phase 12 resilience"""
        if not self.redis_client:
            return
        
        safe_redis_operation(
            lambda client: self._update_redis_metadata_sync(client, meta_key),
            operation_name="update_redis_metadata",
            trace_id=trace_id,
            fallback=None
        )
    
    async def _get_from_r2(self, content_hash: str) -> Optional[bytes]:
        """Get cached audio from R2 with Phase 12 resilience"""
        if not self.r2_client:
            return None
        
        try:
            r2_key = self._get_r2_key_from_hash(content_hash)
            
            # Use external API wrapper for R2 operations (sync call, no await)
            def get_r2_object():
                return self.r2_client.get_object(
                    Bucket=Config.R2_BUCKET_NAME,
                    Key=r2_key
                )
            
            response = external_api_call(
                "r2_cache_get",
                get_r2_object,
                trace_id=None
            )
            
            if 'Body' in response:
                audio_data = response['Body'].read()
                
                # Update access time in Redis if available
                if self.redis_client:
                    meta_key = f"{Config.TTS_CACHE_REDIS_PREFIX}:meta:{content_hash}"
                    await self._update_redis_metadata(meta_key)
                
                return audio_data
            else:
                return None
                
        except Exception as e:
            if "NoSuchKey" not in str(e) and "404" not in str(e) and "Not Found" not in str(e):
                logger.debug("R2 cache get failed", extra={
                    "content_hash": content_hash[:16],
                    "error": str(e),
                    "service": "tts_cache_manager"
                })
            return None
    
    async def _store_in_r2(self, content_hash: str, audio_data: bytes, 
                          metadata: Dict[str, Any]) -> bool:
        """Store audio data in R2 with Phase 12 resilience"""
        if not self.r2_client:
            return False
        
        try:
            r2_key = self._get_r2_key_from_hash(content_hash)
            
            # Use external API wrapper for R2 operations (sync call, no await)
            def put_r2_object():
                # Store audio data
                self.r2_client.put_object(
                    Bucket=Config.R2_BUCKET_NAME,
                    Key=r2_key,
                    Body=audio_data,
                    ContentType=metadata.get('content_type', 'audio/mpeg')
                )
                
                # Store metadata separately
                meta_key = f"{r2_key}.meta"
                self.r2_client.put_object(
                    Bucket=Config.R2_BUCKET_NAME,
                    Key=meta_key,
                    Body=json.dumps(metadata),
                    ContentType='application/json'
                )
                return True
            
            return external_api_call(
                "r2_cache_store",
                put_r2_object,
                trace_id=None
            )
            
        except Exception as e:
            logger.debug("R2 cache store failed", extra={
                "content_hash": content_hash[:16],
                "error": str(e),
                "service": "tts_cache_manager"
            })
            return False
    
    def _get_r2_key_from_hash(self, content_hash: str) -> str:
        """Get R2 key from content hash"""
        hash_prefix = content_hash[:4]
        return f"{Config.TTS_CACHE_R2_PREFIX}/{hash_prefix}/{content_hash}.audio"
    
    def _estimate_duration_ms(self, audio_data: bytes) -> int:
        """Estimate audio duration in milliseconds (placeholder implementation)"""
        # This is a simple estimation - in production, use proper audio analysis
        # Assuming 8kHz, 16-bit mono audio
        try:
            # Very rough estimate: 1KB ≈ 62.5ms for 8kHz 16-bit audio
            estimated_ms = len(audio_data) * 62.5 // 1024
            logger.debug("Audio duration estimated", extra={
                "size_bytes": len(audio_data),
                "estimated_ms": estimated_ms,
                "duration_estimated": True,
                "service": "tts_cache_manager"
            })
            return int(estimated_ms)
        except:
            return 0
    
    def _record_hit(self, start_time: float, content_hash: str, source: str):
        """Record cache hit metrics"""
        latency_seconds = (time.time() - start_time)
        
        self.hit_count += 1
        self.total_lookup_time += latency_seconds
        
        metrics.increment_metric("tts_cache_hits_total", labels={"source": source})
        metrics.observe_latency("tts_cache_lookup_latency_seconds", latency_seconds)
        
        logger.debug("TTS cache hit", extra={
            "content_hash": content_hash[:16],
            "source": source,
            "latency_seconds": latency_seconds,
            "service": "tts_cache_manager"
        })
    
    def _record_miss(self, start_time: float, content_hash: str):
        """Record cache miss metrics"""
        latency_seconds = (time.time() - start_time)
        
        self.miss_count += 1
        self.total_lookup_time += latency_seconds        
        metrics.increment_metric("tts_cache_miss_total")
        metrics.observe_latency("tts_cache_lookup_latency_seconds", latency_seconds)
        
        logger.debug("TTS cache miss", extra={
            "content_hash": content_hash[:16],
            "latency_seconds": latency_seconds,
            "service": "tts_cache_manager"
        })
    
    async def cleanup_expired_entries(self) -> Dict[str, int]:
        """
        Clean up expired cache entries with Phase 12 resilience.
        Returns: Count of cleaned entries by storage type
        """
        # Phase 12: Circuit breaker check
        if _is_circuit_breaker_open("tts_cache_cleanup"):
            logger.warning("TTS cache cleanup blocked by circuit breaker", extra={
                "service": "tts_cache_manager"
            })
            metrics.increment_metric("tts_cache_cleanup_circuit_breaker_hits_total")
            return {
                "redis_cleaned": 0,
                "r2_cleaned": 0,
                "errors": 1,
                "reason": "circuit_breaker_open"
            }
        
        results = {
            "redis_cleaned": 0,
            "r2_cleaned": 0,
            "errors": 0
        }
        
        try:
            # Clean Redis entries
            if self.redis_client:
                results["redis_cleaned"] = await self._cleanup_redis_entries()
            
            # Clean R2 entries (this would typically be done separately due to cost)
            # R2 cleanup might be done via lifecycle policies instead
            
            logger.info("TTS cache cleanup completed", extra={
                **results,
                "service": "tts_cache_manager"
            })
            metrics.increment_metric("tts_cache_cleanup_runs_total")
            
        except Exception as e:
            results["errors"] += 1
            logger.error("Error during TTS cache cleanup", extra={
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "tts_cache_manager"
            })
        
        return results
    
    async def _cleanup_redis_entries(self) -> int:
        """Clean up expired Redis entries with Phase 12 resilience"""
        if not self.redis_client:
            return 0
        
        try:
            # Redis will automatically expire entries based on TTL
            # This method is for manual cleanup if needed
            pattern = f"{Config.TTS_CACHE_REDIS_PREFIX}:*"
            
            # Note: SCAN would be used in production for large datasets
            # This is a simplified implementation
            cleaned_count = 0
            
            # We rely on Redis TTL for automatic cleanup
            # Manual cleanup could be added here if needed
            
            return cleaned_count
            
        except Exception as e:
            logger.error("Error cleaning Redis cache entries", extra={
                "error": str(e),
                "service": "tts_cache_manager"
            })
            return 0
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics with Phase 12 resilience information"""
        total_lookups = self.hit_count + self.miss_count
        hit_rate = self.hit_count / total_lookups if total_lookups > 0 else 0
        avg_latency = self.total_lookup_time / total_lookups if total_lookups > 0 else 0
        
        circuit_breaker_open = _is_circuit_breaker_open("tts_cache")
        
        return {
            "hit_count": self.hit_count,
            "miss_count": self.miss_count,
            "hit_rate": hit_rate,
            "total_lookups": total_lookups,
            "avg_lookup_latency_seconds": avg_latency,
            "redis_available": self.redis_available,
            "r2_available": self.r2_available,
            "circuit_breaker_open": circuit_breaker_open
        }
    
    async def preload_common_phrases(self, phrases: List[Tuple[str, Dict[str, Any]]]):
        """
        Preload cache with common phrases to improve initial performance with Phase 12 resilience.
        """
        logger.info(f"Preloading {len(phrases)} common phrases into TTS cache", extra={
            "service": "tts_cache_manager"
        })
        
        preloaded_count = 0
        for text, voice_settings in phrases:
            # Check if already cached
            existing = await self.get_cached_audio(text, voice_settings)
            if not existing:
                # In production, this would trigger TTS generation
                # For now, we just log the miss
                logger.debug("Common phrase not cached", extra={
                    "text": text[:100],
                    "service": "tts_cache_manager"
                })
            else:
                preloaded_count += 1
        
        logger.info("Common phrases preload completed", extra={
            "total_phrases": len(phrases),
            "already_cached": preloaded_count,
            "service": "tts_cache_manager"
        })


# --------------------------------------------------------------------------
# Global Cache Manager Instance with Phase 12 Resilience
# --------------------------------------------------------------------------

# Global cache manager instance for shared use
_cache_manager: Optional[TTSCacheManager] = None

def get_tts_cache_manager() -> TTSCacheManager:
    """Get or create global TTS cache manager instance with Phase 12 resilience"""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = TTSCacheManager()
    return _cache_manager


# --------------------------------------------------------------------------
# Helper Functions with Phase 12 Resilience
# --------------------------------------------------------------------------

async def get_cached_audio(text: str, voice_settings: Dict[str, Any] = None, params: Dict[str, Any] = None) -> Optional[bytes]:
    """Helper function to get cached audio using global manager with Phase 12 resilience"""
    manager = get_tts_cache_manager()
    return await manager.get_cached_audio(text, voice_settings, params)

async def cache_audio(text: str, audio_data: bytes, 
                     voice_settings: Dict[str, Any] = None,
                     params: Dict[str, Any] = None,
                     provider: str = "unknown") -> Tuple[bool, Optional[str]]:
    """Helper function to cache audio using global manager with Phase 12 resilience"""
    manager = get_tts_cache_manager()
    return await manager.cache_audio(text, audio_data, voice_settings, params, provider)

async def get_cache_stats() -> Dict[str, Any]:
    """Helper function to get cache statistics with Phase 12 resilience"""
    manager = get_tts_cache_manager()
    return await manager.get_cache_stats()