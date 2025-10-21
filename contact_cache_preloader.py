"""
contact_cache_preloader.py
Preload Redis cache entries for a contact using existing knowledge JSON files.
This speeds up per-call startup by pre-populating keys used by the TTS/inference pipeline.
"""
import json
import os
import uuid

# Centralized configuration
try:
    from config import Config
    KNOWLEDGE_DIR = getattr(Config, "KNOWLEDGE_DIR", ".")
    REDIS_URL = getattr(Config, "REDIS_URL", "redis://localhost:6379/0")
except ImportError:
    # Minimal fallbacks
    KNOWLEDGE_DIR = "."
    REDIS_URL = "redis://localhost:6379/0"

# Structured logging with lazy shim
def _get_logger():
    try:
        from logging_utils import log_event
        return log_event
    except Exception:
        def _noop_log(*a, **k): pass
        return _noop_log

log_event = _get_logger()

# Trace ID with fallback
def get_trace_id():
    try:
        from logging_utils import get_trace_id as _get_trace_id
        return _get_trace_id()
    except Exception:
        return str(uuid.uuid4())[:8]

# Metrics with lazy loading
def _get_metrics():
    try:
        from metrics_collector import increment_metric, observe_latency
        return increment_metric, observe_latency
    except ImportError:
        def _noop_metric(*args, **kwargs): pass
        def _noop_latency(*args, **kwargs): pass
        return _noop_metric, _noop_latency

def get_redis():
    try:
        # CORRECTED: Use get_client() instead of get_redis_client()
        from redis_client import get_client, safe_redis_operation
        return get_client(), safe_redis_operation
    except Exception:
        try:
            import redis
            return redis.from_url(REDIS_URL, decode_responses=True), None
        except Exception:
            return None, None

def build_contact_snapshot(contact: dict, knowledge_files: list = None):
    trace_id = get_trace_id()
    snapshot = {"contact": contact, "knowledge": {}}
    
    # Default known filenames
    if knowledge_files is None:
        knowledge_files = [
            "Sara_CallFlow.json", "Sara_KnowledgeBase.json", "Sara_Objections.json",
            "Sara_Opening.json", "Sara_Playbook.json", "Sara_SystemPrompt_Production.json"
        ]
    
    files_loaded = 0
    files_failed = 0
    
    for f in knowledge_files:
        p = os.path.join(KNOWLEDGE_DIR, f)
        try:
            with open(p, "r", encoding="utf-8") as fh:
                snapshot["knowledge"][f] = json.load(fh)
            files_loaded += 1
        except Exception as e:
            files_failed += 1
            log_event("contact_cache_preloader", "knowledge_file_missing", "debug",
                      f"Knowledge file missing or unreadable: {p} -> {e}",
                      file_path=p, error=str(e), trace_id=trace_id)
            snapshot["knowledge"][f] = None
    
    # Metrics - lazy loaded
    increment_metric, _ = _get_metrics()
    if files_loaded > 0:
        increment_metric("contact_cache.knowledge_files_loaded", files_loaded)
    if files_failed > 0:
        increment_metric("contact_cache.knowledge_files_failed", files_failed)
    
    log_event("contact_cache_preloader", "snapshot_built", "info",
              f"Built contact snapshot with {files_loaded} knowledge files ({files_failed} failed)",
              contact_phone=contact.get("phone"), contact_name=contact.get("name"),
              files_loaded=files_loaded, files_failed=files_failed, trace_id=trace_id)
    
    return snapshot

def preload_contact(contact: dict, knowledge_files: list = None, redis_prefix: str = "contact_snapshot:"):
    trace_id = get_trace_id()
    import time
    start_time = time.time()
    
    redis_client, safe_redis_operation = get_redis()
    if not redis_client:
        log_event("contact_cache_preloader", "redis_unavailable", "warning",
                  "Redis client not available; skipping preload",
                  contact_phone=contact.get("phone"), contact_name=contact.get("name"), 
                  trace_id=trace_id)
        return False
    
    key = redis_prefix + (contact.get("phone") or contact.get("name") or "unknown")
    
    # Build snapshot first
    snapshot = build_contact_snapshot(contact, knowledge_files)
    
    # Store in Redis with safe operation wrapper if available
    try:
        if safe_redis_operation:
            def _redis_set_operation(client):
                return client.set(key, json.dumps(snapshot), ex=60*60*24)  # 24h TTL
            
            result = safe_redis_operation(
                _redis_set_operation,
                fallback=False,
                operation_name="contact_cache_preload"
            )
        else:
            # Fallback to direct Redis operation
            result = redis_client.set(key, json.dumps(snapshot), ex=60*60*24)
        
        if result:
            # Metrics - lazy loaded
            increment_metric, observe_latency = _get_metrics()
            increment_metric("contact_cache.preloaded")
            
            latency_ms = (time.time() - start_time) * 1000
            observe_latency("contact_cache.preload_latency", latency_ms)
            
            log_event("contact_cache_preloader", "cache_preloaded", "info",
                      f"Preloaded cache key {key}",
                      cache_key=key, contact_phone=contact.get("phone"), 
                      contact_name=contact.get("name"), latency_ms=latency_ms, 
                      trace_id=trace_id)
            return True
        else:
            # Metrics - lazy loaded
            increment_metric, _ = _get_metrics()
            increment_metric("contact_cache.preload_failed")
            
            log_event("contact_cache_preloader", "cache_preload_failed", "error",
                      f"Failed to preload cache key {key}",
                      cache_key=key, contact_phone=contact.get("phone"), 
                      contact_name=contact.get("name"), trace_id=trace_id)
            return False
            
    except Exception as e:
        # Metrics - lazy loaded
        increment_metric, _ = _get_metrics()
        increment_metric("contact_cache.preload_exception")
        
        log_event("contact_cache_preloader", "cache_preload_exception", "error",
                  f"Exception during cache preload: {e}",
                  cache_key=key, contact_phone=contact.get("phone"), 
                  contact_name=contact.get("name"), error=str(e), trace_id=trace_id)
        return False