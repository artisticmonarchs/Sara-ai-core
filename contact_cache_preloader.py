"""
contact_cache_preloader.py
Preload Redis cache entries for a contact using existing knowledge JSON files.
This speeds up per-call startup by pre-populating keys used by the TTS/inference pipeline.
"""
import json
import os
import logging

logger = logging.getLogger("contact_cache_preloader")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

def get_redis():
    try:
        from redis_client import get_redis_client
        return get_redis_client()
    except Exception:
        try:
            import redis, os
            REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
            return redis.from_url(REDIS_URL, decode_responses=True)
        except Exception:
            return None

def build_contact_snapshot(contact: dict, knowledge_files: list = None):
    snapshot = {"contact": contact, "knowledge": {}}
    knowledge_dir = os.environ.get("KNOWLEDGE_DIR", ".")
    if knowledge_files is None:
        # default known filenames
        knowledge_files = [
            "Sara_CallFlow.json","Sara_KnowledgeBase.json","Sara_Objections.json",
            "Sara_Opening.json","Sara_Playbook.json","Sara_SystemPrompt_Production.json"
        ]
    for f in knowledge_files:
        p = os.path.join(knowledge_dir, f)
        try:
            with open(p, "r", encoding="utf-8") as fh:
                snapshot["knowledge"][f] = json.load(fh)
        except Exception as e:
            logger.debug("Knowledge file missing or unreadable: %s -> %s", p, e)
            snapshot["knowledge"][f] = None
    return snapshot

def preload_contact(contact: dict, knowledge_files: list = None, redis_prefix="contact_snapshot:"):
    r = get_redis()
    if not r:
        logger.warning("Redis client not available; skipping preload")
        return False
    key = redis_prefix + (contact.get("phone") or contact.get("name") or "unknown")
    snapshot = build_contact_snapshot(contact, knowledge_files)
    try:
        r.set(key, json.dumps(snapshot), ex=60*60*24)  # 24h TTL
        logger.info("Preloaded cache key %s", key)
        return True
    except Exception as e:
        logger.exception("Failed to preload cache: %s", e)
        return False
