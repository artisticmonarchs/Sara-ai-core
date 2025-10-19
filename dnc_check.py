"""
dnc_check.py
Simple suppression (Do-Not-Call) list checker.
By default it uses Redis set 'dnc_numbers' if redis_client.get_redis_client exists.
Fallback: loads from a local file specified by env var DNC_FILE (one number per line).
"""
import os
import logging

logger = logging.getLogger("dnc_check")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

def _get_redis():
    try:
        from redis_client import get_redis_client
        return get_redis_client()
    except Exception:
        return None

def is_suppressed(phone: str) -> bool:
    phone = (phone or "").strip()
    r = _get_redis()
    if r:
        try:
            return r.sismember("dnc_numbers", phone)
        except Exception:
            logger.exception("Redis DNC check failed")
            # fallback to file
    dnc_file = os.environ.get("DNC_FILE", "")
    if dnc_file and os.path.exists(dnc_file):
        try:
            with open(dnc_file, "r", encoding="utf-8") as fh:
                nums = set(line.strip() for line in fh if line.strip())
            return phone in nums
        except Exception:
            logger.exception("Failed to read DNC file")
    return False

def add_to_dnc(phone: str):
    r = _get_redis()
    if r:
        try:
            r.sadd("dnc_numbers", phone)
            return True
        except Exception:
            logger.exception("Failed to add to redis dnc")
    # fallback: append to DNC file
    dnc_file = os.environ.get("DNC_FILE", "dnc_list.txt")
    with open(dnc_file, "a", encoding="utf-8") as fh:
        fh.write(phone + "\\n")
    return True
