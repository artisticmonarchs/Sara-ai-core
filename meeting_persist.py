#!/usr/bin/env python3
"""
meeting_persist.py
Persist booking/meeting artifacts to R2 (preferred) or local filesystem as a fallback.
Provides a single function persist_booking(session_id, contact, booking_data) -> dict with keys {status, r2_url, path}

This file expects an r2_client module with an R2Client class exposing upload_bytes(key, bytes) or upload_file(path).
"""
import json
import time
import os
import logging

logger = logging.getLogger("meeting_persist")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

def _safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in name)[:160]

def persist_booking(session_id: str, contact: dict, booking_data: dict, r2_client=None, bucket_prefix="bookings"):
    """
    booking_data should include at least: {email, datetime, notes, meeting_link (optional)}
    contact is the contact row dict
    """
    timestamp = int(time.time())
    filename = f"{_safe_filename(contact.get('name','unknown'))}_{contact.get('phone','unknown')}_{timestamp}.json"
    payload = {
        "session_id": session_id,
        "contact": contact,
        "booking": booking_data,
        "created_at": time.time()
    }
    payload_bytes = json.dumps(payload, indent=2).encode("utf-8")

    # Try R2 client if provided or available
    if r2_client is None:
        try:
            from r2_client import R2Client
            r2_client = R2Client()
        except Exception:
            r2_client = None

    key = f"{bucket_prefix}/{filename}"
    # Attempt R2 upload
    if r2_client:
        try:
            # Prefer upload_bytes if available
            if hasattr(r2_client, "upload_bytes"):
                url = r2_client.upload_bytes(key, payload_bytes)
            elif hasattr(r2_client, "upload_file"):
                tmp_path = os.path.join("/tmp", filename)
                with open(tmp_path, "wb") as fh:
                    fh.write(payload_bytes)
                url = r2_client.upload_file(tmp_path, key)
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            else:
                raise RuntimeError("R2 client missing upload API")
            logger.info("Persisted booking to R2: %s", url)
            return {"status": "ok", "r2_url": url, "key": key}
        except Exception as e:
            logger.exception("R2 upload failed: %s", e)
            # fallthrough to local save

    # Fallback local filesystem save (useful for local dev)
    out_dir = os.environ.get("MEETING_PERSIST_DIR", "bookings")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, filename)
    with open(out_path, "wb") as fh:
        fh.write(payload_bytes)
    logger.info("Persisted booking to local file: %s", out_path)
    return {"status": "ok", "path": out_path}
