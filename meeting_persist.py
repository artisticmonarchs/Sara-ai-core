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

# Configuration isolation with lazy loading
def _get_config():
    try:
        from config import Config
        return Config
    except Exception:
        class FallbackConfig:
            MEETING_PERSIST_DIR = "bookings"
            SERVICE_NAME = "meeting_persist"
        return FallbackConfig

Config = _get_config()

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
def _get_trace_id():
    try:
        from logging_utils import get_trace_id
        return get_trace_id()
    except Exception:
        import uuid
        return str(uuid.uuid4())[:8]

def _safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in name)[:160]

def persist_booking(session_id: str, contact: dict, booking_data: dict, r2_client=None, bucket_prefix="bookings"):
    """
    booking_data should include at least: {email, datetime, notes, meeting_link (optional)}
    contact is the contact row dict
    """
    trace_id = _get_trace_id()
    timestamp = int(time.time())
    filename = f"{_safe_filename(contact.get('name','unknown'))}_{contact.get('phone','unknown')}_{timestamp}.json"
    payload = {
        "session_id": session_id,
        "contact": contact,
        "booking": booking_data,
        "created_at": time.time(),
        "trace_id": trace_id
    }
    payload_bytes = json.dumps(payload, indent=2).encode("utf-8")

    # Try R2 client if provided or available
    if r2_client is None:
        try:
            from r2_client import R2Client
            r2_client = R2Client()
        except Exception as e:
            r2_client = None
            log_event("meeting_persist", "r2_client_init_failed", "warning",
                     "Failed to initialize R2 client", 
                     error=str(e), trace_id=trace_id)

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
            log_event("meeting_persist", "booking_persisted_r2", "info",
                     "Persisted booking to R2", 
                     r2_url=url, key=key, session_id=session_id, trace_id=trace_id)
            return {"status": "ok", "r2_url": url, "key": key}
        except Exception as e:
            log_event("meeting_persist", "r2_upload_failed", "error",
                     "R2 upload failed, falling back to local storage",
                     error=str(e), key=key, session_id=session_id, trace_id=trace_id)
            # fallthrough to local save

    # Fallback local filesystem save (useful for local dev)
    out_dir = getattr(Config, "MEETING_PERSIST_DIR", "bookings")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, filename)
    with open(out_path, "wb") as fh:
        fh.write(payload_bytes)
    log_event("meeting_persist", "booking_persisted_local", "info",
             "Persisted booking to local file",
             local_path=out_path, session_id=session_id, trace_id=trace_id)
    return {"status": "ok", "path": out_path}