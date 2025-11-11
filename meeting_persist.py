# ==============================================================
#  Phase 11-F Compliant – Unified Logging, Metrics & Config Integrated
#  Generated: 2025-11-03T17:29:04.916330
# TODO: Move hardcoded port number to config.py
#  Do not alter business logic – observability patch only.
# ==============================================================

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
import uuid

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

def _validate_payload(contact: dict, booking_data: dict) -> None:
    """Validate payload schema before persistence"""
    if not contact or not isinstance(contact, dict):
        raise ValueError("Contact must be a non-empty dictionary")
    if not booking_data or not isinstance(booking_data, dict):
        raise ValueError("Booking data must be a non-empty dictionary")
    
    required_contact_fields = ['name', 'phone']
    required_booking_fields = ['email', 'datetime']
    # notes and meeting_link are optional per requirements
    
    for field in required_contact_fields:
        if field not in contact:
            raise ValueError(f"Missing required contact field: {field}")
    
    for field in required_booking_fields:
        if field not in booking_data:
            raise ValueError(f"Missing required booking field: {field}")

def _push_latency_metric(latency_ms: float, trace_id: str, storage_type: str) -> None:
    """Push latency histogram metric"""
    try:
        push_metric(
            service="meeting_persist", 
            event="meeting_persist_latency_ms",
            metric_type="histogram",
            value=latency_ms,
            storage_type=storage_type,
            trace_id=trace_id
        )
    except Exception as e:
        log_event("meeting_persist", "metric_push_failed", "warning",
                 "Failed to push latency metric", 
                 error=str(e), trace_id=trace_id)

def persist_booking(session_id: str, contact: dict, booking_data: dict, r2_client=None, bucket_prefix="bookings"):
    """
    booking_data should include at least: {email, datetime, notes, meeting_link (optional)}
    contact is the contact row dict
    """
    trace_id = _get_trace_id()
    start_time = time.time()
    
    # Validate payload schema before any I/O
    try:
        _validate_payload(contact, booking_data)
    except ValueError as e:
        log_event("meeting_persist", "payload_validation_failed", "error",
                 "Payload validation failed", 
                 error=str(e), session_id=session_id, trace_id=trace_id)
        return {"status": "error", "message": str(e)}
    
    timestamp = int(time.time())
    # Use non-PII identifier: session_id + timestamp + random suffix
    random_suffix = str(uuid.uuid4())[:8]
    filename = f"booking_{session_id}_{timestamp}_{random_suffix}.json"
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
    
    # Attempt R2 upload with retry logic for transient errors
    if r2_client:
        max_retries = 3
        base_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                # Prefer upload_bytes if available
                if hasattr(r2_client, "upload_bytes"):
                    url = r2_client.upload_bytes(key, payload_bytes)
                elif hasattr(r2_client, "upload_file"):
                    tmp_path = os.path.join("/tmp", filename)
                    try:
                        with open(tmp_path, "wb") as fh:
                            fh.write(payload_bytes)
                        url = r2_client.upload_file(tmp_path, key)
                    finally:
                        # Ensure temp file cleanup even on upload failure
                        try:
                            os.remove(tmp_path)
                        except Exception:
                            pass  # Temp file might not exist or already removed
                else:
                    raise RuntimeError("R2 client missing upload API")
                
                latency_ms = (time.time() - start_time) * 1000
                _push_latency_metric(latency_ms, trace_id, "r2")
                
                log_event("meeting_persist", "booking_persisted_r2", "info",
                         "Persisted booking to R2", 
                         r2_url=url, key=key, session_id=session_id, trace_id=trace_id,
                         attempt=attempt + 1, latency_ms=latency_ms)
                return {"status": "ok", "r2_url": url, "key": key}
                
            except Exception as e:
                error_msg = str(e).lower()
                is_transient = any(pattern in error_msg for pattern in 
                                 ['timeout', 'connection', 'network', 'temporary', 'retry'])
                
                if attempt < max_retries - 1 and is_transient:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    log_event("meeting_persist", "r2_upload_retry", "warning",
                             "R2 upload failed, retrying",
                             error=str(e), key=key, session_id=session_id, trace_id=trace_id,
                             attempt=attempt + 1, max_retries=max_retries, delay=delay)
                    time.sleep(delay)
                    continue
                else:
                    log_event("meeting_persist", "r2_upload_failed", "error",
                             "R2 upload failed, falling back to local storage",
                             error=str(e), key=key, session_id=session_id, trace_id=trace_id,
                             attempt=attempt + 1, is_transient=is_transient)
                    break  # Fall through to local storage

    # Fallback local filesystem save (useful for local dev)
    try:
        out_dir = getattr(Config, "MEETING_PERSIST_DIR", "bookings")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, filename)
        with open(out_path, "wb") as fh:
            fh.write(payload_bytes)
        
        latency_ms = (time.time() - start_time) * 1000
        _push_latency_metric(latency_ms, trace_id, "local")
        
        log_event("meeting_persist", "booking_persisted_local", "info",
                 "Persisted booking to local file",
                 local_path=out_path, session_id=session_id, trace_id=trace_id,
                 latency_ms=latency_ms)
        return {"status": "ok", "path": out_path}
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _push_latency_metric(latency_ms, trace_id, "failed")
        
        log_event("meeting_persist", "local_persist_failed", "error",
                 "Local persistence failed",
                 error=str(e), session_id=session_id, trace_id=trace_id,
                 latency_ms=latency_ms)
        return {"status": "error", "message": f"All persistence methods failed: {str(e)}"}

try:
    push_metric("system_core", "module_load", "ok", __name__)
except Exception as e:
    log_event("system_core", "metric_load_fail", "error", str(e))