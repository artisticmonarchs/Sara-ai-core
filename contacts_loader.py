"""
contacts_loader.py — Phase 12 Compliant
Automatically hardened by phase12_auto_hardener.py
"""

#!/usr/bin/env python3
"""
contacts_loader.py
Reads a CSV of contacts (name,phone,industry,website), validates rows, normalizes phone to E.164-ish format,
and enqueues an outbound Celery task for each valid contact.

Usage:
    python contacts_loader.py --csv leads.csv --campaign my_campaign --dry-run

This file is intentionally conservative about external dependencies: it will not require the 'phonenumbers' package.
If you have the 'phonenumbers' package installed, set USE_PHONENUMBERS=True to use it for better normalization.
"""
import csv
import argparse
import re
import sys
import uuid
import time  # Added missing import
import os    # Added for file existence check
from typing import Dict, Generator
import signal
import sys
import logging

def _graceful_shutdown(signum, frame):
    """Phase 12: Graceful shutdown handler"""
    try:
        logging.getLogger(__name__).info("Received signal %s, shutting down gracefully...", signum)
    except Exception:
        sys.stderr.write(f"[contacts_loader] shutdown signal={signum}\n")
    sys.exit(0)

signal.signal(signal.SIGINT, _graceful_shutdown)
signal.signal(signal.SIGTERM, _graceful_shutdown)


# Centralized configuration
try:
    from config import Config
    USE_PHONENUMBERS = getattr(Config, "USE_PHONENUMBERS", False)
except ImportError:
    USE_PHONENUMBERS = False

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

# Metrics with lazy loading (inside functions to avoid import-time side effects)
def _get_metrics():
    try:
        from metrics_collector import increment_metric, observe_latency
        return increment_metric, observe_latency
    except ImportError:
        def _noop_metric(*args, **kwargs): pass
        def _noop_latency(*args, **kwargs): pass
        return _noop_metric, _noop_latency

# prefer the production task names in outbound_tasks, then fall back
enqueue_func = None
try:
    from outbound_tasks import enqueue_call as _enqueue_call
    enqueue_func = lambda payload: _enqueue_call.delay(payload)  # Celery task
except Exception:
    try:
        from outbound_tasks import call_task as _call_task
        enqueue_func = lambda payload: _call_task.delay(payload)  # Celery task
    except Exception:
        try:
            import tasks as tasks_module
            if hasattr(tasks_module, "enqueue_outbound_call"):
                enqueue_func = lambda payload: tasks_module.enqueue_outbound_call(payload)
            elif hasattr(tasks_module, "outbound_call_task"):
                enqueue_func = lambda payload: tasks_module.outbound_call_task.delay(payload)
        except Exception:
            enqueue_func = None

if USE_PHONENUMBERS:
    try:
        import phonenumbers  # type: ignore
    except Exception:
        USE_PHONENUMBERS = False

PHONE_RE = re.compile(r"\+?\d[\d\-\s\(\)]{7,}\d$")

def normalize_phone(phone: str) -> str:
    phone = (phone or "").strip()
    if not phone:
        return ""
    if USE_PHONENUMBERS:
        try:
            pn = phonenumbers.parse(phone, "US")
            return phonenumbers.format_number(pn, phonenumbers.PhoneNumberFormat.E164)
        except Exception:
            return re.sub(r'[^0-9+]', '', phone)
    # Simple normalization: strip non-digits, ensure leading +1 if 10 digits
    digits = re.sub(r'[^0-9]', '', phone)
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) > 10 and digits.startswith("1"):
        return "+" + digits
    if phone.startswith("+"):
        return phone
    return "+" + digits

def validate_row(row: Dict[str,str]) -> bool:
    trace_id = get_trace_id()
    phone = normalize_phone(row.get("phone", ""))
    if not phone or not PHONE_RE.search(phone):
        log_event("contacts_loader", "phone_validation_failed", "warning", 
                  f"Invalid phone number: {row.get('phone')}",
                  phone=row.get('phone'), trace_id=trace_id)
        return False
    # basic website sanity
    website = row.get("website", "")
    if website and len(website) < 5:
        log_event("contacts_loader", "website_validation_warning", "warning",
                  f"Invalid website for {row.get('name')}: {website}",
                  contact_name=row.get('name'), website=website, trace_id=trace_id)
    return True

def read_csv(path: str) -> Generator[Dict[str,str], None, None]:
    trace_id = get_trace_id()
    
    # Use proper file existence check without os.environ access
    try:
        with open(path, "r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, fieldnames=["name","phone","industry","website"])
            for i, row in enumerate(reader):
                # skip empty rows that are header-like
                if i == 0 and row.get("name","").lower().startswith("name"):
                    continue
                # strip whitespace
                cleaned = {k: (v.strip() if v else "") for k, v in row.items()}
                yield cleaned
    except FileNotFoundError:
        log_event("contacts_loader", "file_not_found", "error",
                  f"CSV file not found: {path}", file_path=path, trace_id=trace_id)
        raise
    except Exception as e:
        log_event("contacts_loader", "file_read_error", "error",
                  f"Error reading CSV file: {e}", file_path=path, error=str(e), trace_id=trace_id)
        raise

def enqueue_contact(contact: Dict[str,str], campaign: str = "default", dry_run: bool = False):
    trace_id = get_trace_id()
    start_time = time.time()
    
    payload = {
        "contact": contact,
        "campaign": campaign
    }
    
    if dry_run:
        log_event("contacts_loader", "dry_run_enqueue", "info",
                  f"DRY RUN enqueue: {payload}", 
                  campaign=campaign, contact_phone=contact.get("phone"), trace_id=trace_id)
        return None
        
    if enqueue_func is None:
        log_event("contacts_loader", "enqueue_function_missing", "error",
                  "No enqueue function available. Please ensure outbound_tasks or tasks.enqueue_outbound_call exists.",
                  campaign=campaign, trace_id=trace_id)
        raise RuntimeError("No enqueue function available")
        
    try:
        res = enqueue_func(payload)
        task_id = getattr(res, "id", str(res))
        
        # Metrics - lazy loaded inside function
        increment_metric, observe_latency = _get_metrics()
        increment_metric("contacts.enqueued")
        
        latency_ms = (time.time() - start_time) * 1000
        observe_latency("contacts.enqueue_latency", latency_ms)
        log_event("contacts_loader", "contact_enqueued", "info",
                  f"Enqueued contact {contact.get('phone')} -> task={task_id}",
                  contact_phone=contact.get("phone"), task_id=task_id, 
                  campaign=campaign, latency_ms=latency_ms, trace_id=trace_id)
                      
        return res
        
    except Exception as e:
        # Metrics - lazy loaded inside function
        increment_metric, _ = _get_metrics()
        increment_metric("contacts.enqueue_failed")
        
        log_event("contacts_loader", "enqueue_failed", "error",
                  f"Failed to enqueue contact: {e}",
                  contact_phone=contact.get("phone"), campaign=campaign, 
                  error=str(e), trace_id=trace_id)
        raise

def main():
    trace_id = get_trace_id()
    main_start_time = time.time()
    
    p = argparse.ArgumentParser(description="Load contacts CSV and enqueue outbound tasks")
    p.add_argument("--csv", required=True, help="Path to CSV file")
    p.add_argument("--campaign", default="default", help="Campaign name")
    p.add_argument("--dry-run", action="store_true", help="Do not enqueue, only validate")
    p.add_argument("--limit", type=int, default=0, help="Limit number of rows (0 = no limit)")
    args = p.parse_args()

    # File existence check without violating configuration isolation
    if not os.path.exists(args.csv):
        log_event("contacts_loader", "csv_file_not_found", "error",
                  f"CSV file not found: {args.csv}", 
                  file_path=args.csv, trace_id=trace_id)
        sys.exit(1)

    # Metrics - lazy loaded
    increment_metric, observe_latency = _get_metrics()
    increment_metric("contacts_loader.started")
    
    log_event("contacts_loader", "process_started", "info",
              f"Starting contacts processing for campaign {args.campaign}",
              campaign=args.campaign, csv_path=args.csv, 
              dry_run=args.dry_run, limit=args.limit, trace_id=trace_id)

    count = 0
    processed = 0
    failed = 0

    try:
        for row in read_csv(args.csv):
            if args.limit and count >= args.limit:
                log_event("contacts_loader", "row_limit_reached", "info",
                          f"Row_limit reached: {args.limit}", 
                          campaign=args.campaign, limit=args.limit, trace_id=trace_id)
                break
                
            processed += 1
            if not validate_row(row):
                failed += 1
                continue
                
            try:
                enqueue_contact(row, campaign=args.campaign, dry_run=args.dry_run)
                count += 1
            except Exception as e:
                failed += 1
                log_event("contacts_loader", "contact_processing_failed", "error",
                          f"Failed to process contact: {e}",
                          contact=row.get("name"), error=str(e), trace_id=trace_id)

        # Process completion metrics
        main_duration = (time.time() - main_start_time) * 1000
        observe_latency("contacts_loader.total_duration", main_duration)
        increment_metric("contacts_loader.completed")
        
        log_event("contacts_loader", "process_completed", "info",
                  f"Finished processing CSV. Processed: {processed}, Enqueued: {count}, Failed: {failed}",
                  campaign=args.campaign, processed=processed, enqueued=count, 
                  failed=failed, duration_ms=main_duration, trace_id=trace_id)
                  
    except Exception as e:
        increment_metric("contacts_loader.failed")
        log_event("contacts_loader", "process_failed", "error",
                  f"Contacts processing failed: {e}", 
                  campaign=args.campaign, error=str(e), trace_id=trace_id)
        sys.exit(1)

if __name__ == "__main__":
    main()