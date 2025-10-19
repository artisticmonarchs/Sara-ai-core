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
import os
import sys
import logging
from typing import Dict, Generator

# Try to import celery task to enqueue; fallback to printing the payload if not available
try:
    # prefer a dedicated outbound task if present
    from outbound_tasks import outbound_call_task
    enqueue_func = lambda payload: outbound_call_task.delay(payload)
except Exception:
    try:
        import tasks as tasks_module
        # attempt to call an enqueue helper if present
        if hasattr(tasks_module, "enqueue_outbound_call"):
            enqueue_func = lambda payload: tasks_module.enqueue_outbound_call(payload)
        else:
            # fallback: try to find a celery task named 'outbound_call_task' in tasks module
            if hasattr(tasks_module, "outbound_call_task"):
                outbound = getattr(tasks_module, "outbound_call_task")
                enqueue_func = lambda payload: outbound.delay(payload)
            else:
                enqueue_func = None
    except Exception:
        enqueue_func = None

USE_PHONENUMBERS = False
if USE_PHONENUMBERS:
    try:
        import phonenumbers  # type: ignore
    except Exception:
        USE_PHONENUMBERS = False

logger = logging.getLogger("contacts_loader")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(ch)

PHONE_RE = re.compile(r"\\+?\\d[\\d\\-\\s\\(\\)]{7,}\\d$")

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
    phone = normalize_phone(row.get("phone", ""))
    if not phone or not PHONE_RE.search(phone):
        logger.warning("Invalid phone number: %s", row.get("phone"))
        return False
    # basic website sanity
    website = row.get("website", "")
    if website and len(website) < 5:
        logger.warning("Invalid website for %s: %s", row.get("name"), website)
    return True

def read_csv(path: str) -> Generator[Dict[str,str], None, None]:
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, fieldnames=["name","phone","industry","website"])
        for i, row in enumerate(reader):
            # skip empty rows that are header-like
            if i == 0 and row.get("name","").lower().startswith("name"):
                continue
            # strip whitespace
            cleaned = {k: (v.strip() if v else "") for k, v in row.items()}
            yield cleaned

def enqueue_contact(contact: Dict[str,str], campaign: str = "default", dry_run: bool = False):
    payload = {
        "contact": contact,
        "campaign": campaign
    }
    if dry_run:
        logger.info("DRY RUN enqueue: %s", payload)
        return None
    if enqueue_func is None:
        logger.error("No enqueue function available. Please ensure outbound_tasks or tasks.enqueue_outbound_call exists.")
        raise RuntimeError("No enqueue function available")
    res = enqueue_func(payload)
    logger.info("Enqueued contact %s -> task=%s", contact.get("phone"), getattr(res, "id", str(res)))
    return res

def main():
    p = argparse.ArgumentParser(description="Load contacts CSV and enqueue outbound tasks")
    p.add_argument("--csv", required=True, help="Path to CSV file")
    p.add_argument("--campaign", default="default", help="Campaign name")
    p.add_argument("--dry-run", action="store_true", help="Do not enqueue, only validate")
    p.add_argument("--limit", type=int, default=0, help="Limit number of rows (0 = no limit)")
    args = p.parse_args()

    if not os.path.exists(args.csv):
        logger.error("CSV file not found: %s", args.csv)
        sys.exit(1)

    count = 0
    for row in read_csv(args.csv):
        if args.limit and count >= args.limit:
            break
        if not validate_row(row):
            continue
        try:
            enqueue_contact(row, campaign=args.campaign, dry_run=args.dry_run)
            count += 1
        except Exception as e:
            logger.exception("Failed to enqueue contact: %s", e)

    logger.info("Finished processing CSV. Enqueued: %d", count)

if __name__ == "__main__":
    main()
