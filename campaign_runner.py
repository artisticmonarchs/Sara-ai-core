#!/usr/bin/env python3
"""
campaign_runner.py
CLI tool to run an outbound campaign from a CSV file. Implements a simple rate limiter and daily-cap using Redis.
It uses contacts_loader.enqueue_contact to actually enqueue tasks (or outbound_tasks.outbound_call_task.delay).

Usage:
    python campaign_runner.py --csv leads.csv --campaign my_campaign --rate 20 --daily-cap 300
"""
import argparse
import time
import logging
from datetime import datetime, timedelta
import redis
import os
import sys

from contacts_loader import read_csv, validate_row, normalize_phone, enqueue_contact

# Attempt to import project redis client to reuse config; fallback to local redis
try:
    from redis_client import get_redis_client
    r = get_redis_client()
except Exception:
    REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(REDIS_URL, decode_responses=True)

logger = logging.getLogger("campaign_runner")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

def campaign_daily_key(campaign_name: str, date: datetime):
    return f"campaign:{campaign_name}:count:{date.strftime('%Y%m%d')}"

def get_daily_count(campaign_name: str):
    key = campaign_daily_key(campaign_name, datetime.utcnow())
    val = r.get(key)
    return int(val) if val else 0

def incr_daily_count(campaign_name: str, amount: int = 1, expiry_hours: int = 48):
    key = campaign_daily_key(campaign_name, datetime.utcnow())
    pipe = r.pipeline()
    pipe.incrby(key, amount)
    pipe.expire(key, expiry_hours * 3600)
    pipe.execute()

def run_campaign(csv_path: str, campaign_name: str, rate_per_min: int = 20, daily_cap: int = 300, dry_run: bool = False):
    logger.info("Starting campaign %s from %s (rate=%d/min, daily_cap=%d, dry_run=%s)", campaign_name, csv_path, rate_per_min, daily_cap, dry_run)
    interval = 60.0 / max(1, rate_per_min)
    current_count = get_daily_count(campaign_name)
    logger.info("Already this UTC day: %d calls", current_count)

    for row in read_csv(csv_path):
        if dry_run:
            logger.info("DRY ROW: %s", row)
            continue
        if not validate_row(row):
            continue
        if current_count >= daily_cap:
            logger.warning("Daily cap reached (%d). Stopping.", daily_cap)
            break
        try:
            enqueue_contact(row, campaign=campaign_name, dry_run=dry_run)
            current_count += 1
            incr_daily_count(campaign_name, 1)
        except Exception as e:
            logger.exception("Failed to enqueue: %s", e)
        time.sleep(interval)
    logger.info("Campaign finished. Enqueued %d contacts this run.", current_count)

def main():
    p = argparse.ArgumentParser(description="Run outbound campaign from CSV")
    p.add_argument("--csv", required=True)
    p.add_argument("--campaign", default="default")
    p.add_argument("--rate", type=int, default=20, help="calls per minute")
    p.add_argument("--daily-cap", type=int, default=300)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    run_campaign(args.csv, args.campaign, rate_per_min=args.rate, daily_cap=args.daily_cap, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
