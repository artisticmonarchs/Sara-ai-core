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
from datetime import datetime, timedelta
import sys
import uuid

# Centralized configuration ONLY
try:
    from config import Config
except ImportError:
    # Minimal fallback that doesn't violate isolation
    import os
    class Config:
        REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        SERVICE_NAME = "campaign_runner"

# Structured logging with proper lazy shim
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

from contacts_loader import read_csv, validate_row, normalize_phone, enqueue_contact

def campaign_daily_key(campaign_name: str, date: datetime):
    return f"campaign:{campaign_name}:count:{date.strftime('%Y%m%d')}"

def get_daily_count(campaign_name: str):
    trace_id = get_trace_id()
    
    # CORRECTED: Use get_client() instead of get_redis_client()
    try:
        from redis_client import get_client, safe_redis_operation
    except ImportError:
        # Fallback that doesn't instantiate at import time
        import redis
        def get_client():
            return redis.from_url(Config.REDIS_URL, decode_responses=True)
        def safe_redis_operation(func, fallback=None, operation_name=None):
            try:
                client = get_client()
                return func(client)
            except Exception:
                return fallback
    
    def _get_redis_count(client):
        key = campaign_daily_key(campaign_name, datetime.utcnow())
        val = client.get(key)
        return int(val) if val else 0
    
    count = safe_redis_operation(
        _get_redis_count, 
        fallback=0,
        operation_name="campaign_daily_count_get"
    )
    
    log_event("campaign_runner", "daily_count_retrieved", "info",
              f"Retrieved daily count for campaign {campaign_name}",
              campaign=campaign_name, daily_count=count, trace_id=trace_id)
    
    return count

def incr_daily_count(campaign_name: str, amount: int = 1, expiry_hours: int = 48):
    trace_id = get_trace_id()
    start_time = time.time()
    
    # CORRECTED: Use get_client() instead of get_redis_client()
    try:
        from redis_client import get_client, safe_redis_operation
    except ImportError:
        import redis
        def get_client():
            return redis.from_url(Config.REDIS_URL, decode_responses=True)
        def safe_redis_operation(func, fallback=None, operation_name=None):
            try:
                client = get_client()
                return func(client)
            except Exception:
                return fallback
    
    def _incr_redis_count(client):
        key = campaign_daily_key(campaign_name, datetime.utcnow())
        pipe = client.pipeline()
        pipe.incrby(key, amount)
        pipe.expire(key, expiry_hours * 3600)
        result = pipe.execute()
        return result[0] if result else None
    
    new_count = safe_redis_operation(
        _incr_redis_count,
        fallback=None,
        operation_name="campaign_daily_count_incr"
    )
    
    # Metrics - LAZY loaded inside function to avoid import-time side effects
    def _get_metrics():
        try:
            from metrics_collector import increment_metric, observe_latency
            return increment_metric, observe_latency
        except ImportError:
            def _noop_metric(*args, **kwargs): pass
            def _noop_latency(*args, **kwargs): pass
            return _noop_metric, _noop_latency
    
    increment_metric, observe_latency = _get_metrics()
    
    latency_ms = (time.time() - start_time) * 1000
    observe_latency("campaign.redis_incr_latency", latency_ms)
    
    if new_count is not None:
        increment_metric("campaign.daily_count_increments")
        log_event("campaign_runner", "daily_count_incremented", "info",
                  f"Incremented daily count for {campaign_name} by {amount}",
                  campaign=campaign_name, amount=amount, new_count=new_count, 
                  trace_id=trace_id, latency_ms=latency_ms)
    else:
        increment_metric("campaign.redis_operation_failed")
        log_event("campaign_runner", "daily_count_increment_failed", "error",
                  f"Failed to increment daily count for {campaign_name}",
                  campaign=campaign_name, amount=amount, trace_id=trace_id)

def run_campaign(csv_path: str, campaign_name: str, rate_per_min: int = 20, daily_cap: int = 300, dry_run: bool = False):
    trace_id = get_trace_id()
    campaign_start_time = time.time()
    
    # Metrics - LAZY loaded inside function to avoid import-time side effects
    def _get_metrics():
        try:
            from metrics_collector import increment_metric, observe_latency
            return increment_metric, observe_latency
        except ImportError:
            def _noop_metric(*args, **kwargs): pass
            def _noop_latency(*args, **kwargs): pass
            return _noop_metric, _noop_latency
    
    increment_metric, observe_latency = _get_metrics()
    
    increment_metric("campaign.started")
    log_event("campaign_runner", "campaign_start", "info", 
              f"Starting campaign {campaign_name} from {csv_path}", 
              campaign=campaign_name, csv_path=csv_path, rate_per_min=rate_per_min, 
              daily_cap=daily_cap, dry_run=dry_run, trace_id=trace_id)
    
    interval = 60.0 / max(1, rate_per_min)
    current_count = get_daily_count(campaign_name)
    
    contacts_processed = 0
    contacts_enqueued = 0
    contacts_failed = 0

    for row in read_csv(csv_path):
        contacts_processed += 1
        contact_start_time = time.time()
        
        if dry_run:
            log_event("campaign_runner", "dry_run_row", "debug", f"DRY ROW: {row}",
                      campaign=campaign_name, trace_id=trace_id)
            continue
            
        if not validate_row(row):
            increment_metric("campaign.contact_validation_failed")
            log_event("campaign_runner", "contact_validation_failed", "warning",
                      f"Invalid contact row skipped", campaign=campaign_name, 
                      row_data=str(row), trace_id=trace_id)
            continue
            
        if current_count >= daily_cap:
            increment_metric("campaign.daily_cap_reached")
            log_event("campaign_runner", "daily_cap_reached", "warning", 
                      f"Daily cap reached ({daily_cap}). Stopping.",
                      campaign=campaign_name, daily_cap=daily_cap, 
                      current_count=current_count, trace_id=trace_id)
            break
            
        try:
            # Enqueue with latency observation
            enqueue_contact(row, campaign=campaign_name, dry_run=dry_run)
            current_count += 1
            contacts_enqueued += 1
            
            # Record successful enqueue
            contact_latency = (time.time() - contact_start_time) * 1000
            observe_latency("campaign.contact_enqueue_latency", contact_latency)
            increment_metric("campaign.contact_enqueued")
            
            # Update Redis count
            incr_daily_count(campaign_name, 1)
            
        except Exception as e:
            contacts_failed += 1
            increment_metric("campaign.contact_enqueue_failed")
            log_event("campaign_runner", "enqueue_failed", "error", 
                      f"Failed to enqueue contact: {e}", 
                      campaign=campaign_name, error=str(e), trace_id=trace_id)
        
        time.sleep(interval)
    
    # Campaign completion metrics
    campaign_duration = (time.time() - campaign_start_time) * 1000
    observe_latency("campaign.total_duration", campaign_duration)
    increment_metric("campaign.completed")
    
    log_event("campaign_runner", "campaign_finished", "info", 
              f"Campaign finished. Processed {contacts_processed}, enqueued {contacts_enqueued}, failed {contacts_failed}",
              campaign=campaign_name, contacts_processed=contacts_processed,
              contacts_enqueued=contacts_enqueued, contacts_failed=contacts_failed,
              final_count=current_count, duration_ms=campaign_duration, trace_id=trace_id)

def main():
    trace_id = get_trace_id()
    
    p = argparse.ArgumentParser(description="Run outbound campaign from CSV")
    p.add_argument("--csv", required=True)
    p.add_argument("--campaign", default="default")
    p.add_argument("--rate", type=int, default=20, help="calls per minute")
    p.add_argument("--daily-cap", type=int, default=300)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    try:
        run_campaign(args.csv, args.campaign, rate_per_min=args.rate, 
                    daily_cap=args.daily_cap, dry_run=args.dry_run)
        
        # Final metrics - lazy loaded
        def _get_metrics():
            try:
                from metrics_collector import increment_metric
                return increment_metric
            except ImportError:
                def _noop_metric(*args, **kwargs): pass
                return _noop_metric
        
        increment_metric = _get_metrics()
        increment_metric("campaign.run_success")
        
    except Exception as e:
        # Error metrics - lazy loaded  
        def _get_metrics():
            try:
                from metrics_collector import increment_metric
                return increment_metric
            except ImportError:
                def _noop_metric(*args, **kwargs): pass
                return _noop_metric
        
        increment_metric = _get_metrics()
        increment_metric("campaign.run_failed")
        
        log_event("campaign_runner", "main_execution_failed", "error",
                  f"Campaign execution failed: {e}", error=str(e), trace_id=trace_id)
        sys.exit(1)

if __name__ == "__main__":
    main()