#!/usr/bin/env python3
"""
tools/validate_metrics.py — Phase 10M Prometheus Validation Utility (loop-enabled)

Validates endpoint health and metric updates for Sara AI Core production.

Usage:
    python tools/validate_metrics.py --base-url https://your-prod-url
    python tools/validate_metrics.py --base-url https://your-prod-url --loop 300
"""

import argparse
import json
import time
import requests
from datetime import datetime

ENDPOINTS = ["/healthz", "/metrics", "/metrics_snapshot", "/system_status"]

KEY_METRICS = [
    "api_healthz_requests_total",
    "tts_requests_total",
    "tts_failures_total",
    "tts_latency_ms_count",
]

def fetch_metrics(base_url):
    resp = requests.get(f"{base_url}/metrics", timeout=5)
    resp.raise_for_status()
    return resp.text

def parse_metrics(raw_text):
    metrics = {}
    for line in raw_text.splitlines():
        if line.startswith("#") or " " not in line:
            continue
        name, val = line.split(" ", 1)
        try:
            metrics[name] = float(val.strip())
        except ValueError:
            continue
    return metrics

def log_json(event_type, **kwargs):
    entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "event": event_type,
        **kwargs,
    }
    print(json.dumps(entry), flush=True)

def validate_endpoints(base_url):
    for ep in ENDPOINTS:
        url = f"{base_url}{ep}"
        try:
            resp = requests.get(url, timeout=5)
            log_json("endpoint_check", endpoint=ep, status=resp.status_code)
            if resp.status_code != 200:
                raise AssertionError(f"Endpoint {ep} returned {resp.status_code}")
        except Exception as e:
            log_json("endpoint_error", endpoint=ep, error=str(e))
            raise

def validate_metric_increments(base_url):
    before = parse_metrics(fetch_metrics(base_url))
    # Trigger a few requests to increment counters
    for _ in range(3):
        requests.get(f"{base_url}/healthz", timeout=3)
    time.sleep(2)
    after = parse_metrics(fetch_metrics(base_url))

    for metric in KEY_METRICS:
        before_val = before.get(metric, 0.0)
        after_val = after.get(metric, 0.0)
        delta = after_val - before_val
        log_json("metric_delta", metric=metric, before=before_val, after=after_val, delta=delta)
        if delta < 0:
            raise AssertionError(f"Metric {metric} decreased unexpectedly")

def loop_validation(base_url, duration):
    start_time = time.time()
    while time.time() - start_time < duration:
        try:
            validate_metric_increments(base_url)
            log_json("loop_iteration_complete", elapsed=int(time.time() - start_time))
        except Exception as e:
            log_json("loop_error", error=str(e))
        time.sleep(10)  # interval between iterations

def main():
    parser = argparse.ArgumentParser(description="Prometheus metrics validation utility")
    parser.add_argument("--base-url", required=True, help="Base URL of the Sara AI Core deployment")
    parser.add_argument("--loop", type=int, help="Run continuous validation for N seconds")
    args = parser.parse_args()

    log_json("validation_start", base_url=args.base_url)

    try:
        validate_endpoints(args.base_url)
        if args.loop:
            log_json("loop_mode_start", duration=args.loop)
            loop_validation(args.base_url, args.loop)
        else:
            validate_metric_increments(args.base_url)
        log_json("validation_complete", result="success")
    except Exception as e:
        log_json("validation_failed", error=str(e))
        raise SystemExit(1)

if __name__ == "__main__":
    main()
