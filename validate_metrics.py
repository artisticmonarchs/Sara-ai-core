"""
validate_metrics.py — Phase 11-D Compliant
Prometheus metrics validation utility with structured logging.
"""

import argparse
import json
import time
import requests
import os
import logging

# Define module logger
logger = logging.getLogger(__name__)

# Safe import with fallbacks
try:
    from logging_utils import log_event
except ImportError:
    # Fallback implementation
    logging.basicConfig(level=logging.INFO)
    
    def log_event(service: str, event: str, status: str, message: str, **extra) -> None:
        log_msg = f"[{service}] {event}: {message} ({status})"
        if extra:
            log_msg += f" [extra: {extra}]"
        logging.info(log_msg)

# Opt-in validation on import
VALIDATE_ON_IMPORT = os.getenv('VALIDATE_METRICS_ON_IMPORT', '').lower() in ('1', 'true', 'yes')

ENDPOINTS = ["/healthz", "/metrics", "/metrics_snapshot", "/system_status"]

KEY_METRICS = [
    "api_healthz_requests_total",
    "tts_requests_total",
    "tts_failures_total",
    "tts_latency_ms_count",
]

# Required histogram buckets for latency metrics
REQUIRED_HISTOGRAM_BUCKETS = {
    "asr_latency_seconds": [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0],
    "tts_latency_seconds": [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0],
    "gpt_latency_seconds": [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0],
    "ws_latency_seconds": [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0],
}


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


def validate_histogram_buckets(raw_text):
    """Validate that required latency histograms have proper bucket definitions."""
    errors = []
    
    for histogram_name, required_buckets in REQUIRED_HISTOGRAM_BUCKETS.items():
        bucket_metric_name = f"{histogram_name}_bucket"
        found_buckets = set()
        
        # Look for bucket lines in the metrics text
        for line in raw_text.splitlines():
            if bucket_metric_name in line and "le=" in line:
                # Extract the le (less than or equal) value from the metric
                try:
                    # Metric format: name_bucket{le="0.001"} value
                    le_start = line.find('le="') + 4
                    le_end = line.find('"', le_start)
                    if le_start > 3 and le_end > le_start:
                        le_value = float(line[le_start:le_end])
                        found_buckets.add(le_value)
                except (ValueError, IndexError):
                    continue
        
        # Check if we found all required buckets
        missing_buckets = set(required_buckets) - found_buckets
        if missing_buckets:
            errors.append(f"Histogram {histogram_name} missing buckets: {sorted(missing_buckets)}")
        elif not found_buckets:
            errors.append(f"Histogram {histogram_name} not found or has no buckets defined")
        else:
            log_event(
                service="validate_metrics",
                event="histogram_buckets_valid",
                status="info",
                message=f"Histogram {histogram_name} buckets validated",
                extra={
                    "histogram": histogram_name,
                    "found_buckets_count": len(found_buckets),
                    "required_buckets_count": len(required_buckets)
                }
            )
    
    return errors


def validate_endpoints(base_url):
    for ep in ENDPOINTS:
        url = f"{base_url}{ep}"
        try:
            resp = requests.get(url, timeout=5)
            log_event(
                service="validate_metrics",
                event="endpoint_check",
                status="info",
                message=f"Endpoint {ep} check",
                extra={"endpoint": ep, "status_code": resp.status_code}
            )
            if resp.status_code != 200:
                raise AssertionError(f"Endpoint {ep} returned {resp.status_code}")
        except Exception as e:
            log_event(
                service="validate_metrics", 
                event="endpoint_error",
                status="error",
                message=f"Endpoint {ep} failed",
                extra={"endpoint": ep, "error": str(e)}
            )
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
        log_event(
            service="validate_metrics",
            event="metric_delta",
            status="info", 
            message=f"Metric {metric} delta",
            extra={"metric": metric, "before": before_val, "after": after_val, "delta": delta}
        )
        if delta < 0:
            raise AssertionError(f"Metric {metric} decreased unexpectedly")


def loop_validation(base_url, duration):
    start_time = time.time()
    while time.time() - start_time < duration:
        try:
            validate_metric_increments(base_url)
            log_event(
                service="validate_metrics",
                event="loop_iteration_complete", 
                status="info",
                message="Validation iteration complete",
                extra={"elapsed": int(time.time() - start_time)}
            )
        except Exception as e:
            log_event(
                service="validate_metrics",
                event="loop_error",
                status="error",
                message="Loop validation error",
                extra={"error": str(e)}
            )
        time.sleep(10)  # interval between iterations


def validate_on_startup(base_url):
    """Validate metrics on import or startup; exit non-zero if invalid."""
    try:
        log_event(
            service="validate_metrics",
            event="startup_validation_start",
            status="info",
            message="Starting metrics validation on startup"
        )
        
        # Fetch and validate metrics
        raw_metrics = fetch_metrics(base_url)
        
        # Validate histogram buckets
        bucket_errors = validate_histogram_buckets(raw_metrics)
        if bucket_errors:
            for error in bucket_errors:
                log_event(
                    service="validate_metrics",
                    event="histogram_bucket_error",
                    status="error",
                    message=error
                )
            raise AssertionError(f"Histogram bucket validation failed: {bucket_errors}")
        
        # Validate endpoints
        validate_endpoints(base_url)
        
        log_event(
            service="validate_metrics",
            event="startup_validation_complete",
            status="info",
            message="Startup validation completed successfully"
        )
        
    except Exception as e:
        log_event(
            service="validate_metrics",
            event="startup_validation_failed",
            status="error",
            message=f"Startup validation failed: {str(e)}",
            extra={"error_details": str(e)}
        )
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description="Prometheus metrics validation utility")
    parser.add_argument("--base-url", required=True, help="Base URL of the Sara AI Core deployment")
    parser.add_argument("--loop", type=int, help="Run continuous validation for N seconds")
    args = parser.parse_args()

    log_event(
        service="validate_metrics",
        event="validation_start",
        status="info",
        message="Metrics validation started",
        extra={"base_url": args.base_url}
    )

    try:
        # Run startup validation (includes histogram bucket checks)
        validate_on_startup(args.base_url)
        
        if args.loop:
            log_event(
                service="validate_metrics",
                event="loop_mode_start", 
                status="info",
                message="Loop validation mode started",
                extra={"duration": args.loop}
            )
            loop_validation(args.base_url, args.loop)
        else:
            validate_metric_increments(args.base_url)
        log_event(
            service="validate_metrics",
            event="validation_complete",
            status="info",
            message="Validation completed successfully",
            extra={"result": "success"}
        )
    except Exception as e:
        log_event(
            service="validate_metrics",
            event="validation_failed",
            status="error",
            message="Validation failed",
            extra={"error": str(e)}
        )
        raise SystemExit(1)


# Opt-in validation on import
if VALIDATE_ON_IMPORT:
    import sys
    base_url_from_env = os.getenv('METRICS_BASE_URL')
    if base_url_from_env:
        validate_on_startup(base_url_from_env)
    else:
        logger.warning("VALIDATE_METRICS_ON_IMPORT is set but METRICS_BASE_URL is not provided")


if __name__ == "__main__":
    main()