# r2_client.py
"""
Cloudflare R2 Client Utilities - Phase 11-D Compliant
Handles connection checks and basic R2 health diagnostics with full observability.
"""

import os
import time
import traceback
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# --------------------------------------------------------------------------
# Phase 11-D: Metrics Integration
# --------------------------------------------------------------------------
try:
    import metrics_collector as metrics  # type: ignore
except ImportError:
    # Fallback for metrics unavailable
    class FallbackMetrics:
        def inc_metric(self, name, increment=1): pass
        def observe_latency(self, name, value): pass
    metrics = FallbackMetrics()

# --------------------------------------------------------------------------
# Phase 11-D: Structured Logging
# --------------------------------------------------------------------------
try:
    from logging_utils import log_event
except ImportError:
    # Fallback logging
    def log_event(service, event, status, message, trace_id=None, session_id=None, extra=None):
        print(f"[{service}] {event}: {message} - {status}")

# --------------------------------------------------------------------------
# Phase 11-D: Redis Circuit Breaker Support
# --------------------------------------------------------------------------
def _redis():
    """Get Redis client for circuit breaker checks."""
    try:
        from redis_client import get_redis_client
        return get_redis_client()
    except Exception:
        return None

def is_r2_circuit_breaker_open() -> bool:
    """
    Check if circuit breaker is open for R2 service.
    Returns True if calls should be blocked (breaker open).
    """
    client = _redis()
    if not client:
        return False  # Proceed if Redis unavailable
    
    try:
        key = "circuit_breaker:r2:state"
        state = client.get(key)
        return state == b"open"
    except Exception:
        return False  # Proceed on Redis errors

def report_r2_failure():
    """Report R2 failure to circuit breaker system."""
    client = _redis()
    if not client:
        return
    
    try:
        key = "circuit_breaker:r2:failures"
        client.incr(key)
        # Set expiration if this is the first failure
        if client.get(key) == b"1":
            client.expire(key, 300)  # 5-minute window
    except Exception:
        pass  # Silent fail on circuit breaker updates

def report_r2_success():
    """Report R2 success to circuit breaker system."""
    client = _redis()
    if not client:
        return
    
    try:
        # Reset failure count on success
        key = "circuit_breaker:r2:failures"
        client.delete(key)
        
        # Ensure circuit breaker is closed
        state_key = "circuit_breaker:r2:state"
        client.set(state_key, "closed")
    except Exception:
        pass  # Silent fail on circuit breaker updates

# --------------------------------------------------------------------------
# R2 Client Initialization with Observability
# --------------------------------------------------------------------------
def get_r2_client():
    """Create and return a Cloudflare R2-compatible boto3 client with observability."""
    trace_id = f"r2_init_{int(time.time())}"
    
    try:
        client = boto3.client(
            "s3",
            endpoint_url=os.getenv("R2_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
            region_name="auto",  # Cloudflare R2 ignores region, but boto3 requires it
        )
        
        metrics.inc_metric("r2_client_initializations_total")
        log_event(
            service="r2_client",
            event="client_initialized",
            status="ok",
            message="R2 client initialized successfully",
            trace_id=trace_id
        )
        return client
        
    except Exception as e:
        metrics.inc_metric("r2_client_initialization_failures_total")
        log_event(
            service="r2_client",
            event="client_initialization_failed",
            status="error",
            message="Failed to initialize R2 client",
            trace_id=trace_id,
            extra={
                "error": str(e),
                "stack": traceback.format_exc()
            }
        )
        raise

# --------------------------------------------------------------------------
# Enhanced R2 Connection Check with Full Observability
# --------------------------------------------------------------------------
def check_r2_connection(client=None, bucket=None, trace_id=None):
    """
    Verify R2 connectivity by attempting to list buckets or check specific bucket.
    Returns (True, metadata) if successful, (False, error_info) otherwise.
    """
    start_time = time.time()
    trace_id = trace_id or f"r2_check_{int(time.time())}"
    
    # Phase 11-D: Circuit breaker check
    if is_r2_circuit_breaker_open():
        metrics.inc_metric("r2_circuit_breaker_hits_total")
        log_event(
            service="r2_client",
            event="connection_check_blocked",
            status="blocked",
            message="R2 connection check blocked by circuit breaker",
            trace_id=trace_id
        )
        return False, {"error": "Circuit breaker open", "blocked": True}
    
    metrics.inc_metric("r2_connection_checks_total")
    
    try:
        if client is None:
            client = get_r2_client()
        
        # If bucket is specified, check bucket access; otherwise list buckets
        if bucket:
            client.list_objects_v2(Bucket=bucket, MaxKeys=1)
            operation = "bucket_access"
            target = bucket
        else:
            client.list_buckets()
            operation = "list_buckets"
            target = "all"
        
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        # Phase 11-D: Success reporting
        metrics.inc_metric("r2_connection_checks_ok_total")
        metrics.observe_latency("r2_connection_check_latency_ms", latency_ms)
        report_r2_success()
        
        log_event(
            service="r2_client",
            event="connection_check_ok",
            status="ok",
            message=f"R2 connection check successful for {operation}",
            trace_id=trace_id,
            extra={
                "operation": operation,
                "target": target,
                "latency_ms": latency_ms
            }
        )
        
        return True, {
            "operation": operation,
            "target": target,
            "latency_ms": latency_ms,
            "bucket": bucket if bucket else None
        }
        
    except Exception as e:
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        # Phase 11-D: Failure reporting
        metrics.inc_metric("r2_connection_check_failures_total")
        metrics.observe_latency("r2_connection_check_failure_latency_ms", latency_ms)
        report_r2_failure()
        
        error_info = {
            "error": str(e),
            "error_type": type(e).__name__,
            "latency_ms": latency_ms,
            "operation": "bucket_access" if bucket else "list_buckets",
            "target": bucket if bucket else "all"
        }
        
        log_event(
            service="r2_client",
            event="connection_check_failed",
            status="error",
            message="R2 connection check failed",
            trace_id=trace_id,
            extra={
                **error_info,
                "stack": traceback.format_exc()
            }
        )
        
        return False, error_info

# --------------------------------------------------------------------------
# Enhanced R2 Operations with Observability
# --------------------------------------------------------------------------
def upload_to_r2(client, bucket, key, data, content_type=None, trace_id=None):
    """
    Upload data to R2 with full observability.
    Returns (success, metadata) tuple.
    """
    start_time = time.time()
    trace_id = trace_id or f"r2_upload_{int(time.time())}"
    
    # Phase 11-D: Circuit breaker check
    if is_r2_circuit_breaker_open():
        metrics.inc_metric("r2_circuit_breaker_hits_total")
        log_event(
            service="r2_client",
            event="upload_blocked",
            status="blocked",
            message="R2 upload blocked by circuit breaker",
            trace_id=trace_id,
            extra={"bucket": bucket, "key": key}
        )
        return False, {"error": "Circuit breaker open", "blocked": True}
    
    metrics.inc_metric("r2_upload_requests_total")
    
    try:
        put_args = {
            "Bucket": bucket,
            "Key": key,
            "Body": data
        }
        if content_type:
            put_args["ContentType"] = content_type
        
        client.put_object(**put_args)
        latency_ms = round((time.time() - start_time) * 1000, 2)
        size_bytes = len(data) if data else 0
        
        # Phase 11-D: Success reporting
        metrics.inc_metric("r2_upload_success_total")
        metrics.observe_latency("r2_upload_latency_ms", latency_ms)
        report_r2_success()
        
        log_event(
            service="r2_client",
            event="upload_success",
            status="ok",
            message="R2 upload completed successfully",
            trace_id=trace_id,
            extra={
                "bucket": bucket,
                "key": key,
                "size_bytes": size_bytes,
                "latency_ms": latency_ms,
                "content_type": content_type
            }
        )
        
        return True, {
            "bucket": bucket,
            "key": key,
            "size_bytes": size_bytes,
            "latency_ms": latency_ms,
            "content_type": content_type
        }
        
    except Exception as e:
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        # Phase 11-D: Failure reporting
        metrics.inc_metric("r2_upload_failures_total")
        metrics.observe_latency("r2_upload_failure_latency_ms", latency_ms)
        report_r2_failure()
        
        error_info = {
            "error": str(e),
            "error_type": type(e).__name__,
            "bucket": bucket,
            "key": key,
            "latency_ms": latency_ms
        }
        
        log_event(
            service="r2_client",
            event="upload_failed",
            status="error",
            message="R2 upload failed",
            trace_id=trace_id,
            extra={
                **error_info,
                "stack": traceback.format_exc()
            }
        )
        
        return False, error_info

def download_from_r2(client, bucket, key, trace_id=None):
    """
    Download data from R2 with full observability.
    Returns (success, data_or_error) tuple.
    """
    start_time = time.time()
    trace_id = trace_id or f"r2_download_{int(time.time())}"
    
    # Phase 11-D: Circuit breaker check
    if is_r2_circuit_breaker_open():
        metrics.inc_metric("r2_circuit_breaker_hits_total")
        log_event(
            service="r2_client",
            event="download_blocked",
            status="blocked",
            message="R2 download blocked by circuit breaker",
            trace_id=trace_id,
            extra={"bucket": bucket, "key": key}
        )
        return False, {"error": "Circuit breaker open", "blocked": True}
    
    metrics.inc_metric("r2_download_requests_total")
    
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        latency_ms = round((time.time() - start_time) * 1000, 2)
        size_bytes = len(data) if data else 0
        
        # Phase 11-D: Success reporting
        metrics.inc_metric("r2_download_success_total")
        metrics.observe_latency("r2_download_latency_ms", latency_ms)
        report_r2_success()
        
        log_event(
            service="r2_client",
            event="download_success",
            status="ok",
            message="R2 download completed successfully",
            trace_id=trace_id,
            extra={
                "bucket": bucket,
                "key": key,
                "size_bytes": size_bytes,
                "latency_ms": latency_ms
            }
        )
        
        return True, data
        
    except Exception as e:
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        # Phase 11-D: Failure reporting
        metrics.inc_metric("r2_download_failures_total")
        metrics.observe_latency("r2_download_failure_latency_ms", latency_ms)
        report_r2_failure()
        
        error_info = {
            "error": str(e),
            "error_type": type(e).__name__,
            "bucket": bucket,
            "key": key,
            "latency_ms": latency_ms
        }
        
        log_event(
            service="r2_client",
            event="download_failed",
            status="error",
            message="R2 download failed",
            trace_id=trace_id,
            extra={
                **error_info,
                "stack": traceback.format_exc()
            }
        )
        
        return False, error_info

# --------------------------------------------------------------------------
# Backward Compatibility Wrapper
# --------------------------------------------------------------------------
def check_r2_connection_legacy():
    """
    Legacy compatibility wrapper for the original check_r2_connection function.
    Returns True if successful, False otherwise.
    """
    success, _ = check_r2_connection()
    return success

# Maintain backward compatibility for old callers (optional)
check_r2_connection_simple = check_r2_connection_legacy

# --------------------------------------------------------------------------
# Phase 11-F Compatibility Alias (Non-breaking)
# --------------------------------------------------------------------------
class R2Client:
    """
    Lightweight compatibility wrapper for Phase 11-F test scripts.
    Provides get_client() and check_connection() methods mapped to
    existing Phase 11-D functions.
    """
    def __init__(self):
        self.client = get_r2_client()

    def check_connection(self, bucket=None):
        return check_r2_connection(self.client, bucket=bucket)

    def upload(self, bucket, key, data, content_type=None):
        return upload_to_r2(self.client, bucket, key, data, content_type)

    def download(self, bucket, key):
        return download_from_r2(self.client, bucket, key)

# --- Phase 11-F compatibility aliases ---
RedisClient = RedisCircuitClient

