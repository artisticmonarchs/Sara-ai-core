"""
Cloudflare R2 Client Utilities - Phase 11-F Enhanced
Handles audio files, transcripts, metadata with versioned folder structure.
Enhanced with manifest indexing, checksum verification, and robust retry logic.
"""

import os
import time
import traceback
import functools
import hashlib
import json
import base64
import socket
import errno
import random
import re
from datetime import datetime, timezone
from typing import Tuple, Dict, Any, Optional, List
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError, ReadTimeoutError, ConnectTimeoutError, SSLError
from botocore.config import Config as BotoConfig
from redis_client import RedisCircuitClient

# --------------------------------------------------------------------------
# Configuration Constants
# --------------------------------------------------------------------------
CIRCUIT_BREAKER_THRESHOLD = 3  # Failures before opening circuit
CIRCUIT_BREAKER_COOLDOWN = 300  # 5 minutes in seconds
MULTIPART_THRESHOLD = 5 * 1024 * 1024  # 5 MB
MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB chunks
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds for exponential backoff
VERIFY_DOWNLOAD_MAX_BYTES = int(os.getenv("R2_VERIFY_DOWNLOAD_MAX_BYTES", "10485760"))  # 10MB
FAIL_OPEN_ON_LARGE = os.getenv("R2_CHECKSUM_FAIL_OPEN", "0") == "1"
ENV = os.getenv("R2_ENV", "default")

# Folder structure constants
CALLS_BASE_PATH = "calls"
MANIFEST_FILENAME = "manifest.json"
AUDIO_FILENAME = "audio.wav"
TRANSCRIPT_FILENAME = "transcript.json"
METADATA_FILENAME = "metadata.json"

# Key validation
_BAD_KEY = re.compile(r"(?:^/)|(?:\.\.)|(?://)|[\x00-\x1F]")

# --------------------------------------------------------------------------
# Phase 11-F: Metrics Integration
# --------------------------------------------------------------------------
try:
    import metrics_collector as metrics  # type: ignore
except ImportError:
    # Fallback for metrics unavailable
    class FallbackMetrics:
        def inc_metric(self, name, increment=1): 
            pass
        def increment_metric(self, name, increment=1): 
            pass
        def observe_latency(self, name, value): 
            pass
    metrics = FallbackMetrics()

# --------------------------------------------------------------------------
# Phase 11-F: Structured Logging
# --------------------------------------------------------------------------
try:
    from logging_utils import log_event
except ImportError:
    import logging
    _fallback_logger = logging.getLogger("r2_client")
    if not _fallback_logger.handlers:
        logging.basicConfig(level=logging.INFO)
    def log_event(service, event, status, message, trace_id=None, session_id=None, extra=None):
        level = logging.INFO if status in ("ok", "info", "success") else logging.WARNING if status in ("warn", "blocked") else logging.ERROR
        _fallback_logger.log(level, f"[{service}] {event} ({status}) {message} extra={extra or {}}")

# --------------------------------------------------------------------------
# Phase 2: External API Wrapper Import
# --------------------------------------------------------------------------
try:
    from core.utils.external_api import external_api_call
except ImportError:
    def external_api_call(service, operation, *args, trace_id=None, **kwargs):
        return operation(*args, **kwargs)

# --------------------------------------------------------------------------
# Phase 11-F: Utility Functions
# --------------------------------------------------------------------------
def generate_checksum(data: bytes) -> str:
    """Generate MD5 checksum for data verification."""
    return hashlib.md5(data).hexdigest()

def generate_sha256(data: bytes) -> str:
    """Generate SHA-256 checksum for data verification."""
    return hashlib.sha256(data).hexdigest()

def _b64_md5(data: bytes) -> str:
    """Generate base64-encoded MD5 for Content-MD5 header."""
    return base64.b64encode(hashlib.md5(data).digest()).decode("ascii")

def _as_str(v):
    """Convert bytes to string for Redis comparisons."""
    return v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v

def get_date_folder() -> str:
    """Get current date in YYYY/MM/DD format for folder structure (UTC)."""
    return datetime.now(timezone.utc).strftime("%Y/%m/%d")

def build_call_path(call_sid: str, date_folder: Optional[str] = None) -> str:
    """
    Build versioned folder path for call data.
    
    Args:
        call_sid: Unique call identifier
        date_folder: Optional date folder (defaults to current date)
    
    Returns:
        Full path: calls/{date}/{call_sid}/
    """
    if not date_folder:
        date_folder = get_date_folder()
    return f"{CALLS_BASE_PATH}/{date_folder}/{call_sid}/"

def build_file_key(call_sid: str, filename: str, date_folder: Optional[str] = None) -> str:
    """
    Build full S3 key for a call file.
    
    Args:
        call_sid: Unique call identifier
        filename: Target filename
        date_folder: Optional date folder
    
    Returns:
        Full S3 key: calls/{date}/{call_sid}/{filename}
    """
    call_path = build_call_path(call_sid, date_folder)
    return f"{call_path}{filename}"

def _validate_key(key: str):
    """Validate R2 key format."""
    if (not key
        or _BAD_KEY.search(key)
        or len(key) > 1024
        or key.endswith(' ')
        or key.startswith(' ')
        or key.endswith('/')):
        raise ValueError(f"Invalid R2 key: {key!r}")

# --------------------------------------------------------------------------
# Phase 11-F: Error Mapping for Retry Logic
# --------------------------------------------------------------------------
def is_retriable_error(error: Exception) -> bool:
    """
    Map R2 client errors into retriable vs non-retriable categories.
    
    Args:
        error: Exception from R2 operation
        
    Returns:
        True if error is retriable, False otherwise
    """
    if isinstance(error, ClientError):
        error_code = error.response.get('Error', {}).get('Code', '')
        http_status = error.response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        
        # Non-retriable errors: 4xx client errors (except 429), wrong-region, auth issues
        if error_code in ('PermanentRedirect', 'AuthorizationHeaderMalformed', 'AccessDenied', 'NoSuchBucket', 'ExpiredToken', 'InvalidAccessKeyId', 'SignatureDoesNotMatch'):
            return False
        if 400 <= http_status < 500 and http_status != 429:
            return False
            
        # Retriable errors: 5xx server errors, 429 rate limiting, timeouts
        if http_status >= 500:
            return True
        if error_code in ['RequestTimeout', 'RequestTimeoutException', 'SlowDown', 'ServiceUnavailable', 'Throttling']:
            return True
        if http_status == 429:  # Rate limiting
            return True
            
    # Network errors, connection issues are generally retriable
    if isinstance(error, (EndpointConnectionError, ReadTimeoutError, ConnectTimeoutError, TimeoutError, socket.timeout)):
        return True
        
    if isinstance(error, (ConnectionError,)):  # builtin
        return True
        
    # OS-level network errors
    if isinstance(error, OSError) and getattr(error, "errno", None) in {
        errno.ECONNRESET,
        errno.ENETUNREACH,
        errno.EHOSTUNREACH,
        getattr(errno, "ETIMEDOUT", 110),
        getattr(errno, "EPIPE", 32),
        getattr(errno, "ECONNABORTED", 103),
    }:
        return True
        
    # SSL errors (often transient)
    if isinstance(error, SSLError):
        return True
        
    # DNS resolution glitches
    if isinstance(error, socket.gaierror):
        return True
        
    # Default: non-retriable for unknown errors
    return False

# --------------------------------------------------------------------------
# Phase 11-F: Redis Circuit Breaker Support (Enhanced)
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
        key = f"circuit_breaker:{ENV}:r2:state"
        state = _as_str(client.get(key))
        
        # Check if circuit is open
        if state == "open":
            # Check if cooldown period has expired
            cooldown_key = f"circuit_breaker:{ENV}:r2:cooldown_until"
            cooldown_until = _as_str(client.get(cooldown_key))
            
            if cooldown_until:
                if time.time() < float(cooldown_until):
                    return True  # Still in cooldown
                else:
                    # Cooldown expired, transition to half-open
                    client.set(key, "half-open")
                    log_event(
                        service="r2_client",
                        event="circuit_breaker_half_open",
                        status="info",
                        message="Circuit breaker transitioning to half-open state"
                    )
                    return False
            return True
        
        return False
    except Exception as e:
        log_event(
            service="r2_client",
            event="circuit_breaker_check_error",
            status="warn",
            message="Error checking circuit breaker state",
            extra={"error": str(e)}
        )
        return False  # Proceed on Redis errors

def report_r2_failure():
    """Report R2 failure to circuit breaker system."""
    client = _redis()
    if not client:
        return
    
    try:
        fail_key = f"circuit_breaker:{ENV}:r2:failures"
        state_key = f"circuit_breaker:{ENV}:r2:state"
        
        # Increment failure count
        fail_count = client.incr(fail_key)
        
        # Set expiration on first failure
        if fail_count == 1:
            client.expire(fail_key, 60)  # 1-minute window for counting failures
        
        # Open circuit breaker if threshold exceeded
        if fail_count >= CIRCUIT_BREAKER_THRESHOLD:
            client.set(state_key, "open")
            cooldown_until = time.time() + CIRCUIT_BREAKER_COOLDOWN
            client.set(f"circuit_breaker:{ENV}:r2:cooldown_until", str(cooldown_until))
            
            metrics.inc_metric("r2_circuit_breaker_opened_total")
            log_event(
                service="r2_client",
                event="circuit_breaker_opened",
                status="warn",
                message=f"Circuit breaker opened after {fail_count} failures",
                extra={
                    "fail_count": fail_count,
                    "cooldown_seconds": CIRCUIT_BREAKER_COOLDOWN
                }
            )
    except Exception as e:
        log_event(
            service="r2_client",
            event="circuit_breaker_update_error",
            status="error",
            message="Failed to update circuit breaker state",
            extra={"error": str(e)}
        )

def report_r2_success():
    """Report R2 success to circuit breaker system."""
    client = _redis()
    if not client:
        return
    
    try:
        # Reset failure count on success
        fail_key = f"circuit_breaker:{ENV}:r2:failures"
        state_key = f"circuit_breaker:{ENV}:r2:state"
        
        current_state = _as_str(client.get(state_key))
        
        # If in half-open state, close the circuit
        if current_state == "half-open":
            client.set(state_key, "closed")
            client.delete(fail_key)
            client.delete(f"circuit_breaker:{ENV}:r2:cooldown_until")
            
            metrics.inc_metric("r2_circuit_breaker_closed_total")
            log_event(
                service="r2_client",
                event="circuit_breaker_closed",
                status="ok",
                message="Circuit breaker closed after successful operation"
            )
        elif current_state == "closed" or not current_state:
            # Normal operation, just reset failure count
            client.delete(fail_key)
    except Exception as e:
        log_event(
            service="r2_client",
            event="circuit_breaker_success_error",
            status="warn",
            message="Failed to report success to circuit breaker",
            extra={"error": str(e)}
        )

# --------------------------------------------------------------------------
# Enhanced Retry Decorator with Exponential Backoff and Jitter
# --------------------------------------------------------------------------
def r2_retry(max_attempts=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY):
    """
    Retry decorator for R2 operations with exponential backoff and jitter.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Base delay in seconds between retries (exponential backoff + jitter)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Check if error is retriable
                    if not is_retriable_error(e):
                        log_event(
                            service="r2_client",
                            event="non_retriable_error",
                            status="error",
                            message=f"Non-retriable error encountered, not retrying: {type(e).__name__}",
                            extra={
                                "function": func.__name__,
                                "error": str(e),
                                "error_type": type(e).__name__
                            }
                        )
                        raise
                    
                    if attempt < max_attempts:
                        base = delay * (2 ** (attempt - 1))
                        wait_time = base + random.uniform(0, base * 0.25)  # +25% jitter
                        log_event(
                            service="r2_client",
                            event="retry_attempt",
                            status="warn",
                            message=f"Retry attempt {attempt}/{max_attempts} in {wait_time:.2f}s after retriable error",
                            extra={
                                "function": func.__name__,
                                "attempt": attempt,
                                "wait_seconds": wait_time,
                                "error": str(e),
                                "error_type": type(e).__name__
                            }
                        )
                        time.sleep(wait_time)
                    else:
                        log_event(
                            service="r2_client",
                            event="retry_exhausted",
                            status="error",
                            message=f"All {max_attempts} retry attempts exhausted",
                            extra={
                                "function": func.__name__,
                                "error": str(e),
                                "error_type": type(e).__name__
                            }
                        )
            
            # Re-raise the last exception if all retries failed
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator

# --------------------------------------------------------------------------
# Multipart Upload Part Retry Support
# --------------------------------------------------------------------------
def _upload_part_with_retry(client, bucket: str, key: str, upload_id: str, part_number: int, body: bytes, max_attempts: int = 3, sse: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Upload a single multipart part with retry logic.
    
    Args:
        client: boto3 S3 client
        bucket: Bucket name
        key: Object key
        upload_id: Multipart upload ID
        part_number: Part number
        body: Part data
        max_attempts: Maximum retry attempts per part
        sse: Optional server-side encryption settings
    
    Returns:
        Upload part response
    """
    attempt = 1
    while True:
        try:
            # Include ContentMD5 for part-level integrity verification
            md5_b64 = _b64_md5(body)
            args = {
                "Bucket": bucket,
                "Key": key,
                "PartNumber": part_number,
                "UploadId": upload_id,
                "Body": body,
                "ContentMD5": md5_b64,
            }
            if sse:
                args.update(sse)
            return client.upload_part(**args)
        except Exception as e:
            if attempt >= max_attempts or not is_retriable_error(e):
                raise
            # Jittered linear backoff for part uploads
            sleep_time = attempt * (0.75 + random.random() * 0.5)
            time.sleep(sleep_time)
            attempt += 1

# --------------------------------------------------------------------------
# R2 Client Initialization with Observability and Timeouts
# --------------------------------------------------------------------------
def get_r2_client():
    """Create and return a Cloudflare R2-compatible boto3 client with observability."""
    trace_id = f"r2_init_{int(time.time())}"
    
    try:
        # Try to get config from Config class first
        try:
            from config import Config
            endpoint_url = f"https://{Config.R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
            access_key = Config.R2_ACCESS_KEY_ID
            secret_key = Config.R2_SECRET_ACCESS_KEY
            region = Config.R2_REGION
            # Get timeouts from config if available
            connect_timeout = getattr(Config, 'R2_CONNECT_TIMEOUT', 10)
            read_timeout = getattr(Config, 'R2_READ_TIMEOUT', 30)
        except Exception:
            # Fallback to environment variables
            endpoint_url = os.getenv("R2_ENDPOINT_URL")
            access_key = os.getenv("R2_ACCESS_KEY_ID")
            secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
            region = os.getenv("R2_REGION", "auto")
            connect_timeout = int(os.getenv("R2_CONNECT_TIMEOUT", "10"))
            read_timeout = int(os.getenv("R2_READ_TIMEOUT", "30"))
        
        # Guard against missing credentials
        if not endpoint_url:
            raise ValueError("Missing R2 endpoint URL")
        if not access_key or not secret_key:
            raise ValueError("Missing R2 credentials")
        
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=BotoConfig(
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                retries={'max_attempts': 0},  # We handle retries ourselves
                s3={'addressing_style': 'virtual'},
                signature_version='s3v4',
                user_agent_extra="r2-client/11-F",
                max_pool_connections=64,  # Increased for parallel operations
            )
        )
        
        metrics.inc_metric("r2_client_initializations_total")
        log_event(
            service="r2_client",
            event="client_initialized",
            status="ok",
            message="R2 client initialized successfully",
            trace_id=trace_id,
            extra={
                "connect_timeout": connect_timeout,
                "read_timeout": read_timeout,
                "max_pool_connections": 64
            }
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
# Enhanced R2 Connection Check with /system_status Format
# --------------------------------------------------------------------------
def check_r2_connection(client=None, bucket=None, trace_id=None) -> Dict[str, Any]:
    """
    Verify R2 connectivity by attempting to list buckets or check specific bucket.
    
    Note: If a default bucket is configured, this will check bucket access.
    Otherwise, it will perform a lightweight auth probe.
    
    Returns:
        {"ok": bool, "latency_ms": float, "status": str, ...}
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
        return {
            "ok": False,
            "status": "circuit_breaker_open",
            "latency_ms": 0,
            "error": "Circuit breaker open",
            "blocked": True
        }
    
    metrics.inc_metric("r2_connection_checks_total")
    
    try:
        if client is None:
            client = get_r2_client()
        
        # Get bucket from config if not provided
        if not bucket:
            try:
                from config import Config
                bucket = Config.R2_BUCKET_NAME
            except Exception:
                bucket = os.getenv("R2_BUCKET_NAME")
        
        # Phase 2: Use external API wrapper for R2 operations
        def list_objects():
            if bucket:
                return client.list_objects_v2(Bucket=bucket, MaxKeys=1)
            else:
                # Lightweight auth probe when no bucket is configured
                try:
                    client.list_objects_v2(Bucket="__r2_connection_probe__", MaxKeys=0)
                except ClientError as ce:
                    code = ce.response.get("Error", {}).get("Code")
                    if code in ("NoSuchBucket", "InvalidBucketName"):
                        return {"Probe": "ok"}
                    raise
                return {"Probe": "ok"}

        # If bucket is specified, check bucket access; otherwise perform auth probe
        operation = "bucket_access" if bucket else "auth_probe"
        target = bucket if bucket else "auth"
        
        external_api_call(
            "r2",
            list_objects,
            trace_id=trace_id
        )
        
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
        
        return {
            "ok": True,
            "status": "healthy",
            "latency_ms": latency_ms,
            "operation": operation,
            "target": target,
            "bucket": bucket if bucket else None
        }
        
    except Exception as e:
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        # Phase 11-D: Failure reporting - only trip breaker on retriable errors
        metrics.inc_metric("r2_connection_check_failures_total")
        metrics.observe_latency("r2_connection_check_failure_latency_ms", latency_ms)
        if is_retriable_error(e):
            report_r2_failure()
        
        error_info = {
            "error": str(e),
            "error_type": type(e).__name__,
            "latency_ms": latency_ms,
            "operation": "bucket_access" if bucket else "auth_probe",
            "target": bucket if bucket else "auth",
            "timed_out": isinstance(e, (ReadTimeoutError, ConnectTimeoutError, socket.timeout, TimeoutError))
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
        
        return {
            "ok": False,
            "status": "error",
            "latency_ms": latency_ms,
            "error": str(e),
            "error_type": type(e).__name__,
            "operation": "bucket_access" if bucket else "auth_probe",
            "target": bucket if bucket else "auth",
            "timed_out": isinstance(e, (ReadTimeoutError, ConnectTimeoutError, socket.timeout, TimeoutError))
        }

# --------------------------------------------------------------------------
# Multipart Upload Support
# --------------------------------------------------------------------------
def _upload_multipart(client, bucket: str, key: str, data: bytes, content_type: Optional[str], trace_id: str, sse: Optional[Dict[str, str]] = None, sha256_checksum: Optional[str] = None, cache_control: Optional[str] = None, content_disposition: Optional[str] = None, content_encoding: Optional[str] = None) -> Tuple[bool, Dict[str, Any]]:
    """
    Upload large files using S3 multipart upload.
    
    Args:
        client: boto3 S3 client
        bucket: Target bucket name
        key: Object key
        data: File data as bytes
        content_type: Optional content type
        trace_id: Trace ID for logging
        sse: Optional server-side encryption settings
        sha256_checksum: SHA-256 checksum to store as metadata
        cache_control: Optional Cache-Control header
        content_disposition: Optional Content-Disposition header
        content_encoding: Optional Content-Encoding header
    
    Returns:
        (success, metadata) tuple
    """
    start_time = time.time()
    
    try:
        # Initiate multipart upload
        create_args = {"Bucket": bucket, "Key": key}
        if content_type:
            create_args["ContentType"] = content_type
        if sse:
            create_args.update(sse)
        if sha256_checksum:
            create_args["Metadata"] = {"sha256": sha256_checksum}
        if cache_control:
            create_args["CacheControl"] = cache_control
        if content_disposition:
            create_args["ContentDisposition"] = content_disposition
        if content_encoding:
            create_args["ContentEncoding"] = content_encoding
        
        response = client.create_multipart_upload(**create_args)
        upload_id = response["UploadId"]
        
        log_event(
            service="r2_client",
            event="multipart_upload_initiated",
            status="ok",
            message=f"Initiated multipart upload for {key}",
            trace_id=trace_id,
            extra={"upload_id": upload_id, "file_key": key, "size_bytes": len(data)}
        )
        
        # Upload parts
        parts = []
        part_number = 1
        offset = 0
        
        while offset < len(data):
            chunk = data[offset:offset + MULTIPART_CHUNK_SIZE]
            
            part_response = _upload_part_with_retry(
                client, bucket, key, upload_id, part_number, chunk, sse=sse
            )
            
            parts.append({
                "PartNumber": part_number,
                "ETag": part_response["ETag"]
            })
            
            # Only log every 10th part or last part to reduce noise
            if part_number % 10 == 0 or offset + MULTIPART_CHUNK_SIZE >= len(data):
                log_event(
                    service="r2_client",
                    event="multipart_part_uploaded",
                    status="ok",
                    message=f"Uploaded part {part_number} for {key}",
                    trace_id=trace_id,
                    extra={
                        "upload_id": upload_id,
                        "part_number": part_number,
                        "part_size": len(chunk),
                        "file_key": key
                    }
                )
            
            part_number += 1
            offset += MULTIPART_CHUNK_SIZE
        
        # Complete multipart upload
        client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        
        latency_ms = round((time.time() - start_time) * 1000, 2)
        size_bytes = len(data)
        
        log_event(
            service="r2_client",
            event="multipart_upload_completed",
            status="ok",
            message=f"Completed multipart upload for {key}",
            trace_id=trace_id,
            extra={
                "upload_id": upload_id,
                "total_parts": len(parts),
                "size_bytes": size_bytes,
                "latency_ms": latency_ms,
                "file_key": key
            }
        )
        
        return True, {
            "bucket": bucket,
            "key": key,
            "size_bytes": size_bytes,
            "latency_ms": latency_ms,
            "content_type": content_type,
            "upload_type": "multipart",
            "parts": len(parts)
        }
        
    except Exception as e:
        # Abort multipart upload on error
        try:
            if 'upload_id' in locals():
                client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id
                )
                log_event(
                    service="r2_client",
                    event="multipart_upload_aborted",
                    status="warn",
                    message=f"Aborted multipart upload for {key}",
                    trace_id=trace_id,
                    extra={"upload_id": upload_id, "file_key": key, "error": str(e), "size_bytes": len(data)}
                )
        except Exception as abort_error:
            log_event(
                service="r2_client",
                event="multipart_upload_abort_failed",
                status="error",
                message=f"Failed to abort multipart upload for {key}",
                trace_id=trace_id,
                extra={"upload_id": upload_id, "file_key": key, "error": str(abort_error), "size_bytes": len(data)}
            )
        
        raise

# --------------------------------------------------------------------------
# Phase 11-F: Checksum Verification
# --------------------------------------------------------------------------
def verify_upload_checksum(client, bucket: str, key: str, original_checksum: str, trace_id: str,
                           fail_open_on_large: Optional[bool] = None, sse: Optional[Dict[str, str]] = None) -> bool:
    """
    Verify uploaded file integrity by comparing checksums.
    
    Args:
        client: boto3 S3 client
        bucket: Bucket name
        key: Object key to verify
        original_checksum: Expected MD5 checksum
        trace_id: Trace ID for logging
        fail_open_on_large: Override for large file verification policy
        sse: Optional server-side encryption settings
    
    Returns:
        True if checksums match, False otherwise
    """
    if fail_open_on_large is None:
        fail_open_on_large = FAIL_OPEN_ON_LARGE
        
    try:
        # Get object metadata which includes ETag (MD5 for standard uploads)
        head_kwargs = {"Bucket": bucket, "Key": key}
        if sse:
            head_kwargs.update(sse)
        response = client.head_object(**head_kwargs)
        etag = response.get('ETag', '').strip('"')
        if etag.startswith("W/"):
            etag = etag[2:].strip('"')
        
        # Don't trust ETag if server-side encryption is enabled
        if response.get("ServerSideEncryption"):
            etag = ""  # Force download path
        
        # For multipart uploads or encrypted objects, ETag is not the MD5, so we need to download and verify
        if '-' in etag or not etag:  # Multipart upload ETag contains hyphen or empty due to encryption
            # Check if file is too large for download verification
            size = response.get("ContentLength", 0)
            if size and size > VERIFY_DOWNLOAD_MAX_BYTES:
                log_event(
                    service="r2_client",
                    event="checksum_skipped_large",
                    status="warn",
                    message=f"Skipping download checksum for large object ({size} bytes)",
                    trace_id=trace_id,
                    extra={
                        "file_key": key,
                        "size_bytes": size,
                        "threshold": VERIFY_DOWNLOAD_MAX_BYTES,
                        "fail_open": fail_open_on_large
                    }
                )
                return True if fail_open_on_large else False
            
            # Disable checksum verification during download to avoid circular verification
            download_success, downloaded_data = download_from_r2(
                client, bucket, key, trace_id, verify_checksum=False, sse=sse
            )
            if download_success:
                downloaded_checksum = generate_checksum(downloaded_data)
                checksum_match = (downloaded_checksum == original_checksum)
                
                log_event(
                    service="r2_client",
                    event="checksum_verification",
                    status="ok" if checksum_match else "error",
                    message=f"Checksum verification {'passed' if checksum_match else 'failed'} for {key}",
                    trace_id=trace_id,
                    extra={
                        "file_key": key,
                        "expected_checksum": original_checksum,
                        "actual_checksum": downloaded_checksum,
                        "checksum_match": checksum_match,
                        "verification_method": "download"
                    }
                )
                return checksum_match
            else:
                log_event(
                    service="r2_client",
                    event="checksum_verification_failed",
                    status="error",
                    message=f"Failed to download file for checksum verification: {key}",
                    trace_id=trace_id,
                    extra={"file_key": key}
                )
                return False
        else:
            # For standard uploads, ETag should be the MD5
            checksum_match = (etag == original_checksum)
            
            log_event(
                service="r2_client",
                event="checksum_verification",
                status="ok" if checksum_match else "error",
                message=f"Checksum verification {'passed' if checksum_match else 'failed'} for {key}",
                trace_id=trace_id,
                extra={
                    "file_key": key,
                    "expected_checksum": original_checksum,
                    "actual_checksum": etag,
                    "checksum_match": checksum_match,
                    "verification_method": "etag"
                }
            )
            return checksum_match
            
    except Exception as e:
        log_event(
            service="r2_client",
            event="checksum_verification_error",
            status="error",
            message=f"Error during checksum verification for {key}",
            trace_id=trace_id,
            extra={
                "file_key": key,
                "error": str(e)
            }
        )
        return False

# --------------------------------------------------------------------------
# Enhanced R2 Operations with Observability, Retry, and Checksum Verification
# --------------------------------------------------------------------------
@r2_retry(max_attempts=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY)
def upload_to_r2(client, bucket: str, key: str, data: bytes, content_type: Optional[str] = None, trace_id: Optional[str] = None, verify_checksum: bool = True, sse: Optional[Dict[str, str]] = None, cache_control: Optional[str] = None, content_disposition: Optional[str] = None, content_encoding: Optional[str] = None) -> Tuple[bool, Dict[str, Any]]:
    """
    Upload data to R2 with full observability, retry logic, multipart support, and checksum verification.
    Returns (success, metadata) tuple.
    """
    start_time = time.time()
    trace_id = trace_id or f"r2_upload_{int(time.time())}"
    
    # Validate key format
    _validate_key(key)
    
    # Normalize data to handle None input
    if data is None:
        log_event(
            service="r2_client",
            event="upload_empty_body",
            status="warn",
            message=f"Empty payload uploaded for {key}",
            trace_id=trace_id,
            extra={"file_key": key}
        )
        data = b""
    
    # Phase 11-D: Circuit breaker check
    if is_r2_circuit_breaker_open():
        metrics.inc_metric("r2_circuit_breaker_hits_total")
        log_event(
            service="r2_client",
            event="upload_blocked",
            status="blocked",
            message="R2 upload blocked by circuit breaker",
            trace_id=trace_id,
            extra={"bucket": bucket, "file_key": key, "operation": "upload"}
        )
        return False, {"error": "Circuit breaker open", "blocked": True}
    
    metrics.inc_metric("r2_upload_requests_total")
    size_bytes = len(data) if data else 0
    original_checksum = generate_checksum(data) if verify_checksum else None
    sha256_checksum = generate_sha256(data)
    
    try:
        # Precompute metadata once (avoid post-upload copy_object roundtrip)
        meta = {}
        if sha256_checksum:
            meta["sha256"] = sha256_checksum
        
        # Determine upload method based on size
        if size_bytes > MULTIPART_THRESHOLD:
            log_event(
                service="r2_client",
                event="upload_multipart_selected",
                status="info",
                message=f"Using multipart upload for large file: {key}",
                trace_id=trace_id,
                extra={"size_bytes": size_bytes, "threshold": MULTIPART_THRESHOLD, "file_key": key}
            )
            # Pass sha256 via CreateMultipartUpload metadata inside _upload_multipart
            success, metadata = _upload_multipart(client, bucket, key, data, content_type, trace_id, sse, sha256_checksum, cache_control, content_disposition, content_encoding)
        else:
            # Standard upload for smaller files
            put_args = {
                "Bucket": bucket,
                "Key": key,
                "Body": data
            }
            if content_type:
                put_args["ContentType"] = content_type
            # Send MD5 even for empty bodies
            put_args["ContentMD5"] = _b64_md5(data)
            if sse:
                put_args.update(sse)
            if meta:
                put_args["Metadata"] = meta
            if cache_control:
                put_args["CacheControl"] = cache_control
            if content_disposition:
                put_args["ContentDisposition"] = content_disposition
            if content_encoding:
                put_args["ContentEncoding"] = content_encoding
            
            # Phase 2: Use external API wrapper for R2 put operations
            def put_object():
                return client.put_object(**put_args)

            external_api_call(
                "r2",
                put_object,
                trace_id=trace_id
            )
            
            latency_ms = round((time.time() - start_time) * 1000, 2)
            
            success = True
            metadata = {
                "bucket": bucket,
                "key": key,
                "size_bytes": size_bytes,
                "latency_ms": latency_ms,
                "content_type": content_type,
                "upload_type": "standard"
            }
        
        # Record upload latency metric
        metrics.observe_latency("r2_upload_latency_ms", metadata.get("latency_ms", 0))
        
        if success and verify_checksum and original_checksum:
            # Verify upload integrity
            verification_success = verify_upload_checksum(client, bucket, key, original_checksum, trace_id, sse=sse)
            if not verification_success:
                log_event(
                    service="r2_client",
                    event="upload_checksum_mismatch",
                    status="error",
                    message=f"Upload checksum verification failed for {key}",
                    trace_id=trace_id,
                    extra={"file_key": key, "expected_checksum": original_checksum}
                )
                # Optionally delete the corrupted upload
                try:
                    client.delete_object(Bucket=bucket, Key=key)
                    log_event(
                        service="r2_client",
                        event="corrupted_upload_deleted",
                        status="warn",
                        message=f"Deleted corrupted upload: {key}",
                        trace_id=trace_id,
                        extra={"file_key": key}
                    )
                except Exception as delete_error:
                    log_event(
                        service="r2_client",
                        event="corrupted_upload_delete_failed",
                        status="error",
                        message=f"Failed to delete corrupted upload: {key}",
                        trace_id=trace_id,
                        extra={"file_key": key, "error": str(delete_error)}
                    )
                
                raise Exception(f"Upload checksum verification failed for {key}")
        
        # Phase 11-D: Success reporting
        if success:
            metrics.inc_metric("r2_upload_success_total")
            report_r2_success()
            
            # Include checksums in metadata for downstream use
            if original_checksum:
                metadata["md5"] = original_checksum
            if sha256_checksum:
                metadata["sha256"] = sha256_checksum
            
            log_event(
                service="r2_client",
                event="upload_success",
                status="ok",
                message="R2 upload completed successfully",
                trace_id=trace_id,
                extra={
                    "bucket": bucket,
                    "file_key": key,
                    "operation": "upload",
                    "size_bytes": size_bytes,
                    "latency_ms": metadata.get("latency_ms"),
                    "content_type": content_type,
                    "upload_type": metadata.get("upload_type"),
                    "checksum_verified": verify_checksum and original_checksum is not None,
                    "sha256_stored": sha256_checksum is not None
                }
            )
        
        return success, metadata
        
    except Exception as e:
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        # Phase 11-D: Failure reporting - only trip breaker on retriable errors
        metrics.inc_metric("r2_upload_failures_total")
        metrics.observe_latency("r2_upload_failure_latency_ms", latency_ms)
        should_break = is_retriable_error(e)
        if should_break:
            report_r2_failure()
        else:
            metrics.inc_metric("r2_upload_nonretriable_failures_total")
        
        error_info = {
            "error": str(e),
            "error_type": type(e).__name__,
            "bucket": bucket,
            "key": key,
            "latency_ms": latency_ms,
            "size_bytes": size_bytes,
            "timed_out": isinstance(e, (ReadTimeoutError, ConnectTimeoutError, socket.timeout, TimeoutError))
        }
        
        log_event(
            service="r2_client",
            event="upload_failed",
            status="error",
            message="R2 upload failed",
            trace_id=trace_id,
            extra={
                **error_info,
                "file_key": key,
                "operation": "upload",
                "stack": traceback.format_exc()
            }
        )
        
        raise  # Re-raise for retry decorator

@r2_retry(max_attempts=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY)
def download_from_r2(client, bucket: str, key: str, trace_id: Optional[str] = None,
                     verify_checksum: bool = True, sse: Optional[Dict[str, str]] = None) -> Tuple[bool, bytes]:
    """
    Download data from R2 with full observability and retry logic.
    Returns (success, data_or_error) tuple.
    """
    start_time = time.time()
    trace_id = trace_id or f"r2_download_{int(time.time())}"
    
    # Validate key format
    _validate_key(key)
    
    # Phase 11-D: Circuit breaker check
    if is_r2_circuit_breaker_open():
        metrics.inc_metric("r2_circuit_breaker_hits_total")
        log_event(
            service="r2_client",
            event="download_blocked",
            status="blocked",
            message="R2 download blocked by circuit breaker",
            trace_id=trace_id,
            extra={"bucket": bucket, "file_key": key, "operation": "download"}
        )
        return False, b""
    
    metrics.inc_metric("r2_download_requests_total")
    
    try:
        # Phase 2: Use external API wrapper for R2 get operations
        def get_object():
            kwargs = {"Bucket": bucket, "Key": key}
            if sse:
                kwargs.update(sse)
            return client.get_object(**kwargs)

        response = external_api_call("r2", get_object, trace_id=trace_id)
        body = response["Body"]
        try:
            data = body.read()
        finally:
            try:
                body.close()
            except Exception:
                pass
        
        latency_ms = round((time.time() - start_time) * 1000, 2)
        size_bytes = len(data) if data else 0
        
        # Verify checksum if requested
        if verify_checksum and data:
            # Get stored SHA-256 from metadata if available
            stored_sha256 = None
            try:
                head_kwargs = {"Bucket": bucket, "Key": key}
                if sse:
                    head_kwargs.update(sse)
                head_response = client.head_object(**head_kwargs)
                stored_sha256 = head_response.get('Metadata', {}).get('sha256')
            except Exception:
                pass
            
            if stored_sha256:
                # Verify against stored SHA-256
                computed_sha256 = generate_sha256(data)
                if computed_sha256 != stored_sha256:
                    metrics.inc_metric("r2_checksum_mismatch_total")
                    log_event(
                        service="r2_client",
                        event="checksum_mismatch",
                        status="error",
                        message=f"SHA-256 checksum mismatch for {key}",
                        trace_id=trace_id,
                        extra={
                            "file_key": key,
                            "expected_sha256": stored_sha256,
                            "computed_sha256": computed_sha256,
                            "size_bytes": size_bytes
                        }
                    )
                    raise Exception(f"SHA-256 checksum mismatch for {key}")
                else:
                    log_event(
                        service="r2_client",
                        event="checksum_verified",
                        status="ok",
                        message=f"SHA-256 checksum verified for {key}",
                        trace_id=trace_id,
                        extra={
                            "file_key": key,
                            "sha256": computed_sha256,
                            "size_bytes": size_bytes
                        }
                    )
            else:
                # Fallback: compare ETag for likely single-part objects
                try:
                    head_kwargs = {"Bucket": bucket, "Key": key}
                    if sse:
                        head_kwargs.update(sse)
                    _head = client.head_object(**head_kwargs)
                    et = _head.get('ETag', '').strip('"')
                except Exception:
                    et = ""
                if et.startswith("W/"):
                    et = et[2:].strip('"')
                if et and '-' not in et:
                    md5_hex = generate_checksum(data)
                    if md5_hex != et:
                        metrics.inc_metric("r2_checksum_mismatch_total")
                        log_event(
                            service="r2_client",
                            event="checksum_mismatch",
                            status="error",
                            message=f"MD5/ETag mismatch for {key}",
                            trace_id=trace_id,
                            extra={"file_key": key, "expected_etag": et, "computed_md5": md5_hex}
                        )
                        raise Exception(f"MD5/ETag checksum mismatch for {key}")
        
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
                "file_key": key,
                "operation": "download",
                "size_bytes": size_bytes,
                "latency_ms": latency_ms,
                "checksum_verified": verify_checksum and data is not None
            }
        )
        
        return True, data
        
    except Exception as e:
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        # Phase 11-D: Failure reporting - only trip breaker on retriable errors
        metrics.inc_metric("r2_download_failures_total")
        metrics.observe_latency("r2_download_failure_latency_ms", latency_ms)
        should_break = is_retriable_error(e)
        if should_break:
            report_r2_failure()
        else:
            metrics.inc_metric("r2_download_nonretriable_failures_total")
        
        error_info = {
            "error": str(e),
            "error_type": type(e).__name__,
            "bucket": bucket,
            "key": key,
            "latency_ms": latency_ms,
            "timed_out": isinstance(e, (ReadTimeoutError, ConnectTimeoutError, socket.timeout, TimeoutError))
        }
        
        log_event(
            service="r2_client",
            event="download_failed",
            status="error",
            message="R2 download failed",
            trace_id=trace_id,
            extra={
                **error_info,
                "file_key": key,
                "operation": "download",
                "stack": traceback.format_exc()
            }
        )
        
        raise  # Re-raise for retry decorator

# --------------------------------------------------------------------------
# Phase 11-F: Call Data Management with Versioned Folder Structure
# --------------------------------------------------------------------------
def upload_call_data(
    call_sid: str,
    audio_data: Optional[bytes] = None,
    transcript_data: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    date_folder: Optional[str] = None,
    trace_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Upload complete call data package to R2 with versioned folder structure.
    
    Args:
        call_sid: Unique call identifier
        audio_data: Raw audio bytes (optional)
        transcript_data: Transcript JSON data (optional)
        metadata: Call metadata (optional)
        date_folder: Date folder override (defaults to current date)
        trace_id: Trace ID for logging
    
    Returns:
        Dictionary with upload results and manifest
    """
    start_time = time.time()
    trace_id = trace_id or f"call_upload_{int(time.time())}"
    client = get_r2_client()
    
    try:
        # Get bucket from config
        try:
            from config import Config
            bucket = Config.R2_BUCKET_NAME
        except Exception:
            bucket = os.getenv("R2_BUCKET_NAME")
        
        if not bucket:
            raise ValueError("R2_BUCKET_NAME not configured")
        
        # Build file paths
        audio_key = build_file_key(call_sid, AUDIO_FILENAME, date_folder) if audio_data else None
        transcript_key = build_file_key(call_sid, TRANSCRIPT_FILENAME, date_folder) if transcript_data else None
        metadata_key = build_file_key(call_sid, METADATA_FILENAME, date_folder) if metadata else None
        manifest_key = build_file_key(call_sid, MANIFEST_FILENAME, date_folder)
        
        upload_results = {}
        manifest = {
            "call_sid": call_sid,
            "date_folder": date_folder or get_date_folder(),
            "upload_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "files": {}
        }
        
        # Upload audio file
        if audio_data and audio_key:
            success, audio_metadata = upload_to_r2(
                client, bucket, audio_key, audio_data, 
                content_type="audio/wav", trace_id=trace_id
            )
            if success:
                upload_results["audio"] = audio_metadata
                manifest["files"]["audio"] = {
                    "key": audio_key,
                    "size_bytes": len(audio_data),
                    "content_type": "audio/wav",
                    "checksum": generate_checksum(audio_data),
                    "sha256": generate_sha256(audio_data)
                }
        
        # Upload transcript
        if transcript_data and transcript_key:
            transcript_bytes = json.dumps(transcript_data, indent=2, ensure_ascii=False, sort_keys=True).encode('utf-8')
            success, transcript_metadata = upload_to_r2(
                client, bucket, transcript_key, transcript_bytes,
                content_type="application/json", trace_id=trace_id
            )
            if success:
                upload_results["transcript"] = transcript_metadata
                manifest["files"]["transcript"] = {
                    "key": transcript_key,
                    "size_bytes": len(transcript_bytes),
                    "content_type": "application/json",
                    "checksum": generate_checksum(transcript_bytes),
                    "sha256": generate_sha256(transcript_bytes)
                }
        
        # Upload metadata
        if metadata and metadata_key:
            metadata_bytes = json.dumps(metadata, indent=2, ensure_ascii=False, sort_keys=True).encode('utf-8')
            success, metadata_metadata = upload_to_r2(
                client, bucket, metadata_key, metadata_bytes,
                content_type="application/json", trace_id=trace_id
            )
            if success:
                upload_results["metadata"] = metadata_metadata
                manifest["files"]["metadata"] = {
                    "key": metadata_key,
                    "size_bytes": len(metadata_bytes),
                    "content_type": "application/json",
                    "checksum": generate_checksum(metadata_bytes),
                    "sha256": generate_sha256(metadata_bytes)
                }
        
        # Upload manifest
        manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True).encode('utf-8')
        success, manifest_metadata = upload_to_r2(
            client, bucket, manifest_key, manifest_bytes,
            content_type="application/json", trace_id=trace_id
        )
        if success:
            upload_results["manifest"] = manifest_metadata
            manifest["files"]["manifest"] = {
                "key": manifest_key,
                "size_bytes": len(manifest_bytes),
                "content_type": "application/json",
                "checksum": generate_checksum(manifest_bytes),
                "sha256": generate_sha256(manifest_bytes)
            }
        
        total_latency_ms = round((time.time() - start_time) * 1000, 2)
        
        log_event(
            service="r2_client",
            event="call_data_upload_complete",
            status="ok",
            message=f"Call data upload completed for {call_sid}",
            trace_id=trace_id,
            extra={
                "call_sid": call_sid,
                "date_folder": date_folder,
                "files_uploaded": list(upload_results.keys()),
                "total_latency_ms": total_latency_ms,
                "bucket": bucket
            }
        )
        
        return {
            "success": True,
            "call_sid": call_sid,
            "upload_results": upload_results,
            "manifest": manifest,
            "total_latency_ms": total_latency_ms
        }
        
    except Exception as e:
        log_event(
            service="r2_client",
            event="call_data_upload_failed",
            status="error",
            message=f"Call data upload failed for {call_sid}",
            trace_id=trace_id,
            extra={
                "call_sid": call_sid,
                "error": str(e),
                "stack": traceback.format_exc()
            }
        )
        return {
            "success": False,
            "call_sid": call_sid,
            "error": str(e),
            "error_type": type(e).__name__
        }

def get_call_manifest(client, bucket: str, call_sid: str, date_folder: Optional[str] = None,
                      trace_id: Optional[str] = None, sse: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
    """
    Retrieve call manifest from R2.
    
    Args:
        client: boto3 S3 client
        bucket: Bucket name
        call_sid: Call identifier
        date_folder: Date folder (optional)
        trace_id: Trace ID for logging
        sse: Optional server-side encryption settings
    
    Returns:
        Manifest dictionary or None if not found
    """
    trace_id = trace_id or f"get_manifest_{int(time.time())}"
    manifest_key = build_file_key(call_sid, MANIFEST_FILENAME, date_folder)
    
    try:
        success, data = download_from_r2(client, bucket, manifest_key, trace_id, sse=sse)
        if success and data:
            manifest = json.loads(data.decode('utf-8'))
            log_event(
                service="r2_client",
                event="manifest_retrieved",
                status="ok",
                message=f"Retrieved manifest for call {call_sid}",
                trace_id=trace_id,
                extra={"call_sid": call_sid, "manifest_key": manifest_key}
            )
            return manifest
        return None
    except Exception as e:
        log_event(
            service="r2_client",
            event="manifest_retrieval_failed",
            status="error",
            message=f"Failed to retrieve manifest for call {call_sid}",
            trace_id=trace_id,
            extra={
                "call_sid": call_sid,
                "manifest_key": manifest_key,
                "error": str(e)
            }
        )
        return None

def download_call_data(call_sid: str, date_folder: Optional[str] = None,
                       trace_id: Optional[str] = None, sse: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Download complete call data package from R2.
    
    Args:
        call_sid: Call identifier
        date_folder: Date folder (optional)
        trace_id: Trace ID for logging
        sse: Optional server-side encryption settings
    
    Returns:
        Dictionary with downloaded call data
    """
    trace_id = trace_id or f"call_download_{int(time.time())}"
    client = get_r2_client()
    
    try:
        # Get bucket from config
        try:
            from config import Config
            bucket = Config.R2_BUCKET_NAME
        except Exception:
            bucket = os.getenv("R2_BUCKET_NAME")
        
        if not bucket:
            raise ValueError("R2_BUCKET_NAME not configured")
        
        # Get manifest first
        manifest = get_call_manifest(client, bucket, call_sid, date_folder, trace_id, sse=sse)
        if not manifest:
            return {"success": False, "error": "Manifest not found", "call_sid": call_sid}
        
        downloaded_data = {"manifest": manifest, "files": {}}
        
        # Download files listed in manifest
        for file_type, file_info in manifest.get("files", {}).items():
            if file_type == "manifest":
                continue  # Skip manifest itself
                
            file_key = file_info.get("key")
            if file_key:
                success, data = download_from_r2(client, bucket, file_key, trace_id, verify_checksum=True, sse=sse)
                if success:
                    if file_type in ["transcript", "metadata"]:
                        # Parse JSON files
                        try:
                            downloaded_data["files"][file_type] = json.loads(data.decode('utf-8'))
                        except json.JSONDecodeError as e:
                            downloaded_data["files"][file_type] = {"raw_data": data, "parse_error": str(e)}
                    else:
                        # Keep binary data as-is
                        downloaded_data["files"][file_type] = data
                else:
                    downloaded_data["files"][file_type] = {"error": "Download failed"}
        
        log_event(
            service="r2_client",
            event="call_data_download_complete",
            status="ok",
            message=f"Call data download completed for {call_sid}",
            trace_id=trace_id,
            extra={
                "call_sid": call_sid,
                "files_downloaded": list(downloaded_data["files"].keys())
            }
        )
        
        downloaded_data["success"] = True
        return downloaded_data
        
    except Exception as e:
        log_event(
            service="r2_client",
            event="call_data_download_failed",
            status="error",
            message=f"Call data download failed for {call_sid}",
            trace_id=trace_id,
            extra={
                "call_sid": call_sid,
                "error": str(e),
                "stack": traceback.format_exc()
            }
        )
        return {
            "success": False,
            "call_sid": call_sid,
            "error": str(e),
            "error_type": type(e).__name__
        }

# --------------------------------------------------------------------------
# Backward Compatibility Wrapper
# --------------------------------------------------------------------------
def check_r2_connection_legacy():
    """
    Legacy compatibility wrapper for the original check_r2_connection function.
    Returns True if successful, False otherwise.
    """
    result = check_r2_connection()
    return result.get("ok", False)

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

    def upload(self, bucket, key, data, content_type=None, verify_checksum=True, sse=None, cache_control=None, content_disposition=None, content_encoding=None):
        return upload_to_r2(self.client, bucket, key, data, content_type, verify_checksum=verify_checksum, sse=sse, cache_control=cache_control, content_disposition=content_disposition, content_encoding=content_encoding)

    def download(self, bucket, key, sse=None):
        return download_from_r2(self.client, bucket, key, sse=sse)

# --- Phase 11-F compatibility aliases ---
RedisClient = RedisCircuitClient