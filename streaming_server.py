"""
streaming_server.py — Phase 11-F (Unified Metrics Registry + Global Sync)
Sara AI Core — Streaming Service

Uses unified metrics_registry.REGISTRY as the shared Prometheus registry

Restores persisted snapshot from Redis into metrics_collector on startup

Starts background metrics sync for global aggregation

/metrics returns generate_latest(REGISTRY) for unified metrics

/metrics_snapshot persists combined snapshot to Redis for cross-service restore

Adds RedisCircuitBreaker to isolate Redis outages

Retains SSE streaming, Twilio webhook, health endpoints, and structured logging

Enhanced with SSE heartbeat, connection lifecycle logging, and graceful disconnect handling

Added max concurrent streams enforcement with Redis counter

Implemented graceful shutdown handlers for Render scaling

Enhanced structured JSON logging throughout

Updated for new Render deployment with Valkey compatibility

Added retry logic for inference/TTS failures

Enhanced security headers and CORS

Auto-recovery for Redis circuit breaker

Sentry enrichment with deployment metadata
"""

import json
import time
import uuid
import logging_utils
import traceback
import threading
import os
import signal
import atexit
import sys
import asyncio
from typing import Generator, Optional, Dict, Any

from flask import Flask, request, jsonify, Response, stream_with_context
from flask_sock import Sock

# --------------------------------------------------------------------------
# Phase 11-D Configuration Isolation
# --------------------------------------------------------------------------
try:
    from config import config
except ImportError:
    # Fallback config for backward compatibility
    import os
    class config:
        STREAM_HEARTBEAT_INTERVAL = float(os.getenv("STREAM_HEARTBEAT_INTERVAL", "25")) # Increased for ALB/NGINX
        PORT = int(os.getenv("PORT", "7000"))
        MAX_CONCURRENT_STREAMS = int(os.getenv("MAX_CONCURRENT_STREAMS", "50"))
        STREAM_TIMEOUT_SECONDS = int(os.getenv("STREAM_TIMEOUT_SECONDS", "300"))

# --------------------------------------------------------------------------
# Phase 11-D Metrics Integration - Lazy Import Shim
# --------------------------------------------------------------------------
def _get_metrics():
    """Lazy metrics shim to avoid circular imports at import-time"""
    try:
        # Import the module and get the MetricsCollector class or instance
        import metrics_collector as metrics_module
        # Check if metrics_module has a MetricsCollector class
        if hasattr(metrics_module, 'MetricsCollector'):
            return metrics_module.MetricsCollector()
        else:
            # If it's already a singleton instance, return it directly
            return metrics_module
    except Exception:
        # safe no-op fallbacks
        class NoopMetrics:
            def increment_metric(self, *a, **k): pass
            def observe_latency(self, *a, **k): pass
            def get_snapshot(self, *a, **k): return {}
        return NoopMetrics()

# Lazy metrics instance - will be initialized on first use
_metrics = None

def get_metrics():
    """Get or initialize lazy metrics instance"""
    global _metrics
    if _metrics is None:
        _metrics = _get_metrics()
    return _metrics

# --------------------------------------------------------------------------
# Local modules
# --------------------------------------------------------------------------
from tasks import run_tts
from gpt_client import generate_reply
from logging_utils import log_event

# --------------------------------------------------------------------------
# Shared registry & registry-backed metrics (Phase 11-D) - Lazy imports
# --------------------------------------------------------------------------
class _NoopTimer:
    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): return False

def _get_registry():
    """Lazy import for metrics registry"""
    try:
        from metrics_registry import REGISTRY
        return REGISTRY
    except Exception:
        return None

def _get_prometheus_client():
    """Lazy import for prometheus client - unified implementation"""
    try:
        from prometheus_client import Summary, Gauge, Counter, generate_latest, CONTENT_TYPE_LATEST
        return Summary, Gauge, Counter, generate_latest, CONTENT_TYPE_LATEST
    except Exception:
        # Fallback no-op classes with unified generate_latest signature
        class NoopMetric:
            def __init__(self, *args, **kwargs): pass
            def inc(self, *args, **kwargs): pass
            def set(self, *args, **kwargs): pass
            def observe(self, *args, **kwargs): pass
            def labels(self, *args, **kwargs): return self
            def time(self): return _NoopTimer()
        
        # Single unified fallback that accepts optional registry parameter
        def _generate_latest_fallback(registry=None):
            return "# metrics_unavailable\n"
            
        return NoopMetric, NoopMetric, NoopMetric, _generate_latest_fallback, "text/plain"

# Initialize metrics with lazy loading - SINGLE DEFINITION
Summary, Gauge, Counter, generate_latest, CONTENT_TYPE_LATEST = _get_prometheus_client()
SHARED_REGISTRY = _get_registry()

# Use default registry if shared registry isn't critical to avoid double registration
def _get_metric_registry():
    """Get appropriate registry, defaulting to avoid double registration issues"""
    return SHARED_REGISTRY # Keep using shared registry for consistency

METRIC_REGISTRY = _get_metric_registry()

# --------------------------------------------------------------------------
# Uptime Gauge using Shared Registry
# --------------------------------------------------------------------------
UPTIME_GAUGE = Gauge(
    "streaming_server_uptime_seconds",
    "Uptime of the streaming server in seconds",
    registry=METRIC_REGISTRY
)

def report_health_metrics(start_time, active_streams, redis_state, redis_failures):
    """
    Report health metrics using shared registry.
    """
    from time import time
    import logging_utils
    import metrics_collector as mc
    
    try:
        uptime_seconds = time() - start_time
        UPTIME_GAUGE.set(uptime_seconds)

        mc.increment_metric("streaming.health_check.count", 1)
        mc.observe_latency("streaming.health_check.latency", 0.001)

        logging_utils.log_event(
            service="streaming_server",
            event="health_reported",
            status="ok",
            message="Health metrics reported successfully",
            extra={
                "uptime_seconds": uptime_seconds,
                "active_streams": active_streams,
                "redis_state": redis_state,
                "redis_failures": redis_failures
            }
        )

    except Exception as e:
        logging_utils.log_event(
            service="streaming_server",
            event="health_report_failed",
            status="error",
            message="Failed to report health metrics",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )

# Register streaming-specific metrics
stream_latency_ms = Summary(
    "stream_latency_ms",
    "TTS stream completion latency (milliseconds)",
    registry=METRIC_REGISTRY,
)

stream_bytes_out_total = Counter(
    "stream_bytes_out_total",
    "Total number of audio bytes streamed out",
    registry=METRIC_REGISTRY,
)

# Phase 11-D Unified Metrics Schema
stream_events_total = Counter(
    "stream_events_total",
    "Total stream events processed",
    ["event_type"],
    registry=METRIC_REGISTRY,
)

stream_errors_total = Counter(
    "stream_errors_total",
    "Total stream errors",
    ["error_type"],
    registry=METRIC_REGISTRY,
)

latency_seconds = Summary(
    "latency_seconds",
    "Stream processing latency in seconds",
    ["stage"],
    registry=METRIC_REGISTRY,
)

redis_retries_total = Counter(
    "redis_retries_total",
    "Total Redis operation retries",
    registry=METRIC_REGISTRY,
)

# --------------------------------------------------------------------------
# New SSE Resilience Metrics
# --------------------------------------------------------------------------
stream_active_connections = Gauge(
    "stream_active_connections",
    "Number of currently active SSE connections",
    registry=METRIC_REGISTRY,
)

stream_duration_seconds = Summary(
    "stream_duration_seconds",
    "Duration of SSE streaming sessions in seconds",
    registry=METRIC_REGISTRY,
)

stream_heartbeat_sent_total = Counter(
    "stream_heartbeat_sent_total",
    "Total number of SSE heartbeat events sent",
    registry=METRIC_REGISTRY,
)

# New metrics for connection management
stream_rejected_total = Counter(
    "stream_rejected_total",
    "Total stream requests rejected",
    ["reason"],
    registry=METRIC_REGISTRY,
)

stream_heartbeat_failures_total = Counter(
    "stream_heartbeat_failures_total",
    "Total heartbeat failures",
    registry=METRIC_REGISTRY,
)

# New metrics for retry logic
stream_retry_attempts_total = Counter(
    "stream_retry_attempts_total",
    "Total retry attempts for operations",
    ["operation"],
    registry=METRIC_REGISTRY,
)

# --------------------------------------------------------------------------
# SSE Connection Metrics (WebSocket equivalent for SSE)
# --------------------------------------------------------------------------
sse_connections_opened_total = Counter(
    "sse_connections_opened_total",
    "Total number of SSE connections opened",
    registry=METRIC_REGISTRY,
)

sse_connections_closed_total = Counter(
    "sse_connections_closed_total",
    "Total number of SSE connections closed",
    ["reason"],
    registry=METRIC_REGISTRY,
)

sse_connection_duration_ms = Summary(
    "sse_connection_duration_ms",
    "SSE connection duration in milliseconds",
    registry=METRIC_REGISTRY,
)

sse_heartbeat_timeouts_total = Counter(
    "sse_heartbeat_timeouts_total",
    "Total number of SSE heartbeat timeouts",
    registry=METRIC_REGISTRY,
)

sse_backpressure_events_total = Counter(
    "sse_backpressure_events_total",
    "Total number of SSE backpressure events (slow sends)",
    registry=METRIC_REGISTRY,
)

# --------------------------------------------------------------------------
# Redis client (use centralized redis_client module)
# --------------------------------------------------------------------------
# Unified import pattern required by Sean:
from redis_client import get_client

# --------------------------------------------------------------------------
# Configuration Constants
# --------------------------------------------------------------------------
HEARTBEAT_INTERVAL = 25 # Increased to 25s for ALB/NGINX compatibility
CHECKPOINT_TTL = 3600 # 1 hour TTL for Redis checkpoints
BACKPRESSURE_THRESHOLD = 1.0 # seconds for slow send detection
HEARTBEAT_TIMEOUT = 30 # seconds for heartbeat timeout

# Clamp max concurrent streams to reasonable limits
MAX_CONCURRENT_STREAMS = max(1, min(int(os.getenv("MAX_CONCURRENT_STREAMS", "50")), 1000))

# --------------------------------------------------------------------------
# CORS Configuration
# --------------------------------------------------------------------------
# Read allowed origins from environment
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "").split(",")
ALLOWED_ORIGINS = [origin.strip() for origin in ALLOWED_ORIGINS if origin.strip()]

# --------------------------------------------------------------------------
# URL Masking for Secure Logging
# --------------------------------------------------------------------------
def _mask_url(u: str) -> str:
    """Mask credentials in URLs for secure logging"""
    try:
        from urllib.parse import urlsplit, urlunsplit
        sp = urlsplit(u or "")
        host = f"[{sp.hostname}]" if sp.hostname and ":" in sp.hostname else (sp.hostname or "")
        netloc = host + (f":{sp.port}" if sp.port else "")
        if sp.username or sp.password:
            netloc = f":@{netloc}"
        return urlunsplit((sp.scheme, netloc, sp.path, sp.query, sp.fragment))
    except Exception:
        return "<redacted>"

# --------------------------------------------------------------------------
# Retry Logic for Inference/TTS Failures
# --------------------------------------------------------------------------
def with_retry(func, operation_name="unknown", max_retries=2, delay=1.0):
    """Retry wrapper for transient failures in inference and TTS operations"""
    for attempt in range(max_retries):
        try:
            result = func()
            if attempt > 0:
                log_event(
                    service="streaming_server",
                    event="retry_success",
                    status="info",
                    message=f"Retry successful for {operation_name}",
                    extra={"operation": operation_name, "attempt": attempt + 1},
                )
            return result
        except Exception as e:
            stream_retry_attempts_total.labels(operation=operation_name).inc()
            if attempt == max_retries - 1:
                log_event(
                    service="streaming_server",
                    event="retry_exhausted",
                    status="error",
                    message=f"All retries exhausted for {operation_name}",
                    extra={"operation": operation_name, "attempts": max_retries, "error": str(e)},
                )
                raise
            else:
                log_event(
                    service="streaming_server",
                    event="retry_attempt",
                    status="warn",
                    message=f"Retry attempt for {operation_name}",
                    extra={"operation": operation_name, "attempt": attempt + 1, "max_retries": max_retries, "error": str(e)},
                )
            time.sleep(delay * (attempt + 1)) # Exponential backoff

# --------------------------------------------------------------------------
# Stream State Management for Recovery Checkpoints
# --------------------------------------------------------------------------
class StreamStateManager:
    def __init__(self):
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self.checkpoint_interval = 30 # seconds
        self.shutting_down = False
        # Thread lock for active_streams access
        self._lock = threading.RLock()
    
    def create_checkpoint(self, stream_id: str, state: Dict[str, Any]):
        """Store stream state checkpoint in Redis"""
        try:
            checkpoint = {
                "stream_id": stream_id,
                "state": state,
                "timestamp": time.time(),
                "service": "streaming_server"
            }
            
            # Use safe_redis_call for centralized error handling
            result = safe_redis_call(
                lambda client: client.setex(
                    f"stream_checkpoint:{stream_id}", 
                    CHECKPOINT_TTL, 
                    json.dumps(checkpoint)
                )
            )
            
            if result:
                log_event(
                    service="streaming_server",
                    event="stream_checkpoint_saved",
                    status="info",
                    message="Stream state checkpoint saved",
                    extra={"stream_id": stream_id, "state_keys": list(state.keys())},
                )
                return True
            return False
            
        except Exception as e:
            log_event(
                service="streaming_server",
                event="checkpoint_save_failed",
                status="warn",
                message="Failed to save stream checkpoint",
                extra={"stream_id": stream_id, "error": str(e)},
            )
            return False
    
    def restore_checkpoint(self, stream_id: str) -> Optional[Dict[str, Any]]:
        """Restore stream state from Redis checkpoint"""
        try:
            raw = safe_redis_call(
                lambda client: client.get(f"stream_checkpoint:{stream_id}")
            )
            
            if raw:
                data = raw.decode("utf-8")
                checkpoint = json.loads(data)
                log_event(
                    service="streaming_server",
                    event="stream_checkpoint_restored",
                    status="info",
                    message="Stream state checkpoint restored",
                    extra={"stream_id": stream_id},
                )
                return checkpoint.get("state", {})
            return None
            
        except Exception as e:
            log_event(
                service="streaming_server",
                event="checkpoint_restore_failed",
                status="warn",
                message="Failed to restore stream checkpoint",
                extra={"stream_id": stream_id, "error": str(e)},
            )
            return None
    
    def get_active_stream_count(self) -> int:
        """Get current number of active streams"""
        with self._lock:
            return len(self.active_streams)
    
    def add_stream(self, session_id: str, stream_state: Dict[str, Any]) -> None:
        """Thread-safe add stream to active streams"""
        with self._lock:
            self.active_streams[session_id] = stream_state
            stream_active_connections.set(len(self.active_streams))
    
    def remove_stream(self, session_id: str) -> bool:
        """Thread-safe remove stream from active streams"""
        with self._lock:
            if session_id in self.active_streams:
                del self.active_streams[session_id]
                stream_active_connections.set(len(self.active_streams))
                return True
            return False
    
    def get_stream(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Thread-safe get stream state"""
        with self._lock:
            return self.active_streams.get(session_id)
    
    def cleanup_all_streams(self):
        """Clean up all active streams during shutdown"""
        self.shutting_down = True
        with self._lock:
            active_count = len(self.active_streams)
            if active_count > 0:
                log_event(
                    service="streaming_server",
                    event="stream_cleanup_started",
                    status="info",
                    message="Cleaning up active streams during shutdown",
                    extra={"active_streams_count": active_count},
                )
                
                # Create final checkpoints for all active streams
                for stream_id, state in self.active_streams.items():
                    try:
                        state["stage"] = "shutdown"
                        state["shutdown_time"] = time.time()
                        self.create_checkpoint(stream_id, state)
                    except Exception as e:
                        log_event(
                            service="streaming_server",
                            event="stream_cleanup_error",
                            status="warn",
                            message="Failed to cleanup stream during shutdown",
                            extra={"stream_id": stream_id, "error": str(e)},
                        )
                
                self.active_streams.clear()
                stream_active_connections.set(0)
                
                log_event(
                    service="streaming_server",
                    event="stream_cleanup_completed",
                    status="info",
                    message="All active streams cleaned up",
                    extra={"cleaned_streams_count": active_count},
                )

# Initialize stream state manager
stream_state_mgr = StreamStateManager()

# --------------------------------------------------------------------------
# Concurrent Streams Management - SINGLE DEFINITION
# --------------------------------------------------------------------------
class ConcurrentStreamsManager:
    def __init__(self, max_streams: int = MAX_CONCURRENT_STREAMS):
        self.max_streams = max_streams
        self.redis_key = "streaming_server:active_streams_count"
    
    def can_accept_new_stream(self) -> bool:
        """Check if we can accept a new stream based on current capacity"""
        try:
            current_count = stream_state_mgr.get_active_stream_count()
            
            # First check local count (fast path)
            if current_count >= self.max_streams:
                log_event(
                    service="streaming_server",
                    event="stream_rejected_max_capacity",
                    status="info",  # Downgraded to info to reduce noise
                    message="Stream rejected - maximum concurrent streams reached",
                    extra={
                        "current_streams": current_count,
                        "max_streams": self.max_streams
                    },
                )
                stream_rejected_total.labels(reason="max_concurrent").inc()
                return False
            
            # Then check Redis counter for cross-instance coordination
            count = safe_redis_call(lambda client: client.get(self.redis_key))
            if count is None:
                # Redis unavailable - fall back to local enforcement only
                # Allow the stream since local count is within limits (we already checked above)
                log_event(
                    service="streaming_server",
                    event="redis_unavailable_fallback",
                    status="warn",
                    message="Redis unavailable, falling back to local stream count enforcement",
                    extra={
                        "local_streams": current_count,
                        "max_streams": self.max_streams
                    },
                )
                return True
                
            global_count = int(count)
            if global_count >= self.max_streams:
                log_event(
                    service="streaming_server",
                    event="stream_rejected_global_capacity",
                    status="info",  # Downgraded to info to reduce noise
                    message="Stream rejected - global maximum concurrent streams reached",
                    extra={
                        "global_streams": global_count,
                        "max_streams": self.max_streams
                    },
                )
                stream_rejected_total.labels(reason="global_max_concurrent").inc()
                return False
            
            return True
            
        except Exception as e:
            log_event(
                service="streaming_server",
                event="stream_capacity_check_failed",
                status="error",
                message="Failed to check stream capacity",
                extra={"error": str(e)},
            )
            # On error, be conservative and reject new streams
            return False
    
    def increment_global_count(self) -> bool:
        """Atomically increment global stream counter in Redis, enforcing max capacity"""
        try:
            # Use Lua script for atomic check-and-increment
            lua_script = """
            local current = redis.call('GET', KEYS[1])
            current = tonumber(current) or 0
            if current >= tonumber(ARGV[1]) then
                return -1  -- At or over capacity
            end
            local new_val = redis.call('INCR', KEYS[1])
            redis.call('PEXPIRE', KEYS[1], ARGV[2])
            return new_val
            """
            
            # Use safe_redis_call for consistency with other Redis operations
            result = safe_redis_call(
                lambda client: client.eval(lua_script, 1, self.redis_key, self.max_streams, CHECKPOINT_TTL * 1000)
            )
            
            if result == -1:
                # At capacity - reject
                stream_rejected_total.labels(reason="global_max_concurrent").inc()
                return False
            elif result is None:
                # Redis unavailable - allow with local enforcement (Phase-12 policy)
                log_event(
                    service="streaming_server",
                    event="redis_unavailable_at_increment",
                    status="warn",
                    message="Redis unavailable during increment, proceeding with local enforcement",
                    extra={"max_streams": self.max_streams},
                )
                return True
            else:
                # Successfully incremented
                log_event(
                    service="streaming_server",
                    event="global_stream_count_incremented",
                    status="info",
                    message="Global stream count incremented",
                    extra={"new_count": result},
                )
                return True
                
        except Exception as e:
            log_event(
                service="streaming_server",
                event="global_count_increment_failed",
                status="warn",
                message="Failed to increment global stream count",
                extra={"error": str(e)},
            )
            # On Redis error, allow with local enforcement (Phase-12 policy)
            return True
    
    def decrement_global_count(self) -> bool:
        """Decrement global stream counter in Redis"""
        try:
            result = safe_redis_call(lambda client: client.decr(self.redis_key))
            if result is not None:
                # Don't let it go negative - force set to 0 if it goes negative
                if result < 0:
                    safe_redis_call(lambda client: client.set(self.redis_key, 0))
                    result = 0
                # Use PEXPIRE for consistency with increment
                safe_redis_call(lambda client: client.pexpire(self.redis_key, CHECKPOINT_TTL * 1000))
                log_event(
                    service="streaming_server",
                    event="global_stream_count_decremented",
                    status="info",
                    message="Global stream count decremented",
                    extra={"new_count": result},
                )
                return True
            else:
                # Redis unavailable - schedule a best-effort retry
                log_event(
                    service="streaming_server",
                    event="global_count_decrement_failed",
                    status="warn",
                    message="Failed to decrement global stream count (Redis unavailable)",
                    extra={"redis_key": self.redis_key},
                )
                # Schedule a retry to clean up the count - USE threading.Timer consistently
                threading.Timer(5.0, self._retry_decrement).start()
                return False
            
        except Exception as e:
            log_event(
                service="streaming_server",
                event="global_count_decrement_error",
                status="warn",
                message="Error decrementing global stream count",
                extra={"error": str(e)},
            )
            # Schedule a retry to clean up the count - USE threading.Timer consistently
            threading.Timer(5.0, self._retry_decrement).start()
            return False
    
    def _retry_decrement(self):
        """Best-effort retry to decrement global count"""
        try:
            current_count = safe_redis_call(lambda client: client.get(self.redis_key))
            if current_count is not None:
                count_val = int(current_count)
                if count_val > 0:
                    # Try to decrement again
                    safe_redis_call(lambda client: client.decr(self.redis_key))
                    # Ensure it doesn't go negative and set TTL
                    safe_redis_call(lambda client: client.pexpire(self.redis_key, CHECKPOINT_TTL * 1000))
                    log_event(
                        service="streaming_server",
                        event="global_count_retry_success",
                        status="info",
                        message="Global count decrement retry succeeded",
                        extra={"redis_key": self.redis_key},
                    )
        except Exception:
            # Silent fail - count will expire via TTL eventually
            pass

    def get_global_count(self) -> int:
        """Get current global stream count from Redis"""
        try:
            count = safe_redis_call(lambda client: client.get(self.redis_key))
            return int(count) if count else 0
        except Exception:
            return 0

# Initialize concurrent streams manager
streams_manager = ConcurrentStreamsManager(max_streams=MAX_CONCURRENT_STREAMS)

# --------------------------------------------------------------------------
# Async Health Monitoring - SINGLE DEFINITION with real Redis status
# --------------------------------------------------------------------------
class HealthMonitor:
    def __init__(self):
        self.running = False
        self.thread = None
        self.uptime_start = time.time()
        self.redis_state = "UNKNOWN"
        self.redis_failures = 0
    
    def start(self):
        """Start the health monitoring background thread"""
        if self.running:
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        log_event(
            service="streaming_server",
            event="health_monitor_started",
            status="info",
            message="Health monitoring started",
        )
    
    def stop(self):
        """Stop the health monitoring"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
    
    def _monitor_loop(self):
        """Background monitoring loop"""
        while self.running:
            try:
                self._report_health()
                time.sleep(15)
            except Exception as e:
                log_event(
                    service="streaming_server",
                    event="health_monitor_error",
                    status="error",
                    message="Health monitor loop error",
                    extra={"error": str(e)},
                )
                time.sleep(15)
    
    def _report_health(self):
        """Report health metrics to system monitor - SINGLE IMPLEMENTATION with real Redis status"""
        try:
            uptime = time.time() - self.uptime_start
            active_streams = stream_state_mgr.get_active_stream_count()
            
            # Check actual Redis status - NO HARDCODED VALUES
            redis_ok = safe_redis_ping()
            if redis_ok:
                self.redis_state = "CLOSED"
                self.redis_failures = 0
            else:
                self.redis_state = "OPEN"
                self.redis_failures += 1
            
            report_health_metrics(self.uptime_start, active_streams, self.redis_state, self.redis_failures)
            
        except Exception as e:
            log_event(
                service="streaming_server",
                event="health_report_failed",
                status="error",
                message="Failed to report health metrics",
                extra={"error": str(e)},
            )

# Initialize health monitor
health_monitor = HealthMonitor()

# --------------------------------------------------------------------------
# Graceful Shutdown Handler - SINGLE DEFINITION
# --------------------------------------------------------------------------
class GracefulShutdown:
    def __init__(self):
        self.shutting_down = False
        self.shutdown_callbacks = []
    
    def register_shutdown_callback(self, callback):
        """Register a callback to be called during shutdown"""
        self.shutdown_callbacks.append(callback)
    
    def initiate_shutdown(self, signum=None, frame=None):
        """Initiate graceful shutdown"""
        if self.shutting_down:
            return
            
        self.shutting_down = True
        log_event(
            service="streaming_server",
            event="shutdown_initiated",
            status="info",
            message="Graceful shutdown initiated",
            extra={"signal": signum},
        )
        
        # Execute all registered shutdown callbacks
        for callback in self.shutdown_callbacks:
            try:
                callback()
            except Exception as e:
                log_event(
                    service="streaming_server",
                    event="shutdown_callback_error",
                    status="error",
                    message="Shutdown callback failed",
                    extra={"error": str(e)},
                )
        
        log_event(
            service="streaming_server",
            event="shutdown_completed",
            status="info",
            message="Graceful shutdown completed",
        )
        
        # Use sys.exit for proper cleanup
        sys.exit(0)

# Initialize graceful shutdown handler
graceful_shutdown = GracefulShutdown()

# --------------------------------------------------------------------------
# Import diagnostic helpers for parity with app.py (fallback stubs)
# --------------------------------------------------------------------------
try:
    from utils import check_redis_status, check_r2_connectivity
except Exception:
    def check_r2_connectivity():
        return "not_available"
    
    def check_redis_status():
        return "not_available"

# --------------------------------------------------------------------------
# Sentry Integration (Safe Import) with Enhanced Metadata - SINGLE DEFINITION
# --------------------------------------------------------------------------
def capture_exception_safe(exception, context=None):
    """Safely capture exceptions with Sentry if available"""
    try:
        import sentry_utils
        if hasattr(sentry_utils, 'capture_exception_safe'):
            # Enhanced context with deployment metadata
            enhanced_context = {
                "service": "streaming_server",
                "env_mode": os.getenv("ENV_MODE", "unknown"),
                "phase": os.getenv("SARA_PHASE", "unknown"),
                "streaming_url": _mask_url(os.getenv("STREAMING_URL", "unknown")),
                "app_url": _mask_url(os.getenv("APP_URL", "unknown"))
            }
            if context:
                enhanced_context.update(context)
            
            sentry_utils.capture_exception_safe(exception, enhanced_context)
    except ImportError:
        # Sentry not available, log normally
        log_event(
            service="streaming_server",
            event="sentry_capture_failed",
            status="warn",
            message="Sentry not available for exception capture",
            extra={
                "error": str(exception), 
                "context": context,
                "env_mode": os.getenv("ENV_MODE", "unknown")
            }
        )

# --------------------------------------------------------------------------
# Flask app with Enhanced Security Headers - SINGLE DEFINITION
# --------------------------------------------------------------------------
app = Flask(__name__)
sock = Sock(app)
app.config["JSON_SORT_KEYS"] = False

# Enhanced security headers middleware - SINGLE IMPLEMENTATION
@app.after_request
def add_security_headers(response):
    """Add security headers to all responses"""
    # Enhanced CORS with origin allowlist
    request_origin = request.headers.get('Origin', '')
    if request_origin in ALLOWED_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = request_origin
        response.headers["Vary"] = "Origin"
    elif ALLOWED_ORIGINS:
        # If we have an allowlist but origin not in it, don't set CORS headers
        # This will cause browser to block the request
        pass
    else:
        # Fallback to wildcard only if no allowlist configured
        response.headers["Access-Control-Allow-Origin"] = "*"
    
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, Accept, Cache-Control"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Cross-Origin-Resource-Policy"] = "same-site"
    return response

# Handle CORS preflight requests
@app.route("/", methods=["OPTIONS"])
@app.route("/<path:path>", methods=["OPTIONS"])
def options_handler(path=None):
    return "", 200

# --------------------------------------------------------------------------
# Lazy Initialization Functions (Phase 11-D - No side effects at import time)
# --------------------------------------------------------------------------
def _initialize_metrics_system():
    """Lazy metrics system initialization - called on first request"""
    try:
        from metrics_registry import restore_snapshot_to_collector
        from global_metrics_store import start_background_sync
        
        restore_snapshot_to_collector(get_metrics())
        start_background_sync(service_name="streaming")
        
        # Start health monitoring
        health_monitor.start()
        
        log_event(
            service="streaming",
            event="global_metrics_sync_started",
            status="ok",
            message="Streaming server metrics restored and sync started",
            extra={"phase": "11-F"}
        )
    except Exception as e:
        log_event(
            service="streaming",
            event="metrics_startup_failed",
            status="error",
            message="Failed to initialize unified metrics",
            extra={"error": str(e), "stack": traceback.format_exc(), "phase": "11-F"}
        )

def _initialize_metrics_restore():
    """Lazy metrics restore - called on first request"""
    try:
        client = get_client()
        if client:
            try:
                from metrics_registry import restore_snapshot_to_collector
                result = restore_snapshot_to_collector(get_metrics())
                
                if result:
                    log_event(
                        service="streaming_server",
                        event="metrics_snapshot_restored",
                        status="info",
                        message="Restored snapshot into metrics_collector from Redis using standardized restoration.",
                    )
                else:
                    log_event(
                        service="streaming_server",
                        event="metrics_restore_no_data",
                        status="info",
                        message="No metrics snapshot found in Redis for restoration.",
                    )
            finally:
                try:
                    client.close()
                except Exception:
                    pass
    except Exception as e:
        log_event(
            service="streaming_server",
            event="restore_at_startup_failed",
            status="warn",
            message="Failed to restore metrics snapshot at startup (best-effort).",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )

def _ensure_initialized():
    """Ensure metrics are initialized on first use"""
    _initialize_metrics_system()
    _initialize_metrics_restore()

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def new_trace() -> str:
    trace_id = str(uuid.uuid4())
    log_event(
        service="streaming_server",
        event="trace_created",
        status="info",
        message="New trace context created",
        extra={"trace_id": trace_id},
    )
    return trace_id

def sse_format(event: Optional[str] = None, data: Optional[dict] = None) -> str:
    msg_lines = []
    if event:
        msg_lines.append(f"event: {event}")
        stream_events_total.labels(event_type=event).inc()
    if data is not None:
        msg_lines.append(f"data: {json.dumps(data, separators=(',', ':'))}")
    msg_lines.append("")
    return "\n".join(msg_lines) + "\n"

def safe_redis_ping(client=None, trace_id: Optional[str] = None, session_id: Optional[str] = None) -> bool:
    """Enhanced Redis ping with Valkey compatibility"""
    if client is None:
        client = get_client()
        close_client = True # Only close if we created the client
    else:
        close_client = False # Don't close a client provided by caller
    
    if not client:
        log_event(
            service="streaming_server",
            event="redis_client_none",
            status="warn",
            message="Redis client unavailable (Valkey init failed)"
        )
        return False
    
    try:
        return client.ping()
    except Exception as e:
        # Emit metric for Redis failure
        try:
            from metrics_collector import increment_metric
            increment_metric("redis_call_error")
        except Exception:
            pass
        return False
    finally:
        if close_client:
            try:
                if client:
                    client.close()
            except Exception:
                pass

def safe_redis_call(func, *args, **kwargs):
    """Utility wrapper to call Redis operations via circuit breaker."""
    client = get_client()
    if not client:
        return None
    
    try:
        return func(client, *args, **kwargs)  # pass client
    except Exception as e:
        # Emit metric for Redis failure
        try:
            from metrics_collector import increment_metric
            increment_metric("redis_call_error")
        except Exception:
            pass
        return None
    finally:
        try:
            client.close()
        except Exception:
            pass

def record_heartbeat(stream_state: Dict[str, Any]) -> bool:
    """Record SSE heartbeat in stream state (does not emit SSE)"""
    try:
        stream_heartbeat_sent_total.inc()
        stream_state["last_heartbeat"] = time.time()
        stream_state["heartbeat_count"] = stream_state.get("heartbeat_count", 0) + 1
        
        return True
    except Exception as e:
        log_event(
            service="streaming_server",
            event="heartbeat_failed",
            status="warn",
            message="Failed to record heartbeat",
            extra={
                "session_id": stream_state.get("session_id"),
                "error": str(e)
            },
        )
        stream_heartbeat_failures_total.inc()
        return False

# --------------------------------------------------------------------------
# Heartbeat Management with Timeout Detection
# --------------------------------------------------------------------------
def check_heartbeat_timeout(stream_state: Dict[str, Any]) -> bool:
    """Check if heartbeat has timed out"""
    last_heartbeat = stream_state.get("last_heartbeat")
    if not last_heartbeat:
        return False
    
    time_since_heartbeat = time.time() - last_heartbeat
    if time_since_heartbeat > HEARTBEAT_TIMEOUT:
        sse_heartbeat_timeouts_total.inc()
        log_event(
            service="streaming_server",
            event="heartbeat_timeout",
            status="warn",
            message="SSE heartbeat timeout detected",
            trace_id=stream_state.get("trace_id"),
            session_id=stream_state.get("session_id"),
            extra={
                "time_since_heartbeat_seconds": round(time_since_heartbeat, 2),
                "timeout_seconds": HEARTBEAT_TIMEOUT,
                "stage": stream_state.get("stage", "unknown")
            },
        )
        return True
    return False

# --------------------------------------------------------------------------
# Prometheus Metrics Endpoint - FIXED: No unconditional prometheus_client import
# --------------------------------------------------------------------------
@app.route("/metrics", methods=["GET"])
def metrics_endpoint():
    """
    Expose unified metrics from shared REGISTRY.
    """
    try:
        try:
            get_metrics().increment_metric("streaming_metrics_requests_total", 1)
            stream_events_total.labels(event_type="metrics_request").inc()
        except Exception:
            pass
        
        # Generate unified metrics with fallback - using unified generate_latest signature
        # CRITICAL FIX: Avoid unconditional prometheus_client import that causes 500s
        try:
            # Use SHARED_REGISTRY if available, otherwise None (will use fallback path)
            registry_to_use = SHARED_REGISTRY
            
            # If we don't have a shared registry, try to get default but don't fail if unavailable
            if registry_to_use is None:
                try:
                    from prometheus_client import REGISTRY as default_registry
                    registry_to_use = default_registry
                except ImportError:
                    # Prometheus not installed - use None to trigger fallback
                    registry_to_use = None
            
            # Unified call - generate_latest handles both real and fallback cases
            metrics_data = generate_latest(registry_to_use)
            return Response(metrics_data, mimetype=CONTENT_TYPE_LATEST, status=200)
            
        except Exception as e:
            log_event(
                service="streaming_server",
                event="generate_latest_failed",
                status="error",
                message="Failed to generate REGISTRY output",
                extra={"error": str(e), "stack": traceback.format_exc(), "error_code": "METRICS_GENERATION_FAILED"},
            )
            stream_errors_total.labels(error_type="metrics_generation").inc()
            return Response("# metrics_export_error 1\n", mimetype="text/plain", status=500)

    except Exception as e:
        log_event(
            service="streaming_server",
            event="metrics_endpoint_error",
            status="error",
            message="Failed to serve /metrics",
            extra={"error": str(e), "stack": traceback.format_exc(), "error_code": "METRICS_ENDPOINT_FAILED"},
        )
        stream_errors_total.labels(error_type="metrics_endpoint").inc()
        return Response("# metrics_export_error 1\n", mimetype="text/plain", status=500)

# --------------------------------------------------------------------------
# /metrics_snapshot — JSON snapshot persisted to Redis (Phase 11-F)
# --------------------------------------------------------------------------
@app.route("/metrics_snapshot", methods=["GET"])
def metrics_snapshot():
    try:
        try:
            get_metrics().increment_metric("streaming_metrics_snapshot_requests_total", 1)
            stream_events_total.labels(event_type="metrics_snapshot").inc()
        except Exception:
            pass
        
        coll_snap = {}
        try:
            coll_snap = get_metrics().get_snapshot()
        except Exception:
            coll_snap = {}

        registry_snap = {}
        try:
            # Use the same safe approach as /metrics endpoint
            registry_to_use = SHARED_REGISTRY
            if registry_to_use is None:
                try:
                    from prometheus_client import REGISTRY as default_registry
                    registry_to_use = default_registry
                except ImportError:
                    registry_to_use = None
            
            if registry_to_use is not None:
                for family in registry_to_use.collect():
                    fam_name = family.name
                    samples = []
                    for s in family.samples:
                        # handle tuple-like and object sample representations
                        try:
                            sample_name = s[0] if isinstance(s, (list, tuple)) else getattr(s, "name", None)
                            labels = s[1] if isinstance(s, (list, tuple)) else getattr(s, "labels", {})
                            value = s[2] if isinstance(s, (list, tuple)) else getattr(s, "value", None)
                        except Exception:
                            sample_name, labels, value = getattr(s, "name", None), getattr(s, "labels", {}), getattr(s, "value", None)
                        samples.append({"name": sample_name, "labels": labels, "value": value})
                    registry_snap[fam_name] = {
                        "documentation": getattr(family, "documentation", ""),
                        "type": getattr(family, "type", ""),
                        "samples": samples,
                    }
        except Exception:
            registry_snap = {}

        payload = {
            "service": "streaming",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "collector_snapshot": coll_snap,
            "registry_snapshot": registry_snap,
        }

        # Persist collector snapshot via helper (guarded)
        try:
            from metrics_collector import push_snapshot_from_collector
            client = get_client()
            if client:
                try:
                    push_snapshot_from_collector(get_metrics().get_snapshot())
                finally:
                    try:
                        client.close()
                    except Exception:
                        pass
        except Exception as e:
            log_event(
                service="streaming_server",
                event="push_snapshot_failed",
                status="warn",
                message="push_snapshot_from_collector failed",
                extra={"error": str(e), "error_code": "SNAPSHOT_PUSH_FAILED"},
            )
            stream_errors_total.labels(error_type="snapshot_push").inc()

        # Also save combined payload as convenience/fallback
        try:
            from metrics_registry import save_metrics_snapshot
            if callable(save_metrics_snapshot):
                client = get_client()
                if client:
                    try:
                        save_metrics_snapshot(payload)
                    finally:
                        try:
                            client.close()
                        except Exception:
                            pass
        except Exception as e:
            log_event(service="streaming_server", event="metrics_registry_backup_failed",
                     status="warn", message=str(e), extra={"error_code": "BACKUP_SNAPSHOT_FAILED"})
            stream_errors_total.labels(error_type="backup_snapshot").inc()

        return jsonify(payload), 200
    except Exception as e:
        log_event(
            service="streaming_server",
            event="metrics_snapshot_error",
            status="error",
            message="Failed to produce metrics snapshot",
            extra={"error": str(e), "stack": traceback.format_exc(), "error_code": "SNAPSHOT_GENERATION_FAILED"},
        )
        stream_errors_total.labels(error_type="snapshot_generation").inc()
        return jsonify({"error": "snapshot_failure"}), 500

# --------------------------------------------------------------------------
# Enhanced Health Endpoints with Redis Global Count - SINGLE IMPLEMENTATION
# --------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    try:
        get_metrics().increment_metric("streaming_healthz_requests_total", 1)
        stream_events_total.labels(event_type="healthz").inc()
    except Exception:
        pass
    
    active_streams = stream_state_mgr.get_active_stream_count()
    redis_global_count = streams_manager.get_global_count()

    # Check actual Redis status - NO HARDCODED VALUES
    redis_ok = safe_redis_ping()
    redis_circuit_state = "CLOSED" if redis_ok else "OPEN"

    log_event(
        service="streaming",
        event="healthz_check",
        status="ok",
        message="Streaming health OK",
        extra={
            "active_streams": active_streams,
            "redis_global_count": redis_global_count,
            "redis_circuit_state": redis_circuit_state,
            "uptime_seconds": time.time() - health_monitor.uptime_start,
            "max_concurrent_streams": streams_manager.max_streams,
            "env_mode": os.getenv("ENV_MODE", "unknown")
        }
    )
    return jsonify({
        "status": "ok", 
        "service": "streaming_server",
        "active_streams": active_streams,
        "redis_global_count": redis_global_count,
        "redis_circuit_state": redis_circuit_state,
        "max_concurrent_streams": streams_manager.max_streams,
        "env_mode": os.getenv("ENV_MODE", "unknown")
    }), 200

@app.route("/health", methods=["GET"])
def health():
    trace_id = new_trace()
    active_streams = stream_state_mgr.get_active_stream_count()
    redis_global_count = streams_manager.get_global_count()
    
    # Check actual Redis status - NO HARDCODED VALUES
    redis_ok = safe_redis_ping()
    redis_circuit_state = "CLOSED" if redis_ok else "OPEN"

    log_event(
        service="streaming",
        event="health_check",
        status="ok",
        message="Streaming health check",
        trace_id=trace_id,
        extra={
            "active_streams": active_streams,
            "redis_global_count": redis_global_count,
            "redis_circuit_state": redis_circuit_state,
            "max_concurrent_streams": streams_manager.max_streams,
            "env_mode": os.getenv("ENV_MODE", "unknown")
        }
    )

    try:
        if redis_ok:
            return jsonify({
                "status": "ok", 
                "redis": "connected",
                "circuit_state": redis_circuit_state,
                "active_streams": active_streams,
                "redis_global_count": redis_global_count,
                "max_concurrent_streams": streams_manager.max_streams,
                "env_mode": os.getenv("ENV_MODE", "unknown")
            }), 200
        else:
            log_event(
                service="streaming_server",
                event="health_degraded",
                status="warn",
                message="Redis unreachable during health check",
                trace_id=trace_id,
                extra={"circuit_state": redis_circuit_state, "failures": health_monitor.redis_failures},
            )
            return jsonify({
                "status": "degraded", 
                "redis": "unreachable",
                "circuit_state": redis_circuit_state
            }), 503
    except Exception as e:
        log_event(
            service="streaming_server",
            event="health_exception",
            status="error",
            message="Health endpoint failure",
            trace_id=trace_id,
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        stream_errors_total.labels(error_type="health_general").inc()
        return jsonify({"status": "degraded", "redis": "error"}), 503

# --------------------------------------------------------------------------
# System Status Endpoint (parity schema) - SINGLE IMPLEMENTATION
# --------------------------------------------------------------------------
@app.route("/system_status", methods=["GET"])
def system_status():
    try:
        active_streams = stream_state_mgr.get_active_stream_count()
        redis_global_count = streams_manager.get_global_count()
        
        # Check actual Redis status - NO HARDCODED VALUES
        redis_ok = safe_redis_ping()
        redis_circuit_state = "CLOSED" if redis_ok else "OPEN"
        redis_connectivity = "connected" if redis_ok else "unreachable"
        
        log_event(
            service="streaming",
            event="system_status_check",
            status="ok",
            message="System status check",
            extra={
                "active_streams": active_streams,
                "redis_global_count": redis_global_count,
                "redis_circuit_state": redis_circuit_state,
                "uptime_seconds": time.time() - health_monitor.uptime_start,
                "max_concurrent_streams": streams_manager.max_streams,
                "env_mode": os.getenv("ENV_MODE", "unknown"),
                "phase": os.getenv("SARA_PHASE", "unknown")
            }
        )
        return jsonify({
            "service": "streaming_server",
            "status": "ok",
            "redis_connectivity": redis_connectivity,
            "r2_connectivity": "ok",
            "active_streams": active_streams,
            "redis_global_count": redis_global_count,
            "redis_circuit_state": redis_circuit_state,
            "max_concurrent_streams": streams_manager.max_streams,
            "uptime_seconds": round(time.time() - health_monitor.uptime_start, 2),
            "env_mode": os.getenv("ENV_MODE", "unknown"),
            "phase": os.getenv("SARA_PHASE", "unknown")
        }), 200
    except Exception as e:
        trace_id = new_trace()
        log_event(
            service="streaming_server",
            event="system_status_failed",
            status="error",
            message="Failed to check system status",
            trace_id=trace_id,
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        stream_errors_total.labels(error_type="system_status").inc()
        return jsonify({
            "status": "error",
            "service": "streaming_server",
            "message": str(e),
        }), 500

# --------------------------------------------------------------------------
# Twilio Media Streams WebSocket Endpoint with Duplex Controller
# --------------------------------------------------------------------------
from simple_websocket.errors import ConnectionClosed  # add near imports

@sock.route("/media")
def media(ws):
    """
    Twilio Media Streams WebSocket endpoint integrated with duplex controller stack.
    Receives JSON text frames with events: connected, start, media, mark, stop.
    Sends outbound audio as Twilio 'media' JSON messages.
    """
    import json
    import base64
    import time
    
    call_sid = None
    stream_sid = None
    trace_id = None
    session_id = str(uuid.uuid4())
    start_ts = time.time()
    outbound_frame_sequence = 0  # Initialize early to avoid NameError in finally block
    controller = None  # Store controller reference for reuse

    log_event(
        service="streaming_server",
        event="twilio_ws_connect",
        status="info",
        message="Twilio WebSocket connection opened",
        extra={"stream_sid": stream_sid, "session_id": session_id}
    )

    try:
        # Capacity control
        if not streams_manager.can_accept_new_stream():
            log_event(
                service="streaming_server",
                event="twilio_ws_rejected_capacity",
                status="info",
                message="Twilio WebSocket rejected - maximum concurrent streams reached"
            )
            ws.close()
            return

        # Track locally
        stream_state_mgr.add_stream(session_id, {
            "session_id": session_id,
            "stage": "open",
            "opened_at": start_ts,
            "type": "twilio_media_stream"
        })
        if not streams_manager.increment_global_count():
            # best effort: still continue but logged upstream
            pass

        # Initialize DuplexControllerManager from the correct module
        from duplex_voice_controller import controller_manager
        
        log_event(
            service="streaming_server",
            event="twilio_media_stream_connected",
            status="info",
            message="Twilio Media Stream connected",
            session_id=session_id,
            extra={
                "type": "websocket",
                "active_connections": stream_state_mgr.get_active_stream_count(),
                "stream_sid": stream_sid
            }
        )

        while True:
            # Check for outbound audio frames from controller
            if call_sid and stream_sid and controller:
                # Poll for outbound audio frames using correct SYNC method
                outbound_audio = controller.next_outbound_frame_sync(timeout_ms=100)
                if outbound_audio:
                    # Encode and send outbound audio to Twilio
                    try:
                        encoded_payload = base64.b64encode(outbound_audio).decode('utf-8')
                        media_message = {
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {
                                "payload": encoded_payload
                            }
                        }
                        ws.send(json.dumps(media_message))
                        outbound_frame_sequence += 1
                        
                        log_event(
                            service="streaming_server",
                            event="twilio_outbound_audio_sent",
                            status="info",
                            message="Sent outbound audio frame to Twilio",
                            trace_id=trace_id,
                            session_id=session_id,
                            extra={
                                "frame_sequence": outbound_frame_sequence,
                                "audio_bytes": len(outbound_audio),
                                "encoded_length": len(encoded_payload),
                                "stream_sid": stream_sid,
                                "call_sid": call_sid
                            }
                        )
                    except Exception as e:
                        stream_errors_total.labels(error_type="outbound_media_send").inc()
                        log_event(
                            service="streaming_server",
                            event="outbound_audio_send_error",
                            status="error",
                            message="Error sending outbound audio to Twilio",
                            trace_id=trace_id,
                            session_id=session_id,
                            extra={
                                "error": str(e),
                                "frame_sequence": outbound_frame_sequence,
                                "call_sid": call_sid,
                                "stream_sid": stream_sid
                            }
                        )

            try:
                raw_msg = ws.receive()
                if raw_msg is None:
                    # client closed gracefully
                    break
            except ConnectionClosed as e:
                log_event(
                    service="streaming_server",
                    event="twilio_media_stream_closed_by_peer",
                    status="info",
                    message=f"Twilio closed websocket: {getattr(e, 'reason', str(e))}",
                    extra={"call_sid": call_sid, "stream_sid": stream_sid},
                )
                break

            try:
                msg = json.loads(raw_msg)
            except Exception as e:
                log_event(
                    service="streaming_server",
                    event="twilio_ws_bad_json",
                    status="error",
                    message="Received non-JSON frame from Twilio",
                    extra={"raw": raw_msg[:200] if raw_msg else "empty", "error": str(e), "call_sid": call_sid, "stream_sid": stream_sid}
                )
                continue

            event_type = msg.get("event")
            event = (msg.get("event") or "").lower()
            
            # Handle Twilio "connected" event (no-op)
            if event == "connected":
                # Twilio sends "connected" before "start" - treat as no-op
                log_event(
                    service="streaming_server",
                    event="twilio_ws_connected",
                    status="info",
                    message="Twilio WebSocket connected (initial handshake)",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={"stream_sid": stream_sid}
                )
                continue
            
            log_event(
                service="streaming_server",
                event="twilio_ws_inbound",
                status="info",
                message=f"Received Twilio WS event: {event_type}",
                extra={"event_type": event_type, "stream_sid": stream_sid, "call_sid": call_sid}
            )
            
            if event == "start":
                # Twilio sends streamSid + start info - extract SIDs from start payload
                stream_sid = msg.get("start", {}).get("streamSid")
                call_sid = msg.get("start", {}).get("callSid")
                trace_id = msg.get("start", {}).get("trace_id") or new_trace()
                
                if not stream_sid:
                    log_event(
                        service="streaming_server",
                        event="twilio_ws_missing_sid",
                        status="error",
                        message="Missing streamSid in Twilio start event",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={"message": msg}
                    )
                    continue
                
                if not call_sid:
                    call_sid = session_id  # Fallback to session ID
                
                # Create duplex controller for this call using sync adapter
                controller = controller_manager.get_or_create_controller_sync(
                    call_sid=call_sid,
                    stream_sid=stream_sid,
                    trace_id=trace_id,
                    websocket=ws
                )
                
                # Enqueue greeting message using sync wrapper
                greeting_text = "Hey, this is Sara calling from Noblecom Solutions. Just checking you can hear me clearly on your side."
                if controller:
                    controller.enqueue_greeting_sync(greeting_text)
                    greeting_enqueued = True
                else:
                    greeting_enqueued = False
                    log_event(
                        service="streaming_server",
                        event="greeting_enqueue_skipped",
                        status="warn",
                        message="Controller not available for greeting",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={"call_sid": call_sid, "stream_sid": stream_sid}
                    )
                
                log_event(
                    service="streaming_server",
                    event="twilio_stream_started",
                    status="info",
                    message="Twilio media stream started",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={
                        "call_sid": call_sid,
                        "stream_sid": stream_sid,
                        "controller_initialized": True,
                        "greeting_enqueued": greeting_enqueued
                    }
                )

            elif event == "media":
                # Inbound audio from Twilio (base64-encoded)
                media_data = msg.get("media") or {}
                b64_payload = media_data.get("payload")
                
                if b64_payload and call_sid and stream_sid:
                    try:
                        # Decode audio and forward to duplex controller using sync adapter
                        audio_bytes = base64.b64decode(b64_payload)
                        if controller:
                            # Use sync adapter for inbound audio processing
                            controller.handle_inbound_audio_sync(audio_bytes)
                            audio_processed = True
                        else:
                            # Get controller if not stored
                            controller = controller_manager.get_or_create_controller_sync(
                                call_sid=call_sid,
                                stream_sid=stream_sid,
                                trace_id=trace_id,
                                websocket=ws
                            )
                            if controller:
                                controller.handle_inbound_audio_sync(audio_bytes)
                                audio_processed = True
                            else:
                                audio_processed = False
                                log_event(
                                    service="streaming_server",
                                    event="inbound_audio_skipped",
                                    status="warn",
                                    message="Controller not available for inbound audio",
                                    trace_id=trace_id,
                                    session_id=session_id,
                                    extra={"call_sid": call_sid, "stream_sid": stream_sid, "audio_bytes": len(audio_bytes)}
                                )
                        
                        if audio_processed:
                            log_event(
                                service="streaming_server",
                                event="twilio_ws_inbound_media",
                                status="info",
                                message="Processed inbound media from Twilio",
                                trace_id=trace_id,
                                session_id=session_id,
                                extra={
                                    "payload_length": len(b64_payload),
                                    "audio_bytes": len(audio_bytes),
                                    "stream_sid": stream_sid,
                                    "call_sid": call_sid
                                }
                            )
                    except Exception as e:
                        stream_errors_total.labels(error_type="media_decode").inc()
                        log_event(
                            service="streaming_server",
                            event="media_processing_error",
                            status="error",
                            message="Error processing inbound media",
                            trace_id=trace_id,
                            session_id=session_id,
                            extra={"error": str(e), "call_sid": call_sid, "stream_sid": stream_sid}
                        )

            elif event == "mark":
                # Marks are optional sync points - log only (no handler)
                mark_name = msg.get("mark", {}).get("name")
                if mark_name and call_sid and stream_sid:
                    log_event(
                        service="streaming_server",
                        event="twilio_ws_mark",
                        status="info",
                        message="Received mark event from Twilio",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={"mark_name": mark_name, "call_sid": call_sid, "stream_sid": stream_sid}
                    )

            elif event == "stop":
                # graceful end from Twilio
                if call_sid and stream_sid:
                    # Cleanup controller using sync adapters
                    if controller:
                        controller.stop_sync()
                    controller_manager.cleanup_controller_sync(call_sid, stream_sid, trace_id)
                    controller = None
                
                log_event(
                    service="streaming_server",
                    event="twilio_media_ws_session_stopped",
                    status="info",
                    message="Twilio media WebSocket session stopped",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={"call_sid": call_sid, "stream_sid": stream_sid}
                )
                break

            else:
                # Unknown event - log at debug level to reduce noise
                log_event(
                    service="streaming_server",
                    event="twilio_ws_unknown_event",
                    status="debug",  # Downgraded from info to debug
                    message="Received unknown event type from Twilio",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={"event_type": event, "call_sid": call_sid, "stream_sid": stream_sid}
                )

    except Exception as e:
        stream_errors_total.labels(error_type="ws_media").inc()
        log_event(
            service="streaming_server",
            event="twilio_media_stream_error",
            status="error",
            message="Twilio media stream error",
            session_id=session_id,
            trace_id=trace_id,
            extra={
                "error": str(e),
                "stack": traceback.format_exc(),
                "call_sid": call_sid,
                "stream_sid": stream_sid
            }
        )
        # Capture exception with Sentry
        capture_exception_safe(e, {
            "service": "streaming_server",
            "session_id": session_id,
            "trace_id": trace_id,
            "call_sid": call_sid,
            "stream_sid": stream_sid,
            "endpoint": "media_websocket"
        })
    finally:
        # Cleanup
        if call_sid:
            try:
                # Cleanup controller if still exists using sync adapters
                if controller:
                    controller.stop_sync()
                controller_manager.cleanup_controller_sync(call_sid, stream_sid, trace_id)
                log_event(
                    service="streaming_server",
                    event="twilio_ws_controller_cleanup",
                    status="info",
                    message="Cleaned up duplex controller for Twilio call",
                    session_id=session_id,
                    trace_id=trace_id,
                    extra={"call_sid": call_sid}
                )
            except Exception as e:
                log_event(
                    service="streaming_server",
                    event="twilio_ws_cleanup_error",
                    status="warn",
                    message="Error cleaning up duplex controller",
                    session_id=session_id,
                    trace_id=trace_id,
                    extra={"call_sid": call_sid, "error": str(e)}
                )
        
        stream_state_mgr.remove_stream(session_id)
        streams_manager.decrement_global_count()
        
        log_event(
            service="streaming_server",
            event="twilio_media_stream_closed",
            status="info",
            message="Twilio media stream connection closed",
            session_id=session_id,
            trace_id=trace_id,
            extra={
                "call_sid": call_sid,
                "stream_sid": stream_sid,
                "duration_seconds": round(time.time() - start_ts, 2),
                "outbound_frames_sent": outbound_frame_sequence,
                "active_connections": stream_state_mgr.get_active_stream_count()
            }
        )

# --------------------------------------------------------------------------
# SSE Streaming Endpoint with Enhanced Resilience and Retry Logic
# --------------------------------------------------------------------------
@app.route("/stream", methods=["POST"])
def stream():
    trace_id = None
    session_id = None
    stream_start_time = time.time()
    global_count_inc = False
    
    try:
        get_metrics().increment_metric("stream_requests_total", 1)
        stream_events_total.labels(event_type="stream_request").inc()
    except Exception:
        pass

    # Check concurrent streams capacity
    if not streams_manager.can_accept_new_stream():
        log_event(
            service="streaming_server",
            event="stream_rejected_capacity",
            status="info",
            message="Stream request rejected - maximum concurrent streams reached",
            extra={"max_streams": streams_manager.max_streams},
        )
        return jsonify({"error": "Service at capacity", "reason": "max_concurrent_streams"}), 503

    try:
        data = request.get_json(silent=True) or {}
        session_id = data.get("session_id") or str(uuid.uuid4())
        trace_id = data.get("trace_id") or new_trace()
        user_text = (data.get("text") or data.get("message") or "").strip()

        if not user_text:
            log_event(
                service="streaming_server",
                event="stream_rejected",
                status="error",
                message="No input text provided",
                trace_id=trace_id,
                session_id=session_id,
            )
            stream_errors_total.labels(error_type="no_input_text").inc()
            return jsonify({"error": "No input text"}), 400

        # Increment global stream count
        if not streams_manager.increment_global_count():
            stream_rejected_total.labels(reason="global_max_concurrent").inc()
            log_event(
                service="streaming_server",
                event="stream_rejected_global_capacity_at_increment",
                status="info",
                message="Stream request rejected at atomic increment step",
                extra={"max_streams": streams_manager.max_streams},
            )
            return jsonify({"error": "Service at capacity", "reason": "global_max_concurrent_streams"}), 503

        global_count_inc = True

        # Create initial stream state
        stream_state = {
            "session_id": session_id,
            "trace_id": trace_id,
            "user_text": user_text,
            "stage": "received",
            "start_time": stream_start_time,
            "last_checkpoint": stream_start_time,
            "last_heartbeat": stream_start_time,
            "heartbeat_count": 0
        }
        
        # Use thread-safe stream addition
        stream_state_mgr.add_stream(session_id, stream_state)
        
        # Track connection opened
        sse_connections_opened_total.inc()
        
        # Save initial checkpoint
        stream_state_mgr.create_checkpoint(session_id, stream_state)

        log_event(
            service="streaming_server",
            event="client_connected",
            status="ok",
            message=f"SSE client connected for stream session ({len(user_text)} chars)",
            trace_id=trace_id,
            session_id=session_id,
            extra={
                "text_length": len(user_text), 
                "stage": "initialized",
                "active_connections": stream_state_mgr.get_active_stream_count(),
                "max_concurrent_streams": streams_manager.max_streams,
                "env_mode": os.getenv("ENV_MODE", "unknown")
            },
        )

        @stream_with_context
        def event_stream() -> Generator[str, None, None]:
            nonlocal global_count_inc
            last_heartbeat_time = time.time()
            total_backpressure_events = 0
            
            try:
                # Send initial status with backpressure detection
                status_data = sse_format("status", {"stage": "received", "trace_id": trace_id})
                start_time = time.time()
                yield status_data  # ACTUAL SEND
                send_time = time.time() - start_time
                if send_time > BACKPRESSURE_THRESHOLD:
                    sse_backpressure_events_total.inc()
                    total_backpressure_events += 1
                    log_event(
                        service="streaming_server",
                        event="backpressure_detected",
                        status="warn",
                        message="Slow SSE send detected for status",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={
                            "send_duration_seconds": round(send_time, 3),
                            "operation": "status",
                            "threshold_seconds": BACKPRESSURE_THRESHOLD
                        },
                    )

                # Main processing loop with heartbeat and retry logic
                start_infer = time.time()
                with latency_seconds.labels(stage="inference").time():
                    # Use retry wrapper for inference
                    reply_text = with_retry(
                        lambda: generate_reply(user_text, trace_id=trace_id),
                        operation_name="inference",
                        max_retries=2,
                        delay=1.0
                    )
                infer_ms = round((time.time() - start_infer) * 1000, 2)

                # Update stream state
                stream_state["stage"] = "inference_complete"
                stream_state["inference_latency_ms"] = infer_ms
                stream_state["reply_text_length"] = len(reply_text)

                # Check for heartbeat timeout
                if check_heartbeat_timeout(stream_state):
                    log_event(
                        service="streaming_server",
                        event="heartbeat_timeout_detected",
                        status="warn",
                        message="Heartbeat timeout detected during inference",
                        trace_id=trace_id,
                        session_id=session_id,
                    )

                # Send heartbeat if needed during long processing
                current_time = time.time()
                if current_time - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                    heartbeat_data = sse_format("heartbeat", {
                        "timestamp": current_time,
                        "stage": "inference_complete"
                    })
                    start_time = time.time()
                    yield heartbeat_data  # ACTUAL SEND
                    send_time = time.time() - start_time
                    if send_time > BACKPRESSURE_THRESHOLD:
                        sse_backpressure_events_total.inc()
                        total_backpressure_events += 1
                    
                    if send_time <= BACKPRESSURE_THRESHOLD:
                        record_heartbeat(stream_state)
                        last_heartbeat_time = current_time
                    else:
                        log_event(
                            service="streaming_server",
                            event="heartbeat_send_failed",
                            status="warn",
                            message="Failed to send heartbeat due to backpressure",
                            trace_id=trace_id,
                            session_id=session_id,
                        )

                # Save checkpoint after inference
                if time.time() - stream_state["last_checkpoint"] > 30:
                    stream_state_mgr.create_checkpoint(session_id, stream_state)
                    stream_state["last_checkpoint"] = time.time()

                try:
                    get_metrics().observe_latency("inference_latency_ms", infer_ms)
                except Exception:
                    pass

                try:
                    stream_latency_ms.observe(infer_ms)
                except Exception:
                    pass

                log_event(
                    service="streaming_server",
                    event="inference_done",
                    status="ok",
                    message=f"Inference completed ({len(reply_text)} chars)",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={
                        "inference_latency_ms": infer_ms,
                        "reply_text_length": len(reply_text),
                        "stage": "inference_complete"
                    },
                )
                
                # Send reply text with backpressure detection
                reply_data = sse_format("reply_text", {"text": reply_text})
                start_time = time.time()
                yield reply_data  # ACTUAL SEND
                send_time = time.time() - start_time
                if send_time > BACKPRESSURE_THRESHOLD:
                    sse_backpressure_events_total.inc()
                    total_backpressure_events += 1

                start_tts = time.time()
                with latency_seconds.labels(stage="tts").time():
                    # Use retry wrapper for TTS
                    tts_result = with_retry(
                        lambda: run_tts(
                            {"text": reply_text, "trace_id": trace_id, "session_id": session_id},
                            inline=True,
                        ),
                        operation_name="tts_generation",
                        max_retries=2,
                        delay=1.0
                    )
                tts_ms = round((time.time() - start_tts) * 1000, 2)

                # Check for heartbeat timeout during TTS
                if check_heartbeat_timeout(stream_state):
                    log_event(
                        service="streaming_server",
                        event="heartbeat_timeout_detected_tts",
                        status="warn",
                        message="Heartbeat timeout detected during TTS",
                        trace_id=trace_id,
                        session_id=session_id,
                    )

                # Send heartbeat during TTS processing if needed
                current_time = time.time()
                if current_time - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                    heartbeat_data = sse_format("heartbeat", {
                        "timestamp": current_time,
                        "stage": "tts_processing"
                    })
                    start_time = time.time()
                    yield heartbeat_data  # ACTUAL SEND
                    send_time = time.time() - start_time
                    if send_time > BACKPRESSURE_THRESHOLD:
                        sse_backpressure_events_total.inc()
                        total_backpressure_events += 1
                    
                    if send_time <= BACKPRESSURE_THRESHOLD:
                        record_heartbeat(stream_state)
                        last_heartbeat_time = current_time
                    else:
                        log_event(
                            service="streaming_server",
                            event="heartbeat_send_failed_tts",
                            status="warn",
                            message="Failed to send heartbeat during TTS due to backpressure",
                            trace_id=trace_id,
                            session_id=session_id,
                        )

                if isinstance(tts_result, dict) and tts_result.get("error"):
                    stream_errors_total.labels(error_type="tts_generation").inc()
                    log_event(
                        service="streaming_server",
                        event="tts_failed",
                        status="error",
                        message="TTS generation failed",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={"tts_result": tts_result, "tts_latency_ms": tts_ms},
                    )
                    error_data = sse_format("error", tts_result)
                    start_time = time.time()
                    yield error_data  # ACTUAL SEND
                    send_time = time.time() - start_time
                    if send_time > BACKPRESSURE_THRESHOLD:
                        total_backpressure_events += 1
                    return

                if isinstance(tts_result, dict) and tts_result.get("cached"):
                    stream_events_total.labels(event_type="tts_cache_hit").inc()
                    log_event(
                        service="streaming_server",
                        event="cache_hit",
                        status="ok",
                        message="TTS result served from cache",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={"cached": True, "tts_latency_ms": tts_ms},
                    )

                # If we have a bytes count from TTS, increment the shared Counter
                if isinstance(tts_result, dict):
                    bytes_out = tts_result.get("bytes", None)
                    if isinstance(bytes_out, (int, float)):
                        try:
                            stream_bytes_out_total.inc(bytes_out)
                        except Exception:
                            pass

                audio_url = tts_result.get("audio_url") if isinstance(tts_result, dict) else None
                
                # Update final stream state
                stream_state["stage"] = "complete"
                stream_state["tts_latency_ms"] = tts_ms
                stream_state["audio_url"] = audio_url
                stream_state["end_time"] = time.time()

                # Final checkpoint
                stream_state_mgr.create_checkpoint(session_id, stream_state)

                log_event(
                    service="streaming_server",
                    event="tts_done",
                    status="ok",
                    message="TTS generated successfully",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={
                        "audio_url": audio_url, 
                        "tts_latency_ms": tts_ms,
                        "stage": "complete",
                        "total_duration_ms": round((time.time() - stream_state["start_time"]) * 1000, 2)
                    },
                )

                # Send final events with backpressure detection
                audio_data = sse_format("audio_ready", {"url": audio_url})
                start_time = time.time()
                yield audio_data  # ACTUAL SEND
                send_time = time.time() - start_time
                if send_time > BACKPRESSURE_THRESHOLD:
                    sse_backpressure_events_total.inc()
                    total_backpressure_events += 1
                
                complete_data = sse_format("complete", {"trace_id": trace_id, "session_id": session_id})
                start_time = time.time()
                yield complete_data  # ACTUAL SEND
                send_time = time.time() - start_time
                if send_time > BACKPRESSURE_THRESHOLD:
                    total_backpressure_events += 1

                # Record successful stream duration
                stream_duration = time.time() - stream_start_time
                stream_duration_seconds.observe(stream_duration)
                sse_connection_duration_ms.observe(stream_duration * 1000)

                # Log backpressure summary if any issues detected
                if total_backpressure_events > 0:
                    log_event(
                        service="streaming_server",
                        event="backpressure_summary",
                        status="warn",
                        message=f"SSE stream completed with backpressure events",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={
                            "total_backpressure_events": total_backpressure_events,
                            "total_duration_seconds": round(stream_duration, 2)
                        },
                    )

            except Exception as exc:
                err_id = str(uuid.uuid4())
                log_event(
                    service="streaming_server",
                    event="stream_exception",
                    status="error",
                    message=str(exc),
                    trace_id=trace_id or new_trace(),
                    session_id=session_id or "unknown",
                    extra={
                        "error_id": err_id, 
                        "stack": traceback.format_exc(),
                        "stage": stream_state.get("stage", "unknown")
                    },
                )
                # Capture exception with Sentry
                capture_exception_safe(exc, {
                    "service": "streaming_server",
                    "session_id": session_id,
                    "trace_id": trace_id,
                    "stage": stream_state.get("stage", "unknown")
                })
                stream_errors_total.labels(error_type="stream_internal").inc()
                
                # Try to send error event (best effort)
                try:
                    error_data = sse_format("error", {"message": "Streaming error", "error_id": err_id})
                    yield error_data  # ACTUAL SEND
                except Exception:
                    pass
                
            finally:
                # Clean up stream state regardless of success or failure
                # Use thread-safe stream removal
                stream_state_mgr.remove_stream(session_id)
                
                # Track connection closed
                close_reason = "completed" if stream_state.get("stage") == "complete" else "error"
                sse_connections_closed_total.labels(reason=close_reason).inc()
                
                # Decrement global stream count
                if global_count_inc:
                    # Best effort - don't fail the stream if decrement fails
                    streams_manager.decrement_global_count()
                    global_count_inc = False
                
                log_event(
                    service="streaming_server",
                    event="client_disconnected",
                    status="info",
                    message="SSE client disconnected",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={
                        "stage": stream_state.get("stage", "unknown"),
                        "total_duration": round(time.time() - stream_start_time, 2),
                        "active_connections": stream_state_mgr.get_active_stream_count(),
                        "heartbeats_sent": stream_state.get("heartbeat_count", 0),
                        "backpressure_events": total_backpressure_events,
                        "close_reason": close_reason,
                        "env_mode": os.getenv("ENV_MODE", "unknown")
                    },
                )

        headers = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        return Response(event_stream(), headers=headers)
    except Exception as e:
        trace = trace_id or new_trace()
        log_event(
            service="streaming_server",
            event="fatal_error",
            status="error",
            message=str(e),
            trace_id=trace,
            session_id=session_id or "unknown",
            extra={"stack": traceback.format_exc()},
        )
        # Capture fatal exception with Sentry
        capture_exception_safe(e, {
            "service": "streaming_server", 
            "session_id": session_id,
            "trace_id": trace,
            "stage": "stream_setup"
        })
        stream_errors_total.labels(error_type="fatal").inc()
        
        # Clean up on fatal error
        if global_count_inc:
            streams_manager.decrement_global_count()
            global_count_inc = False
            
        # Use thread-safe stream removal
        if session_id:
            stream_state_mgr.remove_stream(session_id)
            sse_connections_closed_total.labels(reason="fatal_error").inc()
            
        return jsonify({"error": "Internal server error", "trace_id": trace}), 500

# --------------------------------------------------------------------------
# Twilio webhook compatibility
# --------------------------------------------------------------------------
@app.route("/twilio_tts", methods=["POST"])
def twilio_tts():
    from flask import Response as TwilioResponse
    
    trace_id = new_trace()
    session_id = str(uuid.uuid4())

    try:
        get_metrics().increment_metric("twilio_requests_total", 1)
        stream_events_total.labels(event_type="twilio_request").inc()
    except Exception:
        pass

    try:
        text = request.form.get("SpeechResult") or request.form.get("text") or "Hello from Sara AI"
        with latency_seconds.labels(stage="twilio_tts").time():
            # Use retry wrapper for Twilio TTS
            tts_result = with_retry(
                lambda: run_tts({"text": text, "session_id": session_id, "trace_id": trace_id}, inline=True),
                operation_name="twilio_tts",
                max_retries=2,
                delay=1.0
            )

        if isinstance(tts_result, dict) and tts_result.get("error"):
            stream_errors_total.labels(error_type="twilio_tts").inc()
            log_event(
                service="streaming_server",
                event="twilio_tts_failed",
                status="error",
                message="TTS generation for Twilio failed",
                trace_id=trace_id,
                session_id=session_id,
                extra={"tts_result": tts_result},
            )
            return TwilioResponse(
                "<Response><Say>Sorry, an error occurred generating speech.</Say></Response>",
                mimetype="application/xml",
            )

        audio_url = tts_result.get("audio_url") if isinstance(tts_result, dict) else None
        log_event(
            service="streaming_server",
            event="twilio_tts_done",
            status="ok",
            message="Generated Twilio-compatible TTS",
            trace_id=trace_id,
            session_id=session_id,
            extra={"audio_url": audio_url, "text_length": len(text)},
        )

        twiml = f"<Response><Play>{audio_url}</Play></Response>"
        return TwilioResponse(twiml, mimetype="application/xml")

    except Exception as e:
        log_event(
            service="streaming_server",
            event="twilio_tts_exception",
            status="error",
            message="Twilio TTS handler error",
            trace_id=trace_id,
            session_id=session_id,
            extra={"stack": traceback.format_exc()},
        )
        # Capture Twilio exception with Sentry
        capture_exception_safe(e, {
            "service": "streaming_server",
            "session_id": session_id,
            "trace_id": trace_id,
            "endpoint": "twilio_tts"
        })
        stream_errors_total.labels(error_type="twilio_exception").inc()
        return TwilioResponse(
            "<Response><Say>Sorry, an internal error occurred.</Say></Response>",
            mimetype="application/xml",
        )

# --------------------------------------------------------------------------
# Stream Recovery Endpoint
# --------------------------------------------------------------------------
@app.route("/stream/recover/<session_id>", methods=["GET"])
def recover_stream(session_id: str):
    """Recover stream state from checkpoint"""
    try:
        state = stream_state_mgr.restore_checkpoint(session_id)
        if state:
            log_event(
                service="streaming_server",
                event="stream_recovered",
                status="info",
                message="Stream state checkpoint restored",
                extra={"session_id": session_id, "stage": state.get("stage")},
            )
            stream_events_total.labels(event_type="stream_recovery").inc()
            return jsonify({"status": "recovered", "state": state}), 200
        else:
            log_event(
                service="streaming_server",
                event="stream_recovery_failed",
                status="warn",
                message="No checkpoint found for stream recovery",
                extra={"session_id": session_id},
            )
            stream_errors_total.labels(error_type="recovery_not_found").inc()
            return jsonify({"status": "not_found"}), 404
    except Exception as e:
        log_event(
            service="streaming_server",
            event="stream_recovery_error",
            status="error",
            message="Stream recovery failed",
            extra={"session_id": session_id, "error": str(e)},
        )
        stream_errors_total.labels(error_type="recovery_error").inc()
        return jsonify({"status": "error", "message": str(e)}), 500

# --------------------------------------------------------------------------
# Entrypoint with Graceful Shutdown
# --------------------------------------------------------------------------
if __name__ == "__main__":
    # Register shutdown callbacks
    graceful_shutdown.register_shutdown_callback(health_monitor.stop)
    graceful_shutdown.register_shutdown_callback(stream_state_mgr.cleanup_all_streams)
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, graceful_shutdown.initiate_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown.initiate_shutdown)

    # Initialize metrics system on startup with delay to avoid race conditions
    def _delayed_init():
        try:
            _ensure_initialized()
            log_event(
                service="streaming", 
                event="metrics_initialized", 
                status="ok",
                message="Metrics initialized successfully",
                extra={
                    "max_concurrent_streams": streams_manager.max_streams,
                    "heartbeat_interval": HEARTBEAT_INTERVAL,
                    "env_mode": os.getenv("ENV_MODE", "unknown"),
                    "phase": os.getenv("SARA_PHASE", "unknown")
                }
            )
            
            # Final startup sanity check
            log_event(
                service="streaming",
                event="startup_summary",
                status="ok",
                message="Streaming server startup complete",
                extra={
                    "env_mode": os.getenv("ENV_MODE"),
                    "redis_url": _mask_url(os.getenv("REDIS_URL", "")),
                    "app_url": _mask_url(os.getenv("APP_URL", "")),
                    "streaming_url": _mask_url(os.getenv("STREAMING_URL", "")),
                    "phase": os.getenv("SARA_PHASE", "unknown"),
                    "max_concurrent_streams": streams_manager.max_streams,
                    "heartbeat_interval": HEARTBEAT_INTERVAL
                }
            )
        except Exception as e:
            log_event(
                service="streaming", 
                event="metrics_init_failed", 
                status="error",
                message=str(e)
            )

    # Delay 5s to allow Prometheus and Redis to be ready - USE threading.Timer consistently
    threading.Timer(5.0, _delayed_init).start()

    # Enhanced cleanup handler
    def _cleanup():
        try:
            health_monitor.stop()
            stream_state_mgr.cleanup_all_streams()
            log_event(
                service="streaming", 
                event="shutdown", 
                status="ok",
                message="Streaming server exited cleanly",
                extra={
                    "active_streams_cleaned": stream_state_mgr.get_active_stream_count(),
                    "uptime_seconds": time.time() - health_monitor.uptime_start,
                    "env_mode": os.getenv("ENV_MODE", "unknown")
                }
            )
        except Exception as e:
            log_event(
                service="streaming", 
                event="shutdown_error", 
                status="warn",
                message=str(e)
            )
    atexit.register(_cleanup)

    log_event(
        service="streaming",
        event="startup",
        status="ok",
        message="Streaming server initialized with unified metrics registry",
        extra={
            "phase": "11-F",
            "max_concurrent_streams": streams_manager.max_streams,
            "heartbeat_interval": HEARTBEAT_INTERVAL,
            "graceful_shutdown_enabled": True,
            "env_mode": os.getenv("ENV_MODE", "unknown")
        }
    )

    log_event(
        service="streaming", 
        event="metrics_namespace",
        status="info", 
        message="Per-service metric isolation deferred to 11-G"
    )

    # Render requires binding to $PORT; fall back to STREAMING_PORT then 5000
    port = int(os.getenv("PORT", os.getenv("STREAMING_PORT", "5000")))
    app.run(host="0.0.0.0", port=port, threaded=True)