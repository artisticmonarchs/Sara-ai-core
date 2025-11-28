"""voice_pipeline.py — Phase 12
Production-grade voice orchestration with duplex streaming, Prometheus metrics, and structured observability.
"""

from __future__ import annotations
import json
import time
import traceback
from typing import Optional, Dict, Any, Callable, List
import threading
import uuid
from collections import deque  # NEW: More efficient jitter buffer

from celery_app import celery
from logging_utils import get_json_logger, log_event, get_trace_id
import signal
import sys

# MOVED: Signal handlers to main block to avoid Celery conflicts

# Phase 11-F canonical imports
from config import Config
from redis_client import get_redis_client, safe_redis_operation

# PHASE 11-F ADDITION: Duplex streaming integration
try:
    from duplex_voice_controller import DuplexVoiceController
    from realtime_voice_engine import RealtimeVoiceEngine
    DUPLEX_AVAILABLE = True
except ImportError:
    DUPLEX_AVAILABLE = False
    DuplexVoiceController = None
    RealtimeVoiceEngine = None

# Lazy imports to avoid circular dependencies
def _get_metrics():
    """Lazy import metrics to avoid circular imports"""
    try:
        from metrics_collector import increment_metric, observe_latency, set_gauge
        return increment_metric, observe_latency, set_gauge
    except Exception:
        def _noop_inc(*a, **k): pass
        def _noop_obs(*a, **k): pass
        def _noop_gauge(*a, **k): pass
        return _noop_inc, _noop_obs, _noop_gauge

def _get_sentry():
    """Lazy import sentry to avoid import-time side effects"""
    try:
        from sentry_utils import init_sentry, capture_exception_safe
        return init_sentry, capture_exception_safe
    except Exception:
        def _noop_init(*a, **k): pass
        def _noop_capture(*a, **k): pass
        return _noop_init, _noop_capture

def _get_global_metrics():
    """Lazy import global_metrics_store to avoid import-time side effects"""
    try:
        from global_metrics_store import start_background_sync
        return start_background_sync
    except Exception:
        def _noop_sync(*a, **k): pass
        return _noop_sync

# --------------------------------------------------------------------------
# Compliance Metadata - UPDATED FOR PHASE 12
# --------------------------------------------------------------------------
__phase__ = "12"
__schema_version__ = "phase_12_v1"
__service__ = "voice_pipeline"

# --------------------------------------------------------------------------
# NEW: Jitter Buffer Implementation
# --------------------------------------------------------------------------
class JitterBuffer:
    """Thread-safe jitter buffer for audio frame management"""
    def __init__(self, max_size: int = 100):
        self._buffer = deque()  # FIX: Use deque for O(1) operations
        self._max_size = max_size
        self._lock = threading.RLock()
        
    def enqueue(self, audio_chunk: bytes, call_sid: str, trace_id: str) -> bool:
        """Add audio chunk to buffer, returns True if successful, False if dropped"""
        with self._lock:
            if len(self._buffer) >= self._max_size:
                try:
                    increment_metric, _, _ = _get_metrics()
                    increment_metric("voice_pipeline_dropped_frames_total")
                except Exception:
                    pass
                _structured_log(
                    "frame_dropped",
                    level="warn",
                    message="Audio frame dropped due to full jitter buffer",
                    trace_id=trace_id,
                    call_sid=call_sid,
                    extra={"buffer_depth": len(self._buffer), "max_size": self._max_size}
                )
                return False
                
            self._buffer.append(audio_chunk)
            # Set gauge to actual buffer length (thread-safe)
            current_depth = len(self._buffer)
            _update_jitter_buffer_depth(current_depth)
            return True
            
    def dequeue(self) -> Optional[bytes]:
        """Remove and return next audio chunk from buffer"""
        with self._lock:
            if not self._buffer:
                # Edge case: buffer empty - set to 0, don't decrement
                _update_jitter_buffer_depth(0)
                return None
                
            chunk = self._buffer.popleft()  # FIX: O(1) operation with deque
            # Set gauge to actual buffer length (thread-safe)
            current_depth = len(self._buffer)
            _update_jitter_buffer_depth(max(0, current_depth))  # Clamp to ≥ 0
            return chunk
            
    def get_depth(self) -> int:
        """Get current buffer depth"""
        with self._lock:
            return len(self._buffer)

# Global jitter buffer instance
_jitter_buffer = JitterBuffer()

def _update_jitter_buffer_depth(depth: int):
    """Update jitter buffer depth gauge metric"""
    try:
        _, _, set_gauge = _get_metrics()
        set_gauge("voice_jitter_buffer_depth", depth)
    except Exception:
        pass

# --------------------------------------------------------------------------
# NEW: Barge-in Cancellation System
# --------------------------------------------------------------------------
def cancel_pending_tts_tasks(call_sid: str, trace_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Cancel all in-flight and pending TTS tasks for a call when barge-in occurs.
    Returns cancellation summary.
    """
    trace = trace_id or get_trace_id()
    start_time = time.time()
    
    try:
        # Get current TTS task IDs from session state
        session_state = _get_call_state(call_sid) or {}
        tts_task_ids = session_state.get("active_tts_tasks", [])
        
        if not tts_task_ids:
            return {"ok": True, "cancelled_count": 0, "message": "No TTS tasks to cancel"}
        
        cancelled_count = 0
        for task_id in tts_task_ids:
            try:
                # Revoke the Celery task
                celery.control.revoke(task_id, terminate=True)
                cancelled_count += 1
                _structured_log(
                    "tts_task_cancelled",
                    level="info",
                    message="TTS task cancelled due to barge-in",
                    trace_id=trace,
                    call_sid=call_sid,
                    extra={"task_id": task_id}
                )
            except Exception as e:
                _structured_log(
                    "tts_cancel_failed",
                    level="warn",
                    message="Failed to cancel TTS task",
                    trace_id=trace,
                    call_sid=call_sid,
                    extra={"task_id": task_id, "error": str(e)}
                )
        
        # Clear the TTS task list from session state
        session_state["active_tts_tasks"] = []
        _store_call_state(call_sid, trace, session_state)
        
        # Record interruption metric
        try:
            increment_metric, _, _ = _get_metrics()
            increment_metric("voice_pipeline_interruptions_total")
        except Exception:
            pass
        
        # NEW: Enhanced interruption logging with structured data
        _structured_log(
            "barge_in_interruption",
            level="info",
            message="Barge-in detected, cancelled pending TTS tasks",
            trace_id=trace,
            call_sid=call_sid,
            extra={
                "tts_task_ids": tts_task_ids,
                "cancelled_count": cancelled_count,
                "reason": "user_interruption"
            }
        )
        
        # NEW: Record cancellation latency
        latency_ms = (time.time() - start_time) * 1000
        try:
            _, observe_latency, _ = _get_metrics()
            observe_latency("voice_pipeline_barge_in_cancel_latency_seconds", latency_ms / 1000.0)
        except Exception:
            pass
        
        _record_metrics("barge_in_cancellation", "success", latency_ms, trace)
        
        return {
            "ok": True,
            "cancelled_count": cancelled_count,
            "total_tasks": len(tts_task_ids),
            "trace_id": trace
        }
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("barge_in_cancellation", "failure", latency_ms, trace)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace, "call_sid": call_sid})
        _structured_log(
            "barge_in_cancel_failed",
            level="error",
            message="Barge-in cancellation failed",
            trace_id=trace,
            call_sid=call_sid,
            extra={"error": str(e), "traceback": traceback.format_exc()}
        )
        return {"ok": False, "error": str(e)}

def _register_tts_task(call_sid: str, task_id: str, trace_id: str):
    """Register a TTS task for potential cancellation"""
    session_state = _get_call_state(call_sid) or {}
    active_tasks = session_state.get("active_tts_tasks", [])
    active_tasks.append(task_id)
    session_state["active_tts_tasks"] = active_tasks
    _store_call_state(call_sid, trace_id, session_state)

def _unregister_tts_task(call_sid: str, task_id: str, trace_id: str):
    """Remove a TTS task from cancellation tracking"""
    session_state = _get_call_state(call_sid) or {}
    active_tasks = session_state.get("active_tts_tasks", [])
    if task_id in active_tasks:
        active_tasks.remove(task_id)
        session_state["active_tts_tasks"] = active_tasks
        _store_call_state(call_sid, trace_id, session_state)

# --------------------------------------------------------------------------
# Initialization - ENHANCED FOR DUPLEX STREAMING
# --------------------------------------------------------------------------
def initialize_voice_pipeline():
    """Initialize voice pipeline services with duplex streaming support - call this explicitly from app startup"""
    init_sentry, capture_exception_safe = _get_sentry()
    start_background_sync = _get_global_metrics()
    
    init_sentry()
    start_background_sync(service_name="voice_pipeline")
    
    # PHASE 11-F ADDITION: Initialize duplex controller if available
    duplex_controller = None
    if DUPLEX_AVAILABLE:
        try:
            duplex_controller = DuplexVoiceController()
            log_event(
                service=__service__,
                event="duplex_controller_initialized", 
                status="info",
                message="Duplex voice controller initialized successfully",
                trace_id=get_trace_id()
            )
        except Exception as e:
            log_event(
                service=__service__,
                event="duplex_controller_init_failed", 
                status="warn",
                message="Failed to initialize duplex controller, falling back to standard mode",
                trace_id=get_trace_id(),
                extra={"error": str(e)}
            )
    
    log_event(
        service=__service__,
        event="startup_init", 
        status="info",
        message="Voice pipeline initialized with Phase 12 compliance and duplex streaming",
        extra={"phase": __phase__, "schema_version": __schema_version__, "duplex_available": DUPLEX_AVAILABLE}
    )
    
    return duplex_controller

logger = get_json_logger("celery_task")

# Optional imports (ASR / Twilio) - ENHANCED WITH LAZY LOADING
try:
    from asr_service import process_audio_buffer
except Exception:
    process_audio_buffer = None

try:
    import twilio_client
except Exception:
    twilio_client = None

# --------------------------------------------------------------------------
# Centralized configuration (from config.settings) - ENHANCED
# --------------------------------------------------------------------------
SERVICE_NAME = __service__
SARA_ENV = Config.SARA_ENV

CALL_STATE_TTL = Config.CALL_STATE_TTL
PARTIAL_THROTTLE_SECONDS = Config.PARTIAL_THROTTLE_SECONDS
PARTIAL_THROTTLE_PREFIX = "partial_throttle:"

INFERENCE_TASK_NAME = Config.INFERENCE_TASK_NAME
EVENT_TASK_NAME = Config.EVENT_TASK_NAME
CELERY_VOICE_QUEUE = Config.CELERY_VOICE_QUEUE

# PHASE 11-F ADDITION: Duplex streaming configuration
DUPLEX_STREAMING_ENABLED = getattr(Config, 'DUPLEX_STREAMING_ENABLED', True) and DUPLEX_AVAILABLE

# --------------------------------------------------------------------------
# NEW: Enhanced Circuit Breaker Implementation with Inference Checks
# --------------------------------------------------------------------------
def _is_circuit_breaker_open(service: str = "voice_pipeline") -> bool:
    """Enhanced circuit breaker check with inference-specific checks."""
    try:
        client = get_redis_client()
        if not client:
            return False
        key = f"circuit_breaker:{service}:state"
        state = safe_redis_operation(lambda c: c.get(key))
        
        # Record circuit breaker check metric
        try:
            increment_metric, _, _ = _get_metrics()
            increment_metric("voice_pipeline_circuit_breaker_checks_total")
        except Exception:
            pass
            
        if state is None:
            return False
        if isinstance(state, bytes):
            try:
                state = state.decode("utf-8")
            except Exception:
                pass
        return str(state).lower() == "open"
    except Exception:
        # Best-effort: do not block on checker errors
        return False

def _check_inference_circuit_breaker() -> bool:
    """Specialized circuit breaker check for inference services."""
    try:
        client = get_redis_client()
        if not client:
            return False
            
        # Check GPT service circuit breaker
        gpt_key = "circuit_breaker:gpt_service:state"
        gpt_state = safe_redis_operation(lambda c: c.get(gpt_key))
        
        # Check TTS service circuit breaker  
        tts_key = "circuit_breaker:tts_service:state"
        tts_state = safe_redis_operation(lambda c: c.get(tts_key))
        
        # FIX: Properly decode bytes before comparison
        def _decode_state(state):
            if state is None:
                return None
            if isinstance(state, bytes):
                try:
                    return state.decode("utf-8")
                except Exception:
                    return str(state)
            return str(state)
        
        gpt_state = _decode_state(gpt_state)
        tts_state = _decode_state(tts_state)
        
        gpt_open = gpt_state and gpt_state.lower() == "open"
        tts_open = tts_state and tts_state.lower() == "open"
        
        if gpt_open or tts_open:
            _structured_log(
                "inference_circuit_open",
                level="warn",
                message="Inference circuit breaker open - blocking inference",
                trace_id=get_trace_id(),
                extra={"gpt_open": gpt_open, "tts_open": tts_open}
            )
            return True
            
        return False
    except Exception:
        return False

# --------------------------------------------------------------------------
# NEW: Async Task Processing with Enhanced Metrics - PHASE 12 UPDATED
# --------------------------------------------------------------------------
# FIX: Changed from @celery.shared_task to @celery.task for correct Celery usage
@celery.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={'max_retries': Config.CELERY_MAX_RETRIES},
    acks_late=True,
    name="voice_pipeline.process_audio_async"
)
def process_audio_async(self, call_sid: str, audio_chunk: bytes, payload: Optional[Dict[str, Any]] = None):
    """
    Async Celery task for non-blocking audio processing.
    """
    # Phase 12: Idempotency key pattern - CHECK but don't SET until success
    task_id = f"task:{self.request.id}"
    redis_client = get_redis_client()
    if redis_client and redis_client.exists(task_id):
        logger.info("Task duplicate detected, skipping execution", extra={"task": self.name, "id": self.request.id})
        return {"ok": True, "status": "duplicate_skipped"}

    trace_id = payload.get("trace_id") if payload else get_trace_id()
    start_time = time.time()
    
    logger.info("Task started", extra={"task": self.name, "id": self.request.id, "call_sid": call_sid, "trace_id": trace_id})
    
    try:
        # Update task state
        self.update_state(
            state="PROGRESS",
            meta={
                "call_sid": call_sid,
                "trace_id": trace_id,
                "chunk_size": len(audio_chunk),
                "stage": "processing"
            }
        )
        
        # Process audio chunk
        process_audio_chunk(call_sid, audio_chunk, payload, trace_id, async_processing=False)
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("async_audio_processing", "success", latency_ms, trace_id)
        
        # SUCCESS: Now set idempotency key
        if redis_client:
            redis_client.setex(task_id, 3600, "done")
        
        logger.info("Task completed successfully", extra={"task": self.name, "id": self.request.id, "call_sid": call_sid, "processing_time_ms": latency_ms})
        
        return {
            "ok": True,
            "call_sid": call_sid,
            "trace_id": trace_id,
            "processing_time_ms": latency_ms
        }
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("async_audio_processing", "failure", latency_ms, trace_id)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {
            "service": SERVICE_NAME,
            "trace_id": trace_id,
            "call_sid": call_sid,
            "task": "process_audio_async"
        })
        logger.error("Task failed", extra={
            "task": self.name, 
            "id": self.request.id, 
            "call_sid": call_sid,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "processing_time_ms": latency_ms
        })
        # Phase 12: Let Celery handle retries via decorator
        raise

# FIX: Changed from @celery.shared_task to @celery.task for correct Celery usage
@celery.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={'max_retries': Config.CELERY_MAX_RETRIES},
    acks_late=True,
    name="voice_pipeline.process_tts_async"
)
def process_tts_async(self, call_sid: str, text: str, payload: Optional[Dict[str, Any]] = None):
    """
    Async Celery task for non-blocking TTS processing.
    """
    # Phase 12: Idempotency key pattern - CHECK but don't SET until success
    task_id = f"task:{self.request.id}"
    redis_client = get_redis_client()
    if redis_client and redis_client.exists(task_id):
        logger.info("Task duplicate detected, skipping execution", extra={"task": self.name, "id": self.request.id})
        return {"ok": True, "status": "duplicate_skipped"}

    trace_id = payload.get("trace_id") if payload else get_trace_id()
    start_time = time.time()
    
    logger.info("Task started", extra={"task": self.name, "id": self.request.id, "call_sid": call_sid, "trace_id": trace_id})
    
    try:
        # Update task state
        self.update_state(
            state="PROGRESS",
            meta={
                "call_sid": call_sid,
                "trace_id": trace_id,
                "text_length": len(text),
                "stage": "tts_processing"
            }
        )
        
        # Check TTS circuit breaker before processing
        if _check_inference_circuit_breaker():
            _structured_log(
                "tts_circuit_blocked",
                level="warn",
                message="TTS processing blocked by circuit breaker",
                trace_id=trace_id,
                call_sid=call_sid
            )
            return {"ok": False, "error": "TTS service unavailable"}
        
        # Import TTS function lazily to avoid circular imports
        try:
            from tasks import run_tts
            tts_result = run_tts(
                {"text": text, "trace_id": trace_id, "call_sid": call_sid},
                inline=True
            )
            
            if isinstance(tts_result, dict) and tts_result.get("error"):
                raise Exception(f"TTS generation failed: {tts_result.get('error')}")
                
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("async_tts_processing", "success", latency_ms, trace_id)
            
            # Record TTS latency specifically
            try:
                _, observe_latency, _ = _get_metrics()
                observe_latency("voice_pipeline_tts_latency_seconds", latency_ms / 1000.0)
            except Exception:
                pass
            
            # SUCCESS: Now set idempotency key
            if redis_client:
                redis_client.setex(task_id, 3600, "done")
            
            logger.info("Task completed successfully", extra={"task": self.name, "id": self.request.id, "call_sid": call_sid, "processing_time_ms": latency_ms})
            
            return {
                "ok": True,
                "call_sid": call_sid,
                "trace_id": trace_id,
                "tts_result": tts_result,
                "processing_time_ms": latency_ms
            }
            
        except ImportError:
            raise Exception("TTS service not available")
            
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("async_tts_processing", "failure", latency_ms, trace_id)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {
            "service": SERVICE_NAME,
            "trace_id": trace_id,
            "call_sid": call_sid,
            "task": "process_tts_async"
        })
        logger.error("Task failed", extra={
            "task": self.name, 
            "id": self.request.id, 
            "call_sid": call_sid,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "processing_time_ms": latency_ms
        })
        # Phase 12: Let Celery handle retries via decorator
        raise
    finally:
        # Always unregister TTS task when complete
        _unregister_tts_task(call_sid, self.request.id, trace_id)

# FIX: Changed from @celery.shared_task to @celery.task for correct Celery usage
@celery.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={'max_retries': Config.CELERY_MAX_RETRIES},
    acks_late=True,
    name="voice_pipeline.process_inference_async"
)
def process_inference_async(self, call_sid: str, transcript: str, payload: Optional[Dict[str, Any]] = None):
    """
    Async Celery task for non-blocking inference processing with circuit breaker protection.
    """
    # Phase 12: Idempotency key pattern - CHECK but don't SET until success
    task_id = f"task:{self.request.id}"
    redis_client = get_redis_client()
    if redis_client and redis_client.exists(task_id):
        logger.info("Task duplicate detected, skipping execution", extra={"task": self.name, "id": self.request.id})
        return {"ok": True, "status": "duplicate_skipped"}

    trace_id = payload.get("trace_id") if payload else get_trace_id()
    start_time = time.time()
    
    logger.info("Task started", extra={"task": self.name, "id": self.request.id, "call_sid": call_sid, "trace_id": trace_id})
    
    try:
        # Update task state
        self.update_state(
            state="PROGRESS",
            meta={
                "call_sid": call_sid,
                "trace_id": trace_id,
                "transcript_length": len(transcript),
                "stage": "inference_processing"
            }
        )
        
        # Check inference circuit breaker before processing
        if _check_inference_circuit_breaker():
            _structured_log(
                "inference_circuit_blocked",
                level="warn",
                message="Inference processing blocked by circuit breaker",
                trace_id=trace_id,
                call_sid=call_sid
            )
            return {"ok": False, "error": "Inference service unavailable"}
        
        # Import GPT function lazily
        try:
            from gpt_client import generate_reply
            
            # Record inference start time
            inference_start = time.time()
            reply_text = generate_reply(transcript, trace_id=trace_id)
            inference_latency_ms = (time.time() - inference_start) * 1000
            
            # Record inference-specific metrics
            try:
                _, observe_latency, _ = _get_metrics()
                observe_latency("voice_pipeline_inference_latency_seconds", inference_latency_ms / 1000.0)
                observe_latency("voice_pipeline_gpt_latency_seconds", inference_latency_ms / 1000.0)
            except Exception:
                pass
            
            # Dispatch TTS asynchronously and register for potential cancellation
            tts_task = process_tts_async.delay(call_sid, reply_text, {"trace_id": trace_id})
            _register_tts_task(call_sid, tts_task.id, trace_id)
            
            total_latency_ms = (time.time() - start_time) * 1000
            _record_metrics("async_inference_processing", "success", total_latency_ms, trace_id)
            
            # SUCCESS: Now set idempotency key
            if redis_client:
                redis_client.setex(task_id, 3600, "done")
            
            logger.info("Task completed successfully", extra={"task": self.name, "id": self.request.id, "call_sid": call_sid, "processing_time_ms": total_latency_ms})
            
            return {
                "ok": True,
                "call_sid": call_sid,
                "trace_id": trace_id,
                "reply_text": reply_text,
                "inference_latency_ms": inference_latency_ms,
                "total_processing_time_ms": total_latency_ms,
                "tts_task_id": tts_task.id
            }
            
        except ImportError:
            raise Exception("GPT service not available")
            
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("async_inference_processing", "failure", latency_ms, trace_id)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {
            "service": SERVICE_NAME,
            "trace_id": trace_id,
            "call_sid": call_sid,
            "task": "process_inference_async"
        })
        logger.error("Task failed", extra={
            "task": self.name, 
            "id": self.request.id, 
            "call_sid": call_sid,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "processing_time_ms": latency_ms
        })
        # Phase 12: Let Celery handle retries via decorator
        raise

# --------------------------------------------------------------------------
# NEW: Call Session Management Functions with Enhanced TTL Management
# --------------------------------------------------------------------------
def start_call(call_sid: str, metadata: Optional[Dict[str, Any]] = None, trace_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Initialize a new call session with Redis-based tracking and TTL enforcement.
    Returns session metadata including trace_id and start timestamp.
    """
    trace = trace_id or get_trace_id()
    start_time = time.time()
    
    try:
        session_data = {
            "call_sid": call_sid,
            "trace_id": trace,
            "start_time": start_time,
            "status": "active",
            "last_activity": start_time,
            "metadata": metadata or {},
            "schema_version": __schema_version__,
            "active_tts_tasks": []  # NEW: Track TTS tasks for cancellation
        }
        
        # Store initial session state with TTL
        _store_call_state(call_sid, trace, session_data)
        
        # Record metrics
        try:
            increment_metric, _, _ = _get_metrics()
            increment_metric("voice_pipeline_calls_started_total")
        except Exception:
            pass
        
        _structured_log(
            "call_started",
            level="info",
            message="New call session started",
            trace_id=trace,
            call_sid=call_sid,
            extra={"metadata": metadata, "ttl_seconds": CALL_STATE_TTL}
        )
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("start_call", "success", latency_ms, trace)
        
        return {"ok": True, "session": session_data, "trace_id": trace}
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("start_call", "failure", latency_ms, trace)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace, "call_sid": call_sid})
        _structured_log(
            "call_start_failed",
            level="error",
            message="Failed to start call session",
            trace_id=trace,
            call_sid=call_sid,
            extra={"error": str(e), "traceback": traceback.format_exc()}
        )
        return {"ok": False, "error": str(e)}

def stop_call(call_sid: str, reason: str = "normal", trace_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Terminate a call session and clean up resources with TTL validation.
    Returns cleanup status and session summary.
    """
    trace = trace_id or get_trace_id()
    start_time = time.time()
    
    try:
        # Get final session state before cleanup
        session_state = _get_call_state(call_sid)
        
        # Clear session data
        clear_session(call_sid)
        
        # Clean up any partial throttle keys
        rr = _get_r_client()
        if rr:
            try:
                throttle_key = _partial_throttle_key(call_sid)
                safe_redis_operation(
                    lambda c: c.delete(throttle_key),
                    fallback=None,
                    operation_name="cleanup_throttle_key"
                )
            except Exception:
                pass
        
        # PHASE 11-F ADDITION: Notify duplex controller if available
        if DUPLEX_STREAMING_ENABLED and DuplexVoiceController:
            try:
                # FIX: Use proper duplex controller access pattern
                controller = DuplexVoiceController()
                controller.stop_call(call_sid, trace)
            except Exception as e:
                _structured_log(
                    "duplex_stop_failed",
                    level="warn",
                    message="Failed to stop call in duplex controller",
                    trace_id=trace,
                    call_sid=call_sid,
                    extra={"error": str(e)}
                )
        
        # Calculate call duration
        call_duration = 0
        if session_state and session_state.get("start_time"):
            call_duration = time.time() - session_state["start_time"]
        
        # Record metrics
        try:
            increment_metric, observe_latency, _ = _get_metrics()
            increment_metric("voice_pipeline_calls_stopped_total")
            observe_latency("voice_pipeline_call_duration_seconds", call_duration)
        except Exception:
            pass
        
        _structured_log(
            "call_stopped",
            level="info",
            message="Call session stopped",
            trace_id=trace,
            call_sid=call_sid,
            extra={
                "reason": reason,
                "duration_seconds": round(call_duration, 2),
                "final_state": session_state,
                "ttl_respected": True
            }
        )
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("stop_call", "success", latency_ms, trace)
        
        return {
            "ok": True,
            "call_sid": call_sid,
            "duration_seconds": call_duration,
            "reason": reason,
            "final_state": session_state
        }
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("stop_call", "failure", latency_ms, trace)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace, "call_sid": call_sid})
        _structured_log(
            "call_stop_failed",
            level="error",
            message="Failed to stop call session",
            trace_id=trace,
            call_sid=call_sid,
            extra={"error": str(e), "traceback": traceback.format_exc()}
        )
        return {"ok": False, "error": str(e)}

# --------------------------------------------------------------------------
# Enhanced Metrics Recording Helper with Step Latency Tracking
# --------------------------------------------------------------------------
def _record_metrics(event_type: str, status: str, latency_ms: float = None, trace_id: str = None):
    """Record standardized metrics for voice pipeline operations with enhanced step tracking."""
    try:
        increment_metric, observe_latency, set_gauge = _get_metrics()
        # Counters (best-effort)
        try:
            increment_metric(f"voice_pipeline_{event_type}_{status}_total")
        except Exception:
            pass

        # Histogram / latency: convert ms -> seconds for Prometheus
        if latency_ms is not None:
            try:
                latency_s = float(latency_ms) / 1000.0
                observe_latency(f"voice_pipeline_{event_type}_latency_seconds", latency_s)
                # Add aggregate total latency view
                if event_type != "total":
                    observe_latency("voice_pipeline_total_latency_seconds", latency_s)
            except Exception:
                # fallback to legacy ms metric if needed
                try:
                    observe_latency(f"voice_pipeline_{event_type}_latency_ms", float(latency_ms))
                except Exception:
                    pass
                    
        # Enhanced: Record pipeline-specific metrics
        if event_type in ["asr_processing", "chunk_processing"] and status == "failure":
            try:
                increment_metric("voice_pipeline_asr_errors_total")
            except Exception:
                pass
        elif event_type == "inference_processing" and status == "failure":
            try:
                increment_metric("voice_pipeline_inference_errors_total")
            except Exception:
                pass
        elif event_type == "tts_processing" and status == "failure":
            try:
                increment_metric("voice_pipeline_tts_errors_total")
            except Exception:
                pass
                
    except Exception as e:
        log_event(
            service=SERVICE_NAME,
            event="metrics_record_failed",
            status="warn",
            message="Failed to record voice pipeline metrics",
            trace_id=trace_id or get_trace_id(),
            extra={"error": str(e), "schema_version": __schema_version__}
        )

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _resolve_trace_id(payload: Optional[Dict[str, Any]] = None, override: Optional[str] = None) -> str:
    if override:
        return override
    if payload and payload.get("trace_id"):
        return payload["trace_id"]
    return get_trace_id()

def _redis_key(call_sid: str) -> str:
    return f"call:{call_sid}"

def _partial_throttle_key(call_sid: str) -> str:
    return f"{PARTIAL_THROTTLE_PREFIX}{call_sid}"

def _structured_log(event: str, level: str = "info", message: Optional[str] = None,
                    trace_id: Optional[str] = None, call_sid: Optional[str] = None, **extra):
    log_event(service=SERVICE_NAME, event=event, status=level, message=message or event,
              trace_id=trace_id or get_trace_id(), 
              extra={**({"call_sid": call_sid} if call_sid else {}), **extra, "schema_version": __schema_version__})

def _get_r_client() -> Optional[Any]:
    """
    Return a fresh redis client for each call, safely handling exceptions.
    No global mutable state - consistent with Phase 12 stateless design.
    """
    try:
        rr = get_redis_client()
        if not rr:
            return None
            
        # Some redis clients lazily connect; try ping if possible
        if hasattr(rr, "ping"):
            rr.ping()
        # Record success metric
        try:
            increment_metric, _, _ = _get_metrics()
            increment_metric("voice_pipeline_redis_success_total")
        except Exception:
            pass
        return rr
    except Exception as e:
        # Record failure metric
        try:
            increment_metric, _, _ = _get_metrics()
            increment_metric("voice_pipeline_redis_failures_total")
        except Exception:
            pass
        _structured_log("redis_unavailable", level="warn",
                        message=f"Redis unavailable: {e}", trace_id=get_trace_id())
        return None

# --------------------------------------------------------------------------
# Session state helpers (merge semantics) with Enhanced TTL Enforcement
# --------------------------------------------------------------------------
def _store_call_state(call_sid: str, trace_id: str, state: Dict[str, Any]) -> None:
    rr = _get_r_client()
    if not rr:
        _structured_log("redis_unavailable_store", level="warn",
                        message="Redis unavailable during store", trace_id=trace_id, call_sid=call_sid)
        # record pipeline-level error metric
        try:
            increment_metric, _, _ = _get_metrics()
            increment_metric("voice_pipeline_errors_total")
        except Exception:
            pass
        return
    
    start_time = time.time()
    try:
        key = _redis_key(call_sid)
        raw = safe_redis_operation(lambda c: c.get(key), fallback=b"{}", operation_name="get_call_state")
        if raw is None:
            raw = b"{}"
        
        # Safe decoding
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except Exception:
                raw = "{}"
        
        try:
            existing = json.loads(raw)
        except Exception:
            existing = {}
        
        merged = {**existing, **state, "trace_id": trace_id, "schema_version": __schema_version__}
        # ENHANCED: Always set TTL to ensure session expiration
        safe_redis_operation(
            lambda c: c.set(key, json.dumps(merged), ex=CALL_STATE_TTL),
            fallback=None,
            operation_name="set_call_state"
        )
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("store_state", "success", latency_ms, trace_id)
        _structured_log("call_state_merged", message="Merged call state stored", trace_id=trace_id, call_sid=call_sid,
                       extra={"ttl_seconds": CALL_STATE_TTL})
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("store_state", "failure", latency_ms, trace_id)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        _structured_log("redis_set_failed", level="error", message=str(e),
                        trace_id=trace_id, call_sid=call_sid, extra={"traceback": traceback.format_exc()})

def _get_call_state(call_sid: str) -> Optional[Dict[str, Any]]:
    rr = _get_r_client()
    if not rr:
        _structured_log("redis_unavailable_get", level="warn",
                        message="Redis unavailable during get", trace_id=get_trace_id(), call_sid=call_sid)
        return None
    
    start_time = time.time()
    try:
        data = safe_redis_operation(
            lambda c: c.get(_redis_key(call_sid)), 
            fallback=None,
            operation_name="get_call_state"
        )
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("get_state", "success", latency_ms)
        
        if not data:
            return None
            
        # Safe decoding
        if isinstance(data, bytes):
            try:
                data = data.decode("utf-8")
            except Exception:
                return None
                
        try:
            return json.loads(data)
        except Exception:
            return None
            
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("get_state", "failure", latency_ms)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {"service": SERVICE_NAME, "call_sid": call_sid})
        _structured_log("redis_get_failed", level="warn", message=str(e),
                        trace_id=get_trace_id(), call_sid=call_sid, extra={"traceback": traceback.format_exc()})
        return None

def get_session_metadata(call_sid: str):
    return _get_call_state(call_sid)

def clear_session(call_sid: str):
    rr = _get_r_client()
    trace_id = get_trace_id()
    if not rr:
        _structured_log("redis_unavailable_clear", level="warn",
                        message="Redis unavailable during clear", trace_id=trace_id, call_sid=call_sid)
        return
    
    start_time = time.time()
    try:
        safe_redis_operation(
            lambda c: c.delete(_redis_key(call_sid)),
            fallback=None,
            operation_name="clear_session"
        )
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("clear_session", "success", latency_ms, trace_id)
        _structured_log("clear_session", message="Session cleared", trace_id=trace_id, call_sid=call_sid)
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("clear_session", "failure", latency_ms, trace_id)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        _structured_log("redis_delete_failed", level="warn", message=str(e),
                        trace_id=trace_id, call_sid=call_sid, extra={"traceback": traceback.format_exc()})

# --------------------------------------------------------------------------
# Partial throttle & fallback guard
# --------------------------------------------------------------------------
_throttle_lock = threading.RLock()  # Changed to RLock for reentrant safety

def _should_dispatch_partial(call_sid: str) -> bool:
    """
    Returns True if we SHOULD dispatch (i.e. not throttled), or if Redis is unavailable
    we allow dispatch but count fallback occurrences.
    """
    with _throttle_lock:
        # Circuit breaker check
        if _is_circuit_breaker_open("redis"):
            try:
                increment_metric, _, _ = _get_metrics()
                increment_metric("voice_pipeline_circuit_breaker_hits_total")
            except Exception:
                pass
            _structured_log("redis_circuit_breaker_open", level="warn",
                            message="Redis circuit breaker open - allowing dispatch", call_sid=call_sid, trace_id=get_trace_id())
            return True

        rr = _get_r_client()
        if not rr:
            # Redis unavailable - allow dispatch but record metric
            try:
                increment_metric, _, _ = _get_metrics()
                increment_metric("voice_pipeline_redis_failures_total")
            except Exception:
                pass
            _structured_log("partial_throttle_no_redis", level="warn",
                            message="Redis unavailable - allowing dispatch", call_sid=call_sid, trace_id=get_trace_id())
            return True

        start_time = time.time()
        try:
            # Use a quick NX set for throttle (atomic)
            key = _partial_throttle_key(call_sid)
            was_set = safe_redis_operation(
                lambda c: c.set(key, "1", nx=True, ex=int(PARTIAL_THROTTLE_SECONDS)),
                fallback=False,
                operation_name="partial_throttle_set"
            )
            
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("throttle_check", "success", latency_ms)
            
            if not was_set:
                # Throttled - increment metric here only (FIX: remove duplicate)
                try:
                    increment_metric, _, _ = _get_metrics()
                    increment_metric("voice_pipeline_partial_throttled_total")
                except Exception:
                    pass
                _structured_log("partial_throttled", message="Partial dispatch throttled", trace_id=get_trace_id(), call_sid=call_sid)
                return False
            return True
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("throttle_check", "failure", latency_ms)
            _, capture_exception_safe = _get_sentry()
            capture_exception_safe(e, {"service": SERVICE_NAME, "call_sid": call_sid})
            _structured_log("partial_throttle_error", level="warn",
                            message=str(e), trace_id=get_trace_id(), call_sid=call_sid,
                            extra={"traceback": traceback.format_exc()})
            return True

# --------------------------------------------------------------------------
# Twilio wrapper with retries & backoff
# --------------------------------------------------------------------------
def _twilio_with_retries(func: Callable, *args, max_attempts: int = 3, trace_id: Optional[str] = None, call_sid: Optional[str] = None, **kwargs):
    trace_id = trace_id or get_trace_id()
    last_exc = None
    start_time = time.time()
    
    for attempt in range(1, max_attempts + 1):
        try:
            result = func(*args, **kwargs)
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("twilio_operation", "success", latency_ms, trace_id)
            return result
        except Exception as e:
            last_exc = e
            _structured_log("twilio_call_failed", level="warn",
                            message=f"Twilio call failed (attempt {attempt}) - {e}",
                            trace_id=trace_id, call_sid=call_sid,
                            extra={"attempt": attempt, "traceback": traceback.format_exc()})
            # short exponential backoff
            time.sleep(0.2 * attempt)
    
    # All retries failed
    latency_ms = (time.time() - start_time) * 1000
    _record_metrics("twilio_operation", "failure", latency_ms, trace_id)
    try:
        increment_metric, _, _ = _get_metrics()
        increment_metric("voice_pipeline_errors_total")
    except Exception:
        pass
    
    # Capture final exception
    if last_exc:
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(last_exc, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        
    return None

# --------------------------------------------------------------------------
# Internal handlers for ASR results - ENHANCED FOR DUPLEX STREAMING
# --------------------------------------------------------------------------
def _handle_partial_result(call_sid: str, text: str, payload: dict, trace_id: str):
    # NEW: Increment partials processed counter at decision point
    try:
        increment_metric, _, _ = _get_metrics()
        increment_metric("voice_pipeline_partials_processed_total")
    except Exception:
        pass
    
    _store_call_state(call_sid, trace_id,
                      {"last_partial": text, "last_partial_ts": int(time.time())})
    _structured_log("partial_stored", message="Stored partial transcript", trace_id=trace_id, call_sid=call_sid, preview=text[:120])

    if not _should_dispatch_partial(call_sid):
        # FIX: Removed duplicate throttle metric increment - already handled in _should_dispatch_partial
        _structured_log("partial_throttled", message="Partial dispatch throttled", trace_id=trace_id, call_sid=call_sid)
        return

    start_time = time.time()
    try:
        # PHASE 11-F ADDITION: Use duplex controller if available and enabled
        if DUPLEX_STREAMING_ENABLED and DuplexVoiceController:
            try:
                # FIX: Use proper duplex controller access pattern
                controller = DuplexVoiceController()
                controller.handle_partial_transcript(call_sid, text, trace_id)
                _structured_log("partial_handled_duplex", message="Partial transcript handled via duplex controller", 
                              trace_id=trace_id, call_sid=call_sid)
                latency_ms = (time.time() - start_time) * 1000
                _record_metrics("partial_dispatch", "success", latency_ms, trace_id)
                return
            except Exception as e:
                _structured_log("duplex_partial_failed", level="warn",
                              message="Duplex controller failed, falling back to standard dispatch",
                              trace_id=trace_id, call_sid=call_sid, extra={"error": str(e)})
        
        # Fallback to standard dispatch
        if twilio_client and hasattr(twilio_client, "update_partial_transcript"):
            res = _twilio_with_retries(twilio_client.update_partial_transcript, call_sid, text,
                                       payload=payload, trace_id=trace_id)
            if res is None:
                _structured_log("partial_dispatch_twilio_failed", level="warn",
                                message="Twilio partial dispatch failed after retries", trace_id=trace_id, call_sid=call_sid)
            else:
                _structured_log("partial_dispatched_via_twilio_client", message="Partial dispatched via Twilio client", trace_id=trace_id, call_sid=call_sid, extra={"result": res})
        else:
            celery.send_task(
                EVENT_TASK_NAME,
                args=[{"type": "partial_transcript", "callSid": call_sid, "text": text}],
                kwargs={"trace_id": trace_id},
                queue=CELERY_VOICE_QUEUE
            )
            _structured_log("partial_event_sent_fallback", message="Partial event sent via Celery fallback", trace_id=trace_id, call_sid=call_sid)
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("partial_dispatch", "success", latency_ms, trace_id)
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("partial_dispatch", "failure", latency_ms, trace_id)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        _structured_log("partial_dispatch_failed", level="error", message=str(e), trace_id=trace_id, call_sid=call_sid,
                        extra={"traceback": traceback.format_exc()})

def _handle_final_result(call_sid: str, text: str, payload: dict, trace_id: str):
    start_time = time.time()
    try:
        process_final_transcript(call_sid, text, payload=payload, trace_id=trace_id, trigger_inference=True)
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("final_processing", "success", latency_ms, trace_id)
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("final_processing", "failure", latency_ms, trace_id)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        _structured_log("final_process_failed", level="error", message=str(e), trace_id=trace_id, call_sid=call_sid, extra={"traceback": traceback.format_exc()})

# --------------------------------------------------------------------------
# Audio chunk processing - ENHANCED FOR DUPLEX STREAMING WITH ASYNC SUPPORT
# --------------------------------------------------------------------------
def process_audio_chunk(call_sid: str, audio_chunk: bytes,
                        payload: Optional[Dict[str, Any]] = None,
                        trace_id: Optional[str] = None,
                        async_processing: bool = True) -> None:
    """
    Process audio chunk with optional async processing.
    Set async_processing=False for synchronous processing.
    """
    trace = _resolve_trace_id(payload, trace_id)
    
    # Circuit breaker check first - drop frames if circuit open
    if _is_circuit_breaker_open("voice_pipeline"):
        try:
            increment_metric, _, _ = _get_metrics()
            increment_metric("voice_pipeline_dropped_frames_total")
        except Exception:
            pass
        _structured_log("circuit_breaker_blocked", level="warn",
                        message="Voice pipeline request blocked by open circuit breaker", 
                        trace_id=trace, call_sid=call_sid)
        return

    # NEW: Only enqueue if NOT dispatching to async worker
    if not async_processing:
        if not _jitter_buffer.enqueue(audio_chunk, call_sid, trace):
            # Frame was dropped due to full buffer
            return
    else:
        # Async path: pass frame directly to worker, no local enqueue
        pass

    # metrics: count chunk
    try:
        increment_metric, _, _ = _get_metrics()
        increment_metric("voice_pipeline_chunks_total")
    except Exception:
        pass

    _structured_log("audio_chunk_received", message="Audio chunk received", trace_id=trace,
                  call_sid=call_sid, extra={"bytes": len(audio_chunk), "async": async_processing})

    _store_call_state(call_sid, trace, {"last_chunk_received_at": int(time.time())})

    # Async processing: dispatch to worker (no local processing)
    if async_processing:
        try:
            task_result = process_audio_async.delay(call_sid, audio_chunk, payload)
            _structured_log("audio_chunk_queued_async", message="Audio chunk queued for async processing", 
                          trace_id=trace, call_sid=call_sid, extra={"task_id": task_result.id})
            return
        except Exception as e:
            _structured_log("async_dispatch_failed", level="warn",
                          message="Async dispatch failed, falling back to sync processing",
                          trace_id=trace, call_sid=call_sid, extra={"error": str(e)})
            # Fall through to synchronous processing - now enqueue for sync processing
            if not _jitter_buffer.enqueue(audio_chunk, call_sid, trace):
                return

    # PHASE 11-F ADDITION: Route to duplex engine if available
    if DUPLEX_STREAMING_ENABLED and RealtimeVoiceEngine:
        try:
            # FIX: Use proper realtime engine access pattern
            engine = RealtimeVoiceEngine()
            if engine:
                # NEW: Use jitter buffer for duplex engine
                buffered_chunk = _jitter_buffer.dequeue()
                if buffered_chunk:
                    engine.process_inbound_audio(call_sid, buffered_chunk, trace_id)
                    _structured_log("audio_routed_duplex", message="Audio chunk routed to duplex engine", 
                                  trace_id=trace, call_sid=call_sid)
                return
        except Exception as e:
            _structured_log("duplex_audio_failed", level="warn",
                          message="Duplex engine failed, falling back to standard processing",
                          trace_id=trace, call_sid=call_sid, extra={"error": str(e)})

    # Fallback to standard ASR processing with jitter buffer
    if not process_audio_buffer:
        _structured_log("asr_missing", level="error", message="ASR backend unavailable", 
                        trace_id=trace, call_sid=call_sid)
        try:
            increment_metric, _, _ = _get_metrics()
            increment_metric("voice_pipeline_asr_failures_total")
        except Exception:
            pass
        # FIX: Hard return - don't fall through
        return

    start_time = time.time()
    try:
        # NEW: Use jitter buffer for ASR processing
        buffered_chunk = _jitter_buffer.dequeue()
        if not buffered_chunk:
            return

        # Safe ASR result unpacking
        try:
            consumed, result = process_audio_buffer(call_sid, bytearray(buffered_chunk))
        except (ValueError, TypeError) as e:
            try:
                increment_metric, _, _ = _get_metrics()
                increment_metric("voice_pipeline_asr_invalid_output_total")
            except Exception:
                pass
            _structured_log("asr_invalid_output", level="error", 
                          message="ASR returned invalid output format", trace_id=trace, call_sid=call_sid,
                          extra={"error": str(e), "traceback": traceback.format_exc()})
            return
            
        duration = time.time() - start_time
        latency_ms = duration * 1000
        
        try:
            _, observe_latency, _ = _get_metrics()
            observe_latency("voice_pipeline_asr_latency_seconds", duration)
        except Exception:
            pass

        if consumed:
            _structured_log("asr_consumed", message="ASR consumed bytes", trace_id=trace, call_sid=call_sid, extra={"consumed": consumed})

        if not result:
            _structured_log("asr_no_result", message="No transcript for chunk", trace_id=trace, call_sid=call_sid)
            return

        rtype = result.get("type")
        text = result.get("text", "")
        if rtype == "partial":
            _handle_partial_result(call_sid, text, payload or {}, trace)
        elif rtype == "final":
            _handle_final_result(call_sid, text, payload or {}, trace)
        else:
            _structured_log("asr_unknown_type", level="warn", message="ASR returned unknown type", trace_id=trace, call_sid=call_sid, extra={"result": result})
            
        _record_metrics("chunk_processing", "success", latency_ms, trace)
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("chunk_processing", "failure", latency_ms, trace)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace, "call_sid": call_sid})
        _structured_log("asr_processing_failed", level="error", message=str(e), trace_id=trace, call_sid=call_sid, extra={"traceback": traceback.format_exc()})

# --------------------------------------------------------------------------
# Final transcript handling - ENHANCED FOR DUPLEX STREAMING WITH CIRCUIT BREAKER
# --------------------------------------------------------------------------
def process_final_transcript(call_sid: str, final_text: str,
                             payload: Optional[Dict[str, Any]] = None,
                             trace_id: Optional[str] = None,
                             trigger_inference: bool = True) -> Dict[str, Any]:
    resolved = _resolve_trace_id(payload, trace_id)
    start_time = time.time()
    
    try:
        _store_call_state(call_sid, resolved,
                          {"last_transcript": final_text,
                           "last_transcript_ts": int(time.time()),
                           "status": "transcribed"})
        _structured_log("final_stored", message="Final transcript stored", trace_id=resolved, call_sid=call_sid, preview=final_text[:150])

        dispatch_success = False
        
        # PHASE 11-F ADDITION: Use duplex controller for final transcript if available
        if DUPLEX_STREAMING_ENABLED and DuplexVoiceController:
            try:
                # FIX: Use proper duplex controller access pattern
                controller = DuplexVoiceController()
                controller.handle_final_transcript(call_sid, final_text, trace_id)
                _structured_log("final_handled_duplex", message="Final transcript handled via duplex controller", 
                              trace_id=resolved, call_sid=call_sid)
                dispatch_success = True
                info = {"ok": True, "via": "duplex_controller"}
            except Exception as e:
                _structured_log("duplex_final_failed", level="warn",
                              message="Duplex controller failed, falling back to standard dispatch",
                              trace_id=resolved, call_sid=call_sid, extra={"error": str(e)})

        if not dispatch_success:
            # Fallback to standard dispatch
            if twilio_client and hasattr(twilio_client, "update_final_transcript"):
                res = _twilio_with_retries(twilio_client.update_final_transcript, call_sid, final_text,
                                           payload=payload, trace_id=resolved)
                if res is None:
                    _structured_log("final_dispatched_twilio_failed", level="warn",
                                    message="Twilio final dispatch failed after retries", trace_id=resolved, call_sid=call_sid)
                    # fallback to celery event
                else:
                    _structured_log("final_dispatched_via_twilio_client", message="Final dispatched via Twilio client",
                                    trace_id=resolved, call_sid=call_sid, extra={"result": res})
                    dispatch_success = True
                    info = {"ok": True, "via": "twilio_client"}

            if not dispatch_success:
                # fallback or no twilio_client
                celery.send_task(
                    EVENT_TASK_NAME,
                    args=[{"type": "final_transcript", "callSid": call_sid, "text": final_text}],
                    kwargs={"trace_id": resolved},
                    queue=CELERY_VOICE_QUEUE
                )
                info = {"ok": True, "via": "celery_event"}
                _structured_log("final_event_sent_fallback", message="Final event sent via Celery fallback", trace_id=resolved, call_sid=call_sid)

        if trigger_inference:
            # Check inference circuit breaker before dispatching
            if _check_inference_circuit_breaker():
                _structured_log(
                    "inference_circuit_blocked_final",
                    level="warn",
                    message="Inference blocked by circuit breaker for final transcript",
                    trace_id=resolved,
                    call_sid=call_sid
                )
                info["inference"] = {"ok": False, "error": "Inference service unavailable"}
            else:
                dispatch_info = _dispatch_inference_final(final_text, call_sid, resolved)
                info["inference"] = dispatch_info
            
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("final_processing", "success", latency_ms, resolved)
        return info
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("final_processing", "failure", latency_ms, resolved)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": resolved, "call_sid": call_sid})
        _structured_log("final_store_failed", level="error", message=str(e), trace_id=resolved, call_sid=call_sid, extra={"traceback": traceback.format_exc()})
        return {"ok": False, "error": str(e)}

def _dispatch_inference_final(final_text: str, call_sid: str, trace_id: str) -> Dict[str, Any]:
    start_time = time.time()
    try:
        if twilio_client and hasattr(twilio_client, "_dispatch_inference_task"):
            res = _twilio_with_retries(twilio_client._dispatch_inference_task, final_text, call_sid, trace_id=trace_id)
            if res is None:
                _structured_log("inference_dispatch_twilio_failed", level="warn", message="Twilio inference dispatch failed", trace_id=trace_id, call_sid=call_sid)
                return {"ok": False, "error": "twilio_dispatch_failed"}
            _structured_log("inference_dispatched_via_twilio_client", message="Inference dispatched via Twilio client", trace_id=trace_id, call_sid=call_sid)
            try:
                increment_metric, _, _ = _get_metrics()
                increment_metric("voice_pipeline_inference_dispatched_total")
            except Exception:
                pass
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("inference_dispatch", "success", latency_ms, trace_id)
            return {"ok": True, "meta": res}
        else:
            # Use enhanced async inference task
            task = process_inference_async.delay(call_sid, final_text, {"trace_id": trace_id})
            try:
                increment_metric, _, _ = _get_metrics()
                increment_metric("voice_pipeline_inference_dispatched_total")
            except Exception:
                pass
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("inference_dispatch", "success", latency_ms, trace_id)
            return {"ok": True, "task_id": getattr(task, "id", None)}
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("inference_dispatch", "failure", latency_ms, trace_id)
        _, capture_exception_safe = _get_sentry()
        capture_exception_safe(e, {"service": SERVICE_NAME, "trace_id": trace_id, "call_sid": call_sid})
        _structured_log("inference_dispatch_error", level="error", message=str(e), trace_id=trace_id, call_sid=call_sid, extra={"traceback": traceback.format_exc()})
        return {"ok": False, "error": str(e)}

# --------------------------------------------------------------------------
# Test hooks (preserved) — instrumented to emit metrics
# --------------------------------------------------------------------------
def _test_simulate_chunk_flow():
    try:
        increment_metric, _, _ = _get_metrics()
        increment_metric("voice_pipeline_test_runs_total")
    except Exception:
        pass
        
    test_call = "SIM-TEST-CALL-123"
    trace = get_trace_id()
    _structured_log("test_start", message="Simulating chunk flow", trace_id=trace, call_sid=test_call)
    # Fake ASR stub output
    global process_audio_buffer

    def fake_asr(call_sid, buf):
        if len(buf) < 4000:
            return len(buf), {"type": "partial", "text": f"partial-{len(buf)}"}
        return len(buf), {"type": "final", "text": "This is the final transcript."}

    process_audio_buffer = fake_asr
    for size in [1000, 1200, 1500, 5000]:
        try:
            process_audio_chunk(test_call, b"x" * size, trace_id=trace)
        except Exception as e:
            _structured_log("test_chunk_error", level="error", message=str(e), trace_id=trace, call_sid=test_call, extra={"traceback": traceback.format_exc()})
        time.sleep(0.2)
    _structured_log("test_done", message="Simulation complete", trace_id=trace, call_sid=test_call)

def _test_simulate_final_flow():
    try:
        increment_metric, _, _ = _get_metrics()
        increment_metric("voice_pipeline_test_runs_total")
    except Exception:
        pass
        
    call = "SIM-FINAL-456"
    trace = get_trace_id()
    res = process_final_transcript(call, "Simulated final transcript.", trace_id=trace, trigger_inference=False)
    _structured_log("test_final_result", message="Final flow result", trace_id=trace, call_sid=call, extra={"result": res})

__all__ = [
    "process_audio_chunk", "process_final_transcript",
    "_store_call_state", "_get_call_state", "get_session_metadata",
    "clear_session", "_test_simulate_chunk_flow", "_test_simulate_final_flow",
    "initialize_voice_pipeline", "DUPLEX_STREAMING_ENABLED",
    # NEW: Session management and async processing
    "start_call", "stop_call", "process_audio_async", "process_tts_async", "process_inference_async",
    # NEW: Barge-in cancellation and jitter buffer
    "cancel_pending_tts_tasks", "_jitter_buffer"
]

# --------------------------------------------------------------------------
# Health check endpoint (if this module runs as a service) - ENHANCED
# --------------------------------------------------------------------------
if __name__ == "__main__":
    from flask import Flask, jsonify
    
    # FIX: Signal handlers moved here to avoid Celery conflicts
    def _graceful_shutdown(signum, frame):
        """Phase 12: Graceful shutdown handler - only for standalone Flask runner"""
        log_event(
            service="voice_pipeline",
            event="shutdown_signal_received",
            status="info",
            message=f"Received signal {signum}, shutting down gracefully...",
            trace_id=get_trace_id()
        )
        sys.exit(0)

    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    
    # Initialize explicitly when run as main
    duplex_controller = initialize_voice_pipeline()
    
    app = Flask(__name__)
    
    @app.route("/health", methods=["GET"])
    def health_check():
        """Health check endpoint for voice pipeline service."""
        trace_id = get_trace_id()
        start_time = time.time()
        
        try:
            # Record health check metric
            try:
                increment_metric, _, _ = _get_metrics()
                increment_metric("voice_pipeline_health_checks_total")
            except Exception:
                pass
            
            # Redis health
            redis_ok = safe_redis_operation(
                lambda c: c.ping(),
                fallback=False,
                operation_name="health_check_ping"
            )
            
            # Circuit breaker states
            pipeline_breaker_open = _is_circuit_breaker_open("voice_pipeline")
            redis_breaker_open = _is_circuit_breaker_open("redis")
            
            # Celery broker check
            try:
                insp = celery.control.inspect(timeout=1)
                insp_result = insp.ping()
                celery_ok = bool(insp_result)
            except Exception:
                celery_ok = False

            # PHASE 11-F ADDITION: Duplex controller health
            duplex_healthy = False
            if DUPLEX_STREAMING_ENABLED and duplex_controller:
                try:
                    duplex_healthy = duplex_controller.is_healthy()
                except Exception:
                    duplex_healthy = False

            overall_status = "healthy" if all([redis_ok, celery_ok]) and not any([pipeline_breaker_open, redis_breaker_open]) else "degraded"

            _structured_log(
                "health_check", 
                level="info" if overall_status == "healthy" else "warn",
                message="Health check completed", 
                trace_id=trace_id,
                extra={
                    "redis_ok": redis_ok,
                    "celery_ok": celery_ok,
                    "duplex_healthy": duplex_healthy,
                    "pipeline_breaker_open": pipeline_breaker_open,
                    "redis_breaker_open": redis_breaker_open
                }
            )

            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("health_check", "success", latency_ms, trace_id)
            
            return jsonify({
                "service": "voice_pipeline",
                "status": overall_status,
                "trace_id": trace_id,
                "schema_version": __schema_version__,
                "duplex_enabled": DUPLEX_STREAMING_ENABLED,
                "components": {
                    "redis": {
                        "status": "healthy" if redis_ok else "unhealthy",
                        "circuit_breaker": "open" if redis_breaker_open else "closed"
                    },
                    "celery_broker": {
                        "status": "healthy" if celery_ok else "unhealthy"
                    },
                    "duplex_controller": {
                        "status": "healthy" if duplex_healthy else "unhealthy",
                        "enabled": DUPLEX_STREAMING_ENABLED
                    },
                    "circuit_breakers": {
                        "voice_pipeline": "open" if pipeline_breaker_open else "closed",
                        "redis": "open" if redis_breaker_open else "closed"
                    }
                }
            })

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("health_check", "failure", latency_ms, trace_id)
            _, capture_exception_safe = _get_sentry()
            capture_exception_safe(e, {"service": "voice_pipeline", "health_check": "main"})
            return jsonify({
                "service": "voice_pipeline", 
                "status": "unhealthy",
                "trace_id": trace_id,
                "schema_version": __schema_version__,
                "error": str(e)
            }), 500

    # Startup logging
    log_event(
        service=SERVICE_NAME,
        event="startup",
        status="info", 
        message="Voice pipeline service starting with Phase 12 standardization and duplex streaming",
        extra={"phase": __phase__, "schema_version": __schema_version__, "duplex_enabled": DUPLEX_STREAMING_ENABLED}
    )
    
    port = Config.VOICE_PIPELINE_PORT
    app.run(host="0.0.0.0", port=port)