"""
campaign_runner.py — Sara AI Core (Phase 11-F)
CLI tool to run an outbound campaign from a CSV file with Duplex Streaming support.
SEQUENTIAL VERSION - Runs calls one by one, no parallel processing.

Usage:
    python campaign_runner.py --csv leads.csv --campaign my_campaign --daily-cap 300
"""
import argparse
import time
from datetime import datetime, timedelta
import sys
import uuid
import signal
import os

def _graceful_shutdown(signum, frame):
    """Phase 12: Graceful shutdown handler"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, _graceful_shutdown)
signal.signal(signal.SIGTERM, _graceful_shutdown)


# Phase 11-F Compliance Metadata
__phase__ = "11-F"
__service__ = "campaign_runner"
__schema_version__ = "phase_11f_v1"

# Phase 11-F: Centralized configuration ONLY
try:
    from config import Config
except ImportError:
    # Minimal fallback that doesn't violate isolation
    import os
    class Config:
        REDIS_URL = os.getenv("REDIS_URL")
        SERVICE_NAME = "campaign_runner"

# Phase 11-F: Structured logging with proper observability integration
def _get_logger():
    try:
        from logging_utils import log_event, get_trace_id
        return log_event, get_trace_id
    except Exception:
        def _noop_log(*a, **k): pass
        def _fallback_trace_id(): return str(uuid.uuid4())[:8]
        return _noop_log, _fallback_trace_id

log_event, get_trace_id = _get_logger()

# Phase 11-F: Metrics with unified registry integration
def _get_metrics():
    try:
        from metrics_collector import increment_metric, observe_latency
        return increment_metric, observe_latency
    except ImportError:
        def _noop_metric(*args, **kwargs): pass
        def _noop_latency(*args, **kwargs): pass
        return _noop_metric, _noop_latency

# Phase 11-F: Duplex streaming integration
def _get_duplex_modules():
    try:
        from duplex_voice_controller import DuplexVoiceController
        from realtime_voice_engine import RealtimeVoiceEngine
        return DuplexVoiceController, RealtimeVoiceEngine
    except ImportError:
        return None, None

DuplexVoiceController, RealtimeVoiceEngine = _get_duplex_modules()
DUPLEX_AVAILABLE = DuplexVoiceController is not None and RealtimeVoiceEngine is not None

# Phase 11-F: Structured logging wrapper
def _structured_log(event: str, level: str = "info", message: str = None, trace_id: str = None, **extra):
    """Structured logging wrapper with Phase 11-F schema"""
    log_event(
        service=__service__,
        event=event,
        status=level,
        message=message or event,
        trace_id=trace_id or get_trace_id(),
        extra={**extra, "schema_version": __schema_version__, "phase": __phase__}
    )

# Phase 11-F: Metrics recording helper
def _record_metrics(event_type: str, status: str, latency_ms: float = None, trace_id: str = None):
    """Record metrics for campaign operations"""
    try:
        increment_metric, observe_latency = _get_metrics()
        increment_metric(f"campaign_runner_{event_type}_{status}_total")
        if latency_ms is not None:
            observe_latency(f"campaign_runner_{event_type}_latency_seconds", latency_ms / 1000.0)
    except Exception:
        pass

# Phase 11-F: Circuit breaker check
def _is_circuit_breaker_open(service: str = "campaign_runner") -> bool:
    """Check if circuit breaker is open for campaign operations"""
    try:
        from redis_client import get_client
        with get_client() as r:
            state = r.get(f"circuit_breaker:{service}:state")
            if not state:
                return False
            if isinstance(state, bytes):
                state = state.decode("utf-8")
            return state.lower() == "open"
    except Exception:
        # Emit metric for Redis failure
        try:
            from metrics_collector import increment_metric
            increment_metric("redis.circuit.breaker.failure")
            increment_metric("redis.call.error")
        except Exception:
            pass
        return False

from contacts_loader import read_csv, validate_row, normalize_phone

# Phase 11-F: Enhanced sequential call execution with duplex support
def execute_call_directly(contact_row, campaign_name: str, duplex_enabled: bool = None):
    """Execute call directly instead of enqueueing to Celery with duplex streaming support"""
    trace_id = get_trace_id()
    
    # Phase 11-F: Determine duplex mode
    if duplex_enabled is None:
        duplex_enabled = DUPLEX_AVAILABLE
    
    try:
        # Import the actual call execution function
        try:
            from outbound_tasks import execute_outbound_call
            call_function = execute_outbound_call
        except ImportError:
            # Fallback to the task function if direct function doesn't exist
            from outbound_tasks import outbound_call_task
            # Use apply() to run synchronously instead of delay() for async
            def sync_call_wrapper(contact, campaign):
                payload = {
                    "contact": contact,
                    "campaign": campaign,
                    "duplex_enabled": duplex_enabled,
                    "meta": {
                        "session_id": str(uuid.uuid4()),
                        "trace_id": trace_id
                    }
                }
                return outbound_call_task.apply(args=[payload]).get()
            call_function = sync_call_wrapper
        
        _structured_log("call_starting", level="info",
                       message=f"Starting sequential call to {contact_row.get('phone', 'unknown')}",
                       campaign=campaign_name, phone=contact_row.get('phone'), 
                       duplex_enabled=duplex_enabled, trace_id=trace_id)
        
        start_time = time.time()
        
        # Execute call synchronously - this will block until call completes
        result = call_function(contact_row, campaign_name)
        
        call_duration = time.time() - start_time
        
        _structured_log("call_completed", level="info",
                       message=f"Call completed in {call_duration:.2f}s",
                       campaign=campaign_name, phone=contact_row.get('phone'), 
                       duration_seconds=call_duration, duplex_enabled=duplex_enabled,
                       trace_id=trace_id)
        
        return result
        
    except Exception as e:
        _structured_log("call_execution_failed", level="error",
                       message=f"Call execution failed: {e}",
                       campaign=campaign_name, phone=contact_row.get('phone'),
                       duplex_enabled=duplex_enabled, error=str(e), trace_id=trace_id)
        raise

def campaign_daily_key(campaign_name: str, date: datetime):
    return f"campaign:{campaign_name}:count:{date.strftime('%Y%m%d')}"

def get_daily_count(campaign_name: str):
    trace_id = get_trace_id()
    
    try:
        from redis_client import get_client
    except ImportError:
        _structured_log("redis_client_unavailable", level="error",
                       message="Redis client module not available",
                       campaign=campaign_name, trace_id=trace_id)
        return 0
    
    try:
        with get_client() as r:
            key = campaign_daily_key(campaign_name, datetime.utcnow())
            val = r.get(key)
            count = int(val) if val else 0
            
            _structured_log("daily_count_retrieved", level="info",
                           message=f"Retrieved daily count for campaign {campaign_name}",
                           campaign=campaign_name, daily_count=count, trace_id=trace_id)
            
            return count
    except Exception as e:
        # Emit metric for Redis failure
        try:
            from metrics_collector import increment_metric
            increment_metric("redis.call.error")
        except Exception:
            pass
        
        _structured_log("daily_count_retrieval_failed", level="error",
                       message=f"Failed to retrieve daily count: {e}",
                       campaign=campaign_name, error=str(e), trace_id=trace_id)
        return 0

def incr_daily_count(campaign_name: str, amount: int = 1, expiry_hours: int = 48):
    trace_id = get_trace_id()
    start_time = time.time()
    
    try:
        from redis_client import get_client
    except ImportError:
        _structured_log("redis_client_unavailable", level="error",
                       message="Redis client module not available",
                       campaign=campaign_name, trace_id=trace_id)
        return None
    
    try:
        with get_client() as r:
            key = campaign_daily_key(campaign_name, datetime.utcnow())
            pipe = r.pipeline()
            pipe.incrby(key, amount)
            pipe.expire(key, expiry_hours * 3600)
            result = pipe.execute()
            new_count = result[0] if result else None
            
            latency_ms = (time.time() - start_time) * 1000
            _record_metrics("redis_operation", "success", latency_ms, trace_id)
            
            if new_count is not None:
                _structured_log("daily_count_incremented", level="info",
                               message=f"Incremented daily count for {campaign_name} by {amount}",
                               campaign=campaign_name, amount=amount, new_count=new_count, 
                               trace_id=trace_id, latency_ms=latency_ms)
            else:
                _structured_log("daily_count_increment_failed", level="error",
                               message=f"Failed to increment daily count for {campaign_name}",
                               campaign=campaign_name, amount=amount, trace_id=trace_id)
            
            return new_count
    except Exception as e:
        # Emit metric for Redis failure
        try:
            from metrics_collector import increment_metric
            increment_metric("redis.call.error")
        except Exception:
            pass
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("redis_operation", "failure", latency_ms, trace_id)
        
        _structured_log("daily_count_increment_exception", level="error",
                       message=f"Exception incrementing daily count: {e}",
                       campaign=campaign_name, amount=amount, error=str(e), trace_id=trace_id)
        return None

# Phase 11-F: Enhanced campaign runner with duplex support
def run_campaign(csv_path: str, campaign_name: str, daily_cap: int = 300, dry_run: bool = False, duplex_enabled: bool = None):
    trace_id = get_trace_id()
    campaign_start_time = time.time()
    
    # Phase 11-F: Determine duplex mode
    if duplex_enabled is None:
        duplex_enabled = DUPLEX_AVAILABLE
    
    # Phase 11-F: Circuit breaker check
    if _is_circuit_breaker_open("campaign_runner"):
        _structured_log("circuit_breaker_blocked", level="warning",
                       message="Campaign execution blocked by circuit breaker",
                       trace_id=trace_id, campaign=campaign_name)
        _record_metrics("campaign", "blocked", None, trace_id)
        return
    
    _record_metrics("campaign", "started", None, trace_id)
    _structured_log("campaign_start", level="info", 
                   message=f"Starting SEQUENTIAL campaign {campaign_name} from {csv_path}", 
                   campaign=campaign_name, csv_path=csv_path, 
                   daily_cap=daily_cap, dry_run=dry_run, duplex_enabled=duplex_enabled,
                   trace_id=trace_id)
    
    current_count = get_daily_count(campaign_name)
    
    contacts_processed = 0
    contacts_called = 0
    contacts_failed = 0

    for row in read_csv(csv_path):
        contacts_processed += 1
        contact_start_time = time.time()
        
        if dry_run:
            _structured_log("dry_run_row", level="debug", 
                           message=f"DRY RUN: Would call {row.get('phone', 'unknown')}",
                           campaign=campaign_name, trace_id=trace_id)
            continue
            
        if not validate_row(row):
            _record_metrics("contact_validation", "failed", None, trace_id)
            _structured_log("contact_validation_failed", level="warning",
                           message=f"Invalid contact row skipped", campaign=campaign_name, 
                           row_data=str(row), trace_id=trace_id)
            continue
            
        if current_count >= daily_cap:
            _record_metrics("daily_cap", "reached", None, trace_id)
            _structured_log("daily_cap_reached", level="warning", 
                           message=f"Daily cap reached ({daily_cap}). Stopping.",
                           campaign=campaign_name, daily_cap=daily_cap, 
                           current_count=current_count, trace_id=trace_id)
            break
            
        try:
            # Phase 11-F: Execute call with duplex support
            execute_call_directly(row, campaign_name, duplex_enabled)
            current_count += 1
            contacts_called += 1
            
            # Record successful call
            contact_latency = (time.time() - contact_start_time) * 1000
            _record_metrics("contact_call", "success", contact_latency, trace_id)
            
            # Update Redis count
            incr_daily_count(campaign_name, 1)
            
        except Exception as e:
            contacts_failed += 1
            _record_metrics("contact_call", "failed", None, trace_id)
            _structured_log("call_failed", level="error", 
                           message=f"Call failed for contact: {e}", 
                           campaign=campaign_name, error=str(e), trace_id=trace_id)
        
        # NOTE: No sleep interval - calls run sequentially, one after another
    
    # Campaign completion metrics
    campaign_duration = (time.time() - campaign_start_time) * 1000
    _record_metrics("campaign", "completed", campaign_duration, trace_id)
    
    _structured_log("campaign_finished", level="info", 
                   message=f"SEQUENTIAL campaign finished. Processed {contacts_processed}, called {contacts_called}, failed {contacts_failed}",
                   campaign=campaign_name, contacts_processed=contacts_processed,
                   contacts_called=contacts_called, contacts_failed=contacts_failed,
                   final_count=current_count, duration_ms=campaign_duration, 
                   duplex_enabled=duplex_enabled, trace_id=trace_id)

# Phase 11-F: Health check function
def campaign_runner_health_check():
    """Health check for campaign runner service"""
    trace_id = get_trace_id()
    start_time = time.time()
    
    try:
        # Check circuit breaker state
        circuit_breaker_open = _is_circuit_breaker_open("campaign_runner")
        
        # Check Redis connectivity
        try:
            from redis_client import get_client
            redis_ok = False
            with get_client() as r:
                redis_ok = r.ping()
        except Exception:
            redis_ok = False
        
        # Check duplex availability
        duplex_available = DUPLEX_AVAILABLE
        
        status = "healthy" if not circuit_breaker_open and redis_ok else "degraded"
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "success", latency_ms, trace_id)
        
        _structured_log("health_check", level="info",
                       message="Campaign runner health check completed",
                       trace_id=trace_id, status=status, circuit_breaker_open=circuit_breaker_open,
                       redis_ok=redis_ok, duplex_available=duplex_available)
        
        return {
            "service": __service__,
            "status": status,
            "phase": __phase__,
            "schema_version": __schema_version__,
            "trace_id": trace_id,
            "components": {
                "circuit_breaker": "open" if circuit_breaker_open else "closed",
                "redis": "healthy" if redis_ok else "unhealthy",
                "duplex_available": duplex_available
            }
        }
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        _record_metrics("health_check", "failed", latency_ms, trace_id)
        _structured_log("health_check_failed", level="error",
                       message="Campaign runner health check failed",
                       trace_id=trace_id, error=str(e))
        return {
            "service": __service__,
            "status": "unhealthy",
            "error": str(e),
            "phase": __phase__,
            "schema_version": __schema_version__,
            "trace_id": trace_id
        }

def main():
    trace_id = get_trace_id()
    
    p = argparse.ArgumentParser(description="Run SEQUENTIAL outbound campaign from CSV - One call at a time with Phase 11-F duplex streaming")
    p.add_argument("--csv", required=True, help="Path to CSV file with contacts")
    p.add_argument("--campaign", default="default", help="Campaign name for tracking")
    p.add_argument("--daily-cap", type=int, default=300, help="Maximum daily calls")
    p.add_argument("--dry-run", action="store_true", help="Simulate without making calls")
    p.add_argument("--duplex-enabled", action="store_true", help="Enable duplex streaming (auto-detected if not specified)")
    p.add_argument("--no-duplex", action="store_true", help="Disable duplex streaming")
    
    args = p.parse_args()

    # Phase 11-F: Determine duplex mode
    duplex_enabled = None
    if args.duplex_enabled:
        duplex_enabled = True
    elif args.no_duplex:
        duplex_enabled = False
    
    try:
        run_campaign(args.csv, args.campaign, 
                    daily_cap=args.daily_cap, dry_run=args.dry_run,
                    duplex_enabled=duplex_enabled)
        
        _record_metrics("campaign_run", "success", None, trace_id)
        
    except Exception as e:
        _record_metrics("campaign_run", "failed", None, trace_id)
        
        _structured_log("main_execution_failed", level="error",
                       message=f"Campaign execution failed: {e}", error=str(e), trace_id=trace_id)
        sys.exit(1)

if __name__ == "__main__":
    main()

# Phase 11-F: Exports
__all__ = [
    "run_campaign",
    "campaign_runner_health_check",
    "get_daily_count",
    "incr_daily_count",
    "__phase__",
    "__service__",
    "__schema_version__"
]