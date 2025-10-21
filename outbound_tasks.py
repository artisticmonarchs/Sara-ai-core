"""
outbound_tasks.py
Provides a Celery-registered task 'outbound_call_task' that wraps existing call pipeline logic.
This file is intentionally defensive: it tries to find existing helpers in tasks.py and calls them.
If you prefer to fold this into tasks.py directly, copy the outbound_call_task implementation into that file.
"""
import time
import json
import uuid

# Configuration isolation with lazy loading
def _get_config():
    try:
        from config import Config
        return Config
    except Exception:
        class FallbackConfig:
            SERVICE_NAME = "outbound_tasks"
        return FallbackConfig

Config = _get_config()

# Structured logging with lazy shim
def _get_logger():
    try:
        from logging_utils import log_event
        return log_event
    except Exception:
        def _noop_log(*a, **k): pass
        return _noop_log

log_event = _get_logger()

# Trace ID with fallback
def _get_trace_id():
    try:
        from logging_utils import get_trace_id
        return get_trace_id()
    except Exception:
        return str(uuid.uuid4())[:8]

# Metrics with lazy loading
def _get_metrics():
    try:
        from metrics_collector import increment_metric, observe_latency
        return increment_metric, observe_latency
    except ImportError:
        def _noop_metric(*args, **kwargs): pass
        def _noop_latency(*args, **kwargs): pass
        return _noop_metric, _noop_latency

# Try to get the celery app instance from celery_app
def _get_celery():
    try:
        import celery_app as _ca
        # common names used in repos: celery_app.celery or celery_app.app or celery_app.celery_app
        for name in ("celery", "celery_app", "app"):
            if hasattr(_ca, name):
                return getattr(_ca, name)
    except Exception:
        pass
    # Last resort: try to import Celery and create a local app (not recommended)
    return None

_celery = _get_celery()

if _celery is None:
    log_event("outbound_tasks", "celery_app_not_found", "warning",
             "Celery app not found in celery_app module. Ensure workers import outbound_tasks to register the task.",
             trace_id=_get_trace_id())

# Define the task decorator only if celery instance is available
if _celery is not None:
    @_celery.task(name="sara.outbound_call_task", bind=True, acks_late=True)
    def outbound_call_task(self, payload):
        """
        payload = {
            "contact": {name, phone, industry, website},
            "campaign": "<campaign-name>",
            "meta": {... optional ...}
        }
        The task will attempt to find pipeline functions in the existing tasks.py module:
           - perform_outbound_call(payload)
           - run_call_pipeline(contact, session_id)
           - perform_tts_core(...)
        and will call the first found function. It will also attempt to persist a booking using meeting_persist.persist_booking()
        when the called function returns a dict containing {"booking_confirmed": True, "booking": {...}}
        """
        trace_id = _get_trace_id()
        start_time = time.time()
        increment_metric, observe_latency = _get_metrics()
        
        contact = payload.get("contact", {})
        campaign = payload.get("campaign", "default")
        session_id = payload.get("meta", {}).get("session_id", str(uuid.uuid4()))
        phone = contact.get("phone", "unknown")

        log_event("outbound_tasks", "task_started", "info",
                 "Starting outbound_call_task",
                 contact_phone=phone, campaign=campaign, session_id=session_id, trace_id=trace_id)

        increment_metric("outbound_tasks.started")

        # Try several candidate pipeline entrypoints
        candidate_names = [
            "perform_outbound_call",
            "run_call_pipeline",
            "perform_tts_core",
            "outbound_call_handler"
        ]

        result = None
        function_used = None
        
        for name in candidate_names:
            try:
                import tasks as tasks_module
                fn = getattr(tasks_module, name, None)
                if callable(fn):
                    try:
                        result = fn(contact=contact, session_id=session_id, campaign=campaign)
                        function_used = name
                        log_event("outbound_tasks", "pipeline_executed", "info",
                                 "Pipeline callable executed successfully",
                                 function_name=name, contact_phone=phone, trace_id=trace_id)
                        break
                    except TypeError:
                        # maybe signature differs: try passing only contact
                        try:
                            result = fn(contact)
                            function_used = name
                            log_event("outbound_tasks", "pipeline_executed_fallback", "info",
                                     "Pipeline callable executed with fallback signature",
                                     function_name=name, contact_phone=phone, trace_id=trace_id)
                            break
                        except Exception as e:
                            log_event("outbound_tasks", "pipeline_execution_failed", "error",
                                     "Candidate function failed with fallback signature",
                                     function_name=name, contact_phone=phone, error=str(e), trace_id=trace_id)
                            increment_metric("outbound_tasks.pipeline_failed")
                    except Exception as e:
                        log_event("outbound_tasks", "pipeline_execution_failed", "error",
                                 "Candidate function failed",
                                 function_name=name, contact_phone=phone, error=str(e), trace_id=trace_id)
                        increment_metric("outbound_tasks.pipeline_failed")
            except Exception as e:
                log_event("outbound_tasks", "module_import_failed", "error",
                         "Failed to import tasks module",
                         function_name=name, contact_phone=phone, error=str(e), trace_id=trace_id)

        # If the pipeline returned booking info, persist
        if isinstance(result, dict) and result.get("booking_confirmed"):
            booking = result.get("booking", {})
            try:
                import meeting_persist
                persist_res = meeting_persist.persist_booking(session_id, contact, booking)
                log_event("outbound_tasks", "booking_persisted", "info",
                         "Booking persisted successfully",
                         session_id=session_id, contact_phone=phone, persist_result=persist_res, trace_id=trace_id)
                increment_metric("outbound_tasks.booking_persisted")
            except Exception as e:
                log_event("outbound_tasks", "booking_persist_failed", "error",
                         "Failed to persist booking",
                         session_id=session_id, contact_phone=phone, error=str(e), trace_id=trace_id)
                increment_metric("outbound_tasks.booking_persist_failed")

        # Record metrics and completion
        latency_ms = (time.time() - start_time) * 1000
        observe_latency("outbound_tasks.execution_latency", latency_ms)
        increment_metric("outbound_tasks.completed")
        
        log_event("outbound_tasks", "task_completed", "info",
                 "Outbound call task completed",
                 contact_phone=phone, function_used=function_used, 
                 latency_ms=latency_ms, trace_id=trace_id)

        # Return result for introspection
        return result
else:
    # Provide a no-op fallback so imports don't fail in dev environments
    def outbound_call_task(payload):
        trace_id = _get_trace_id()
        log_event("outbound_tasks", "task_dry_run", "info",
                 "Celery not configured - would enqueue payload",
                 payload_summary=json.dumps(payload)[:200], trace_id=trace_id)
        return {"status": "dryrun"}