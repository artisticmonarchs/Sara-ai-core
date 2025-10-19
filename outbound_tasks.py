"""
outbound_tasks.py
Provides a Celery-registered task 'outbound_call_task' that wraps existing call pipeline logic.
This file is intentionally defensive: it tries to find existing helpers in tasks.py and calls them.
If you prefer to fold this into tasks.py directly, copy the outbound_call_task implementation into that file.
"""
import time
import logging
import json

logger = logging.getLogger("outbound_tasks")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

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
    logger.warning("Celery app not found in celery_app module. Ensure workers import outbound_tasks to register the task.")

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
        import tasks as tasks_module  # local project tasks
        import meeting_persist
        import uuid

        contact = payload.get("contact", {})
        campaign = payload.get("campaign", "default")
        session_id = payload.get("meta", {}).get("session_id", str(uuid.uuid4()))

        logger.info("Starting outbound_call_task for %s (campaign=%s)", contact.get("phone"), campaign)

        # Try several candidate pipeline entrypoints
        candidate_names = [
            "perform_outbound_call",
            "run_call_pipeline",
            "perform_tts_core",
            "outbound_call_handler"
        ]

        result = None
        for name in candidate_names:
            fn = getattr(tasks_module, name, None)
            if callable(fn):
                try:
                    result = fn(contact=contact, session_id=session_id, campaign=campaign)
                    logger.info("Pipeline callable %s executed for %s", name, contact.get("phone"))
                    break
                except TypeError:
                    # maybe signature differs: try passing only contact
                    try:
                        result = fn(contact)
                        logger.info("Pipeline callable %s executed with fallback signature for %s", name, contact.get("phone"))
                        break
                    except Exception as e:
                        logger.exception("Candidate %s failed: %s", name, e)
                except Exception as e:
                    logger.exception("Candidate %s failed: %s", name, e)

        # If the pipeline returned booking info, persist
        if isinstance(result, dict) and result.get("booking_confirmed"):
            booking = result.get("booking", {})
            try:
                persist_res = meeting_persist.persist_booking(session_id, contact, booking)
                logger.info("Persist booking result: %s", persist_res)
            except Exception as e:
                logger.exception("Failed to persist booking: %s", e)

        # Return result for introspection
        return result
else:
    # Provide a no-op fallback so imports don't fail in dev environments
    def outbound_call_task(payload):
        logger.info("Celery not configured. Would enqueue payload: %s", json.dumps(payload))
        return {"status": "dryrun"}
