"""
app.py — Sara AI Core API
Phase 11-D compliant version
Decoupled from Celery internals, unified observability, and R2 centralized client.
"""

import time
import traceback
from flask import Flask, request, jsonify, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import signal
import sys
import logging
import asyncio

# Minimal process logger (prevents NameError in signal handler)
logger = logging.getLogger("sara.app")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

def _graceful_shutdown(signum, frame):
    """Phase 12: Graceful shutdown handler"""
    try:
        logger.info(f"Received signal {signum}, shutting down gracefully...")
    except Exception:
        # Fallback: never let signal handler crash
        sys.stderr.write(f"[shutdown] signal={signum}\n")
    sys.exit(0)

signal.signal(signal.SIGINT, _graceful_shutdown)
signal.signal(signal.SIGTERM, _graceful_shutdown)


# --------------------------------------------------------------------------
# Phase 11-D Configuration Isolation
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    # Fallback config for backward compatibility
    import os
    class Config:
        PORT = int(os.getenv("PORT", "5000"))
        # TODO: Move hardcoded port number to config.py
        R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

# --------------------------------------------------------------------------
# Phase 11-D Metrics Integration - Lazy Import Shim
# --------------------------------------------------------------------------
def _get_metrics():
    """Lazy metrics shim to avoid circular imports at import-time"""
    try:
        import metrics_collector as metrics
        return metrics
    except Exception:
        # safe no-op fallbacks
        class NoopMetrics:
            def inc_metric(self, *a, **k): pass
            def increment_metric(self, *a, **k): pass
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
# Internal imports with safe fallbacks
# --------------------------------------------------------------------------
from celery_app import celery
from logging_utils import log_event, get_trace_id
from redis_client import get_client
from r2_client import get_r2_client, check_r2_connection as r2_check  # type: ignore

# Safe import of utils functions with comprehensive fallbacks
try:
    from utils import increment_metric, restore_metrics_snapshot
except ImportError:
    # Fallback if utils module doesn't exist or symbols are missing
    def increment_metric(*_args, **_kwargs):
        pass
    def restore_metrics_snapshot(*_args, **_kwargs) -> int:
        return 0
except Exception:
    # Additional safety for any other import issues
    def increment_metric(*_args, **_kwargs):
        pass
    def restore_metrics_snapshot(*_args, **_kwargs) -> int:
        return 0


# --------------------------------------------------------------------------
# Lazy Initialization Functions (Phase 11-D - No side effects at import time)
# --------------------------------------------------------------------------
def _initialize_metrics_sync():
    """Lazy metrics sync startup - called on first request"""
    try:
        from global_metrics_store import start_background_sync  # type: ignore
        start_background_sync(service_name="app")
        log_event(service="app", event="global_metrics_sync_started", status="ok",
                  message="Background global metrics sync started for app service")
    except Exception as e:
        log_event(service="app", event="global_metrics_sync_failed", status="error",
                  message="Failed to start background metrics sync",
                  extra={"error": str(e), "stack": traceback.format_exc()})
        
def _initialize_metrics_restore():
    """Lazy metrics restore - called on first request"""
    try:
        from metrics_registry import restore_snapshot_to_collector  # type: ignore
        try:
            restored_ok = restore_snapshot_to_collector(get_metrics())
            if restored_ok:
                log_event(service="app", event="metrics_restored_on_startup", status="info",
                          message="Restored persisted metrics into metrics_collector at app startup")
        except Exception as e:
            log_event(service="app", event="metrics_restore_failed", status="warn",
                      message="Failed to restore metrics snapshot at app startup",
                      extra={"error": str(e), "stack": traceback.format_exc()})
    except Exception as e:
        log_event(service="app", event="metrics_restore_import_failed", status="warn",
                  message="Could not import metrics restore module",
                  extra={"error": str(e)})

def _ensure_initialized():
    """Ensure metrics are initialized on first use"""
    # These will only run once due to module-level caching
    _initialize_metrics_sync()
    _initialize_metrics_restore()

# --------------------------------------------------------------------------
# Phase 11-D: Startup System Validation
# --------------------------------------------------------------------------
def validate_system_dependencies():
    """Validate all critical system dependencies at startup."""
    trace_id = "system_startup"
    components = {}
    
    # Redis validation
    try:
        redis_client = get_client()
        redis_info = redis_client.ping()
        components["redis"] = {"status": "ok", "details": "Connection successful"}
        log_event(service="app", event="redis_validation_ok", status="ok", 
                  message="Redis connection validated at startup", trace_id=trace_id)
    except Exception as e:
        components["redis"] = {"status": "error", "details": str(e)}
        log_event(service="app", event="redis_validation_failed", status="error",
                  message="Redis connection failed at startup", trace_id=trace_id,
                  extra={"error": str(e)})
    
    # R2 validation
    try:
        r2_client = get_r2_client()
        if r2_client:
            components["r2"] = {"status": "ok", "details": "Client initialized"}
            log_event(service="app", event="r2_validation_ok", status="ok",
                      message="R2 client validated at startup", trace_id=trace_id)
        else:
            components["r2"] = {"status": "error", "details": "Client initialization failed"}
            log_event(service="app", event="r2_validation_failed", status="error",
                      message="R2 client initialization failed", trace_id=trace_id)
    except Exception as e:
        components["r2"] = {"status": "error", "details": str(e)}
        log_event(service="app", event="r2_validation_failed", status="error",
                  message="R2 validation failed at startup", trace_id=trace_id,
                  extra={"error": str(e)})
    
    # Metrics system validation
    try:
        # Test metrics collection
        get_metrics().inc_metric("system_startup_validation_total")
        components["metrics"] = {"status": "ok", "details": "Collector operational"}
        log_event(service="app", event="metrics_validation_ok", status="ok",
                  message="Metrics system validated at startup", trace_id=trace_id)
    except Exception as e:
        components["metrics"] = {"status": "error", "details": str(e)}
        log_event(service="app", event="metrics_validation_failed", status="error",
                  message="Metrics system validation failed", trace_id=trace_id,
                  extra={"error": str(e)})
    
    # Celery validation
    try:
        # Simple Celery inspect to check worker connectivity
        insp = celery.control.inspect()
        active_workers = insp.active() or {}
        components["celery"] = {"status": "ok", "details": f"{len(active_workers)} workers active"}
        log_event(service="app", event="celery_validation_ok", status="ok",
                  message="Celery workers validated at startup", trace_id=trace_id,
                  extra={"active_workers": len(active_workers)})
    except Exception as e:
        components["celery"] = {"status": "error", "details": str(e)}
        log_event(service="app", event="celery_validation_failed", status="error",
                  message="Celery validation failed at startup", trace_id=trace_id,
                  extra={"error": str(e)})
    
    return components

# --------------------------------------------------------------------------
# App setup
# --------------------------------------------------------------------------
app = Flask(__name__)

# Phase 11-D: Initialize Redis client with error handling
try:
    r = get_client()
    log_event(service="app", event="redis_client_initialized", status="ok",
              message="Redis client initialized successfully")
except Exception as e:
    r = None
    log_event(service="app", event="redis_client_failed", status="error",
              message="Failed to initialize Redis client",
              extra={"error": str(e), "stack": traceback.format_exc()})

# --------------------------------------------------------------------------
# Twilio Integration (Phase 11-D)
# --------------------------------------------------------------------------
try:
    from twilio_router import twilio_router_bp  # unified router blueprint
    app.register_blueprint(twilio_router_bp)
    log_event(
        service="app",
        event="twilio_router_registered",
        status="ok",
        message="Twilio router blueprint registered successfully"
    )
except Exception as e:
    log_event(
        service="app",
        event="twilio_router_registration_failed",
        status="error",
        message="Failed to register Twilio router blueprint",
        extra={"error": str(e), "stack": traceback.format_exc()}
    )

# Phase 11-D: Startup components will be validated lazily at process start.
startup_components = {}

# --------------------------------------------------------------------------
# Enhanced Health check endpoints
# --------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    trace_id = get_trace_id()
    try:
        get_metrics().inc_metric("api_healthz_requests_total")
        
        # Enhanced health check with component status
        health_status = "ok"
        status_code = 200
        failing_components = []
        
        # Check critical components
        if not r:
            health_status = "degraded"
            failing_components.append("redis")
        
        try:
            r2_client = get_r2_client()
            if not r2_client:
                health_status = "degraded"
                failing_components.append("r2")
        except Exception:
            health_status = "degraded"
            failing_components.append("r2")
        
        response_data = {
            "status": health_status, 
            "trace_id": trace_id,
            "service": "Sara AI Core API"
        }
        
        if failing_components:
            response_data["failing_components"] = failing_components
            status_code = 503 if health_status == "degraded" else 500
        
        log_event(service="app", event="healthz_checked", status=health_status, 
                  message=f"Health check completed: {health_status}", trace_id=trace_id,
                  extra={"failing_components": failing_components})
        
        return jsonify(response_data), status_code
    except Exception as e:
        get_metrics().inc_metric("api_healthz_failures_total")
        log_event(service="app", event="healthz_error", status="error", 
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500


# Alias for infra that probes /health instead of /healthz
@app.route("/health", methods=["GET"])
def health():
    return healthz()


@app.route("/system_status", methods=["GET"])
def system_status():
    trace_id = get_trace_id()
    try:
        get_metrics().inc_metric("api_system_status_requests_total")
        
        info = {
            "service": "Sara AI Core", 
            "status": "running",
            "components": startup_components,
            "phase": "11-D"
        }
        
        log_event(service="app", event="system_status_ok", status="ok", 
                  trace_id=trace_id, extra={"component_count": len(startup_components)})
        return jsonify({"status": "ok", "info": info, "trace_id": trace_id})
    except Exception as e:
        get_metrics().inc_metric("api_system_status_failures_total")
        log_event(service="app", event="system_status_error", status="error",
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500


# --------------------------------------------------------------------------
# R2 connection check (use centralized r2_client)
# --------------------------------------------------------------------------
def check_r2_connection(trace_id=None, session_id=None):
    start = time.time()
    try:
        try:
            client = get_r2_client()
            if callable(r2_check):
                ok, meta = r2_check(client=client, bucket=Config.R2_BUCKET_NAME)
                latency_ms = round((time.time() - start) * 1000, 2)
                # TODO: Move hardcoded port number to config.py
                if ok:
                    get_metrics().inc_metric("r2_health_checks_ok_total")
                    log_event(
                        service="app",
                        event="r2_status_ok",
                        status="ok",
                        message=f"Bucket {meta.get('bucket')} accessible",
                        trace_id=trace_id,
                        session_id=session_id,
                        extra={"r2_latency_ms": latency_ms, "bucket": meta.get("bucket")},
                    )
                    return {"status": "ok", "bucket": meta.get("bucket"), "r2_latency_ms": latency_ms}
                else:
                    get_metrics().inc_metric("r2_health_checks_failed_total")
                    log_event(
                        service="app",
                        event="r2_status_error",
                        status="error",
                        message=meta.get("error") or "r2_check returned False",
                        trace_id=trace_id,
                        session_id=session_id,
                    )
                    return {"status": "error", "message": meta.get("error")}
            else:
                bucket = Config.R2_BUCKET_NAME
                client.list_objects_v2(Bucket=bucket, MaxKeys=1)
                latency_ms = round((time.time() - start) * 1000, 2)
                # TODO: Move hardcoded port number to config.py
                get_metrics().inc_metric("r2_health_checks_ok_total")
                log_event(
                    service="app",
                    event="r2_status_ok",
                    status="ok",
                    message=f"Bucket {bucket} accessible",
                    trace_id=trace_id,
                    session_id=session_id,
                    extra={"r2_latency_ms": latency_ms, "bucket": bucket},
                )
                return {"status": "ok", "bucket": bucket, "r2_latency_ms": latency_ms}
        except Exception as inner_e:
            get_metrics().inc_metric("r2_health_checks_failed_total")
            log_event(
                service="app",
                event="r2_status_error",
                status="error",
                message=str(inner_e),
                trace_id=trace_id,
                session_id=session_id,
            )
            return {"status": "error", "message": str(inner_e)}
    except Exception as e:
        get_metrics().inc_metric("r2_health_checks_failed_total")
        log_event(
            service="app",
            event="r2_status_exception",
            status="error",
            message=str(e),
            trace_id=trace_id,
            session_id=session_id,
        )
        return {"status": "error", "message": str(e)}


@app.route("/r2_status", methods=["GET"])
def r2_status():
    trace_id = get_trace_id()
    get_metrics().inc_metric("api_r2_status_requests_total")
    result = check_r2_connection(trace_id=trace_id)
    return jsonify({**result, "trace_id": trace_id}), (200 if result.get("status") == "ok" else 500)


# --------------------------------------------------------------------------
# Metrics snapshot restore and persistence
# --------------------------------------------------------------------------
@app.route("/metrics_snapshot", methods=["GET", "POST"])  # CHANGED: Added GET method
def metrics_snapshot():
    trace_id = get_trace_id()
    try:
        get_metrics().inc_metric("api_metrics_snapshot_requests_total")
        
        # For GET requests, return current snapshot without restoring
        if request.method == "GET":
            try:
                snapshot = get_metrics().get_snapshot()
                log_event(service="app", event="metrics_snapshot_retrieved", status="ok",
                          message="Metrics snapshot retrieved via GET", trace_id=trace_id)
                return jsonify({"status": "ok", "snapshot": snapshot, "trace_id": trace_id})
            except Exception as e:
                log_event(service="app", event="metrics_snapshot_retrieve_failed", status="error",
                          message="Failed to retrieve metrics snapshot", trace_id=trace_id,
                          extra={"error": str(e)})
                return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500
        
        # For POST requests, restore and persist (original behavior)
        restore_metrics_snapshot()
        log_event(service="app", event="metrics_snapshot_restored", status="ok",
                  message="Metrics snapshot restored via API", trace_id=trace_id)
    except Exception as e:
        get_metrics().inc_metric("api_metrics_snapshot_failures_total")
        log_event(
            service="app",
            event="metrics_restore_fail",
            status="error",
            message=str(e),
            trace_id=trace_id,
        )

    # Persist using helper that expects metrics_collector.get_snapshot (callable)
    try:
        from metrics_collector import push_snapshot_from_collector
        pushed = False
        try:
            pushed = push_snapshot_from_collector(get_metrics().get_snapshot)
        except TypeError:
            try:
                pushed = push_snapshot_from_collector(get_metrics().get_snapshot())
            except Exception as e:
                raise

        if not pushed:
            log_event(
                service="app",
                event="push_snapshot_warn",
                status="warn",
                message="push_snapshot_from_collector returned False (not saved).",
                trace_id=trace_id,
            )
    except Exception as e:
        log_event(
            service="app",
            event="push_snapshot_failed",
            status="error",
            message="Failed to push snapshot from collector to Redis",
            trace_id=trace_id,
            extra={"error": str(e), "stack": traceback.format_exc()},
        )

    return jsonify({"status": "ok", "trace_id": trace_id})


# --------------------------------------------------------------------------
# /metrics endpoint — unified legacy + REGISTRY output
# --------------------------------------------------------------------------
@app.route("/metrics", methods=["GET"])
def metrics_endpoint():
    """Expose aggregated metrics in Prometheus plain-text format (legacy + REGISTRY)."""
    try:
        get_metrics().inc_metric("api_metrics_requests_total")

        parts = []
        try:
            from metrics_collector import export_prometheus
            legacy = export_prometheus()
            if legacy:
                parts.append(legacy if isinstance(legacy, str) else legacy.decode("utf-8"))
        except Exception as e:
            log_event(
                service="app",
                event="export_prometheus_failed",
                status="warn",
                message="export_prometheus() failed",
                extra={"error": str(e)},
            )

        try:
            from metrics_registry import REGISTRY
            reg_text = generate_latest(REGISTRY).decode("utf-8")
            parts.append(reg_text)
        except Exception as e:
            log_event(
                service="app",
                event="generate_latest_failed",
                status="warn",
                message="Failed to generate REGISTRY output",
                extra={"error": str(e)},
            )

        combined = "\n".join(p for p in parts if p)
        if not combined:
            get_metrics().inc_metric("api_metrics_failures_total")
            return Response("# metrics_export_error 1\n", mimetype="text/plain"), 500

        return Response(combined, mimetype=CONTENT_TYPE_LATEST), 200
    except Exception as e:
        get_metrics().inc_metric("api_metrics_failures_total")
        log_event(
            service="app",
            event="metrics_export_error",
            status="error",
            message="Failed to export metrics",
            extra={"error": str(e), "stack": traceback.format_exc()},
        )
        return Response("# metrics_export_error 1\n", mimetype="text/plain"), 500


# --------------------------------------------------------------------------
# Text-to-Speech async trigger
# --------------------------------------------------------------------------
@app.route("/tts_test", methods=["POST"])
def tts_test():
    trace_id = get_trace_id()
    try:
        get_metrics().inc_metric("api_tts_test_requests_total")
        data = request.get_json(force=True)
        session_id = data.get("session_id")
        text = data.get("text", "")
        
        # Use the proxy pattern to enqueue the TTS task
        from tasks import run_tts  # local import to avoid import-time side effects
        run_tts.delay({"session_id": session_id, "text": text, "trace_id": trace_id})
        
        get_metrics().inc_metric("api_tts_test_dispatched_total")
        log_event(service="app", event="tts_task_dispatched", status="ok", 
                  trace_id=trace_id, extra={"session_id": session_id})
        return jsonify({"status": "queued", "trace_id": trace_id})
    except Exception as e:
        get_metrics().inc_metric("api_tts_test_failures_total")
        log_event(service="app", event="tts_dispatch_failed", status="error", 
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "trace_id": trace_id, "message": str(e)}), 500


# --------------------------------------------------------------------------
# Phase 11-D: Enhanced readiness endpoint
# --------------------------------------------------------------------------
@app.route("/ready", methods=["GET"])
def readiness_probe():
    """Kubernetes-style readiness probe with component checks."""
    trace_id = get_trace_id()
    try:
        get_metrics().inc_metric("api_ready_requests_total")
        
        checks = {}
        all_healthy = True
        
        # Redis check
        try:
            if r and r.ping():
                checks["redis"] = "healthy"
            else:
                checks["redis"] = "unhealthy"
                all_healthy = False
        except Exception:
            checks["redis"] = "unhealthy"
            all_healthy = False
        
        # R2 check
        try:
            r2_client = get_r2_client()
            if r2_client:
                checks["r2"] = "healthy"
            else:
                checks["r2"] = "unhealthy"
                all_healthy = False
        except Exception:
            checks["r2"] = "unhealthy"
            all_healthy = False
        
        # Celery check
        try:
            insp = celery.control.inspect()
            active = insp.active() or {}
            if active:
                checks["celery"] = "healthy"
            else:
                checks["celery"] = "degraded"  # No active workers but service may still function
        except Exception:
            checks["celery"] = "unhealthy"
            all_healthy = False
        
        status_code = 200 if all_healthy else 503
        status = "ready" if all_healthy else "not_ready"
        
        log_event(service="app", event="readiness_checked", status=status,
                  message=f"Readiness check: {status}", trace_id=trace_id,
                  extra={"checks": checks})
        
        return jsonify({
            "status": status,
            "checks": checks,
            "trace_id": trace_id
        }), status_code
        
    except Exception as e:
        get_metrics().inc_metric("api_ready_failures_total")
        log_event(service="app", event="readiness_error", status="error",
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500


# --------------------------------------------------------------------------
# Phase 11-D: Outbound Call and Duplex Streaming Placeholders
# --------------------------------------------------------------------------
@app.route("/outbound_call", methods=["POST"])
def outbound_call():
    """Placeholder for outbound call functionality (lazy-loads outbound_dialer)."""
    trace_id = get_trace_id()
    try:
        get_metrics().inc_metric("api_outbound_call_requests_total")

        # Lazy import so missing outbound_dialer does not break import-time.
        try:
            from outbound_dialer import handle_outbound_call  # type: ignore
        except ImportError as e:
            # Import failure is now treated as an error since we expect the handler to exist
            get_metrics().inc_metric("api_outbound_call_import_failures_total")
            log_event(service="app", event="outbound_call_import_error", status="error",
                      message="Outbound dialer handler import failed - module not found", 
                      trace_id=trace_id, extra={"error": str(e), "stack": traceback.format_exc()})
            return jsonify({"status": "error", "message": "Outbound call handler not available", "trace_id": trace_id}), 500
        except Exception as e:
            # Other import-related errors
            get_metrics().inc_metric("api_outbound_call_import_failures_total")
            log_event(service="app", event="outbound_call_import_error", status="error",
                      message="Outbound dialer handler import failed", 
                      trace_id=trace_id, extra={"error": str(e), "stack": traceback.format_exc()})
            return jsonify({"status": "error", "message": "Outbound call handler import failed", "trace_id": trace_id}), 500

        if callable(handle_outbound_call):
            # delegate to the real implementation if present
            try:
                # Run the async function in an event loop
                result = asyncio.run(handle_outbound_call(request))
                log_event(service="app", event="outbound_call_dispatched", status="ok",
                          message="Outbound call delegated to outbound_dialer", trace_id=trace_id)
                return jsonify({"status": "ok", "result": result, "trace_id": trace_id})
            except Exception as e:
                # log and fallthrough to stub response only for runtime execution errors
                get_metrics().inc_metric("api_outbound_call_runtime_failures_total")
                log_event(service="app", event="outbound_call_handler_error", status="error",
                          message="Outbound call handler runtime error", 
                          trace_id=trace_id, extra={"error": str(e), "stack": traceback.format_exc()})
                # Fall back to stub for runtime errors only
                log_event(service="app", event="outbound_call_stub", status="ok",
                          message="Outbound call stub endpoint called due to handler error", trace_id=trace_id)
                return jsonify({"status": "ok", "message": "stub", "trace_id": trace_id})
        else:
            # This should not happen if import succeeded, but handle defensively
            get_metrics().inc_metric("api_outbound_call_handler_invalid_total")
            log_event(service="app", event="outbound_call_handler_invalid", status="error",
                      message="Imported outbound call handler is not callable", trace_id=trace_id)
            return jsonify({"status": "error", "message": "Outbound call handler invalid", "trace_id": trace_id}), 500

    except Exception as e:
        get_metrics().inc_metric("api_outbound_call_failures_total")
        log_event(service="app", event="outbound_call_stub_error", status="error",
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500

@app.route("/duplex_stream/start", methods=["POST"])
def duplex_stream_start():
    """Placeholder for duplex stream start functionality (lazy-loads duplex_voice_controller)."""
    trace_id = get_trace_id()
    try:
        get_metrics().inc_metric("api_duplex_stream_start_requests_total")

        try:
            from duplex_voice_controller import start_session  # type: ignore
        except Exception:
            start_session = None

        if callable(start_session):
            try:
                result = start_session(request)
                log_event(service="app", event="duplex_stream_start_delegated", status="ok",
                          message="duplex start delegated", trace_id=trace_id)
                return jsonify({"status": "ok", "result": result, "trace_id": trace_id})
            except Exception as e:
                log_event(service="app", event="duplex_start_handler_error", status="error",
                          message=str(e), trace_id=trace_id, extra={"stack": traceback.format_exc()})

        log_event(service="app", event="duplex_stream_start_stub", status="ok",
                  message="Duplex stream start stub endpoint called", trace_id=trace_id)
        return jsonify({"status": "ok", "message": "stub", "trace_id": trace_id})
    except Exception as e:
        get_metrics().inc_metric("api_duplex_stream_start_failures_total")
        log_event(service="app", event="duplex_stream_start_stub_error", status="error",
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500

@app.route("/duplex_stream/chunk", methods=["POST"])
def duplex_stream_chunk():
    """Placeholder for duplex stream chunk functionality (lazy-loads duplex_voice_controller)."""
    trace_id = get_trace_id()
    try:
        get_metrics().inc_metric("api_duplex_stream_chunk_requests_total")

        try:
            from duplex_voice_controller import handle_chunk  # type: ignore
        except Exception:
            handle_chunk = None

        if callable(handle_chunk):
            try:
                result = handle_chunk(request)
                log_event(service="app", event="duplex_stream_chunk_delegated", status="ok",
                          message="duplex chunk delegated", trace_id=trace_id)
                return jsonify({"status": "ok", "result": result, "trace_id": trace_id})
            except Exception as e:
                log_event(service="app", event="duplex_chunk_handler_error", status="error",
                          message=str(e), trace_id=trace_id, extra={"stack": traceback.format_exc()})

        log_event(service="app", event="duplex_stream_chunk_stub", status="ok",
                  message="Duplex stream chunk stub endpoint called", trace_id=trace_id)
        return jsonify({"status": "ok", "message": "stub", "trace_id": trace_id})
    except Exception as e:
        get_metrics().inc_metric("api_duplex_stream_chunk_failures_total")
        log_event(service="app", event="duplex_stream_chunk_stub_error", status="error",
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500

@app.route("/duplex_stream/end", methods=["POST"])
def duplex_stream_end():
    """Placeholder for duplex stream end functionality (lazy-loads duplex_voice_controller)."""
    trace_id = get_trace_id()
    try:
        get_metrics().inc_metric("api_duplex_stream_end_requests_total")

        try:
            from duplex_voice_controller import end_session  # type: ignore
        except Exception:
            end_session = None

        if callable(end_session):
            try:
                result = end_session(request)
                log_event(service="app", event="duplex_stream_end_delegated", status="ok",
                          message="duplex end delegated", trace_id=trace_id)
                return jsonify({"status": "ok", "result": result, "trace_id": trace_id})
            except Exception as e:
                log_event(service="app", event="duplex_end_handler_error", status="error",
                          message=str(e), trace_id=trace_id, extra={"stack": traceback.format_exc()})

        log_event(service="app", event="duplex_stream_end_stub", status="ok",
                  message="Duplex stream end stub endpoint called", trace_id=trace_id)
        return jsonify({"status": "ok", "message": "stub", "trace_id": trace_id})
    except Exception as e:
        get_metrics().inc_metric("api_duplex_stream_end_failures_total")
        log_event(service="app", event="duplex_stream_end_stub_error", status="error",
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500

# --------------------------------------------------------------------------
# Conversation Metrics Snapshot Endpoint
# --------------------------------------------------------------------------
@app.route("/conversation_metrics_snapshot", methods=["GET"])
def conversation_metrics_snapshot():
    """Expose conversation metrics snapshot"""
    trace_id = get_trace_id()
    try:
        get_metrics().inc_metric("api_conversation_metrics_snapshot_requests_total")
        # Use existing metrics snapshot functionality
        from metrics_collector import push_snapshot_from_collector
        try:
            pushed = push_snapshot_from_collector(get_metrics().get_snapshot)
        except TypeError:
            pushed = push_snapshot_from_collector(get_metrics().get_snapshot())
        
        if pushed:
            log_event(service="app", event="conversation_metrics_snapshot_pushed", status="ok",
                      message="Conversation metrics snapshot pushed", trace_id=trace_id)
        else:
            log_event(service="app", event="conversation_metrics_snapshot_failed", status="warn",
                      message="Failed to push conversation metrics snapshot", trace_id=trace_id)
        
        return jsonify({"status": "ok", "message": "conversation metrics snapshot", "trace_id": trace_id})
    except Exception as e:
        get_metrics().inc_metric("api_conversation_metrics_snapshot_failures_total")
        log_event(service="app", event="conversation_metrics_snapshot_error", status="error",
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500


# --------------------------------------------------------------------------
# Main entrypoint
# --------------------------------------------------------------------------
if __name__ == "__main__":
    # Initialize metrics on startup (lazy init)
    _ensure_initialized()

    # Perform one-time system validation at process start (keeps import-time side-effects out).
    try:
        startup_components = validate_system_dependencies()
    except Exception:
        startup_components = {}

    log_event(
        service="app",
        event="application_start",
        status="ok",
        message="Sara AI Core API starting",
        extra={
            "phase": "11-D",
            "validated_components": len(startup_components)
        }
    )

    # Use dedicated config values with safe fallbacks
    host = getattr(Config, "HOST", "0.0.0.0")
    port = getattr(Config, "FLASK_PORT", getattr(Config, "PORT", 5000))
    # TODO: Move hardcoded port number to config.py

    app.run(
        host=host,
        port=port,
        debug=False,
        threaded=True
    )