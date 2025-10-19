"""
app.py — Sara AI Core API
Phase 11-D compliant version
Decoupled from Celery internals, unified observability, and R2 centralized client.
"""

import os
import time
import traceback
from flask import Flask, request, jsonify, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

# --------------------------------------------------------------------------
# Internal imports
# --------------------------------------------------------------------------
from celery_app import celery
from logging_utils import log_event, get_trace_id
from metrics_collector import (
    export_prometheus,
    get_snapshot,
    push_snapshot_from_collector,
)
from metrics_registry import REGISTRY
from redis_client import get_client
from r2_client import get_r2_client, check_r2_connection as r2_check  # type: ignore
from utils import increment_metric, restore_metrics_snapshot

# --------------------------------------------------------------------------
# Phase 11-D: Global Metrics Sync Startup
# --------------------------------------------------------------------------
try:
    from global_metrics_store import start_background_sync  # type: ignore
    start_background_sync(service_name="app")
    log_event(service="app", event="global_metrics_sync_started", status="ok",
              message="Background global metrics sync started for app service")
except Exception as e:
    log_event(service="app", event="global_metrics_sync_failed", status="error",
              message="Failed to start background metrics sync",
              extra={"error": str(e), "stack": traceback.format_exc()})

# --------------------------------------------------------------------------
# Phase 11-D: Metrics Integration
# --------------------------------------------------------------------------
import metrics_collector as metrics  # type: ignore

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
        metrics.inc_metric("system_startup_validation_total")
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
# Phase 11-D: Metrics Snapshot Restore on Startup
# --------------------------------------------------------------------------
try:
    from metrics_registry import restore_snapshot_to_collector  # type: ignore
    try:
        restored_ok = restore_snapshot_to_collector(metrics)
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

# Phase 11-D: Run system validation at startup
startup_components = validate_system_dependencies()

# --------------------------------------------------------------------------
# Enhanced Health check endpoints
# --------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    trace_id = get_trace_id(request)
    try:
        metrics.inc_metric("api_healthz_requests_total")
        
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
        metrics.inc_metric("api_healthz_failures_total")
        log_event(service="app", event="healthz_error", status="error", 
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500


@app.route("/system_status", methods=["GET"])
def system_status():
    trace_id = get_trace_id(request)
    try:
        metrics.inc_metric("api_system_status_requests_total")
        
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
        metrics.inc_metric("api_system_status_failures_total")
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
                ok, meta = r2_check(client=client, bucket=os.getenv("R2_BUCKET_NAME"))
                latency_ms = round((time.time() - start) * 1000, 2)
                if ok:
                    metrics.inc_metric("r2_health_checks_ok_total")
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
                    metrics.inc_metric("r2_health_checks_failed_total")
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
                bucket = os.getenv("R2_BUCKET_NAME")
                client.list_objects_v2(Bucket=bucket, MaxKeys=1)
                latency_ms = round((time.time() - start) * 1000, 2)
                metrics.inc_metric("r2_health_checks_ok_total")
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
            metrics.inc_metric("r2_health_checks_failed_total")
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
        metrics.inc_metric("r2_health_checks_failed_total")
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
    trace_id = get_trace_id(request)
    metrics.inc_metric("api_r2_status_requests_total")
    result = check_r2_connection(trace_id=trace_id)
    return jsonify({**result, "trace_id": trace_id}), (200 if result.get("status") == "ok" else 500)


# --------------------------------------------------------------------------
# Metrics snapshot restore and persistence
# --------------------------------------------------------------------------
@app.route("/metrics_snapshot", methods=["POST"])
def metrics_snapshot():
    trace_id = get_trace_id(request)
    try:
        metrics.inc_metric("api_metrics_snapshot_requests_total")
        restore_metrics_snapshot()
        log_event(service="app", event="metrics_snapshot_restored", status="ok",
                  message="Metrics snapshot restored via API", trace_id=trace_id)
    except Exception as e:
        metrics.inc_metric("api_metrics_snapshot_failures_total")
        log_event(
            service="app",
            event="metrics_restore_fail",
            status="error",
            message=str(e),
            trace_id=trace_id,
        )

    # Persist using helper that expects metrics_collector.get_snapshot (callable)
    try:
        pushed = False
        try:
            pushed = push_snapshot_from_collector(get_snapshot)
        except TypeError:
            try:
                pushed = push_snapshot_from_collector(get_snapshot())
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
        metrics.inc_metric("api_metrics_requests_total")

        parts = []
        try:
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
            metrics.inc_metric("api_metrics_failures_total")
            return Response("# metrics_export_error 1\n", mimetype="text/plain"), 500

        return Response(combined, mimetype=CONTENT_TYPE_LATEST), 200
    except Exception as e:
        metrics.inc_metric("api_metrics_failures_total")
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
    trace_id = get_trace_id(request)
    try:
        metrics.inc_metric("api_tts_test_requests_total")
        data = request.get_json(force=True)
        session_id = data.get("session_id")
        text = data.get("text", "")
        
        celery.send_task("tasks.run_tts", args=[session_id, text])
        
        metrics.inc_metric("api_tts_test_dispatched_total")
        log_event(service="app", event="tts_task_dispatched", status="ok", 
                  trace_id=trace_id, extra={"session_id": session_id})
        return jsonify({"status": "queued", "trace_id": trace_id})
    except Exception as e:
        metrics.inc_metric("api_tts_test_failures_total")
        log_event(service="app", event="tts_dispatch_failed", status="error", 
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "trace_id": trace_id, "message": str(e)}), 500


# --------------------------------------------------------------------------
# Phase 11-D: Enhanced readiness endpoint
# --------------------------------------------------------------------------
@app.route("/ready", methods=["GET"])
def readiness_probe():
    """Kubernetes-style readiness probe with component checks."""
    trace_id = get_trace_id(request)
    try:
        metrics.inc_metric("api_ready_requests_total")
        
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
        metrics.inc_metric("api_ready_failures_total")
        log_event(service="app", event="readiness_error", status="error",
                  message=str(e), trace_id=trace_id)
        return jsonify({"status": "error", "message": str(e), "trace_id": trace_id}), 500


# --------------------------------------------------------------------------
# Main entrypoint
# --------------------------------------------------------------------------
if __name__ == "__main__":
    log_event(service="app", event="application_start", status="ok",
              message="Sara AI Core API starting", 
              extra={"phase": "11-D", "validated_components": len(startup_components)})
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)