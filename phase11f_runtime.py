#!/usr/bin/env python3
"""
phase11f_runtime_render.py
Phase 11-F runtime validator for Sara AI Core (Render environment).

Validates:
 - Persona governance file integrity (assets/)
 - Inference endpoint reachability
 - Redis connectivity (Phase 11-D RedisClient)
 - Prometheus /metrics health

Outputs a JSON summary to /tmp/phase11f_runtime_report.json
"""

import os
import time
import json
import traceback
from typing import Dict, Any

from logging_utils import get_logger
from metrics_collector import increment_metric, observe_latency
from redis_client import RedisClient          # ✅ Phase 11-D class-based client
from global_metrics_store import metrics_safe_sync
from persona_engine import load_governance_files  # ✅ fallback for persona loading

logger = get_logger("phase11f_runtime_render")

INFERENCE_URL = os.getenv("INFERENCE_URL", "https://sara-ai-app.onrender.com/infer")
PROM_URL = os.getenv("PROM_URL", "https://sara-ai-streaming.onrender.com/metrics")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


# -------------------------------
# Persona Integrity Check
# -------------------------------
def check_persona_integrity() -> bool:
    try:
        # Phase 11-D persona_engine loads governance from assets safely
        load_governance_files()
        logger.info({"event": "persona_integrity_verified", "phase": "11-F"})
        increment_metric("persona_integrity_ok_total")
        return True
    except Exception as e:
        logger.error({"event": "persona_integrity_failed", "error": str(e)})
        increment_metric("persona_integrity_fail_total")
        return False


# -------------------------------
# Inference Endpoint Check
# -------------------------------
def check_inference() -> Dict[str, Any]:
    import requests
    start = time.time()
    result = {"ok": False, "error": None}
    try:
        payload = {"input": "Hello Sara, are you online?", "source": "runtime_test"}
        resp = requests.post(INFERENCE_URL, json=payload, timeout=15)
        latency_ms = round((time.time() - start) * 1000, 2)
        result.update({
            "status_code": resp.status_code,
            "latency_ms": latency_ms,
            "response_excerpt": resp.text[:400] if resp.text else "",
            "ok": resp.status_code in (200, 202),
        })
        metric_name = "inference_ok_total" if result["ok"] else "inference_fail_total"
        increment_metric(metric_name)
    except Exception as e:
        result["error"] = str(e)
        increment_metric("inference_fail_total")
        logger.error({
            "event": "inference_error",
            "error": str(e),
            "trace": traceback.format_exc(),
        })
    finally:
        observe_latency("inference_latency_ms", (time.time() - start) * 1000)
    return result


# -------------------------------
# Redis Connectivity Check
# -------------------------------
def check_redis() -> bool:
    try:
        client = RedisClient()
        r = client.client
        r.ping()
        logger.info({"event": "redis_connected", "url": REDIS_URL})
        increment_metric("redis_ok_total")
        return True
    except Exception as e:
        logger.error({"event": "redis_error", "error": str(e)})
        increment_metric("redis_fail_total")
        return False


# -------------------------------
# Main Entry
# -------------------------------
def main():
    logger.info({"event": "phase11f_invoked", "time": str(time.time())})
    start = time.time()

    persona_ok = check_persona_integrity()
    inference_result = check_inference()
    redis_ok = check_redis()

    # ---------------------------
    # Prometheus Metrics Check
    # ---------------------------
    prom = {}
    try:
        import requests
        query_url = f"{PROM_URL}/api/v1/query?query=inference_requests_total"
        r = requests.get(query_url, timeout=5)
        prom["inference_requests_total_ok"] = r.status_code == 200
    except Exception as e:
        prom["error"] = str(e)
        logger.warning({"event": "prometheus_error", "error": str(e)})

    duration = round((time.time() - start) * 1000, 2)
    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "phase": "11-F",
        "summary": {
            "persona": persona_ok,
            "inference": inference_result.get("ok", False),
            "redis": redis_ok,
            "prometheus": prom.get("inference_requests_total_ok", False),
        },
        "duration_ms": duration,
    }

    path = "/tmp/phase11f_runtime_report.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # trigger metrics flush for global sync
    metrics_safe_sync()

    logger.info({"event": "phase11f_complete", "results_summary": summary["summary"]})
    logger.info({"event": "report_written", "path": path})
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
