#!/usr/bin/env python3
"""
phase11f_runtime_render.py
Phase 11-F runtime validation for Render environment.
Verifies persona integrity, inference endpoint, Redis connectivity, and Prometheus metrics availability.
"""

import os
import json
import time
import traceback
from typing import Any, Dict

import requests  # ✅ global import — ensures available everywhere
from logging_utils import get_logger
from metrics_collector import increment_metric, observe_latency
from redis_client import get_redis_client
from persona_engine import verify_persona_integrity

logger = get_logger("phase11f_runtime_render")

INFERENCE_URL = os.getenv("INFERENCE_URL", "http://localhost:8000/inference")
PROM_URL = os.getenv("PROM_URL", "http://localhost:9090")


def check_inference() -> Dict[str, Any]:
    start = time.time()
    out = {"ok": False, "error": None}
    try:
        payload = {"input": "Hello Sara, are you online?", "source": "runtime_test"}
        resp = requests.post(INFERENCE_URL, json=payload, timeout=15)
        latency_ms = round((time.time() - start) * 1000, 2)
        out["status_code"] = resp.status_code
        out["latency_ms"] = latency_ms
        out["response_excerpt"] = resp.text[:400] if resp.text else ""
        out["ok"] = resp.status_code in (200, 202)
        if out["ok"]:
            increment_metric("inference_ok_total")
        else:
            increment_metric("inference_fail_total")
    except Exception as e:
        out["error"] = str(e)
        increment_metric("inference_fail_total")
        logger.error({
            "event": "inference_error",
            "error": str(e),
            "trace": traceback.format_exc()
        })
    finally:
        observe_latency("inference_latency_ms", (time.time() - start) * 1000)
    return out


def check_redis() -> Dict[str, Any]:
    out = {"ok": False, "error": None}
    try:
        client = get_redis_client()
        test_key = "phase11f_test_key"
        client.set(test_key, "ok", ex=5)
        val = client.get(test_key)
        out["ok"] = val == b"ok"
        if out["ok"]:
            increment_metric("redis_ok_total")
            logger.info({"event": "redis_connected", "url": client.connection_pool.connection_kwargs.get("host")})
        else:
            increment_metric("redis_fail_total")
    except Exception as e:
        out["error"] = str(e)
        increment_metric("redis_fail_total")
        logger.error({"event": "redis_error", "error": str(e)})
    return out


def check_prometheus() -> Dict[str, Any]:
    prom = {}
    try:
        q = f"{PROM_URL}/api/v1/query?query=inference_requests_total"
        r = requests.get(q, timeout=5)
        prom["inference_requests_total_ok"] = r.status_code == 200
    except Exception as e:
        prom["error"] = str(e)
        logger.warning({"event": "prometheus_error", "error": str(e)})
    return prom


def main() -> None:
    logger.info({"event": "phase11f_invoked", "time": str(time.time())})
    logger.info({"event": "phase11f_start", "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")})

    results = {
        "persona": verify_persona_integrity(),
        "inference": check_inference().get("ok", False),
        "redis": check_redis().get("ok", False),
    }

    prom_results = check_prometheus()
    results["prometheus"] = prom_results.get("inference_requests_total_ok", False)

    logger.info({"event": "phase11f_complete", "results_summary": results})

    report = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "phase": "11-F",
        "summary": results
    }

    with open("/tmp/phase11f_runtime_report.json", "w") as f:
        json.dump(report, f, indent=2)
    logger.info({"event": "report_written", "path": "/tmp/phase11f_runtime_report.json"})

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
