#!/usr/bin/env python3
"""
phase11f_runtime_render.py — Phase 11-F Render-native runtime validator

Designed to run inside the Render environment. DOES NOT use local fallbacks.
It validates:
  - Persona engine (assets -> persona_engine)
  - Inference endpoint (inference_server: POST /infer)
  - Redis (Render Valkey) connectivity
  - Prometheus/metrics (streaming service)
Outputs a structured JSON report and emits structured logs.

Execution:
  - Deploy to Render and run as a one-off task or inside a shell on the service container:
      python phase11f_runtime_render.py
Environment variables expected (Render will provide these; defaults target your services):
  - REDIS_URL            (e.g. redis://red-d3ip3vodl3ps73dd24o0:6379)
  - INFERENCE_URL        (default: https://sara-ai-core-app.onrender.com/infer)
  - PROM_URL             (default: https://sara-ai-core-streaming.onrender.com/metrics)
  - ASSETS_DIR           (default: ./assets)
  - REPORT_PATH          (default: /tmp/phase11f_runtime_report.json)
"""

from __future__ import annotations

import os
import json
import time
import traceback
from datetime import datetime
from typing import Any, Dict, Optional

# Lazy imports to avoid circular issues in Render deployments
def _get_logger():
    try:
        import importlib
        lu = importlib.import_module("logging_utils")
        if hasattr(lu, "get_json_logger"):
            return lu.get_json_logger("phase11f_runtime_render")
        if hasattr(lu, "logger"):
            return lu.logger
    except Exception:
        pass

    class _Fallback:
        def info(self, *a, **k): print("INFO:", *a)
        def warning(self, *a, **k): print("WARN:", *a)
        def error(self, *a, **k): print("ERROR:", *a)
        def debug(self, *a, **k): print("DEBUG:", *a)
    return _Fallback()

logger = _get_logger()

# Metrics shim (safe)
def _get_metrics_shim():
    try:
        import importlib
        mc = importlib.import_module("metrics_collector")
        inc = getattr(mc, "increment", None) or getattr(mc, "increment_metric", None) or (lambda *a, **k: None)
        obs = getattr(mc, "observe_latency", None) or getattr(mc, "observe_latency_seconds", None) or (lambda *a, **k: None)
        return inc, obs
    except Exception:
        return (lambda *a, **k: None), (lambda *a, **k: None)

increment_metric, observe_latency = _get_metrics_shim()

# Persona engine import (expected to be Phase 11-F file)
try:
    from persona_engine import PersonaEngine
    try:
        PersonaEngine.initialize()
    except Exception:
        # initialize() may already have been called during import; ignore initialization errors here
        pass
except Exception as e:
    PersonaEngine = None
    logger.error({"event": "persona_import_failed", "error": str(e)})
    # keep going, persona checks will be reported as missing in the report

# Redis connector helper (prefer repo helper, else fallback to redis.from_url)
def _get_redis_client(url: str):
    try:
        import importlib
        rc_mod = importlib.import_module("redis_client")
        if hasattr(rc_mod, "get_redis_client"):
            return rc_mod.get_redis_client(url)
        if hasattr(rc_mod, "get_client"):
            return rc_mod.get_client(url)
    except Exception:
        pass
    try:
        import redis as redis_py
        return redis_py.from_url(url, decode_responses=True)
    except Exception as e:
        raise RuntimeError(f"redis client unavailable: {e}")

# Config: rely on Render env variables; provide safe Render-targeted defaults
REDIS_URL = os.getenv("REDIS_URL", "redis://red-d3ip3vodl3ps73dd24o0:6379")
INFERENCE_URL = os.getenv("INFERENCE_URL", "https://sara-ai-core-app.onrender.com/infer")
PROM_URL = os.getenv("PROM_URL", "https://sara-ai-core-streaming.onrender.com")
ASSETS_DIR = os.getenv("ASSETS_DIR", os.path.join(os.getcwd(), "assets"))
REPORT_PATH = os.getenv("REPORT_PATH", "/tmp/phase11f_runtime_report.json")

# Utils
def now_ts() -> str:
    return datetime.utcnow().isoformat()

# -------------------------
# Checks
# -------------------------
def check_persona() -> Dict[str, Any]:
    result: Dict[str, Any] = {"ok": False, "missing": [], "error": None}
    if PersonaEngine is None:
        result["error"] = "PersonaEngine module not importable"
        logger.error({"event": "persona_check_error", "error": result["error"]})
        return result

    try:
        engine = PersonaEngine.get_instance()
    except Exception:
        try:
            engine = PersonaEngine.initialize()
        except Exception as e:
            result["error"] = f"PersonaEngine initialize failed: {e}"
            logger.error({"event": "persona_check_initialize_failed", "error": result["error"]})
            return result

    try:
        pd = engine.get_system_prompt()
        required_keys = ["systemprompt", "playbook", "callflow"]
        missing = [k for k in required_keys if k not in pd or not pd[k]]
        result["missing"] = missing
        result["ok"] = len(missing) == 0
        if result["ok"]:
            logger.info({"event": "persona_integrity_verified", "phase": "11-F"})
            increment_metric("persona_check_ok_total")
        else:
            logger.warning({"event": "persona_integrity_missing", "missing": missing})
            increment_metric("persona_check_fail_total")
    except Exception as e:
        result["error"] = str(e)
        logger.error({"event": "persona_check_error", "error": result["error"], "trace": traceback.format_exc()})
        increment_metric("persona_check_fail_total")
    return result

def check_inference() -> Dict[str, Any]:
    start = time.time()
    out: Dict[str, Any] = {"ok": False, "status_code": None, "latency_ms": None, "response_excerpt": None, "error": None}
    try:
        payload = {"input": "Hello Sara, are you online?", "source": "phase11f_runtime"}
        resp = requests.post(INFERENCE_URL, json=payload, timeout=15)
        out["status_code"] = resp.status_code
        out["latency_ms"] = round((time.time() - start) * 1000, 2)
        out["response_excerpt"] = (resp.text[:1000] if resp.text else "")
        out["ok"] = resp.status_code in (200, 202)
        if out["ok"]:
            increment_metric("inference_ok_total")
            logger.info({"event": "inference_ok", "status": resp.status_code, "latency_ms": out["latency_ms"]})
        else:
            increment_metric("inference_fail_total")
            logger.warning({"event": "inference_bad_status", "status": resp.status_code})
    except Exception as e:
        out["error"] = str(e)
        increment_metric("inference_fail_total")
        logger.error({"event": "inference_error", "error": str(e), "trace": traceback.format_exc()})
    finally:
        observe_latency("inference_latency_ms", (time.time() - start) * 1000)
    return out

def check_redis() -> Dict[str, Any]:
    start = time.time()
    out: Dict[str, Any] = {"ok": False, "error": None, "cache_keys": None, "url": REDIS_URL}
    try:
        client = _get_redis_client(REDIS_URL)
        client.ping()
        out["ok"] = True
        try:
            # Try to count relevant keys (best-effort)
            keys = client.keys("inference_cache:*")
            out["cache_keys"] = len(keys) if isinstance(keys, (list, tuple)) else -1
        except Exception:
            out["cache_keys"] = -1
        increment_metric("redis_check_ok_total")
        logger.info({"event": "redis_connected", "url": REDIS_URL})
    except Exception as e:
        out["error"] = str(e)
        increment_metric("redis_check_fail_total")
        logger.warning({"event": "redis_connect_failed", "url": REDIS_URL, "error": out["error"]})
    finally:
        observe_latency("redis_check_latency_ms", (time.time() - start) * 1000)
    return out

def check_prometheus() -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": False, "error": None}
    try:
        # check a basic query - prefer Prometheus API on PROM_URL
        q = f"{PROM_URL}/api/v1/query?query=inference_requests_total"
        resp = requests.get(q, timeout=8)
        out["ok"] = resp.status_code == 200
        if out["ok"]:
            logger.info({"event": "prometheus_ok", "url": PROM_URL})
        else:
            out["error"] = f"HTTP {resp.status_code}"
            logger.warning({"event": "prometheus_bad_status", "status": resp.status_code})
    except Exception as e:
        out["error"] = str(e)
        logger.warning({"event": "prometheus_error", "error": out["error"]})
    return out

# -------------------------
# Main runner
# -------------------------
def run_all_checks() -> Dict[str, Any]:
    ts = now = datetime.utcnow().isoformat()
    report: Dict[str, Any] = {"timestamp": ts, "phase": "11-F", "results": {}}

    logger.info({"event": "phase11f_start", "timestamp": ts})

    report["results"]["persona"] = check_persona()
    report["results"]["inference"] = check_inference()
    report["results"]["redis"] = check_redis()
    report["results"]["prometheus"] = check_prometheus()

    logger.info({"event": "phase11f_complete", "results_summary": {k: v.get("ok", None) for k, v in report["results"].items()}})
    return report

def persist_report(report: Dict[str, Any]) -> None:
    try:
        folder = os.path.dirname(REPORT_PATH)
        if folder:
            os.makedirs(folder, exist_ok=True)
        with open(REPORT_PATH, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        logger.info({"event": "report_written", "path": REPORT_PATH})
    except Exception as e:
        logger.error({"event": "report_write_failed", "error": str(e), "trace": traceback.format_exc()})

def main():
    logger.info({"event": "phase11f_invoked", "time": now_ts()})
    report = run_all_checks()
    persist_report(report)
    # print compact summary to stdout for immediate visibility in Render logs
    summary = {k: v.get("ok", False) for k, v in report["results"].items()}
    print(json.dumps({"timestamp": report["timestamp"], "phase": "11-F", "summary": summary}, indent=2))

if __name__ == "__main__":
    main()
