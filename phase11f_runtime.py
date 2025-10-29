#!/usr/bin/env python3
"""
phase11f_runtime_render.py — Phase 11-F Runtime Validation (Render-native)

- Uses PersonaEngine (static-load) from persona_engine.py
- Uses metrics_collector module via a safe shim (no class required)
- Connects to Render Valkey Redis via REDIS_URL environment variable
- Performs inference, persona, and redis checks and writes a JSON report
- Assumes all .py files live in repo root, with assets/ containing JSON governance files
"""

from __future__ import annotations

import os
import json
import time
import requests
import redis as redis_py
from datetime import datetime
from typing import Any, Dict

# ---- Config (Render-native, root-level assets) ----
INFERENCE_URL = os.getenv("INFERENCE_URL", "https://sara-ai-core-app.onrender.com/infer")
PROM_URL = os.getenv("PROM_URL", "https://sara-ai-core-streaming.onrender.com/metrics")
REDIS_URL = os.getenv("REDIS_URL", "redis://red-d3ip3vodl3ps73dd24o0:6379")

# assets/ is the only folder in repo; all others are flat in root
ASSETS_DIR = os.getenv("ASSETS_DIR", os.path.join(os.getcwd(), "assets"))
REPORT_PATH = os.getenv("PHASE11F_REPORT_PATH", os.path.join(os.getcwd(), "phase11f_runtime_report.json"))

# ---- Lazy imports and safe shims ----
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


def _get_metrics_shim():
    """Return (increment_fn, observe_latency_fn) from metrics_collector, or no-ops."""
    try:
        import importlib
        mc = importlib.import_module("metrics_collector")
        inc = getattr(mc, "increment", None) or getattr(mc, "increment_metric", None) or getattr(mc, "increment_metric_redis", None)
        obs = getattr(mc, "observe_latency", None) or getattr(mc, "observe_latency_seconds", None)
        if not callable(inc):
            inc = lambda *a, **k: None
        if not callable(obs):
            obs = lambda *a, **k: None
        return inc, obs
    except Exception:
        return (lambda *a, **k: None), (lambda *a, **k: None)

increment_metric, observe_latency = _get_metrics_shim()


# ---- PersonaEngine import ----
try:
    from persona_engine import PersonaEngine
    try:
        PersonaEngine.initialize()
    except Exception:
        pass
except Exception:
    PersonaEngine = None
    logger.warning("persona_engine import failed; persona checks will be skipped")


# ---- Redis connector helper ----
def get_redis_client(url: str):
    try:
        import importlib
        rc_mod = importlib.import_module("redis_client")
        if hasattr(rc_mod, "get_redis_client"):
            return rc_mod.get_redis_client(url)
        if hasattr(rc_mod, "get_client"):
            return rc_mod.get_client(url)
    except Exception:
        pass
    return redis_py.from_url(url, decode_responses=True)


# ---- Checks ----
def check_inference() -> Dict[str, Any]:
    start = time.time()
    out = {"ok": False, "error": None}
    try:
        payload = {"input": "Hello Sara, are you online?", "source": "runtime_test"}
        resp = requests.post(INFERENCE_URL, json=payload, timeout=10)
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
        logger.error({"event": "inference_check_error", "error": str(e)})
    finally:
        observe_latency("inference_latency_ms", (time.time() - start) * 1000)
    return out


def check_persona() -> Dict[str, Any]:
    start = time.time()
    out = {"ok": False, "reason": None}
    if PersonaEngine is None:
        out["reason"] = "persona_engine_missing"
        logger.warning("PersonaEngine not available")
        return out
    try:
        eng = PersonaEngine.get_instance()
    except Exception:
        try:
            eng = PersonaEngine.initialize()
        except Exception as e:
            out["reason"] = f"init_failed: {e}"
            logger.error("PersonaEngine initialize failed: %s", e)
            return out

    try:
        pd = eng.get_system_prompt()
        required_keys = ["systemprompt", "playbook", "callflow"]
        missing = [k for k in required_keys if k not in pd or not pd[k]]
        out["missing"] = missing
        out["ok"] = len(missing) == 0
        if out["ok"]:
            increment_metric("persona_check_ok_total")
            logger.info({"event": "persona_integrity_verified", "phase": "11-F"})
        else:
            increment_metric("persona_check_fail_total")
            logger.warning({"event": "persona_integrity_failed", "missing": missing})
    except Exception as e:
        out["reason"] = str(e)
        increment_metric("persona_check_fail_total")
        logger.error({"event": "persona_check_error", "error": str(e)})
    finally:
        observe_latency("persona_check_latency_ms", (time.time() - start) * 1000)
    return out


def check_redis() -> Dict[str, Any]:
    start = time.time()
    out = {"ok": False, "mode": None, "cache_keys": 0, "error": None}
    try:
        client = get_redis_client(REDIS_URL)
        client.ping()
        out["ok"] = True
        out["mode"] = "connected"
        try:
            keys = client.keys("inference_cache:*")
            out["cache_keys"] = len(keys)
        except Exception:
            out["cache_keys"] = -1
        increment_metric("redis_check_ok_total")
        logger.info({"event": "redis_check_success", "url": REDIS_URL})
    except Exception as e:
        out["error"] = str(e)
        increment_metric("redis_check_fail_total")
        logger.warning({"event": "redis_check_failed", "error": str(e), "url": REDIS_URL})
    finally:
        observe_latency("redis_check_latency_ms", (time.time() - start) * 1000)
    return out


# ---- Main ----
def main():
    t0 = datetime.utcnow().isoformat()
    print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Starting Phase 11-F runtime validation")

    results = {
        "timestamp": t0,
        "phase": "11-F",
        "assets_dir": ASSETS_DIR,
        "inference": check_inference(),
        "persona": check_persona(),
        "redis": check_redis(),
    }

    prom = {}
    try:
        q = f"{PROM_URL}/api/v1/query?query=inference_requests_total"
        r = requests.get(q, timeout=5)
        prom["inference_requests_total_ok"] = r.status_code == 200
    except Exception as e:
        prom["error"] = str(e)
    results["prometheus"] = prom

    try:
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=2)
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Report written to: {REPORT_PATH}")
    except Exception as e:
        logger.error({"event": "report_write_failed", "error": str(e)})

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
