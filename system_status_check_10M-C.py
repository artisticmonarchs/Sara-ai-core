#!/usr/bin/env python3
# system_status_check_10M-C.py — Phase 10M-C: System Status Cross-Validation
# Usage: python system_status_check_10M-C.py
# Requires: requests, json, time

import requests
import json
import time
from datetime import datetime
import uuid

BASE_URL = "https://sara-ai-core-app.onrender.com"
SYSTEM_STATUS_URL = f"{BASE_URL}/system_status"
METRICS_URL = f"{BASE_URL}/metrics"
TTS_TEST_URL = f"{BASE_URL}/tts_test"

SESSION_ID = str(uuid.uuid4())
HEADERS = {
    "Content-Type": "application/json",
    "X-Trace-Source": "system-status-check",
    "X-Test-Phase": "10M-C"
}

def safe_get_json(url):
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def parse_prometheus_metrics(text):
    metrics = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        try:
            k, v = line.strip().split(" ", 1)
            metrics[k] = float(v)
        except ValueError:
            continue
    return metrics

def safe_get_metrics():
    try:
        r = requests.get(METRICS_URL, timeout=10)
        r.raise_for_status()
        return parse_prometheus_metrics(r.text)
    except Exception as e:
        return {"error": str(e)}

def run_tts_test():
    payload = {"session_id": SESSION_ID, "text": "Phase 10M-C system status validation message"}
    try:
        r = requests.post(TTS_TEST_URL, json=payload, headers=HEADERS, timeout=20)
        return {"status": r.status_code, "response": r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text}
    except Exception as e:
        return {"error": str(e)}

def main():
    print("\n🚀 Starting Phase 10M-C system validation")
    print(f"→ Base URL: {BASE_URL}")
    print(f"→ Session ID: {SESSION_ID}\n")

    result = {"phase": "10M-C", "start_time": datetime.utcnow().isoformat() + "Z"}

    # Step 1 — system_status
    print("🔍 Checking /system_status ...")
    sys_status = safe_get_json(SYSTEM_STATUS_URL)
    result["system_status"] = sys_status

    # Step 2 — baseline metrics
    print("📊 Fetching baseline /metrics snapshot ...")
    before_metrics = safe_get_metrics()
    result["metrics_before"] = before_metrics
    tts_req_before = before_metrics.get("sara_tts_requests_total", 0.0)
    tts_fail_before = before_metrics.get("sara_tts_failures_total", 0.0)

    # Step 3 — run test TTS
    print("🗣️  Running test TTS request ...")
    tts_result = run_tts_test()
    result["tts_test"] = tts_result

    # Step 4 — wait and get updated metrics
    print("⏳ Waiting for metric propagation ...")
    time.sleep(5)
    after_metrics = safe_get_metrics()
    result["metrics_after"] = after_metrics
    tts_req_after = after_metrics.get("sara_tts_requests_total", 0.0)
    tts_fail_after = after_metrics.get("sara_tts_failures_total", 0.0)

    # Step 5 — calculate deltas
    result["prometheus_deltas"] = {
        "tts_requests_total_delta": tts_req_after - tts_req_before,
        "tts_failures_total_delta": tts_fail_after - tts_fail_before
    }

    # Step 6 — summary output
    result["end_time"] = datetime.utcnow().isoformat() + "Z"
    print("\n✅ Phase 10M-C system validation complete.\n")
    print(json.dumps(result, indent=2))

    with open(f"system_status_summary_{int(time.time())}.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
        print(f"\n📁 Summary written to {f.name}")

if __name__ == "__main__":
    main()
