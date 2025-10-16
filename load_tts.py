#!/usr/bin/env python3
# load_tts.py — Phase 10M Production Validation Script (fixed endpoint)
# Usage: python load_tts.py
# Requires: aiohttp, asyncio, python-dateutil, pandas (optional)
# pip install aiohttp python-dateutil pandas

import asyncio
import aiohttp
import csv
import json
import time
import uuid
from datetime import datetime
from dateutil import tz
import statistics
import os

# ----------------- CONFIG -----------------
API_BASE = os.getenv("SARA_API_BASE", "https://sara-ai-core-app.onrender.com")  # ✅ production base
TTS_ENDPOINT = f"{API_BASE}/tts_test"  # ✅ fixed: working endpoint confirmed

CONCURRENCY = int(os.getenv("CONCURRENCY", "12"))  # 10–20 threads typical
REQUESTS_PER_WORKER = int(os.getenv("REQS_PER_WORKER", "5"))  # each worker sends N sequential requests
SESSION_ID = os.getenv("SESSION_ID", str(uuid.uuid4()))  # fixed session ID to trigger cache hits
TEXTS = [
    "Hello, this is Sara AI Core Phase 10M test message one.",
    "This is a second message for the same session to validate caching.",
    "Load test to verify Deepgram + R2 + cache performance under concurrency.",
    "Varying text to ensure cache key behavior remains consistent.",
    "Final sample line to complete load sequence."
]

TIMEOUT = aiohttp.ClientTimeout(total=30)
OUTPUT_CSV = f"load_results_{int(time.time())}.csv"
OUTPUT_SUMMARY = f"load_summary_{int(time.time())}.json"

HEADERS = {
    "Content-Type": "application/json",
    "X-Trace-Source": "load-test",
    "X-Test-Phase": "10M-prod-validation"
    # add Authorization header here if /tts_test ever becomes protected
}
# ------------------------------------------

async def call_tts(session, payload):
    start = time.time()
    try:
        async with session.post(TTS_ENDPOINT, json=payload, timeout=TIMEOUT) as resp:
            text = await resp.text()
            elapsed = (time.time() - start) * 1000.0
            status = resp.status
            try:
                data = json.loads(text)
            except Exception:
                data = {"_raw": text}
            return {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "session_id": payload.get("session_id"),
                "text": payload.get("text"),
                "status": status,
                "elapsed_ms": elapsed,
                "response": data
            }
    except Exception as e:
        elapsed = (time.time() - start) * 1000.0
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "session_id": payload.get("session_id"),
            "text": payload.get("text"),
            "status": "error",
            "elapsed_ms": elapsed,
            "error": str(e)
        }

async def worker_task(worker_id):
    results = []
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        for i in range(REQUESTS_PER_WORKER):
            text = TEXTS[(worker_id + i) % len(TEXTS)]
            payload = {"session_id": SESSION_ID, "text": text}
            r = await call_tts(session, payload)
            r["worker_id"] = worker_id
            r["request_index"] = i
            results.append(r)
            await asyncio.sleep(0.05)  # small jitter between requests
    return results

async def main():
    print(f"\n🚀 Starting load test")
    print(f"→ Endpoint: {TTS_ENDPOINT}")
    print(f"→ Concurrency: {CONCURRENCY}")
    print(f"→ Requests per worker: {REQUESTS_PER_WORKER}")
    print(f"→ Session ID: {SESSION_ID}\n")

    tasks = [asyncio.create_task(worker_task(i)) for i in range(CONCURRENCY)]
    start_ts = time.time()
    all_results = []
    completed = await asyncio.gather(*tasks)
    duration = time.time() - start_ts
    for c in completed:
        all_results.extend(c)

    # --- Write CSV ---
    keys = ["timestamp", "worker_id", "request_index", "session_id", "text", "status", "elapsed_ms", "response", "error"]
    with open(OUTPUT_CSV, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in all_results:
            row = {k: r.get(k, "") for k in keys}
            row["response"] = json.dumps(r.get("response", {}))
            writer.writerow(row)

    # --- Compute Summary ---
    latencies = [r["elapsed_ms"] for r in all_results if isinstance(r["status"], int)]
    errors = [r for r in all_results if r["status"] == "error" or (isinstance(r["status"], int) and r["status"] >= 500)]
    success_count = len([r for r in all_results if isinstance(r["status"], int) and r["status"] < 400])
    total = len(all_results)

    cached_flags, tts_latencies, r2_latencies = [], [], []
    for r in all_results:
        resp = r.get("response") or {}
        if isinstance(resp, dict):
            if resp.get("cached") is not None:
                cached_flags.append(bool(resp.get("cached")))
            if isinstance(resp.get("tts_latency_ms"), (int, float)):
                tts_latencies.append(resp["tts_latency_ms"])
            r2_l = resp.get("r2_latency_ms") or resp.get("r2_latency")
            if isinstance(r2_l, (int, float)):
                r2_latencies.append(r2_l)

    cache_hits = sum(1 for v in cached_flags if v)
    cache_misses = len(cached_flags) - cache_hits
    cache_hit_ratio = (cache_hits / len(cached_flags) * 100.0) if cached_flags else None

    summary = {
        "start_time": datetime.utcnow().isoformat() + "Z",
        "duration_s": duration,
        "concurrency": CONCURRENCY,
        "requests_per_worker": REQUESTS_PER_WORKER,
        "total_requests": total,
        "successful_requests": success_count,
        "errors": len(errors),
        "latency_ms": {
            "min": min(latencies) if latencies else None,
            "max": max(latencies) if latencies else None,
            "median": statistics.median(latencies) if latencies else None,
            "mean": statistics.mean(latencies) if latencies else None,
            "p95": sorted(latencies)[int(len(latencies) * 0.95) - 1] if latencies and len(latencies) >= 20 else None
        },
        "cache": {
            "samples": len(cached_flags),
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "cache_hit_ratio_percent": cache_hit_ratio
        },
        "tts_latency_ms_summary": {
            "median": statistics.median(tts_latencies) if tts_latencies else None,
            "mean": statistics.mean(tts_latencies) if tts_latencies else None
        },
        "r2_latency_ms_summary": {
            "median": statistics.median(r2_latencies) if r2_latencies else None,
            "mean": statistics.mean(r2_latencies) if r2_latencies else None
        },
        "raw_csv": OUTPUT_CSV
    }

    with open(OUTPUT_SUMMARY, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\n✅ Load test complete.")
    print("Summary file:", OUTPUT_SUMMARY)
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
