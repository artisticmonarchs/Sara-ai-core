import asyncio
import aiohttp
import time
import json
import uuid
import statistics
from datetime import datetime
import os
import re

# === Configuration ===
API_BASE = os.getenv("SARA_API_BASE", "https://sara-ai-core-app.onrender.com")
TTS_ENDPOINT = f"{API_BASE}/tts_test"       # still using _test variant
METRICS_ENDPOINT = f"{API_BASE}/metrics"
CONCURRENCY = int(os.getenv("LOAD_CONCURRENCY", 12))
REQUESTS_PER_WORKER = int(os.getenv("LOAD_REQUESTS", 5))
TEXT = "Phase 10M-B validation cycle"
SESSION_ID = str(uuid.uuid4())

print(f"\n🚀 Starting Phase 10M-B load + metrics test")
print(f"→ Endpoint: {TTS_ENDPOINT}")
print(f"→ Concurrency: {CONCURRENCY}")
print(f"→ Requests per worker: {REQUESTS_PER_WORKER}")
print(f"→ Session ID: {SESSION_ID}\n")

# === Helper to scrape Prometheus-style metric ===
def parse_metric(text, name):
    match = re.search(rf"^{name}\s+([0-9\.]+)", text, re.MULTILINE)
    return float(match.group(1)) if match else None

async def call_tts(session, i):
    payload = {"text": f"{TEXT} (req {i})"}
    start = time.perf_counter()
    try:
        async with session.post(TTS_ENDPOINT, json=payload, timeout=30) as resp:
            data = await resp.json()
            elapsed = (time.perf_counter() - start) * 1000
            if data.get("status") == "ok":
                return {
                    "ok": True,
                    "latency_ms": elapsed,
                    "tts_latency_ms": data.get("tts_latency_ms"),
                    "r2_latency_ms": data.get("r2_latency_ms"),
                }
            else:
                return {"ok": False, "latency_ms": elapsed}
    except Exception as e:
        return {"ok": False, "error": str(e), "latency_ms": (time.perf_counter() - start) * 1000}

async def worker(worker_id, session, results):
    for i in range(REQUESTS_PER_WORKER):
        res = await call_tts(session, f"{worker_id}-{i}")
        results.append(res)

async def fetch_metrics(session):
    async with session.get(METRICS_ENDPOINT, timeout=15) as resp:
        return await resp.text()

async def main():
    async with aiohttp.ClientSession() as session:
        # baseline metrics
        before_metrics = await fetch_metrics(session)
        before_tts = parse_metric(before_metrics, "tts_requests_total") or 0
        before_fail = parse_metric(before_metrics, "tts_failures_total") or 0

        # === load run ===
        start = datetime.utcnow().isoformat() + "Z"
        t0 = time.perf_counter()
        results = []
        workers = [asyncio.create_task(worker(i, session, results)) for i in range(CONCURRENCY)]
        await asyncio.gather(*workers)
        duration = time.perf_counter() - t0

        # === post metrics ===
        after_metrics = await fetch_metrics(session)
        after_tts = parse_metric(after_metrics, "tts_requests_total") or before_tts
        after_fail = parse_metric(after_metrics, "tts_failures_total") or before_fail

    # === summary ===
    successful = [r for r in results if r.get("ok")]
    errors = [r for r in results if not r.get("ok")]
    latencies = [r["latency_ms"] for r in successful]
    tts_lat = [r["tts_latency_ms"] for r in successful if r.get("tts_latency_ms")]
    r2_lat = [r["r2_latency_ms"] for r in successful if r.get("r2_latency_ms")]

    def summary_stats(values):
        if not values:
            return {"median": None, "mean": None}
        return {
            "median": round(statistics.median(values), 2),
            "mean": round(statistics.mean(values), 2)
        }

    summary = {
        "phase": "10M-B",
        "start_time": start,
        "duration_s": duration,
        "concurrency": CONCURRENCY,
        "requests_per_worker": REQUESTS_PER_WORKER,
        "total_requests": len(results),
        "successful_requests": len(successful),
        "errors": len(errors),
        "latency_ms": summary_stats(latencies),
        "tts_latency_ms_summary": summary_stats(tts_lat),
        "r2_latency_ms_summary": summary_stats(r2_lat),
        "prometheus_deltas": {
            "tts_requests_total_delta": after_tts - before_tts,
            "tts_failures_total_delta": after_fail - before_fail,
        },
        "raw_results_csv": f"load_results_{int(time.time())}.csv",
    }

    filename = f"load_summary_{int(time.time())}.json"
    with open(filename, "w") as f:
        json.dump(summary, f, indent=2)

    print("\n✅ Phase 10M-B load + metrics complete.")
    print(f"Summary file: {filename}")
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
