# --------------------------------------------------------
# tests/light_cycle_load.py — Light-Cycle Load Test
# --------------------------------------------------------

import time
import requests
import argparse
from datetime import datetime

# -------------------------------
# Argument Parsing
# -------------------------------
parser = argparse.ArgumentParser(description="Sara AI Light-Cycle Load Test")
parser.add_argument("--interval", type=int, default=300, help="Seconds between each task batch")
parser.add_argument("--duration", type=int, default=10800, help="Total duration in seconds (3 hours default)")
parser.add_argument("--env", type=str, choices=["local", "render"], default="local", help="Target environment")
args = parser.parse_args()

# -------------------------------
# Environment Configuration
# -------------------------------
if args.env == "local":
    API_URL = "http://127.0.0.1:5000/inference"
else:  # render deployment
    API_URL = "https://sara-ai-core-app.onrender.com/inference"

print(f"\n🚀 Starting light-cycle load test ({args.duration // 60} min total) on '{args.env}' environment...\n")

# -------------------------------
# Main Load Loop
# -------------------------------
start_time = time.time()
cycle = 0

while (time.time() - start_time) < args.duration:
    cycle += 1
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"=== Cycle {cycle} @ {timestamp} ===")

    # Minimal payload for faster response
    payload = {
        "input_text": "test"
    }

    try:
        response = requests.post(API_URL, json=payload, timeout=30)  # 30-second timeout
        if response.status_code == 200:
            task_id = response.json().get("task_id")
            print(f"[{datetime.now().isoformat()}] ✅ Task submitted → {task_id}")
        else:
            print(f"[{datetime.now().isoformat()}] ❌ HTTP {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] ❌ Error sending task: {e}")

    # Sleep until next cycle
    time.sleep(args.interval)

print("\n✅ Light-cycle load test complete.")
