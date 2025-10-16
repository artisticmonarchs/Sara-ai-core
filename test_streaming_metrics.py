"""
tools/test_streaming_metrics.py — Phase 11-A
Validates streaming Prometheus gauges and summary metrics.
"""

import time
import random
from metrics_collector import (
    tts_active_streams,
    stream_latency_ms,
    stream_bytes_out_total,
    get_snapshot,
    export_prometheus,
)


def simulate_stream(stream_id: int):
    """Simulate a single TTS stream lifecycle."""
    tts_active_streams.inc()
    start = time.time()

    # Simulate random latency and audio size
    latency = random.uniform(0.8, 3.0)
    bytes_out = random.randint(50_000, 300_000)
    time.sleep(latency)

    # Record metrics
    stream_latency_ms.observe(latency * 1000)
    stream_bytes_out_total.inc(bytes_out)

    # Decrement active stream count
    tts_active_streams.dec()

    print(f"✅ Stream {stream_id} finished — {latency:.2f}s, {bytes_out} bytes")


def main():
    print("🔹 Simulating 5 concurrent TTS streams...\n")
    for i in range(5):
        simulate_stream(i + 1)

    print("\n--- JSON Snapshot ---")
    snapshot = get_snapshot()
    print(snapshot)

    print("\n--- Prometheus Export ---")
    print(export_prometheus())


if __name__ == "__main__":
    main()
