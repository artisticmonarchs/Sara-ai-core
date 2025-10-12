"""
streaming_ws.py — Sara AI Core (Phase 10B/C)
Real-time Twilio → Deepgram ASR Bridge (Controlled Modular Mode)
"""

import os
import io
import json
import base64
import asyncio
import websockets
from flask import Flask
from flask_sock import Sock
from sara_ai.logging_utils import log_event
from sara_ai.sentry_utils import capture_exception
from sara_ai.celery_app import celery
from redis import Redis
import uuid
import time

# -------------------------------------------------------------------
# Environment & Globals
# -------------------------------------------------------------------

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
DG_LISTEN_MODEL = os.getenv("DEEPGRAM_LISTEN_MODEL", "nova-3")

# Twilio 8 kHz PCM stream parameters
DEEPGRAM_WS_BASE = (
    f"wss://api.deepgram.com/v1/listen?"
    f"model={DG_LISTEN_MODEL}&encoding=linear16&sample_rate=8000"
)

app = Flask(__name__)
sock = Sock(app)
redis_client = Redis.from_url(REDIS_URL, decode_responses=True)

# -------------------------------------------------------------------
# Utility
# -------------------------------------------------------------------

def new_trace():
    return str(uuid.uuid4())

async def enqueue_inference(transcript: str, trace_id: str, session_id: str):
    """Dispatch transcript to Celery for Sara inference"""
    payload = {
        "trace_id": trace_id,
        "text": transcript,
        "session_id": session_id,
        "source": "twilio",
        "timestamp": time.time(),
    }
    celery.send_task("run_inference", args=[payload])
    log_event(
        service="streaming_ws",
        event="inference_enqueued",
        message=f"Transcript dispatched ({len(transcript)} chars)",
        trace_id=trace_id,
        session_id=session_id,
    )

# -------------------------------------------------------------------
# WebSocket Route
# -------------------------------------------------------------------

@sock.route("/media-stream/ws")
async def media_stream(ws):
    """Handle incoming Twilio Media Stream → Deepgram ASR pipeline"""
    trace_id = new_trace()
    session_id = str(uuid.uuid4())
    deepgram_ws = None
    deepgram_reader_task = None

    # Metrics
    redis_client.hincrby("metrics:deepgram", "streams_total", 1)

    try:
        log_event(
            service="streaming_ws",
            event="stream_opened",
            message="New Twilio media session established",
            trace_id=trace_id,
            session_id=session_id,
        )

        # Receive initial 'connected' event from Twilio
        message = await ws.recv()
        data = json.loads(message)
        if data.get("event") == "connected":
            call_sid = data.get("call_sid")
            if call_sid:
                # Store session_id -> call_sid mapping in Redis (1-hour TTL)
                redis_client.set(f"twilio_call:{session_id}", call_sid, ex=3600)
                log_event(
                    service="streaming_ws",
                    event="twilio_call_sid_stored",
                    message=f"Stored Call SID {call_sid} for session {session_id}",
                    trace_id=trace_id,
                    session_id=session_id,
                )

        # Connect to Deepgram live endpoint
        deepgram_ws = await websockets.connect(
            DEEPGRAM_WS_BASE,
            extra_headers={"Authorization": f"Token {DEEPGRAM_API_KEY}"},
            ping_interval=20,
            ping_timeout=20,
        )

        async def deepgram_reader():
            """Listen for transcript messages from Deepgram"""
            try:
                async for msg in deepgram_ws:
                    data = json.loads(msg)
                    if "channel" in data and "alternatives" in data["channel"]:
                        alt = data["channel"]["alternatives"][0]
                        transcript = alt.get("transcript", "").strip()
                        if transcript and data.get("is_final"):
                            redis_client.hincrby("metrics:deepgram", "transcripts_final", 1)
                            await enqueue_inference(transcript, trace_id, session_id)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                capture_exception(e)
                log_event(
                    service="streaming_ws",
                    event="deepgram_reader_error",
                    status="error",
                    message=str(e),
                    trace_id=trace_id,
                    session_id=session_id,
                )

        deepgram_reader_task = asyncio.create_task(deepgram_reader())

        # Main loop for Twilio events
        async for message in ws:
            try:
                data = json.loads(message)
                event_type = data.get("event", "")

                if event_type == "start":
                    log_event(
                        service="streaming_ws",
                        event="stream_start",
                        message="Streaming started",
                        trace_id=trace_id,
                        session_id=session_id,
                    )

                elif event_type == "media":
                    audio_payload = data.get("media", {}).get("payload")
                    if not audio_payload:
                        continue

                    # Decode µ-law → PCM16LE bytes
                    pcm_bytes = base64.b64decode(audio_payload)
                    await deepgram_ws.send(pcm_bytes)
                    await asyncio.sleep(0.02)  # backpressure throttle (~20 ms)

                elif event_type == "stop":
                    log_event(
                        service="streaming_ws",
                        event="stream_stop",
                        message="Twilio stream stop event",
                        trace_id=trace_id,
                        session_id=session_id,
                    )
                    if deepgram_reader_task:
                        deepgram_reader_task.cancel()
                    break

            except Exception as e:
                capture_exception(e)
                log_event(
                    service="streaming_ws",
                    event="media_stream_error",
                    status="error",
                    message=str(e),
                    trace_id=trace_id,
                    session_id=session_id,
                )

    except Exception as e:
        capture_exception(e)
        log_event(
            service="streaming_ws",
            event="connection_error",
            status="error",
            message=str(e),
            trace_id=trace_id,
            session_id=session_id,
        )

    finally:
        # Clean shutdown
        try:
            if deepgram_reader_task:
                deepgram_reader_task.cancel()
            if deepgram_ws:
                await deepgram_ws.close()
        except Exception:
            pass

        log_event(
            service="streaming_ws",
            event="stream_closed",
            message="Session ended cleanly",
            trace_id=trace_id,
            session_id=session_id,
        )

# -------------------------------------------------------------------
# Local debug entry point
# -------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("STREAMING_WS_PORT", 8000)))
