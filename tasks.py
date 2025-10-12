"""
tasks.py — Sara AI Core (Phase 10C)
Unified Celery tasks for inference + TTS with structured logging, Redis metrics, and Twilio-ready audio storage.
"""

import os
import io
import time
import uuid
import traceback
from celery_app import celery
from logging_utils import log_event
from redis import Redis
from deepgram import DeepgramClient, SpeakOptions
import openai

# --------------------------------------------------------------------------
# Environment & Clients
# --------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DG_API_KEY = os.getenv("DEEPGRAM_API_KEY")
DG_SPEAK_MODEL = os.getenv("DEEPGRAM_SPEAK_MODEL", "aura-2-asteria-en")
PUBLIC_AUDIO_PATH = os.getenv("PUBLIC_AUDIO_PATH", "public/audio")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
dg_client = DeepgramClient(DG_API_KEY)
openai.api_key = OPENAI_API_KEY

# --------------------------------------------------------------------------
# Helper
# --------------------------------------------------------------------------
def get_trace():
    return str(uuid.uuid4())

def save_audio_file(session_id: str, trace_id: str, audio_bytes: bytes) -> str:
    folder = os.path.join(PUBLIC_AUDIO_PATH, session_id)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{trace_id}.wav")
    with open(path, "wb") as f:
        f.write(audio_bytes)
    return path

# --------------------------------------------------------------------------
# Inference Task
# --------------------------------------------------------------------------
@celery.task(name="sara_ai.tasks.run_inference", bind=True)
def run_inference(self, payload: dict):
    """
    Process transcript → OpenAI reply → enqueue TTS.
    """
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    transcript = payload.get("text") or payload.get("input") or ""
    
    trace_id = log_event(
        service="tasks",
        event="inference_start",
        status="ok",
        message=f"Received transcript ({len(transcript)} chars)",
        trace_id=trace_id,
        session_id=session_id,
    )

    start_time = time.time()
    try:
        # Generate Sara's reply
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are Sara Hayes, a friendly AI assistant."},
                {"role": "user", "content": transcript}
            ],
            max_tokens=250
        )
        reply_text = response.choices[0].message["content"].strip()
        tokens_used = response.usage.total_tokens

        # Enqueue TTS
        celery.send_task("sara_ai.tasks.run_tts", args=[{
            "text": reply_text,
            "trace_id": trace_id,
            "session_id": session_id
        }])

        latency_ms = round((time.time() - start_time) * 1000, 2)
        log_event(
            service="tasks",
            event="inference_done",
            status="ok",
            message=f"Inference completed ({len(reply_text)} chars, {tokens_used} tokens, {latency_ms}ms)",
            trace_id=trace_id,
            session_id=session_id,
            extra={"tokens_used": tokens_used, "latency_ms": latency_ms},
        )

        return {"trace_id": trace_id, "session_id": session_id, "reply": reply_text}

    except Exception:
        err_msg = traceback.format_exc()
        log_event(
            service="tasks",
            event="inference_error",
            status="error",
            message=err_msg,
            trace_id=trace_id,
            session_id=session_id,
        )
        raise

# --------------------------------------------------------------------------
# TTS Task
# --------------------------------------------------------------------------
@celery.task(name="sara_ai.tasks.run_tts", bind=True)
def run_tts(self, payload: dict):
    """
    Convert text → Deepgram Speak audio, save to public/audio, return URL.
    """
    trace_id = payload.get("trace_id") or get_trace()
    session_id = payload.get("session_id") or str(uuid.uuid4())
    text = payload.get("text") or ""

    trace_id = log_event(
        service="tasks",
        event="tts_start",
        status="ok",
        message=f"TTS task started for {len(text)} chars",
        trace_id=trace_id,
        session_id=session_id,
    )

    if not text:
        log_event(
            service="tasks",
            event="tts_missing_text",
            status="error",
            message="No text provided for TTS",
            trace_id=trace_id,
            session_id=session_id,
        )
        return {"error": "Missing text", "trace_id": trace_id, "session_id": session_id}

    start_time = time.time()
    try:
        speak_opts = SpeakOptions(model=DG_SPEAK_MODEL)
        response = dg_client.speak.v("1").stream({"text": text}, speak_opts)

        audio_bytes = io.BytesIO()
        for chunk in response.stream:
            audio_bytes.write(chunk)

        if audio_bytes.tell() == 0:
            raise RuntimeError("Deepgram returned empty audio stream.")

        audio_path = save_audio_file(session_id, trace_id, audio_bytes.getvalue())
        duration = round(time.time() - start_time, 2)
        public_url = f"/audio/{session_id}/{trace_id}.wav"

        # Metrics
        redis_client.hincrby("metrics:tts", "files_generated", 1)

        log_event(
            service="tasks",
            event="tts_done",
            status="ok",
            message=f"TTS audio generated ({len(text)} chars, {duration}s)",
            trace_id=trace_id,
            session_id=session_id,
            extra={"audio_path": audio_path, "duration_s": duration, "public_url": public_url},
        )

        return {"trace_id": trace_id, "session_id": session_id, "audio_url": public_url}

    except Exception:
        err_msg = traceback.format_exc()
        log_event(
            service="tasks",
            event="tts_failed",
            status="error",
            message=err_msg,
            trace_id=trace_id,
            session_id=session_id,
        )
        return {"error": str(err_msg), "trace_id": trace_id, "session_id": session_id}
