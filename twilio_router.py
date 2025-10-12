"""
twilio_router.py — Sara AI Core (Phase 10C)
Flask endpoint to handle Twilio call playback for generated TTS audio.
"""

import os
from flask import Flask, request, Response, jsonify
from twilio.rest import Client
from logging_utils import log_event
from redis import Redis

# --------------------------------------------------------------------------
# Environment
# --------------------------------------------------------------------------
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PUBLIC_AUDIO_BASE = os.getenv("PUBLIC_AUDIO_BASE", "https://your-domain.com/audio")

redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

app = Flask(__name__)
SERVICE_NAME = "twilio_router"

# --------------------------------------------------------------------------
# Playback Endpoint
# --------------------------------------------------------------------------
@app.route("/twilio/playback", methods=["POST"])
def playback():
    """
    Endpoint to update a Twilio call with <Play> of generated TTS audio.
    Expects JSON payload: { "session_id": str, "trace_id": str }
    """
    payload = request.get_json(silent=True) or {}
    session_id = payload.get("session_id")
    trace_id = payload.get("trace_id")
    trace_log = trace_id or "unknown-trace"

    if not session_id or not trace_id:
        log_event(
            service=SERVICE_NAME,
            event="playback_missing_fields",
            status="error",
            message=f"Missing session_id or trace_id in request: {payload}",
            trace_id=trace_log,
            session_id=session_id,
        )
        return jsonify({"error": "Missing session_id or trace_id", "trace_id": trace_log}), 400

    try:
        # Retrieve Twilio Call SID from Redis
        call_sid = redis_client.get(f"twilio_call:{session_id}")
        if not call_sid:
            log_event(
                service=SERVICE_NAME,
                event="playback_missing_call_sid",
                status="error",
                message=f"No call SID found for session {session_id}",
                trace_id=trace_log,
                session_id=session_id,
            )
            return jsonify({"error": "Call SID not found", "trace_id": trace_log}), 404

        # Construct audio URL
        audio_url = f"{PUBLIC_AUDIO_BASE}/{session_id}/{trace_id}.wav"

        # Build TwiML to play audio
        twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Play>{audio_url}</Play>
</Response>"""

        # Update Twilio call
        twilio_client.calls(call_sid).update(twiml=twiml)

        log_event(
            service=SERVICE_NAME,
            event="twilio_playback_update",
            status="ok",
            message=f"Updated Twilio call {call_sid} with audio {audio_url}",
            trace_id=trace_log,
            session_id=session_id,
        )

        return jsonify({"status": "success", "audio_url": audio_url, "trace_id": trace_log}), 200

    except Exception as e:
        log_event(
            service=SERVICE_NAME,
            event="twilio_playback_error",
            status="error",
            message=str(e),
            trace_id=trace_log,
            session_id=session_id,
        )
        return jsonify({"error": str(e), "trace_id": trace_log}), 500

# --------------------------------------------------------------------------
# Health Check
# --------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"service": SERVICE_NAME, "status": "healthy"}), 200

# --------------------------------------------------------------------------
# Local Debug Entry
# --------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("TWILIO_ROUTER_PORT", 8001)))
