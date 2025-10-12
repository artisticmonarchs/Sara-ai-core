"""
app.py — Sara AI Core (Phase 10C)
Flask API with Redis-backed sessions, trace propagation, and conversation endpoints.
"""

import os
import uuid
import time
import redis
import json as _json
from datetime import timedelta
from flask import Flask, request, jsonify
from logging_utils import log_event
# from tasks import run_tts  # Uncomment when you want to enqueue TTS

# --------------------------------------------------------------------------
# Flask App & Config
# --------------------------------------------------------------------------
app = Flask(__name__)
SERVICE_NAME = "flask_app"

# Session & Redis Config
REDIS_URL = os.environ.get("REDIS_URL")
SESSION_TTL_SECONDS = int(os.environ.get("SESSION_TTL_SECONDS", 60 * 60 * 24))  # 24h default
redis_client = None
USE_REDIS = False

if REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        USE_REDIS = True
        log_event(SERVICE_NAME, "redis_init", message="Connected to Redis", redis_url=REDIS_URL)
    except Exception as e:
        log_event(SERVICE_NAME, "redis_init_failed", level="WARNING",
                  message="Redis connection failed, falling back to in-memory sessions",
                  error=str(e))
else:
    log_event(SERVICE_NAME, "redis_missing", level="WARNING", message="REDIS_URL not set; using in-memory sessions")

# In-memory fallback if Redis unavailable
SESSIONS = {} if not USE_REDIS else None

# --------------------------------------------------------------------------
# Helper Functions
# --------------------------------------------------------------------------
def _session_key(session_id: str) -> str:
    return f"session:{session_id}"

def save_session(session_id: str, data: dict):
    payload = _json.dumps(data)
    if USE_REDIS and redis_client:
        try:
            redis_client.set(_session_key(session_id), payload, ex=SESSION_TTL_SECONDS)
            return True
        except Exception as e:
            log_event(SERVICE_NAME, "redis_set_failed", level="ERROR",
                      message="Failed to set session in Redis", error=str(e))
    # fallback
    SESSIONS[session_id] = data
    return True

def get_session(session_id: str) -> dict:
    if USE_REDIS and redis_client:
        try:
            raw = redis_client.get(_session_key(session_id))
            if raw:
                return _json.loads(raw)
            return None
        except Exception as e:
            log_event(SERVICE_NAME, "redis_get_failed", level="WARNING",
                      message="Redis GET failed", error=str(e))
    return SESSIONS.get(session_id) if SESSIONS is not None else None

def delete_session(session_id: str):
    if USE_REDIS and redis_client:
        try:
            redis_client.delete(_session_key(session_id))
        except Exception as e:
            log_event(SERVICE_NAME, "redis_delete_failed", level="WARNING", message="Redis DELETE failed", error=str(e))
    if SESSIONS is not None and session_id in SESSIONS:
        del SESSIONS[session_id]
    return True

def get_trace() -> str:
    return str(uuid.uuid4())

# --------------------------------------------------------------------------
# Conversation Endpoints
# --------------------------------------------------------------------------
@app.route("/conv/start", methods=["POST"])
def start_conversation():
    data = request.get_json() or {}
    lead_name = data.get("lead_name", "there")
    industry = data.get("industry", "General")

    session_id = str(uuid.uuid4())
    supplied_trace = data.get("trace_id")
    trace_id = supplied_trace or log_event(
        SERVICE_NAME, "conversation_start",
        message=f"New session for {lead_name}",
        lead_name=lead_name, industry=industry
    )

    session_obj = {
        "lead_name": lead_name,
        "industry": industry,
        "history": [],
        "callflow_step": "start",
        "trace_id": trace_id,
    }

    save_session(session_id, session_obj)

    opening_data = SARA_ASSETS.get("opening", {})
    if "openers" in opening_data and isinstance(opening_data["openers"], list) and opening_data["openers"]:
        sara_text = opening_data["openers"][0].replace("{lead_name}", lead_name)
    else:
        sara_text = f"Hello {lead_name}, this is Sara from BrightReach — how are you today?"

    return jsonify({
        "session_id": session_id,
        "trace_id": trace_id,
        "sara_text": sara_text,
        "prompt_preview": f"Lead: {lead_name}\nIndustry: {industry}\nContext: Starting new call."
    })


@app.route("/conv/input", methods=["POST"])
def conversation_input():
    data = request.get_json() or {}
    session_id = data.get("session_id")
    user_text = data.get("user_text", "")

    if not session_id:
        return jsonify({"error": "Invalid or missing session_id"}), 400

    session = get_session(session_id)
    if not session:
        return jsonify({"error": "session not found"}), 404

    # Append user text
    history = session.get("history", [])
    history.append({"user": user_text})
    session["history"] = history

    callflow = SARA_ASSETS.get("callflow", {})
    current_step = session.get("callflow_step", "start")
    next_step_data = callflow.get(current_step, {})

    sara_response = next_step_data.get("response")
    next_step = next_step_data.get("next_step", current_step)

    # Playbook fallback
    if not sara_response:
        playbook = SARA_ASSETS.get("playbook", {})
        for rule in playbook.get("rules", []):
            triggers = rule.get("triggers", [])
            if any(t.lower() in user_text.lower() for t in triggers):
                sara_response = rule.get("response")
                break

    if not sara_response:
        sara_response = f"Thanks for your message: '{user_text}'. Let's continue."

    # QA tagging
    action = "log_only"
    hot_lead = False
    text = user_text.lower()
    if any(k in text for k in ["book", "meeting", "tuesday"]):
        action = "book"
    elif any(k in text for k in ["callback", "call me later"]):
        action = "callback"
    elif any(k in text for k in ["urgent", "today"]):
        action = "book"; hot_lead = True

    session["callflow_step"] = next_step
    session["history"].append({"sara": sara_response})

    # Persist session
    save_session(session_id, session)

    trace_id = session.get("trace_id")
    log_event(
        SERVICE_NAME, "conversation_step",
        message=f"Processed input for session {session_id}",
        action=action, hot_lead=hot_lead,
        trace_id=trace_id
    )

    # Optional TTS enqueue:
    # payload = {"session_id": session_id, "sara_text": sara_response, "trace_id": trace_id, "provider": "deepgram"}
    # run_tts.delay(payload)

    return jsonify({
        "sara_text": sara_response,
        "action": action,
        "hot_lead": hot_lead,
        "trace_id": trace_id
    })


# --------------------------------------------------------------------------
# Health Check
# --------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"service": SERVICE_NAME, "status": "healthy"}), 200


# --------------------------------------------------------------------------
# Local Debug
# --------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("FLASK_APP_PORT", 8000)))
