"""
app.py — Sara AI Core (Phase 10F)
Flask API with Redis-backed sessions, trace propagation,
conversation endpoints, and outbound initialization.
"""

import os
import uuid
import redis
import json as _json
import shutil
from flask import Flask, request, jsonify, send_from_directory
from logging_utils import log_event
from tasks import run_tts

# --------------------------------------------------------------------------
# Flask App & Config
# --------------------------------------------------------------------------
app = Flask(__name__, static_folder="public", static_url_path="/public")
SERVICE_NAME = "flask_app"

# --------------------------------------------------------------------------
# Redis / Session Config
# --------------------------------------------------------------------------
REDIS_URL = os.environ.get("REDIS_URL")
SESSION_TTL_SECONDS = int(os.environ.get("SESSION_TTL_SECONDS", 60 * 60 * 24))  # 24h
redis_client = None
USE_REDIS = False

if REDIS_URL:
    try:
        # decode_responses=True so we get strings back from redis
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        USE_REDIS = True
        try:
            redis_host = REDIS_URL.split("@")[-1]
        except Exception:
            redis_host = "unknown"
        log_event(SERVICE_NAME, "redis_init", message="Connected to Redis", redis_host=redis_host)
    except Exception as e:
        log_event(
            SERVICE_NAME,
            "redis_init_failed",
            level="WARNING",
            message="Redis connection failed — using in-memory sessions",
            error=str(e),
        )
else:
    log_event(SERVICE_NAME, "redis_missing", level="WARNING", message="REDIS_URL not set; using in-memory sessions")

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
            log_event(SERVICE_NAME, "redis_set_failed", level="ERROR", message="Redis SET failed", error=str(e))
    # fallback
    if SESSIONS is not None:
        SESSIONS[session_id] = data
    return True


def get_session(session_id: str) -> dict:
    if USE_REDIS and redis_client:
        try:
            raw = redis_client.get(_session_key(session_id))
            return _json.loads(raw) if raw else None
        except Exception as e:
            log_event(SERVICE_NAME, "redis_get_failed", level="WARNING", message="Redis GET failed", error=str(e))
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
# Load Sara’s Brains
# --------------------------------------------------------------------------
SARA_ASSETS = {}
SARA_BRAIN_PATH = os.environ.get("SARA_BRAIN_PATH", "assets")

brain_files = [
    "Sara_SystemPrompt_Production.json",
    "Sara_KnowledgeBase.json",
    "Sara_CallFlow.json",
    "Sara_Playbook.json",
    "Sara_Objections.json",
    "Sara_Opening.json",
]

loaded_files = []
for filename in brain_files:
    path = os.path.join(SARA_BRAIN_PATH, filename)
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                key = filename.replace(".json", "").lower()
                SARA_ASSETS[key] = _json.load(f)
                loaded_files.append(filename)
        else:
            log_event(SERVICE_NAME, "missing_brain_file", level="WARNING", message=f"Missing: {path}")
    except Exception as e:
        log_event(SERVICE_NAME, "brain_load_error", level="ERROR", message=f"Error loading {filename}", error=str(e))

log_event(SERVICE_NAME, "brain_load_summary", message=f"Loaded: {', '.join(loaded_files)}")

# --------------------------------------------------------------------------
# Conversation Endpoints
# --------------------------------------------------------------------------
@app.route("/conv/start", methods=["POST"])
def start_conversation():
    data = request.get_json() or {}
    lead_name = data.get("lead_name", "there")
    industry = data.get("industry", "General")

    session_id = str(uuid.uuid4())
    trace_id = data.get("trace_id") or get_trace()

    log_event(SERVICE_NAME, "conversation_start", message=f"New session for {lead_name}", lead_name=lead_name, industry=industry, trace_id=trace_id)

    session_obj = {"lead_name": lead_name, "industry": industry, "history": [], "callflow_step": "start", "trace_id": trace_id}
    save_session(session_id, session_obj)

    opening_data = SARA_ASSETS.get("sara_opening", {})
    if "openers" in opening_data and isinstance(opening_data["openers"], list) and opening_data["openers"]:
        sara_text = opening_data["openers"][0].replace("{lead_name}", lead_name)
    else:
        sara_text = f"Hello {lead_name}, this is Sara from Noblecom Solutions — how are you today?"

    return jsonify({
        "session_id": session_id,
        "trace_id": trace_id,
        "sara_text": sara_text,
        "prompt_preview": f"Lead: {lead_name}\nIndustry: {industry}\nContext: Starting new call.",
    })


@app.route("/conv/input", methods=["POST"])
def conversation_input():
    data = request.get_json() or {}
    session_id = data.get("session_id")
    user_text = data.get("user_text", "")

    if not session_id:
        return jsonify({"error": "Missing session_id"}), 400

    session = get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    # Update session
    history = session.get("history", [])
    history.append({"user": user_text})
    session["history"] = history

    # safe callflow retrieval
    callflow = SARA_ASSETS.get("sara_callflow", {}) or {}
    current_step = session.get("callflow_step", "start")
    next_step_data = {}
    if isinstance(callflow, dict):
        next_step_data = callflow.get(current_step, {}) or {}
    # if callflow is not dict, ignore it and use fallback

    sara_response = next_step_data.get("response") if isinstance(next_step_data, dict) else None
    next_step = next_step_data.get("next_step", current_step) if isinstance(next_step_data, dict) else current_step

    # Playbook fallback — handle both dict and list shapes safely
    if not sara_response:
        playbook = SARA_ASSETS.get("sara_objections", {}) or {}
        # if playbook is a dict with "rules"
        rules = []
        if isinstance(playbook, dict):
            rules = playbook.get("rules", []) or []
        elif isinstance(playbook, list):
            rules = playbook
        # iterate rules (each rule expected to be a dict)
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            triggers = rule.get("triggers", []) or []
            if any(t.lower() in user_text.lower() for t in triggers if isinstance(t, str)):
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
        action = "book"
        hot_lead = True

    session["callflow_step"] = next_step
    session["history"].append({"sara": sara_response})
    session["recent_responses"] = (session.get("recent_responses", []) + [sara_response])[-5:]

    save_session(session_id, session)

    trace_id = session.get("trace_id")
    log_event(SERVICE_NAME, "conversation_step", message=f"Processed input for session {session_id}", action=action, hot_lead=hot_lead, trace_id=trace_id)

    # Optional TTS enqueue:
    payload = {"session_id": session_id, "sara_text": sara_response, "trace_id": trace_id, "provider": "deepgram"}
    run_tts.delay(payload)

    return jsonify({"sara_text": sara_response, "action": action, "hot_lead": hot_lead, "trace_id": trace_id})

# --------------------------------------------------------------------------
# Outbound Initialization
# --------------------------------------------------------------------------
@app.route("/outbound", methods=["POST"])
def outbound_call():
    data = request.get_json() or {}
    to_number = data.get("to")
    lead_name = data.get("name", "there")

    if not to_number:
        return jsonify({"error": "Missing 'to' number"}), 400

    session_id = str(uuid.uuid4())
    trace_id = get_trace()
    session_obj = {"lead_name": lead_name, "to_number": to_number, "history": [], "callflow_step": "start", "trace_id": trace_id}
    save_session(session_id, session_obj)

    log_event(SERVICE_NAME, "outbound_call_init", message=f"Outbound call for {lead_name}", to_number=to_number, trace_id=trace_id)

    sara_opening = f"Hello {lead_name}, this is Sara from Noblecom Solutions. How are you today?"
    payload = {"session_id": session_id, "sara_text": sara_opening, "trace_id": trace_id, "provider": "deepgram"}
    run_tts.delay(payload)

    return jsonify({
        "status": "initiated",
        "to": to_number,
        "lead_name": lead_name,
        "trace_id": trace_id,
        "session_id": session_id,
        "sara_opening": sara_opening,
    })

# --------------------------------------------------------------------------
# QA Endpoint: TTS Smoke Test (Phase 10F)
# --------------------------------------------------------------------------
@app.route("/tts_test", methods=["POST"])
def tts_test():
    data = request.get_json() or {}
    text = data.get("text", "Hello from Sara AI test sequence.")
    trace_id = get_trace()

    session_id = f"tts_test_{trace_id}"
    output_folder = os.path.join("public", "audio", session_id)
    os.makedirs(output_folder, exist_ok=True)

    log_event(
        SERVICE_NAME,
        "tts_test_start",
        message="Triggered TTS smoke test",
        trace_id=trace_id,
        text=text,
        output_folder=output_folder,
    )

    payload = {"session_id": session_id, "sara_text": text, "trace_id": trace_id, "provider": "deepgram"}
    run_tts.delay(payload)

    # Return the expected public URL (Flask will serve from /audio/<session>/<file>.wav)
    file_url = f"https://{os.environ.get('PRIMARY_DOMAIN','sara-ai-core-app.onrender.com')}/audio/{session_id}/{trace_id}.wav"
    return jsonify({"status": "queued", "trace_id": trace_id, "file_url": file_url, "text": text})

# --------------------------------------------------------------------------
# Cleanup Endpoint
# --------------------------------------------------------------------------
@app.route("/cleanup/<session_id>", methods=["POST"])
def cleanup_session(session_id):
    folder = os.path.join("public", "audio", session_id)
    try:
        if os.path.exists(folder):
            shutil.rmtree(folder)
        delete_session(session_id)
        log_event(SERVICE_NAME, "cleanup_done", message="Cleaned session", session_id=session_id)
        return jsonify({"status": "cleaned", "session_id": session_id})
    except Exception as e:
        log_event(SERVICE_NAME, "cleanup_failed", level="ERROR", message="Cleanup failed", error=str(e), session_id=session_id)
        return jsonify({"status": "error", "error": str(e)}), 500

# --------------------------------------------------------------------------
# Serve Audio Files
# --------------------------------------------------------------------------
@app.route("/audio/<path:filename>")
def serve_audio(filename):
    """
    Serve audio files from /public/audio.
    'filename' can be 'session_id/trace.wav' or nested paths.
    """
    # guard: prevent path traversal by resolving normalized path against allowed dir
    # send_from_directory does internal checks, but ensure base dir exists
    audio_root = os.path.join(os.getcwd(), "public", "audio")
    return send_from_directory(audio_root, filename, as_attachment=False)

# --------------------------------------------------------------------------
# Health Check
# --------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    status = {"service": SERVICE_NAME, "status": "healthy"}
    if USE_REDIS and redis_client:
        try:
            redis_client.ping()
            status["redis"] = "ok"
        except Exception:
            status["redis"] = "unhealthy"
    return jsonify(status), 200

# --------------------------------------------------------------------------
# Local Debug
# --------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("FLASK_APP_PORT", 8000)))
