# --------------------------------------------------------
# Procfile — Sara AI Core (Phase 5B, Fixed & Production-Ready)
# --------------------------------------------------------

# ✅ Flask API (uses default WSGI worker, not UvicornWorker)
web: gunicorn sara_ai.app:app --bind 0.0.0.0:$PORT --timeout 120

# ✅ Celery worker (correct import path, no .celery suffix)
worker: celery -A sara_ai.celery_app worker --loglevel=info

# ✅ Streaming server (Twilio / WebSocket Gateway)
stream: python -m sara_ai.streaming_server
