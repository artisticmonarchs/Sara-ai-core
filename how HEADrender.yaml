# --------------------------------------------------------
# Procfile — Sara AI Core (Phase 5B, Final Production Build)
# --------------------------------------------------------

# ✅ Flask API (Gunicorn WSGI)
web: gunicorn sara_ai.app:app --bind 0.0.0.0:$PORT --timeout 120

# ✅ Celery Worker (Background Tasks)
worker: celery -A sara_ai.celery_app worker --loglevel=info

# ✅ Streaming Gateway (Twilio WebSocket Server)
stream: python -m sara_ai.streaming_server
