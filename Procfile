# Procfile — Sara AI Core (Render + Local Parity Verified)

# Main Flask API (inference + tts endpoints)
web: gunicorn -k uvicorn.workers.UvicornWorker sara_ai.app:app --bind 0.0.0.0:$PORT

# Streaming Server (for Twilio Media Streams)
streaming: python -m sara_ai.streaming_server

# Celery Worker (background async processing)
worker: celery -A sara_ai.celery_app.celery worker --loglevel=info
