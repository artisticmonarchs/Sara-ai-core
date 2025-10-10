# Procfile — Sara AI Core (Phase 6 Ready, Flattened Structure)

# Main Flask API (inference + TTS endpoints)
web: gunicorn -k uvicorn.workers.UvicornWorker app:app --bind 0.0.0.0:$PORT

# Streaming Server (for Twilio Media Streams)
streaming: python -m streaming_server

# Celery Worker (background async processing)
worker: celery -A celery_app.celery worker --loglevel=info
