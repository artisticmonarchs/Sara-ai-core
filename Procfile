# Procfile — Sara AI Core (Render + Local Parity Verified)

# Main Flask API (inference + tts endpoints)
web: gunicorn -k uvicorn.workers.UvicornWorker sara_ai.app:app --bind 0.0.0.0:$PORT --timeout 120

# Streaming Server (for Twilio Media Streams) — run as web server bound to $PORT
streaming: gunicorn sara_ai.streaming_server:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120

# Celery Worker (background async processing)
worker: celery -A sara_ai.celery_app.celery worker --loglevel=info --concurrency=2 -Q sara-ai-core-queue
