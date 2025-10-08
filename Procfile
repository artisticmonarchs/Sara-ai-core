# Procfile — Sara AI Core (Phase 5B)
# Defines process types for Render deployment

# Main API service (Flask + Uvicorn + Gunicorn)
web: gunicorn -k uvicorn.workers.UvicornWorker sara_ai.app:app --bind 0.0.0.0:$PORT --timeout 120

# Celery worker (background tasks)
worker: celery -A sara_ai.celery_app worker --loglevel=info --pool=prefork --concurrency=2

# Streaming server (Twilio gateway)
streaming: gunicorn -k uvicorn.workers.UvicornWorker sara_ai.streaming_server:app --bind 0.0.0.0:$PORT --timeout 120
