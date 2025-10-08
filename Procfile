# Procfile — Sara AI Core (Phase 5B Final)

# Main API service
web: gunicorn -k uvicorn.workers.UvicornWorker sara_ai.app:app --bind 0.0.0.0:$PORT --timeout 120

# Celery worker
worker: celery -A sara_ai.celery_app worker --loglevel=info --pool=prefork --concurrency=2

# Streaming server
streaming: gunicorn -k uvicorn.workers.UvicornWorker sara_ai.streaming_server:app --bind 0.0.0.0:$PORT --timeout 120
