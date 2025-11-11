# Procfile — aligns with Render service commands

# Web API (Flask/Gunicorn)
web: gunicorn app:app --workers 2 --threads 4 --bind 0.0.0.0:${PORT:-5000} --timeout 120 --log-level info

# Streaming server (Twilio Media Streams)
streaming: python -m streaming_server

# Celery Worker (Celery instance exposed as `celery` in celery_app.py)
worker: celery -A celery_app.celery worker --loglevel=info --concurrency=1
