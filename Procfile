# Procfile
web: gunicorn -k uvicorn.workers.UvicornWorker sara_ai.app:app --bind 0.0.0.0:$PORT --timeout 120
worker: celery -A sara_ai.celery_app.celery worker --pool=solo --loglevel=info
