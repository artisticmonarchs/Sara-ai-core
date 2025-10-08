web: gunicorn -k uvicorn.workers.UvicornWorker sara_ai.app:app --bind 0.0.0.0:$PORT
worker: celery -A sara_ai.celery_app worker --loglevel=info
streaming: python -m sara_ai.streaming_server
