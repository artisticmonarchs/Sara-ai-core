# Dockerfile — Sara AI Core (Phase 12, Production-Ready)
# Focus: stable build, minimal deps, non-root, healthcheck, Render-ready.

FROM python:3.11-slim

# -------- Base OS deps (keep minimal) --------
# - ffmpeg/libsndfile1: audio ops for ASR/TTS pipelines
# - curl,nc: healthcheck & simple diagnostics
# - tini: proper PID 1 signal handling for gunicorn/celery
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ffmpeg \
      libsndfile1 \
      curl \
      netcat-openbsd \
      tini \
 && rm -rf /var/lib/apt/lists/*

# -------- Runtime env --------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=5000

# -------- Non-root user --------
RUN useradd -m -u 1000 appuser
WORKDIR /app

# -------- Dependencies (cache friendly) --------
# Copy only requirements first to leverage Docker layer cache.
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir -r /app/requirements.txt

# -------- Application code --------
COPY . /app
RUN chown -R appuser:appuser /app
USER appuser

# -------- Networking --------
EXPOSE ${PORT}

# -------- Healthcheck --------
# Tries HTTP /health, falls back to TCP port check, then noop.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD sh -c ' \
    if command -v curl >/dev/null 2>&1; then \
      curl -fsS http://127.0.0.1:${PORT}/health || nc -z 127.0.0.1 ${PORT}; \
    else \
      nc -z 127.0.0.1 ${PORT}; \
    fi \
  ' || exit 1

# -------- Entrypoint & Command --------
# Use tini for proper signal handling (gunicorn/celery).
ENTRYPOINT ["/usr/bin/tini", "--"]

# Default API service; override with START_CMD for worker/stream roles, e.g.:
#   START_CMD="celery -A celery_app.app worker -l info -Q default"
#   START_CMD="python twilio_media_stream.py"
CMD [ "sh", "-c", "${START_CMD:-gunicorn app:app --workers 2 --threads 4 --bind 0.0.0.0:${PORT} --timeout 120 --log-level info}" ]
