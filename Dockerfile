# ============================================================
# Dockerfile — Sara AI Core (Phase 10H, Production Ready)
# ------------------------------------------------------------
# ✅ Lightweight
# ✅ Multi-service compatible (API / Worker / Streaming)
# ✅ Build-cache optimized
# ✅ Render & Cloud Run ready
# ============================================================

FROM python:3.11-slim

# ------------------------------
# Environment configuration
# ------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/usr/local/bin:$PATH" \
    LANG=C.UTF-8

WORKDIR /app

# ------------------------------
# System dependencies
# ------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libsndfile1 \
 && rm -rf /var/lib/apt/lists/*

# ------------------------------
# Python dependencies
# ------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# ------------------------------
# Application code
# ------------------------------
COPY . .

# ------------------------------
# Default command (API service)
# You can override START_CMD for worker/streaming roles.
# ------------------------------
CMD ["sh", "-c", "${START_CMD:-gunicorn app:app --workers 2 --threads 4 --bind 0.0.0.0:5000 --timeout 120 --log-level info}"]
