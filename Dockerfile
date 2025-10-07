# --------------------------------------------------------
# Dockerfile — Sara AI Core (Phase 5B, Production Ready)
# --------------------------------------------------------

FROM python:3.11-slim

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y build-essential libsndfile1 && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application code
COPY . .

# Default command: Flask app (Render uses Procfile overrides)
CMD ["gunicorn", "sara_ai.app:app", "--bind", "0.0.0.0:5000", "--timeout", "120"]
