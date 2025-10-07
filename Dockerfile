# Use slim Python base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose default port
EXPOSE 5000

# Start the Flask API with Gunicorn + UvicornWorker
CMD ["gunicorn", "-w", "2", "-k", "uvicorn.workers.UvicornWorker", "sara_ai.app:app", "--bind", "0.0.0.0:5000"]
