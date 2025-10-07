# Sara AI

Sara AI is a modular AI assistant built with Flask, Celery, Redis, and OpenAI.

---

## 🌍 Environment Policy

Sara AI supports **dual-mode environments**:

- **Local Development:**  
  Copy `.env.local` → `.env` before running locally.
  
  ```bash
  cp .env.local .env
Render Deployment:
Copy .env.render → .env (Render sets this automatically on deploy).

bash
Copy code
cp .env.render .env
🚀 Running Locally
bash
Copy code
pip install -r requirements.txt
python sara_ai/app.py
App will start at http://127.0.0.1:5000.

Healthcheck:

bash
Copy code
curl http://127.0.0.1:5000/health
🐳 Running with Docker
Build and run:

bash
Copy code
docker build -t sara-ai .
docker run -p 5000:5000 sara-ai
📡 Endpoints
/ → Root banner

/health → Service health

/inference → Submit prompt for async GPT inference (processed by Celery)

🔧 Services
sara-ai-app → Flask API

sara-ai-worker → Celery worker

sara-ai-streaming → Twilio streaming handler

📊 Observability
Logs: structured JSON logging

Sentry: enabled via SENTRY_DSN in .env

Healthcheck endpoints: /health

🚀 Render Start Commands
Service	Command
sara-ai-app	gunicorn -w 2 -k uvicorn.workers.UvicornWorker sara_ai.app:app
sara-ai-worker	celery -A sara_ai.tasks.celery_app worker --loglevel=info
sara-ai-streaming	gunicorn -w 2 -k uvicorn.workers.UvicornWorker sara_ai.streaming_server:app

