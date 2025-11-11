from logging_utils import log_event
from metrics_collector import increment_metric as push_metric
from redis_client import get_client as get_redis_connection

"""
config.py — Phase 12 (Stable Dual-Mode)
Centralized configuration for Sara AI Core (GPT-5 + Streaming Voice)

- Works both on Render and local automatically.
- Local `.env.local` overrides cloud `.env` safely.
- No edits needed before git push or Render deploy.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

# ===============================================================
# 1️⃣ Load Render/Production defaults (.env)
# ===============================================================
load_dotenv(BASE_DIR / ".env", override=False)

# ===============================================================
# 2️⃣ Local Overrides (only if ENV_MODE is 'local' or not set)
# ===============================================================
env_mode = os.getenv("ENV_MODE", "local").lower()
if env_mode == "local":
    local_env_path = BASE_DIR / ".env.local"
    if local_env_path.exists():
        load_dotenv(local_env_path, override=True)

# ===============================================================
# Helper
# ===============================================================
def _bool(v, default=False):
    if v is None:
        return default
    return str(v).lower() in ("1", "true", "yes", "on")


# ===============================================================
# Config Object
# ===============================================================
class Config:
    """Application configuration singleton."""

    def __init__(self):
        # Environment
        self.ENV_MODE = env_mode
        self.FLASK_ENV = os.getenv("FLASK_ENV", "development")
        self.FLASK_PORT = int(os.getenv("FLASK_PORT", 5000))
        self.STREAMING_PORT = int(os.getenv("STREAMING_PORT", 5001))
        self.INFERENCE_PORT = int(os.getenv("INFERENCE_PORT", 7000))
        self.SARA_PHASE = os.getenv("SARA_PHASE", "12")
        self.PHASE_VERSION = f"{self.SARA_PHASE}-Stable"

        # Redis / Celery
        self.REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
        self.CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", self.REDIS_URL)
        self.CELERY_RESULT_BACKEND = os.getenv(
            "CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/1"
        )
        self.CELERY_WORKER_CONCURRENCY = int(os.getenv("CELERY_WORKER_CONCURRENCY", 2))
        self.CELERY_TASK_MAX_RETRIES = int(os.getenv("CELERY_TASK_MAX_RETRIES", 3))

        # ✅ Celery retry configuration (fix for tasks.py)
        self.CELERY_RETRY_MAX = int(os.getenv("CELERY_RETRY_MAX", 5))
        self.CELERY_RETRY_DELAY = int(os.getenv("CELERY_RETRY_DELAY", 5))
        self.CELERY_RETRY_BACKOFF_MAX = int(os.getenv("CELERY_RETRY_BACKOFF_MAX", 600))

        # Celery tuning
        self.CELERY_TASK_SERIALIZER = os.getenv("CELERY_TASK_SERIALIZER", "json")
        self.CELERY_RESULT_SERIALIZER = os.getenv("CELERY_RESULT_SERIALIZER", "json")
        self.CELERY_ACCEPT_CONTENT = os.getenv(
            "CELERY_ACCEPT_CONTENT", "json"
        ).split(",")
        self.CELERY_TIMEZONE = os.getenv("CELERY_TIMEZONE", "UTC")
        self.CELERY_ENABLE_UTC = _bool(os.getenv("CELERY_ENABLE_UTC", "true"), True)

        # Backward compatibility with Phase 11 configs
        self.RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", 2))
        self.RETRY_BACKOFF_SECONDS = int(os.getenv("RETRY_BACKOFF_SECONDS", 1))

        # Logging / Observability
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.SERVICE_NAME = os.getenv("SERVICE_NAME", "sara_ai_core")
        self.enable_log_buffering = _bool(os.getenv("ENABLE_LOG_BUFFERING", "true"), True)
        self.log_buffer_size = int(os.getenv("LOG_BUFFER_SIZE", 10000))

        # Sentry
        self.SENTRY_DSN = os.getenv("SENTRY_DSN", "")

        # GPT / OpenAI
        self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
        self.OPENAI_API_URL = os.getenv(
            "OPENAI_API_URL", "https://api.openai.com/v1/responses"
        )
        self.OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini")
        self.OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        self.GPT_MODEL = os.getenv("GPT_MODEL", "gpt-5")
        self.GPT_REASONING = os.getenv("GPT_REASONING", "low")
        self.GPT_VERBOSITY = os.getenv("GPT_VERBOSITY", "medium")
        self.OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", 1500))
        self.GPT_MAX_TOKENS = int(os.getenv("GPT_MAX_TOKENS", 1000))
        self.OPENAI_TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", 60))
        self.GPT_TIMEOUT = int(os.getenv("GPT_TIMEOUT", 30))
        self.GPT_MAX_RETRIES = int(os.getenv("GPT_MAX_RETRIES", 3))
        self.GPT_RETRY_DELAY = float(os.getenv("GPT_RETRY_DELAY", 1.0))
        self.GPT_TEMPERATURE = float(os.getenv("GPT_TEMPERATURE", 0.7))
        self.GPT_STREAMING_ENABLED = _bool(os.getenv("GPT_STREAMING_ENABLED", "true"), True)
        self.MODEL_VERSION = os.getenv("MODEL_VERSION", "v2")

        # TTS / Render / Voice
        self.MAX_TTS_TEXT_LEN = int(os.getenv("MAX_TTS_TEXT_LEN", 800))
        self.TTS_VOICE = os.getenv("TTS_VOICE", "sara_default")
        self.TTS_SAMPLE_RATE = int(os.getenv("TTS_SAMPLE_RATE", 24000))
        self.TTS_STREAM_CHUNK_MS = int(os.getenv("TTS_STREAM_CHUNK_MS", 120))
        self.TTS_CACHE_R2_PREFIX = os.getenv("TTS_CACHE_R2_PREFIX", "tts_cache/")

        # Storage (R2 / S3)
        self.R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "sara-ai-audio")
        self.R2_DISPOSITIONS_BUCKET = os.getenv(
            "R2_DISPOSITIONS_BUCKET", self.R2_BUCKET_NAME
        )
        self.R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
        self.R2_REGION = os.getenv("R2_REGION", "auto")
        self.R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
        self.R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
        self.PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "")
        self.PUBLIC_AUDIO_INCLUDE_BUCKET = _bool(
            os.getenv("PUBLIC_AUDIO_INCLUDE_BUCKET", "true"), True
        )

        # Twilio
        self.TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
        self.TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
        self.TWILIO_MEDIA_WS_URL = os.getenv("TWILIO_MEDIA_WS_URL", "")
        self.TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER", "")

        # Memory / Conversation
        self.CONVERSATION_MEMORY_TTL = int(
            os.getenv("CONVERSATION_MEMORY_TTL", 60 * 60 * 4)
        )
        self.MEMORY_REDIS_PREFIX = os.getenv("MEMORY_REDIS_PREFIX", "sara_memory:")

        # Safety / Misc
        self.SIMULATE = _bool(os.getenv("SIMULATE", "false"), False)
        self.COMPANY_NAME = os.getenv("COMPANY_NAME", "Noblecom Solutions")
        self.SARA_NAME = os.getenv("SARA_NAME", "Sara Hayes")
        self.SARA_ROLE = os.getenv(
            "SARA_ROLE", "Senior Growth Consultant at Noblecom Solutions"
        )

        # Circuit breaker
        self.CB_PREFIX = os.getenv("CB_PREFIX", "cb:")
        self.CB_TTL_SECONDS = int(os.getenv("CB_TTL_SECONDS", 30))
        self.CB_FAILURE_THRESHOLD = int(os.getenv("CB_FAILURE_THRESHOLD", 5))

    def as_dict(self):
        return {
            "env_mode": self.ENV_MODE,
            "phase": self.PHASE_VERSION,
            "service": self.SERVICE_NAME,
            "gpt_model": self.GPT_MODEL,
            "redis": self.REDIS_URL,
            "sentry_dsn": bool(self.SENTRY_DSN),
            "openai_key_set": bool(self.OPENAI_API_KEY),
        }


# ===============================================================
# Singleton Instance
# ===============================================================
config = Config()

# ===============================================================
# Logging Bootstrap (safe metrics)
# ===============================================================
LOG_BUFFER_SIZE = int(os.getenv("LOG_BUFFER_SIZE", "5000"))

try:
    push_metric(
        metric_name="module_load",
        service="system_core",
        event="module_load",
        status="ok",
        message=__name__,
    )
except Exception as e:
    log_event(
        service="system_core",
        event="metric_load_fail",
        status="error",
        message=str(e),
    )
