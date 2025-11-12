"""
config.py — Phase 12 (Stable Dual-Mode)
Centralized configuration for Sara AI Core (GPT-5 + Streaming Voice)

- Works on Render and local automatically.
- Local `.env.local` overrides cloud `.env` safely.
- Provides legacy aliases (DG_*) expected by some modules.
- Avoids import cycles by deferring logging/metrics imports.
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
# Helpers
# ===============================================================
def _bool(v, default=False):
    if v is None:
        return default
    return str(v).lower() in ("1", "true", "yes", "on")

def _int(v, default):
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

def _float(v, default):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


# ===============================================================
# Config Object
# ===============================================================
class Config:
    """Application configuration singleton."""

    def __init__(self):
        # Environment
        self.ENV_MODE = env_mode
        self.FLASK_ENV = os.getenv("FLASK_ENV", "development")
        self.FLASK_PORT = _int(os.getenv("FLASK_PORT"), 5000)
        self.STREAMING_PORT = _int(os.getenv("STREAMING_PORT"), 5001)
        self.INFERENCE_PORT = _int(os.getenv("INFERENCE_PORT"), 7000)
        self.SARA_PHASE = os.getenv("SARA_PHASE", "12")
        self.PHASE_VERSION = f"{self.SARA_PHASE}-Stable"

        # Redis / Celery
        self.REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
        self.CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", self.REDIS_URL)
        self.CELERY_RESULT_BACKEND = os.getenv(
            "CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/1"
        )
        self.CELERY_WORKER_CONCURRENCY = _int(os.getenv("CELERY_WORKER_CONCURRENCY"), 2)
        self.CELERY_TASK_MAX_RETRIES = _int(os.getenv("CELERY_TASK_MAX_RETRIES"), 3)

        # ✅ Celery retry configuration (used by tasks.py resilient wrappers)
        self.CELERY_RETRY_MAX = _int(os.getenv("CELERY_RETRY_MAX"), 5)
        self.CELERY_RETRY_DELAY = _int(os.getenv("CELERY_RETRY_DELAY"), 5)
        self.CELERY_RETRY_BACKOFF_MAX = _int(os.getenv("CELERY_RETRY_BACKOFF_MAX"), 600)

        # Celery tuning
        self.CELERY_TASK_SERIALIZER = os.getenv("CELERY_TASK_SERIALIZER", "json")
        self.CELERY_RESULT_SERIALIZER = os.getenv("CELERY_RESULT_SERIALIZER", "json")
        self.CELERY_ACCEPT_CONTENT = os.getenv("CELERY_ACCEPT_CONTENT", "json").split(",")
        self.CELERY_TIMEZONE = os.getenv("CELERY_TIMEZONE", "UTC")
        self.CELERY_ENABLE_UTC = _bool(os.getenv("CELERY_ENABLE_UTC", "true"), True)

        # Backward compatibility with Phase 11 configs
        self.RETRY_ATTEMPTS = _int(os.getenv("RETRY_ATTEMPTS"), 2)
        self.RETRY_BACKOFF_SECONDS = _int(os.getenv("RETRY_BACKOFF_SECONDS"), 1)

        # Logging / Observability
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.SERVICE_NAME = os.getenv("SERVICE_NAME", "sara_ai_core")
        self.ENABLE_LOG_BUFFERING = _bool(os.getenv("ENABLE_LOG_BUFFERING", "true"), True)
        self.LOG_BUFFER_SIZE = _int(os.getenv("LOG_BUFFER_SIZE"), 10000)

        # Sentry
        self.SENTRY_DSN = os.getenv("SENTRY_DSN", "")

        # GPT / OpenAI
        self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
        self.OPENAI_API_URL = os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/responses")
        self.OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        self.OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini")
        self.OPENAI_MAX_TOKENS = _int(os.getenv("OPENAI_MAX_TOKENS"), 1500)
        self.OPENAI_TIMEOUT = _int(os.getenv("OPENAI_TIMEOUT"), 60)

        # Additional GPT controls (Phase 12)
        self.GPT_MODEL = os.getenv("GPT_MODEL", "gpt-5")
        self.GPT_REASONING = os.getenv("GPT_REASONING", "low")
        self.GPT_VERBOSITY = os.getenv("GPT_VERBOSITY", "medium")
        self.GPT_MAX_TOKENS = _int(os.getenv("GPT_MAX_TOKENS"), 1000)
        self.GPT_TIMEOUT = _int(os.getenv("GPT_TIMEOUT"), 30)
        self.GPT_MAX_RETRIES = _int(os.getenv("GPT_MAX_RETRIES"), 3)
        self.GPT_RETRY_DELAY = _float(os.getenv("GPT_RETRY_DELAY"), 1.0)
        self.GPT_TEMPERATURE = _float(os.getenv("GPT_TEMPERATURE"), 0.7)
        self.GPT_STREAMING_ENABLED = _bool(os.getenv("GPT_STREAMING_ENABLED", "true"), True)
        self.MODEL_VERSION = os.getenv("MODEL_VERSION", "v2")

        # Deepgram TTS (REQUIRED on worker)
        # Accept both new (DEEPGRAM_*) and legacy (DG_*) envs; expose both for compatibility
        self.DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY") or os.getenv("DG_API_KEY", "")
        self.DEEPGRAM_SPEAK_MODEL = os.getenv("DEEPGRAM_SPEAK_MODEL") or os.getenv("DG_SPEAK_MODEL", "aura-2-asteria-en")
        # Legacy aliases expected by some code paths (e.g., tasks.py)
        self.DG_API_KEY = self.DEEPGRAM_API_KEY
        self.DG_SPEAK_MODEL = self.DEEPGRAM_SPEAK_MODEL

        # TTS / Voice
        self.MAX_TTS_TEXT_LEN = _int(os.getenv("MAX_TTS_TEXT_LEN"), 800)
        self.TTS_VOICE = os.getenv("TTS_VOICE", "sara_default")
        self.TTS_SAMPLE_RATE = _int(os.getenv("TTS_SAMPLE_RATE"), 24000)
        self.TTS_STREAM_CHUNK_MS = _int(os.getenv("TTS_STREAM_CHUNK_MS"), 120)
        self.TTS_CACHE_R2_PREFIX = os.getenv("TTS_CACHE_R2_PREFIX", "tts_cache/")

        # Storage (R2 / S3)
        self.R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "sara-ai-audio")
        self.R2_DISPOSITIONS_BUCKET = os.getenv("R2_DISPOSITIONS_BUCKET", self.R2_BUCKET_NAME)
        self.R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
        self.R2_REGION = os.getenv("R2_REGION", "auto")
        self.R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
        self.R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
        self.R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL", "")

        # Public host for reading uploaded audio (r2.dev gateway)
        self.PUBLIC_AUDIO_HOST = os.getenv("PUBLIC_AUDIO_HOST", "")
        # r2.dev URLs typically should NOT include the bucket in the path
        self.PUBLIC_AUDIO_INCLUDE_BUCKET = _bool(os.getenv("PUBLIC_AUDIO_INCLUDE_BUCKET", "false"), False)

        # Twilio
        self.TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
        self.TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
        self.TWILIO_MEDIA_WS_URL = os.getenv("TWILIO_MEDIA_WS_URL", "")
        self.TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER", "")

        # Memory / Conversation
        self.CONVERSATION_MEMORY_TTL = _int(os.getenv("CONVERSATION_MEMORY_TTL"), 60 * 60 * 4)
        self.MEMORY_REDIS_PREFIX = os.getenv("MEMORY_REDIS_PREFIX", "sara_memory:")

        # Safety / Misc
        self.SIMULATE = _bool(os.getenv("SIMULATE", "false"), False)
        self.COMPANY_NAME = os.getenv("COMPANY_NAME", "Noblecom Solutions")
        self.SARA_NAME = os.getenv("SARA_NAME", "Sara Hayes")
        self.SARA_ROLE = os.getenv("SARA_ROLE", "Senior Growth Consultant at Noblecom Solutions")

        # Circuit breaker
        self.CB_PREFIX = os.getenv("CB_PREFIX", "cb:")
        self.CB_TTL_SECONDS = _int(os.getenv("CB_TTL_SECONDS"), 30)
        self.CB_FAILURE_THRESHOLD = _int(os.getenv("CB_FAILURE_THRESHOLD"), 5)

    def as_dict(self):
        return {
            "env_mode": self.ENV_MODE,
            "phase": self.PHASE_VERSION,
            "service": self.SERVICE_NAME,
            "redis": self.REDIS_URL,
            "sentry_dsn": bool(self.SENTRY_DSN),
            "openai_key_set": bool(self.OPENAI_API_KEY),
            "deepgram_key_set": bool(self.DEEPGRAM_API_KEY),
            "r2_bucket": self.R2_BUCKET_NAME,
            "public_audio_host": self.PUBLIC_AUDIO_HOST,
            "public_audio_include_bucket": self.PUBLIC_AUDIO_INCLUDE_BUCKET,
        }


# ===============================================================
# Singleton Instance
# ===============================================================
config = Config()

# ===============================================================
# Logging Bootstrap (safe metrics) — lazy imports to avoid cycles
# ===============================================================
try:
    from metrics_collector import increment_metric as _push_metric  # type: ignore
    from logging_utils import log_event as _log_event  # type: ignore

    _push_metric(
        metric_name="module_load",
        service="system_core",
        event="module_load",
        status="ok",
        message=__name__,
    )
except Exception as e:
    try:
        # Fallback logging if metrics/logging modules aren't ready yet
        from logging_utils import log_event as _log_event  # type: ignore
        _log_event(
            service="system_core",
            event="metric_load_fail",
            status="error",
            message=str(e),
        )
    except Exception:
        # Last resort: silent fail to avoid import cycles at cold start
        pass
