"""
config.py — Phase 12 (Stable Dual-Mode)
Centralized configuration for Sara AI Core (GPT-5 + Streaming Voice)

- Works on Render and local automatically.
- Local `.env.local` overrides cloud `.env` safely.
- Provides legacy aliases (DG_*) and snake_case module-level mirrors.
- Avoids import cycles by not importing app/metrics/logging at module import time.
"""

import os
from pathlib import Path

# Optional: python-dotenv (safe if present, no hard dependency in container)
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    def load_dotenv(*_args, **_kwargs):
        return False  # noop if library not present

# --------------------------------------------------------------------------------------
# .env loading (order: .env → .env.local as last override)
# --------------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR / ".env.local")


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _to_bool(val: str | None, default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}

def _to_int(val: str | None, default: int) -> int:
    try:
        return int(str(val).strip()) if val is not None else default
    except Exception:
        return default

def _redact(value: str | None) -> str | None:
    if not value:
        return value
    if len(value) <= 8:
        return "****"
    return value[:4] + "…" + value[-4:]


# --------------------------------------------------------------------------------------
# Core Config
# --------------------------------------------------------------------------------------
class Config:
    # Environment / service identity
    ENV: str = os.getenv("ENV", os.getenv("APP_ENV", "local"))
    DEBUG: bool = _to_bool(os.getenv("DEBUG"), False)
    SERVICE_NAME: str = os.getenv("SERVICE_NAME", "sara_core")

    # Redis & Celery
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", os.getenv("BROKER_URL", REDIS_URL.rsplit("/", 1)[0] + "/0"))
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", REDIS_URL.rsplit("/", 1)[0] + "/1")
    CELERY_TIMEZONE: str = os.getenv("CELERY_TIMEZONE", "UTC")
    CELERY_TASK_ACKS_LATE: bool = _to_bool(os.getenv("CELERY_TASK_ACKS_LATE", "true"), True)
    CELERY_TASK_TRACK_STARTED: bool = _to_bool(os.getenv("CELERY_TASK_TRACK_STARTED", "true"), True)
    CELERY_WORKER_MAX_TASKS_PER_CHILD: int = _to_int(os.getenv("CELERY_WORKER_MAX_TASKS_PER_CHILD"), 200)
    CELERY_WORKER_CONCURRENCY: int = _to_int(os.getenv("CELERY_WORKER_CONCURRENCY"), 4)

    # Sentry
    SENTRY_DSN: str | None = os.getenv("SENTRY_DSN")
    SENTRY_ENABLED: bool = _to_bool(os.getenv("SENTRY_ENABLED", "true" if SENTRY_DSN else "false"), bool(SENTRY_DSN))
    SENTRY_TRACES_SAMPLE_RATE: float = float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.05"))

    # OpenAI / GPT
    OPENAI_API_KEY: str | None = os.getenv("OPENAI_API_KEY")
    OPENAI_BASE_URL: str | None = os.getenv("OPENAI_BASE_URL")  # optional override

    # Deepgram / ASR (with legacy DG_* mirrors)
    DEEPGRAM_API_KEY: str | None = os.getenv("DEEPGRAM_API_KEY") or os.getenv("DG_API_KEY")
    DEEPGRAM_BASE_URL: str = os.getenv("DEEPGRAM_BASE_URL", os.getenv("DG_BASE_URL", "https://api.deepgram.com"))
    DEEPGRAM_MODEL: str = os.getenv("DEEPGRAM_MODEL", os.getenv("DG_MODEL", "nova-2-general"))

    # TTS
    MAX_TTS_TEXT_LEN: int = _to_int(os.getenv("MAX_TTS_TEXT_LEN"), 1000)  # ← important knob used elsewhere
    TTS_VOICE: str = os.getenv("TTS_VOICE", "alloy")
    TTS_RATE: float = float(os.getenv("TTS_RATE", "1.0"))

    # Cloudflare R2
    R2_ACCOUNT_ID: str | None = os.getenv("R2_ACCOUNT_ID")
    R2_ACCESS_KEY_ID: str | None = os.getenv("R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY: str | None = os.getenv("R2_SECRET_ACCESS_KEY")
    R2_BUCKET: str | None = os.getenv("R2_BUCKET")
    R2_PUBLIC_BASE_URL: str | None = os.getenv("R2_PUBLIC_BASE_URL")  # cdn base if you expose files

    # HTTP defaults (used by R2 client / external calls)
    HTTP_CONNECT_TIMEOUT: int = _to_int(os.getenv("HTTP_CONNECT_TIMEOUT"), 10)
    HTTP_READ_TIMEOUT: int = _to_int(os.getenv("HTTP_READ_TIMEOUT"), 30)
    HTTP_MAX_POOL: int = _to_int(os.getenv("HTTP_MAX_POOL"), 64)

    # Logging (keep names your code references!)
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_JSON: bool = _to_bool(os.getenv("LOG_JSON", "true"), True)
    LOG_BUFFER_SIZE: int = _to_int(os.getenv("LOG_BUFFER_SIZE"), 1000)  # ← maps to log_buffer_size

    # Metrics
    PROMETHEUS_ENABLED: bool = _to_bool(os.getenv("PROMETHEUS_ENABLED", "true"), True)
    METRICS_NAMESPACE: str = os.getenv("METRICS_NAMESPACE", "sara")
    METRICS_PUSH_INTERVAL_SEC: int = _to_int(os.getenv("METRICS_PUSH_INTERVAL_SEC"), 5)
    METRICS_FALLBACK_TO_REDIS: bool = _to_bool(os.getenv("METRICS_FALLBACK_TO_REDIS", "true"), True)

    # Circuit Breaker defaults
    CB_FAIL_THRESHOLD: int = _to_int(os.getenv("CB_FAIL_THRESHOLD"), 5)
    CB_COOLDOWN_SECONDS: int = _to_int(os.getenv("CB_COOLDOWN_SECONDS"), 30)
    CB_HALF_OPEN_SUCCESS_THRESHOLD: int = _to_int(os.getenv("CB_HALF_OPEN_SUCCESS_THRESHOLD"), 3)

    # Streaming service limits
    MAX_CONCURRENT_STREAMS: int = _to_int(os.getenv("MAX_CONCURRENT_STREAMS"), 50)
    STREAM_HEALTH_INTERVAL_SEC: int = _to_int(os.getenv("STREAM_HEALTH_INTERVAL_SEC"), 5)

    # Misc
    ENV_MODE: str = os.getenv("ENV_MODE", "render" if "RENDER" in os.environ else "local")

    # Safe dict for debugging (secrets redacted)
    @classmethod
    def as_dict(cls) -> dict:
        return {
            "ENV": cls.ENV,
            "DEBUG": cls.DEBUG,
            "SERVICE_NAME": cls.SERVICE_NAME,
            "REDIS_URL": cls.REDIS_URL,
            "CELERY_BROKER_URL": cls.CELERY_BROKER_URL,
            "CELERY_RESULT_BACKEND": cls.CELERY_RESULT_BACKEND,
            "SENTRY_ENABLED": cls.SENTRY_ENABLED,
            "SENTRY_DSN": _redact(cls.SENTRY_DSN),
            "OPENAI_API_KEY": _redact(cls.OPENAI_API_KEY),
            "DEEPGRAM_API_KEY": _redact(cls.DEEPGRAM_API_KEY),
            "DEEPGRAM_BASE_URL": cls.DEEPGRAM_BASE_URL,
            "DEEPGRAM_MODEL": cls.DEEPGRAM_MODEL,
            "MAX_TTS_TEXT_LEN": cls.MAX_TTS_TEXT_LEN,
            "R2_ACCOUNT_ID": _redact(cls.R2_ACCOUNT_ID),
            "R2_ACCESS_KEY_ID": _redact(cls.R2_ACCESS_KEY_ID),
            "R2_SECRET_ACCESS_KEY": _redact(cls.R2_SECRET_ACCESS_KEY),
            "R2_BUCKET": cls.R2_BUCKET,
            "R2_PUBLIC_BASE_URL": cls.R2_PUBLIC_BASE_URL,
            "HTTP_CONNECT_TIMEOUT": cls.HTTP_CONNECT_TIMEOUT,
            "HTTP_READ_TIMEOUT": cls.HTTP_READ_TIMEOUT,
            "HTTP_MAX_POOL": cls.HTTP_MAX_POOL,
            "LOG_LEVEL": cls.LOG_LEVEL,
            "LOG_JSON": cls.LOG_JSON,
            "LOG_BUFFER_SIZE": cls.LOG_BUFFER_SIZE,
            "PROMETHEUS_ENABLED": cls.PROMETHEUS_ENABLED,
            "METRICS_NAMESPACE": cls.METRICS_NAMESPACE,
            "METRICS_PUSH_INTERVAL_SEC": cls.METRICS_PUSH_INTERVAL_SEC,
            "METRICS_FALLBACK_TO_REDIS": cls.METRICS_FALLBACK_TO_REDIS,
            "CB_FAIL_THRESHOLD": cls.CB_FAIL_THRESHOLD,
            "CB_COOLDOWN_SECONDS": cls.CB_COOLDOWN_SECONDS,
            "CB_HALF_OPEN_SUCCESS_THRESHOLD": cls.CB_HALF_OPEN_SUCCESS_THRESHOLD,
            "MAX_CONCURRENT_STREAMS": cls.MAX_CONCURRENT_STREAMS,
            "STREAM_HEALTH_INTERVAL_SEC": cls.STREAM_HEALTH_INTERVAL_SEC,
            "ENV_MODE": cls.ENV_MODE,
        }


# --------------------------------------------------------------------------------------
# Legacy aliases (so older code keeps working without edits)
# --------------------------------------------------------------------------------------

# Some modules use DG_* env names
DG_API_KEY = Config.DEEPGRAM_API_KEY
DG_BASE_URL = Config.DEEPGRAM_BASE_URL
DG_MODEL = Config.DEEPGRAM_MODEL

# Some modules import config as a module and read snake_case names.
# Mirror the commonly referenced attributes below to avoid AttributeErrors.
redis_url = Config.REDIS_URL
broker_url = Config.CELERY_BROKER_URL
result_backend = Config.CELERY_RESULT_BACKEND

sentry_dsn = Config.SENTRY_DSN
sentry_enabled = Config.SENTRY_ENABLED

openai_api_key = Config.OPENAI_API_KEY
openai_base_url = Config.OPENAI_BASE_URL

http_connect_timeout = Config.HTTP_CONNECT_TIMEOUT
http_read_timeout = Config.HTTP_READ_TIMEOUT
http_max_pool = Config.HTTP_MAX_POOL

log_level = Config.LOG_LEVEL
log_json = Config.LOG_JSON
log_buffer_size = Config.LOG_BUFFER_SIZE  # ← critical for celery_app import path

prometheus_enabled = Config.PROMETHEUS_ENABLED
metrics_namespace = Config.METRICS_NAMESPACE
metrics_push_interval_sec = Config.METRICS_PUSH_INTERVAL_SEC
metrics_fallback_to_redis = Config.METRICS_FALLBACK_TO_REDIS

cb_fail_threshold = Config.CB_FAIL_THRESHOLD
cb_cooldown_seconds = Config.CB_COOLDOWN_SECONDS
cb_half_open_success_threshold = Config.CB_HALF_OPEN_SUCCESS_THRESHOLD

max_concurrent_streams = Config.MAX_CONCURRENT_STREAMS
stream_health_interval_sec = Config.STREAM_HEALTH_INTERVAL_SEC

max_tts_text_len = Config.MAX_TTS_TEXT_LEN  # ← requested earlier for TTS
tts_voice = Config.TTS_VOICE
tts_rate = Config.TTS_RATE

env_mode = Config.ENV_MODE
service_name = Config.SERVICE_NAME
debug = Config.DEBUG

# Convenience Celery settings dict (imported by celery_app without side-effects)
def celery_settings() -> dict:
    return {
        "broker_url": Config.CELERY_BROKER_URL,
        "result_backend": Config.CELERY_RESULT_BACKEND,
        "task_acks_late": Config.CELERY_TASK_ACKS_LATE,
        "task_track_started": Config.CELERY_TASK_TRACK_STARTED,
        "worker_max_tasks_per_child": Config.CELERY_WORKER_MAX_TASKS_PER_CHILD,
        "worker_concurrency": Config.CELERY_WORKER_CONCURRENCY,
        "timezone": Config.CELERY_TIMEZONE,
    }


# --------------------------------------------------------------------------------------
# Optional: emit a tiny bootstrap log without importing project logging utils
# --------------------------------------------------------------------------------------
if _to_bool(os.getenv("CONFIG_DEBUG_SNAPSHOT"), False):
    try:
        import json, sys
        json.dump({"config_snapshot": Config.as_dict()}, sys.stdout, indent=2)
        print()  # newline
    except Exception:
        pass
