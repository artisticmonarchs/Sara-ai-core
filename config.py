# path: config.py
# Production-ready config with strict backward-compat and Phase-12 knobs.

from __future__ import annotations
import os
from typing import Any, Dict, List, Optional, Tuple

# ---- tiny env helpers (no heavy imports) ----
def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(float(raw))
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}

def _env_csv(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name)
    if not raw:
        return list(default)
    return [x.strip() for x in raw.split(",") if x.strip()]

def _redact(val: Optional[str]) -> Optional[str]:
    if not val:
        return val
    return ("******" if len(val) <= 6 else (val[:2] + "****" + val[-2:]))


class Config:
    """
    Backward-compatible config surface + Phase-12 settings.
    Exposes both UPPER_CASE and snake_case on the class.
    """

    # -------- Identity / Phase banner --------
    SERVICE_NAME: str = _env_str("SERVICE_NAME", "sara_core") or "sara_core"
    service_name: str = SERVICE_NAME  # legacy snake_case
    PHASE_VERSION: str = _env_str("PHASE_VERSION", "12-Stable") or "12-Stable"
    phase_version: str = PHASE_VERSION

    ENV: str = _env_str("ENV", _env_str("APP_ENV", "local")) or "local"
    DEBUG: bool = _env_bool("DEBUG", False)
    ENV_MODE: str = _env_str("ENV_MODE", "render" if "RENDER" in os.environ else "local") or "local"

    # -------- Redis / Celery --------
    REDIS_URL: str = _env_str("REDIS_URL", "redis://localhost:6379/0") or "redis://localhost:6379/0"
    CELERY_BROKER_URL: Optional[str] = _env_str("CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND: Optional[str] = _env_str("CELERY_RESULT_BACKEND")
    CELERY_TIMEZONE: str = _env_str("CELERY_TIMEZONE", "UTC") or "UTC"
    CELERY_TASK_ACKS_LATE: bool = _env_bool("CELERY_TASK_ACKS_LATE", True)
    CELERY_TASK_TRACK_STARTED: bool = _env_bool("CELERY_TASK_TRACK_STARTED", True)
    CELERY_WORKER_MAX_TASKS_PER_CHILD: int = _env_int("CELERY_WORKER_MAX_TASKS_PER_CHILD", 200)
    CELERY_WORKER_CONCURRENCY: int = _env_int("CELERY_WORKER_CONCURRENCY", 4)
    CELERY_QUEUES: List[str] = _env_csv(
        "CELERY_QUEUES",
        ["default", "priority", "celery", "voice_pipeline", "tts", "run_tts", "ai_tasks"],
    )
    CELERY_TASK_TIME_LIMIT: int = _env_int("CELERY_TASK_TIME_LIMIT", 60 * 10)
    CELERY_TASK_SOFT_TIME_LIMIT: int = _env_int("CELERY_TASK_SOFT_TIME_LIMIT", 60 * 8)
    CELERY_RETRY_MAX: int = _env_int("CELERY_RETRY_MAX", 5)
    CELERY_RETRY_BACKOFF: bool = _env_bool("CELERY_RETRY_BACKOFF", True)
    CELERY_RETRY_JITTER: bool = _env_bool("CELERY_RETRY_JITTER", True)
    CELERY_PREFETCH_MULTIPLIER: int = _env_int("CELERY_PREFETCH_MULTIPLIER", 1)

    # Derived (if not explicitly set)
    @classmethod
    def get_celery_urls(cls) -> Tuple[str, str]:
        broker = cls.CELERY_BROKER_URL
        backend = cls.CELERY_RESULT_BACKEND
        if broker and backend:
            return broker, backend
        base = cls.REDIS_URL.rsplit("/", 1)[0] if "/" in cls.REDIS_URL else cls.REDIS_URL
        return (broker or f"{base}/0", backend or f"{base}/1")

    # -------- Redis/circuit knobs --------
    REDIS_SOCKET_TIMEOUT: int = _env_int("REDIS_SOCKET_TIMEOUT", 5)
    REDIS_POOL_MAXSIZE: int = _env_int("REDIS_POOL_MAXSIZE", 20)
    CIRCUIT_FAIL_THRESHOLD: int = _env_int("CIRCUIT_FAIL_THRESHOLD", 5)
    CIRCUIT_COOLDOWN_SECS: int = _env_int("CIRCUIT_COOLDOWN_SECS", 30)
    CIRCUIT_HALFOPEN_MAX_CALLS: int = _env_int("CIRCUIT_HALFOPEN_MAX_CALLS", 10)

    # -------- Logging (critical legacy names) --------
    LOG_LEVEL: str = _env_str("LOG_LEVEL", "INFO") or "INFO"
    log_level: str = LOG_LEVEL
    LOG_JSON: bool = _env_bool("LOG_JSON", True)
    log_json: bool = LOG_JSON
    LOG_BUFFER_SIZE: int = _env_int("LOG_BUFFER_SIZE", 1000)
    log_buffer_size: int = LOG_BUFFER_SIZE
    ENABLE_LOG_BUFFERING: bool = _env_bool("ENABLE_LOG_BUFFERING", True)
    enable_log_buffering: bool = ENABLE_LOG_BUFFERING

    # -------- Observability --------
    SENTRY_DSN: Optional[str] = _env_str("SENTRY_DSN")
    SENTRY_ENABLED: bool = _env_bool("SENTRY_ENABLED", bool(SENTRY_DSN))
    SENTRY_TRACES_SAMPLE_RATE: float = _env_float("SENTRY_TRACES_SAMPLE_RATE", 0.05)
    STRUCTURED_LOGGING_ENABLED: bool = _env_bool("STRUCTURED_LOGGING_ENABLED", True)
    PROMETHEUS_ENABLED: bool = _env_bool("PROMETHEUS_ENABLED", True)
    METRICS_NAMESPACE: str = _env_str("METRICS_NAMESPACE", "sara") or "sara"
    METRICS_PUSH_INTERVAL_SEC: int = _env_int("METRICS_PUSH_INTERVAL_SEC", 5)
    METRICS_FALLBACK_TO_REDIS: bool = _env_bool("METRICS_FALLBACK_TO_REDIS", True)

    # -------- AI / TTS --------
    OPENAI_API_KEY: Optional[str] = _env_str("OPENAI_API_KEY")
    OPENAI_BASE_URL: Optional[str] = _env_str("OPENAI_BASE_URL")
    DEEPGRAM_API_KEY: Optional[str] = _env_str("DEEPGRAM_API_KEY") or _env_str("DG_API_KEY")
    DEEPGRAM_BASE_URL: str = _env_str("DEEPGRAM_BASE_URL", _env_str("DG_BASE_URL", "https://api.deepgram.com")) or "https://api.deepgram.com"
    DEEPGRAM_MODEL: str = _env_str("DEEPGRAM_MODEL", _env_str("DG_MODEL", "nova-2-general")) or "nova-2-general"

    MAX_TTS_TEXT_LEN: int = _env_int("MAX_TTS_TEXT_LEN", 1000)
    TTS_VOICE: str = _env_str("TTS_VOICE", "alloy") or "alloy"
    TTS_RATE: float = _env_float("TTS_RATE", 1.0)

    # -------- Cloudflare R2 --------
    R2_ACCOUNT_ID: Optional[str] = _env_str("R2_ACCOUNT_ID")
    R2_ACCESS_KEY_ID: Optional[str] = _env_str("R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY: Optional[str] = _env_str("R2_SECRET_ACCESS_KEY")
    R2_BUCKET: Optional[str] = _env_str("R2_BUCKET")
    R2_PUBLIC_BASE_URL: Optional[str] = _env_str("R2_PUBLIC_BASE_URL")
    HTTP_CONNECT_TIMEOUT: int = _env_int("HTTP_CONNECT_TIMEOUT", 10)
    HTTP_READ_TIMEOUT: int = _env_int("HTTP_READ_TIMEOUT", 30)
    HTTP_MAX_POOL: int = _env_int("HTTP_MAX_POOL", 64)

    # -------- Streaming limits --------
    MAX_CONCURRENT_STREAMS: int = _env_int("MAX_CONCURRENT_STREAMS", 50)
    STREAM_HEALTH_INTERVAL_SEC: int = _env_int("STREAM_HEALTH_INTERVAL_SEC", 5)

    # -------- Helpers --------
    @classmethod
    def celery_settings(cls) -> Dict[str, Any]:
        broker, backend = cls.get_celery_urls()
        return {
            "broker_url": broker,
            "result_backend": backend,
            "task_acks_late": cls.CELERY_TASK_ACKS_LATE,
            "task_track_started": cls.CELERY_TASK_TRACK_STARTED,
            "worker_max_tasks_per_child": cls.CELERY_WORKER_MAX_TASKS_PER_CHILD,
            "worker_concurrency": cls.CELERY_WORKER_CONCURRENCY,
            "timezone": cls.CELERY_TIMEZONE,
            "worker_prefetch_multiplier": cls.CELERY_PREFETCH_MULTIPLIER,
            "task_time_limit": cls.CELERY_TASK_TIME_LIMIT,
            "task_soft_time_limit": cls.CELERY_TASK_SOFT_TIME_LIMIT,
        }

    @classmethod
    def validate(cls, strict: bool = False) -> List[str]:
        problems: List[str] = []
        # legacy musts
        for attr in ("LOG_BUFFER_SIZE", "log_buffer_size", "LOG_LEVEL", "log_level",
                     "ENABLE_LOG_BUFFERING", "LOG_JSON", "SERVICE_NAME", "PHASE_VERSION"):
            if not hasattr(cls, attr):
                problems.append(f"missing {attr}")
        # URLs
        broker, backend = cls.get_celery_urls()
        if not (broker.startswith("redis://") or broker.startswith("rediss://")):
            problems.append("broker_url must be redis/rediss")
        if not (backend.startswith("redis://") or backend.startswith("rediss://")):
            problems.append("result_backend must be redis/rediss")
        if strict and problems:
            raise ValueError("Config validation failed: " + "; ".join(problems))
        return problems

    @classmethod
    def as_dict(cls) -> Dict[str, Any]:
        broker, backend = cls.get_celery_urls()
        return {
            "service_name": cls.SERVICE_NAME,
            "phase_version": cls.PHASE_VERSION,
            "env": cls.ENV, "env_mode": cls.ENV_MODE, "debug": cls.DEBUG,
            "logging": {
                "level": cls.LOG_LEVEL, "json": cls.LOG_JSON,
                "buffer_size": cls.LOG_BUFFER_SIZE, "buffering": cls.ENABLE_LOG_BUFFERING
            },
            "celery": {"broker": broker, "backend": backend, "queues": cls.CELERY_QUEUES},
            "redis": {"socket_timeout": cls.REDIS_SOCKET_TIMEOUT, "pool_maxsize": cls.REDIS_POOL_MAXSIZE},
            "circuit": {
                "fail_threshold": cls.CIRCUIT_FAIL_THRESHOLD,
                "cooldown_secs": cls.CIRCUIT_COOLDOWN_SECS,
                "halfopen_max_calls": cls.CIRCUIT_HALFOPEN_MAX_CALLS,
            },
            "obs": {
                "sentry_enabled": cls.SENTRY_ENABLED,
                "sentry_dsn": _redact(cls.SENTRY_DSN),
                "prometheus_enabled": cls.PROMETHEUS_ENABLED,
                "namespace": cls.METRICS_NAMESPACE,
            },
            "ai": {
                "openai_key": _redact(cls.OPENAI_API_KEY),
                "deepgram_key": _redact(cls.DEEPGRAM_API_KEY),
                "deepgram_model": cls.DEEPGRAM_MODEL,
            },
            "tts": {"max_len": cls.MAX_TTS_TEXT_LEN, "voice": cls.TTS_VOICE, "rate": cls.TTS_RATE},
            "r2": {
                "account": _redact(cls.R2_ACCOUNT_ID),
                "access_key": _redact(cls.R2_ACCESS_KEY_ID),
                "secret_key": _redact(cls.R2_SECRET_ACCESS_KEY),
                "bucket": cls.R2_BUCKET,
                "public_base_url": cls.R2_PUBLIC_BASE_URL,
                "http": {"connect": cls.HTTP_CONNECT_TIMEOUT, "read": cls.HTTP_READ_TIMEOUT, "pool": cls.HTTP_MAX_POOL},
            },
            "streaming": {"max_concurrent": cls.MAX_CONCURRENT_STREAMS, "health_interval": cls.STREAM_HEALTH_INTERVAL_SEC},
        }


# -------- module-level mirrors (legacy import styles) --------
# Identity / phase
SERVICE_NAME = Config.SERVICE_NAME
service_name = Config.service_name
PHASE_VERSION = Config.PHASE_VERSION
phase_version = Config.phase_version

# Logging
LOG_BUFFER_SIZE = Config.LOG_BUFFER_SIZE
log_buffer_size = Config.log_buffer_size
LOG_LEVEL = Config.LOG_LEVEL
log_level = Config.log_level
ENABLE_LOG_BUFFERING = Config.ENABLE_LOG_BUFFERING
enable_log_buffering = Config.enable_log_buffering
LOG_JSON = Config.LOG_JSON
log_json = Config.log_json

# Redis / Celery
REDIS_URL = Config.REDIS_URL
CELERY_BROKER_URL = Config.get_celery_urls()[0]
CELERY_RESULT_BACKEND = Config.get_celery_urls()[1]
CELERY_TIMEZONE = Config.CELERY_TIMEZONE
CELERY_TASK_ACKS_LATE = Config.CELERY_TASK_ACKS_LATE
CELERY_TASK_TRACK_STARTED = Config.CELERY_TASK_TRACK_STARTED
CELERY_WORKER_MAX_TASKS_PER_CHILD = Config.CELERY_WORKER_MAX_TASKS_PER_CHILD
CELERY_WORKER_CONCURRENCY = Config.CELERY_WORKER_CONCURRENCY
CELERY_QUEUES = Config.CELERY_QUEUES
CELERY_TASK_TIME_LIMIT = Config.CELERY_TASK_TIME_LIMIT
CELERY_TASK_SOFT_TIME_LIMIT = Config.CELERY_TASK_SOFT_TIME_LIMIT
CELERY_RETRY_MAX = Config.CELERY_RETRY_MAX
CELERY_RETRY_BACKOFF = Config.CELERY_RETRY_BACKOFF
CELERY_RETRY_JITTER = Config.CELERY_RETRY_JITTER
CELERY_PREFETCH_MULTIPLIER = Config.CELERY_PREFETCH_MULTIPLIER

# Circuit / Redis knobs
REDIS_SOCKET_TIMEOUT = Config.REDIS_SOCKET_TIMEOUT
REDIS_POOL_MAXSIZE = Config.REDIS_POOL_MAXSIZE
CIRCUIT_FAIL_THRESHOLD = Config.CIRCUIT_FAIL_THRESHOLD
CIRCUIT_COOLDOWN_SECS = Config.CIRCUIT_COOLDOWN_SECS
CIRCUIT_HALFOPEN_MAX_CALLS = Config.CIRCUIT_HALFOPEN_MAX_CALLS

# Observability
SENTRY_DSN = Config.SENTRY_DSN
SENTRY_ENABLED = Config.SENTRY_ENABLED
SENTRY_TRACES_SAMPLE_RATE = Config.SENTRY_TRACES_SAMPLE_RATE
STRUCTURED_LOGGING_ENABLED = Config.STRUCTURED_LOGGING_ENABLED
PROMETHEUS_ENABLED = Config.PROMETHEUS_ENABLED
METRICS_NAMESPACE = Config.METRICS_NAMESPACE
METRICS_PUSH_INTERVAL_SEC = Config.METRICS_PUSH_INTERVAL_SEC
METRICS_FALLBACK_TO_REDIS = Config.METRICS_FALLBACK_TO_REDIS

# AI / TTS
OPENAI_API_KEY = Config.OPENAI_API_KEY
OPENAI_BASE_URL = Config.OPENAI_BASE_URL
DEEPGRAM_API_KEY = Config.DEEPGRAM_API_KEY
DEEPGRAM_BASE_URL = Config.DEEPGRAM_BASE_URL
DEEPGRAM_MODEL = Config.DEEPGRAM_MODEL
MAX_TTS_TEXT_LEN = Config.MAX_TTS_TEXT_LEN
TTS_VOICE = Config.TTS_VOICE
TTS_RATE = Config.TTS_RATE

# R2 / HTTP
R2_ACCOUNT_ID = Config.R2_ACCOUNT_ID
R2_ACCESS_KEY_ID = Config.R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY = Config.R2_SECRET_ACCESS_KEY
R2_BUCKET = Config.R2_BUCKET
R2_PUBLIC_BASE_URL = Config.R2_PUBLIC_BASE_URL
HTTP_CONNECT_TIMEOUT = Config.HTTP_CONNECT_TIMEOUT
HTTP_READ_TIMEOUT = Config.HTTP_READ_TIMEOUT
HTTP_MAX_POOL = Config.HTTP_MAX_POOL

# Streaming
MAX_CONCURRENT_STREAMS = Config.MAX_CONCURRENT_STREAMS
STREAM_HEALTH_INTERVAL_SEC = Config.STREAM_HEALTH_INTERVAL_SEC

# Export config symbol for callers doing `from config import config`
config = Config  # class-as-config pattern, supports config.LOG_LEVEL and config.log_level

__all__ = [
    "Config", "config",
    # identity/phase
    "SERVICE_NAME", "service_name", "PHASE_VERSION", "phase_version",
    # logging
    "LOG_BUFFER_SIZE", "log_buffer_size", "LOG_LEVEL", "log_level",
    "ENABLE_LOG_BUFFERING", "enable_log_buffering", "LOG_JSON", "log_json",
    # redis/celery
    "REDIS_URL", "CELERY_BROKER_URL", "CELERY_RESULT_BACKEND", "CELERY_TIMEZONE",
    "CELERY_TASK_ACKS_LATE", "CELERY_TASK_TRACK_STARTED", "CELERY_WORKER_MAX_TASKS_PER_CHILD",
    "CELERY_WORKER_CONCURRENCY", "CELERY_QUEUES", "CELERY_TASK_TIME_LIMIT",
    "CELERY_TASK_SOFT_TIME_LIMIT", "CELERY_RETRY_MAX", "CELERY_RETRY_BACKOFF",
    "CELERY_RETRY_JITTER", "CELERY_PREFETCH_MULTIPLIER",
    # circuit/redis knobs
    "REDIS_SOCKET_TIMEOUT", "REDIS_POOL_MAXSIZE",
    "CIRCUIT_FAIL_THRESHOLD", "CIRCUIT_COOLDOWN_SECS", "CIRCUIT_HALFOPEN_MAX_CALLS",
    # obs
    "SENTRY_DSN", "SENTRY_ENABLED", "SENTRY_TRACES_SAMPLE_RATE", "STRUCTURED_LOGGING_ENABLED",
    "PROMETHEUS_ENABLED", "METRICS_NAMESPACE", "METRICS_PUSH_INTERVAL_SEC", "METRICS_FALLBACK_TO_REDIS",
    # ai/tts
    "OPENAI_API_KEY", "OPENAI_BASE_URL", "DEEPGRAM_API_KEY", "DEEPGRAM_BASE_URL", "DEEPGRAM_MODEL",
    "MAX_TTS_TEXT_LEN", "TTS_VOICE", "TTS_RATE",
    # r2/http
    "R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET", "R2_PUBLIC_BASE_URL",
    "HTTP_CONNECT_TIMEOUT", "HTTP_READ_TIMEOUT", "HTTP_MAX_POOL",
    # streaming
    "MAX_CONCURRENT_STREAMS", "STREAM_HEALTH_INTERVAL_SEC",
]

if __name__ == "__main__":
    print("CONFIG_SMOKE_OK", config.LOG_LEVEL, config.log_level, config.LOG_BUFFER_SIZE, config.log_buffer_size,
          config.SERVICE_NAME, config.PHASE_VERSION, Config.validate(strict=False))
