# path: config.py
# Production-ready config with full backward-compat for legacy names + Phase-12 knobs.

from __future__ import annotations
import os
from typing import Any, Dict, List, Optional, Tuple


# ---------------------- env helpers ----------------------

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


# ---------------------- canonical config ----------------------

class Config:
    """
    Backward-compatible constants + Phase-12 additions.
    IMPORTANT: legacy snake_case + UPPERCASE names are preserved.
    """

    # ---- Logging (legacy) ----
    log_buffer_size: int = _env_int("LOG_BUFFER_SIZE", 8192)       # legacy required
    LOG_LEVEL: str = (_env_str("LOG_LEVEL", "INFO") or "INFO")     # expected by celery_app.py
    ENABLE_LOG_BUFFERING: bool = _env_bool("ENABLE_LOG_BUFFERING", True)
    LOG_JSON: bool = _env_bool("LOG_JSON", True)  # structured JSON logs on by default

    # ---- Phase-12 constraints ----
    MAX_TTS_TEXT_LEN: int = _env_int("MAX_TTS_TEXT_LEN", 1200)

    # ---- Service mode ----
    ENV_MODE: str = _env_str("ENV_MODE", "render") or "render"

    # ---- URLs / Redis / Celery ----
    REDIS_URL: Optional[str] = _env_str("REDIS_URL")
    CELERY_BROKER_URL: Optional[str] = _env_str("CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND: Optional[str] = _env_str("CELERY_RESULT_BACKEND")

    REDIS_SOCKET_TIMEOUT: int = _env_int("REDIS_SOCKET_TIMEOUT", 5)
    REDIS_POOL_MAXSIZE: int = _env_int("REDIS_POOL_MAXSIZE", 20)

    CIRCUIT_FAIL_THRESHOLD: int = _env_int("CIRCUIT_FAIL_THRESHOLD", 5)
    CIRCUIT_COOLDOWN_SECS: int = _env_int("CIRCUIT_COOLDOWN_SECS", 30)
    CIRCUIT_HALFOPEN_MAX_CALLS: int = _env_int("CIRCUIT_HALFOPEN_MAX_CALLS", 10)

    CELERY_QUEUES: List[str] = _env_csv(
        "CELERY_QUEUES",
        ["default", "priority", "celery", "voice_pipeline", "tts", "run_tts", "ai_tasks"],
    )
    CELERY_TASK_ACKS_LATE: bool = _env_bool("CELERY_TASK_ACKS_LATE", True)
    CELERY_TASK_TIME_LIMIT: int = _env_int("CELERY_TASK_TIME_LIMIT", 60 * 10)
    CELERY_TASK_SOFT_TIME_LIMIT: int = _env_int("CELERY_TASK_SOFT_TIME_LIMIT", 60 * 8)
    CELERY_RETRY_MAX: int = _env_int("CELERY_RETRY_MAX", 5)
    CELERY_RETRY_BACKOFF: bool = _env_bool("CELERY_RETRY_BACKOFF", True)
    CELERY_RETRY_JITTER: bool = _env_bool("CELERY_RETRY_JITTER", True)
    CELERY_PREFETCH_MULTIPLIER: int = _env_int("CELERY_PREFETCH_MULTIPLIER", 1)

    # ---- Observability ----
    STRUCTURED_LOGGING_ENABLED: bool = _env_bool("STRUCTURED_LOGGING_ENABLED", True)
    SENTRY_ENABLED: bool = _env_bool("SENTRY_ENABLED", True)
    SENTRY_DSN: Optional[str] = _env_str("SENTRY_DSN")
    PROMETHEUS_ENABLED: bool = _env_bool("PROMETHEUS_ENABLED", False)
    METRICS_FALLBACK_TO_REDIS: bool = _env_bool("METRICS_FALLBACK_TO_REDIS", True)

    # ---- R2 ----
    R2_ENDPOINT: Optional[str] = _env_str("R2_ENDPOINT")
    R2_ACCESS_KEY_ID: Optional[str] = _env_str("R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY: Optional[str] = _env_str("R2_SECRET_ACCESS_KEY")
    R2_BUCKET: Optional[str] = _env_str("R2_BUCKET")
    R2_CONNECT_TIMEOUT_SECS: int = _env_int("R2_CONNECT_TIMEOUT_SECS", 10)
    R2_READ_TIMEOUT_SECS: int = _env_int("R2_READ_TIMEOUT_SECS", 30)
    R2_POOL_MAXSIZE: int = _env_int("R2_POOL_MAXSIZE", 64)

    # ---- AI keys ----
    OPENAI_API_KEY: Optional[str] = _env_str("OPENAI_API_KEY")
    DEEPGRAM_API_KEY: Optional[str] = _env_str("DEEPGRAM_API_KEY")

    # ---- Safety ----
    DAILY_CALLS_SOFT_CAP: int = _env_int("DAILY_CALLS_SOFT_CAP", 300)

    # ---- Helpers ----
    @classmethod
    def get_celery_urls(cls) -> Tuple[str, str]:
        broker = cls.CELERY_BROKER_URL
        backend = cls.CELERY_RESULT_BACKEND
        if broker and backend:
            return broker, backend
        redis = cls.REDIS_URL or "redis://localhost:6379"
        return f"{redis.rstrip('/')}/0", f"{redis.rstrip('/')}/1"

    @classmethod
    def celery_settings(cls) -> Dict[str, Any]:
        broker, backend = cls.get_celery_urls()
        return {
            "broker_url": broker,
            "result_backend": backend,
            "task_acks_late": cls.CELERY_TASK_ACKS_LATE,
            "task_time_limit": cls.CELERY_TASK_TIME_LIMIT,
            "task_soft_time_limit": cls.CELERY_TASK_SOFT_TIME_LIMIT,
            "task_default_queue": cls.CELERY_QUEUES[0] if cls.CELERY_QUEUES else "default",
            "task_queues": cls.CELERY_QUEUES,
            "worker_prefetch_multiplier": cls.CELERY_PREFETCH_MULTIPLIER,
            "retry_policy_defaults": {
                "max_retries": cls.CELERY_RETRY_MAX,
                "retry_backoff": cls.CELERY_RETRY_BACKOFF,
                "retry_jitter": cls.CELERY_RETRY_JITTER,
            },
        }

    @classmethod
    def validate(cls, strict: bool = False) -> List[str]:
        problems: List[str] = []
        if not isinstance(cls.log_buffer_size, int):
            problems.append("log_buffer_size must be int")
        if not isinstance(cls.LOG_LEVEL, str):
            problems.append("LOG_LEVEL must be str")
        broker, backend = cls.get_celery_urls()
        if not (broker.startswith("redis://") or broker.startswith("rediss://")):
            problems.append("broker_url must be redis/rediss")
        if not (backend.startswith("redis://") or backend.startswith("rediss://")):
            problems.append("result_backend must be redis/rediss")
        if strict and problems:
            raise ValueError("Config validation failed: " + "; ".join(problems))
        return problems

    @classmethod
    def dump_for_logging(cls) -> Dict[str, Any]:
        broker, backend = cls.get_celery_urls()
        return {
            "env_mode": cls.ENV_MODE,
            "logging": {
                "log_buffer_size": cls.log_buffer_size,
                "LOG_BUFFER_SIZE": cls.log_buffer_size,
                "LOG_LEVEL": cls.LOG_LEVEL,
                "ENABLE_LOG_BUFFERING": cls.ENABLE_LOG_BUFFERING,
                "LOG_JSON": cls.LOG_JSON,
            },
            "celery": {
                "queues": cls.CELERY_QUEUES,
                "broker_url": broker,
                "result_backend": backend,
                "acks_late": cls.CELERY_TASK_ACKS_LATE,
                "soft_time_limit": cls.CELERY_TASK_SOFT_TIME_LIMIT,
                "time_limit": cls.CELERY_TASK_TIME_LIMIT,
                "prefetch_multiplier": cls.CELERY_PREFETCH_MULTIPLIER,
                "retry": {
                    "max": cls.CELERY_RETRY_MAX,
                    "backoff": cls.CELERY_RETRY_BACKOFF,
                    "jitter": cls.CELERY_RETRY_JITTER,
                },
            },
            "redis": {
                "url": cls.REDIS_URL,
                "socket_timeout": cls.REDIS_SOCKET_TIMEOUT,
                "pool_maxsize": cls.REDIS_POOL_MAXSIZE,
            },
            "circuit": {
                "fail_threshold": cls.CIRCUIT_FAIL_THRESHOLD,
                "cooldown_secs": cls.CIRCUIT_COOLDOWN_SECS,
                "halfopen_max_calls": cls.CIRCUIT_HALFOPEN_MAX_CALLS,
            },
            "r2": {
                "endpoint": cls.R2_ENDPOINT,
                "access_key_id": _redact(cls.R2_ACCESS_KEY_ID),
                "secret_key": _redact(cls.R2_SECRET_ACCESS_KEY),
                "bucket": cls.R2_BUCKET,
                "connect_timeout_s": cls.R2_CONNECT_TIMEOUT_SECS,
                "read_timeout_s": cls.R2_READ_TIMEOUT_SECS,
                "pool_maxsize": cls.R2_POOL_MAXSIZE,
            },
            "ai": {
                "openai_key": _redact(cls.OPENAI_API_KEY),
                "deepgram_key": _redact(cls.DEEPGRAM_API_KEY),
            },
            "caps": {"daily_calls_soft_cap": cls.DAILY_CALLS_SOFT_CAP},
        }


# ---------------------- CLASS-LEVEL aliases (for callers using Config.X) ----------------------
# Why: Some modules access UPPERCASE on the class; keep them identical to snake_case values.
Config.LOG_BUFFER_SIZE = Config.log_buffer_size      # type: ignore[attr-defined]
Config.log_level = Config.LOG_LEVEL                  # type: ignore[attr-defined]
Config.enable_log_buffering = Config.ENABLE_LOG_BUFFERING  # type: ignore[attr-defined]
Config.log_json = Config.LOG_JSON                    # type: ignore[attr-defined]

# ---------------------- MODULE-LEVEL aliases (legacy import styles) ----------------------
# Logging
LOG_BUFFER_SIZE = Config.log_buffer_size
log_buffer_size = Config.log_buffer_size
LOG_LEVEL = Config.LOG_LEVEL
log_level = Config.LOG_LEVEL
ENABLE_LOG_BUFFERING = Config.ENABLE_LOG_BUFFERING
enable_log_buffering = Config.ENABLE_LOG_BUFFERING
LOG_JSON = Config.LOG_JSON
log_json = Config.LOG_JSON

# Phase-12
MAX_TTS_TEXT_LEN = Config.MAX_TTS_TEXT_LEN

# URLs / Redis / Celery
REDIS_URL = Config.REDIS_URL
REDIS_SOCKET_TIMEOUT = Config.REDIS_SOCKET_TIMEOUT
REDIS_POOL_MAXSIZE = Config.REDIS_POOL_MAXSIZE

CIRCUIT_FAIL_THRESHOLD = Config.CIRCUIT_FAIL_THRESHOLD
CIRCUIT_COOLDOWN_SECS = Config.CIRCUIT_COOLDOWN_SECS
CIRCUIT_HALFOPEN_MAX_CALLS = Config.CIRCUIT_HALFOPEN_MAX_CALLS

CELERY_BROKER_URL = Config.CELERY_BROKER_URL
CELERY_RESULT_BACKEND = Config.CELERY_RESULT_BACKEND
CELERY_QUEUES = Config.CELERY_QUEUES
CELERY_TASK_ACKS_LATE = Config.CELERY_TASK_ACKS_LATE
CELERY_TASK_TIME_LIMIT = Config.CELERY_TASK_TIME_LIMIT
CELERY_TASK_SOFT_TIME_LIMIT = Config.CELERY_TASK_SOFT_TIME_LIMIT
CELERY_RETRY_MAX = Config.CELERY_RETRY_MAX
CELERY_RETRY_BACKOFF = Config.CELERY_RETRY_BACKOFF
CELERY_RETRY_JITTER = Config.CELERY_RETRY_JITTER
CELERY_PREFETCH_MULTIPLIER = Config.CELERY_PREFETCH_MULTIPLIER

# Observability
STRUCTURED_LOGGING_ENABLED = Config.STRUCTURED_LOGGING_ENABLED
SENTRY_ENABLED = Config.SENTRY_ENABLED
SENTRY_DSN = Config.SENTRY_DSN
PROMETHEUS_ENABLED = Config.PROMETHEUS_ENABLED
METRICS_FALLBACK_TO_REDIS = Config.METRICS_FALLBACK_TO_REDIS

# R2
R2_ENDPOINT = Config.R2_ENDPOINT
R2_ACCESS_KEY_ID = Config.R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY = Config.R2_SECRET_ACCESS_KEY
R2_BUCKET = Config.R2_BUCKET
R2_CONNECT_TIMEOUT_SECS = Config.R2_CONNECT_TIMEOUT_SECS
R2_READ_TIMEOUT_SECS = Config.R2_READ_TIMEOUT_SECS
R2_POOL_MAXSIZE = Config.R2_POOL_MAXSIZE

# AI / Misc
OPENAI_API_KEY = Config.OPENAI_API_KEY
DEEPGRAM_API_KEY = Config.DEEPGRAM_API_KEY
ENV_MODE = Config.ENV_MODE
DAILY_CALLS_SOFT_CAP = Config.DAILY_CALLS_SOFT_CAP

# Export `config` name for `from config import config`
config = Config  # class-as-config pattern

__all__ = [
    "Config", "config",
    # logging
    "LOG_BUFFER_SIZE", "log_buffer_size", "LOG_LEVEL", "log_level",
    "ENABLE_LOG_BUFFERING", "enable_log_buffering", "LOG_JSON", "log_json",
    # phase-12
    "MAX_TTS_TEXT_LEN",
    # urls / redis / celery
    "REDIS_URL", "REDIS_SOCKET_TIMEOUT", "REDIS_POOL_MAXSIZE",
    "CIRCUIT_FAIL_THRESHOLD", "CIRCUIT_COOLDOWN_SECS", "CIRCUIT_HALFOPEN_MAX_CALLS",
    "CELERY_BROKER_URL", "CELERY_RESULT_BACKEND", "CELERY_QUEUES",
    "CELERY_TASK_ACKS_LATE", "CELERY_TASK_SOFT_TIME_LIMIT", "CELERY_TASK_TIME_LIMIT",
    "CELERY_RETRY_MAX", "CELERY_RETRY_BACKOFF", "CELERY_RETRY_JITTER", "CELERY_PREFETCH_MULTIPLIER",
    # observability
    "STRUCTURED_LOGGING_ENABLED", "SENTRY_ENABLED", "SENTRY_DSN",
    "PROMETHEUS_ENABLED", "METRICS_FALLBACK_TO_REDIS",
    # r2
    "R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET",
    "R2_CONNECT_TIMEOUT_SECS", "R2_READ_TIMEOUT_SECS", "R2_POOL_MAXSIZE",
    # ai / misc
    "OPENAI_API_KEY", "DEEPGRAM_API_KEY", "ENV_MODE", "DAILY_CALLS_SOFT_CAP",
]

if __name__ == "__main__":
    # Quick smoke to ensure aliases resolve in all styles.
    print(
        "OK",
        Config.log_buffer_size,
        LOG_BUFFER_SIZE,
        getattr(config, "log_buffer_size", None),
        getattr(config, "LOG_BUFFER_SIZE", None),
        LOG_LEVEL,
        ENABLE_LOG_BUFFERING,
    )
