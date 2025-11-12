# path: config.py
# Production-ready configuration with backward-compat, safe defaults, and helpers.

from __future__ import annotations
import os
from typing import Any, Dict, List, Optional, Tuple


# ---------------------- env parsers (import-safe) ----------------------

def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    val = os.getenv(name)
    return val if val not in (None, "") else default


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
    # Why: prevent secrets exposure in logs.
    if not val:
        return val
    if len(val) <= 6:
        return "******"
    return val[:2] + "****" + val[-2:]


# ---------------------- canonical config ----------------------

class Config:
    """
    Single source of truth. Legacy names are preserved.
    Defaults tuned to observed prod values (Phase-12).
    """

    # ---- Legacy logging field (REQUIRED by existing imports) ----
    log_buffer_size: int = _env_int("LOG_BUFFER_SIZE", 8192)

    # ---- Phase-12 constraints ----
    MAX_TTS_TEXT_LEN: int = _env_int("MAX_TTS_TEXT_LEN", 1200)

    # ---- Service mode flags ----
    ENV_MODE: str = _env_str("ENV_MODE", "render") or "render"

    # ---- Redis / Celery URLs ----
    REDIS_URL: Optional[str] = _env_str("REDIS_URL")
    CELERY_BROKER_URL: Optional[str] = _env_str("CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND: Optional[str] = _env_str("CELERY_RESULT_BACKEND")

    # ---- Redis / circuit knobs ----
    REDIS_SOCKET_TIMEOUT: int = _env_int("REDIS_SOCKET_TIMEOUT", 5)
    REDIS_POOL_MAXSIZE: int = _env_int("REDIS_POOL_MAXSIZE", 20)
    CIRCUIT_FAIL_THRESHOLD: int = _env_int("CIRCUIT_FAIL_THRESHOLD", 5)
    CIRCUIT_COOLDOWN_SECS: int = _env_int("CIRCUIT_COOLDOWN_SECS", 30)
    CIRCUIT_HALFOPEN_MAX_CALLS: int = _env_int("CIRCUIT_HALFOPEN_MAX_CALLS", 10)

    # ---- Celery execution knobs ----
    CELERY_QUEUES: List[str] = _env_csv(
        "CELERY_QUEUES",
        ["default", "priority", "celery", "voice_pipeline", "tts", "run_tts", "ai_tasks"],
    )
    CELERY_TASK_ACKS_LATE: bool = _env_bool("CELERY_TASK_ACKS_LATE", True)
    CELERY_TASK_TIME_LIMIT: int = _env_int("CELERY_TASK_TIME_LIMIT", 60 * 10)  # hard limit
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

    # ---- R2 / object storage ----
    R2_ENDPOINT: Optional[str] = _env_str("R2_ENDPOINT")
    R2_ACCESS_KEY_ID: Optional[str] = _env_str("R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY: Optional[str] = _env_str("R2_SECRET_ACCESS_KEY")
    R2_BUCKET: Optional[str] = _env_str("R2_BUCKET")
    R2_CONNECT_TIMEOUT_SECS: int = _env_int("R2_CONNECT_TIMEOUT_SECS", 10)
    R2_READ_TIMEOUT_SECS: int = _env_int("R2_READ_TIMEOUT_SECS", 30)
    R2_POOL_MAXSIZE: int = _env_int("R2_POOL_MAXSIZE", 64)

    # ---- AI providers (names only; secrets stay in env) ----
    OPENAI_API_KEY: Optional[str] = _env_str("OPENAI_API_KEY")
    DEEPGRAM_API_KEY: Optional[str] = _env_str("DEEPGRAM_API_KEY")

    # ---- Safety caps ----
    DAILY_CALLS_SOFT_CAP: int = _env_int("DAILY_CALLS_SOFT_CAP", 300)

    # -------- Helpers (no heavy imports) --------

    @classmethod
    def get_celery_urls(cls) -> Tuple[str, str]:
        """
        Returns (broker_url, result_backend) with sensible fallbacks.
        """
        broker = cls.CELERY_BROKER_URL
        backend = cls.CELERY_RESULT_BACKEND
        if broker and backend:
            return broker, backend

        # Fallback to REDIS_URL/0 and /1 if explicit URLs not provided.
        redis = cls.REDIS_URL or "redis://localhost:6379"
        if not broker:
            broker = f"{redis.rstrip('/')}/0"
        if not backend:
            backend = f"{redis.rstrip('/')}/1"
        return broker, backend

    @classmethod
    def celery_settings(cls) -> Dict[str, Any]:
        """
        Minimal Celery settings dict; import-safe.
        """
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
            # Retry knobs exposed for tasks to reference
            "retry_policy_defaults": {
                "max_retries": cls.CELERY_RETRY_MAX,
                "retry_backoff": cls.CELERY_RETRY_BACKOFF,
                "retry_jitter": cls.CELERY_RETRY_JITTER,
            },
        }

    @classmethod
    def validate(cls, strict: bool = False) -> List[str]:
        """
        Validates critical wiring; returns list of problems.
        If strict=True and problems exist, raises ValueError.
        """
        problems: List[str] = []

        # Legacy surface
        if not isinstance(cls.log_buffer_size, int):
            problems.append("log_buffer_size must be int")

        # Broker/backend resolution
        broker, backend = cls.get_celery_urls()
        if not broker.startswith("redis://") and not broker.startswith("rediss://"):
            problems.append("CELERY_BROKER_URL/REDIS_URL must be redis/rediss")
        if not backend.startswith("redis://") and not backend.startswith("rediss://"):
            problems.append("CELERY_RESULT_BACKEND/REDIS_URL must be redis/rediss")

        # R2 presence (warn only; your app logs showed successful init)
        r2_missing = [n for n in ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")
                      if not getattr(cls, n)]
        if r2_missing:
            problems.append(f"R2 config incomplete: {', '.join(r2_missing)}")

        # AI keys (optional, but warn if features are enabled later)
        if not cls.OPENAI_API_KEY:
            problems.append("OPENAI_API_KEY not set")
        if not cls.DEEPGRAM_API_KEY:
            problems.append("DEEPGRAM_API_KEY not set")

        if strict and problems:
            raise ValueError("Config validation failed: " + "; ".join(problems))
        return problems

    @classmethod
    def dump_for_logging(cls) -> Dict[str, Any]:
        """
        Redacted snapshot for diagnostics.
        """
        broker, backend = cls.get_celery_urls()
        return {
            "env_mode": cls.ENV_MODE,
            "log_buffer_size": cls.log_buffer_size,
            "max_tts_text_len": cls.MAX_TTS_TEXT_LEN,
            "redis_socket_timeout": cls.REDIS_SOCKET_TIMEOUT,
            "redis_pool_maxsize": cls.REDIS_POOL_MAXSIZE,
            "circuit": {
                "fail_threshold": cls.CIRCUIT_FAIL_THRESHOLD,
                "cooldown_secs": cls.CIRCUIT_COOLDOWN_SECS,
                "halfopen_max_calls": cls.CIRCUIT_HALFOPEN_MAX_CALLS,
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
            "observability": {
                "structured_logging": cls.STRUCTURED_LOGGING_ENABLED,
                "sentry_enabled": cls.SENTRY_ENABLED,
                "sentry_dsn": _redact(cls.SENTRY_DSN),
                "prometheus_enabled": cls.PROMETHEUS_ENABLED,
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


# ---------------------- module-level aliases (back-compat) ----------------------
# Logging
LOG_BUFFER_SIZE = Config.log_buffer_size
log_buffer_size = Config.log_buffer_size

# Phase-12
MAX_TTS_TEXT_LEN = Config.MAX_TTS_TEXT_LEN

# Redis / Circuit
REDIS_URL = Config.REDIS_URL
REDIS_SOCKET_TIMEOUT = Config.REDIS_SOCKET_TIMEOUT
REDIS_POOL_MAXSIZE = Config.REDIS_POOL_MAXSIZE
CIRCUIT_FAIL_THRESHOLD = Config.CIRCUIT_FAIL_THRESHOLD
CIRCUIT_COOLDOWN_SECS = Config.CIRCUIT_COOLDOWN_SECS
CIRCUIT_HALFOPEN_MAX_CALLS = Config.CIRCUIT_HALFOPEN_MAX_CALLS

# Celery
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

# AI
OPENAI_API_KEY = Config.OPENAI_API_KEY
DEEPGRAM_API_KEY = Config.DEEPGRAM_API_KEY

# Misc
ENV_MODE = Config.ENV_MODE
DAILY_CALLS_SOFT_CAP = Config.DAILY_CALLS_SOFT_CAP

__all__ = [
    "Config",
    # legacy/module-level aliases
    "LOG_BUFFER_SIZE", "log_buffer_size",
    "MAX_TTS_TEXT_LEN",
    "REDIS_URL", "REDIS_SOCKET_TIMEOUT", "REDIS_POOL_MAXSIZE",
    "CIRCUIT_FAIL_THRESHOLD", "CIRCUIT_COOLDOWN_SECS", "CIRCUIT_HALFOPEN_MAX_CALLS",
    "CELERY_BROKER_URL", "CELERY_RESULT_BACKEND", "CELERY_QUEUES",
    "CELERY_TASK_ACKS_LATE", "CELERY_TASK_SOFT_TIME_LIMIT", "CELERY_TASK_TIME_LIMIT",
    "CELERY_RETRY_MAX", "CELERY_RETRY_BACKOFF", "CELERY_RETRY_JITTER", "CELERY_PREFETCH_MULTIPLIER",
    "STRUCTURED_LOGGING_ENABLED", "SENTRY_ENABLED", "SENTRY_DSN",
    "PROMETHEUS_ENABLED", "METRICS_FALLBACK_TO_REDIS",
    "R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET",
    "R2_CONNECT_TIMEOUT_SECS", "R2_READ_TIMEOUT_SECS", "R2_POOL_MAXSIZE",
    "OPENAI_API_KEY", "DEEPGRAM_API_KEY",
    "ENV_MODE", "DAILY_CALLS_SOFT_CAP",
]


# ---------------------- local smoke (optional) ----------------------
if __name__ == "__main__":
    # Prints legacy + derived URLs; zero side-effects.
    print(
        "OK",
        getattr(Config, "log_buffer_size", None),
        getattr(__import__("config"), "log_buffer_size", None),
        Config.get_celery_urls(),
    )
