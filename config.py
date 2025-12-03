# /app/config.py
"""
config.py — Phase 12 (Stable Dual-Mode)
Centralized configuration for Sara AI Core (Celery + App + Streaming).

Why these changes:
- Restore all legacy/public names your code imports at module import time
  (PHASE_VERSION, CELERY_* serializer knobs, ENABLE_* logging flags, etc.).
- Export `config` symbol (class alias) so `from config import config` works.
- Provide upper AND lower-case module-level aliases for backward-compat.
"""

from __future__ import annotations
import os
from pathlib import Path

# Optional: dotenv (no hard dependency)
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # library not installed in container
    def load_dotenv(*_args, **_kwargs):
        return False

# -----------------------------------------------------------------------------
# .env loading (order: .env -> .env.local)
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR / ".env.local")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _to_bool(val: str | None, default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}

def _to_int(val: str | None, default: int) -> int:
    try:
        return int(str(val).strip()) if val is not None else default
    except Exception:
        return default

def _to_float(val: str | None, default: float) -> float:
    try:
        return float(str(val).strip()) if val is not None else default
    except Exception:
        return default

def _csv_list(val: str | None, default: list[str]) -> list[str]:
    if not val:
        return list(default)
    return [x.strip() for x in val.split(",") if x.strip()]

def _redact(value: str | None) -> str | None:
    if not value:
        return value
    return (value[:4] + "…" + value[-4:]) if len(value) > 8 else "****"


# -----------------------------------------------------------------------------
# Core Config (class attributes -> fast import, no side-effects)
# -----------------------------------------------------------------------------
class Config:
    # Identity / env
    ENV: str = os.getenv("ENV", os.getenv("APP_ENV", "local"))
    ENV_MODE: str = os.getenv("ENV_MODE", os.getenv("RENDER", "render" if os.getenv("RENDER") else "local"))
    DEBUG: bool = _to_bool(os.getenv("DEBUG"), False)
    SERVICE_NAME: str = os.getenv("SERVICE_NAME", "sara-ai-core")
    PHASE_VERSION: str = os.getenv("PHASE_VERSION", "12-Stable")

    # Redis & Celery
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    _redis_base = REDIS_URL.rsplit("/", 1)[0] if "/" in REDIS_URL else REDIS_URL
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", os.getenv("BROKER_URL", f"{_redis_base}/0"))
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", f"{_redis_base}/1")
    CELERY_TIMEZONE: str = os.getenv("CELERY_TIMEZONE", "UTC")
    CELERY_ENABLE_UTC: bool = _to_bool(os.getenv("CELERY_ENABLE_UTC", "true"), True)
    CELERY_WORKER_CONCURRENCY: int = _to_int(os.getenv("CELERY_WORKER_CONCURRENCY"), 4)
    CELERY_TASK_ACKS_LATE: bool = _to_bool(os.getenv("CELERY_TASK_ACKS_LATE", "true"), True)
    CELERY_TASK_TRACK_STARTED: bool = _to_bool(os.getenv("CELERY_TASK_TRACK_STARTED", "true"), True)
    # Serializers/content (these were missing in your logs)
    CELERY_TASK_SERIALIZER: str = os.getenv("CELERY_TASK_SERIALIZER", "json")
    CELERY_RESULT_SERIALIZER: str = os.getenv("CELERY_RESULT_SERIALIZER", "json")
    CELERY_ACCEPT_CONTENT: list[str] = _csv_list(os.getenv("CELERY_ACCEPT_CONTENT"), ["json"])

    # Sentry
    SENTRY_DSN: str | None = os.getenv("SENTRY_DSN")
    SENTRY_ENVIRONMENT: str = os.getenv("SENTRY_ENVIRONMENT", ENV_MODE)
    SENTRY_TRACES_SAMPLE_RATE: float = float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.05"))

    # OpenAI / GPT
    OPENAI_API_KEY: str | None = os.getenv("OPENAI_API_KEY")
    OPENAI_BASE_URL: str | None = os.getenv("OPENAI_BASE_URL")

    # Deepgram / ASR (legacy mirrors)
    DEEPGRAM_API_KEY: str | None = os.getenv("DEEPGRAM_API_KEY") or os.getenv("DG_API_KEY")
    DEEPGRAM_BASE_URL: str = os.getenv("DEEPGRAM_BASE_URL", os.getenv("DG_BASE_URL", "https://api.deepgram.com"))
    DEEPGRAM_MODEL: str = os.getenv("DEEPGRAM_MODEL", os.getenv("DG_MODEL", "nova-2-general"))

    # TTS
    MAX_TTS_TEXT_LEN: int = _to_int(os.getenv("MAX_TTS_TEXT_LEN"), 1200)
    TTS_VOICE: str = os.getenv("TTS_VOICE", "alloy")
    TTS_RATE: float = float(os.getenv("TTS_RATE", "1.0"))

    # Cloudflare R2
    R2_ACCOUNT_ID: str | None = os.getenv("R2_ACCOUNT_ID")
    R2_ACCESS_KEY_ID: str | None = os.getenv("R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY: str | None = os.getenv("R2_SECRET_ACCESS_KEY")
    R2_BUCKET: str | None = os.getenv("R2_BUCKET")
    R2_PUBLIC_BASE_URL: str | None = os.getenv("R2_PUBLIC_BASE_URL")

    # HTTP defaults (R2/externals)
    HTTP_CONNECT_TIMEOUT: int = _to_int(os.getenv("HTTP_CONNECT_TIMEOUT"), 10)
    HTTP_READ_TIMEOUT: int = _to_int(os.getenv("HTTP_READ_TIMEOUT"), 30)
    HTTP_MAX_POOL: int = _to_int(os.getenv("HTTP_MAX_POOL"), 64)

    # Logging (names your code references)
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    ENABLE_STRUCTURED_LOGGING: bool = _to_bool(os.getenv("ENABLE_STRUCTURED_LOGGING", "true"), True)
    ENABLE_LOG_BUFFERING: bool = _to_bool(os.getenv("ENABLE_LOG_BUFFERING", "true"), True)
    ENABLE_SENSITIVE_DATA_MASKING: bool = _to_bool(os.getenv("ENABLE_SENSITIVE_DATA_MASKING", "true"), True)
    LOG_BUFFER_SIZE: int = _to_int(os.getenv("LOG_BUFFER_SIZE"), 1000)
    LOG_FLUSH_INTERVAL: int = _to_int(os.getenv("LOG_FLUSH_INTERVAL"), 30)

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

    # Twilio MediaStream Configuration - NEW
    TWILIO_MEDIA_WS_URL: str = os.getenv("TWILIO_MEDIA_WS_URL", 
                                         f"{os.getenv('APP_BASE_URL', 'wss://your-streaming-server.example.com').replace('https://', 'wss://').replace('http://', 'ws://')}/media")
    DUPLEX_STREAMING_ENABLED: bool = _to_bool(os.getenv("DUPLEX_STREAMING_ENABLED", "true"), True)
    
    # MediaStream buffer and queue settings - NEW
    MEDIA_STREAM_BUFFER_MS: int = _to_int(os.getenv("MEDIA_STREAM_BUFFER_MS"), 200)
    MEDIA_STREAM_RECONNECT_ATTEMPTS: int = _to_int(os.getenv("MEDIA_STREAM_RECONNECT_ATTEMPTS"), 3)
    MEDIA_STREAM_RECONNECT_DELAY: float = _to_float(os.getenv("MEDIA_STREAM_RECONNECT_DELAY"), 1.0)
    MEDIA_STREAM_SILENCE_FRAME_MS: int = _to_int(os.getenv("MEDIA_STREAM_SILENCE_FRAME_MS"), 20)
    MEDIA_STREAM_MAX_QUEUE_SIZE: int = _to_int(os.getenv("MEDIA_STREAM_MAX_QUEUE_SIZE"), 100)
    MEDIA_STREAM_PING_INTERVAL: int = _to_int(os.getenv("MEDIA_STREAM_PING_INTERVAL"), 30)
    WEBSOCKET_OPERATION_TIMEOUT: float = _to_float(os.getenv("WEBSOCKET_OPERATION_TIMEOUT"), 10.0)

    # Duplex Controller Settings - NEW
    CALL_STATE_TTL: int = _to_int(os.getenv("CALL_STATE_TTL"), 3600)
    MAX_CONVERSATION_TURNS: int = _to_int(os.getenv("MAX_CONVERSATION_TURNS"), 20)
    MAX_CONSECUTIVE_ERRORS: int = _to_int(os.getenv("MAX_CONSECUTIVE_ERRORS"), 5)
    INTERRUPTION_DEBOUNCE_MS: int = _to_int(os.getenv("INTERRUPTION_DEBOUNCE_MS"), 150)
    TTS_CANCEL_TIMEOUT: float = _to_float(os.getenv("TTS_CANCEL_TIMEOUT"), 0.15)

    # Outbound Dialer Settings - NEW
    DIAL_TIMEOUT_SECONDS: int = _to_int(os.getenv("DIAL_TIMEOUT_SECONDS"), 30)
    
    # Webhook URLs for Twilio callbacks - NEW (fixes missing attributes)
    VOICE_WEBHOOK_BASE: str = os.getenv("VOICE_WEBHOOK_BASE", os.getenv("APP_URL", "https://your-app-url.example.com"))
    STATUS_WEBHOOK_BASE: str = os.getenv("STATUS_WEBHOOK_BASE", os.getenv("APP_URL", "https://your-app-url.example.com"))

    # Outbound Dialer Settings - MISSING ATTRIBUTES (FIX)
    MAX_CONCURRENT_CALLS: int = _to_int(os.getenv("MAX_CONCURRENT_CALLS"), 10)
    RATE_LIMIT_CALLS_PER_MINUTE: int = _to_int(os.getenv("RATE_LIMIT_CALLS_PER_MINUTE"), 60)
    RETRY_ATTEMPTS: int = _to_int(os.getenv("RETRY_ATTEMPTS"), 2)
    RETRY_DELAY_SECONDS: int = _to_int(os.getenv("RETRY_DELAY_SECONDS", os.getenv("RETRY_BACKOFF_SECONDS", "1")), 1)  # FIX: Fixed env var name
    TIMEZONE_CHECK_ENABLED: bool = _to_bool(os.getenv("TIMEZONE_CHECK_ENABLED", "true"), True)
    DNC_CHECK_ENABLED: bool = _to_bool(os.getenv("DNC_CHECK_ENABLED", "true"), True)

    # NEW: Voice pipeline specific settings - ADDED MISSING ATTRIBUTES
    SARA_ENV: str = os.getenv("SARA_ENV", "prod")
    PARTIAL_THROTTLE_SECONDS: float = _to_float(os.getenv("PARTIAL_THROTTLE_SECONDS"), 0.6)
    INFERENCE_TASK_NAME: str = os.getenv("INFERENCE_TASK_NAME", "gpt_inference")
    EVENT_TASK_NAME: str = os.getenv("EVENT_TASK_NAME", "voice_event")
    CELERY_VOICE_QUEUE: str = os.getenv("CELERY_VOICE_QUEUE", "voice")
    CELERY_MAX_RETRIES: int = _to_int(os.getenv("CELERY_MAX_RETRIES"), 3)
    VOICE_PIPELINE_PORT: int = _to_int(os.getenv("VOICE_PIPELINE_PORT"), 8080)

    # Snapshot for debug (no secrets in clear)
    @classmethod
    def as_dict(cls) -> dict:
        return {
            "ENV": cls.ENV,
            "ENV_MODE": cls.ENV_MODE,
            "DEBUG": cls.DEBUG,
            "SERVICE_NAME": cls.SERVICE_NAME,
            "PHASE_VERSION": cls.PHASE_VERSION,
            "REDIS_URL": cls.REDIS_URL,
            "CELERY_BROKER_URL": cls.CELERY_BROKER_URL,
            "CELERY_RESULT_BACKEND": cls.CELERY_RESULT_BACKEND,
            "CELERY_TIMEZONE": cls.CELERY_TIMEZONE,
            "CELERY_ENABLE_UTC": cls.CELERY_ENABLE_UTC,
            "CELERY_WORKER_CONCURRENCY": cls.CELERY_WORKER_CONCURRENCY,
            "CELERY_TASK_SERIALIZER": cls.CELERY_TASK_SERIALIZER,
            "CELERY_RESULT_SERIALIZER": cls.CELERY_RESULT_SERIALIZER,
            "CELERY_ACCEPT_CONTENT": cls.CELERY_ACCEPT_CONTENT,
            "SENTRY_DSN": _redact(cls.SENTRY_DSN),
            "OPENAI_API_KEY": _redact(cls.OPENAI_API_KEY),
            "DEEPGRAM_API_KEY": _redact(cls.DEEPGRAM_API_KEY),
            "R2_ACCESS_KEY_ID": _redact(cls.R2_ACCESS_KEY_ID),
            "R2_SECRET_ACCESS_KEY": _redact(cls.R2_SECRET_ACCESS_KEY),
            "R2_BUCKET": cls.R2_BUCKET,
            "HTTP_CONNECT_TIMEOUT": cls.HTTP_CONNECT_TIMEOUT,
            "HTTP_READ_TIMEOUT": cls.HTTP_READ_TIMEOUT,
            "HTTP_MAX_POOL": cls.HTTP_MAX_POOL,
            "LOG_LEVEL": cls.LOG_LEVEL,
            "ENABLE_STRUCTURED_LOGGING": cls.ENABLE_STRUCTURED_LOGGING,
            "ENABLE_LOG_BUFFERING": cls.ENABLE_LOG_BUFFERING,
            "ENABLE_SENSITIVE_DATA_MASKING": cls.ENABLE_SENSITIVE_DATA_MASKING,
            "LOG_BUFFER_SIZE": cls.LOG_BUFFER_SIZE,
            "LOG_FLUSH_INTERVAL": cls.LOG_FLUSH_INTERVAL,
            "PROMETHEUS_ENABLED": cls.PROMETHEUS_ENABLED,
            "METRICS_NAMESPACE": cls.METRICS_NAMESPACE,
            "METRICS_PUSH_INTERVAL_SEC": cls.METRICS_PUSH_INTERVAL_SEC,
            "METRICS_FALLBACK_TO_REDIS": cls.METRICS_FALLBACK_TO_REDIS,
            "CB_FAIL_THRESHOLD": cls.CB_FAIL_THRESHOLD,
            "CB_COOLDOWN_SECONDS": cls.CB_COOLDOWN_SECONDS,
            "CB_HALF_OPEN_SUCCESS_THRESHOLD": cls.CB_HALF_OPEN_SUCCESS_THRESHOLD,
            "MAX_CONCURRENT_STREAMS": cls.MAX_CONCURRENT_STREAMS,
            "STREAM_HEALTH_INTERVAL_SEC": cls.STREAM_HEALTH_INTERVAL_SEC,
            # NEW: Twilio MediaStream configs
            "TWILIO_MEDIA_WS_URL": cls.TWILIO_MEDIA_WS_URL,
            "DUPLEX_STREAMING_ENABLED": cls.DUPLEX_STREAMING_ENABLED,
            "MEDIA_STREAM_BUFFER_MS": cls.MEDIA_STREAM_BUFFER_MS,
            "MEDIA_STREAM_RECONNECT_ATTEMPTS": cls.MEDIA_STREAM_RECONNECT_ATTEMPTS,
            "MEDIA_STREAM_RECONNECT_DELAY": cls.MEDIA_STREAM_RECONNECT_DELAY,
            "MEDIA_STREAM_SILENCE_FRAME_MS": cls.MEDIA_STREAM_SILENCE_FRAME_MS,
            "MEDIA_STREAM_MAX_QUEUE_SIZE": cls.MEDIA_STREAM_MAX_QUEUE_SIZE,
            "MEDIA_STREAM_PING_INTERVAL": cls.MEDIA_STREAM_PING_INTERVAL,
            "WEBSOCKET_OPERATION_TIMEOUT": cls.WEBSOCKET_OPERATION_TIMEOUT,
            # NEW: Duplex Controller configs
            "CALL_STATE_TTL": cls.CALL_STATE_TTL,
            "MAX_CONVERSATION_TURNS": cls.MAX_CONVERSATION_TURNS,
            "MAX_CONSECUTIVE_ERRORS": cls.MAX_CONSECUTIVE_ERRORS,
            "INTERRUPTION_DEBOUNCE_MS": cls.INTERRUPTION_DEBOUNCE_MS,
            "TTS_CANCEL_TIMEOUT": cls.TTS_CANCEL_TIMEOUT,
            # NEW: Outbound Dialer configs
            "DIAL_TIMEOUT_SECONDS": cls.DIAL_TIMEOUT_SECONDS,
            # NEW: Webhook URL configs
            "VOICE_WEBHOOK_BASE": cls.VOICE_WEBHOOK_BASE,
            "STATUS_WEBHOOK_BASE": cls.STATUS_WEBHOOK_BASE,
            # NEW: Missing Outbound Dialer configs
            "MAX_CONCURRENT_CALLS": cls.MAX_CONCURRENT_CALLS,
            "RATE_LIMIT_CALLS_PER_MINUTE": cls.RATE_LIMIT_CALLS_PER_MINUTE,
            "RETRY_ATTEMPTS": cls.RETRY_ATTEMPTS,
            "RETRY_DELAY_SECONDS": cls.RETRY_DELAY_SECONDS,
            "TIMEZONE_CHECK_ENABLED": cls.TIMEZONE_CHECK_ENABLED,
            "DNC_CHECK_ENABLED": cls.DNC_CHECK_ENABLED,
            # NEW: Voice pipeline configs - ADDED MISSING ATTRIBUTES
            "SARA_ENV": cls.SARA_ENV,
            "PARTIAL_THROTTLE_SECONDS": cls.PARTIAL_THROTTLE_SECONDS,
            "INFERENCE_TASK_NAME": cls.INFERENCE_TASK_NAME,
            "EVENT_TASK_NAME": cls.EVENT_TASK_NAME,
            "CELERY_VOICE_QUEUE": cls.CELERY_VOICE_QUEUE,
            "CELERY_MAX_RETRIES": cls.CELERY_MAX_RETRIES,
            "VOICE_PIPELINE_PORT": cls.VOICE_PIPELINE_PORT,
        }


# -----------------------------------------------------------------------------
# Export `config` (what your modules import) and module-level aliases
# -----------------------------------------------------------------------------
# `from config import config` imports this symbol (class-level attr access is intentional).
config = Config

# Upper-case mirrors (explicit for things your code uses)
SERVICE_NAME = Config.SERVICE_NAME
ENV_MODE = Config.ENV_MODE
PHASE_VERSION = Config.PHASE_VERSION

LOG_LEVEL = Config.LOG_LEVEL
LOG_BUFFER_SIZE = Config.LOG_BUFFER_SIZE
ENABLE_STRUCTURED_LOGGING = Config.ENABLE_STRUCTURED_LOGGING
ENABLE_LOG_BUFFERING = Config.ENABLE_LOG_BUFFERING
ENABLE_SENSITIVE_DATA_MASKING = Config.ENABLE_SENSITIVE_DATA_MASKING
LOG_FLUSH_INTERVAL = Config.LOG_FLUSH_INTERVAL

CELERY_BROKER_URL = Config.CELERY_BROKER_URL
CELERY_RESULT_BACKEND = Config.CELERY_RESULT_BACKEND
CELERY_TIMEZONE = Config.CELERY_TIMEZONE
CELERY_ENABLE_UTC = Config.CELERY_ENABLE_UTC
CELERY_WORKER_CONCURRENCY = Config.CELERY_WORKER_CONCURRENCY
CELERY_TASK_SERIALIZER = Config.CELERY_TASK_SERIALIZER
CELERY_RESULT_SERIALIZER = Config.CELERY_RESULT_SERIALIZER
CELERY_ACCEPT_CONTENT = Config.CELERY_ACCEPT_CONTENT

SENTRY_DSN = Config.SENTRY_DSN

# NEW: Twilio MediaStream upper-case aliases
TWILIO_MEDIA_WS_URL = Config.TWILIO_MEDIA_WS_URL
DUPLEX_STREAMING_ENABLED = Config.DUPLEX_STREAMING_ENABLED
MEDIA_STREAM_BUFFER_MS = Config.MEDIA_STREAM_BUFFER_MS
MEDIA_STREAM_RECONNECT_ATTEMPTS = Config.MEDIA_STREAM_RECONNECT_ATTEMPTS
MEDIA_STREAM_RECONNECT_DELAY = Config.MEDIA_STREAM_RECONNECT_DELAY
MEDIA_STREAM_SILENCE_FRAME_MS = Config.MEDIA_STREAM_SILENCE_FRAME_MS
MEDIA_STREAM_MAX_QUEUE_SIZE = Config.MEDIA_STREAM_MAX_QUEUE_SIZE
MEDIA_STREAM_PING_INTERVAL = Config.MEDIA_STREAM_PING_INTERVAL
WEBSOCKET_OPERATION_TIMEOUT = Config.WEBSOCKET_OPERATION_TIMEOUT

# NEW: Duplex Controller upper-case aliases
CALL_STATE_TTL = Config.CALL_STATE_TTL
MAX_CONVERSATION_TURNS = Config.MAX_CONVERSATION_TURNS
MAX_CONSECUTIVE_ERRORS = Config.MAX_CONSECUTIVE_ERRORS
INTERRUPTION_DEBOUNCE_MS = Config.INTERRUPTION_DEBOUNCE_MS
TTS_CANCEL_TIMEOUT = Config.TTS_CANCEL_TIMEOUT

# NEW: Outbound Dialer upper-case aliases
DIAL_TIMEOUT_SECONDS = Config.DIAL_TIMEOUT_SECONDS

# NEW: Webhook URL upper-case aliases
VOICE_WEBHOOK_BASE = Config.VOICE_WEBHOOK_BASE
STATUS_WEBHOOK_BASE = Config.STATUS_WEBHOOK_BASE

# NEW: Missing Outbound Dialer upper-case aliases
MAX_CONCURRENT_CALLS = Config.MAX_CONCURRENT_CALLS
RATE_LIMIT_CALLS_PER_MINUTE = Config.RATE_LIMIT_CALLS_PER_MINUTE
RETRY_ATTEMPTS = Config.RETRY_ATTEMPTS
RETRY_DELAY_SECONDS = Config.RETRY_DELAY_SECONDS
TIMEZONE_CHECK_ENABLED = Config.TIMEZONE_CHECK_ENABLED
DNC_CHECK_ENABLED = Config.DNC_CHECK_ENABLED

# NEW: Voice pipeline upper-case aliases - ADDED MISSING ATTRIBUTES
SARA_ENV = Config.SARA_ENV
PARTIAL_THROTTLE_SECONDS = Config.PARTIAL_THROTTLE_SECONDS
INFERENCE_TASK_NAME = Config.INFERENCE_TASK_NAME
EVENT_TASK_NAME = Config.EVENT_TASK_NAME
CELERY_VOICE_QUEUE = Config.CELERY_VOICE_QUEUE
CELERY_MAX_RETRIES = Config.CELERY_MAX_RETRIES
VOICE_PIPELINE_PORT = Config.VOICE_PIPELINE_PORT

# Lower-case aliases for legacy call-sites (defensive; you had errors like `phase_version`, `log_level`, etc.)
service_name = SERVICE_NAME
env_mode = ENV_MODE
phase_version = PHASE_VERSION

log_level = LOG_LEVEL
log_buffer_size = LOG_BUFFER_SIZE
enable_structured_logging = ENABLE_STRUCTURED_LOGGING
enable_log_buffering = ENABLE_LOG_BUFFERING
enable_sensitive_data_masking = ENABLE_SENSITIVE_DATA_MASKING
log_flush_interval = LOG_FLUSH_INTERVAL

celery_broker_url = CELERY_BROKER_URL
celery_result_backend = CELERY_RESULT_BACKEND
celery_timezone = CELERY_TIMEZONE
celery_enable_utc = CELERY_ENABLE_UTC
celery_worker_concurrency = CELERY_WORKER_CONCURRENCY
celery_task_serializer = CELERY_TASK_SERIALIZER
celery_result_serializer = CELERY_RESULT_SERIALIZER
celery_accept_content = CELERY_ACCEPT_CONTENT

# NEW: Twilio MediaStream lower-case aliases
twilio_media_ws_url = TWILIO_MEDIA_WS_URL
duplex_streaming_enabled = DUPLEX_STREAMING_ENABLED
media_stream_buffer_ms = MEDIA_STREAM_BUFFER_MS
media_stream_reconnect_attempts = MEDIA_STREAM_RECONNECT_ATTEMPTS
media_stream_reconnect_delay = MEDIA_STREAM_RECONNECT_DELAY
media_stream_silence_frame_ms = MEDIA_STREAM_SILENCE_FRAME_MS
media_stream_max_queue_size = MEDIA_STREAM_MAX_QUEUE_SIZE
media_stream_ping_interval = MEDIA_STREAM_PING_INTERVAL
websocket_operation_timeout = WEBSOCKET_OPERATION_TIMEOUT

# NEW: Duplex Controller lower-case aliases
call_state_ttl = CALL_STATE_TTL
max_conversation_turns = MAX_CONVERSATION_TURNS
max_consecutive_errors = MAX_CONSECUTIVE_ERRORS
interruption_debounce_ms = INTERRUPTION_DEBOUNCE_MS
tts_cancel_timeout = TTS_CANCEL_TIMEOUT

# NEW: Outbound Dialer lower-case aliases
dial_timeout_seconds = DIAL_TIMEOUT_SECONDS

# NEW: Webhook URL lower-case aliases
voice_webhook_base = VOICE_WEBHOOK_BASE
status_webhook_base = STATUS_WEBHOOK_BASE

# NEW: Missing Outbound Dialer lower-case aliases
max_concurrent_calls = MAX_CONCURRENT_CALLS
rate_limit_calls_per_minute = RATE_LIMIT_CALLS_PER_MINUTE
retry_attempts = RETRY_ATTEMPTS
retry_delay_seconds = RETRY_DELAY_SECONDS
timezone_check_enabled = TIMEZONE_CHECK_ENABLED
dnc_check_enabled = DNC_CHECK_ENABLED

# NEW: Voice pipeline lower-case aliases - ADDED MISSING ATTRIBUTES
sara_env = SARA_ENV
partial_throttle_seconds = PARTIAL_THROTTLE_SECONDS
inference_task_name = INFERENCE_TASK_NAME
event_task_name = EVENT_TASK_NAME
celery_voice_queue = CELERY_VOICE_QUEUE
celery_max_retries = CELERY_MAX_RETRIES
voice_pipeline_port = VOICE_PIPELINE_PORT

# Optional: print a tiny config snapshot if requested (no secrets)
if _to_bool(os.getenv("CONFIG_DEBUG_SNAPSHOT"), False):
    try:
        import json, sys
        json.dump({"config_snapshot": Config.as_dict()}, sys.stdout, indent=2)
        print()
    except Exception:
        pass