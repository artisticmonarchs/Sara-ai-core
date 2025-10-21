"""
config.py — Phase 11-D Compliant
Centralized configuration for Sara AI Core.
Only this module may access environment variables directly.
"""

import os
from dotenv import load_dotenv

# Determine which environment file to load
ENV_MODE = os.getenv("ENV_MODE", "local").lower()
env_file = ".env.render" if ENV_MODE == "render" else ".env.local"

# Load the appropriate environment file
load_dotenv(env_file)


class Config:
    """Centralized configuration for Sara AI Core."""
    
    # Redis Configuration
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # Sentry Configuration
    SENTRY_DSN = os.getenv("SENTRY_DSN", "")
    
    # Server Configuration
    FLASK_PORT = int(os.getenv("FLASK_PORT", "5000"))
    
    # External Services
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
    DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY", "")
    TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
    TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
    TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER", "")
    
    # R2/S3 Configuration
    R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
    R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
    R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
    R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "")
    R2_REGION = os.getenv("R2_REGION", "auto")
    
    # Service Identification
    SERVICE_NAME = os.getenv("SERVICE_NAME", "sara-ai-core")
    
    # Metrics Configuration
    METRICS_SYNC_INTERVAL = int(os.getenv("METRICS_SYNC_INTERVAL", "30"))
    REDIS_METRIC_TTL_DAYS = int(os.getenv("REDIS_METRIC_TTL_DAYS", "30"))
    ENABLE_METRICS_SYNC = os.getenv("ENABLE_METRICS_SYNC", "true").lower() == "true"   # ✅ Added for metrics_collector
    
    # Environment Mode
    SARA_ENV = os.getenv("SARA_ENV", "development")
    
    # Phase 11-D: Logging Configuration
    LOG_BUFFER_SIZE = int(os.getenv("LOG_BUFFER_SIZE", "1000"))
    LOG_FLUSH_INTERVAL = int(os.getenv("LOG_FLUSH_INTERVAL", "30"))
    ENABLE_STRUCTURED_LOGGING = os.getenv("ENABLE_STRUCTURED_LOGGING", "true").lower() == "true"
    ENABLE_LOG_BUFFERING = os.getenv("ENABLE_LOG_BUFFERING", "true").lower() == "true"
    
    # Backward compatibility alias
    log_buffer_size = LOG_BUFFER_SIZE

    # Voice Pipeline Configuration - ADDED FOR PHASE 11-D COMPLIANCE
    CALL_STATE_TTL = int(os.getenv("CALL_STATE_TTL", "14400"))  # 4 hours in seconds
    PARTIAL_THROTTLE_SECONDS = float(os.getenv("PARTIAL_THROTTLE_SECONDS", "1.5"))
    INFERENCE_TASK_NAME = os.getenv("INFERENCE_TASK_NAME", "sara_ai.tasks.voice_pipeline.run_inference")
    EVENT_TASK_NAME = os.getenv("EVENT_TASK_NAME", "sara_ai.tasks.voice_pipeline.dispatch_event")
    CELERY_VOICE_QUEUE = os.getenv("CELERY_VOICE_QUEUE", "voice_pipeline")
    VOICE_PIPELINE_PORT = int(os.getenv("VOICE_PIPELINE_PORT", "7000"))
    
    # Additional Metrics Configuration - ADDED FOR PHASE 11-D COMPLIANCE
    SNAPSHOT_INTERVAL = int(os.getenv("SNAPSHOT_INTERVAL", "60"))
    ENABLE_METRICS_PERSISTENCE = os.getenv("ENABLE_METRICS_PERSISTENCE", "true").lower() == "true"
    METRICS_ENDPOINT_ENABLED = os.getenv("METRICS_ENDPOINT_ENABLED", "true").lower() == "true"