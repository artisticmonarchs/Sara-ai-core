"""
config.py — Phase 6 Ready (Flattened Structure)
Environment configuration loader for Sara AI Core.
Automatically switches between local and Render modes.
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

    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    SENTRY_DSN = os.getenv("SENTRY_DSN", "")
    FLASK_PORT = int(os.getenv("FLASK_PORT", 5000))
