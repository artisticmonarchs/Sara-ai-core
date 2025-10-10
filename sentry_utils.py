"""
sentry_utils.py — Phase 6 Ready (Flattened Structure)
Handles Sentry initialization for centralized error tracking.
"""

import os
import logging
import sentry_sdk
from dotenv import load_dotenv

# Load environment variables from .env (if available)
load_dotenv()


def init_sentry():
    """
    Initialize Sentry for error monitoring, if SENTRY_DSN is configured.
    Safe to call even when SENTRY_DSN is missing.
    """
    dsn = os.getenv("SENTRY_DSN")
    if not dsn:
        logging.info(
            '{"service": "sentry", "event": "init", "status": "skipped", '
            '"message": "SENTRY_DSN not set; skipping initialization"}'
        )
        return

    try:
        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=1.0,
            enable_tracing=True,
        )
        logging.info(
            '{"service": "sentry", "event": "init", "status": "success", '
            '"message": "Sentry initialized successfully"}'
        )
    except Exception as e:
        logging.error(
            f'{{"service": "sentry", "event": "init", "status": "error", '
            f'"message": "Failed to initialize Sentry: {str(e)}"}}'
        )
