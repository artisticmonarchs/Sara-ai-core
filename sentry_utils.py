import os
import logging
import sentry_sdk
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

def init_sentry():
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
