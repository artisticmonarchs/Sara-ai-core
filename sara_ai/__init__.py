import os
import logging
from logging.handlers import RotatingFileHandler

# Logging bootstrap for all Sara AI services
# Ensures logs always go to console + rotating file
# Compatible with log_event() and Sentry integration

# Create logs directory if missing
os.makedirs("logs", exist_ok=True)
log_file = os.path.join("logs", "sara_ai.log")

# Avoid duplicate handlers in multi-import scenarios
root_logger = logging.getLogger()
if not root_logger.hasHandlers():
    root_logger.setLevel(logging.INFO)

    # Rotating file handler for persistent logs
    file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
    file_handler.setFormatter(logging.Formatter("%(message)s"))

    # Console handler for visibility in terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
