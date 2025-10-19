"""
__init__.py — Phase 11-D
Centralized logging bootstrap for Sara AI services
Provides structured JSON logging with trace correlation and metrics integration
"""

import os
import logging
from logging.handlers import RotatingFileHandler

# Phase 11-D: Structured JSON logging bootstrap
# Compatible with logging_utils.py and Prometheus metrics integration

def setup_structured_logging():
    """
    Initialize structured JSON logging for Sara AI services.
    This complements logging_utils.py and ensures all log sources are structured.
    """
    
    # Avoid duplicate handlers in multi-import scenarios
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return  # Already configured
    
    root_logger.setLevel(logging.INFO)
    
    # Phase 11-D: JSON formatter for structured logging
    class JSONFormatter(logging.Formatter):
        def format(self, record):
            log_entry = {
                "timestamp": self.formatTime(record),
                "level": record.levelname,
                "service": getattr(record, 'service', 'unknown'),
                "message": record.getMessage(),
                "component": record.name,
            }
            
            # Add trace_id if available
            trace_id = getattr(record, 'trace_id', None)
            if trace_id:
                log_entry["trace_id"] = trace_id
                
            # Add any extra structured data
            extra_data = getattr(record, 'structured_data', {})
            if extra_data:
                log_entry.update(extra_data)
                
            return json.dumps(log_entry, ensure_ascii=False)
    
    formatter = JSONFormatter()
    
    # Console handler for Render/cloud environments (primary)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Optional file handler for local development
    if os.getenv("ENABLE_FILE_LOGS", "false").lower() == "true":
        os.makedirs("logs", exist_ok=True)
        log_file = os.path.join("logs", "sara_ai.json.log")
        
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=5_000_000, 
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Phase 11-D: Log initialization event
    root_logger.info(
        "Structured logging initialized",
        extra={
            'service': 'logging_bootstrap',
            'structured_data': {
                'event': 'logging_initialized',
                'phase': '11-D',
                'file_logging_enabled': os.getenv("ENABLE_FILE_LOGS", "false").lower() == "true",
                'schema_version': 'phase_11d_v1'
            }
        }
    )

# Import JSON for formatter
import json

# Auto-initialize structured logging
setup_structured_logging()