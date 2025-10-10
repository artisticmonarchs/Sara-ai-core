import logging
import json
import uuid
from datetime import datetime

def log_event(service, event, status, message="", level="INFO", extra=None, trace_id=None):
    if trace_id is None:
        trace_id = str(uuid.uuid4())

    payload = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": service,
        "event": event,
        "status": status,
        "message": message,
        "level": level,
        "trace_id": trace_id,
        "extra": extra or {}
    }

    log_line = json.dumps(payload)
    if level.upper() == "ERROR":
        logging.error(log_line)
    elif level.upper() == "WARNING":
        logging.warning(log_line)
    else:
        logging.info(log_line)

    return trace_id
