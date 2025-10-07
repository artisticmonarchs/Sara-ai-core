import uuid

def generate_id() -> str:
    return str(uuid.uuid4())

def safe_get(d: dict, key: str, default=None):
    return d.get(key, default)
