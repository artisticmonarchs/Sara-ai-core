"""
utils.py — Phase 6 Ready (Flattened Structure)
Utility helpers for ID generation and safe dictionary access.
"""

import uuid


def generate_id() -> str:
    """Generate a unique UUID string for traceable operations."""
    return str(uuid.uuid4())


def safe_get(d: dict, key: str, default=None):
    """Safely retrieve a value from a dictionary with a default fallback."""
    return d.get(key, default)
