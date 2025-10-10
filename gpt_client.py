"""
gpt_client.py — Phase 6 Ready (Flattened Structure)
Handles interaction with the OpenAI GPT model for Sara AI.
Includes trace logging, timeout handling, and safe fallbacks.
"""

import logging
import uuid
from openai import OpenAI
from logging_utils import log_event        # ✅ fixed import

# Initialize OpenAI client (uses OPENAI_API_KEY from environment)
client = OpenAI(timeout=60)

logger = logging.getLogger("gpt_client")
logger.setLevel(logging.INFO)


def generate_reply(prompt: str, trace_id: str | None = None) -> str:
    """
    Generate a GPT response from the given prompt.
    Adds trace_id for observability across services.
    """
    trace_id = trace_id or str(uuid.uuid4())

    log_event(
        service="gpt_client",
        event="gpt_request",
        status="initiated",
        extra={"trace_id": trace_id, "prompt_preview": prompt[:100]},  # ✅ use correct key
    )

    try:
        response = client.chat.completions.create(
            model="gpt-5-mini",           # ✅ model name consistent with Render settings
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=1000,
            temperature=0.7,
        )

        reply = response.choices[0].message["content"]
        log_event(
            service="gpt_client",
            event="gpt_response",
            status="success",
            extra={
                "trace_id": trace_id,
                "reply_preview": reply[:100],
            },
        )
        return reply

    except Exception as e:
        log_event(
            service="gpt_client",
            event="gpt_error",
            status="failed",
            extra={
                "trace_id": trace_id,
                "error": str(e),
            },
        )
        return "I'm sorry, something went wrong while generating a reply."
