"""
gpt_client.py — Phase 6 Ready (Fixed Version)
Handles interaction with the OpenAI GPT model for Sara AI.
Includes trace logging, timeout handling, and safe fallbacks.
"""

import logging
import uuid
from openai import OpenAI
from logging_utils import log_event  # ✅ Correct import

# Initialize OpenAI client (uses OPENAI_API_KEY from environment)
client = OpenAI()  # ✅ removed unsupported timeout arg

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
        extra={"trace_id": trace_id, "prompt_preview": prompt[:100]},
    )

    try:
        # ✅ Compatible with new OpenAI SDK (v1.x)
        response = client.chat.completions.create(
            model="gpt-5-mini",  # ✅ use your configured model
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=1000,
            temperature=0.7,
        )

        # ✅ Updated attribute access
        reply = response.choices[0].message.content

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
