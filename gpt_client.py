"""
gpt_client.py — Phase 10D (Fixed for new OpenAI SDK)
Handles GPT inference for Sara AI with full trace logging,
environment-safe proxy handling, and fault-tolerant fallback.
"""

import os
import uuid
import logging
from openai import OpenAI
from logging_utils import log_event


# --------------------------------------------------------------------------
# Environment & Safety Fixes
# --------------------------------------------------------------------------
# Render and some CI/CD environments inject proxy variables automatically.
# These break the new OpenAI SDK (v1+), which no longer accepts `proxies`.
for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
    os.environ.pop(key, None)

# Initialize OpenAI client safely (uses OPENAI_API_KEY from environment)
try:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
except Exception as e:
    logging.exception("Failed to initialize OpenAI client")
    raise RuntimeError(f"OpenAI client init failed: {e}")


# --------------------------------------------------------------------------
# Logger Setup
# --------------------------------------------------------------------------
logger = logging.getLogger("gpt_client")
logger.setLevel(logging.INFO)


# --------------------------------------------------------------------------
# Core Function
# --------------------------------------------------------------------------
def generate_reply(prompt: str, trace_id: str | None = None) -> str:
    """
    Generate a GPT response from the given prompt.
    Includes observability hooks and safe fallback on error.
    """
    trace_id = trace_id or str(uuid.uuid4())

    log_event(
        service="gpt_client",
        event="gpt_request",
        status="initiated",
        message="GPT request initiated",
        trace_id=trace_id,
        extra={"prompt_preview": prompt[:120]},
    )

    try:
        # ✅ Compatible with OpenAI SDK v1.0+
        response = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-5-mini"),
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=int(os.getenv("GPT_MAX_TOKENS", "1000")),
            temperature=float(os.getenv("GPT_TEMPERATURE", "0.7")),
        )

        reply = response.choices[0].message.content.strip()

        log_event(
            service="gpt_client",
            event="gpt_response",
            status="success",
            message="GPT response generated",
            trace_id=trace_id,
            extra={
                "reply_preview": reply[:120],
                "usage": getattr(response, "usage", {}),
            },
        )
        return reply

    except Exception as e:
        logger.exception("GPT request failed")
        log_event(
            service="gpt_client",
            event="gpt_error",
            status="failed",
            message="GPT request failed",
            trace_id=trace_id,
            extra={"error": str(e)},
        )
        return "I'm sorry, something went wrong while generating a reply."


# --------------------------------------------------------------------------
# Optional Local Test
# --------------------------------------------------------------------------
if __name__ == "__main__":
    print("Testing GPT client…")
    sample = generate_reply("Hello Sara, what is your role?")
    print("→", sample)
