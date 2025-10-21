"""
gpt_client.py — Phase 11-D
Handles GPT inference for Sara AI with full trace logging,
metrics integration, and fault-tolerant fallback.
"""

import os
import uuid
import time
from openai import OpenAI
from logging_utils import log_event

# --------------------------------------------------------------------------
# Phase 11-D Configuration Isolation
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    # Fallback config for backward compatibility
    class Config:
        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
        OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini")
        GPT_MAX_TOKENS = int(os.getenv("GPT_MAX_TOKENS", "1000"))
        GPT_TEMPERATURE = float(os.getenv("GPT_TEMPERATURE", "0.7"))

# --------------------------------------------------------------------------
# Phase 11-D Metrics Integration - Lazy Import Shim
# --------------------------------------------------------------------------
def _get_metrics():
    """Lazy metrics shim to avoid circular imports at import-time"""
    try:
        from metrics_collector import increment_metric as _inc, observe_latency as _obs
        return _inc, _obs
    except Exception:
        # safe no-op fallbacks
        def _noop_inc(*a, **k): pass
        def _noop_obs(*a, **k): pass
        return _noop_inc, _noop_obs

# --------------------------------------------------------------------------
# Environment & Safety Fixes
# --------------------------------------------------------------------------
# Render and some CI/CD environments inject proxy variables automatically.
# These break the new OpenAI SDK (v1.0+), which does not support proxies.
for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
    if key in os.environ:
        del os.environ[key]


# --------------------------------------------------------------------------
# OpenAI Client Initialization
# --------------------------------------------------------------------------
try:
    client = OpenAI(api_key=Config.OPENAI_API_KEY)
except Exception as e:
    # Replace logging.exception with structured logging
    log_event(
        service="gpt_client",
        event="client_init",
        status="error",
        message=f"Failed to initialize OpenAI client: {e}"
    )
    raise RuntimeError(f"OpenAI client init failed: {e}")


# --------------------------------------------------------------------------
# Core Function: GPT Reply Generation
# --------------------------------------------------------------------------
def generate_reply(prompt: str, trace_id: str | None = None) -> str:
    """
    Generate a GPT response from the given prompt.
    Includes observability hooks, metrics, and safe fallback on error.
    """
    trace_id = trace_id or str(uuid.uuid4())
    start_time = time.time()

    # Lazy load metrics functions
    increment_metric, observe_latency = _get_metrics()

    # Phase 11-D: Increment request counter
    increment_metric("gpt_requests_total")

    log_event(
        service="gpt_client",
        event="gpt_request",
        status="initiated",
        message="GPT request initiated",
        trace_id=trace_id,
        extra={"prompt_preview": prompt[:120]},
    )

    try:
        # ✅ Compatible with OpenAI SDK v1.51+
        response = client.chat.completions.create(
            model=Config.OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=Config.GPT_MAX_TOKENS,
            temperature=Config.GPT_TEMPERATURE,
        )

        reply = response.choices[0].message.content.strip()
        latency_ms = (time.time() - start_time) * 1000

        # Phase 11-D: Record success metrics
        increment_metric("gpt_success_total")
        observe_latency("gpt_latency_ms", latency_ms)

        log_event(
            service="gpt_client",
            event="gpt_response",
            status="success",
            message="GPT response generated",
            trace_id=trace_id,
            extra={
                "reply_preview": reply[:120],
                "usage": getattr(response, "usage", {}),
                "latency_ms": round(latency_ms, 2)
            },
        )
        return reply

    except Exception as e:
        # Lazy load metrics functions for error case
        increment_metric, _ = _get_metrics()
        
        # Phase 11-D: Record failure metrics
        increment_metric("gpt_failures_total")
        
        # Replace logger.exception with structured logging
        log_event(
            service="gpt_client",
            event="gpt_error",
            status="failed",
            message=f"GPT request failed: {e}",
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