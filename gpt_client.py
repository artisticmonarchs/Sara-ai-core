"""
gpt_client.py — Phase 11-D
Handles GPT inference for Sara AI with full trace logging,
metrics integration, and fault-tolerant fallback.
"""

import os
import uuid
import time
import asyncio
import json
import httpx
from typing import AsyncGenerator, Optional, Dict, Any
from openai import OpenAI, AsyncOpenAI
from logging_utils import log_event
import signal
import sys

def _graceful_shutdown(signum, frame):
    """Phase 12: Graceful shutdown handler"""
    log_event(
        service="gpt_client",
        event="shutdown",
        status="info",
        message=f"Signal {signum} received, exiting gracefully."
    )
    sys.exit(0)

# Move signal handlers to main guard to avoid import-side effects
if __name__ == "__main__":
    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)


# Import well-known OpenAI SDK error classes if available
try:
    from openai import RateLimitError, APIError, InternalServerError, Timeout
    OPENAI_ERRORS_AVAILABLE = True
except ImportError:
    # Fallback if SDK-specific errors aren't present
    RateLimitError = type('RateLimitError', (Exception,), {})
    APIError = type('APIError', (Exception,), {})
    InternalServerError = type('InternalServerError', (Exception,), {})
    Timeout = type('Timeout', (Exception,), {})
    OPENAI_ERRORS_AVAILABLE = False

# --------------------------------------------------------------------------
# Phase 11-D Configuration Isolation - FIXED IMPORT
# --------------------------------------------------------------------------
try:
    from config import config as Config
except ImportError:
    # Fallback config for backward compatibility
    class Config:
        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
        OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini")
        # Pick OPENAI_BASE_URL first, then OPENAI_API_URL, default to API root:
        OPENAI_BASE_URL = (
            os.getenv("OPENAI_BASE_URL")
            or os.getenv("OPENAI_API_URL")
            or "https://api.openai.com/v1"
        )
        MODEL_VERSION = os.getenv("MODEL_VERSION", "v2")
        GPT_MAX_TOKENS = int(os.getenv("GPT_MAX_TOKENS", "1000"))
        GPT_TEMPERATURE = float(os.getenv("GPT_TEMPERATURE", "0.7"))
        GPT_STREAMING_ENABLED = os.getenv("GPT_STREAMING_ENABLED", "true").lower() == "true"
        GPT_MAX_RETRIES = int(os.getenv("GPT_MAX_RETRIES", "3"))
        GPT_RETRY_DELAY = float(os.getenv("GPT_RETRY_DELAY", "1.0"))
        GPT_TIMEOUT = int(os.getenv("GPT_TIMEOUT", "30"))
        GPT_MAX_PROMPT_TOKENS = int(os.getenv("GPT_MAX_PROMPT_TOKENS", "4000"))
        GPT_MAX_PROMPT_CHARS = int(os.getenv("GPT_MAX_PROMPT_CHARS", "16000"))

# --------------------------------------------------------------------------
# Phase 11-D Metrics Integration - Lazy Import Shim
# --------------------------------------------------------------------------
def _get_metrics():
    """Lazy metrics shim to avoid circular imports at import-time"""
    try:
        from metrics_collector import increment_metric as _inc, observe_latency as _obs, histogram as _hist
        return _inc, _obs, _hist
    except Exception:
        # safe no-op fallbacks
        def _noop_inc(*a, **k): pass
        def _noop_obs(*a, **k): pass
        def _noop_hist(*a, **k): pass
        return _noop_inc, _noop_obs, _noop_hist

# --------------------------------------------------------------------------
# Phase 2: External API Wrapper Import - UPDATED TO with_external_api
# --------------------------------------------------------------------------
try:
    from external_api import with_external_api
except ImportError:
    # Fallback to direct call if with_external_api not available
    def with_external_api(service, trace_id=None, retry_check=None):
        def decorator(func):
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)
            return wrapper
        return decorator

# --------------------------------------------------------------------------
# Circuit Breaker Integration - UPDATED to use redis_client
# --------------------------------------------------------------------------
def _check_circuit_breaker(trace_id: str | None = None) -> bool:
    """Check Redis circuit breaker flag before making API calls"""
    try:
        # Use redis_client.safe_redis_operation per project standard
        from redis_client import safe_redis_operation
        circuit_open = safe_redis_operation(
            lambda r: r.get("circuit_breaker:openai") == b"true",
            trace_id=trace_id
        )
        return not circuit_open
    except ImportError:
        # If redis_client not available, assume circuit is closed (allow requests)
        return True
    except Exception as e:
        log_event(
            service="gpt_client",
            event="circuit_breaker_check_failed",
            status="warning",
            message=f"Circuit breaker check failed: {e}",
            trace_id=trace_id
        )
        # On failure, allow requests to proceed
        return True

# --------------------------------------------------------------------------
# Prompt/Token Validation
# --------------------------------------------------------------------------
def _validate_prompt_size(prompt: str, trace_id: str | None = None) -> tuple[bool, int]:
    """Pre-flight size checks for prompt length and token estimates"""
    if not prompt or not prompt.strip():
        log_event(
            service="gpt_client",
            event="prompt_validation",
            status="error",
            message="Empty prompt provided",
            trace_id=trace_id
        )
        return False, 0
    
    # Character length check (rough proxy for tokens)
    max_chars = getattr(Config, 'GPT_MAX_PROMPT_CHARS', 16000)
    if len(prompt) > max_chars:
        log_event(
            service="gpt_client",
            event="prompt_validation",
            status="error",
            message=f"Prompt exceeds maximum character limit: {len(prompt)} > {max_chars}",
            trace_id=trace_id,
            extra={
                "prompt_length": len(prompt),
                "max_allowed": max_chars
            }
        )
        return False, 0
    
    # Rough token estimation (4 chars ~= 1 token for English text)
    estimated_tokens = len(prompt) // 4
    max_tokens = getattr(Config, 'GPT_MAX_PROMPT_TOKENS', 4000)
    if estimated_tokens > max_tokens:
        log_event(
            service="gpt_client",
            event="prompt_validation",
            status="error",
            message=f"Prompt exceeds estimated token limit: {estimated_tokens} > {max_tokens}",
            trace_id=trace_id,
            extra={
                "estimated_tokens": estimated_tokens,
                "max_allowed": max_tokens
            }
        )
        return False, estimated_tokens
    
    return True, estimated_tokens

# --------------------------------------------------------------------------
# Responses API Helper Functions
# --------------------------------------------------------------------------
def _use_responses_api() -> bool:
    """
    We use Responses API based on explicit flag only.
    """
    return os.getenv("USE_OPENAI_RESPONSES", "").lower() in ("1", "true", "yes")

USE_RESPONSES = _use_responses_api()

def _extract_from_responses(data) -> str:
    """Extract text from Responses API response (supports SDK object or dict)."""
    # 1) SDK object path
    txt = getattr(data, "output_text", None)
    if isinstance(txt, str) and txt.strip():
        return txt.strip()
    out = getattr(data, "output", None)
    if out:
        # list of dicts/objects: {type: "output_text", content: "..."}
        for item in out:
            itype = getattr(item, "type", None) if not isinstance(item, dict) else item.get("type")
            if itype == "output_text":
                content = getattr(item, "content", None) if not isinstance(item, dict) else item.get("content")
                if isinstance(content, str) and content.strip():
                    return content.strip()
    # 2) Dict path
    if isinstance(data, dict):
        seq = data.get("output") or []
        for item in seq:
            if item.get("type") == "output_text":
                c = item.get("content", "")
                if isinstance(c, str) and c.strip():
                    return c.strip()
        if isinstance(data.get("output_text"), str):
            return data["output_text"].strip()
    return ""

# --------------------------------------------------------------------------
# Environment & Safety Fixes - OPTIONAL proxy disabling
# --------------------------------------------------------------------------
# Optionally disable proxy env vars for OpenAI only (doesn't affect other clients)
# Gate this behind an explicit flag to avoid breaking other network clients
if os.getenv("OPENAI_DISABLE_PROXY", "0").lower() in ("1", "true", "yes"):
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        os.environ.pop(key, None)
        # record this environment mutation for diagnostics
        log_event(
            service="gpt_client", 
            event="env_proxy_removed", 
            status="debug",
            message=f"Removed proxy env var {key} for OpenAI client compatibility"
        )

# --------------------------------------------------------------------------
# Retry and Backoff Configuration - Simplified for Phase 2
# --------------------------------------------------------------------------
class RetryConfig:
    """Configuration for retry and backoff behavior - Simplified for Phase 2"""
    MAX_RETRIES = getattr(Config, 'GPT_MAX_RETRIES', 3)
    INITIAL_DELAY = getattr(Config, 'GPT_RETRY_DELAY', 1.0)
    MAX_DELAY = 10.0
    BACKOFF_FACTOR = 2.0

# --------------------------------------------------------------------------
# OpenAI Client Initialization - LAZY LOADING VERSION
# --------------------------------------------------------------------------
# Globals that other functions in this module already reference
client: Optional[OpenAI] = None
async_client: Optional[AsyncOpenAI] = None

def _build_httpx_clients(timeout_s: float):
    """
    Build sync/async httpx clients only if proxy env vars are set.
    OpenAI SDK v1.x does NOT accept `proxies=` directly; it accepts
    an httpx client via `http_client=...`.
    """
    proxy = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
    if not proxy:
        return None, None

    # NOTE: httpx accepts a mapping or a single string for `proxies`
    sync_http = httpx.Client(proxies=proxy, timeout=timeout_s)
    async_http = httpx.AsyncClient(proxies=proxy, timeout=timeout_s)
    return sync_http, async_http

def _get_base_url() -> Optional[str]:
    # Support both names you used across the codebase/envs
    base = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_URL")
    if base:
        return base.rstrip("/")
    return None

def _ensure_clients():
    """
    Lazily (re)build the OpenAI clients exactly once.
    Safe to call repeatedly.
    """
    global client, async_client

    if client is not None and async_client is not None:
        return

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        # Don't crash import; just log a clear message.
        log_event(
            service="gpt_client",
            event="client_init_skipped",
            status="warn",
            message="OPENAI_API_KEY not set; client will be initialized when provided."
        )
        # Leave clients as None; callers should handle this gracefully.
        return

    timeout_s = float(os.getenv("GPT_TIMEOUT", "30"))
    base_url = _get_base_url()

    sync_http, async_http = _build_httpx_clients(timeout_s)

    # Build clients. Only pass http_client if we actually created one for proxies.
    client_kwargs = {"api_key": api_key}
    async_kwargs = {"api_key": api_key}

    if base_url:
        client_kwargs["base_url"] = base_url
        async_kwargs["base_url"] = base_url

    if sync_http:
        client_kwargs["http_client"] = sync_http
    if async_http:
        async_kwargs["http_client"] = async_http

    try:
        _client = OpenAI(**client_kwargs)
        _async_client = AsyncOpenAI(**async_kwargs)
    except Exception as e:
        # Do NOT crash at import time anywhere. Just log.
        log_event(
            service="gpt_client",
            event="client_init_failed",
            status="error",
            message=f"Failed to initialize OpenAI client: {e}"
        )
        # Keep them None so the rest of the app can still boot
        return

    client = _client
    async_client = _async_client
    log_event(
        service="gpt_client",
        event="client_init_ok",
        status="info",
        message="OpenAI clients initialized",
        extra={
            "base_url": base_url or "default",
            "timeout_s": timeout_s,
            "proxy_enabled": bool(sync_http or async_http)
        }
    )

# --------------------------------------------------------------------------
# Token Latency Tracking
# --------------------------------------------------------------------------
class TokenLatencyTracker:
    """Track token-level latency metrics"""
    
    def __init__(self):
        self.start_time = None
        self.first_token_time = None
        self.token_count = 0
        self.token_timestamps = []
    
    def start(self):
        """Start tracking"""
        self.start_time = time.time()
        self.first_token_time = None
        self.token_count = 0
        self.token_timestamps = []
    
    def record_token(self):
        """Record a token arrival"""
        current_time = time.time()
        if self.first_token_time is None:
            self.first_token_time = current_time
        
        self.token_count += 1
        self.token_timestamps.append(current_time)
    
    def get_metrics(self) -> Dict[str, float]:
        """Get latency metrics in milliseconds"""
        if not self.start_time:
            return {}

        current_time = time.time()
        metrics = {
            "total_latency_ms": (current_time - self.start_time) * 1000,
            "token_count": self.token_count,
        }

        if self.first_token_time:
            metrics["first_token_latency_ms"] = (self.first_token_time - self.start_time) * 1000
            metrics["tokens_per_second"] = (
                self.token_count / (current_time - self.first_token_time)
                if current_time > self.first_token_time else 0
            )

        if len(self.token_timestamps) > 1:
            gaps = [
                (self.token_timestamps[i] - self.token_timestamps[i - 1]) * 1000
                for i in range(1, len(self.token_timestamps))
            ]
            if gaps:
                metrics["avg_token_latency_ms"] = sum(gaps) / len(gaps)
                metrics["max_token_latency_ms"] = max(gaps)

        return metrics

# --------------------------------------------------------------------------
# Error Classification Helper
# --------------------------------------------------------------------------
def _is_retryable_error(error: Exception) -> bool:
    """
    Map OpenAI/timeout errors to retryable/non-retryable consistently.
    """
    if isinstance(error, (RateLimitError, Timeout, InternalServerError)):
        return True
    elif isinstance(error, APIError):
        # Some API errors might be retryable, others not
        # For now, treat all API errors as potentially retryable
        return True
    elif isinstance(error, asyncio.TimeoutError):
        return True
    elif hasattr(error, 'status_code'):
        # Handle HTTP status codes for retryability
        status_code = getattr(error, 'status_code', 0)
        return status_code in [429, 500, 502, 503, 504]
    return False

# --------------------------------------------------------------------------
# Core Function: GPT Reply Generation (Non-streaming)
# --------------------------------------------------------------------------
async def generate_reply(prompt: str, trace_id: str | None = None, persona: str = "default") -> str:
    """
    Generate a GPT response from the given prompt.
    Includes observability hooks, metrics, and safe fallback on error.
    """
    trace_id = trace_id or str(uuid.uuid4())
    start_time = time.time()

    # Lazy load metrics functions
    increment_metric, observe_latency, histogram = _get_metrics()

    # Check circuit breaker first
    if not _check_circuit_breaker(trace_id):
        increment_metric("gpt_circuit_breaker_trips_total", labels={"persona": persona})
        log_event(
            service="gpt_client",
            event="circuit_breaker_tripped",
            status="warning",
            message="Circuit breaker tripped - short-circuiting GPT request",
            trace_id=trace_id,
            extra={"persona": persona}
        )
        return "I'm temporarily unavailable due to high load. Please try again shortly."

    # Validate prompt size and get estimated tokens
    is_valid, estimated_tokens = _validate_prompt_size(prompt, trace_id)
    if not is_valid:
        increment_metric("gpt_validation_errors_total", labels={"persona": persona, "error_type": "prompt_too_large"})
        return "I'm sorry, the input is too long for me to process. Please try a shorter message."

    # Phase 11-D: Increment request counter
    increment_metric("gpt_requests_total", labels={"model": Config.OPENAI_MODEL, "persona": persona})

    log_event(
        service="gpt_client",
        event="gpt_request",
        status="initiated",
        message="GPT request initiated",
        trace_id=trace_id,
        extra={
            "prompt_preview": prompt[:120],
            "model": Config.OPENAI_MODEL,
            "model_version": getattr(Config, 'MODEL_VERSION', 'v2'),
            "streaming": False,
            "persona": persona,
            "estimated_prompt_tokens": estimated_tokens
        },
    )

    try:
        # Ensure clients are initialized
        _ensure_clients()
        if async_client is None:
            raise RuntimeError("OpenAI client not initialized - check OPENAI_API_KEY")
        
        # Use client.with_options for explicit per-call timeout enforcement (seconds)
        timeout_client = async_client.with_options(timeout=getattr(Config, 'GPT_TIMEOUT', 30))
        
        # Check if we should use Responses API
        if USE_RESPONSES:
            @with_external_api("openai", trace_id=trace_id, retry_check=_is_retryable_error)
            async def create_completion():
                response = await timeout_client.responses.create(
                    model=Config.OPENAI_MODEL,
                    input=prompt,
                    max_output_tokens=getattr(Config, 'GPT_MAX_TOKENS', 1000),
                    temperature=getattr(Config, 'GPT_TEMPERATURE', 0.7),
                    metadata={"trace_id": trace_id} if trace_id else None
                )
                return response
            
            response = await create_completion()
            reply = _extract_from_responses(response)
            if not reply:
                raise RuntimeError("No text output from Responses API")
        else:
            # Fallback to Chat Completions
            @with_external_api("openai", trace_id=trace_id, retry_check=_is_retryable_error)
            async def create_completion():
                return await timeout_client.chat.completions.create(
                    model=Config.OPENAI_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=getattr(Config, 'GPT_MAX_TOKENS', 1000),
                    temperature=getattr(Config, 'GPT_TEMPERATURE', 0.7),
                    stream=False  # Non-streaming for this function
                )

            # ✅ Compatible with OpenAI SDK v1.51+ with GPT-5 API v2
            response = await create_completion()
            reply = response.choices[0].message.content.strip()

        latency_ms = (time.time() - start_time) * 1000

        # Phase 11-D: Record success metrics
        increment_metric("gpt_success_total", labels={"model": Config.OPENAI_MODEL, "persona": persona})
        observe_latency("gpt_response_latency_ms", latency_ms)
        increment_metric("gpt_calls_total", labels={"model": Config.OPENAI_MODEL, "persona": persona})
        
        # Record token usage metrics (only available in Chat Completions)
        if hasattr(response, 'usage') and response.usage:
            # Use separate counters for token metrics
            increment_metric("gpt_prompt_tokens_total", value=response.usage.prompt_tokens)
            increment_metric("gpt_completion_tokens_total", value=response.usage.completion_tokens)
            increment_metric("gpt_total_tokens_total", value=response.usage.total_tokens)
            
            # Log token counts for observability
            log_event(
                service="gpt_client",
                event="token_usage",
                status="debug",
                trace_id=trace_id,
                extra={
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens
                }
            )

        log_event(
            service="gpt_client",
            event="gpt_response",
            status="success",
            message="GPT response generated",
            trace_id=trace_id,
            extra={
                "reply_preview": reply[:120],
                "usage": getattr(response, "usage", {}),
                "latency_ms": round(latency_ms, 2),
                "model_version": getattr(Config, 'MODEL_VERSION', 'v2'),
                "persona": persona
            },
        )
        return reply

    except Exception as e:
        # If we get here, external API wrapper has exhausted all retries
        increment_metric("gpt_failures_total", labels={"model": Config.OPENAI_MODEL, "persona": persona})
        
        log_event(
            service="gpt_client",
            event="gpt_error",
            status="failed",
            message=f"GPT request failed: {e}",
            trace_id=trace_id,
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "final_attempt": True,
                "persona": persona,
                "retryable": _is_retryable_error(e)
            },
        )
        return "I'm sorry, something went wrong while generating a reply."

# --------------------------------------------------------------------------
# Streaming Function: GPT Reply Generation with Token Streaming
# --------------------------------------------------------------------------
async def generate_reply_streaming(
    prompt: str, 
    trace_id: str | None = None,
    persona: str = "default"
) -> AsyncGenerator[str, None]:
    """
    Generate a GPT response with streaming support.
    Yields tokens incrementally for real-time TTS pipeline integration.
    """
    trace_id = trace_id or str(uuid.uuid4())
    start_time = time.time()

    # Lazy load metrics functions
    increment_metric, observe_latency, histogram = _get_metrics()

    # Check circuit breaker first
    if not _check_circuit_breaker(trace_id):
        increment_metric("gpt_circuit_breaker_trips_total", labels={"persona": persona})
        log_event(
            service="gpt_client",
            event="circuit_breaker_tripped",
            status="warning",
            message="Circuit breaker tripped - short-circuiting GPT streaming request",
            trace_id=trace_id,
            extra={"persona": persona}
        )
        yield "I'm temporarily unavailable due to high load. Please try again shortly."
        return

    # Validate prompt size and get estimated tokens
    is_valid, estimated_tokens = _validate_prompt_size(prompt, trace_id)
    if not is_valid:
        increment_metric("gpt_validation_errors_total", labels={"persona": persona, "error_type": "prompt_too_large"})
        yield "I'm sorry, the input is too long for me to process. Please try a shorter message."
        return

    # Phase 11-D: Increment request counter
    increment_metric("gpt_requests_total", labels={"model": Config.OPENAI_MODEL, "persona": persona})
    increment_metric("gpt_streaming_requests_total", labels={"model": Config.OPENAI_MODEL, "persona": persona})

    log_event(
        service="gpt_client",
        event="gpt_streaming_request",
        status="initiated",
        message="GPT streaming request initiated",
        trace_id=trace_id,
        extra={
            "prompt_preview": prompt[:120],
            "model": Config.OPENAI_MODEL,
            "model_version": getattr(Config, 'MODEL_VERSION', 'v2'),
            "streaming": True,
            "persona": persona,
            "estimated_prompt_tokens": estimated_tokens
        },
    )

    latency_tracker = TokenLatencyTracker()
    latency_tracker.start()

    try:
        # Ensure clients are initialized
        _ensure_clients()
        if async_client is None:
            raise RuntimeError("OpenAI client not initialized - check OPENAI_API_KEY")
        
        # Use client.with_options for explicit per-call timeout enforcement (seconds)
        timeout_client = async_client.with_options(timeout=getattr(Config, 'GPT_TIMEOUT', 30))
        
        # Check if we should use Responses API (streaming not yet supported in Responses API)
        if USE_RESPONSES:
            # Responses API doesn't support streaming yet, fall back to Chat Completions for streaming
            log_event(
                service="gpt_client",
                event="gpt_streaming_fallback",
                status="info",
                message="Streaming not supported in Responses API, falling back to Chat Completions",
                trace_id=trace_id
            )

        @with_external_api("openai", trace_id=trace_id, retry_check=_is_retryable_error)
        async def create_streaming_completion():
            return await timeout_client.chat.completions.create(
                model=Config.OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=getattr(Config, 'GPT_MAX_TOKENS', 1000),
                temperature=getattr(Config, 'GPT_TEMPERATURE', 0.7),
                stream=True  # Enable streaming
            )

        # ✅ GPT-5 API v2 with streaming
        stream = await create_streaming_completion()

        full_reply = ""
        token_count = 0
        
        async for chunk in stream:
            # Allow immediate cancellation from callers
            if asyncio.current_task().cancelled():
                raise asyncio.CancelledError()

            # defensive access: SDK event shape may differ between versions
            token = None
            try:
                # Newer SDK: chunk.choices[0].delta.content
                if getattr(chunk, "choices", None):
                    c0 = chunk.choices[0]
                    # delta-based streaming
                    if getattr(c0, "delta", None) and getattr(c0.delta, "content", None) is not None:
                        token = c0.delta.content
                    # older shape: c0.text or c0.message
                    elif getattr(c0, "text", None):
                        token = c0.text
            except Exception:
                token = None

            if token:
                full_reply += token
                token_count += 1
                latency_tracker.record_token()
                yield token

        # Record streaming completion metrics
        latency_ms = (time.time() - start_time) * 1000
        token_metrics = latency_tracker.get_metrics()
        
        increment_metric("gpt_streaming_success_total", labels={"model": Config.OPENAI_MODEL, "persona": persona})
        increment_metric("gpt_calls_total", labels={"model": Config.OPENAI_MODEL, "persona": persona})
        observe_latency("gpt_response_latency_ms", latency_ms)
        
        # Record token count
        increment_metric("gpt_streaming_tokens_total", value=token_count)

        log_event(
            service="gpt_client",
            event="gpt_streaming_response",
            status="success",
            message="GPT streaming response completed",
            trace_id=trace_id,
            extra={
                "reply_preview": full_reply[:120],
                "total_tokens": token_count,
                "latency_ms": round(latency_ms, 2),
                "model_version": getattr(Config, 'MODEL_VERSION', 'v2'),
                "token_metrics": token_metrics,
                "persona": persona
            },
        )
        return  # end generator normally

    except Exception as e:
        # Immediately re-raise cancellation to allow caller to stop streaming
        if isinstance(e, asyncio.CancelledError):
            raise
        
        # If we get here, external API wrapper has exhausted all retries
        increment_metric("gpt_streaming_failures_total", labels={"model": Config.OPENAI_MODEL, "persona": persona})
        
        log_event(
            service="gpt_client",
            event="gpt_streaming_error",
            status="failed",
            message=f"GPT streaming request failed: {e}",
            trace_id=trace_id,
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "final_attempt": True,
                "persona": persona,
                "retryable": _is_retryable_error(e)
            },
        )
        
        # Yield fallback message
        fallback = "I'm sorry, something went wrong while generating a reply."
        yield fallback

# --------------------------------------------------------------------------
# Hybrid Function: Best of Both Worlds
# --------------------------------------------------------------------------
async def generate_reply_adaptive(
    prompt: str, 
    trace_id: str | None = None,
    prefer_streaming: bool = None,
    persona: str = "default"
) -> AsyncGenerator[str, None]:
    """
    Adaptive GPT response generator that chooses between streaming and non-streaming
    based on configuration and performance requirements.
    """
    if prefer_streaming is None:
        prefer_streaming = getattr(Config, 'GPT_STREAMING_ENABLED', True)
    
    if prefer_streaming:
        async for token in generate_reply_streaming(prompt, trace_id, persona):
            yield token
    else:
        reply = await generate_reply(prompt, trace_id, persona)
        # Simulate token streaming for non-streaming responses
        words = reply.split()
        for i, word in enumerate(words):
            yield word + (" " if i < len(words) - 1 else "")

# --------------------------------------------------------------------------
# Backward Compatibility Wrapper
# --------------------------------------------------------------------------
def generate_reply_sync(prompt: str, trace_id: str | None = None, persona: str = "default") -> str:
    """
    Synchronous wrapper for backward compatibility.
    Use only when async context is not available.
    """
    return asyncio.run(generate_reply(prompt, trace_id, persona))

# --------------------------------------------------------------------------
# Health Check and Model Validation
# --------------------------------------------------------------------------
async def validate_gpt_connection(trace_id: str | None = None) -> bool:
    """
    Validate GPT API connection and model availability.
    """
    trace_id = trace_id or str(uuid.uuid4())
    try:
        # Ensure clients are initialized
        _ensure_clients()
        if async_client is None:
            log_event(
                service="gpt_client",
                event="connection_validation_failed",
                status="error",
                message="OpenAI client not initialized - check OPENAI_API_KEY",
                trace_id=trace_id
            )
            return False
        
        # Test with a simple prompt
        test_prompt = "Hello, respond with just 'OK'"
        
        # Use client.with_options for explicit per-call timeout enforcement (seconds)
        timeout_client = async_client.with_options(timeout=getattr(Config, 'GPT_TIMEOUT', 30))
        
        # Check if we should use Responses API
        if USE_RESPONSES:
            @with_external_api("openai", trace_id=trace_id, retry_check=_is_retryable_error)
            async def create_test_completion():
                response = await timeout_client.responses.create(
                    model=Config.OPENAI_MODEL,
                    input=test_prompt,
                    max_output_tokens=5,
                    temperature=0.1,
                    metadata={"trace_id": trace_id} if trace_id else None
                )
                return response
            
            response = await create_test_completion()
            result = _extract_from_responses(response)
        else:
            # Fallback to Chat Completions
            @with_external_api("openai", trace_id=trace_id, retry_check=_is_retryable_error)
            async def create_test_completion():
                return await timeout_client.chat.completions.create(
                    model=Config.OPENAI_MODEL,
                    messages=[{"role": "user", "content": test_prompt}],
                    max_tokens=5,
                    temperature=0.1
                )
            
            response = await create_test_completion()
            result = response.choices[0].message.content.strip()
        
        return result.upper() == "OK"
        
    except Exception as e:
        log_event(
            service="gpt_client",
            event="connection_validation_failed",
            status="error",
            message=f"GPT connection validation failed: {e}",
            trace_id=trace_id,
            extra={"error": str(e)}
        )
        return False

async def validate_gpt_streaming(trace_id: str | None = None) -> bool:
    """
    Validate GPT streaming connection.
    """
    trace_id = trace_id or str(uuid.uuid4())
    try:
        # Ensure clients are initialized
        _ensure_clients()
        if async_client is None:
            log_event(
                service="gpt_client",
                event="streaming_validation_failed",
                status="error",
                message="OpenAI client not initialized - check OPENAI_API_KEY",
                trace_id=trace_id
            )
            return False
        
        test_prompt = "Say OK"
        async for token in generate_reply_streaming(test_prompt, trace_id):
            if "OK" in token.upper():
                return True
        return False
    except Exception as e:
        log_event(
            service="gpt_client",
            event="streaming_validation_failed",
            status="error",
            message=f"GPT streaming validation failed: {e}",
            trace_id=trace_id,
            extra={"error": str(e)}
        )
        return False

def get_model_info() -> Dict[str, Any]:
    """Get information about the configured model"""
    return {
        "model": Config.OPENAI_MODEL,
        "model_version": getattr(Config, 'MODEL_VERSION', 'v2'),
        "max_tokens": getattr(Config, 'GPT_MAX_TOKENS', 1000),
        "temperature": getattr(Config, 'GPT_TEMPERATURE', 0.7),
        "streaming_enabled": getattr(Config, 'GPT_STREAMING_ENABLED', True),
        "max_retries": getattr(Config, 'GPT_MAX_RETRIES', 3),
        "timeout": getattr(Config, 'GPT_TIMEOUT', 30),
        "openai_errors_available": OPENAI_ERRORS_AVAILABLE,
        "api_type": "responses" if USE_RESPONSES else "chat_completions"
    }

# --------------------------------------------------------------------------
# Optional Local Test
# --------------------------------------------------------------------------
async def test_streaming():
    """Test streaming functionality"""
    log_event(
        service="gpt_client",
        event="local_test",
        status="info",
        message="Testing GPT streaming client…"
    )
    prompt = "Hello Sara, what is your role? Please explain in about 50 words."
    
    log_event(
        service="gpt_client",
        event="local_test",
        status="info",
        message="Streaming response:"
    )
    async for token in generate_reply_streaming(prompt):
        log_event(
            service="gpt_client",
            event="local_test_token",
            status="debug",
            message=token
        )

if __name__ == "__main__":
    # Test both synchronous and streaming
    log_event(
        service="gpt_client",
        event="local_test",
        status="info",
        message="Testing GPT client…"
    )
    
    # Test synchronous (for backward compatibility)
    sample = asyncio.run(generate_reply("Hello Sara, what is your role?"))
    log_event(
        service="gpt_client",
        event="local_test_result",
        status="info",
        message=f"Synchronous: {sample}"
    )
    
    # Test streaming
    asyncio.run(test_streaming())
    
    # Test model info
    model_info = get_model_info()
    log_event(
        service="gpt_client",
        event="local_test_model_info",
        status="info",
        message=f"Model Info: {model_info}"
    )
    
    # Test connection validation
    conn_valid = asyncio.run(validate_gpt_connection())
    stream_valid = asyncio.run(validate_gpt_streaming())
    log_event(
        service="gpt_client",
        event="local_test_validation",
        status="info",
        message=f"Connection validation: {conn_valid}, Streaming validation: {stream_valid}"
    )