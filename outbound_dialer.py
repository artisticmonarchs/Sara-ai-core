"""
outbound_dialer.py — Phase 12 Compliant
Automatically hardened by phase12_auto_hardener.py
"""

"""
outbound_dialer.py
Manage parallel, rate-limited outbound calls through Twilio.
Maintains Redis queue, enforces concurrency limits, and attaches DuplexVoiceController.
"""

import asyncio
import json
import time
import uuid
import re
import random
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import traceback

# --------------------------------------------------------------------------
# Configuration and Imports
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    import os
    class Config:
        MAX_CONCURRENT_CALLS = int(os.getenv("MAX_CONCURRENT_CALLS", "10"))
        DIAL_TIMEOUT_SECONDS = int(os.getenv("DIAL_TIMEOUT_SECONDS", "45"))
        RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))
        RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
        RATE_LIMIT_CALLS_PER_MINUTE = int(os.getenv("RATE_LIMIT_CALLS_PER_MINUTE", "60"))
        TIMEZONE_CHECK_ENABLED = os.getenv("TIMEZONE_CHECK_ENABLED", "true").lower() == "true"
        DNC_CHECK_ENABLED = os.getenv("DNC_CHECK_ENABLED", "true").lower() == "true"
        VOICE_WEBHOOK_BASE = os.getenv("VOICE_WEBHOOK_BASE", "https://your-app.com")
        STATUS_WEBHOOK_BASE = os.getenv("STATUS_WEBHOOK_BASE", "https://your-app.com")

try:
    # Phase 12: use the unified helpers and avoid raw ops
    from redis_client import get_redis_client, safe_redis_operation
except ImportError:
    def get_redis_client(): return None
    async def safe_redis_operation(op, fallback=None, operation_name="redis_op"): 
        try:
            return await op()
        except Exception:
            return fallback

try:
    from twilio_client import make_outbound_call
except ImportError:
    async def make_outbound_call(phone_number: str, call_config: Dict[str, Any]) -> Dict[str, Any]:
        return {"error": "twilio_client not available", "call_sid": f"fake_{uuid.uuid4()}"}

try:
    from duplex_voice_controller import controller_manager
except ImportError:
    class controller_manager:
        @staticmethod
        async def get_controller(call_sid: str, websocket, config: Optional[Dict] = None):
            return None
        @staticmethod
        async def remove_controller(call_sid: str):
            pass

try:
    from call_disposition_tracker import track_call_disposition
except ImportError:
    async def track_call_disposition(call_sid: str, disposition: str, metadata: Dict[str, Any]):
        pass

try:
    import metrics_collector as metrics
except ImportError:
    class metrics:
        @staticmethod
        def increment_metric(*args, **kwargs): pass
        @staticmethod
        def set_gauge(*args, **kwargs): pass
        @staticmethod
        def observe_latency(*args, **kwargs): pass

try:
    from logging_utils import get_logger
    logger = get_logger("outbound_dialer")
except ImportError:
    import logging
    logger = logging.getLogger("outbound_dialer")

# --------------------------------------------------------------------------
# External API Wrapper Integration
# --------------------------------------------------------------------------
try:
    from external_api import with_external_api
except ImportError:
    # Fallback decorator if external_api not available
    def with_external_api(service_name, timeout_seconds=None):
        def decorator(func):
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)
            return wrapper
        return decorator

# --------------------------------------------------------------------------
# Shared Circuit Breaker Configuration
# --------------------------------------------------------------------------

CB_TWILIO_PREFIX = "circuit_breaker:twilio"
CB_THRESHOLD = 5          # tune as needed
CB_COOLDOWN_SECONDS = 120 # tune as needed

def _cb_now() -> float:
    return time.time()

async def _twilio_cb_state(redis):
    if not redis: 
        return "closed"
    async def _op():
        state = await redis.get(f"{CB_TWILIO_PREFIX}:state")
        return (state.decode() if isinstance(state, (bytes, bytearray)) else state) or "closed"
    return await safe_redis_operation(_op, fallback="closed", operation_name="twilio_cb_get_state")

async def _twilio_cb_is_open(redis) -> bool:
    state = await _twilio_cb_state(redis)
    if state != "open":
        return False
    # check cooldown
    async def _op():
        v = await redis.get(f"{CB_TWILIO_PREFIX}:cooldown_until")
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="ignore")
        return float(v) if v else 0.0
    cooldown_until = await safe_redis_operation(_op, fallback=0.0, operation_name="twilio_cb_get_cooldown_until")
    if _cb_now() < cooldown_until:
        return True
    # transition half-open
    async def _set_half_open():
        await redis.set(f"{CB_TWILIO_PREFIX}:state", "half-open")
    await safe_redis_operation(_set_half_open, fallback=None, operation_name="twilio_cb_half_open")
    return False

async def _twilio_cb_record_failure(redis):
    if not redis: 
        return
    async def _op():
        fails = await redis.incr(f"{CB_TWILIO_PREFIX}:fail_count")
        if fails == 1:
            await redis.expire(f"{CB_TWILIO_PREFIX}:fail_count", 60)
        if fails >= CB_THRESHOLD:
            await redis.set(f"{CB_TWILIO_PREFIX}:state", "open")
            until = _cb_now() + CB_COOLDOWN_SECONDS
            await redis.set(f"{CB_TWILIO_PREFIX}:cooldown_until", str(until))
    await safe_redis_operation(_op, fallback=None, operation_name="twilio_cb_record_failure")
    metrics.increment_metric("twilio_circuit_failures_total")

async def _twilio_cb_record_success(redis):
    if not redis: 
        return
    async def _op():
        state = await redis.get(f"{CB_TWILIO_PREFIX}:state")
        state = state.decode() if isinstance(state, (bytes, bytearray)) else state
        if state == "half-open":
            await redis.set(f"{CB_TWILIO_PREFIX}:state", "closed")
            await redis.delete(f"{CB_TWILIO_PREFIX}:fail_count")
            await redis.delete(f"{CB_TWILIO_PREFIX}:cooldown_until")
        else:
            await redis.delete(f"{CB_TWILIO_PREFIX}:fail_count")
    await safe_redis_operation(_op, fallback=None, operation_name="twilio_cb_record_success")

# --------------------------------------------------------------------------
# Data Models
# --------------------------------------------------------------------------

@dataclass
class Lead:
    """Represents an outbound call lead"""
    phone_number: str
    campaign_id: str
    lead_id: str
    metadata: Dict[str, Any]
    timezone: Optional[str] = None
    priority: int = 1
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "phone_number": self.phone_number,
            "campaign_id": self.campaign_id,
            "lead_id": self.lead_id,
            "metadata": self.metadata,
            "timezone": self.timezone,
            "priority": self.priority
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Lead':
        return cls(
            phone_number=data["phone_number"],
            campaign_id=data["campaign_id"],
            lead_id=data["lead_id"],
            metadata=data.get("metadata", {}),
            timezone=data.get("timezone"),
            priority=data.get("priority", 1)
        )


@dataclass
class CallAttempt:
    """Tracks a single call attempt"""
    lead: Lead
    call_sid: Optional[str] = None
    attempt_number: int = 1
    status: str = "pending"  # pending, dialing, connected, failed, completed
    start_time: float = 0
    end_time: float = 0
    trace_id: str = None
    
    def __post_init__(self):
        if self.trace_id is None:
            self.trace_id = str(uuid.uuid4())
    
    @property
    def duration(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0

# --------------------------------------------------------------------------
# Phone Number Validation
# --------------------------------------------------------------------------

def validate_phone_number(phone_number: str) -> bool:
    """Validate E.164 phone number format"""
    pattern = r'^\+[1-9]\d{1,14}$'
    return bool(re.match(pattern, phone_number))

# --------------------------------------------------------------------------
# Circuit Breaker for Twilio
# --------------------------------------------------------------------------

class TwilioCircuitBreaker:
    """Circuit breaker for Twilio API calls"""
    
    def __init__(self, failure_threshold: int = 5, recovery_time: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.failures = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def can_make_call(self) -> bool:
        """Check if calls can be made based on circuit state"""
        if self.state == "OPEN":
            if time.time() - self.last_failure_time < self.recovery_time:
                return False
            else:
                self.state = "HALF_OPEN"
                logger.info("Twilio circuit breaker HALF_OPEN - testing recovery")
                return True
        return True
    
    def record_success(self):
        """Record successful call"""
        self.failures = 0
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            logger.info("Twilio circuit breaker CLOSED - recovery confirmed")
    
    def record_failure(self):
        """Record failed call"""
        self.failures += 1
        self.last_failure_time = time.time()
        
        if self.failures >= self.failure_threshold:
            self.state = "OPEN"
            logger.error("Twilio circuit breaker OPENED due to consecutive failures", extra={
                "failures": self.failures,
                "recovery_time_seconds": self.recovery_time
            })
        
        metrics.increment_metric("twilio_circuit_failures_total")

# --------------------------------------------------------------------------
# Rate Limiter
# --------------------------------------------------------------------------

class RateLimiter:
    """Rate limiter for outbound calls"""
    
    def __init__(self, redis_client, calls_per_minute: int):
        self.redis_client = redis_client
        self.calls_per_minute = calls_per_minute
        self.window_seconds = 60
    
    async def can_make_call(self, campaign_id: str) -> bool:
        """Check if a call can be made for the given campaign"""
        if not self.redis_client:
            return True

        key = f"rate_limit:{campaign_id}:{int(time.time() // self.window_seconds)}"
        async def _op():
            current_count = await self.redis_client.get(key)
            count = int(current_count) if current_count else 0
            if count >= self.calls_per_minute:
                logger.debug("Rate limit exceeded", extra={"campaign_id": campaign_id, "count": count})
                return False
            return True
        return await safe_redis_operation(_op, fallback=True, operation_name="rate_limit_can_make_call")
    
    async def record_call(self, campaign_id: str):
        """Record a call attempt for rate limiting"""
        if not self.redis_client:
            return

        key = f"rate_limit:{campaign_id}:{int(time.time() // self.window_seconds)}"
        async def _op():
            await self.redis_client.incr(key)
            await self.redis_client.expire(key, self.window_seconds * 2)  # Extra buffer
        await safe_redis_operation(_op, fallback=None, operation_name="rate_limit_record_call")

# --------------------------------------------------------------------------
# DNC and Timezone Services
# --------------------------------------------------------------------------

class DNCChecker:
    """Do Not Call list checker"""
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
    
    async def is_dnc_number(self, phone_number: str) -> bool:
        """Check if number is in DNC list"""
        if not self.redis_client or not Config.DNC_CHECK_ENABLED:
            return False
        dnc_key = f"dnc:{phone_number}"
        async def _op():
            is_dnc = await self.redis_client.get(dnc_key)
            return bool(is_dnc)
        return await safe_redis_operation(_op, fallback=False, operation_name="dnc_is_number")


class TimezoneChecker:
    """Timezone-based calling restrictions"""
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
    
    async def is_callable_time(self, phone_number: str, timezone: Optional[str] = None) -> bool:
        """Check if current time is appropriate for calling"""
        if not self.redis_client or not Config.TIMEZONE_CHECK_ENABLED:
            return True
        
        try:
            # Simple implementation - avoid calling between 9 PM and 9 AM local time
            # In production, this would use proper timezone conversion
            current_hour = time.localtime().tm_hour
            return 9 <= current_hour < 21  # 9 AM to 9 PM
        except Exception as e:
            logger.error("Timezone check failed", extra={"phone_number": phone_number, "error": str(e)})
            return True

# --------------------------------------------------------------------------
# Twilio API Wrapper with Guardrails
# --------------------------------------------------------------------------

class TwilioAPIWrapper:
    """Wrapper for Twilio API calls with guardrails"""
    
    @staticmethod
    @with_external_api("twilio", timeout_seconds=Config.DIAL_TIMEOUT_SECONDS)
    async def make_outbound_call_with_guardrails(phone_number: str, call_config: Dict[str, Any], 
                                               trace_id: str, contact_id: str, campaign_id: str) -> Dict[str, Any]:
        """
        Make outbound call via Twilio with proper guardrails and validation
        """
        start_time = time.time()
        
        try:
            # Validate phone number format
            if not validate_phone_number(phone_number):
                error_msg = "Invalid E.164 phone number format"
                logger.warning("Phone number validation failed", extra={
                    "trace_id": trace_id,
                    "contact_id": contact_id,
                    "to": phone_number,
                    "error": error_msg
                })
                metrics.increment_metric("twilio_place_call_total", 
                                       tags={"result": "error", "error_code": "validation_failed", "campaign_id": campaign_id})
                return {"error": error_msg, "error_code": "validation_failed"}
            
            # Redact sensitive data from logs
            safe_call_config = {k: v for k, v in call_config.items() if "auth" not in k.lower() and "token" not in k.lower() and "password" not in k.lower()}
            
            logger.info("Making Twilio outbound call", extra={
                "trace_id": trace_id,
                "contact_id": contact_id,
                "to": phone_number,
                "call_config": safe_call_config
            })
            
            # Make the actual Twilio call
            result = await make_outbound_call(phone_number, call_config)
            
            # Calculate latency
            latency_ms = (time.time() - start_time) * 1000
            metrics.observe_latency("twilio_place_call_latency_ms", latency_ms)
            
            if "error" in result:
                error_code = result.get("error_code", "unknown")
                logger.error("Twilio call failed", extra={
                    "trace_id": trace_id,
                    "contact_id": contact_id,
                    "to": phone_number,
                    "error": result["error"],
                    "error_code": error_code
                })
                metrics.increment_metric("twilio_place_call_total", 
                                       tags={"result": "error", "error_code": error_code, "campaign_id": campaign_id})
            else:
                logger.info("Twilio call initiated successfully", extra={
                    "trace_id": trace_id,
                    "contact_id": contact_id,
                    "to": phone_number,
                    "call_sid": result.get("call_sid")
                })
                metrics.increment_metric("twilio_place_call_total", 
                                       tags={"result": "success", "campaign_id": campaign_id})
            
            return result
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            metrics.observe_latency("twilio_place_call_latency_ms", latency_ms)
            
            error_msg = str(e)
            error_code = "exception"
            
            logger.error("Exception making Twilio call", extra={
                "trace_id": trace_id,
                "contact_id": contact_id,
                "to": phone_number,
                "error": error_msg,
                "error_code": error_code,
                "stack": traceback.format_exc()
            })
            metrics.increment_metric("twilio_place_call_total", 
                                   tags={"result": "error", "error_code": error_code, "campaign_id": campaign_id})
            
            return {"error": error_msg, "error_code": error_code}

# --------------------------------------------------------------------------
# Exponential Backoff Helper
# --------------------------------------------------------------------------

async def _sleep_backoff(base_delay: float, attempt: int):
    """Exponential backoff with jitter"""
    # exponential + jitter (25%)
    delay = base_delay * (2 ** (attempt - 1))
    delay += random.uniform(0, delay * 0.25)
    await asyncio.sleep(min(delay, 30))  # cap at 30 seconds

# --------------------------------------------------------------------------
# Main OutboundDialer Class
# --------------------------------------------------------------------------

class OutboundDialer:
    """
    Manages parallel, rate-limited outbound calls through Twilio.
    Maintains Redis queue, enforces concurrency limits, and attaches DuplexVoiceController.
    """
    
    def __init__(self):
        self.redis_client = get_redis_client()
        self.is_running = False
        self.active_calls: Dict[str, CallAttempt] = {}
        self.worker_task: Optional[asyncio.Task] = None
        
        # Core components
        self.circuit_breaker = TwilioCircuitBreaker()
        self.rate_limiter = RateLimiter(self.redis_client, Config.RATE_LIMIT_CALLS_PER_MINUTE)
        self.dnc_checker = DNCChecker(self.redis_client)
        self.timezone_checker = TimezoneChecker(self.redis_client)
        self.twilio_wrapper = TwilioAPIWrapper()
        
        # Concurrency guard for dialing phase
        self._dial_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_CALLS)
        
        # Queue names
        self.leads_queue = "outbound:leads"
        self.pending_queue = "outbound:pending"
        self.failed_queue = "outbound:failed"
        
        # Queue gauge optimization
        self._queue_gauge_counter = 0
        
        logger.info("OutboundDialer initialized", extra={
            "max_concurrent_calls": Config.MAX_CONCURRENT_CALLS,
            "rate_limit": Config.RATE_LIMIT_CALLS_PER_MINUTE
        })
    
    # --------------------------------------------------------------------------
    # Public API
    # --------------------------------------------------------------------------
    
    async def start(self):
        """Start the outbound dialer worker"""
        if self.is_running:
            logger.warning("Outbound dialer already running")
            return
        
        self.is_running = True
        self.worker_task = asyncio.create_task(self._worker_loop())
        
        metrics.set_gauge("outbound_dialer_active", 1)
        logger.info("Outbound dialer started")
    
    async def stop(self):
        """Stop the outbound dialer worker"""
        if not self.is_running:
            return
        
        self.is_running = False
        
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
        
        # Clean up active calls
        for call_sid in list(self.active_calls.keys()):
            await self._cleanup_call(call_sid)
        
        metrics.set_gauge("outbound_dialer_active", 0)
        logger.info("Outbound dialer stopped")
    
    async def pause(self):
        """Pause the dialer without tearing down active calls"""
        if not self.is_running:
            logger.warning("Outbound dialer already paused")
            return
        
        self.is_running = False
        metrics.set_gauge("outbound_dialer_active", 0)
        logger.info("Outbound dialer paused")
    
    async def resume(self):
        """Resume the dialer if paused"""
        if self.is_running:
            logger.warning("Outbound dialer already running")
            return
        
        self.is_running = True
        self.worker_task = asyncio.create_task(self._worker_loop())
        
        metrics.set_gauge("outbound_dialer_active", 1)
        logger.info("Outbound dialer resumed")
    
    async def add_lead(self, lead: Lead) -> bool:
        """
        Add a lead to the outbound queue
        Returns: True if lead was added successfully
        """
        try:
            queue_data = lead.to_dict()
            queue_data["added_at"] = time.time()
            
            # Use priority-based queue (higher priority = processed first)
            priority_score = lead.priority
            
            if self.redis_client:
                payload = json.dumps(queue_data)
                async def _op():
                    return await self.redis_client.zadd(self.leads_queue, {payload: priority_score})
                await safe_redis_operation(_op, fallback=None, operation_name="leads_zadd")
                
                metrics.increment_metric("leads_added_total")
                logger.info("Lead added to queue", extra={
                    "lead_id": lead.lead_id,
                    "campaign_id": lead.campaign_id,
                    "phone_number": lead.phone_number,
                    "priority": lead.priority
                })
                return True
            else:
                logger.error("Redis client not available - cannot add lead")
                return False
                
        except Exception as e:
            logger.error("Failed to add lead to queue", extra={
                "lead_id": lead.lead_id,
                "error": str(e),
                "stack": traceback.format_exc()
            })
            metrics.increment_metric("lead_add_errors_total")
            return False
    
    async def get_status(self) -> Dict[str, Any]:
        """Get current dialer status"""
        queue_size = await self._get_queue_size()
        return {
            "is_running": self.is_running,
            "active_calls": len(self.active_calls),
            "max_concurrent_calls": Config.MAX_CONCURRENT_CALLS,
            "dial_semaphore_available": self._dial_semaphore._value,  # Diagnostic info
            "circuit_breaker_state": self.circuit_breaker.state,
            "queue_size": queue_size
        }
    
    # --------------------------------------------------------------------------
    # Worker Loop
    # --------------------------------------------------------------------------
    
    async def _worker_loop(self):
        """Main worker loop that processes leads from Redis queue"""
        logger.info("Outbound dialer worker loop started")
        
        try:
            while self.is_running:
                try:
                    # Check if we can make more calls
                    if (len(self.active_calls) >= Config.MAX_CONCURRENT_CALLS or 
                        not self.circuit_breaker.can_make_call()):
                        await asyncio.sleep(1)
                        continue
                    
                    # Get next lead from queue
                    lead = await self._get_next_lead()
                    if not lead:
                        await asyncio.sleep(0.5)
                        continue
                    
                    # Process the lead
                    asyncio.create_task(self._process_lead(lead))
                    
                    # Small delay to prevent tight looping
                    await asyncio.sleep(0.1)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Error in worker loop", extra={
                        "error": str(e),
                        "stack": traceback.format_exc()
                    })
                    await asyncio.sleep(1)  # Back off on error
                
                # Emit queue size gauge for dashboard monitoring (optimized: every ~2s)
                self._queue_gauge_counter += 1
                if self._queue_gauge_counter >= 20:  # ~2 seconds at 0.1s sleep
                    queue_size = await self._get_queue_size()
                    metrics.set_gauge("outbound_dialer_queue_size", queue_size)
                    self._queue_gauge_counter = 0
        
        except asyncio.CancelledError:
            logger.info("Outbound dialer worker loop cancelled")
        finally:
            logger.info("Outbound dialer worker loop ended")
    
    async def _get_next_lead(self) -> Optional[Lead]:
        """Get next lead from Redis sorted set (highest priority first)"""
        if not self.redis_client:
            return None
        
        try:
            async def _op():
                return await self.redis_client.zpopmax(self.leads_queue, count=1)
            result = await safe_redis_operation(_op, fallback=None, operation_name="leads_zpopmax")
            if not result:
                return None
            
            lead_data_str, score = result[0]
            # Bytes-safe decode for queue pop
            if isinstance(lead_data_str, (bytes, bytearray)):
                lead_data_str = lead_data_str.decode("utf-8", errors="replace")
            lead_data = json.loads(lead_data_str)
            
            return Lead.from_dict(lead_data)
            
        except Exception as e:
            logger.error("Failed to get next lead from queue", extra={"error": str(e)})
            return None
    
    async def _get_queue_size(self) -> int:
        """Get current queue size"""
        if not self.redis_client:
            return 0
        
        async def _op():
            return await self.redis_client.zcard(self.leads_queue)
        return await safe_redis_operation(_op, fallback=0, operation_name="leads_zcard")
    
    # --------------------------------------------------------------------------
    # Lead Processing
    # --------------------------------------------------------------------------
    
    async def _process_lead(self, lead: Lead):
        """
        Process a single lead through the dialing pipeline
        """
        call_attempt = CallAttempt(lead=lead, start_time=time.time())
        
        try:
            logger.info("Processing lead", extra={
                "trace_id": call_attempt.trace_id,
                "contact_id": lead.lead_id,
                "lead_id": lead.lead_id,
                "campaign_id": lead.campaign_id,
                "phone_number": lead.phone_number
            })
            
            # Step 1: Pre-dial checks
            if not await self._perform_pre_dial_checks(lead):
                await self._handle_failed_attempt(call_attempt, "pre_dial_check_failed")
                return
            
            # Step 2: Rate limiting check
            if not await self.rate_limiter.can_make_call(lead.campaign_id):
                # Requeue with lower priority
                lead.priority = max(1, lead.priority - 1)
                await self.add_lead(lead)
                logger.info("Rate limit exceeded - requeued lead", extra={
                    "trace_id": call_attempt.trace_id,
                    "contact_id": lead.lead_id,
                    "campaign_id": lead.campaign_id
                })
                return
            
            # Step 2.5: Shared CB check (skip if open)
            if await _twilio_cb_is_open(self.redis_client):
                logger.warning("Twilio CB open - skipping call", extra={"campaign_id": lead.campaign_id, "lead_id": lead.lead_id})
                metrics.increment_metric("twilio_calls_blocked_by_cb_total")
                await self._handle_failed_attempt(call_attempt, "circuit_breaker_open")
                return
            
            # Step 3: Make Twilio call (guard with semaphore)
            async with self._dial_semaphore:
                call_result = await self._make_twilio_call(lead, call_attempt.trace_id)
            
            if not call_result or "error" in call_result:
                await self._handle_failed_attempt(call_attempt, "twilio_call_failed", call_result)
                return
            
            call_sid = call_result.get("call_sid")
            if not call_sid:
                await self._handle_failed_attempt(call_attempt, "no_call_sid", call_result)
                return
            
            # Step 4: Track active call
            call_attempt.call_sid = call_sid
            call_attempt.status = "dialing"
            self.active_calls[call_sid] = call_attempt
            
            await self.rate_limiter.record_call(lead.campaign_id)
            self.circuit_breaker.record_success()
            
            metrics.increment_metric("calls_initiated_total")
            metrics.set_gauge("active_calls", len(self.active_calls))
            # Avoid high-cardinality metric names; use a tag/label instead
            metrics.increment_metric("calls_per_campaign_total", tags={"campaign_id": lead.campaign_id})
            
            logger.info("Call initiated successfully", extra={
                "trace_id": call_attempt.trace_id,
                "contact_id": lead.lead_id,
                "call_sid": call_sid,
                "lead_id": lead.lead_id,
                "campaign_id": lead.campaign_id
            })
            
        except Exception as e:
            logger.error("Error processing lead", extra={
                "trace_id": call_attempt.trace_id,
                "contact_id": lead.lead_id,
                "lead_id": lead.lead_id,
                "error": str(e),
                "stack": traceback.format_exc()
            })
            await self._handle_failed_attempt(call_attempt, "processing_error", {"error": str(e)})
    
    async def _perform_pre_dial_checks(self, lead: Lead) -> bool:
        """Perform all pre-dial checks"""
        try:
            # Phone number validation
            if not validate_phone_number(lead.phone_number):
                logger.info("Lead skipped - invalid phone number format", extra={
                    "contact_id": lead.lead_id,
                    "phone_number": lead.phone_number
                })
                metrics.increment_metric("validation_blocks_total")
                return False
            
            # DNC check
            if await self.dnc_checker.is_dnc_number(lead.phone_number):
                logger.info("Lead skipped - DNC number", extra={
                    "contact_id": lead.lead_id,
                    "phone_number": lead.phone_number
                })
                metrics.increment_metric("dnc_blocks_total")
                return False
            
            # Timezone check
            if not await self.timezone_checker.is_callable_time(lead.phone_number, lead.timezone):
                logger.info("Lead skipped - outside callable hours", extra={
                    "contact_id": lead.lead_id,
                    "phone_number": lead.phone_number
                })
                metrics.increment_metric("timezone_blocks_total")
                return False
            
            # Additional checks can be added here
            # - Number validity
            # - Carrier restrictions
            # - Custom business rules
            
            return True
            
        except Exception as e:
            logger.error("Pre-dial check error", extra={
                "contact_id": lead.lead_id,
                "phone_number": lead.phone_number,
                "error": str(e)
            })
            return False
    
    async def _make_twilio_call(self, lead: Lead, trace_id: str, attempt: int = 1) -> Optional[Dict[str, Any]]:
        """Make outbound call via Twilio with retry logic"""
        try:
            call_config = {
                "to": lead.phone_number,
                "from_": self._get_caller_id(lead.campaign_id),
                "url": self._get_webhook_url(lead),
                "timeout": Config.DIAL_TIMEOUT_SECONDS,
                "status_callback": self._get_status_callback_url(lead),
                "status_events": ["initiated", "ringing", "answered", "completed"],
                "machine_detection": "Enable",
                "async_amd": "true"
            }
            
            # Use the wrapped Twilio API call with guardrails
            result = await self.twilio_wrapper.make_outbound_call_with_guardrails(
                phone_number=lead.phone_number,
                call_config=call_config,
                trace_id=trace_id,
                contact_id=lead.lead_id,
                campaign_id=lead.campaign_id
            )
            
            # Check if we should retry based on error type
            if "error" in result and attempt < Config.RETRY_ATTEMPTS:
                error_code = result.get("error_code", "")
                
                # Only retry on 5xx/429 errors (retryable)
                if self._is_retryable_twilio_error(error_code):
                    logger.info("Retrying Twilio call", extra={
                        "trace_id": trace_id,
                        "contact_id": lead.lead_id,
                        "attempt": attempt + 1,
                        "error_code": error_code
                    })
                    await _sleep_backoff(Config.RETRY_DELAY_SECONDS, attempt)
                    return await self._make_twilio_call(lead, trace_id, attempt + 1)
                else:
                    # Non-retryable error (4xx validation/auth errors)
                    logger.warning("Non-retryable Twilio error", extra={
                        "trace_id": trace_id,
                        "contact_id": lead.lead_id,
                        "error_code": error_code,
                        "error": result.get("error")
                    })
                    # Record shared CB failure for non-retryable errors
                    await _twilio_cb_record_failure(self.redis_client)
            
            # Record success for shared circuit breaker
            if result and "error" not in result:
                await _twilio_cb_record_success(self.redis_client)
            
            return result
            
        except Exception as e:
            logger.error("Twilio call failed", extra={
                "trace_id": trace_id,
                "contact_id": lead.lead_id,
                "phone_number": lead.phone_number,
                "attempt": attempt,
                "error": str(e)
            })
            
            if attempt < Config.RETRY_ATTEMPTS:
                logger.info("Retrying call", extra={
                    "trace_id": trace_id,
                    "contact_id": lead.lead_id,
                    "attempt": attempt + 1
                })
                await _sleep_backoff(Config.RETRY_DELAY_SECONDS, attempt)
                return await self._make_twilio_call(lead, trace_id, attempt + 1)
            else:
                self.circuit_breaker.record_failure()
                # Record shared CB failure when giving up after max attempts
                await _twilio_cb_record_failure(self.redis_client)
                return {"error": str(e), "error_code": "exception"}
    
    def _is_retryable_twilio_error(self, error_code: str) -> bool:
        """Determine if a Twilio error is retryable (5xx/429 only)"""
        if not error_code:
            return False
        
        # Retry on 5xx server errors and 429 rate limits
        if error_code.startswith('5') or error_code == '429':
            return True
        
        # Do not retry on 4xx validation/auth errors
        return False
    
    def _get_caller_id(self, campaign_id: str) -> str:
        """Get caller ID for campaign (placeholder implementation)"""
        # In production, this would map campaign_id to specific caller IDs
        # For now, use a default caller ID
        return "+15551234567"  # Default caller ID
    
    def _get_webhook_url(self, lead: Lead) -> str:
        """Generate webhook URL for Twilio call"""
        return f"{Config.VOICE_WEBHOOK_BASE}/twilio/voice?lead_id={lead.lead_id}&campaign_id={lead.campaign_id}"
    
    def _get_status_callback_url(self, lead: Lead) -> str:
        """Generate status callback URL for Twilio"""
        return f"{Config.STATUS_WEBHOOK_BASE}/twilio/status?lead_id={lead.lead_id}&campaign_id={lead.campaign_id}"
    
    # --------------------------------------------------------------------------
    # Call Lifecycle Management
    # --------------------------------------------------------------------------
    
    async def handle_call_connected(self, call_sid: str, websocket):
        """
        Handle call connection - attach DuplexVoiceController
        """
        try:
            if call_sid not in self.active_calls:
                logger.warning("Call connected but not in active calls", extra={"call_sid": call_sid})
                return
            
            call_attempt = self.active_calls[call_sid]
            call_attempt.status = "connected"
            
            # Attach duplex voice controller
            controller_config = {
                "campaign_id": call_attempt.lead.campaign_id,
                "lead_id": call_attempt.lead.lead_id
            }
            
            controller = await controller_manager.get_controller(call_sid, websocket, controller_config)
            if controller:
                await controller.start()
                logger.info("DuplexVoiceController attached to call", extra={
                    "trace_id": call_attempt.trace_id,
                    "contact_id": call_attempt.lead.lead_id,
                    "call_sid": call_sid
                })
            else:
                logger.error("Failed to attach DuplexVoiceController", extra={
                    "trace_id": call_attempt.trace_id,
                    "contact_id": call_attempt.lead.lead_id,
                    "call_sid": call_sid
                })
            
            metrics.increment_metric("calls_connected_total")
            
        except Exception as e:
            logger.error("Error handling call connection", extra={
                "call_sid": call_sid,
                "error": str(e),
                "stack": traceback.format_exc()
            })
    
    async def handle_call_completed(self, call_sid: str, disposition: str, metadata: Dict[str, Any]):
        """
        Handle call completion - track disposition and cleanup
        """
        try:
            if call_sid not in self.active_calls:
                logger.warning("Call completed but not in active calls", extra={"call_sid": call_sid})
                return
            
            call_attempt = self.active_calls[call_sid]
            call_attempt.status = "completed"
            call_attempt.end_time = time.time()
            
            # Track call disposition
            await track_call_disposition(call_sid, disposition, {
                **metadata,
                "lead_id": call_attempt.lead.lead_id,
                "campaign_id": call_attempt.lead.campaign_id,
                "duration": call_attempt.duration,
                "attempt_number": call_attempt.attempt_number,
                "trace_id": call_attempt.trace_id
            })
            
            # Cleanup
            await self._cleanup_call(call_sid)
            
            metrics.increment_metric("calls_completed_total")
            # Keep consistent units - using seconds for call duration
            # Ensure histogram buckets are seconds in metrics_registry.py (e.g., [1, 3, 10, 30, 60, 120, 300, 900])
            metrics.observe_latency("call_duration_seconds", call_attempt.duration)
            metrics.set_gauge("active_calls", len(self.active_calls))
            
            logger.info("Call completed", extra={
                "trace_id": call_attempt.trace_id,
                "contact_id": call_attempt.lead.lead_id,
                "call_sid": call_sid,
                "disposition": disposition,
                "duration": call_attempt.duration,
                "lead_id": call_attempt.lead.lead_id
            })
            
        except Exception as e:
            logger.error("Error handling call completion", extra={
                "call_sid": call_sid,
                "error": str(e),
                "stack": traceback.format_exc()
            })
    
    async def handle_call_failed(self, call_sid: str, error_reason: str, metadata: Dict[str, Any]):
        """
        Handle call failure - retry or mark as failed
        """
        try:
            if call_sid not in self.active_calls:
                logger.warning("Call failed but not in active calls", extra={"call_sid": call_sid})
                return
            
            call_attempt = self.active_calls[call_sid]
            call_attempt.status = "failed"
            call_attempt.end_time = time.time()
            
            logger.info("Call failed", extra={
                "trace_id": call_attempt.trace_id,
                "contact_id": call_attempt.lead.lead_id,
                "call_sid": call_sid,
                "error_reason": error_reason,
                "lead_id": call_attempt.lead.lead_id,
                "attempt_number": call_attempt.attempt_number
            })
            
            # Check if we should retry
            if (call_attempt.attempt_number < Config.RETRY_ATTEMPTS and 
                self._is_retryable_error(error_reason)):
                
                call_attempt.lead.priority = max(1, call_attempt.lead.priority - 1)
                call_attempt.attempt_number += 1
                
                # Requeue for retry
                await self.add_lead(call_attempt.lead)
                logger.info("Call requeued for retry", extra={
                    "trace_id": call_attempt.trace_id,
                    "contact_id": call_attempt.lead.lead_id,
                    "call_sid": call_sid,
                    "new_attempt": call_attempt.attempt_number
                })
                
            else:
                # Final failure - move to failed queue
                await self._move_to_failed_queue(call_attempt, error_reason)
            
            await self._cleanup_call(call_sid)
            metrics.increment_metric("call_failures_total")
            
        except Exception as e:
            logger.error("Error handling call failure", extra={
                "call_sid": call_sid,
                "error": str(e),
                "stack": traceback.format_exc()
            })
    
    async def _handle_failed_attempt(self, call_attempt: CallAttempt, failure_reason: str, 
                                   details: Optional[Dict] = None):
        """Handle a failed call attempt"""
        call_attempt.status = "failed"
        call_attempt.end_time = time.time()
        
        logger.info("Call attempt failed", extra={
            "trace_id": call_attempt.trace_id,
            "contact_id": call_attempt.lead.lead_id,
            "lead_id": call_attempt.lead.lead_id,
            "failure_reason": failure_reason,
            "details": details
        })
        
        metrics.increment_metric("call_attempt_failures_total")
        
        # Could implement retry logic here as well
        await self._move_to_failed_queue(call_attempt, failure_reason)
    
    def _is_retryable_error(self, error_reason: str) -> bool:
        """Determine if an error is retryable"""
        retryable_errors = {
            "busy", "no-answer", "timeout", "network-error", "temporary-failure"
        }
        return error_reason in retryable_errors
    
    async def _move_to_failed_queue(self, call_attempt: CallAttempt, failure_reason: str):
        """Move failed call attempt to failed queue"""
        if not self.redis_client:
            return
        
        try:
            failed_data = {
                "lead": call_attempt.lead.to_dict(),
                "failure_reason": failure_reason,
                "attempt_number": call_attempt.attempt_number,
                "failed_at": time.time(),
                "trace_id": call_attempt.trace_id
            }
            
            payload = json.dumps(failed_data)
            async def _op():
                return await self.redis_client.lpush(self.failed_queue, payload)
            await safe_redis_operation(_op, fallback=None, operation_name="failed_queue_lpush")
            
            metrics.increment_metric("leads_moved_to_failed_total")
            
        except Exception as e:
            logger.error("Failed to move lead to failed queue", extra={
                "trace_id": call_attempt.trace_id,
                "contact_id": call_attempt.lead.lead_id,
                "lead_id": call_attempt.lead.lead_id,
                "error": str(e)
            })
    
    async def _cleanup_call(self, call_sid: str):
        """Clean up call resources"""
        try:
            # Remove from active calls
            if call_sid in self.active_calls:
                del self.active_calls[call_sid]
            
            # Remove duplex controller
            await controller_manager.remove_controller(call_sid)
            
        except Exception as e:
            logger.error("Error during call cleanup", extra={
                "call_sid": call_sid,
                "error": str(e)
            })


# --------------------------------------------------------------------------
# Global Dialer Instance
# --------------------------------------------------------------------------

# Global dialer instance for shared use
_dialer_instance: Optional[OutboundDialer] = None

async def get_outbound_dialer() -> OutboundDialer:
    """Get or create global outbound dialer instance"""
    global _dialer_instance
    if _dialer_instance is None:
        _dialer_instance = OutboundDialer()
        await _dialer_instance.start()
    return _dialer_instance

async def shutdown_outbound_dialer():
    """Shutdown the global outbound dialer"""
    global _dialer_instance
    if _dialer_instance:
        await _dialer_instance.stop()
        _dialer_instance = None