"""
conversation_state_manager.py — Phase 12 (Celery Task Resilience)
Short-term in-call memory manager for context continuity with full fault tolerance, idempotency, and unified resilience patterns.
"""

import json
import time
import uuid
from typing import Dict, Any, Optional, List
import traceback

# --------------------------------------------------------------------------
# Phase 12: Configuration and Imports with Resilience Patterns
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    import os
    class Config:
        CONVERSATION_STATE_TTL = int(os.getenv("CONVERSATION_STATE_TTL", "3600"))
        MAX_STATE_SIZE_KB = int(os.getenv("MAX_STATE_SIZE_KB", "20"))
        REDIS_RETRY_ATTEMPTS = int(os.getenv("REDIS_RETRY_ATTEMPTS", "3"))
        REDIS_RETRY_DELAY = float(os.getenv("REDIS_RETRY_DELAY", "0.1"))
        CELERY_MAX_RETRIES = int(os.getenv("CELERY_MAX_RETRIES", "3"))

# Phase 12: Celery imports
try:
    from celery_app import celery
except ImportError:
    # Fallback for when Celery is not available
    class CeleryMock:
        def shared_task(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator
    celery = CeleryMock()

# Phase 12: Transient error detection
class TransientError(Exception):
    """Base class for transient errors that should trigger retries"""
    pass

# Phase 12: Structured logging
try:
    from logging_utils import get_logger
    logger = get_logger("celery_task")
except ImportError:
    import logging
    logger = logging.getLogger("celery_task")

# Phase 12: Redis client compliance
try:
    from redis_client import get_redis_client, safe_redis_operation
except ImportError:
    import os
    import redis
    redis_client_instance = None
    
    def get_redis_client():
        global redis_client_instance
        if redis_client_instance is None:
            try:
                redis_url = os.getenv("REDIS_URL")
                redis_client_instance = redis.from_url(redis_url)
            except Exception:
                redis_client_instance = None
        return redis_client_instance
    
    # Fallback safe_redis_operation
    def safe_redis_operation(operation_func, fallback=None, operation_name=None, trace_id=None, **kwargs):
        """Safe wrapper for Redis operations with error handling"""
        try:
            return operation_func()
        except Exception as e:
            logger.error(f"Redis operation failed: {operation_name}", extra={
                "error": str(e),
                "trace_id": trace_id,
                "operation": operation_name,
                "service": "conversation_state_manager"
            })
            if fallback is not None:
                return fallback
            raise

# Phase 12: Metrics integration
try:
    import metrics_collector as metrics
except ImportError:
    class metrics:
        @staticmethod
        def observe_latency(*args, **kwargs): pass
        @staticmethod
        def increment_metric(*args, **kwargs): pass
        @staticmethod
        def set_gauge(*args, **kwargs): pass
        
        # Phase 12: Add histogram support
        @staticmethod
        def histogram(*args, **kwargs): pass

# Phase 12: Circuit breaker check
def _is_circuit_breaker_open(service: str = "conversation_state") -> bool:
    """Check if circuit breaker is open for conversation state operations"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            return False
        
        state = safe_redis_operation(
            lambda client: client.get(f"circuit_breaker:{service}:state"),
            operation_name="circuit_breaker_get",
            trace_id=None
        )
        
        if not state:
            return False
        if isinstance(state, bytes):
            state = state.decode("utf-8")
        return state.lower() == "open"
    except Exception:
        return False

# --------------------------------------------------------------------------
# Data Models
# --------------------------------------------------------------------------

class ConversationState:
    """
    Phase 12: Represents the complete state of a conversation for a call with resilience patterns.
    Designed to be serialized to Redis and stay under 20 KB.
    """
    
    def __init__(self, call_sid: str):
        self.call_sid = call_sid
        self.created_at = time.time()
        self.updated_at = time.time()
        
        # Core conversation data
        self.last_user_message: Optional[str] = None
        self.last_assistant_message: Optional[str] = None
        self.conversation_history: List[Dict[str, Any]] = []
        
        # Engagement tracking
        self.engagement_score: float = 0.5  # 0.0 to 1.0
        self.talk_ratio: float = 0.0  # user speech time / total call time
        self.interruption_count: int = 0
        self.silence_duration: float = 0.0
        
        # Objection handling
        self.objections_raised: List[str] = []
        self.objections_resolved: List[str] = []
        self.current_objection: Optional[str] = None
        
        # Call context
        self.campaign_id: Optional[str] = None
        self.lead_id: Optional[str] = None
        self.call_stage: str = "greeting"  # greeting, objection_handling, closing, etc.
        self.call_duration: float = 0.0
        
        # LLM context
        self.llm_context: Dict[str, Any] = {}
        self.last_llm_prompt: Optional[str] = None
        self.last_llm_response: Optional[str] = None
        
        # Metadata - Phase 12: Schema compliance
        self.schema_v: str = "1.1"  # Updated schema version
        self.phase_tag: str = "phase_12"  # Phase identifier
        self.trace_id: str = str(uuid.uuid4())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary for serialization"""
        return {
            # Metadata - Phase 12: Schema compliance
            "call_sid": self.call_sid,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "schema_v": self.schema_v,
            "phase_tag": self.phase_tag,
            "trace_id": self.trace_id,
            
            # Conversation content
            "last_user_message": self.last_user_message,
            "last_assistant_message": self.last_assistant_message,
            "conversation_history": self.conversation_history,
            
            # Engagement metrics
            "engagement_score": self.engagement_score,
            "talk_ratio": self.talk_ratio,
            "interruption_count": self.interruption_count,
            "silence_duration": self.silence_duration,
            
            # Objection tracking
            "objections_raised": self.objections_raised,
            "objections_resolved": self.objections_resolved,
            "current_objection": self.current_objection,
            
            # Call context
            "campaign_id": self.campaign_id,
            "lead_id": self.lead_id,
            "call_stage": self.call_stage,
            "call_duration": self.call_duration,
            
            # LLM context
            "llm_context": self.llm_context,
            "last_llm_prompt": self.last_llm_prompt,
            "last_llm_response": self.last_llm_response
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConversationState':
        """Create state from dictionary with validation"""
        # Phase 12: Schema validation
        if not cls._validate_schema(data):
            raise ValueError("Invalid conversation state schema")
        
        state = cls(data["call_sid"])
        
        # Metadata - Phase 12: Schema compliance
        state.created_at = data.get("created_at", time.time())
        state.updated_at = data.get("updated_at", time.time())
        state.schema_v = data.get("schema_v", "1.1")
        state.phase_tag = data.get("phase_tag", "phase_12")
        state.trace_id = data.get("trace_id", str(uuid.uuid4()))
        
        # Conversation content
        state.last_user_message = data.get("last_user_message")
        state.last_assistant_message = data.get("last_assistant_message")
        state.conversation_history = data.get("conversation_history", [])
        
        # Engagement metrics
        state.engagement_score = data.get("engagement_score", 0.5)
        state.talk_ratio = data.get("talk_ratio", 0.0)
        state.interruption_count = data.get("interruption_count", 0)
        state.silence_duration = data.get("silence_duration", 0.0)
        
        # Objection tracking
        state.objections_raised = data.get("objections_raised", [])
        state.objections_resolved = data.get("objections_resolved", [])
        state.current_objection = data.get("current_objection")
        
        # Call context
        state.campaign_id = data.get("campaign_id")
        state.lead_id = data.get("lead_id")
        state.call_stage = data.get("call_stage", "greeting")
        state.call_duration = data.get("call_duration", 0.0)
        
        # LLM context
        state.llm_context = data.get("llm_context", {})
        state.last_llm_prompt = data.get("last_llm_prompt")
        state.last_llm_response = data.get("last_llm_response")
        
        return state
    
    @staticmethod
    def _validate_schema(data: Dict[str, Any]) -> bool:
        """Validate conversation state schema"""
        trace_id = data.get('trace_id', 'unknown')
        try:
            required_fields = ["call_sid", "schema_v", "phase_tag"]
            for field in required_fields:
                if field not in data:
                    logger.error("Missing required field in conversation state", extra={
                        "missing_field": field,
                        "trace_id": trace_id,
                        "service": "conversation_state_manager"
                    })
                    return False
            
            # Validate types
            if not isinstance(data.get("conversation_history", []), list):
                logger.error("Invalid conversation_history type", extra={
                    "trace_id": trace_id,
                    "service": "conversation_state_manager"
                })
                return False
            if not isinstance(data.get("objections_raised", []), list):
                logger.error("Invalid objections_raised type", extra={
                    "trace_id": trace_id,
                    "service": "conversation_state_manager"
                })
                return False
            if not isinstance(data.get("objections_resolved", []), list):
                logger.error("Invalid objections_resolved type", extra={
                    "trace_id": trace_id,
                    "service": "conversation_state_manager"
                })
                return False
            
            return True
        except Exception as e:
            logger.error("Schema validation failed", extra={
                "error": str(e),
                "trace_id": trace_id,
                "service": "conversation_state_manager"
            })
            return False
    
    def add_conversation_turn(self, user_message: str, assistant_message: str):
        """Add a conversation turn to history"""
        turn = {
            "user": user_message,
            "assistant": assistant_message,
            "timestamp": time.time(),
            "turn_number": len(self.conversation_history) + 1
        }
        
        self.conversation_history.append(turn)
        
        # Keep history manageable (last 20 turns)
        if len(self.conversation_history) > 20:
            self.conversation_history = self.conversation_history[-20:]
        
        self.last_user_message = user_message
        self.last_assistant_message = assistant_message
        self.updated_at = time.time()
    
    def add_objection(self, objection: str):
        """Add a new objection"""
        if objection not in self.objections_raised:
            self.objections_raised.append(objection)
            self.current_objection = objection
            self.updated_at = time.time()
    
    def resolve_objection(self, objection: str):
        """Mark an objection as resolved"""
        if objection in self.objections_raised and objection not in self.objections_resolved:
            self.objections_resolved.append(objection)
            if self.current_objection == objection:
                self.current_objection = None
            self.updated_at = time.time()
    
    def update_engagement_score(self, score: float):
        """Update engagement score with bounds checking"""
        self.engagement_score = max(0.0, min(1.0, score))
        self.updated_at = time.time()
    
    def increment_interruption_count(self):
        """Increment interruption counter"""
        self.interruption_count += 1
        self.updated_at = time.time()
    
    def record_barge_in(self):
        """Record a barge-in event with metrics"""
        self.interruption_count += 1
        self.updated_at = time.time()
        metrics.increment_metric("barge_in_events_total")
        logger.info("Barge-in event recorded", extra={
            "call_sid": self.call_sid,
            "trace_id": self.trace_id,
            "total_interruptions": self.interruption_count,
            "service": "conversation_state_manager"
        })
    
    def get_estimated_size_kb(self) -> float:
        """Estimate the size of the state in KB"""
        try:
            serialized = json.dumps(self.to_dict())
            return len(serialized.encode('utf-8')) / 1024.0
        except Exception:
            return 0.0
    
    def is_too_large(self) -> bool:
        """Check if state exceeds size limits"""
        return self.get_estimated_size_kb() > Config.MAX_STATE_SIZE_KB

# --------------------------------------------------------------------------
# Conversation State Manager with Phase 12 Resilience
# --------------------------------------------------------------------------

class ConversationStateManager:
    """
    Phase 12: Manages short-term in-call memory for context continuity with full resilience patterns.
    Handles Redis storage with TTL and size limits.
    """
    
    def __init__(self):
        self.redis_client = get_redis_client()
        self.redis_available = self.redis_client is not None
        
        logger.info("ConversationStateManager initialized", extra={
            "redis_available": self.redis_available,
            "max_state_size_kb": Config.MAX_STATE_SIZE_KB,
            "default_ttl": Config.CONVERSATION_STATE_TTL,
            "service": "conversation_state_manager"
        })
    
    def _get_redis_key(self, call_sid: str) -> str:
        """Get Redis key for conversation state"""
        return f"conversation_state:{call_sid}"
    
    async def save_state(self, state: ConversationState) -> bool:
        """
        Save conversation state to Redis with TTL and Phase 12 resilience.
        Returns: True if successful
        """
        # Phase 12: Circuit breaker check
        if _is_circuit_breaker_open("conversation_state"):
            logger.warning("Conversation state save blocked by circuit breaker", extra={
                "call_sid": state.call_sid,
                "trace_id": state.trace_id,
                "service": "conversation_state_manager"
            })
            metrics.increment_metric("conversation_state_circuit_breaker_hits_total")
            return False
        
        if not self.redis_client:
            logger.warning("Redis client not available - cannot save state", extra={
                "call_sid": state.call_sid,
                "trace_id": state.trace_id,
                "service": "conversation_state_manager"
            })
            return False
        
        start_time = time.time()
        
        try:
            # Check size limits
            if state.is_too_large():
                await self._handle_oversized_state(state)
                return False
            
            # Prepare data for storage
            state_data = state.to_dict()
            redis_key = self._get_redis_key(state.call_sid)
            
            # Phase 12: Write-once session creation with SETNX
            created = safe_redis_operation(
                lambda client: client.set(
                    redis_key,
                    json.dumps(state_data),
                    ex=Config.CONVERSATION_STATE_TTL,
                    nx=True  # Only set if not exists
                ),
                operation_name="redis_state_create_nx",
                trace_id=state.trace_id
            )
            
            if created:
                # New session created successfully
                operation_type = "create"
            else:
                # Session exists, update with TTL
                safe_redis_operation(
                    lambda client: client.setex(
                        redis_key,
                        Config.CONVERSATION_STATE_TTL,
                        json.dumps(state_data)
                    ),
                    operation_name="redis_state_update_ex",
                    trace_id=state.trace_id
                )
                operation_type = "update"
            
            latency_ms = (time.time() - start_time) * 1000
            
            # Phase 12: Metrics with operation type
            metrics.increment_metric("conversation_state_writes_total", op=operation_type)
            metrics.histogram("conversation_state_write_latency_ms", latency_ms)
            
            if created or operation_type == "update":
                logger.info("Conversation state saved", extra={
                    "call_sid": state.call_sid,
                    "trace_id": state.trace_id,
                    "size_kb": state.get_estimated_size_kb(),
                    "latency_ms": latency_ms,
                    "turn_count": len(state.conversation_history),
                    "operation": operation_type,
                    "service": "conversation_state_manager"
                })
                return True
            else:
                metrics.increment_metric("conversation_state_save_errors_total")
                logger.error("Failed to save conversation state", extra={
                    "call_sid": state.call_sid,
                    "trace_id": state.trace_id,
                    "operation": operation_type,
                    "service": "conversation_state_manager"
                })
                return False
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            metrics.increment_metric("conversation_state_save_errors_total")
            logger.error("Error saving conversation state", extra={
                "call_sid": state.call_sid,
                "trace_id": state.trace_id,
                "error": str(e),
                "latency_ms": latency_ms,
                "stack": traceback.format_exc(),
                "service": "conversation_state_manager"
            })
            return False
    
    async def load_state(self, call_sid: str, trace_id: Optional[str] = None) -> Optional[ConversationState]:
        """
        Load conversation state from Redis with Phase 12 resilience.
        Returns: ConversationState or None if not found
        """
        # Phase 12: Circuit breaker check
        if _is_circuit_breaker_open("conversation_state"):
            logger.warning("Conversation state load blocked by circuit breaker", extra={
                "call_sid": call_sid,
                "trace_id": trace_id,
                "service": "conversation_state_manager"
            })
            metrics.increment_metric("conversation_state_circuit_breaker_hits_total")
            return None
        
        if not self.redis_client:
            logger.warning("Redis client not available - cannot load state", extra={
                "call_sid": call_sid,
                "trace_id": trace_id,
                "service": "conversation_state_manager"
            })
            return None
        
        start_time = time.time()
        resolved_trace_id = trace_id or str(uuid.uuid4())
        
        try:
            redis_key = self._get_redis_key(call_sid)
            
            # Phase 12: Use safe_redis_operation (sync)
            state_data_str = safe_redis_operation(
                lambda client: client.get(redis_key),
                operation_name="redis_state_load",
                trace_id=resolved_trace_id
            )
            
            if not state_data_str:
                logger.debug("No conversation state found", extra={
                    "call_sid": call_sid,
                    "trace_id": resolved_trace_id,
                    "service": "conversation_state_manager"
                })
                return None
            
            # Parse state data with validation
            state_data = json.loads(state_data_str)
            state = ConversationState.from_dict(state_data)
            
            latency_ms = (time.time() - start_time) * 1000
            metrics.increment_metric("conversation_state_reads_total")
            metrics.histogram("conversation_state_read_latency_ms", latency_ms)
            
            logger.info("Conversation state loaded", extra={
                "call_sid": call_sid,
                "trace_id": resolved_trace_id,
                "size_kb": state.get_estimated_size_kb(),
                "latency_ms": latency_ms,
                "age_seconds": time.time() - state.updated_at,
                "schema_v": state.schema_v,
                "phase_tag": state.phase_tag,
                "service": "conversation_state_manager"
            })
            
            return state
            
        except json.JSONDecodeError as e:
            metrics.increment_metric("conversation_state_load_errors_total")
            logger.error("Failed to parse conversation state JSON", extra={
                "call_sid": call_sid,
                "trace_id": resolved_trace_id,
                "error": str(e),
                "service": "conversation_state_manager"
            })
            return None
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            metrics.increment_metric("conversation_state_load_errors_total")
            logger.error("Error loading conversation state", extra={
                "call_sid": call_sid,
                "trace_id": resolved_trace_id,
                "error": str(e),
                "latency_ms": latency_ms,
                "stack": traceback.format_exc(),
                "service": "conversation_state_manager"
            })
            return None
    
    async def delete_state(self, call_sid: str, trace_id: Optional[str] = None) -> bool:
        """
        Delete conversation state from Redis with Phase 12 resilience.
        Returns: True if successful or state didn't exist
        """
        # Phase 12: Circuit breaker check
        if _is_circuit_breaker_open("conversation_state"):
            logger.warning("Conversation state delete blocked by circuit breaker", extra={
                "call_sid": call_sid,
                "trace_id": trace_id,
                "service": "conversation_state_manager"
            })
            metrics.increment_metric("conversation_state_circuit_breaker_hits_total")
            return False
        
        if not self.redis_client:
            logger.warning("Redis client not available - cannot delete state", extra={
                "call_sid": call_sid,
                "trace_id": trace_id,
                "service": "conversation_state_manager"
            })
            return False
        
        start_time = time.time()
        resolved_trace_id = trace_id or str(uuid.uuid4())
        
        try:
            redis_key = self._get_redis_key(call_sid)
            
            # Phase 12: Use safe_redis_operation (sync)
            result = safe_redis_operation(
                lambda client: client.delete(redis_key),
                operation_name="redis_state_delete",
                trace_id=resolved_trace_id
            )
            
            latency_ms = (time.time() - start_time) * 1000
            metrics.increment_metric("conversation_state_writes_total", op="delete")
            metrics.histogram("conversation_state_delete_latency_ms", latency_ms)
            
            logger.info("Conversation state deleted", extra={
                "call_sid": call_sid,
                "trace_id": resolved_trace_id,
                "result": result,
                "latency_ms": latency_ms,
                "service": "conversation_state_manager"
            })
            
            return True
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            metrics.increment_metric("conversation_state_delete_errors_total")
            logger.error("Error deleting conversation state", extra={
                "call_sid": call_sid,
                "trace_id": resolved_trace_id,
                "error": str(e),
                "latency_ms": latency_ms,
                "stack": traceback.format_exc(),
                "service": "conversation_state_manager"
            })
            return False
    
    async def _handle_oversized_state(self, state: ConversationState):
        """
        Handle oversized conversation state by trimming history.
        """
        original_size = state.get_estimated_size_kb()
        logger.warning("Conversation state oversized, trimming history", extra={
            "call_sid": state.call_sid,
            "trace_id": state.trace_id,
            "current_size_kb": original_size,
            "max_size_kb": Config.MAX_STATE_SIZE_KB,
            "service": "conversation_state_manager"
        })
        
        # Trim conversation history to reduce size
        if len(state.conversation_history) > 10:
            state.conversation_history = state.conversation_history[-10:]
            new_size = state.get_estimated_size_kb()
            logger.info("Trimmed conversation history", extra={
                "call_sid": state.call_sid,
                "trace_id": state.trace_id,
                "original_size_kb": original_size,
                "new_size_kb": new_size,
                "under_limit": new_size <= Config.MAX_STATE_SIZE_KB,
                "service": "conversation_state_manager"
            })
        
        metrics.increment_metric("conversation_state_oversized_total")

# --------------------------------------------------------------------------
# Phase 12: Celery Tasks with Resilience Patterns - FIXED ASYNC/SYNC MISMATCH
# --------------------------------------------------------------------------

@celery.shared_task(
    bind=True,
    autoretry_for=(TransientError,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={'max_retries': Config.CELERY_MAX_RETRIES},
    acks_late=True,
    name="conversation_state.save_state_async"
)
async def save_state_async(self, state_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase 12: Celery task for async conversation state saving with full resilience.
    """
    # Phase 12: Idempotency key pattern with safe_redis_operation
    task_id = f"task:{self.request.id}"
    redis_client = get_redis_client()
    
    if redis_client:
        task_exists = safe_redis_operation(
            lambda client: client.exists(task_id),
            operation_name="idempotency_check",
            trace_id=state_data.get('trace_id')
        )
        
        if task_exists:
            logger.info("Task duplicate detected, skipping execution", extra={
                "task": self.name, 
                "id": self.request.id,
                "call_sid": state_data.get('call_sid', 'unknown'),
                "trace_id": state_data.get('trace_id', 'unknown'),
                "service": "conversation_state_manager"
            })
            return {"ok": True, "status": "duplicate_skipped"}
        
        safe_redis_operation(
            lambda client: client.setex(task_id, 3600, "done"),
            operation_name="idempotency_set",
            trace_id=state_data.get('trace_id')
        )

    call_sid = state_data.get('call_sid', 'unknown')
    trace_id = state_data.get('trace_id', str(uuid.uuid4()))
    
    # Phase 12: Task-specific metrics
    metrics.increment_metric("celery_task_started", task=self.name)
    logger.info("Task started", extra={
        "task": self.name, 
        "id": self.request.id, 
        "call_sid": call_sid,
        "trace_id": trace_id,
        "service": "conversation_state_manager"
    })
    
    start_time = time.time()
    
    try:
        # Update task state
        self.update_state(
            state="PROGRESS",
            meta={
                "call_sid": call_sid,
                "trace_id": trace_id,
                "stage": "saving_state"
            }
        )
        
        # Create state object from data
        state = ConversationState.from_dict(state_data)
        
        # Save state using manager - NOW PROPERLY AWAITED
        manager = ConversationStateManager()
        success = await manager.save_state(state)
        
        latency_ms = (time.time() - start_time) * 1000
        metrics.histogram("turn_processing_latency_ms", latency_ms)
        
        if success:
            metrics.increment_metric("celery_task_completed", task=self.name)
            logger.info("Task completed successfully", extra={
                "task": self.name, 
                "id": self.request.id, 
                "call_sid": call_sid,
                "trace_id": trace_id,
                "processing_time_ms": latency_ms,
                "service": "conversation_state_manager"
            })
            return {
                "ok": True,
                "call_sid": call_sid,
                "trace_id": trace_id,
                "processing_time_ms": latency_ms
            }
        else:
            metrics.increment_metric("celery_task_failed", task=self.name)
            logger.error("Task failed to save state", extra={
                "task": self.name, 
                "id": self.request.id, 
                "call_sid": call_sid,
                "trace_id": trace_id,
                "processing_time_ms": latency_ms,
                "service": "conversation_state_manager"
            })
            raise TransientError("Failed to save conversation state")
            
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        metrics.increment_metric("celery_task_failed", task=self.name)
        logger.error("Task failed", extra={
            "task": self.name, 
            "id": self.request.id, 
            "call_sid": call_sid,
            "trace_id": trace_id,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "processing_time_ms": latency_ms,
            "service": "conversation_state_manager"
        })
        # Phase 12: Let Celery handle retries via decorator for transient errors
        if isinstance(e, TransientError):
            raise
        else:
            # For non-transient errors, don't retry
            return {"ok": False, "error": str(e)}

@celery.shared_task(
    bind=True,
    autoretry_for=(TransientError,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={'max_retries': Config.CELERY_MAX_RETRIES},
    acks_late=True,
    name="conversation_state.load_state_async"
)
async def load_state_async(self, call_sid: str, trace_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Phase 12: Celery task for async conversation state loading with full resilience.
    """
    # Phase 12: Idempotency key pattern with safe_redis_operation
    task_id = f"task:{self.request.id}"
    redis_client = get_redis_client()
    
    if redis_client:
        task_exists = safe_redis_operation(
            lambda client: client.exists(task_id),
            operation_name="idempotency_check",
            trace_id=trace_id
        )
        
        if task_exists:
            logger.info("Task duplicate detected, skipping execution", extra={
                "task": self.name, 
                "id": self.request.id,
                "call_sid": call_sid,
                "trace_id": trace_id,
                "service": "conversation_state_manager"
            })
            return {"ok": True, "status": "duplicate_skipped"}
        
        safe_redis_operation(
            lambda client: client.setex(task_id, 3600, "done"),
            operation_name="idempotency_set",
            trace_id=trace_id
        )

    resolved_trace_id = trace_id or str(uuid.uuid4())
    
    # Phase 12: Task-specific metrics
    metrics.increment_metric("celery_task_started", task=self.name)
    logger.info("Task started", extra={
        "task": self.name, 
        "id": self.request.id, 
        "call_sid": call_sid,
        "trace_id": resolved_trace_id,
        "service": "conversation_state_manager"
    })
    
    start_time = time.time()
    
    try:
        # Update task state
        self.update_state(
            state="PROGRESS",
            meta={
                "call_sid": call_sid,
                "trace_id": resolved_trace_id,
                "stage": "loading_state"
            }
        )
        
        # Load state using manager - NOW PROPERLY AWAITED
        manager = ConversationStateManager()
        state = await manager.load_state(call_sid, resolved_trace_id)
        
        latency_ms = (time.time() - start_time) * 1000
        metrics.histogram("turn_processing_latency_ms", latency_ms)
        
        if state:
            metrics.increment_metric("celery_task_completed", task=self.name)
            logger.info("Task completed successfully", extra={
                "task": self.name, 
                "id": self.request.id, 
                "call_sid": call_sid,
                "trace_id": resolved_trace_id,
                "processing_time_ms": latency_ms,
                "service": "conversation_state_manager"
            })
            return {
                "ok": True,
                "call_sid": call_sid,
                "trace_id": resolved_trace_id,
                "state": state.to_dict(),
                "processing_time_ms": latency_ms
            }
        else:
            metrics.increment_metric("celery_task_completed", task=self.name)
            logger.info("Task completed - no state found", extra={
                "task": self.name, 
                "id": self.request.id, 
                "call_sid": call_sid,
                "trace_id": resolved_trace_id,
                "processing_time_ms": latency_ms,
                "service": "conversation_state_manager"
            })
            return {
                "ok": True,
                "call_sid": call_sid,
                "trace_id": resolved_trace_id,
                "state": None,
                "processing_time_ms": latency_ms
            }
            
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        metrics.increment_metric("celery_task_failed", task=self.name)
        logger.error("Task failed", extra={
            "task": self.name, 
            "id": self.request.id, 
            "call_sid": call_sid,
            "trace_id": resolved_trace_id,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "processing_time_ms": latency_ms,
            "service": "conversation_state_manager"
        })
        # Phase 12: Let Celery handle retries via decorator for transient errors
        if isinstance(e, TransientError):
            raise
        else:
            # For non-transient errors, don't retry
            return {"ok": False, "error": str(e)}

@celery.shared_task(
    bind=True,
    autoretry_for=(TransientError,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={'max_retries': Config.CELERY_MAX_RETRIES},
    acks_late=True,
    name="conversation_state.delete_state_async"
)
async def delete_state_async(self, call_sid: str, trace_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Phase 12: Celery task for async conversation state deletion with full resilience.
    """
    # Phase 12: Idempotency key pattern with safe_redis_operation
    task_id = f"task:{self.request.id}"
    redis_client = get_redis_client()
    
    if redis_client:
        task_exists = safe_redis_operation(
            lambda client: client.exists(task_id),
            operation_name="idempotency_check",
            trace_id=trace_id
        )
        
        if task_exists:
            logger.info("Task duplicate detected, skipping execution", extra={
                "task": self.name, 
                "id": self.request.id,
                "call_sid": call_sid,
                "trace_id": trace_id,
                "service": "conversation_state_manager"
            })
            return {"ok": True, "status": "duplicate_skipped"}
        
        safe_redis_operation(
            lambda client: client.setex(task_id, 3600, "done"),
            operation_name="idempotency_set",
            trace_id=trace_id
        )

    resolved_trace_id = trace_id or str(uuid.uuid4())
    
    # Phase 12: Task-specific metrics
    metrics.increment_metric("celery_task_started", task=self.name)
    logger.info("Task started", extra={
        "task": self.name, 
        "id": self.request.id, 
        "call_sid": call_sid,
        "trace_id": resolved_trace_id,
        "service": "conversation_state_manager"
    })
    
    start_time = time.time()
    
    try:
        # Update task state
        self.update_state(
            state="PROGRESS",
            meta={
                "call_sid": call_sid,
                "trace_id": resolved_trace_id,
                "stage": "deleting_state"
            }
        )
        
        # Delete state using manager - NOW PROPERLY AWAITED
        manager = ConversationStateManager()
        success = await manager.delete_state(call_sid, resolved_trace_id)
        
        latency_ms = (time.time() - start_time) * 1000
        metrics.histogram("turn_processing_latency_ms", latency_ms)
        
        metrics.increment_metric("celery_task_completed", task=self.name)
        logger.info("Task completed successfully", extra={
            "task": self.name, 
            "id": self.request.id, 
            "call_sid": call_sid,
            "trace_id": resolved_trace_id,
            "processing_time_ms": latency_ms,
            "success": success,
            "service": "conversation_state_manager"
        })
        
        return {
            "ok": True,
            "call_sid": call_sid,
            "trace_id": resolved_trace_id,
            "deleted": success,
            "processing_time_ms": latency_ms
        }
            
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        metrics.increment_metric("celery_task_failed", task=self.name)
        logger.error("Task failed", extra={
            "task": self.name, 
            "id": self.request.id, 
            "call_sid": call_sid,
            "trace_id": resolved_trace_id,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "processing_time_ms": latency_ms,
            "service": "conversation_state_manager"
        })
        # Phase 12: Let Celery handle retries via decorator for transient errors
        if isinstance(e, TransientError):
            raise
        else:
            # For non-transient errors, don't retry
            return {"ok": False, "error": str(e)}

# --------------------------------------------------------------------------
# Export public interface
# --------------------------------------------------------------------------

__all__ = [
    "ConversationState",
    "ConversationStateManager", 
    "save_state_async",
    "load_state_async", 
    "delete_state_async",
    "TransientError"
]