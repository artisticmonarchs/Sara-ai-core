"""
duplex_voice_controller.py — Phase 12 (Celery Task Resilience)
Real-time full-duplex conversation controller with full fault tolerance, idempotency, and unified resilience patterns.
"""

import asyncio
import json
import time
import uuid
import os
import audioop
import queue  # ADDED: For thread-safe sync queue
from typing import Optional, Dict, Any, Callable, List
import traceback
import threading  # ADDED: For sync method threading

# Phase 12: Core imports with resilience patterns
try:
    from realtime_voice_engine import RealtimeVoiceEngine
except ImportError:
    # Fallback for development
    class RealtimeVoiceEngine:
        def __init__(self): pass
        async def start(self): pass
        async def stop(self): pass

try:
    from gpt_client import generate_reply
except ImportError:
    async def generate_reply(text, trace_id=None):
        return f"Echo: {text}"

# Phase 12: Configuration
try:
    from config import Config
except ImportError:
    import os
    class Config:
        CELERY_MAX_RETRIES = int(os.getenv("CELERY_MAX_RETRIES", "3"))
        CALL_STATE_TTL = int(os.getenv("CALL_STATE_TTL", "3600"))
        MAX_CONVERSATION_TURNS = int(os.getenv("MAX_CONVERSATION_TURNS", "20"))
        MAX_CONSECUTIVE_ERRORS = int(os.getenv("MAX_CONSECUTIVE_ERRORS", "5"))
        INTERRUPTION_DEBOUNCE_MS = int(os.getenv("INTERRUPTION_DEBOUNCE_MS", "150"))
        TTS_CANCEL_TIMEOUT = float(os.getenv("TTS_CANCEL_TIMEOUT", "0.15"))

# Phase 12: Transient error detection
class TransientError(Exception):
    """Base class for transient errors that should trigger retries"""
    pass

# Phase 12: Metrics integration
try:
    import metrics_collector as metrics
except ImportError:
    class metrics:
        @staticmethod
        def observe_latency(*args, **kwargs): pass
        @staticmethod
        def increment_metric(*args, **kwargs): pass

# Phase 12: Redis client compliance
try:
    from redis_client import get_client as get_redis_client
except ImportError:
    def get_redis_client():
        return None

# Phase 12: Structured logging
try:
    from logging_utils import get_logger
    logger = get_logger("duplex_voice_controller")
except ImportError:
    import logging
    logger = logging.getLogger("duplex_voice_controller")

# Phase 12: External API wrapper
try:
    from core.utils.external_api import external_api_call
except ImportError:
    def external_api_call(service, operation, *args, trace_id=None, **kwargs):
        return operation(*args, **kwargs)

# Phase 12: Jitter buffer import for inbound audio
try:
    from voice_pipeline import JitterBuffer
except ImportError:
    # Fallback implementation
    class JitterBuffer:
        def __init__(self):
            self.buffer = bytearray()
        def push(self, pcm_bytes: bytes):
            self.buffer.extend(pcm_bytes)
        def pop_available(self) -> bytes:
            data = bytes(self.buffer)
            self.buffer.clear()
            return data
        def has_data(self) -> bool:
            return len(self.buffer) > 0

# --------------------------------------------------------------------------
# Controller Manager (Singleton) with Resilience - MOVED UP
# --------------------------------------------------------------------------

class DuplexControllerManager:
    """
    Phase 12: Manages multiple DuplexVoiceController instances with resilience patterns
    """
    
    def __init__(self):
        self.controllers: Dict[str, 'DuplexVoiceController'] = {}  # CHANGED: Forward reference
        self._lock = asyncio.Lock()
        # ADDED: Event loop for sync method execution
        self._event_loop = None
        self._event_loop_thread = None
    
    async def get_controller(self, call_sid: str, websocket, config: Optional[Dict] = None) -> 'DuplexVoiceController':  # CHANGED: Forward reference
        """Get or create controller for call_sid with Phase 12 resilience"""
        async with self._lock:
            if call_sid not in self.controllers:
                from . import DuplexVoiceController  # CHANGED: Local import to avoid circular reference
                self.controllers[call_sid] = DuplexVoiceController(call_sid, websocket, config)
                logger.info("New controller created", extra={
                    "call_sid": call_sid,
                    "service": "streaming_server",
                    "controller": "duplex"
                })
            
            return self.controllers[call_sid]
    
    def get_or_create_controller_sync(self, call_sid: str, stream_sid: str, trace_id: Optional[str] = None, websocket = None) -> 'DuplexVoiceController':
        """
        Synchronous adapter for get_or_create controller.
        Called from Flask /media loop to attach controller to Twilio media session.
        """
        try:
            # Use existing async method via thread-safe execution
            if self._event_loop is None:
                self._event_loop = asyncio.new_event_loop()
                self._event_loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
                self._event_loop_thread.start()
            
            # CHANGED: Handle case where websocket is None (Flask media handler)
            # Create a minimal websocket-like object if None
            ws_for_controller = websocket
            if ws_for_controller is None:
                class MinimalWebSocket:
                    def __init__(self):
                        self.closed = False
                    async def close(self):
                        self.closed = True
                ws_for_controller = MinimalWebSocket()
            
            # Run async method in dedicated event loop
            future = asyncio.run_coroutine_threadsafe(
                self.get_controller(call_sid, ws_for_controller, config={"trace_id": trace_id}),
                self._event_loop
            )
            controller = future.result(timeout=5.0)
            
            # CHANGED: Always set stream_sid and metadata
            controller.stream_sid = stream_sid
            if trace_id:
                controller.trace_id = trace_id
            
            logger.info("Controller retrieved/created via sync adapter", extra={
                "call_sid": call_sid,
                "stream_sid": stream_sid,
                "trace_id": trace_id,
                "service": "streaming_server",
                "controller": "duplex",
                "event": "duplex_controller_created"  # ADDED: Explicit event marker
            })
            
            return controller
            
        except Exception as e:
            logger.error("Error in get_or_create_controller_sync", extra={
                "call_sid": call_sid,
                "stream_sid": stream_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
            raise
    
    async def remove_controller(self, call_sid: str):
        """Remove and cleanup controller with Phase 12 resilience"""
        async with self._lock:
            if call_sid in self.controllers:
                controller = self.controllers[call_sid]
                await controller.stop()
                del self.controllers[call_sid]
                logger.info("Controller removed", extra={
                    "call_sid": call_sid,
                    "service": "streaming_server",
                    "controller": "duplex"
                })
    
    def cleanup_controller_sync(self, call_sid: str, stream_sid: str, trace_id: Optional[str] = None):
        """
        Synchronous adapter for controller cleanup.
        Called from Flask /media loop on "stop" or cleanup.
        """
        try:
            # Use existing async method via thread-safe execution
            if self._event_loop is None:
                self._event_loop = asyncio.new_event_loop()
                self._event_loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
                self._event_loop_thread.start()
            
            # Check if controller exists before attempting removal
            if call_sid in self.controllers:
                # Call stop_sync on controller first
                controller = self.controllers[call_sid]
                if hasattr(controller, 'stop_sync'):
                    controller.stop_sync()
                
                # Then remove from manager
                future = asyncio.run_coroutine_threadsafe(
                    self.remove_controller(call_sid),
                    self._event_loop
                )
                future.result(timeout=5.0)
                
                logger.info("Controller cleaned up via sync adapter", extra={
                    "call_sid": call_sid,
                    "stream_sid": stream_sid,
                    "trace_id": trace_id,
                    "service": "streaming_server",
                    "controller": "duplex"
                })
            else:
                logger.debug("Controller already removed, skipping cleanup", extra={
                    "call_sid": call_sid,
                    "stream_sid": stream_sid,
                    "service": "streaming_server",
                    "controller": "duplex"
                })
                
        except Exception as e:
            logger.error("Error in cleanup_controller_sync", extra={
                "call_sid": call_sid,
                "stream_sid": stream_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
            # Don't raise - cleanup should be best-effort
    
    def _run_event_loop(self):
        """Run dedicated event loop for sync method execution"""
        asyncio.set_event_loop(self._event_loop)
        self._event_loop.run_forever()
    
    def get_active_controllers(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all active controllers with resilience information"""
        return {
            call_sid: controller.get_status()
            for call_sid, controller in self.controllers.items()
        }
    
    async def get_controller_heartbeat(self, call_sid: str) -> Optional[Dict[str, Any]]:
        """Get heartbeat for specific controller with resilience checks"""
        if call_sid in self.controllers:
            return await self.controllers[call_sid].heartbeat()
        return None

    async def health_check(self) -> Dict[str, Any]:
        """Health check for controller manager with Phase 12 resilience"""
        try:
            active_controllers = len(self.controllers)
            circuit_breaker_open = _is_circuit_breaker_open("duplex_controller")
            
            status = "healthy"
            if circuit_breaker_open:
                status = "degraded"
            
            return {
                "service": "duplex_controller_manager",
                "status": status,
                "active_controllers": active_controllers,
                "circuit_breaker_open": circuit_breaker_open
            }
        except Exception as e:
            return {
                "service": "duplex_controller_manager",
                "status": "unhealthy",
                "error": str(e)
            }


# Phase 12: Global manager instance - MOVED UP
controller_manager = DuplexControllerManager()

# Phase 12: Circuit breaker check
def _is_circuit_breaker_open(service: str = "duplex_controller") -> bool:
    """Check if circuit breaker is open for duplex operations"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            return False
        state = redis_client.get(f"circuit_breaker:{service}:state")
        if not state:
            return False
        if isinstance(state, bytes):
            state = state.decode("utf-8")
        return state.lower() == "open"
    except Exception:
        return False

# Phase 12: Idempotency key helper
def _check_idempotency_key(operation_id: str) -> bool:
    """Check if operation has already been executed"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            return False
            
        idempotency_key = f"duplex_controller:{operation_id}"
        if redis_client.exists(idempotency_key):
            return True
            
        # Set key with 1-hour TTL
        redis_client.setex(idempotency_key, 3600, "executed")
        return False
    except Exception:
        return False


# --------------------------------------------------------------------------
# DuplexVoiceController Class
# --------------------------------------------------------------------------

class DuplexVoiceController:
    """
    Phase 12: Real-time full-duplex conversation controller with full resilience patterns.
    Maintains ASR listening while TTS streams with instant interruption capability.
    """
    
    def __init__(self, call_sid: str, websocket, config: Optional[Dict[str, Any]] = None):
        self.call_sid = call_sid
        self.websocket = websocket
        self.config = config or {}
        
        # Core components
        self.voice_engine = RealtimeVoiceEngine()
        self.redis_client = get_redis_client()
        
        # State management
        self.is_active = False
        self.is_playing = False
        self.is_listening = False
        self.last_interruption_time = 0
        self.last_voice_detection_time = 0
        self.conversation_history = []
        
        # Async control
        self._asr_task = None
        self._tts_task = None
        self._control_task = None
        self._logging_task = None
        self._stop_event = asyncio.Event()
        
        # Circuit breaker
        self.consecutive_errors = 0
        self.last_error_time = 0
        
        # Performance tracking
        self.trace_id = str(uuid.uuid4())
        self.start_time = time.time()
        self.last_context_log_time = time.time()
        self.last_activity_time = time.time()
        
        # Twilio media session for outbound audio queue
        self.twilio_media_session = None
        
        # Phase 12: Per-call state for inbound/outbound audio bridging
        self.jitter_buffer = JitterBuffer()  # Inbound buffer for STT
        self.outbound_frames = asyncio.Queue()  # Thread-safe queue of PCM frames for Twilio
        self._sync_outbound_frames = queue.Queue(maxsize=1000)  # CHANGED: Thread-safe queue.Queue for sync access
        self.greeting_sent = False  # Flag to track if initial greeting has been sent
        
        # ADDED: Stream metadata and event loop for sync methods
        self.stream_sid = None
        self._sync_event_loop = None
        self._sync_event_loop_thread = None
        
        # ADDED: Inbound queue for non-blocking audio processing
        self._inbound_queue = queue.Queue(maxsize=1000)
        self._inbound_worker_task = None
        self._closed = False  # ADDED: Closed flag for lifecycle management
        
        # ADDED: Echo mode support
        self.echo_mode = config.get("echo_mode", False) if config else False
        self._echo_frames = queue.Queue(maxsize=1000)  # Thread-safe echo queue
        
        logger.info("DuplexVoiceController initialized", extra={
            "call_sid": call_sid,
            "trace_id": self.trace_id,
            "service": "streaming_server",
            "controller": "duplex",
            "event": "duplex_controller_initialized",  # ADDED: Explicit event marker
            "echo_mode": self.echo_mode
        })
    
    # --------------------------------------------------------------------------
    # Static Accessor Methods for External Modules
    # --------------------------------------------------------------------------
    
    @staticmethod
    async def get_instance(call_sid: str, websocket = None, config: Optional[Dict[str, Any]] = None) -> 'DuplexVoiceController':
        """
        Static method to get controller instance for compatibility with existing code.
        Uses the global controller_manager to ensure consistent access patterns.
        """
        # CHANGED: Access the global controller_manager that's now defined above
        return await controller_manager.get_controller(call_sid, websocket, config)
    
    @staticmethod
    async def get_existing_controller(call_sid: str) -> Optional['DuplexVoiceController']:
        """
        Get existing controller without creating a new one.
        Used by voice pipeline and other modules that need to access existing controllers.
        """
        # Access the controller manager's internal dictionary safely
        if hasattr(controller_manager, 'controllers') and call_sid in controller_manager.controllers:
            return controller_manager.controllers[call_sid]
        return None
    
    @staticmethod
    async def remove_controller(call_sid: str):
        """Remove controller instance - delegates to controller manager"""
        await controller_manager.remove_controller(call_sid)
    
    # --------------------------------------------------------------------------
    # Twilio Media Session Attachment
    # --------------------------------------------------------------------------
    
    def attach_twilio_session(self, media_session):
        """
        Attach Twilio media session for outbound audio queue
        Called when /media receives "start" to establish the session
        """
        self.twilio_media_session = media_session
        logger.info("Twilio media session attached to controller", extra={
            "call_sid": self.call_sid,
            "trace_id": self.trace_id,
            "service": "streaming_server",
            "controller": "duplex"
        })
    
    # --------------------------------------------------------------------------
    # Phase 12: Sync Adapter Methods for Server Calls
    # --------------------------------------------------------------------------
    
    def handle_inbound_audio_sync(self, pcm_bytes: bytes):
        """
        Synchronous adapter for inbound audio processing.
        Called from Flask /media loop when Twilio sends "media" frames.
        Non-blocking - schedules async processing.
        """
        try:
            if self._closed:
                return
            
            # Push pcm_bytes into thread-safe inbound queue
            try:
                self._inbound_queue.put_nowait(pcm_bytes)
            except queue.Full:
                # Drop frame if queue is full (backpressure)
                logger.warning("Inbound queue full, dropping audio frame", extra={
                    "call_sid": self.call_sid,
                    "queue_size": self._inbound_queue.qsize(),
                    "service": "streaming_server",
                    "controller": "duplex"
                })
            
            # If in echo mode, also push to echo queue for immediate playback
            if self.echo_mode:
                try:
                    self._echo_frames.put_nowait(pcm_bytes)
                except queue.Full:
                    # Drop echo frame if queue is full
                    pass
            
            # Schedule async processing on dedicated event loop
            if self._sync_event_loop is None:
                self._sync_event_loop = asyncio.new_event_loop()
                self._sync_event_loop_thread = threading.Thread(
                    target=self._run_sync_event_loop,
                    daemon=True
                )
                self._sync_event_loop_thread.start()
            
            # Start inbound worker if not already running
            if self._inbound_worker_task is None or self._inbound_worker_task.done():
                self._inbound_worker_task = asyncio.run_coroutine_threadsafe(
                    self._inbound_worker(),
                    self._sync_event_loop
                )
            
        except Exception as e:
            logger.error("Error in handle_inbound_audio_sync", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
    
    # ADDED: Alias for handle_inbound_audio_sync to match WS handler expectations
    def handle_inbound_frame(self, pcm_bytes: bytes) -> None:
        """Alias for handle_inbound_audio_sync to match WS handler interface"""
        self.handle_inbound_audio_sync(pcm_bytes)
    
    def next_outbound_frame_sync(self, timeout_ms: int = 0) -> Optional[bytes]:
        """
        Synchronous adapter for outbound frame retrieval.
        Called from Flask /media loop to get audio for Twilio.
        Returns μ-law 8 kHz frame or None if timeout.
        """
        try:
            if self._closed:
                return None
            
            timeout_sec = timeout_ms / 1000.0
            return self._sync_outbound_frames.get(timeout=timeout_sec)
        except queue.Empty:
            return None
        except Exception as e:
            logger.error("Error in next_outbound_frame_sync", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
            return None
    
    def next_echo_frame(self, timeout_ms: int = 0) -> Optional[bytes]:
        """
        Get next echo frame for echo mode.
        Returns μ-law 8 kHz frame or None if timeout or not in echo mode.
        """
        try:
            if not self.echo_mode or self._closed:
                return None
            
            timeout_sec = timeout_ms / 1000.0
            return self._echo_frames.get(timeout=timeout_sec)
        except queue.Empty:
            return None
        except Exception as e:
            logger.error("Error in next_echo_frame", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
            return None
    
    def enqueue_greeting_sync(self, text: str):
        """
        Synchronous adapter for greeting enqueue.
        Called from Flask /media loop after "start" event.
        Non-blocking - schedules async processing.
        """
        try:
            if self._closed:
                return
            
            # Schedule async processing without blocking
            if self._sync_event_loop is None:
                self._sync_event_loop = asyncio.new_event_loop()
                self._sync_event_loop_thread = threading.Thread(
                    target=self._run_sync_event_loop,
                    daemon=True
                )
                self._sync_event_loop_thread.start()
            
            # Schedule async processing
            asyncio.run_coroutine_threadsafe(
                self.enqueue_greeting_if_needed(),
                self._sync_event_loop
            )
            
        except Exception as e:
            logger.error("Error in enqueue_greeting_sync", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
    
    def stop_sync(self):
        """
        Synchronous adapter for controller shutdown.
        Called from Flask /media loop on "stop" or cleanup.
        """
        try:
            self._closed = True
            
            # Cancel inbound worker
            if self._inbound_worker_task and not self._inbound_worker_task.done():
                self._inbound_worker_task.cancel()
            
            # Clear queues
            while not self._inbound_queue.empty():
                try:
                    self._inbound_queue.get_nowait()
                except queue.Empty:
                    break
            
            while not self._sync_outbound_frames.empty():
                try:
                    self._sync_outbound_frames.get_nowait()
                except queue.Empty:
                    break
            
            # Clear echo queue
            while not self._echo_frames.empty():
                try:
                    self._echo_frames.get_nowait()
                except queue.Empty:
                    break
            
            # Schedule async stop if event loop exists
            if self._sync_event_loop and not self._sync_event_loop.is_closed():
                future = asyncio.run_coroutine_threadsafe(
                    self.stop(),
                    self._sync_event_loop
                )
                future.result(timeout=5.0)  # Wait for stop to complete
            
            logger.info("Controller stopped via sync adapter", extra={
                "call_sid": self.call_sid,
                "service": "streaming_server",
                "controller": "duplex"
            })
            
        except Exception as e:
            logger.error("Error in stop_sync", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
            # Don't raise - cleanup should be best-effort
    
    def _run_sync_event_loop(self):
        """Run dedicated event loop for sync method execution"""
        asyncio.set_event_loop(self._sync_event_loop)
        self._sync_event_loop.run_forever()
    
    # --------------------------------------------------------------------------
    # Async Worker for Inbound Audio Processing
    # --------------------------------------------------------------------------
    
    async def _inbound_worker(self):
        """
        Async worker that processes inbound audio frames from the queue.
        This does the heavy work (ASR processing) without blocking the WS loop.
        """
        try:
            while not self._closed and self.is_active:
                try:
                    # Get next audio frame from queue with timeout
                    pcm_bytes = await asyncio.get_event_loop().run_in_executor(
                        None, 
                        lambda: self._inbound_queue.get(timeout=0.1)
                    )
                    
                    if pcm_bytes:
                        # Process the audio asynchronously
                        await self._process_inbound_audio_async(pcm_bytes)
                        
                except queue.Empty:
                    # No audio to process, continue loop
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Error in inbound worker", extra={
                        "call_sid": self.call_sid,
                        "error": str(e),
                        "service": "streaming_server",
                        "controller": "duplex"
                    })
                    await asyncio.sleep(0.1)  # Back off on error
                    
        except asyncio.CancelledError:
            logger.info("Inbound worker cancelled", extra={
                "call_sid": self.call_sid,
                "service": "streaming_server",
                "controller": "duplex"
            })
        except Exception as e:
            logger.error("Unexpected error in inbound worker", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
    
    async def _process_inbound_audio_async(self, pcm_bytes: bytes):
        """
        Process inbound audio asynchronously (heavy work).
        Called by inbound worker, not by WS loop.
        """
        try:
            # Phase 12: Circuit breaker check
            if _is_circuit_breaker_open("audio_processing"):
                logger.warning("Inbound audio processing blocked by circuit breaker", extra={
                    "call_sid": self.call_sid,
                    "trace_id": self.trace_id,
                    "service": "streaming_server",
                    "controller": "duplex"
                })
                metrics.increment_metric("inbound_audio_circuit_breaker_hits_total")
                return
            
            # Push to jitter buffer for STT processing
            self.jitter_buffer.push(pcm_bytes)
            
            # Update activity timestamp
            self.last_activity_time = time.time()
            
            # For MVP: Option A - Just store, don't process STT yet
            # Future: Option B - Process STT when enough audio buffered
            
            logger.debug("Inbound audio processed asynchronously", extra={
                "call_sid": self.call_sid,
                "audio_length": len(pcm_bytes),
                "jitter_buffer_has_data": self.jitter_buffer.has_data(),
                "service": "streaming_server",
                "controller": "duplex"
            })
            
            metrics.increment_metric("inbound_audio_chunks_processed_total")
            
        except Exception as e:
            logger.error("Error processing inbound audio async", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "streaming_server",
                "controller": "duplex"
            })
            metrics.increment_metric("inbound_audio_processing_errors_total")
            await self._handle_error()
    
    # --------------------------------------------------------------------------
    # Phase 12: Outbound Methods (TTS → Twilio)
    # --------------------------------------------------------------------------
    
    async def enqueue_greeting_if_needed(self):
        """
        Enqueue initial greeting if not already sent.
        Uses persona-appropriate greeting text (no AI mentions).
        """
        if self.greeting_sent or self._closed:
            return
            
        try:
            # Persona-appropriate greeting (no AI/system/bot mentions)
            greeting_text = "Hey, this is Sara calling from Noblecom Solutions. Just checking you can hear me clearly on your side."
            
            await self.enqueue_tts_text(greeting_text)
            self.greeting_sent = True
            
            logger.info("Greeting enqueued", extra={
                "call_sid": self.call_sid,
                "greeting_text": greeting_text,
                "service": "streaming_server",
                "controller": "duplex"
            })
            
        except Exception as e:
            logger.error("Error enqueuing greeting", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "streaming_server",
                "controller": "duplex"
            })
            await self._handle_error()
    
    async def enqueue_tts_text(self, text: str):
        """
        Convert text to TTS audio and enqueue as PCM frames for Twilio.
        Only accepts persona-appropriate text (no AI mentions).
        """
        try:
            if self._closed:
                return
            
            # Phase 12: Circuit breaker check
            if _is_circuit_breaker_open("tts_generation"):
                logger.warning("TTS generation blocked by circuit breaker", extra={
                    "call_sid": self.call_sid,
                    "trace_id": self.trace_id,
                    "service": "streaming_server",
                    "controller": "duplex"
                })
                metrics.increment_metric("tts_generation_circuit_breaker_hits_total")
                return
            
            start_time = time.time()
            
            # Generate TTS audio frames using voice engine
            async for audio_frame in self.voice_engine.text_to_speech_stream(text):
                if self._closed:
                    break
                
                # Convert to Twilio-compatible format and chunk into 20ms frames
                twilio_frames = await self._convert_to_twilio_format_chunked(audio_frame)
                
                for twilio_frame in twilio_frames:
                    if self._closed:
                        break
                    
                    # Enqueue frame for async outbound streaming
                    await self.outbound_frames.put(twilio_frame)
                    
                    # Also enqueue to sync queue for WS loop access
                    try:
                        self._sync_outbound_frames.put_nowait(twilio_frame)
                    except queue.Full:
                        # Drop frame if queue is full (backpressure)
                        logger.warning("Sync outbound queue full, dropping frame", extra={
                            "call_sid": self.call_sid,
                            "queue_size": self._sync_outbound_frames.qsize(),
                            "service": "streaming_server",
                            "controller": "duplex"
                        })
                    
                    logger.debug("TTS frame enqueued", extra={
                        "call_sid": self.call_sid,
                        "frame_length": len(twilio_frame),
                        "service": "streaming_server",
                        "controller": "duplex"
                    })
            
            generation_latency = (time.time() - start_time) * 1000
            metrics.observe_latency("tts_generation_latency_ms", generation_latency)
            
            logger.info("TTS text enqueued successfully", extra={
                "call_sid": self.call_sid,
                "text_length": len(text),
                "generation_latency_ms": generation_latency,
                "service": "streaming_server",
                "controller": "duplex"
            })
            
            metrics.increment_metric("tts_texts_enqueued_total")
            
        except Exception as e:
            logger.error("Error enqueuing TTS text", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "streaming_server",
                "controller": "duplex"
            })
            metrics.increment_metric("tts_enqueue_errors_total")
            await self._handle_error()
    
    async def next_outbound_frame(self, timeout_ms: int = 100) -> Optional[bytes]:
        """
        Get next outbound PCM frame for Twilio streaming.
        Returns None if no frames available within timeout.
        """
        try:
            if self._closed:
                return None
            
            # Try to get frame with timeout
            frame = await asyncio.wait_for(
                self.outbound_frames.get(), 
                timeout=timeout_ms / 1000.0
            )
            return frame
            
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error("Error getting next outbound frame", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
            return None
        
    # --------------------------------------------------------------------------
    # Audio Format Conversion (Twilio-compatible)
    # --------------------------------------------------------------------------
    
    async def _convert_to_twilio_format_chunked(self, audio_frame: bytes) -> List[bytes]:
        """
        Convert audio frame to Twilio-compatible format (8 kHz μ-law mono)
        and chunk into 20ms frames (160 bytes each for μ-law).
        """
        try:
            # Assume input is 16-bit PCM, 16kHz, mono
            # Convert to 8kHz using audioop.ratecv
            converted_frame = audioop.ratecv(audio_frame, 2, 1, 16000, 8000, None)
            
            # Convert to μ-law
            ulaw_frame = audioop.lin2ulaw(converted_frame[0], 2)
            
            # Chunk into 20ms frames (160 bytes for μ-law, 8000 samples/sec * 0.02 sec = 160 samples)
            frame_size = 160  # 20 ms * 8000 samples/sec * 1 byte/sample (μ-law)
            frames = []
            
            for i in range(0, len(ulaw_frame), frame_size):
                chunk = ulaw_frame[i:i + frame_size]
                if len(chunk) == frame_size:  # Only add full frames
                    frames.append(chunk)
                # Note: Last partial chunk is dropped to ensure consistent frame size
            
            return frames
            
        except Exception as e:
            logger.error("Error converting audio to Twilio format", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "input_length": len(audio_frame),
                "service": "streaming_server",
                "controller": "duplex"
            })
            # Return empty list as fallback
            return []
    
    async def _convert_to_twilio_format(self, audio_frame: bytes) -> bytes:
        """
        Convert audio frame to Twilio-compatible format (8 kHz μ-law mono)
        Legacy method for compatibility.
        """
        frames = await self._convert_to_twilio_format_chunked(audio_frame)
        if frames:
            return frames[0]  # Return first frame for backward compatibility
        return b''
    
    # --------------------------------------------------------------------------
    # Public API with Resilience Patterns
    # --------------------------------------------------------------------------
    
    async def start(self):
        """Start the duplex conversation controller with Phase 12 resilience"""
        if self.is_active or self._closed:
            logger.warning("Controller already active or closed", extra={
                "call_sid": self.call_sid,
                "service": "streaming_server",
                "controller": "duplex"
            })
            return
        
        # Phase 12: Circuit breaker check
        if _is_circuit_breaker_open("duplex_controller"):
            logger.warning("Duplex controller start blocked by circuit breaker", extra={
                "call_sid": self.call_sid,
                "trace_id": self.trace_id,
                "service": "streaming_server",
                "controller": "duplex"
            })
            metrics.increment_metric("duplex_controller_circuit_breaker_hits_total")
            raise TransientError("Duplex controller blocked by circuit breaker")
        
        # Phase 12: Idempotency check
        if _check_idempotency_key(f"start:{self.call_sid}"):
            logger.info("Duplicate controller start skipped", extra={
                "call_sid": self.call_sid,
                "trace_id": self.trace_id,
                "service": "streaming_server",
                "controller": "duplex"
            })
            return
        
        self.is_active = True
        self._stop_event.clear()
        self.consecutive_errors = 0
        self._closed = False
        
        try:
            # Start voice engine first to avoid Redis race
            await self.voice_engine.start()
            
            # Load conversation state from Redis
            await self._load_conversation_state()
            
            # Start control loop
            self._control_task = asyncio.create_task(self._control_loop())
            
            # Start periodic context logging
            self._logging_task = asyncio.create_task(self._periodic_context_logging())
            
            logger.info("DuplexVoiceController started", extra={
                "call_sid": self.call_sid,
                "trace_id": self.trace_id,
                "service": "streaming_server", 
                "controller": "duplex"
            })
            
            metrics.increment_metric("duplex_controller_started_total")
            
        except Exception as e:
            self.is_active = False
            self._closed = True
            logger.error("Failed to start duplex controller", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "trace_id": self.trace_id,
                "stack": traceback.format_exc(),
                "service": "streaming_server",
                "controller": "duplex"
            })
            metrics.increment_metric("duplex_controller_start_errors_total")
            raise TransientError(f"Duplex controller start failed: {str(e)}") from e
    
    async def stop(self):
        """Stop the duplex conversation controller and cleanup with Phase 12 resilience"""
        if not self.is_active or self._closed:
            return
        
        # Phase 12: Idempotency check
        if _check_idempotency_key(f"stop:{self.call_sid}"):
            logger.info("Duplicate controller stop skipped", extra={
                "call_sid": self.call_sid,
                "trace_id": self.trace_id,
                "service": "streaming_server",
                "controller": "duplex"
            })
            return
        
        self.is_active = False
        self._closed = True
        self._stop_event.set()
        
        try:
            # Cancel tasks with timeout protection
            tasks = [self._asr_task, self._tts_task, self._control_task, self._logging_task]
            for task in tasks:
                if task and not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=2.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass
                    except Exception as e:
                        logger.error("Error cancelling task", extra={
                            "call_sid": self.call_sid,
                            "error": str(e),
                            "service": "streaming_server",
                            "controller": "duplex"
                        })
            
            # Cancel inbound worker
            if self._inbound_worker_task and not self._inbound_worker_task.done():
                self._inbound_worker_task.cancel()
            
            # Clear queues
            while not self._inbound_queue.empty():
                try:
                    self._inbound_queue.get_nowait()
                except queue.Empty:
                    break
            
            while not self._sync_outbound_frames.empty():
                try:
                    self._sync_outbound_frames.get_nowait()
                except queue.Empty:
                    break
            
            # Clear echo queue
            while not self._echo_frames.empty():
                try:
                    self._echo_frames.get_nowait()
                except queue.Empty:
                    break
            
            # Clear async queues
            while not self.outbound_frames.empty():
                try:
                    await self.outbound_frames.get()
                except Exception:
                    break
            
            # Stop voice engine
            await self.voice_engine.stop()
            
            # Save conversation state
            await self._save_conversation_state()
            
            # Log final context snapshot
            await self._log_conversation_context("final_snapshot")
            
            # Close WebSocket gracefully
            if self.websocket and not self.websocket.closed:
                try:
                    await self.websocket.close()
                except Exception as e:
                    logger.error("Error closing WebSocket", extra={
                        "call_sid": self.call_sid,
                        "error": str(e),
                        "service": "streaming_server",
                        "controller": "duplex"
                    })
            
            logger.info("DuplexVoiceController stopped", extra={
                "call_sid": self.call_sid,
                "trace_id": self.trace_id,
                "duration_seconds": time.time() - self.start_time,
                "service": "streaming_server",
                "controller": "duplex"
            })
            
            metrics.increment_metric("duplex_controller_stopped_total")
            
        except Exception as e:
            logger.error("Error during duplex controller stop", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "trace_id": self.trace_id,
                "stack": traceback.format_exc(),
                "service": "streaming_server",
                "controller": "duplex"
            })
            metrics.increment_metric("duplex_controller_stop_errors_total")
    
    async def handle_user_audio(self, audio_data: bytes):
        """
        Handle incoming user audio data from WebSocket with Phase 12 resilience
        """
        try:
            if self._closed:
                return
            
            start_time = time.time()
            
            # Phase 12: Circuit breaker check
            if _is_circuit_breaker_open("audio_processing"):
                logger.warning("Audio processing blocked by circuit breaker", extra={
                    "call_sid": self.call_sid,
                    "trace_id": self.trace_id,
                    "service": "streaming_server",
                    "controller": "duplex"
                })
                metrics.increment_metric("audio_processing_circuit_breaker_hits_total")
                return
            
            # Update activity timestamp
            self.last_activity_time = time.time()
            
            # Use external API wrapper for voice engine processing
            def process_audio():
                return self.voice_engine.process_audio(audio_data)
            
            await external_api_call(
                "voice_engine",
                process_audio,
                trace_id=self.trace_id
            )
            
            latency_ms = (time.time() - start_time) * 1000
            metrics.observe_latency("user_audio_processing_latency_ms", latency_ms)
            
            # Reset error counter on successful processing
            self.consecutive_errors = 0
            
        except Exception as e:
            logger.error("Error handling user audio", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "streaming_server",
                "controller": "duplex"
            })
            metrics.increment_metric("user_audio_processing_errors_total")
            await self._handle_error()
    
    async def interrupt_playback(self):
        """
        Immediately stop TTS playback when user speaks with Phase 12 resilience
        Target: stop-to-silence ≤ 150 ms
        """
        try:
            if self._closed:
                return
            
            interrupt_start = time.time()
            
            if self.is_playing:
                self.is_playing = False
                
                # Stop TTS task with configurable timeout
                if self._tts_task and not self._tts_task.done():
                    self._tts_task.cancel()
                    try:
                        await asyncio.wait_for(self._tts_task, timeout=Config.TTS_CANCEL_TIMEOUT)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass
                
                # CHANGED: Do not send Twilio JSON frames from controller
                # Server will handle sending silence frames
                
                self.last_interruption_time = time.time()
                
                latency_ms = (time.time() - interrupt_start) * 1000
                metrics.observe_latency("interruption_latency_ms", latency_ms)
                
                logger.info("Playback interrupted", extra={
                    "call_sid": self.call_sid,
                    "interruption_latency_ms": latency_ms,
                    "service": "streaming_server",
                    "controller": "duplex"
                })
                
                metrics.increment_metric("playback_interruptions_total")
            
        except Exception as e:
            logger.error("Error interrupting playback", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "streaming_server",
                "controller": "duplex"
            })
            metrics.increment_metric("interruption_errors_total")
            await self._handle_error()
    
    async def heartbeat(self) -> Dict[str, Any]:
        """
        Health check endpoint for supervisor probing with Phase 12 resilience
        """
        try:
            if self._closed:
                return {
                    "status": "closed",
                    "call_sid": self.call_sid,
                    "trace_id": self.trace_id
                }
            
            # Check circuit breaker state
            circuit_breaker_open = _is_circuit_breaker_open("duplex_controller")
            
            # Check Redis connectivity
            redis_ok = False
            if self.redis_client:
                try:
                    redis_ok = self.redis_client.ping()
                except Exception:
                    redis_ok = False
            
            status = "active" if self.is_active else "inactive"
            if circuit_breaker_open:
                status = "degraded"
            
            return {
                "status": status,
                "call_sid": self.call_sid,
                "uptime_seconds": time.time() - self.start_time,
                "last_activity_seconds": time.time() - self.last_activity_time,
                "conversation_turns": len(self.conversation_history),
                "consecutive_errors": self.consecutive_errors,
                "is_playing": self.is_playing,
                "is_listening": self.is_listening,
                "trace_id": self.trace_id,
                "circuit_breaker_open": circuit_breaker_open,
                "redis_ok": redis_ok,
                "greeting_sent": self.greeting_sent,
                "outbound_queue_size": self.outbound_frames.qsize(),
                "sync_outbound_queue_size": self._sync_outbound_frames.qsize(),
                "inbound_queue_size": self._inbound_queue.qsize(),
                "jitter_buffer_has_data": self.jitter_buffer.has_data(),
                "closed": self._closed,
                "echo_mode": self.echo_mode,
                "echo_queue_size": self._echo_frames.qsize()
            }
            
        except Exception as e:
            logger.error("Error in heartbeat check", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
            return {
                "status": "error",
                "call_sid": self.call_sid,
                "error": str(e),
                "trace_id": self.trace_id
            }
    
    # --------------------------------------------------------------------------
    # Core Control Loop with Resilience
    # --------------------------------------------------------------------------
    
    async def _control_loop(self):
        """
        Main control loop managing ASR and TTS coordination with Phase 12 resilience
        """
        logger.info("Control loop started", extra={
            "call_sid": self.call_sid,
            "service": "streaming_server",
            "controller": "duplex"
        })
        
        try:
            while self.is_active and not self._stop_event.is_set() and not self._closed:
                try:
                    # Phase 12: Circuit breaker check
                    if _is_circuit_breaker_open("duplex_controller"):
                        logger.error("Circuit breaker triggered - stopping controller", extra={
                            "call_sid": self.call_sid,
                            "service": "streaming_server",
                            "controller": "duplex"
                        })
                        await self.stop()
                        break
                    
                    # Circuit breaker based on consecutive errors
                    if self.consecutive_errors >= Config.MAX_CONSECUTIVE_ERRORS:
                        logger.error("Consecutive error limit reached - stopping controller", extra={
                            "call_sid": self.call_sid,
                            "consecutive_errors": self.consecutive_errors,
                            "service": "streaming_server",
                            "controller": "duplex"
                        })
                        await self.stop()
                        break
                    
                    # Check for user speech via voice activity detection
                    if await self._detect_user_voice():
                        await self._handle_user_speech()
                    
                    # Check if we should generate a response
                    if not self.is_playing and await self._should_respond():
                        await self._generate_and_play_response()
                    
                    await asyncio.sleep(0.01)  # 10ms control loop
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Error in control loop", extra={
                        "call_sid": self.call_sid,
                        "error": str(e),
                        "stack": traceback.format_exc(),
                        "service": "streaming_server",
                        "controller": "duplex"
                    })
                    await self._handle_error()
                    await asyncio.sleep(0.1)  # Back off on error
        
        except asyncio.CancelledError:
            logger.info("Control loop cancelled", extra={
                "call_sid": self.call_sid,
                "service": "streaming_server", 
                "controller": "duplex"
            })
        finally:
            logger.info("Control loop ended", extra={
                "call_sid": self.call_sid,
                "service": "streaming_server",
                "controller": "duplex"
            })
    
    async def _detect_user_voice(self) -> bool:
        """
        Detect user voice activity using VAD or ASR partial transcripts with resilience
        """
        try:
            if self._closed:
                return False
            
            # Use external API wrapper for voice activity detection
            def detect_voice():
                return self.voice_engine.detect_voice_activity()
            
            vad_detected = await external_api_call(
                "voice_activity_detection",
                detect_voice,
                trace_id=self.trace_id
            )
            
            # Also check for ASR partial transcripts indicating speech
            asr_detected = await self.voice_engine.has_partial_transcript()
            
            voice_detected = vad_detected or asr_detected
            
            if voice_detected:
                current_time = time.time()
                # Debounce voice detection to prevent double activations
                if (current_time - self.last_voice_detection_time) * 1000 < Config.INTERRUPTION_DEBOUNCE_MS:
                    return False
                
                self.last_voice_detection_time = current_time
                
                if self.is_playing:
                    # Immediate interruption required
                    await self.interrupt_playback()
                    return True
                    
                return True
            
            return False
            
        except Exception as e:
            logger.error("Error detecting user voice", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
            await self._handle_error()
            return False
    
    async def _handle_user_speech(self):
        """
        Handle detected user speech - get final transcript and prepare response with resilience
        """
        try:
            start_time = time.time()
            
            # Get final transcript from voice engine with external API wrapper
            def get_transcript():
                return self.voice_engine.get_final_transcript()
            
            transcript = await external_api_call(
                "speech_recognition",
                get_transcript,
                trace_id=self.trace_id
            )
            
            if transcript and transcript.strip():
                # Update activity timestamp
                self.last_activity_time = time.time()
                
                # Add to conversation history
                self.conversation_history.append({
                    "role": "user",
                    "content": transcript,
                    "timestamp": time.time()
                })
                
                # Trim conversation history if needed
                self._trim_conversation_history()
                
                # Save state after user speech
                await self._save_conversation_state()
                
                logger.info("User speech processed", extra={
                    "call_sid": self.call_sid,
                    "transcript": transcript,
                    "processing_latency_ms": (time.time() - start_time) * 1000,
                    "service": "streaming_server",
                    "controller": "duplex"
                })
                
                metrics.increment_metric("user_speech_processed_total")
                metrics.observe_latency("speech_processing_latency_ms", (time.time() - start_time) * 1000)
                
                # Reset error counter on successful processing
                self.consecutive_errors = 0
            
        except Exception as e:
            logger.error("Error handling user speech", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "streaming_server",
                "controller": "duplex"
            })
            metrics.increment_metric("speech_handling_errors_total")
            await self._handle_error()
    
    async def _should_respond(self) -> bool:
        """
        Determine if we should generate a response with resilience checks
        """
        if not self.conversation_history or self._closed:
            return False
        
        last_entry = self.conversation_history[-1]
        
        # Respond if last entry was from user and we're not already playing
        return (last_entry["role"] == "user" and 
                not self.is_playing and
                time.time() - last_entry["timestamp"] < 30)  # 30s timeout
    
    async def _generate_and_play_response(self):
        """
        Generate LLM response and stream via TTS with Phase 12 resilience
        """
        try:
            start_time = time.time()
            
            # Get conversation context
            context = self._get_conversation_context()
            
            # Generate response with external API wrapper
            def generate_reply_internal():
                return generate_reply(context, trace_id=self.trace_id)
            
            llm_response = await external_api_call(
                "llm_generation",
                generate_reply_internal,
                trace_id=self.trace_id
            )
            
            if llm_response:
                # Update activity timestamp
                self.last_activity_time = time.time()
                
                # Add to conversation history
                self.conversation_history.append({
                    "role": "assistant", 
                    "content": llm_response,
                    "timestamp": time.time()
                })
                
                # Trim conversation history if needed
                self._trim_conversation_history()
                
                # Save state after assistant response
                await self._save_conversation_state()
                
                # Start TTS streaming
                self.is_playing = True
                self._tts_task = asyncio.create_task(
                    self._stream_tts_response(llm_response)
                )
                
                generation_latency = (time.time() - start_time) * 1000
                metrics.observe_latency("response_generation_latency_ms", generation_latency)
                
                logger.info("Response generated and TTS started", extra={
                    "call_sid": self.call_sid,
                    "response_length": len(llm_response),
                    "generation_latency_ms": generation_latency,
                    "service": "streaming_server",
                    "controller": "duplex"
                })
                
                metrics.increment_metric("responses_generated_total")
                
                # Reset error counter on successful generation
                self.consecutive_errors = 0
            
        except Exception as e:
            logger.error("Error generating response", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "streaming_server",
                "controller": "duplex"
            })
            metrics.increment_metric("response_generation_errors_total")
            self.is_playing = False
            await self._handle_error()
    
    async def _stream_tts_response(self, text: str):
        """
        Stream TTS response to WebSocket with interrupt capability and resilience
        Now also enqueues TTS chunks to Twilio outbound queue
        """
        try:
            start_time = time.time()
            frame_sequence = 0
            
            # Convert text to audio frames (streaming) with external API wrapper
            async for audio_frame in self.voice_engine.text_to_speech_stream(text):
                if not self.is_playing or not self.is_active or self._closed:
                    break
                
                # Convert to Twilio-compatible format and chunk into 20ms frames
                twilio_frames = await self._convert_to_twilio_format_chunked(audio_frame)
                
                for twilio_frame in twilio_frames:
                    if not self.is_playing or self._closed:
                        break
                    
                    # Enqueue to sync outbound queue for WS loop access
                    try:
                        self._sync_outbound_frames.put_nowait(twilio_frame)
                        frame_sequence += 1
                        
                        logger.debug("TTS frame enqueued for Twilio", extra={
                            "call_sid": self.call_sid,
                            "frame_sequence": frame_sequence,
                            "frame_length": len(twilio_frame),
                            "service": "streaming_server",
                            "controller": "duplex"
                        })
                    except queue.Full:
                        logger.warning("Sync outbound queue full, dropping TTS frame", extra={
                            "call_sid": self.call_sid,
                            "queue_size": self._sync_outbound_frames.qsize(),
                            "service": "streaming_server",
                            "controller": "duplex"
                        })
                
                # Small delay to maintain real-time streaming
                await asyncio.sleep(0.02)  # 50ms chunks
            
            streaming_latency = (time.time() - start_time) * 1000
            metrics.observe_latency("tts_streaming_latency_ms", streaming_latency)
            
            logger.info("TTS streaming completed", extra={
                "call_sid": self.call_sid,
                "streaming_latency_ms": streaming_latency,
                "total_frames": frame_sequence,
                "interrupted": not self.is_playing,
                "service": "streaming_server",
                "controller": "duplex"
            })
            
        except asyncio.CancelledError:
            logger.info("TTS streaming cancelled", extra={
                "call_sid": self.call_sid,
                "service": "streaming_server",
                "controller": "duplex"
            })
        except Exception as e:
            logger.error("Error in TTS streaming", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "stack": traceback.format_exc(),
                "service": "streaming_server",
                "controller": "duplex"
            })
            metrics.increment_metric("tts_streaming_errors_total")
            await self._handle_error()
        finally:
            self.is_playing = False
    
    # --------------------------------------------------------------------------
    # Error Handling & Circuit Breaker
    # --------------------------------------------------------------------------
    
    async def _handle_error(self):
        """Handle errors and implement circuit breaker logic with Phase 12 resilience"""
        self.consecutive_errors += 1
        self.last_error_time = time.time()
        
        logger.warning("Error recorded in controller", extra={
            "call_sid": self.call_sid,
            "consecutive_errors": self.consecutive_errors,
            "max_consecutive_errors": Config.MAX_CONSECUTIVE_ERRORS,
            "service": "streaming_server",
            "controller": "duplex"
        })
        
        metrics.increment_metric("duplex_controller_errors_total")
        
        if self.consecutive_errors >= Config.MAX_CONSECUTIVE_ERRORS:
            logger.error("Max consecutive errors reached - circuit breaker engaged", extra={
                "call_sid": self.call_sid,
                "consecutive_errors": self.consecutive_errors,
                "service": "streaming_server",
                "controller": "duplex"
            })
            metrics.increment_metric("circuit_breaker_triggered_total")
    
    # --------------------------------------------------------------------------
    # Periodic Context Logging with Resilience
    # --------------------------------------------------------------------------
    
    async def _periodic_context_logging(self):
        """
        Log conversation context snapshot every 30 seconds with resilience
        """
        try:
            while self.is_active and not self._stop_event.is_set() and not self._closed:
                await asyncio.sleep(30)  # 30-second intervals
                
                if self.is_active and not self._closed:
                    await self._log_conversation_context("periodic_snapshot")
                    
        except asyncio.CancelledError:
            logger.info("Periodic context logging cancelled", extra={
                "call_sid": self.call_sid,
                "service": "streaming_server",
                "controller": "duplex"
            })
        except Exception as e:
            logger.error("Error in periodic context logging", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
    
    async def _log_conversation_context(self, snapshot_type: str):
        """
        Log current conversation context snapshot with resilience
        """
        try:
            if not self.conversation_history or self._closed:
                return
            
            current_time = time.time()
            context_snapshot = {
                "snapshot_type": snapshot_type,
                "call_sid": self.call_sid,
                "trace_id": self.trace_id,
                "timestamp": current_time,
                "conversation_turns": len(self.conversation_history),
                "recent_exchanges": self.conversation_history[-4:],  # Last 2 exchanges
                "controller_status": {
                    "is_active": self.is_active,
                    "is_playing": self.is_playing,
                    "is_listening": self.is_listening,
                    "uptime_seconds": current_time - self.start_time,
                    "consecutive_errors": self.consecutive_errors,
                    "closed": self._closed
                }
            }
            
            logger.info("Conversation context snapshot", extra={
                "call_sid": self.call_sid,
                "snapshot_type": snapshot_type,
                "conversation_turns": len(self.conversation_history),
                "recent_exchanges": context_snapshot["recent_exchanges"],
                "controller_status": context_snapshot["controller_status"],
                "service": "streaming_server",
                "controller": "duplex"
            })
            
            # Update last log time
            self.last_context_log_time = current_time
            
            metrics.increment_metric("context_snapshots_logged_total")
            
        except Exception as e:
            logger.error("Error logging conversation context", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
    
    # --------------------------------------------------------------------------
    # WebSocket Communication with Resilience - CHANGED: Removed Twilio JSON sending
    # --------------------------------------------------------------------------
    
    async def _send_audio_frame(self, audio_data: bytes):
        """Send audio frame to Twilio WebSocket with resilience"""
        # CHANGED: Do not send Twilio JSON frames from controller
        # Server will handle sending appropriate frames
        pass
    
    async def _send_silence_frame(self):
        """Send silence frame to immediately stop audio with resilience"""
        # CHANGED: Do not send Twilio JSON frames from controller
        # Server will handle sending silence frames
        pass
    
    # --------------------------------------------------------------------------
    # State Persistence with Resilience
    # --------------------------------------------------------------------------
    
    async def _save_conversation_state(self):
        """Persist conversation state to Redis with Phase 12 resilience"""
        try:
            if self.redis_client and not self._closed:
                state_key = f"session:{self.call_sid}"
                state_data = {
                    "conversation_history": self.conversation_history,
                    "last_updated": time.time(),
                    "trace_id": self.trace_id,
                    "call_sid": self.call_sid,
                    "last_activity_time": self.last_activity_time
                }
                
                # Use async Redis client or wrap sync call
                if hasattr(self.redis_client, 'setex') and callable(getattr(self.redis_client, 'setex')):
                    # Async Redis client
                    await self.redis_client.setex(
                        state_key, 
                        Config.CALL_STATE_TTL,
                        json.dumps(state_data)
                    )
                else:
                    # Sync Redis client - wrap in thread
                    await asyncio.to_thread(
                        self.redis_client.setex,
                        state_key,
                        Config.CALL_STATE_TTL,
                        json.dumps(state_data)
                    )
                
                logger.debug("Conversation state saved", extra={
                    "call_sid": self.call_sid,
                    "redis_key": state_key,
                    "history_entries": len(self.conversation_history),
                    "service": "streaming_server",
                    "controller": "duplex"
                })
                
                metrics.increment_metric("conversation_state_saved_total")
        except Exception as e:
            logger.error("Error saving conversation state", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
    
    async def _load_conversation_state(self):
        """Load conversation state from Redis with Phase 12 resilience"""
        try:
            if self.redis_client and not self._closed:
                state_key = f"session:{self.call_sid}"
                
                # Use async Redis client or wrap sync call
                if hasattr(self.redis_client, 'get') and callable(getattr(self.redis_client, 'get')):
                    # Async Redis client
                    state_data = await self.redis_client.get(state_key)
                else:
                    # Sync Redis client - wrap in thread
                    state_data = await asyncio.to_thread(self.redis_client.get, state_key)
                
                if state_data:
                    state = json.loads(state_data)
                    self.conversation_history = state.get("conversation_history", [])
                    self.last_activity_time = state.get("last_activity_time", time.time())
                    
                    # Trim loaded history
                    self._trim_conversation_history()
                    
                    logger.info("Conversation state loaded", extra={
                        "call_sid": self.call_sid,
                        "redis_key": state_key,
                        "history_entries": len(self.conversation_history),
                        "service": "streaming_server",
                        "controller": "duplex"
                    })
                    
                    metrics.increment_metric("conversation_state_restored_total")
        except Exception as e:
            logger.error("Error loading conversation state", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
    
    def _trim_conversation_history(self):
        """Trim conversation history to limit payload size"""
        if len(self.conversation_history) > Config.MAX_CONVERSATION_TURNS:
            # Keep the most recent turns
            self.conversation_history = self.conversation_history[-Config.MAX_CONVERSATION_TURNS:]
            logger.debug("Conversation history trimmed", extra={
                "call_sid": self.call_sid,
                "max_turns": Config.MAX_CONVERSATION_TURNS,
                "service": "streaming_server",
                "controller": "duplex"
            })
    
    # --------------------------------------------------------------------------
    # Utility Methods
    # --------------------------------------------------------------------------
    
    def _get_conversation_context(self) -> str:
        """Format conversation history for LLM context"""
        if not self.conversation_history or self._closed:
            return "Hello! How can I help you today?"
        
        # Get last few exchanges for context
        recent_history = self.conversation_history[-6:]  # Last 3 exchanges
        
        context_lines = []
        for entry in recent_history:
            role = "User" if entry["role"] == "user" else "Assistant"
            context_lines.append(f"{role}: {entry['content']}")
        
        return "\n".join(context_lines)
    
    def get_status(self) -> Dict[str, Any]:
        """Get current controller status with resilience information"""
        return {
            "call_sid": self.call_sid,
            "is_active": self.is_active,
            "is_playing": self.is_playing,
            "is_listening": self.is_listening,
            "conversation_turns": len(self.conversation_history),
            "uptime_seconds": time.time() - self.start_time,
            "last_interruption_ms": (time.time() - self.last_interruption_time) * 1000 if self.last_interruption_time else 0,
            "trace_id": self.trace_id,
            "last_context_log_time": self.last_context_log_time,
            "consecutive_errors": self.consecutive_errors,
            "last_activity_seconds": time.time() - self.last_activity_time,
            "circuit_breaker_open": _is_circuit_breaker_open("duplex_controller"),
            "twilio_session_attached": self.twilio_media_session is not None,
            "greeting_sent": self.greeting_sent,
            "outbound_queue_size": self.outbound_frames.qsize(),
            "sync_outbound_queue_size": self._sync_outbound_frames.qsize(),
            "inbound_queue_size": self._inbound_queue.qsize(),
            "jitter_buffer_has_data": self.jitter_buffer.has_data(),
            "closed": self._closed,
            "echo_mode": self.echo_mode,
            "echo_queue_size": self._echo_frames.qsize()
        }