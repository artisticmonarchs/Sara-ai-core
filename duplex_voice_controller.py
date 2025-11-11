"""
duplex_voice_controller.py — Phase 12 (Celery Task Resilience)
Real-time full-duplex conversation controller with full fault tolerance, idempotency, and unified resilience patterns.
"""

import asyncio
import json
import time
import uuid
import os
from typing import Optional, Dict, Any, Callable
import traceback

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
        
        logger.info("DuplexVoiceController initialized", extra={
            "call_sid": call_sid,
            "trace_id": self.trace_id,
            "service": "streaming_server",
            "controller": "duplex"
        })
    
    # --------------------------------------------------------------------------
    # Public API with Resilience Patterns
    # --------------------------------------------------------------------------
    
    async def start(self):
        """Start the duplex conversation controller with Phase 12 resilience"""
        if self.is_active:
            logger.warning("Controller already active", extra={
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
        if not self.is_active:
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
                
                # Send silence or stop marker to WebSocket (AI→Twilio)
                await self._send_silence_frame()
                
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
                "redis_ok": redis_ok
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
            while self.is_active and not self._stop_event.is_set():
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
        if not self.conversation_history:
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
        """
        try:
            start_time = time.time()
            
            # Convert text to audio frames (streaming) with external API wrapper
            async for audio_frame in self.voice_engine.text_to_speech_stream(text):
                if not self.is_playing or not self.is_active:
                    break
                
                # Send audio frame to Twilio WebSocket
                await self._send_audio_frame(audio_frame)
                
                # Small delay to maintain real-time streaming
                await asyncio.sleep(0.02)  # 50ms chunks
            
            streaming_latency = (time.time() - start_time) * 1000
            metrics.observe_latency("tts_streaming_latency_ms", streaming_latency)
            
            logger.info("TTS streaming completed", extra={
                "call_sid": self.call_sid,
                "streaming_latency_ms": streaming_latency,
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
            while self.is_active and not self._stop_event.is_set():
                await asyncio.sleep(30)  # 30-second intervals
                
                if self.is_active:
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
            if not self.conversation_history:
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
                    "consecutive_errors": self.consecutive_errors
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
    # WebSocket Communication with Resilience
    # --------------------------------------------------------------------------
    
    async def _send_audio_frame(self, audio_data: bytes):
        """Send audio frame to Twilio WebSocket with resilience"""
        try:
            if self.websocket and not self.websocket.closed:
                # Twilio MediaStream format
                message = {
                    "event": "media",
                    "streamSid": self.call_sid,
                    "media": {
                        "payload": audio_data.hex()  # Convert to hex string
                    }
                }
                await self.websocket.send(json.dumps(message))
        except Exception as e:
            logger.error("Error sending audio frame", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
    
    async def _send_silence_frame(self):
        """Send silence frame to immediately stop audio with resilience"""
        try:
            if self.websocket and not self.websocket.closed:
                # Send empty media packet or silence
                silence_message = {
                    "event": "media", 
                    "streamSid": self.call_sid,
                    "media": {
                        "payload": ""  # Empty payload for silence
                    }
                }
                await self.websocket.send(json.dumps(silence_message))
        except Exception as e:
            logger.error("Error sending silence frame", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "service": "streaming_server",
                "controller": "duplex"
            })
    
    # --------------------------------------------------------------------------
    # State Persistence with Resilience
    # --------------------------------------------------------------------------
    
    async def _save_conversation_state(self):
        """Persist conversation state to Redis with Phase 12 resilience"""
        try:
            if self.redis_client:
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
            if self.redis_client:
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
        if not self.conversation_history:
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
            "circuit_breaker_open": _is_circuit_breaker_open("duplex_controller")
        }


# --------------------------------------------------------------------------
# Controller Manager (Singleton) with Resilience
# --------------------------------------------------------------------------

class DuplexControllerManager:
    """
    Phase 12: Manages multiple DuplexVoiceController instances with resilience patterns
    """
    
    def __init__(self):
        self.controllers: Dict[str, DuplexVoiceController] = {}
        self._lock = asyncio.Lock()
    
    async def get_controller(self, call_sid: str, websocket, config: Optional[Dict] = None) -> DuplexVoiceController:
        """Get or create controller for call_sid with Phase 12 resilience"""
        async with self._lock:
            if call_sid not in self.controllers:
                self.controllers[call_sid] = DuplexVoiceController(call_sid, websocket, config)
                logger.info("New controller created", extra={
                    "call_sid": call_sid,
                    "service": "streaming_server",
                    "controller": "duplex"
                })
            
            return self.controllers[call_sid]
    
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


# Phase 12: Global manager instance
controller_manager = DuplexControllerManager()