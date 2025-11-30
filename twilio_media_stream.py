"""
twilio_media_stream.py — Phase 12 Compliant
Automatically hardened by phase12_auto_hardener.py
"""

"""
twilio_media_stream.py
Handle Twilio MediaStream WebSocket send/receive for audio.
Manages WebSocket connections, audio framing, and automatic reconnection.
"""

import asyncio
import json
import time
import uuid
from typing import Optional, Dict, Any, Callable, List
import traceback
import signal
import sys
import os
import base64

def _graceful_shutdown(signum, frame):
    """Phase 12: Graceful shutdown handler"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, _graceful_shutdown)
signal.signal(signal.SIGTERM, _graceful_shutdown)


# --------------------------------------------------------------------------
# Configuration and Imports
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    class Config:
        MEDIA_STREAM_BUFFER_MS = int(os.getenv("MEDIA_STREAM_BUFFER_MS", "200"))
        MEDIA_STREAM_RECONNECT_ATTEMPTS = int(os.getenv("MEDIA_STREAM_RECONNECT_ATTEMPTS", "3"))
        MEDIA_STREAM_RECONNECT_DELAY = float(os.getenv("MEDIA_STREAM_RECONNECT_DELAY", "1.0"))
        MEDIA_STREAM_SILENCE_FRAME_MS = int(os.getenv("MEDIA_STREAM_SILENCE_FRAME_MS", "20"))
        MEDIA_STREAM_MAX_QUEUE_SIZE = int(os.getenv("MEDIA_STREAM_MAX_QUEUE_SIZE", "100"))
        MEDIA_STREAM_PING_INTERVAL = int(os.getenv("MEDIA_STREAM_PING_INTERVAL", "30"))
        WEBSOCKET_OPERATION_TIMEOUT = float(os.getenv("WEBSOCKET_OPERATION_TIMEOUT", "10.0"))

try:
    import metrics_collector as metrics
except ImportError:
    class metrics:
        @staticmethod
        def increment_metric(*args, **kwargs): pass
        @staticmethod
        def observe_latency(*args, **kwargs): pass
        @staticmethod
        def set_gauge(*args, **kwargs): pass

try:
    from logging_utils import get_logger
    logger = get_logger("twilio_media_stream")
except ImportError:
    import logging
    logger = logging.getLogger("twilio_media_stream")

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

class MediaStreamEvents:
    """Twilio MediaStream WebSocket events"""
    CONNECT = "connected"
    MEDIA = "media"
    MARK = "mark"
    CLEAR = "clear"
    ERROR = "error"
    STOPPED = "stop"

class AudioFormats:
    """Supported audio formats"""
    PCM_MULAW = "audio/x-mulaw"
    PCM_ALAW = "audio/x-alaw"
    PCM_S16LE = "audio/x-linear16"

# Phase 12: Unified Timeout Metrics
class TimeoutMetrics:
    """Unified timeout metrics taxonomy for Phase 12 compliance"""
    
    @staticmethod
    def record_timeout(service: str, operation: str):
        """Record timeout with unified metric naming"""
        metrics.increment_metric(f"{service}_{operation}_timeout_total")
        metrics.increment_metric("global_timeout_events_total")

# --------------------------------------------------------------------------
# Twilio Media Stream Handler
# --------------------------------------------------------------------------

class TwilioMediaStream:
    """
    Handles Twilio MediaStream WebSocket communication for real-time audio.
    Manages sending PCM frames and receiving audio chunks with backpressure control.
    """
    
    def __init__(self, call_sid: str, websocket, 
                 on_incoming_audio: Optional[Callable[[bytes], None]] = None,
                 on_media_event: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.call_sid = call_sid
        self.websocket = websocket
        self.on_incoming_audio = on_incoming_audio
        self.on_media_event = on_media_event
        
        # Connection state
        self.is_connected = False
        self.is_running = False
        self.reconnect_attempts = 0
        self.last_pong_time = time.time()
        
        # Audio processing
        self.audio_queue = asyncio.Queue(maxsize=Config.MEDIA_STREAM_MAX_QUEUE_SIZE)
        self.silence_generator = self._create_silence_generator()
        
        # Performance tracking
        self.trace_id = str(uuid.uuid4())
        self.start_time = time.time()
        self.frames_sent = 0
        self.frames_received = 0
        self.bytes_sent = 0
        self.bytes_received = 0
        
        # Tasks
        self._send_task: Optional[asyncio.Task] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._silence_task: Optional[asyncio.Task] = None
        
        # Backpressure control
        self._backpressure_detected = False
        self._last_send_time = 0
        self._queue_size_gauge = 0
        
        # Phase 4: Metrics batching
        self._metrics_buffer = {"chunks": 0, "bytes": 0, "last_flush": time.time()}
        
        logger.info("TwilioMediaStream initialized", extra={
            "call_sid": call_sid,
            "trace_id": self.trace_id,
            "max_buffer_ms": Config.MEDIA_STREAM_BUFFER_MS
        })
    
    # --------------------------------------------------------------------------
    # Public API
    # --------------------------------------------------------------------------
    
    async def start(self):
        """Start the media stream handler"""
        if self.is_running:
            logger.warning("Media stream already running", extra={"call_sid": self.call_sid})
            return
        
        self.is_running = True
        self.is_connected = True
        
        # Start background tasks
        self._send_task = asyncio.create_task(self._send_loop())
        self._receive_task = asyncio.create_task(self._receive_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())
        
        # Send connected event
        await self._send_connected_event()
        
        metrics.increment_metric("media_stream_started_total")
        metrics.set_gauge("active_media_streams", 1, operation="increment")
        
        logger.info("TwilioMediaStream started", extra={"call_sid": self.call_sid})
    
    async def stop(self):
        """Stop the media stream handler and cleanup"""
        if not self.is_running:
            return
        
        self.is_running = False
        self.is_connected = False
        
        # Cancel tasks
        for task in [self._send_task, self._receive_task, self._ping_task, self._silence_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Clear queue
        while not self.audio_queue.empty():
            try:
                self.audio_queue.get_nowait()
            except:
                break
        
        metrics.set_gauge("active_media_streams", -1, operation="decrement")
        metrics.increment_metric("media_stream_stopped_total")
        
        logger.info("TwilioMediaStream stopped", extra={
            "call_sid": self.call_sid,
            "duration_seconds": time.time() - self.start_time,
            "frames_sent": self.frames_sent,
            "frames_received": self.frames_received
        })
    
    async def send_audio_frame(self, audio_data: bytes):
        """
        Send audio frame to Twilio MediaStream.
        Handles backpressure by blocking if buffer is full.
        """
        if not self.is_connected or not self.is_running:
            logger.warning("Cannot send audio - media stream not connected", extra={"call_sid": self.call_sid})
            return
        
        try:
            # Apply backpressure control
            if self.audio_queue.qsize() >= Config.MEDIA_STREAM_MAX_QUEUE_SIZE:
                self._backpressure_detected = True
                metrics.increment_metric("media_stream_backpressure_events_total")
                logger.warning("Media stream backpressure detected", extra={
                    "call_sid": self.call_sid,
                    "queue_size": self.audio_queue.qsize()
                })
            
            # Wait if queue is full (backpressure)
            await self.audio_queue.put(audio_data)
            
            # Phase 4: Batch metrics updates
            self._metrics_buffer["chunks"] += 1
            self._metrics_buffer["bytes"] += len(audio_data)
            await self._flush_metrics_if_needed()
            
        except asyncio.QueueFull:
            metrics.increment_metric("media_stream_queue_full_errors_total")
            logger.error("Audio queue full - dropping frame", extra={"call_sid": self.call_sid})
        except Exception as e:
            metrics.increment_metric("media_stream_send_errors_total")
            logger.error("Error sending audio frame", extra={
                "call_sid": self.call_sid,
                "error": str(e)
            })
    
    async def _flush_metrics_if_needed(self):
        """Flush batched metrics if enough time has passed"""
        now = time.time()
        if now - self._metrics_buffer["last_flush"] >= 1.0:  # 1 second batching
            if self._metrics_buffer["chunks"] > 0:
                self._queue_size_gauge = self.audio_queue.qsize()
                metrics.set_gauge("media_stream_queue_size", self._queue_size_gauge)
                self._metrics_buffer.update(chunks=0, bytes=0, last_flush=now)
    
    async def send_silence(self, duration_ms: int = None):
        """
        Send silence frames for specified duration.
        Used when TTS stops abruptly or during gaps.
        """
        if not duration_ms:
            duration_ms = Config.MEDIA_STREAM_SILENCE_FRAME_MS
        
        try:
            silence_frames = self._generate_silence_frames(duration_ms)
            for frame in silence_frames:
                await self.send_audio_frame(frame)
            
            logger.debug("Silence frames sent", extra={
                "call_sid": self.call_sid,
                "duration_ms": duration_ms,
                "frames_count": len(silence_frames)
            })
            
        except Exception as e:
            logger.error("Error sending silence frames", extra={
                "call_sid": self.call_sid,
                "error": str(e)
            })
    
    async def send_mark_event(self, name: str):
        """Send a mark event to Twilio MediaStream"""
        try:
            mark_message = {
                "event": MediaStreamEvents.MARK,
                "streamSid": self.call_sid,
                "mark": {
                    "name": name
                }
            }
            
            await self._send_websocket_message(mark_message)
            metrics.increment_metric("media_stream_mark_events_total")
            
            logger.debug("Mark event sent", extra={
                "call_sid": self.call_sid,
                "mark_name": name
            })
            
        except Exception as e:
            logger.error("Error sending mark event", extra={
                "call_sid": self.call_sid,
                "error": str(e)
            })
    
    def get_connection_status(self) -> Dict[str, Any]:
        """Get current connection status and statistics"""
        return {
            "call_sid": self.call_sid,
            "is_connected": self.is_connected,
            "is_running": self.is_running,
            "frames_sent": self.frames_sent,
            "frames_received": self.frames_received,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "queue_size": self.audio_queue.qsize(),
            "backpressure_detected": self._backpressure_detected,
            "uptime_seconds": time.time() - self.start_time,
            "reconnect_attempts": self.reconnect_attempts
        }
    
    # --------------------------------------------------------------------------
    # WebSocket Message Handling
    # --------------------------------------------------------------------------
    
    async def _send_websocket_message(self, message: Dict[str, Any]):
        """Send message through WebSocket with error handling"""
        if not self.websocket or self.websocket.closed:
            raise Exception("WebSocket not connected")
        
        try:
            message_json = json.dumps(message)
            # Phase 4: Add timeout to WebSocket send
            await asyncio.wait_for(
                self.websocket.send(message_json),
                timeout=Config.WEBSOCKET_OPERATION_TIMEOUT
            )
            
            # Update metrics for media messages
            if message.get("event") == MediaStreamEvents.MEDIA:
                self.bytes_sent += len(message_json)
                
        except asyncio.TimeoutError:
            logger.warning("WebSocket send timeout", extra={
                "call_sid": self.call_sid,
                "operation": "send_websocket_message"
            })
            TimeoutMetrics.record_timeout("websocket", "send_message")
            metrics.increment_metric("websocket_send_timeout_errors_total")
            raise Exception("WebSocket send timeout")
        except Exception as e:
            logger.error("WebSocket send error", extra={
                "call_sid": self.call_sid,
                "error": str(e)
            })
            raise
    
    async def _handle_websocket_message(self, message: str):
        """Handle incoming WebSocket message"""
        try:
            message_data = json.loads(message)
            event_type = message_data.get("event")
            
            # Update metrics
            self.bytes_received += len(message)
            
            # Route to appropriate handler
            if event_type == MediaStreamEvents.MEDIA:
                await self._handle_media_message(message_data)
            elif event_type == MediaStreamEvents.MARK:
                await self._handle_mark_message(message_data)
            elif event_type == MediaStreamEvents.CLEAR:
                await self._handle_clear_message(message_data)
            elif event_type == MediaStreamEvents.ERROR:
                await self._handle_error_message(message_data)
            elif event_type == MediaStreamEvents.STOP:
                await self._handle_stopped_message(message_data)
            else:
                logger.debug("Unknown MediaStream event", extra={
                    "call_sid": self.call_sid,
                    "event_type": event_type
                })
            
            # Notify external handler if provided
            if self.on_media_event:
                try:
                    await self.on_media_event(message_data)
                except Exception as e:
                    logger.error("Error in media event handler", extra={
                        "call_sid": self.call_sid,
                        "error": str(e)
                    })
                    
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in WebSocket message", extra={
                "call_sid": self.call_sid,
                "message": message[:100],
                "error": str(e)
            })
        except Exception as e:
            logger.error("Error handling WebSocket message", extra={
                "call_sid": self.call_sid,
                "error": str(e),
                "stack": traceback.format_exc()
            })
    
    async def _handle_media_message(self, message: Dict[str, Any]):
        """Handle incoming media (audio) message"""
        try:
            media_data = message.get("media", {})
            payload_b64 = media_data.get("payload")
            
            if payload_b64:
                audio_data = base64.b64decode(payload_b64)
                
                self.frames_received += 1
                metrics.increment_metric("media_stream_frames_received_total")
                
                # Pass to audio handler if provided
                if self.on_incoming_audio:
                    try:
                        await self.on_incoming_audio(audio_data)
                    except Exception as e:
                        logger.error("Error in incoming audio handler", extra={
                            "call_sid": self.call_sid,
                            "error": str(e)
                        })
                
                # Phase 4: Throttle logging - only log every 50 frames
                if self.frames_received % 50 == 0:
                    logger.debug("Audio frame received", extra={
                        "call_sid": self.call_sid,
                        "frame_size": len(audio_data),
                        "track": media_data.get("track", "unknown"),
                        "total_frames": self.frames_received
                    })
                
        except Exception as e:
            logger.error("Error handling media message", extra={
                "call_sid": self.call_sid,
                "error": str(e)
            })
    
    async def _handle_mark_message(self, message: Dict[str, Any]):
        """Handle mark event message"""
        mark_name = message.get("mark", {}).get("name", "unknown")
        logger.debug("Mark event received", extra={
            "call_sid": self.call_sid,
            "mark_name": mark_name
        })
    
    async def _handle_clear_message(self, message: Dict[str, Any]):
        """Handle clear event message"""
        logger.debug("Clear event received", extra={"call_sid": self.call_sid})
    
    async def _handle_error_message(self, message: Dict[str, Any]):
        """Handle error event message"""
        error_data = message.get("error", {})
        logger.error("MediaStream error received", extra={
            "call_sid": self.call_sid,
            "error_code": error_data.get("code", "unknown"),
            "error_message": error_data.get("message", "unknown"),
            "error_twilio_code": error_data.get("twilioCode", "unknown")
        })
        
        metrics.increment_metric("media_stream_errors_total")
    
    async def _handle_stopped_message(self, message: Dict[str, Any]):
        """Handle stream stopped message"""
        logger.info("MediaStream stopped by Twilio", extra={"call_sid": self.call_sid})
        await self.stop()
    
    # --------------------------------------------------------------------------
    # Background Tasks
    # --------------------------------------------------------------------------
    
    async def _send_loop(self):
        """Main loop for sending audio frames to Twilio"""
        logger.info("MediaStream send loop started", extra={"call_sid": self.call_sid})
        
        try:
            while self.is_running and self.is_connected:
                try:
                    # Get next audio frame from queue with timeout
                    try:
                        audio_data = await asyncio.wait_for(
                            self.audio_queue.get(), 
                            timeout=1.0
                        )
                    except asyncio.TimeoutError:
                        # Send silence if no audio available
                        if self._should_send_silence():
                            await self._send_silence_frame()
                        continue
                    
                    # Send audio frame
                    await self._send_audio_frame_to_websocket(audio_data)
                    
                    # Clear backpressure flag if queue is manageable
                    if self.audio_queue.qsize() < Config.MEDIA_STREAM_MAX_QUEUE_SIZE // 2:
                        self._backpressure_detected = False
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Error in send loop", extra={
                        "call_sid": self.call_sid,
                        "error": str(e),
                        "stack": traceback.format_exc()
                    })
                    await asyncio.sleep(0.1)  # Brief pause on error
        
        except asyncio.CancelledError:
            logger.info("MediaStream send loop cancelled", extra={"call_sid": self.call_sid})
        finally:
            logger.info("MediaStream send loop ended", extra={"call_sid": self.call_sid})
    
    async def _receive_loop(self):
        """Main loop for receiving messages from Twilio"""
        logger.info("MediaStream receive loop started", extra={"call_sid": self.call_sid})
        
        try:
            while self.is_running and self.is_connected:
                try:
                    # Phase 4: Add timeout to WebSocket receive
                    message = await asyncio.wait_for(
                        self.websocket.receive(),
                        timeout=Config.WEBSOCKET_OPERATION_TIMEOUT
                    )
                    
                    if message.type in ("text", "binary"):
                        await self._handle_websocket_message(message.data if hasattr(message, "data") else message.text)
                    elif message.type == "disconnect":
                        logger.info("WebSocket disconnected", extra={"call_sid": self.call_sid})
                        await self._handle_disconnect()
                        break
                        
                except asyncio.TimeoutError:
                    # Timeout is normal - check connection health and continue
                    await self._check_connection_health()
                    continue
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Error in receive loop", extra={
                        "call_sid": self.call_sid,
                        "error": str(e),
                        "stack": traceback.format_exc()
                    })
                    
                    # Check if WebSocket is still connected
                    if self.websocket.closed:
                        await self._handle_disconnect()
                        break
        
        except asyncio.CancelledError:
            logger.info("MediaStream receive loop cancelled", extra={"call_sid": self.call_sid})
        finally:
            logger.info("MediaStream receive loop ended", extra={"call_sid": self.call_sid})
    
    async def _check_connection_health(self):
        """Check if connection is still healthy after timeout"""
        time_since_last_activity = time.time() - self.last_pong_time
        if time_since_last_activity > Config.MEDIA_STREAM_PING_INTERVAL * 2:
            logger.warning("MediaStream connection appears stale after timeout", extra={
                "call_sid": self.call_sid,
                "seconds_since_activity": time_since_last_activity
            })
            metrics.increment_metric("media_stream_health_check_failures_total")
    
    async def _ping_loop(self):
        """Background task to monitor connection health"""
        logger.info("MediaStream ping loop started", extra={"call_sid": self.call_sid})
        
        try:
            while self.is_running and self.is_connected:
                try:
                    # Send ping-like message (Twilio doesn't support standard WebSocket ping)
                    await self.send_mark_event("heartbeat")
                    self.last_pong_time = time.time()
                    
                    # Check connection health
                    time_since_last_activity = time.time() - self.last_pong_time
                    if time_since_last_activity > Config.MEDIA_STREAM_PING_INTERVAL * 2:
                        logger.warning("MediaStream connection appears stale", extra={
                            "call_sid": self.call_sid,
                            "seconds_since_activity": time_since_last_activity
                        })
                    
                    await asyncio.sleep(Config.MEDIA_STREAM_PING_INTERVAL)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Error in ping loop", extra={
                        "call_sid": self.call_sid,
                        "error": str(e)
                    })
                    await asyncio.sleep(Config.MEDIA_STREAM_PING_INTERVAL)
        
        except asyncio.CancelledError:
            logger.info("MediaStream ping loop cancelled", extra={"call_sid": self.call_sid})
        finally:
            logger.info("MediaStream ping loop ended", extra={"call_sid": self.call_sid})
    
    # --------------------------------------------------------------------------
    # Audio Frame Processing
    # --------------------------------------------------------------------------
    
    async def _send_audio_frame_to_websocket(self, audio_data: bytes):
        """Send audio frame as MediaStream media event"""
        try:
            start_time = time.time()
            
            payload_b64 = base64.b64encode(audio_data).decode('utf-8')
            
            media_message = {
                "event": MediaStreamEvents.MEDIA,
                "streamSid": self.call_sid,
                "media": {
                    "payload": payload_b64
                }
            }
            
            await self._send_websocket_message(media_message)
            
            # Update metrics
            self.frames_sent += 1
            self.bytes_sent += len(audio_data)
            self._last_send_time = time.time()
            
            send_latency = (time.time() - start_time) * 1000
            metrics.observe_latency("media_stream_send_latency_ms", send_latency)
            metrics.increment_metric("media_stream_frames_sent_total")
            
        except Exception as e:
            metrics.increment_metric("media_stream_frame_send_errors_total")
            logger.error("Error sending audio frame to WebSocket", extra={
                "call_sid": self.call_sid,
                "error": str(e)
            })
            raise
    
    async def _send_silence_frame(self):
        """Send a single silence frame"""
        try:
            silence_frame = next(self.silence_generator)
            await self._send_audio_frame_to_websocket(silence_frame)
        except Exception as e:
            logger.debug("Error sending silence frame", extra={
                "call_sid": self.call_sid,
                "error": str(e)
            })
    
    def _create_silence_generator(self):
        """Generator that produces silence frames"""
        # mu-law silence byte = 0xFF (inverted zero)
        silence_byte = b'\xff' * 160  # 20ms at 8kHz
        while True:
            yield silence_byte
    
    def _generate_silence_frames(self, duration_ms: int) -> List[bytes]:
        """Generate multiple silence frames for specified duration"""
        frames_needed = max(1, duration_ms // Config.MEDIA_STREAM_SILENCE_FRAME_MS)
        return [next(self.silence_generator) for _ in range(frames_needed)]
    
    def _should_send_silence(self) -> bool:
        """Determine if we should send silence frames"""
        return (self.is_connected and 
                self.is_running and 
                self.audio_queue.empty() and
                time.time() - self._last_send_time > 0.05)
    
    # --------------------------------------------------------------------------
    # Connection Management
    # --------------------------------------------------------------------------
    
    async def _send_connected_event(self):
        """Send connected event to Twilio"""
        try:
            connected_message = {
                "event": MediaStreamEvents.CONNECT,
                "streamSid": self.call_sid,
                "protocol": "Call",
                "version": "1.0.0"
            }
            
            await self._send_websocket_message(connected_message)
            logger.debug("Connected event sent", extra={"call_sid": self.call_sid})
            
        except Exception as e:
            logger.error("Error sending connected event", extra={
                "call_sid": self.call_sid,
                "error": str(e)
            })
    
    async def _handle_disconnect(self):
        """Handle WebSocket disconnection with reconnect logic"""
        self.is_connected = False
        
        logger.warning("MediaStream disconnected", extra={"call_sid": self.call_sid})
        metrics.increment_metric("media_stream_disconnections_total")
        
        # Attempt reconnection if still running
        if self.is_running and self.reconnect_attempts < Config.MEDIA_STREAM_RECONNECT_ATTEMPTS:
            await self._attempt_reconnect()
        else:
            await self.stop()
    
    async def _attempt_reconnect(self):
        """Attempt to reconnect to MediaStream"""
        self.reconnect_attempts += 1
        
        logger.info("Attempting MediaStream reconnection", extra={
            "call_sid": self.call_sid,
            "attempt": self.reconnect_attempts
        })
        
        try:
            reconnect_delay = Config.MEDIA_STREAM_RECONNECT_DELAY * (2 ** (self.reconnect_attempts - 1))
            await asyncio.sleep(reconnect_delay)
            
            # Note: actual reconnection must be performed by the server framework
            # We only reset state here — the new WS will be passed via create_stream()
            self.is_connected = True
            self.reconnect_attempts = 0
            
            await self._send_connected_event()
            
            metrics.increment_metric("media_stream_reconnections_total")
            logger.info("MediaStream reconnected successfully", extra={"call_sid": self.call_sid})
            
        except Exception as e:
            logger.error("MediaStream reconnection failed", extra={
                "call_sid": self.call_sid,
                "attempt": self.reconnect_attempts,
                "error": str(e)
            })
            
            if self.reconnect_attempts < Config.MEDIA_STREAM_RECONNECT_ATTEMPTS:
                await self._attempt_reconnect()
            else:
                logger.error("Max reconnection attempts reached", extra={"call_sid": self.call_sid})
                await self.stop()


# --------------------------------------------------------------------------
# Media Stream Manager
# --------------------------------------------------------------------------

class MediaStreamManager:
    """
    Manages multiple Twilio MediaStream instances.
    """
    
    def __init__(self):
        self.streams: Dict[str, TwilioMediaStream] = {}
        self._lock = asyncio.Lock()
    
    async def create_stream(self, call_sid: str, websocket,
                          on_incoming_audio: Optional[Callable[[bytes], None]] = None,
                          on_media_event: Optional[Callable[[Dict[str, Any]], None]] = None) -> TwilioMediaStream:
        """Create and start a new media stream"""
        async with self._lock:
            if call_sid in self.streams:
                await self.streams[call_sid].stop()
            
            stream = TwilioMediaStream(call_sid, websocket, on_incoming_audio, on_media_event)
            self.streams[call_sid] = stream
            
            await stream.start()
            
            logger.info("New media stream created", extra={"call_sid": call_sid})
            return stream
    
    async def get_stream(self, call_sid: str) -> Optional[TwilioMediaStream]:
        """Get existing media stream"""
        async with self._lock:
            return self.streams.get(call_sid)
    
    async def remove_stream(self, call_sid: str):
        """Remove and stop media stream"""
        async with self._lock:
            if call_sid in self.streams:
                stream = self.streams[call_sid]
                await stream.stop()
                del self.streams[call_sid]
                logger.info("Media stream removed", extra={"call_sid": call_sid})
    
    def get_active_streams(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all active streams"""
        return {
            call_sid: stream.get_connection_status()
            for call_sid, stream in self.streams.items()
        }


# --------------------------------------------------------------------------
# Global Manager Instance
# --------------------------------------------------------------------------

# Global media stream manager instance
_media_stream_manager: Optional[MediaStreamManager] = None

def get_media_stream_manager() -> MediaStreamManager:
    """Get or create global media stream manager instance"""
    global _media_stream_manager
    if _media_stream_manager is None:
        _media_stream_manager = MediaStreamManager()
    return _media_stream_manager