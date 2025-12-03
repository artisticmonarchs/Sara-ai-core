"""
realtime_voice_engine.py — Phase 12 (Celery Task Resilience)
Low-latency streaming TTS engine for real-time voice responses with full fault tolerance, idempotency, and unified resilience patterns.
"""

import asyncio
import time
import json
import base64
import hashlib
import numpy as np
from typing import AsyncGenerator, Optional, Dict, Any, List, Tuple
import traceback
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import os
import audioop  # Added for audio format conversion

# --------------------------------------------------------------------------
# Phase 12: Configuration and Imports with Resilience Patterns
# --------------------------------------------------------------------------
try:
    from config import Config
except ImportError:
    class Config:
        TTS_PROVIDER = os.getenv("TTS_PROVIDER", "deepgram")
        DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
        ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
        TTS_STREAM_TIMEOUT = int(os.getenv("TTS_STREAM_TIMEOUT", "30"))
        TTS_FALLBACK_ENABLED = os.getenv("TTS_FALLBACK_ENABLED", "true").lower() == "true"
        AUDIO_BUFFER_SIZE = int(os.getenv("AUDIO_BUFFER_SIZE", "10"))
        MAX_JITTER_COMPENSATION_MS = int(os.getenv("MAX_JITTER_COMPENSATION_MS", "100"))
        SILENCE_DETECTION_THRESHOLD = float(os.getenv("SILENCE_DETECTION_THRESHOLD", "0.01"))
        SILENCE_DURATION_MS = int(os.getenv("SILENCE_DURATION_MS", "500"))
        THREAD_POOL_WORKERS = int(os.getenv("THREAD_POOL_WORKERS", "4"))
        CELERY_MAX_RETRIES = int(os.getenv("CELERY_MAX_RETRIES", "3"))
        REDIS_OPERATION_TIMEOUT = float(os.getenv("REDIS_OPERATION_TIMEOUT", "5.0"))
        EXTERNAL_API_TIMEOUT = float(os.getenv("EXTERNAL_API_TIMEOUT", "30.0"))
        R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "sara-ai-audio")
        # Added output format configuration
        TTS_OUTPUT_SAMPLE_RATE = int(os.getenv("TTS_OUTPUT_SAMPLE_RATE", "16000"))
        TTS_OUTPUT_BITS_PER_SAMPLE = int(os.getenv("TTS_OUTPUT_BITS_PER_SAMPLE", "16"))
        TTS_OUTPUT_CHANNELS = int(os.getenv("TTS_OUTPUT_CHANNELS", "1"))

def _stable_cache_key_from_text(text: str) -> str:
    """Generate stable cache key across processes/machines"""
    return f"tts_cache:{hashlib.sha1(text.encode('utf-8')).hexdigest()}"

# Phase 12: Transient error detection
class TransientError(Exception):
    """Base class for transient errors that should trigger retries"""
    pass

# Phase 12: Redis client compliance
try:
    from redis_client import get_client
except ImportError:
    def get_client():
        return None

try:
    from r2_client import get_r2_client
except ImportError:
    def get_r2_client():
        return None

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
        @staticmethod
        def histogram(*args, **kwargs): pass

# Phase 12: Structured logging
try:
    from logging_utils import get_logger
    logger = get_logger("realtime_voice_engine")
except ImportError:
    import logging
    logger = logging.getLogger("realtime_voice_engine")

# Phase 12: External API wrapper
try:
    from external_api import external_api_call_async
except ImportError:
    # Fallback implementation for async external API calls
    async def external_api_call_async(service, operation, *args, trace_id=None, **kwargs):
        return await operation(*args, **kwargs)

# Phase 12: Unified Timeout Metrics
class TimeoutMetrics:
    """Unified timeout metrics taxonomy for Phase 12 compliance"""
    
    @staticmethod
    def record_timeout(service: str, operation: str):
        """Record timeout with unified metric naming"""
        metrics.increment_metric(f"{service}_{operation}_timeout_total")
        metrics.increment_metric("global_timeout_events_total")
        
    @staticmethod
    def record_operation_timeout(operation: str, service: str = "voice_engine"):
        """MEDIUM PRIORITY: Unified timeout metric naming"""
        metrics.increment_metric(f"{service}.{operation}.timeout_total")
        metrics.increment_metric("global_timeout_events_total")

# Phase 12: Circuit breaker check
async def _is_circuit_breaker_open(service: str = "voice_engine") -> bool:
    """Check if circuit breaker is open for voice engine operations"""
    try:
        redis_client = get_client()
        if not redis_client:
            return False
        # MEDIUM PRIORITY FIX: Add timeout to circuit breaker check
        loop = asyncio.get_event_loop()
        state = await asyncio.wait_for(
            loop.run_in_executor(
                None,  # Use default executor
                lambda: redis_client.get(f"circuit_breaker:{service}:state")
            ),
            timeout=Config.REDIS_OPERATION_TIMEOUT
        )
        if not state:
            return False
        if isinstance(state, bytes):
            state = state.decode("utf-8")
        return state.lower() == "open"
    except asyncio.TimeoutError:
        logger.warning("Circuit breaker check timeout", extra={
            "service": "realtime_voice_engine",
            "operation": "circuit_breaker_check"
        })
        TimeoutMetrics.record_timeout("redis", "circuit_breaker_check")
        return False  # Fail closed - don't block on timeout
    except Exception:
        return False

# --------------------------------------------------------------------------
# Audio Format Conversion Utilities
# --------------------------------------------------------------------------

def _ensure_output_format(audio_data: bytes, input_sample_rate: int = 16000, 
                         input_bits_per_sample: int = 16, input_channels: int = 1) -> bytes:
    """
    Convert audio to standard output format: 16kHz, 16-bit, mono PCM.
    
    Args:
        audio_data: Raw audio bytes
        input_sample_rate: Source sample rate
        input_bits_per_sample: Source bits per sample (8, 16, 24, 32)
        input_channels: Source channel count
    
    Returns:
        bytes: Audio in 16kHz, 16-bit, mono PCM format
    """
    if not audio_data:
        return audio_data
    
    target_sample_rate = Config.TTS_OUTPUT_SAMPLE_RATE
    target_bits_per_sample = Config.TTS_OUTPUT_BITS_PER_SAMPLE
    target_channels = Config.TTS_OUTPUT_CHANNELS
    
    # Skip conversion if already in target format
    if (input_sample_rate == target_sample_rate and 
        input_bits_per_sample == target_bits_per_sample and 
        input_channels == target_channels):
        return audio_data
    
    # Convert to 16-bit PCM if needed
    processed_data = audio_data
    
    # Handle μ-law to PCM conversion if needed
    if input_bits_per_sample == 8 and len(audio_data) > 0:
        # Assume μ-law if 8-bit (Twilio compatibility)
        try:
            processed_data = audioop.ulaw2lin(audio_data, 2)  # Convert to 16-bit linear
            input_bits_per_sample = 16
        except Exception as e:
            logger.warning("Failed to convert μ-law to PCM, assuming already PCM", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "component": "audio_format"
            })
    
    # Resample if needed
    if input_sample_rate != target_sample_rate:
        try:
            # audioop.ratecv expects 16-bit PCM (2 bytes per sample)
            if input_bits_per_sample == 16:
                processed_data, _ = audioop.ratecv(
                    processed_data, 
                    2,  # 2 bytes per sample for 16-bit
                    input_channels, 
                    input_sample_rate, 
                    target_sample_rate, 
                    None
                )
                input_sample_rate = target_sample_rate
        except Exception as e:
            logger.error("Failed to resample audio", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "component": "audio_format",
                "from_rate": input_sample_rate,
                "to_rate": target_sample_rate
            })
    
    # Convert to mono if needed
    if input_channels != target_channels and input_channels > 1:
        try:
            # Simple average of channels (for stereo to mono)
            if input_bits_per_sample == 16 and input_channels == 2:
                # For 16-bit stereo, interleaved LRLR...
                mono_data = bytearray()
                for i in range(0, len(processed_data), 4):  # 2 bytes per channel * 2 channels
                    if i + 3 < len(processed_data):
                        left = audioop.getsample(processed_data, 2, i // 2)
                        right = audioop.getsample(processed_data, 2, i // 2 + 1)
                        avg = (left + right) // 2
                        mono_data.extend(audioop.lin2lin(audioop.getsample(avg, 2, 0), 2, 2))
                processed_data = bytes(mono_data)
                input_channels = 1
        except Exception as e:
            logger.error("Failed to convert to mono", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "component": "audio_format"
            })
    
    return processed_data

# --------------------------------------------------------------------------
# Data Structures
# --------------------------------------------------------------------------

@dataclass
class AudioPacket:
    """Packetized audio buffer with timing information"""
    data: bytes
    timestamp: float
    sequence: int
    duration_ms: float
    is_silence: bool = False

class AudioBufferManager:
    """Async packetized audio buffer manager with jitter compensation and Phase 12 resilience"""
    
    def __init__(self, max_size: int = 10):
        self.buffer = asyncio.Queue(maxsize=max_size)
        self.sequence_number = 0
        self.last_output_time = 0
        self.jitter_buffer_ms = 50  # Initial jitter buffer
        self.stats = {
            'packets_received': 0,
            'packets_played': 0,
            'jitter_compensations': 0,
            'buffer_overflows': 0,
            'backpressure_events': 0  # MEDIUM PRIORITY: Enhanced backpressure metrics
        }
        # Phase 4: Metrics batching
        self._metrics_buffer = {"chunks": 0, "bytes": 0, "last_flush": time.time()}
        
    async def put_packet(self, audio_data: bytes, duration_ms: float, is_silence: bool = False):
        """Add an audio packet to the buffer with Phase 12 resilience"""
        packet = AudioPacket(
            data=audio_data,
            timestamp=time.time(),
            sequence=self.sequence_number,
            duration_ms=duration_ms,
            is_silence=is_silence
        )
        
        try:
            # MEDIUM PRIORITY: Enhanced backpressure detection
            current_size = self.buffer.qsize()
            if current_size >= self.buffer.maxsize * 0.8:  # 80% capacity threshold
                self.stats['backpressure_events'] += 1
                metrics.increment_metric("audio_buffer_backpressure_events_total")
                metrics.set_gauge("audio_buffer_backpressure_level", 
                                (current_size / self.buffer.maxsize) * 100)
                
            self.buffer.put_nowait(packet)
            self.sequence_number += 1
            self.stats['packets_received'] += 1
            
            # Phase 4: Batch metrics updates
            self._metrics_buffer["chunks"] += 1
            self._metrics_buffer["bytes"] += len(audio_data)
            await self._flush_metrics_if_needed()
            
        except asyncio.QueueFull:
            self.stats['buffer_overflows'] += 1
            metrics.increment_metric("audio_buffer_overflows_total")
            logger.warning("Audio buffer overflow, dropping packet", extra={
                "service": "realtime_voice_engine",
                "component": "audio_buffer",
                "buffer_size": self.buffer.qsize(),
                "max_size": self.buffer.maxsize
            })
    
    async def _flush_metrics_if_needed(self):
        """Flush batched metrics if enough time has passed"""
        now = time.time()
        if now - self._metrics_buffer["last_flush"] >= 1.0:  # 1 second batching
            if self._metrics_buffer["chunks"] > 0:
                metrics.set_gauge("audio_buffer_size", self.buffer.qsize())
                metrics.increment_metric("audio_packets_received_total", self._metrics_buffer["chunks"])
                self._metrics_buffer.update(chunks=0, bytes=0, last_flush=now)
    
    async def get_packet(self) -> Optional[AudioPacket]:
        """Get the next audio packet with jitter compensation and Phase 12 resilience"""
        if self.buffer.empty():
            return None
            
        packet = await self.buffer.get()
        current_time = time.time()
        
        # Calculate jitter and apply compensation
        expected_time = self.last_output_time + (packet.duration_ms / 1000)
        if self.last_output_time > 0:
            jitter = current_time - expected_time
            jitter_ms = jitter * 1000
            
            # Update jitter buffer dynamically
            if abs(jitter_ms) > Config.MAX_JITTER_COMPENSATION_MS:
                self.jitter_buffer_ms = min(200, max(10, self.jitter_buffer_ms * 1.1))
                self.stats['jitter_compensations'] += 1
                metrics.histogram("audio_jitter_ms", jitter_ms)
            else:
                self.jitter_buffer_ms = max(10, self.jitter_buffer_ms * 0.99)
        
        # Apply jitter compensation delay
        if self.last_output_time > 0 and current_time < expected_time:
            compensation_delay = expected_time - current_time
            await asyncio.sleep(min(compensation_delay, self.jitter_buffer_ms / 1000))
        
        self.last_output_time = time.time()
        self.stats['packets_played'] += 1
        metrics.increment_metric("audio_packets_played_total")
        
        return packet
    
    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        return {
            **self.stats,
            'current_size': self.buffer.qsize(),
            'max_size': self.buffer.maxsize,
            'jitter_buffer_ms': self.jitter_buffer_ms,
            'sequence_number': self.sequence_number,
            'utilization_percent': (self.buffer.qsize() / self.buffer.maxsize) * 100
        }

class SilenceDetector:
    """Dynamic silence detection for pausing inference with Phase 12 resilience"""
    
    def __init__(self, threshold: float = 0.01, min_silence_duration_ms: int = 500):
        self.threshold = threshold
        self.min_silence_duration_ms = min_silence_duration_ms
        self.silence_start_time = None
        self.is_silence_detected = False
        self.consecutive_silence_frames = 0
        self.min_silence_frames = int((min_silence_duration_ms / 20))  # Assuming 20ms frames
        
    def analyze_audio(self, audio_data: bytes) -> bool:
        """Analyze audio data for silence with Phase 12 resilience"""
        try:
            # Convert audio bytes to numpy array for analysis
            audio_array = np.frombuffer(audio_data, dtype=np.int16)
            
            # Calculate RMS energy
            if len(audio_array) > 0:
                rms_energy = np.sqrt(np.mean(audio_array.astype(np.float32) ** 2)) / 32768.0
                
                is_silent = rms_energy < self.threshold
                
                if is_silent:
                    self.consecutive_silence_frames += 1
                    if self.consecutive_silence_frames >= self.min_silence_frames:
                        if not self.is_silence_detected:
                            self.silence_start_time = time.time()
                            self.is_silence_detected = True
                            metrics.increment_metric("silence_detected_total")
                else:
                    self.consecutive_silence_frames = 0
                    self.is_silence_detected = False
                
                # Update metrics
                metrics.histogram("audio_energy_level", rms_energy)
                metrics.set_gauge("silence_detected", 1 if self.is_silence_detected else 0)
                
                return self.is_silence_detected
                
        except Exception as e:
            logger.error("Silence detection error", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "component": "silence_detector"
            })
        
        return False
    
    def reset(self):
        """Reset silence detection state"""
        self.silence_start_time = None
        self.is_silence_detected = False
        self.consecutive_silence_frames = 0

# --------------------------------------------------------------------------
# Performance Counters with Phase 12 Resilience
# --------------------------------------------------------------------------

class PerformanceCounters:
    """Prometheus performance counters for audio processing with Phase 12 resilience"""
    
    @staticmethod
    def record_audio_processing_latency(latency_ms: float):
        """Record audio processing latency"""
        metrics.observe_latency("audio_processing_latency_ms", latency_ms)
        metrics.histogram("audio_processing_latency_distribution_ms", latency_ms)
    
    @staticmethod
    def record_buffer_health(buffer_size: int, capacity: int):
        """Record buffer health metrics"""
        utilization = (buffer_size / capacity) * 100 if capacity > 0 else 0
        metrics.set_gauge("audio_buffer_utilization_percent", utilization)
        metrics.histogram("audio_buffer_utilization_distribution", utilization)
        
        # MEDIUM PRIORITY: Enhanced backpressure metrics
        if utilization > 80:
            metrics.increment_metric("audio_buffer_high_utilization_events_total")
    
    @staticmethod
    def record_packet_loss(lost_packets: int, total_packets: int):
        """Record packet loss metrics"""
        if total_packets > 0:
            loss_percentage = (lost_packets / total_packets) * 100
            metrics.set_gauge("audio_packet_loss_percent", loss_percentage)
            metrics.histogram("audio_packet_loss_distribution", loss_percentage)
    
    @staticmethod
    def record_voice_activity(has_voice: bool):
        """Record voice activity metrics"""
        metrics.set_gauge("voice_activity_detected", 1 if has_voice else 0)
        if has_voice:
            metrics.increment_metric("voice_activity_events_total")

# --------------------------------------------------------------------------
# TTS Provider Interfaces with Phase 12 Resilience
# --------------------------------------------------------------------------

class TTSProvider:
    """Base class for TTS providers with Phase 12 resilience patterns"""
    
    async def stream_audio(self, text: str, voice_settings: Optional[Dict] = None) -> AsyncGenerator[bytes, None]:
        """Stream audio for given text with resilience"""
        raise NotImplementedError
    
    async def validate_connection(self) -> bool:
        """Validate provider connection with resilience"""
        raise NotImplementedError

class DeepgramTTSProvider(TTSProvider):
    """Deepgram streaming TTS implementation with Phase 12 resilience"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.deepgram.com/v1/speak"
        self.voice_settings = {
            "model": os.getenv("DEEPGRAM_SPEAK_MODEL", "aura-2-asteria-en"),
            "encoding": "mulaw",  # μ-law 8kHz for Twilio compatibility
            "container": "none",
            "sample_rate": 8000
        }
        self.thread_pool = ThreadPoolExecutor(max_workers=Config.THREAD_POOL_WORKERS)
    
    async def stream_audio(self, text: str, voice_settings: Optional[Dict] = None) -> AsyncGenerator[bytes, None]:
        """Stream audio using Deepgram API with Phase 12 resilience"""
        import aiohttp
        
        # Phase 12: Circuit breaker check
        if await _is_circuit_breaker_open("deepgram_tts"):
            logger.warning("Deepgram TTS blocked by circuit breaker", extra={
                "service": "realtime_voice_engine",
                "provider": "deepgram"
            })
            metrics.increment_metric("deepgram_tts_circuit_breaker_hits_total")
            raise TransientError("Deepgram TTS blocked by circuit breaker")
        
        settings = {**self.voice_settings, **(voice_settings or {})}
        
        try:
            # Run blocking operations in thread pool
            loop = asyncio.get_event_loop()
            
            # Use external API wrapper for Deepgram calls with timeout
            async def make_deepgram_request():
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self.base_url,
                        headers={
                            "Authorization": f"Token {self.api_key}",
                            "Content-Type": "application/json"
                        },
                        json={
                            "text": text,
                            **settings
                        },
                        timeout=aiohttp.ClientTimeout(total=Config.TTS_STREAM_TIMEOUT)
                    ) as response:
                        return response
            
            # Phase 4: Add timeout wrapper
            response = await asyncio.wait_for(
                external_api_call_async(
                    "deepgram_tts",
                    make_deepgram_request,
                    trace_id=None
                ),
                timeout=Config.EXTERNAL_API_TIMEOUT
            )
            
            if response.status == 200:
                chunk_count = 0
                async for chunk in response.content.iter_chunked(1024):
                    if chunk:
                        chunk_count += 1
                        # Phase 4: Throttle logging - only log every 50 chunks
                        if chunk_count % 50 == 0:
                            logger.debug("Processing TTS chunk", extra={
                                "chunk_number": chunk_count,
                                "chunk_size": len(chunk),
                                "service": "realtime_voice_engine"
                            })
                        
                        # Process chunk: Deepgram outputs μ-law 8kHz, convert to 16kHz 16-bit PCM
                        processed_chunk = await loop.run_in_executor(
                            self.thread_pool, 
                            self._process_audio_chunk, 
                            chunk
                        )
                        yield processed_chunk
            else:
                error_text = await response.text()
                logger.error("Deepgram TTS request failed", extra={
                    "status": response.status,
                    "error": error_text,
                    "service": "realtime_voice_engine",
                    "provider": "deepgram"
                })
                raise TransientError(f"Deepgram TTS failed: {error_text}")
                        
        except asyncio.TimeoutError:
            logger.error("Deepgram TTS request timed out", extra={
                "service": "realtime_voice_engine",
                "provider": "deepgram"
            })
            TimeoutMetrics.record_timeout("deepgram_tts", "stream_audio")
            metrics.increment_metric("deepgram_tts_timeout_errors_total")
            raise TransientError("Deepgram TTS request timed out")
        except Exception as e:
            logger.error("Deepgram TTS streaming error", extra={
                "error": str(e),
                "service": "realtime_voice_engine", 
                "provider": "deepgram"
            })
            metrics.increment_metric("deepgram_tts_streaming_errors_total")
            raise TransientError(f"Deepgram TTS streaming error: {str(e)}") from e
    
    def _process_audio_chunk(self, chunk: bytes) -> bytes:
        """Process audio chunk - Deepgram outputs μ-law 8kHz, convert to 16kHz 16-bit PCM"""
        # Convert μ-law 8kHz to 16kHz 16-bit PCM
        if not chunk:
            return chunk
        
        try:
            # Convert μ-law to 16-bit linear PCM
            pcm_data = audioop.ulaw2lin(chunk, 2)  # 2 bytes per sample for 16-bit
            
            # Resample from 8kHz to 16kHz
            resampled_data, _ = audioop.ratecv(
                pcm_data,
                2,  # 2 bytes per sample
                1,  # 1 channel (mono)
                8000,  # Source sample rate
                16000,  # Target sample rate
                None
            )
            
            return resampled_data
        except Exception as e:
            logger.error("Failed to process Deepgram audio chunk", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "provider": "deepgram"
            })
            # Return original chunk as fallback
            return chunk
    
    async def validate_connection(self) -> bool:
        """Validate Deepgram connection with Phase 12 resilience"""
        try:
            import aiohttp
            
            # Use external API wrapper for connection validation with timeout
            async def validate_connection_internal():
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        "https://api.deepgram.com/v1/projects",
                        headers={"Authorization": f"Token {self.api_key}"},
                        timeout=10
                    ) as response:
                        return response.status == 200
            
            return await asyncio.wait_for(
                external_api_call_async(
                    "deepgram_validation",
                    validate_connection_internal,
                    trace_id=None
                ),
                timeout=Config.EXTERNAL_API_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.error("Deepgram connection validation timed out", extra={
                "service": "realtime_voice_engine",
                "provider": "deepgram"
            })
            TimeoutMetrics.record_timeout("deepgram_tts", "validate_connection")
            return False
        except Exception as e:
            logger.error("Deepgram connection validation failed", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "provider": "deepgram"
            })
            return False

class ElevenLabsTTSProvider(TTSProvider):
    """ElevenLabs streaming TTS implementation with Phase 12 resilience"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.elevenlabs.io/v1/text-to-speech"
        self.voice_id = "21m00Tcm4TlvDq8ikWAM"  # Default voice
        self.thread_pool = ThreadPoolExecutor(max_workers=Config.THREAD_POOL_WORKERS)
    
    async def stream_audio(self, text: str, voice_settings: Optional[Dict] = None) -> AsyncGenerator[bytes, None]:
        """Stream audio using ElevenLabs API with Phase 12 resilience"""
        import aiohttp
        
        # Phase 12: Circuit breaker check
        if await _is_circuit_breaker_open("elevenlabs_tts"):
            logger.warning("ElevenLabs TTS blocked by circuit breaker", extra={
                "service": "realtime_voice_engine",
                "provider": "elevenlabs"
            })
            metrics.increment_metric("elevenlabs_tts_circuit_breaker_hits_total")
            raise TransientError("ElevenLabs TTS blocked by circuit breaker")
        
        voice_id = voice_settings.get("voice_id", self.voice_id) if voice_settings else self.voice_id
        
        try:
            # Run blocking operations in thread pool
            loop = asyncio.get_event_loop()
            
            # Use external API wrapper for ElevenLabs calls with timeout
            async def make_elevenlabs_request():
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.base_url}/{voice_id}/stream",
                        headers={
                            "Xi-Api-Key": self.api_key,
                            "Content-Type": "application/json"
                        },
                        json={
                            "text": text,
                            "model_id": "eleven_monolingual_v1",
                            "voice_settings": {
                                "stability": 0.5,
                                "similarity_boost": 0.5
                            }
                        },
                        timeout=aiohttp.ClientTimeout(total=Config.TTS_STREAM_TIMEOUT)
                    ) as response:
                        return response
            
            # Phase 4: Add timeout wrapper
            response = await asyncio.wait_for(
                external_api_call_async(
                    "elevenlabs_tts",
                    make_elevenlabs_request,
                    trace_id=None
                ),
                timeout=Config.EXTERNAL_API_TIMEOUT
            )
            
            if response.status == 200:
                chunk_count = 0
                async for chunk in response.content.iter_chunked(1024):
                    if chunk:
                        chunk_count += 1
                        # Phase 4: Throttle logging - only log every 50 chunks
                        if chunk_count % 50 == 0:
                            logger.debug("Processing TTS chunk", extra={
                                "chunk_number": chunk_count,
                                "chunk_size": len(chunk),
                                "service": "realtime_voice_engine"
                            })
                        
                        # Process chunk: ElevenLabs outputs PCM, ensure 16kHz 16-bit mono
                        processed_chunk = await loop.run_in_executor(
                            self.thread_pool, 
                            self._process_audio_chunk, 
                            chunk
                        )
                        yield processed_chunk
            else:
                error_text = await response.text()
                logger.error("ElevenLabs TTS request failed", extra={
                    "status": response.status,
                    "error": error_text,
                    "service": "realtime_voice_engine",
                    "provider": "elevenlabs"
                })
                raise TransientError(f"ElevenLabs TTS failed: {error_text}")
                        
        except asyncio.TimeoutError:
            logger.error("ElevenLabs TTS request timed out", extra={
                "service": "realtime_voice_engine",
                "provider": "elevenlabs"
            })
            TimeoutMetrics.record_timeout("elevenlabs_tts", "stream_audio")
            metrics.increment_metric("elevenlabs_tts_timeout_errors_total")
            raise TransientError("ElevenLabs TTS request timed out")
        except Exception as e:
            logger.error("ElevenLabs TTS streaming error", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "provider": "elevenlabs"
            })
            metrics.increment_metric("elevenlabs_tts_streaming_errors_total")
            raise TransientError(f"ElevenLabs TTS streaming error: {str(e)}") from e
    
    def _process_audio_chunk(self, chunk: bytes) -> bytes:
        """Process audio chunk - ElevenLabs outputs PCM, ensure 16kHz 16-bit mono"""
        # Note: ElevenLabs API outputs 24kHz PCM by default
        # We need to convert to 16kHz 16-bit mono
        if not chunk:
            return chunk
        
        try:
            # Assume input is 24kHz, 16-bit, mono (common ElevenLabs default)
            # Convert to target format
            processed_chunk = _ensure_output_format(
                chunk,
                input_sample_rate=24000,  # ElevenLabs default
                input_bits_per_sample=16,
                input_channels=1
            )
            return processed_chunk
        except Exception as e:
            logger.error("Failed to process ElevenLabs audio chunk", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "provider": "elevenlabs"
            })
            # Return original chunk as fallback
            return chunk
    
    async def validate_connection(self) -> bool:
        """Validate ElevenLabs connection with Phase 12 resilience"""
        try:
            import aiohttp
            
            # Use external API wrapper for connection validation with timeout
            async def validate_connection_internal():
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        "https://api.elevenlabs.io/v1/user",
                        headers={"Xi-Api-Key": self.api_key},
                        timeout=10
                    ) as response:
                        return response.status == 200
            
            return await asyncio.wait_for(
                external_api_call_async(
                    "elevenlabs_validation",
                    validate_connection_internal,
                    trace_id=None
                ),
                timeout=Config.EXTERNAL_API_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.error("ElevenLabs connection validation timed out", extra={
                "service": "realtime_voice_engine",
                "provider": "elevenlabs"
            })
            TimeoutMetrics.record_timeout("elevenlabs_tts", "validate_connection")
            return False
        except Exception as e:
            logger.error("ElevenLabs connection validation failed", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "provider": "elevenlabs"
            })
            return False

class FallbackTTSProvider(TTSProvider):
    """Fallback TTS provider using cached phrases with Phase 12 resilience"""
    
    def __init__(self):
        self.redis_client = get_client()
        self.r2_client = get_r2_client()
        self.thread_pool = ThreadPoolExecutor(max_workers=Config.THREAD_POOL_WORKERS)
        self.silence_detector = SilenceDetector()
    
    async def stream_audio(self, text: str, voice_settings: Optional[Dict] = None) -> AsyncGenerator[bytes, None]:
        """Try to get cached audio or generate simple fallback with Phase 12 resilience"""
        
        # Try to get from Redis cache first
        cached_audio = await self._get_cached_audio(text)
        if cached_audio:
            logger.info("Serving cached TTS audio", extra={
                "text_length": len(text),
                "service": "realtime_voice_engine",
                "provider": "fallback"
            })
            # Ensure cached audio is in correct format
            processed_audio = await self._ensure_audio_format(cached_audio)
            yield processed_audio
            return
        
        # Try to get from R2 cache
        r2_audio = await self._get_r2_cached_audio(text)
        if r2_audio:
            logger.info("Serving R2 cached TTS audio", extra={
                "text_length": len(text),
                "service": "realtime_voice_engine", 
                "provider": "fallback"
            })
            # Ensure cached audio is in correct format
            processed_audio = await self._ensure_audio_format(r2_audio)
            # Cache in Redis for future use
            await self._cache_audio(text, processed_audio)
            yield processed_audio
            return
        
        # Generate simple fallback audio (could be a beep or simple tone)
        # For now, return empty - caller should handle this case
        logger.warning("No cached audio available for fallback", extra={
            "text": text[:100],
            "service": "realtime_voice_engine",
            "provider": "fallback"
        })
        yield b""
    
    async def _ensure_audio_format(self, audio_data: bytes) -> bytes:
        """Ensure audio is in 16kHz 16-bit mono PCM format"""
        if not audio_data:
            return audio_data
        
        try:
            loop = asyncio.get_event_loop()
            processed_data = await loop.run_in_executor(
                self.thread_pool,
                lambda: _ensure_output_format(audio_data)
            )
            return processed_data
        except Exception as e:
            logger.error("Failed to ensure audio format", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "provider": "fallback"
            })
            return audio_data
    
    async def _get_cached_audio(self, text: str) -> Optional[bytes]:
        """Get cached audio from Redis with Phase 12 resilience"""
        if not self.redis_client:
            return None
        
        try:
            cache_key = _stable_cache_key_from_text(text)
            # MEDIUM PRIORITY FIX: Enhanced timeout protection for Redis operations
            loop = asyncio.get_event_loop()
            cached = await asyncio.wait_for(
                loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.redis_client.get(cache_key)
                ),
                timeout=Config.REDIS_OPERATION_TIMEOUT
            )
            if cached:
                metrics.increment_metric("redis_cache_hits_total")
                return base64.b64decode(cached)
            else:
                metrics.increment_metric("redis_cache_misses_total")
        except asyncio.TimeoutError:
            logger.warning("Redis cache get timeout", extra={
                "service": "realtime_voice_engine",
                "provider": "fallback",
                "operation": "get_cached_audio"
            })
            TimeoutMetrics.record_timeout("redis", "get_cached_audio")
            metrics.increment_metric("redis_timeout_errors_total")
        except Exception as e:
            # Emit metric for Redis failure
            metrics.increment_metric("redis.call.error")
            logger.error("Redis cache get failed", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "provider": "fallback"
            })
        
        return None
    
    async def _get_r2_cached_audio(self, text: str) -> Optional[bytes]:
        """Get cached audio from R2 with Phase 12 resilience"""
        if not self.r2_client:
            return None
        
        try:
            # Run blocking R2 operation in thread pool with timeout
            loop = asyncio.get_event_loop()
            object_key = f"tts_cache/{hashlib.sha1(text.encode('utf-8')).hexdigest()}.wav"
            
            # Use external API wrapper for R2 operations with timeout
            async def get_r2_object_async():
                return await loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.r2_client.get_object(
                        Bucket=Config.R2_BUCKET_NAME,
                        Key=object_key
                    )
                )
            
            response = await asyncio.wait_for(
                external_api_call_async(
                    "r2_cache",
                    get_r2_object_async,
                    trace_id=None
                ),
                timeout=Config.EXTERNAL_API_TIMEOUT
            )
            if response and 'Body' in response:
                metrics.increment_metric("r2_cache_hits_total")
                return response['Body'].read()
            else:
                metrics.increment_metric("r2_cache_misses_total")
                return None
        except asyncio.TimeoutError:
            logger.warning("R2 cache get timeout", extra={
                "service": "realtime_voice_engine",
                "provider": "fallback",
                "operation": "get_r2_cached_audio"
            })
            TimeoutMetrics.record_timeout("r2", "get_cached_audio")
            metrics.increment_metric("r2_timeout_errors_total")
            return None
        except Exception as e:
            logger.debug("R2 cache miss", extra={
                "error": str(e),
                "service": "realtime_voice_engine",
                "provider": "fallback"
            })
            metrics.increment_metric("r2_cache_errors_total")
            return None
    
    async def _cache_audio(self, text: str, audio_data: bytes):
        """Cache audio in Redis with Phase 12 resilience"""
        if not self.redis_client or not audio_data:
            return
        
        try:
            cache_key = _stable_cache_key_from_text(text)
            # MEDIUM PRIORITY FIX: Enhanced timeout protection for Redis operations
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(
                loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.redis_client.setex(
                        cache_key,
                        86400,  # 24 hour TTL
                        base64.b64encode(audio_data).decode('utf-8')
                    )
                ),
                timeout=Config.REDIS_OPERATION_TIMEOUT
            )
            metrics.increment_metric("redis_cache_sets_total")
        except asyncio.TimeoutError:
            logger.warning("Redis cache set timeout", extra={
                "service": "realtime_voice_engine",
                "provider": "fallback",
                "operation": "cache_audio"
            })
            TimeoutMetrics.record_timeout("redis", "cache_audio")
            metrics.increment_metric("redis_timeout_errors_total")
        except Exception as e:
            # Emit metric for Redis failure
            metrics.increment_metric("redis.call.error")
            logger.error("Redis cache set failed", extra={
                "error": str(e),
                "service": "realtime_voice_engine", 
                "provider": "fallback"
            })
    
    async def validate_connection(self) -> bool:
        """Fallback provider is always available"""
        return True

# --------------------------------------------------------------------------
# Main RealtimeVoiceEngine Class with Phase 12 Resilience
# --------------------------------------------------------------------------

class RealtimeVoiceEngine:
    """
    Phase 12: Low-latency streaming TTS engine with fallback support and full resilience patterns.
    Provides < 300 ms startup latency for real-time responses.
    """
    
    def __init__(self, provider: Optional[str] = None):
        self.provider_name = provider or Config.TTS_PROVIDER
        self.active_provider: Optional[TTSProvider] = None
        self.fallback_provider = FallbackTTSProvider()
        self.is_connected = False
        self.stream_start_time = 0
        
        # New components
        self.audio_buffer = AudioBufferManager(max_size=Config.AUDIO_BUFFER_SIZE)
        self.silence_detector = SilenceDetector(
            threshold=Config.SILENCE_DETECTION_THRESHOLD,
            min_silence_duration_ms=Config.SILENCE_DURATION_MS
        )
        self.thread_pool = ThreadPoolExecutor(max_workers=Config.THREAD_POOL_WORKERS)
        self._inference_paused = False
        
        self._initialize_provider()
    
    def _initialize_provider(self):
        """Initialize the configured TTS provider with Phase 12 resilience"""
        try:
            if self.provider_name == "deepgram":
                if not Config.DEEPGRAM_API_KEY:
                    raise ValueError("Deepgram API key not configured")
                self.active_provider = DeepgramTTSProvider(Config.DEEPGRAM_API_KEY)
                
            elif self.provider_name == "elevenlabs":
                if not Config.ELEVENLABS_API_KEY:
                    raise ValueError("ElevenLabs API key not configured")
                self.active_provider = ElevenLabsTTSProvider(Config.ELEVENLABS_API_KEY)
                
            else:
                logger.warning(f"Unknown TTS provider: {self.provider_name}, using fallback", extra={
                    "service": "realtime_voice_engine"
                })
                self.active_provider = None
            
            logger.info("TTS provider initialized", extra={
                "provider": self.provider_name,
                "service": "realtime_voice_engine"
            })
            
        except Exception as e:
            logger.error("Failed to initialize TTS provider", extra={
                "provider": self.provider_name,
                "error": str(e),
                "service": "realtime_voice_engine"
            })
            self.active_provider = None
    
    async def start(self):
        """Initialize the voice engine with Phase 12 resilience"""
        try:
            if self.active_provider:
                self.is_connected = await self.active_provider.validate_connection()
            else:
                self.is_connected = False
            
            metrics.set_gauge("tts_provider_status", 1 if self.is_connected else 0)
            logger.info("Voice engine started", extra={
                "provider": self.provider_name,
                "connected": self.is_connected,
                "service": "realtime_voice_engine"
            })
            
        except Exception as e:
            logger.error("Failed to start voice engine", extra={
                "error": str(e),
                "service": "realtime_voice_engine"
            })
            self.is_connected = False
    
    async def stop(self):
        """Cleanup the voice engine with Phase 12 resilience"""
        self.is_connected = False
        self.thread_pool.shutdown(wait=False)
        logger.info("Voice engine stopped", extra={
            "service": "realtime_voice_engine"
        })
    
    async def text_to_speech_stream(self, text: str, voice_settings: Optional[Dict] = None) -> AsyncGenerator[bytes, None]:
        """
        Convert text to streaming audio with low latency and Phase 12 resilience.
        Supports both plain text and SSML input.
        
        Yields audio chunks in 16kHz, 16-bit, mono PCM format.
        """
        self.stream_start_time = time.time()
        chunks_yielded = 0
        # CRITICAL FIX: Pre-declare provider to prevent UnboundLocalError
        provider: Optional[TTSProvider] = None

        try:
            metrics.increment_metric("tts_stream_requests_total")
            
            # Phase 12: Circuit breaker check
            if await _is_circuit_breaker_open("tts_streaming"):
                logger.warning("TTS streaming blocked by circuit breaker", extra={
                    "service": "realtime_voice_engine"
                })
                metrics.increment_metric("tts_streaming_circuit_breaker_hits_total")
                raise TransientError("TTS streaming blocked by circuit breaker")
            
            # Measure startup latency
            startup_start = time.time()
            
            # Choose provider (primary with fallback)
            provider = self.active_provider if self.is_connected else self.fallback_provider
            
            if not provider and Config.TTS_FALLBACK_ENABLED:
                provider = self.fallback_provider
            
            if not provider:
                raise TransientError("No TTS provider available")
            
            # Pre-process text for SSML support
            processed_text = self._preprocess_text(text, voice_settings or {})
            
            startup_latency = (time.time() - startup_start) * 1000
            metrics.observe_latency("tts_stream_start_latency_ms", startup_latency)
            
            logger.info("TTS stream starting", extra={
                "text_length": len(text),
                "processed_length": len(processed_text),
                "startup_latency_ms": startup_latency,
                "provider": provider.__class__.__name__,
                "service": "realtime_voice_engine"
            })
            
            # Stream audio chunks through buffer manager
            async for audio_chunk in provider.stream_audio(processed_text, voice_settings):
                if audio_chunk:  # Skip empty chunks
                    # Analyze for silence detection
                    is_silence = await self._analyze_audio_chunk(audio_chunk)
                    
                    # Calculate chunk duration (approximate)
                    # 16kHz 16-bit mono: bytes_per_ms = (16000 * 2) / 1000 = 32 bytes/ms
                    duration_ms = len(audio_chunk) / 32
                    
                    # Add to buffer manager
                    await self.audio_buffer.put_packet(audio_chunk, duration_ms, is_silence)
                    
                    # Get packet from buffer (with jitter compensation)
                    packet = await self.audio_buffer.get_packet()
                    if packet:
                        chunks_yielded += 1
                        yield packet.data
            
            # Log stream completion
            stream_duration = (time.time() - self.stream_start_time) * 1000
            metrics.observe_latency("tts_stream_duration_ms", stream_duration)
            metrics.increment_metric("tts_chunks_total", chunks_yielded)
            
            # Record performance metrics
            buffer_stats = self.audio_buffer.get_stats()
            PerformanceCounters.record_buffer_health(
                buffer_stats['current_size'], 
                Config.AUDIO_BUFFER_SIZE
            )
            
            logger.info("TTS stream completed", extra={
                "chunks_yielded": chunks_yielded,
                "total_duration_ms": stream_duration,
                "avg_chunk_rate": chunks_yielded / (stream_duration / 1000) if stream_duration > 0 else 0,
                "buffer_stats": buffer_stats,
                "service": "realtime_voice_engine"
            })
            
        except asyncio.CancelledError:
            # Stream was interrupted - log metrics
            interrupted_duration = (time.time() - self.stream_start_time) * 1000
            metrics.increment_metric("tts_stream_interrupted_total")
            metrics.observe_latency("tts_stream_interrupted_duration_ms", interrupted_duration)
            
            logger.info("TTS stream interrupted", extra={
                "chunks_yielded": chunks_yielded,
                "interrupted_at_ms": interrupted_duration,
                "service": "realtime_voice_engine"
            })
            raise
            
        except Exception as e:
            metrics.increment_metric("tts_stream_errors_total")
            logger.error("TTS stream error", extra={
                "error": str(e),
                "chunks_yielded": chunks_yielded,
                "stack": traceback.format_exc(),
                "service": "realtime_voice_engine"
            })
            
            # Try fallback if primary provider failed
            if (provider != self.fallback_provider and 
                Config.TTS_FALLBACK_ENABLED and 
                chunks_yielded == 0):
                
                logger.info("Attempting fallback TTS", extra={
                    "service": "realtime_voice_engine"
                })
                try:
                    async for fallback_chunk in self.fallback_provider.stream_audio(text, voice_settings):
                        if fallback_chunk:
                            yield fallback_chunk
                    metrics.increment_metric("tts_fallback_used_total")
                except Exception as fallback_error:
                    logger.error("Fallback TTS also failed", extra={
                        "error": str(fallback_error),
                        "service": "realtime_voice_engine"
                    })
                    raise TransientError(f"Primary and fallback TTS failed: {str(e)}") from e
            else:
                raise TransientError(f"TTS stream failed: {str(e)}") from e

    async def synthesize_text_to_frames(self, text: str, voice_settings: Optional[Dict] = None) -> List[bytes]:
        """
        Synthesize text to Twilio-compatible audio frames.
        
        Returns:
            List[bytes]: List of 20ms PCM frames at 8kHz sample rate, μ-law encoded, suitable for Twilio
        """
        frames = []
        total_audio = b""
        
        try:
            # Collect all audio chunks from TTS stream (in 16kHz 16-bit PCM)
            async for audio_chunk in self.text_to_speech_stream(text, voice_settings):
                if audio_chunk:
                    total_audio += audio_chunk
            
            # Convert 16kHz 16-bit PCM to 8kHz μ-law for Twilio
            # First resample 16kHz → 8kHz
            try:
                # Resample to 8kHz
                resampled_audio, _ = audioop.ratecv(
                    total_audio,
                    2,  # 2 bytes per sample for 16-bit
                    1,  # 1 channel (mono)
                    16000,  # Source sample rate
                    8000,   # Target sample rate
                    None
                )
                
                # Convert to μ-law
                ulaw_audio = audioop.lin2ulaw(resampled_audio, 2)
                
                # Split into 20ms frames (160 bytes per frame for 8kHz μ-law)
                # 8000 samples/sec * 0.02s = 160 samples, μ-law uses 1 byte per sample
                frame_size = 160
                for i in range(0, len(ulaw_audio), frame_size):
                    frame = ulaw_audio[i:i + frame_size]
                    if len(frame) == frame_size:  # Only yield complete frames
                        frames.append(frame)
                    elif len(frame) > 0:  # Pad final frame if needed
                        padded_frame = frame + b"\x00" * (frame_size - len(frame))
                        frames.append(padded_frame)
            except Exception as e:
                logger.error("Failed to convert audio to Twilio format", extra={
                    "error": str(e),
                    "service": "realtime_voice_engine"
                })
                # Fallback: use original audio
                frame_size = 320  # 8kHz 16-bit PCM: 8000 * 2 * 0.02 = 320
                for i in range(0, len(total_audio), frame_size):
                    frame = total_audio[i:i + frame_size]
                    if len(frame) == frame_size:
                        frames.append(frame)
                    elif len(frame) > 0:
                        padded_frame = frame + b"\x00" * (frame_size - len(frame))
                        frames.append(padded_frame)
            
            logger.info("Text synthesized to frames", extra={
                "text_length": len(text),
                "total_audio_bytes": len(total_audio),
                "frames_generated": len(frames),
                "frame_format": "8kHz_μ-law" if 'ulaw_audio' in locals() else "8kHz_16-bit_PCM",
                "service": "realtime_voice_engine"
            })
            
            metrics.increment_metric("tts_frames_generated_total", len(frames))
            metrics.histogram("tts_frame_count_per_synthesis", len(frames))
            
        except Exception as e:
            logger.error("Failed to synthesize text to frames", extra={
                "error": str(e),
                "text": text[:100],
                "service": "realtime_voice_engine"
            })
            metrics.increment_metric("tts_frame_synthesis_errors_total")
            raise
        
        return frames
    
    async def _analyze_audio_chunk(self, audio_chunk: bytes) -> bool:
        """Analyze audio chunk for silence detection with Phase 12 resilience"""
        try:
            # Run silence detection in thread pool with timeout
            loop = asyncio.get_event_loop()
            is_silence = await asyncio.wait_for(
                loop.run_in_executor(
                    self.thread_pool,
                    self.silence_detector.analyze_audio,
                    audio_chunk
                ),
                timeout=1.0  # 1 second timeout for audio analysis
            )
            
            # Pause inference if silence detected
            if is_silence and not self._inference_paused:
                self._inference_paused = True
                metrics.increment_metric("inference_paused_total")
                logger.info("Inference paused due to silence detection", extra={
                    "service": "realtime_voice_engine"
                })
            elif not is_silence and self._inference_paused:
                self._inference_paused = False
                metrics.increment_metric("inference_resumed_total")
                logger.info("Inference resumed", extra={
                    "service": "realtime_voice_engine"
                })
            
            return is_silence
            
        except asyncio.TimeoutError:
            logger.warning("Silence detection timeout", extra={
                "service": "realtime_voice_engine",
                "operation": "analyze_audio_chunk"
            })
            TimeoutMetrics.record_timeout("silence_detector", "analyze_audio")
            return False
        except Exception as e:
            logger.error("Silence detection analysis failed", extra={
                "error": str(e),
                "service": "realtime_voice_engine"
            })
            return False
    
    def _preprocess_text(self, text: str, voice_settings: Dict) -> str:
        """
        Preprocess text for TTS, handling SSML and other formatting.
        """
        # Check if text is already SSML
        if text.strip().startswith('<speak>') and text.strip().endswith('</speak>'):
            return text  # Already SSML
        
        # Handle SSML enablement
        if voice_settings.get('ssml', False):
            return f'<speak>{self._escape_ssml(text)}</speak>'
        
        # Basic text cleaning for TTS
        cleaned_text = self._clean_text_for_tts(text)
        return cleaned_text
    
    def _escape_ssml(self, text: str) -> str:
        """Escape special characters for SSML"""
        # Basic SSML escaping
        escaped = (text
                  .replace('&', '&amp;')
                  .replace('<', '&lt;')
                  .replace('>', '&gt;')
                  .replace('"', '&quot;')
                  .replace("'", '&apos;'))
        return escaped
    
    def _clean_text_for_tts(self, text: str) -> str:
        """Clean text for better TTS output"""
        # Remove excessive whitespace
        cleaned = ' '.join(text.split())
        
        # Handle common abbreviations (expand as needed)
        abbreviations = {
            'Dr.': 'Doctor',
            'Mr.': 'Mister',
            'Mrs.': 'Misses',
            'Ms.': 'Miss',
            'etc.': 'etcetera',
            'vs.': 'versus',
            'approx.': 'approximately',
            'dept.': 'department',
            'est.': 'established',
        }
        
        for abbr, expansion in abbreviations.items():
            cleaned = cleaned.replace(abbr, expansion)
        
        return cleaned
    
    async def process_audio(self, audio_data: bytes):
        """
        Process incoming audio data with new buffer management and Phase 12 resilience
        """
        start_time = time.time()
        
        try:
            # Run audio processing in thread pool with timeout
            loop = asyncio.get_event_loop()
            processed_data = await asyncio.wait_for(
                loop.run_in_executor(
                    self.thread_pool,
                    self._process_audio_data,
                    audio_data
                ),
                timeout=2.0  # 2 second timeout for audio processing
            )
            
            # Analyze for silence
            is_silence = await self._analyze_audio_chunk(processed_data)
            
            # Calculate duration and add to buffer
            # Assuming 16kHz 16-bit mono: bytes_per_ms = (16000 * 2) / 1000 = 32 bytes/ms
            duration_ms = len(processed_data) / 32
            await self.audio_buffer.put_packet(processed_data, duration_ms, is_silence)
            
            # Record performance metrics
            processing_latency = (time.time() - start_time) * 1000
            PerformanceCounters.record_audio_processing_latency(processing_latency)
            
        except asyncio.TimeoutError:
            logger.warning("Audio processing timeout", extra={
                "service": "realtime_voice_engine",
                "operation": "process_audio"
            })
            TimeoutMetrics.record_timeout("audio_processor", "process_audio")
            metrics.increment_metric("audio_processing_timeout_errors_total")
        except Exception as e:
            logger.error("Audio processing failed", extra={
                "error": str(e),
                "service": "realtime_voice_engine"
            })
    
    def _process_audio_data(self, audio_data: bytes) -> bytes:
        """Process audio data - ensure 16kHz 16-bit mono PCM format"""
        return _ensure_output_format(audio_data)
    
    async def detect_voice_activity(self) -> bool:
        """
        Detect voice activity using silence detector with Phase 12 resilience
        """
        try:
            return not self.silence_detector.is_silence_detected
        except Exception as e:
            logger.error("Voice activity detection failed", extra={
                "error": str(e),
                "service": "realtime_voice_engine"
            })
            return False
    
    async def has_partial_transcript(self) -> bool:
        """
        Check if there are partial transcripts available with Phase 12 resilience
        """
        # Implementation would depend on ASR integration
        # For now, return False as placeholder
        return False
    
    async def get_final_transcript(self) -> Optional[str]:
        """
        Get final speech transcript with Phase 12 resilience
        """
        # Implementation would depend on ASR integration
        # For now, return None as placeholder
        return None
    
    def is_inference_paused(self) -> bool:
        """Check if inference is currently paused due to silence"""
        return self._inference_paused
    
    async def validate_connection(self) -> bool:
        """Validate TTS provider connection with Phase 12 resilience"""
        if not self.active_provider:
            return False
        
        try:
            return await asyncio.wait_for(
                self.active_provider.validate_connection(),
                timeout=Config.EXTERNAL_API_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.warning("TTS provider connection validation timeout", extra={
                "provider": self.provider_name,
                "service": "realtime_voice_engine"
            })
            TimeoutMetrics.record_timeout("tts_provider", "validate_connection")
            return False
        except Exception as e:
            logger.error("TTS provider connection validation failed", extra={
                "error": str(e),
                "service": "realtime_voice_engine"
            })
            return False
    
    async def get_status(self) -> Dict[str, Any]:
        """Get current engine status with Phase 12 resilience information"""
        buffer_stats = self.audio_buffer.get_stats()
        
        return {
            "provider": self.provider_name,
            "connected": self.is_connected,
            "fallback_available": Config.TTS_FALLBACK_ENABLED,
            "stream_start_time": self.stream_start_time,
            "buffer_stats": buffer_stats,
            "inference_paused": self._inference_paused,
            "silence_detected": self.silence_detector.is_silence_detected,
            "circuit_breaker_open": await _is_circuit_breaker_open("voice_engine")
        }
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        buffer_stats = self.audio_buffer.get_stats()
        
        return {
            "audio_buffer": buffer_stats,
            "silence_detection": {
                "is_silence_detected": self.silence_detector.is_silence_detected,
                "consecutive_silence_frames": self.silence_detector.consecutive_silence_frames
            },
            "inference_paused": self._inference_paused
        }

# --------------------------------------------------------------------------
# Utility Functions with Phase 12 Resilience
# --------------------------------------------------------------------------

async def create_voice_engine(provider: Optional[str] = None) -> RealtimeVoiceEngine:
    """Create and initialize a voice engine instance with Phase 12 resilience"""
    engine = RealtimeVoiceEngine(provider)
    await engine.start()
    return engine

# Global engine instance for shared use
_global_engine: Optional[RealtimeVoiceEngine] = None

async def get_global_voice_engine() -> RealtimeVoiceEngine:
    """Get or create global voice engine instance with Phase 12 resilience"""
    global _global_engine
    if _global_engine is None:
        _global_engine = RealtimeVoiceEngine()
        await _global_engine.start()
    return _global_engine