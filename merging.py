import os
import re
import asyncio
import tempfile
import subprocess
import json
import time
import math
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Union, Any
from dataclasses import dataclass, field
from enum import Enum
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================
# CONSTANTS AND CONFIGURATION
# ============================

class MergeState(str, Enum):
    """States for merging process"""
    WAITING_SOURCE = "waiting_for_source"
    WAITING_TARGET = "waiting_for_target"
    PROCESSING = "processing"
    CANCELLED = "cancelled"
    COMPLETED = "completed"

class ProcessStage(str, Enum):
    """Processing stages"""
    DOWNLOADING = "Downloading"
    EXTRACTING = "Extracting"
    ANALYZING = "Analyzing"
    REENCODING = "Re-encoding"
    MERGING = "Merging"
    UPLOADING = "Uploading"
    CLEANING = "Cleaning"

# Global configuration
class Config:
    """Configuration constants"""
    # Throttling
    EDIT_INTERVAL = 1.2  # Minimum seconds between updates
    MAX_CONSECUTIVE_ERRORS = 3
    
    # File processing
    MAX_AUDIO_SIZE_MB = 30
    TARGET_SIZE_LIMIT_MB = 3000
    MAX_FILE_SIZE_MB = 2000  # 2GB limit
    MIN_FILE_SIZE_KB = 10    # 10KB minimum
    
    # Bitrate limits
    MIN_BITRATE = 64000      # 64 kbps
    MAX_BITRATE = 320000     # 320 kbps
    DEFAULT_BITRATE = 128000 # 128 kbps
    
    # FFmpeg settings
    DEFAULT_AUDIO_CODEC = "aac"
    DEFAULT_SAMPLE_RATE = 48000
    DEFAULT_CHANNELS = 2
    
    # Progress bar
    PROGRESS_BAR_LENGTH = 16

# ============================
# DATA CLASSES
# ============================

@dataclass
class MediaStream:
    """Represents a media stream (audio/subtitle)"""
    index: int
    codec_type: str
    codec: str
    language: str = "und"
    title: str = ""
    bitrate: int = 0
    channels: int = 2
    sample_rate: int = 48000
    duration: float = 0
    start_time: float = 0
    start_pts: int = 0

@dataclass
class MediaFile:
    """Represents a media file"""
    message: Message
    file_id: str
    filename: str
    file_size: int
    mime_type: str
    episode_info: Dict[str, int] = field(default_factory=dict)

@dataclass
class ProcessingMetrics:
    """Tracks processing metrics"""
    start_time: float = field(default_factory=time.time)
    bytes_processed: int = 0
    files_processed: int = 0
    errors: int = 0
    cleanup_count: int = 0

# ============================
# ERROR HANDLING
# ============================

class MergeError(Exception):
    """Base exception for merge operations"""
    pass

class FFmpegError(MergeError):
    """FFmpeg-related errors"""
    pass

class FileError(MergeError):
    """File-related errors"""
    pass

class ConfigurationError(MergeError):
    """Configuration errors"""
    pass

# ============================
# GLOBAL STATE MANAGEMENT
# ============================

class StateManager:
    """Manages global processing states with thread safety"""
    
    def __init__(self):
        self._merging_users = {}
        self._processing_states = {}
        self._last_edit_time = {}
        self._lock = asyncio.Lock()
    
    async def get_user_state(self, user_id: int) -> Optional[Dict]:
        """Get user's merge state"""
        async with self._lock:
            return self._merging_users.get(user_id)
    
    async def set_user_state(self, user_id: int, state: Dict):
        """Set user's merge state"""
        async with self._lock:
            self._merging_users[user_id] = state
    
    async def remove_user_state(self, user_id: int):
        """Remove user's merge state"""
        async with self._lock:
            self._merging_users.pop(user_id, None)
    
    async def set_cancelled(self, user_id: int, cancelled: bool = True):
        """Set cancellation state for user"""
        async with self._lock:
            if user_id in self._processing_states:
                self._processing_states[user_id]["cancelled"] = cancelled
    
    async def is_cancelled(self, user_id: int) -> bool:
        """Check if user cancelled processing"""
        async with self._lock:
            if user_id in self._processing_states:
                return self._processing_states[user_id].get("cancelled", False)
            return False
    
    async def update_edit_time(self, user_id: int, timestamp: float):
        """Update last edit time with throttling"""
        async with self._lock:
            self._last_edit_time[user_id] = timestamp
    
    async def should_update(self, user_id: int, current_time: float) -> bool:
        """Check if we should update based on throttling"""
        async with self._lock:
            last_time = self._last_edit_time.get(user_id, 0)
            return (current_time - last_time) >= Config.EDIT_INTERVAL
    
    async def cleanup_user(self, user_id: int):
        """Clean up all user states"""
        async with self._lock:
            self._merging_users.pop(user_id, None)
            self._processing_states.pop(user_id, None)
            self._last_edit_time.pop(user_id, None)

# Initialize global state manager
state_manager = StateManager()

# ============================
# HELPER FUNCTIONS
# ============================

def sanitize_filename(filename: str) -> str:
    """Sanitize filename to remove unsafe characters"""
    # Remove directory traversal attempts
    filename = os.path.basename(filename)
    # Remove unsafe characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Limit length
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        filename = name[:200] + ext
    return filename

def format_size(bytes: int) -> str:
    """Format bytes to human readable size"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} TB"

def format_duration(seconds: float) -> str:
    """Format seconds to human readable duration"""
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def get_file_extension(filename: str) -> str:
    """Get lowercase file extension"""
    return Path(filename).suffix.lower()

def validate_file_size(file_size: int) -> bool:
    """Validate file size is within acceptable limits"""
    size_mb = file_size / (1024 * 1024)
    if size_mb > Config.MAX_FILE_SIZE_MB:
        logger.warning(f"File too large: {size_mb:.1f}MB > {Config.MAX_FILE_SIZE_MB}MB")
        return False
    if file_size < Config.MIN_FILE_SIZE_KB * 1024:
        logger.warning(f"File too small: {file_size} bytes")
        return False
    return True

# ============================
# PROGRESS SYSTEM
# ============================

class ProgressTracker:
    """Manages progress tracking with throttling"""
    
    @staticmethod
    def make_bar(percent: float, length: int = Config.PROGRESS_BAR_LENGTH) -> str:
        """Create a progress bar visualization"""
        filled = min(length, int(length * percent / 100))
        return "█" * filled + "░" * (length - filled)
    
    @staticmethod
    def format_eta(seconds: float) -> str:
        """Format seconds into human-readable ETA"""
        if seconds <= 0:
            return "0s"
        
        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 0:
            return f"{hours}h {minutes:02d}m"
        elif minutes > 0:
            return f"{minutes}m {seconds:02d}s"
        else:
            return f"{seconds}s"
    
    @classmethod
    async def update(
        cls,
        message: Message,
        user_id: int,
        stage: ProcessStage,
        filename: str,
        current: int,
        total: int,
        start_time: float,
        cancelled_callback_data: str = None
    ) -> bool:
        """
        Update progress message with throttling
        
        Returns: True if updated, False if throttled
        """
        now = time.time()
        
        # Check if we should update (throttling)
        if not await state_manager.should_update(user_id, now):
            return False
        
        # Calculate metrics
        elapsed = now - start_time
        percent = (current / total * 100) if total > 0 else 0
        speed = current / elapsed if elapsed > 0 else 0
        eta = (total - current) / speed if speed > 0 else 0
        
        # Build progress text
        progress_bar = cls.make_bar(percent)
        
        text = f"""
<b>{stage.value}</b>

📁 <code>{filename}</code>

{progress_bar} {percent:.1f}%

├ Size: {format_size(current)} / {format_size(total)}
├ Speed: {format_size(speed)}/s
├ Elapsed: {format_duration(elapsed)}
└ ETA: {cls.format_eta(eta)}
"""
        
        # Add cancel button if provided
        reply_markup = None
        if cancelled_callback_data:
            reply_markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data=cancelled_callback_data)
            ]])
        
        try:
            await message.edit_text(text, reply_markup=reply_markup)
            await state_manager.update_edit_time(user_id, now)
            return True
        except Exception as e:
            logger.warning(f"Failed to update progress: {e}")
            return False

# ============================
# MEDIA PARSING & MATCHING
# ============================

class EpisodeParser:
    """Intelligent episode information parser"""
    
    # Pre-compiled regex patterns for performance
    PATTERNS = [
        # S01E01, S1E1, S01-E01
        re.compile(r's\s*(\d{1,2})\s*e\s*(\d{1,3})', re.IGNORECASE),
        # S01 01, S1 - 01
        re.compile(r's\s*(\d{1,2})\s*[-_]?\s*(\d{1,3})', re.IGNORECASE),
        # Season 1 Episode 01
        re.compile(r'season\s*(\d{1,2})\s*(?:episode|ep)\s*(\d{1,3})', re.IGNORECASE),
        # 1x01, 01x01
        re.compile(r'(\d{1,2})\s*x\s*(\d{1,3})', re.IGNORECASE),
        # Episode 01, EP01, E01
        re.compile(r'(?:episode|ep|e)\s*(\d{1,3})', re.IGNORECASE),
        # Part 1, Pt.1
        re.compile(r'(?:part|pt)\.?\s*(\d{1,3})', re.IGNORECASE),
    ]
    
    @classmethod
    def parse(cls, filename: str) -> Dict[str, int]:
        """
        Parse season and episode numbers from filename
        
        Returns: Dict with 'season' and 'episode' keys
        """
        # Normalize filename
        normalized = re.sub(r'[._]', ' ', filename.lower())
        normalized = re.sub(r'\s+', ' ', normalized)
        
        season, episode = 1, 0
        
        # Try each pattern
        for pattern in cls.PATTERNS:
            match = pattern.search(normalized)
            if match:
                groups = match.groups()
                if len(groups) == 2:
                    season = int(groups[0])
                    episode = int(groups[1])
                else:
                    episode = int(groups[0])
                break
        
        # Fallback: look for standalone episode number
        if episode == 0:
            standalone = re.search(r'\b(\d{2,3})\b', normalized)
            if standalone:
                episode = int(standalone.group(1))
        
        # Special case: if episode looks like a year, reset it
        if episode > 1900 and episode < 2100:
            episode = 0
        
        return {"season": season, "episode": episode}

class FileMatcher:
    """Matches source and target files by episode information"""
    
    @staticmethod
    def match_files(source_files: List[MediaFile], target_files: List[MediaFile]) -> List[Tuple[Optional[MediaFile], MediaFile]]:
        """
        Match source and target files by episode
        
        Returns: List of (source, target) tuples
        """
        matched_pairs = []
        
        for target in target_files:
            target_info = target.episode_info
            
            # Skip if episode not detected
            if target_info.get("episode", 0) == 0:
                logger.warning(f"Episode not detected in target: {target.filename}")
                matched_pairs.append((None, target))
                continue
            
            # Find matching source
            matched_source = None
            for source in source_files:
                source_info = source.episode_info
                
                if (source_info.get("season") == target_info.get("season") and
                    source_info.get("episode") == target_info.get("episode")):
                    matched_source = source
                    break
            
            matched_pairs.append((matched_source, target))
        
        return matched_pairs

# ============================
# FFMPEG INTEGRATION
# ============================

class FFmpegManager:
    """Manages FFmpeg operations with error handling"""
    
    @staticmethod
    async def execute_command(cmd: List[str], description: str = "") -> Tuple[bool, str, str]:
        """Execute FFmpeg command with timeout"""
        try:
            logger.info(f"Executing: {' '.join(cmd[:6])}...")
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=300  # 5 minute timeout
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                return False, "", f"Timeout executing {description}"
            
            if process.returncode != 0:
                error_msg = stderr.decode('utf-8', errors='ignore')[:500]
                logger.error(f"FFmpeg error ({description}): {error_msg}")
                return False, stdout.decode('utf-8', errors='ignore'), error_msg
            
            return True, stdout.decode('utf-8', errors='ignore'), ""
            
        except Exception as e:
            logger.error(f"Error executing FFmpeg command: {e}")
            return False, "", str(e)
    
    @staticmethod
    def get_media_info(file_path: str) -> Dict:
        """Get detailed media information using ffprobe"""
        cmd = [
            'ffprobe',
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_format',
            '-show_streams',
            file_path
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                return json.loads(result.stdout)
        except subprocess.TimeoutError:
            logger.error(f"Timeout getting media info for {file_path}")
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON from ffprobe for {file_path}")
        except Exception as e:
            logger.error(f"Error getting media info: {e}")
        
        return {"streams": [], "format": {}}
    
    @staticmethod
    def extract_streams(media_info: Dict) -> Tuple[List[MediaStream], List[MediaStream]]:
        """Extract audio and subtitle streams from media info"""
        audio_streams = []
        subtitle_streams = []
        
        for stream in media_info.get("streams", []):
            codec_type = stream.get("codec_type", "")
            
            if codec_type == "audio":
                # Parse timing information
                start_time = stream.get("start_time", "0")
                start_pts = stream.get("start_pts", "0")
                
                try:
                    start_time = float(start_time)
                except (ValueError, TypeError):
                    start_time = 0.0
                
                try:
                    start_pts = int(start_pts)
                except (ValueError, TypeError):
                    start_pts = 0
                
                audio_stream = MediaStream(
                    index=stream.get("index", 0),
                    codec_type="audio",
                    codec=stream.get("codec_name", "unknown"),
                    language=stream.get("tags", {}).get("language", "und"),
                    title=stream.get("tags", {}).get("title", ""),
                    bitrate=int(stream.get("bit_rate", 0)) if stream.get("bit_rate") else 0,
                    channels=stream.get("channels", 2),
                    sample_rate=int(stream.get("sample_rate", 48000)),
                    duration=float(stream.get("duration", 0)),
                    start_time=start_time,
                    start_pts=start_pts
                )
                audio_streams.append(audio_stream)
                
            elif codec_type == "subtitle":
                subtitle_stream = MediaStream(
                    index=stream.get("index", 0),
                    codec_type="subtitle",
                    codec=stream.get("codec_name", "unknown"),
                    language=stream.get("tags", {}).get("language", "und"),
                    title=stream.get("tags", {}).get("title", "")
                )
                subtitle_streams.append(subtitle_stream)
        
        return audio_streams, subtitle_streams

# ============================
# MERGE ENGINE
# ============================

class MergeEngine:
    """Main merging engine with optimized workflow"""
    
    def __init__(self):
        self.ffmpeg = FFmpegManager()
    
    async def extract_tracks(
        self,
        source_path: str,
        temp_dir: Path,
        progress_callback=None
    ) -> Dict[str, Any]:
        """Extract audio and subtitle tracks from source file"""
        extracted = {
            "audio_files": [],
            "subtitle_files": [],
            "success": False,
            "error": None
        }
        
        try:
            # Get source info
            media_info = self.ffmpeg.get_media_info(source_path)
            audio_streams, subtitle_streams = self.ffmpeg.extract_streams(media_info)
            
            logger.info(f"Found {len(audio_streams)} audio and {len(subtitle_streams)} subtitle streams")
            
            # Extract audio tracks
            for idx, audio in enumerate(audio_streams):
                audio_output = temp_dir / f"audio_{idx}_{audio.language}.m4a"
                
                cmd = [
                    "ffmpeg", "-y",
                    "-i", source_path,
                    "-map", f"0:a:{idx}",
                    "-c:a", "copy",
                    "-bsf:a", "aac_adtstoasc",
                    str(audio_output)
                ]
                
                success, _, error = await self.ffmpeg.execute_command(cmd, "extract audio")
                if success:
                    extracted["audio_files"].append({
                        "path": str(audio_output),
                        "stream": audio,
                        "original_size": audio_output.stat().st_size if audio_output.exists() else 0
                    })
                    logger.info(f"Extracted audio {idx+1} ({audio.language})")
                else:
                    logger.warning(f"Failed to extract audio {idx+1}: {error}")
            
            # Extract subtitle tracks
            for idx, subtitle in enumerate(subtitle_streams):
                sub_output = temp_dir / f"subtitle_{idx}_{subtitle.language}.srt"
                
                cmd = [
                    "ffmpeg", "-y",
                    "-i", source_path,
                    "-map", f"0:s:{idx}",
                    str(sub_output)
                ]
                
                success, _, error = await self.ffmpeg.execute_command(cmd, "extract subtitle")
                if success:
                    extracted["subtitle_files"].append({
                        "path": str(sub_output),
                        "stream": subtitle
                    })
                    logger.info(f"Extracted subtitle {idx+1} ({subtitle.language})")
                else:
                    logger.warning(f"Failed to extract subtitle {idx+1}: {error}")
            
            # Check if we got any tracks
            if extracted["audio_files"] or extracted["subtitle_files"]:
                extracted["success"] = True
            else:
                extracted["error"] = "No tracks extracted"
                
        except Exception as e:
            logger.error(f"Error extracting tracks: {e}")
            extracted["error"] = str(e)
        
        return extracted
    
    def calculate_bitrate(self, audio_path: str, target_size_mb: int = Config.MAX_AUDIO_SIZE_MB) -> int:
        """Calculate appropriate bitrate for target size"""
        try:
            media_info = self.ffmpeg.get_media_info(audio_path)
            duration = float(media_info.get("format", {}).get("duration", 0))
            
            if duration <= 0:
                return Config.DEFAULT_BITRATE
            
            # Calculate required bitrate
            target_bits = target_size_mb * 8 * 1024 * 1024
            bitrate = int(target_bits / duration)
            
            # Apply limits
            bitrate = max(Config.MIN_BITRATE, min(bitrate, Config.MAX_BITRATE))
            logger.info(f"Calculated bitrate: {bitrate} bps for {duration}s audio")
            
            return bitrate
            
        except Exception as e:
            logger.error(f"Error calculating bitrate: {e}")
            return Config.DEFAULT_BITRATE
    
    async def reencode_audio(
        self,
        audio_path: str,
        target_specs: Dict,
        output_path: str,
        audio_delay: float = 0
    ) -> bool:
        """Re-encode audio to match target specifications"""
        try:
            # Check file size
            if not os.path.exists(audio_path):
                logger.error(f"Audio file not found: {audio_path}")
                return False
            
            file_size_mb = os.path.getsize(audio_path) / (1024 * 1024)
            logger.info(f"Audio file size: {file_size_mb:.2f}MB, delay: {audio_delay:.3f}s")
            
            # Determine bitrate
            if file_size_mb > Config.MAX_AUDIO_SIZE_MB:
                bitrate = self.calculate_bitrate(audio_path, Config.MAX_AUDIO_SIZE_MB)
                logger.info(f"Compressing audio: {file_size_mb:.2f}MB -> target {Config.MAX_AUDIO_SIZE_MB}MB, bitrate {bitrate}")
            else:
                bitrate = target_specs.get("audio_bitrate", Config.DEFAULT_BITRATE)
            
            # Build command with timing adjustment
            cmd = ["ffmpeg", "-y", "-i", audio_path]
            
            # Apply audio delay if needed
            if abs(audio_delay) > 0.001:  # Only if delay > 1ms
                if audio_delay > 0:
                    # Audio starts later, add silence at beginning
                    cmd.extend(["-af", f"adelay={int(audio_delay*1000)}|{int(audio_delay*1000)}"])
                else:
                    # Audio starts earlier, cut beginning
                    cmd.extend(["-ss", str(abs(audio_delay))])
            
            # Add encoding parameters
            cmd.extend([
                "-c:a", target_specs.get("audio_codec", Config.DEFAULT_AUDIO_CODEC),
                "-b:a", str(bitrate),
                "-ar", str(target_specs.get("audio_sample_rate", Config.DEFAULT_SAMPLE_RATE)),
                "-ac", str(target_specs.get("audio_channels", Config.DEFAULT_CHANNELS)),
                "-vn", "-sn",  # No video, no subtitles
                output_path
            ])
            
            success, _, error = await self.ffmpeg.execute_command(cmd, "re-encode audio")
            
            if success:
                new_size_mb = os.path.getsize(output_path) / (1024 * 1024)
                logger.info(f"Re-encoded audio: {new_size_mb:.2f}MB (compression: {(1 - new_size_mb/file_size_mb)*100:.1f}%)")
                return True
            else:
                logger.error(f"Re-encoding failed: {error}")
                return False
                
        except Exception as e:
            logger.error(f"Error re-encoding audio: {e}")
            return False
    
    async def merge_tracks(
        self,
        target_path: str,
        reencoded_tracks: Dict,
        output_path: str
    ) -> bool:
        """Merge re-encoded tracks into target file"""
        try:
            # Get target info
            target_info = self.ffmpeg.get_media_info(target_path)
            target_audio, target_subtitles = self.ffmpeg.extract_streams(target_info)
            
            # Build ffmpeg command
            inputs = [target_path]
            maps = ["-map", "0:v"]  # Target video
            
            # Map target audio streams
            for i in range(len(target_audio)):
                maps.extend(["-map", f"0:a:{i}"])
            
            # Add re-encoded audio tracks
            audio_idx = 1  # Start counting from first input
            for audio_track in reencoded_tracks.get("audio_files", []):
                if os.path.exists(audio_track["path"]):
                    inputs.append(audio_track["path"])
                    maps.extend(["-map", f"{audio_idx}:a:0"])
                    audio_idx += 1
            
            # Map target subtitle streams
            for i in range(len(target_subtitles)):
                maps.extend(["-map", f"0:s:{i}"])
            
            # Add extracted subtitle tracks
            sub_idx = audio_idx
            for sub_track in reencoded_tracks.get("subtitle_files", []):
                if os.path.exists(sub_track["path"]):
                    inputs.append(sub_track["path"])
                    maps.extend(["-map", f"{sub_idx}:s:0"])
                    sub_idx += 1
            
            # Build final command
            cmd = ["ffmpeg", "-y"]
            
            # Add inputs
            for input_file in inputs:
                cmd.extend(["-i", input_file])
            
            # Add maps
            cmd.extend(maps)
            
            # Copy all streams
            cmd.extend(["-c:v", "copy", "-c:a", "copy", "-c:s", "copy"])
            
            # Set stream dispositions
            total_target_audio = len(target_audio)
            if total_target_audio > 0:
                cmd.extend(["-disposition:a:0", "0"])  # Target audio not default
            
            if len(reencoded_tracks.get("audio_files", [])) > 0:
                # Set first source audio as default
                cmd.extend([f"-disposition:a:{total_target_audio}", "default"])
            
            # Container optimizations
            cmd.extend([
                "-movflags", "+faststart",
                "-max_interleave_delta", "0",
                output_path
            ])
            
            success, _, error = await self.ffmpeg.execute_command(cmd, "merge tracks")
            return success
            
        except Exception as e:
            logger.error(f"Error merging tracks: {e}")
            return False
    
    async def optimized_merge(
        self,
        source_path: str,
        target_path: str,
        output_path: str,
        progress_callback=None
    ) -> bool:
        """
        Complete optimized merge workflow
        
        1. Extract tracks from source
        2. Delete source to save space
        3. Analyze target specs
        4. Re-encode extracted tracks
        5. Merge into target
        6. Cleanup temporary files
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            try:
                logger.info(f"Starting optimized merge")
                logger.info(f"Source: {os.path.basename(source_path)}")
                logger.info(f"Target: {os.path.basename(target_path)}")
                
                # Step 1: Extract tracks
                logger.info("Step 1: Extracting tracks from source")
                extracted = await self.extract_tracks(source_path, temp_path)
                
                if not extracted["success"]:
                    logger.error(f"Failed to extract tracks: {extracted.get('error')}")
                    return False
                
                # Step 2: Delete source file
                logger.info("Step 2: Deleting source file")
                if os.path.exists(source_path):
                    os.remove(source_path)
                    logger.info("Source file deleted")
                
                # Step 3: Analyze target
                logger.info("Step 3: Analyzing target specifications")
                target_info = self.ffmpeg.get_media_info(target_path)
                target_format = target_info.get("format", {})
                
                target_specs = {
                    "format": target_format.get("format_name", "matroska"),
                    "duration": float(target_format.get("duration", 0)),
                    "size": int(target_format.get("size", 0))
                }
                
                # Find audio stream in target
                target_audio, _ = self.ffmpeg.extract_streams(target_info)
                if target_audio:
                    target_specs.update({
                        "audio_codec": target_audio[0].codec,
                        "audio_bitrate": target_audio[0].bitrate or Config.DEFAULT_BITRATE,
                        "audio_channels": target_audio[0].channels,
                        "audio_sample_rate": target_audio[0].sample_rate
                    })
                
                # Step 4: Re-encode extracted tracks
                logger.info("Step 4: Re-encoding extracted tracks")
                reencoded_tracks = {
                    "audio_files": [],
                    "subtitle_files": []
                }
                
                # Re-encode audio tracks
                for audio_track in extracted["audio_files"]:
                    reencoded_path = temp_path / f"reencoded_{Path(audio_track['path']).name}"
                    
                    success = await self.reencode_audio(
                        audio_track["path"],
                        target_specs,
                        str(reencoded_path),
                        audio_track["stream"].start_time
                    )
                    
                    if success:
                        reencoded_tracks["audio_files"].append({
                            "path": str(reencoded_path),
                            "language": audio_track["stream"].language
                        })
                        # Delete original extracted audio
                        if os.path.exists(audio_track["path"]):
                            os.remove(audio_track["path"])
                    else:
                        logger.warning(f"Failed to re-encode audio track")
                
                # Copy subtitle tracks
                for sub_track in extracted["subtitle_files"]:
                    reencoded_tracks["subtitle_files"].append(sub_track)
                
                # Step 5: Merge tracks
                logger.info("Step 5: Merging tracks into target")
                success = await self.merge_tracks(target_path, reencoded_tracks, output_path)
                
                # Step 6: Cleanup
                logger.info("Step 6: Cleaning up temporary files")
                cleanup_count = 0
                
                # Delete temporary audio files
                for audio_track in reencoded_tracks["audio_files"]:
                    if os.path.exists(audio_track["path"]):
                        os.remove(audio_track["path"])
                        cleanup_count += 1
                
                # Delete temporary subtitle files
                for sub_track in reencoded_tracks["subtitle_files"]:
                    if os.path.exists(sub_track["path"]):
                        os.remove(sub_track["path"])
                        cleanup_count += 1
                
                logger.info(f"Cleaned up {cleanup_count} temporary files")
                return success
                
            except Exception as e:
                logger.error(f"Error in optimized merge: {e}")
                return False

# ============================
# MERGING STATE CLASS
# ============================

class MergingState:
    """Tracks user's merging state"""
    
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.state = MergeState.WAITING_SOURCE
        self.source_files = []  # List of MediaFile objects
        self.target_files = []  # List of MediaFile objects
        self.progress_msg = None
        self.current_file = 0
        self.total_files = 0
        self.metrics = ProcessingMetrics()
    
    def add_source_file(self, media_file: MediaFile):
        """Add a source file with episode info"""
        media_file.episode_info = EpisodeParser.parse(media_file.filename)
        self.source_files.append(media_file)
    
    def add_target_file(self, media_file: MediaFile):
        """Add a target file with episode info"""
        media_file.episode_info = EpisodeParser.parse(media_file.filename)
        self.target_files.append(media_file)
    
    def reset(self):
        """Reset the state"""
        self.state = MergeState.WAITING_SOURCE
        self.source_files = []
        self.target_files = []
        self.progress_msg = None
        self.current_file = 0
        self.total_files = 0
        self.metrics = ProcessingMetrics()

# ============================
# HELPER TEXT
# ============================

def get_merging_help_text() -> str:
    """Get help text for merging commands"""
    return """
<b>🔧 Auto File Merging Commands</b>

<code>/merging</code> - Start auto file merging process
<code>/done</code> - Proceed to next step after sending files
<code>/cancel_merge</code> - Cancel current merging process

<b>📝 How to use:</b>
1. Send <code>/merging</code>
2. Send all SOURCE files (with desired audio/subtitle tracks)
3. Send <code>/done</code>
4. Send all TARGET files (to add tracks to)
5. Send <code>/done</code> again
6. Wait for processing to complete

<b>🎯 Optimized Workflow</b>
• Extract tracks first, delete source to save space
• Smart compression (30MB limit)
• Target analysis before re-encoding
• Target original audio preserved
• Automatic cleanup

<b>⚠️ Important Notes:</b>
• Files are matched by season and episode numbers
• <b>MKV format works best</b>
• Original target file tracks are preserved
• Only new audio/subtitle tracks are added from source
• Server needs FFmpeg installed
"""

# ============================
# CLEANUP FUNCTIONS
# ============================

def silent_cleanup(*file_paths: str) -> int:
    """
    Silently delete files without raising errors
    
    Returns: Number of successfully deleted files
    """
    deleted = 0
    for file_path in file_paths:
        if file_path and isinstance(file_path, str) and os.path.exists(file_path):
            try:
                os.remove(file_path)
                deleted += 1
                logger.debug(f"Cleaned up: {os.path.basename(file_path)}")
            except Exception as e:
                logger.warning(f"Could not delete {file_path}: {e}")
    return deleted

def cleanup_all_temp_files(temp_dir: str) -> int:
    """Clean up all files in a temporary directory"""
    deleted = 0
    if temp_dir and os.path.exists(temp_dir):
        for file in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, file)
            if os.path.isfile(file_path):
                try:
                    os.remove(file_path)
                    deleted += 1
                except:
                    pass
    return deleted

# Initialize merge engine
merge_engine = MergeEngine()

# Export important components
__all__ = [
    'MergingState',
    'MergeState',
    'ProcessStage',
    'Config',
    'state_manager',
    'ProgressTracker',
    'EpisodeParser',
    'FileMatcher',
    'merge_engine',
    'get_merging_help_text',
    'silent_cleanup',
    'validate_file_size',
    'sanitize_filename',
    'format_size',
    'format_duration'
]
     
