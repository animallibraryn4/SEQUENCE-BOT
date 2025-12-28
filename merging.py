import os
import re
import asyncio
import tempfile
import subprocess
import json
import time
import math
from pathlib import Path
from typing import List, Dict, Tuple
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from config import OWNER_ID
from start import is_subscribed

# Merging state management
merging_users = {}  # Store user's merging state

# Global processing state to track cancellations
PROCESSING_STATES = {}

# Throttling system for multiple users
LAST_EDIT_TIME = {}
EDIT_INTERVAL = 1.2  # Minimum 1.2 seconds between updates

class MergingState:
    """Track user's merging state"""
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.source_files = []  # List of source file messages
        self.target_files = []  # List of target file messages
        self.state = "waiting_for_source"  # waiting_for_source, waiting_for_target, processing
        self.current_processing = 0
        self.total_files = 0

# --- HELP TEXT UPDATE ---
def get_merging_help_text() -> str:
    """Get help text for merging commands"""
    return """
<blockquote><b>🔧 Auto File Merging Commands</b></blockquote>

<blockquote><b>/merging</b> - Start auto file merging process
<b>/done</b> - Proceed to next step after sending files
<b>/cancel_merge</b> - Cancel current merging process</blockquote>

<blockquote><b>📝 How to use:</b>
1. Send <code>/merging</code>
2. Send all SOURCE files (with desired audio/subtitle tracks)
3. Send <code>/done</code>
4. Send all TARGET files (to add tracks to)
5. Send <code>/done</code> again
6. Wait for processing to complete</blockquote>

<blockquote><b>🎯 NEW: MKVToolNix Engine</b>
- ✅ Zero quality loss (no video re-encode)
- ✅ Source audio set as DEFAULT
- ✅ All subtitle tracks preserved
- ✅ MX Player, VLC, Android TV compatible
- ✅ Fast processing</blockquote>

<blockquote><b>⚠️ Important Notes:</b>
- Files are matched by season and episode numbers
- <b>MKV format works best</b> (for MKVToolNix engine)
- Original target file tracks are preserved
- Only new audio/subtitle tracks are added from source
- Server needs MKVToolNix installed</blockquote>"""
    
# --- PROGRESS BAR SYSTEM (MULTI-USER SAFE) ---
def make_bar(percent, length=16):
    """Create a progress bar visualization"""
    filled = int(length * percent / 100)
    return "■" * filled + "□" * (length - filled)

def format_eta(seconds):
    """Format seconds into human-readable ETA"""
    if seconds <= 0:
        return "0s"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m {s}s" if m else f"{s}s"

async def smart_progress_callback(current, total, msg, start_time, stage, filename, user_id, msg_id=None):
    """
    Throttled progress callback for multiple users
    
    Parameters:
    - current: Current bytes downloaded/uploaded
    - total: Total file size
    - msg: Message object to edit
    - start_time: When operation started
    - stage: What stage (Downloading/Uploading)
    - filename: File being processed
    - user_id: User ID for throttling
    - msg_id: Optional message ID for cancel callback
    """
    # Check if processing was cancelled
    if user_id in PROCESSING_STATES and PROCESSING_STATES[user_id].get("cancelled"):
        raise asyncio.CancelledError("Processing cancelled by user")
    
    now = time.time()
    
    # Check if we should update (throttle)
    if user_id in LAST_EDIT_TIME:
        time_since_last = now - LAST_EDIT_TIME[user_id]
        if time_since_last < EDIT_INTERVAL:
            return  # Skip this update, too soon!
    
    diff = now - start_time
    
    if diff == 0 or total == 0:
        return
    
    speed = current / diff
    percent = current * 100 / total
    eta = (total - current) / speed if speed > 0 else 0
    
    # Build message text
    text = (
        f"<blockquote><b>{stage}</b></blockquote>\n\n"
        f"<blockquote>📁 {filename}</blockquote>\n\n"
        f"<blockquote>{make_bar(percent)}</blockquote>\n"
        f"<blockquote>"
        f"» Size  : {current/1024/1024:.1f} MB / {total/1024/1024:.1f} MB\n"
        f"» Done  : {percent:.2f}%\n"
        f"» Speed : {speed/1024/1024:.2f} MB/s\n"
        f"» ETA   : {format_eta(eta)}"
        f"</blockquote>"
    )
    
    # Add cancel button if we have user_id
    reply_markup = None
    if user_id:
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
        ])
    
    try:
        await msg.edit_text(text, reply_markup=reply_markup)
        LAST_EDIT_TIME[user_id] = now  # Update last edit time
    except Exception as e:
        # If message was deleted or other error, skip updating last edit time
        pass

# Simple version for single-user scenarios (backward compatibility)
async def progress_callback(current, total, msg, start_time, stage, filename):
    """Simple progress callback (no throttling, for backward compatibility)"""
    return await smart_progress_callback(current, total, msg, start_time, stage, filename, 0, None)

# Cleanup function to remove user from throttling system
def cleanup_user_throttling(user_id):
    """Remove user from throttling system when done"""
    if user_id in LAST_EDIT_TIME:
        del LAST_EDIT_TIME[user_id]

# --- PARSING ENGINE FOR EPISODE MATCHING ---
def parse_episode_info(filename: str) -> Dict:
    """
    Smart season/episode parser
    Supports: S1-01, S01E01, 1x01, EP01, Episode 01, etc.
    """
    name = filename.lower()

    # normalize separators
    name = re.sub(r'[._]', ' ', name)
    name = re.sub(r'\s+', ' ', name)

    season = None
    episode = None

    patterns = [

        # S01E01, S1 E1, S01-E01
        r's\s*(\d{1,2})\s*e\s*(\d{1,3})',

        # S1 - 01, S01 01, S2_12
        r's\s*(\d{1,2})\s*[- ]\s*(\d{1,3})',

        # Season 1 Episode 01
        r'season\s*(\d{1,2})\s*(?:episode|ep)?\s*(\d{1,3})',

        # 1x01
        r'(\d{1,2})\s*x\s*(\d{1,3})',

        # Episode 01, EP01, E01
        r'(?:episode|ep|e)\s*(\d{1,3})',
    ]

    for p in patterns:
        m = re.search(p, name)
        if m:
            if len(m.groups()) == 2:
                season = int(m.group(1))
                episode = int(m.group(2))
            else:
                episode = int(m.group(1))
            break

    # fallback: standalone episode number (LAST option)
    if episode is None:
        m = re.search(r'\b(\d{1,3})\b', name)
        if m:
            episode = int(m.group(1))

    # default season
    if season is None:
        season = 1

    return {
        "season": season,
        "episode": episode if episode is not None else 0
    }

def match_files_by_episode(source_files: List[Dict], target_files: List[Dict]) -> List[Tuple[Dict, Dict]]:
    """Match source and target files by season and episode"""
    matched_pairs = []
    
    for target in target_files:
        target_info = parse_episode_info(target.get("filename", ""))

        # 🚫 IMPORTANT FIX:
        # Agar episode detect nahi hua, to skip karo
        if target_info["episode"] == 0:
            print(f"[SKIP] Episode not detected in target: {target.get('filename')}")
            continue

        # Find matching source file
        found = False
        for source in source_files:
            source_info = parse_episode_info(source.get("filename", ""))

            if (
                source_info["season"] == target_info["season"] and
                source_info["episode"] == target_info["episode"]
            ):
                matched_pairs.append((source, target))
                found = True
                break

        # Agar match nahi mila, to source None rakho
        if not found:
            matched_pairs.append((None, target))
    
    return matched_pairs

# --- IMPROVED FFMPEG UTILITIES ---
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
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return json.loads(result.stdout)
    except Exception as e:
        print(f"Error getting media info: {e}")
    
    return {"streams": [], "format": {}}

def extract_streams_info(media_info: Dict) -> Dict:
    """Extract audio and subtitle stream information"""
    audio_streams = []
    subtitle_streams = []
    
    for stream in media_info.get("streams", []):
        codec_type = stream.get("codec_type", "")
        
        if codec_type == "audio":
            audio_info = {
                "index": stream.get("index"),
                "codec": stream.get("codec_name"),
                "language": stream.get("tags", {}).get("language", "und"),
                "channels": stream.get("channels", 2),
                "title": stream.get("tags", {}).get("title", "")
            }
            audio_streams.append(audio_info)
            
        elif codec_type == "subtitle":
            sub_info = {
                "index": stream.get("index"),
                "codec": stream.get("codec_name"),
                "language": stream.get("tags", {}).get("language", "und"),
                "title": stream.get("tags", {}).get("title", "")
            }
            subtitle_streams.append(sub_info)
    
    return {
        "audio_streams": audio_streams,
        "subtitle_streams": subtitle_streams,
        "total_streams": len(media_info.get("streams", []))
    }

def merge_audio_subtitles_v2(source_path: str, target_path: str, output_path: str) -> bool:
    try:
        cmd = [
            "ffmpeg", "-y",
            "-i", target_path,     # input 0 (Target Video)
            "-i", source_path,     # input 1 (Source Audio/Subs)
            
            "-map", "0:v:0",       # Target video
            "-map", "0:a?",        # Target audio (Original)
            "-map", "1:a?",        # Source audio (Added)
            "-map", "0:s?",        # Target subs
            "-map", "1:s?",        # Source subs
            
            "-c:v", "copy",        # Video same rahegi (No lag)
            "-c:a", "aac",         # Audio re-encode (Sync ke liye zaroori hai)
            "-b:a", "192k",        # Good quality bitrate
            "-ac", "2",            # Compatibility ke liye stereo
            "-af", "aresample=async=1", # SYNC FIX: Audio gaps ko fill karta hai
            
            "-c:s", "copy",        # Subtitles copy
            
            "-disposition:a:0", "0",# Make TARGET audio non-default

            
            "-disposition:a:1", "default", # Make SOURCE (added) audio default
            "-map_metadata", "0",
            output_path
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print("FFmpeg error:", result.stderr[:500])
            return False
        return True

    except Exception as e:
        print("Merge failed:", e)
        return False

def analyze_and_optimize_audio(source_path: str, target_path: str, output_audio_path: str) -> bool:
    """
    Analyze source audio and optimize it for MX Player compatibility
    """
    try:
        # Get detailed info about both files
        source_info = get_media_info(source_path)
        target_info = get_media_info(target_path)
        
        source_streams = extract_streams_info(source_info)
        target_streams = extract_streams_info(target_info)
        
        # Extract target video properties for alignment
        target_video_stream = None
        for stream in target_info.get("streams", []):
            if stream.get("codec_type") == "video":
                target_video_stream = stream
                break
        
        if not target_video_stream:
            print("No video stream found in target file")
            return False
        
        # Get target video properties for MX Player compatibility
        target_fps = eval(target_video_stream.get("avg_frame_rate", "24000/1001"))
        target_duration = float(target_info.get("format", {}).get("duration", 0))
        
        # Get video timebase for sync
        if "time_base" in target_video_stream:
            time_base = target_video_stream["time_base"]
        else:
            time_base = "1/1000"
        
        # Prepare audio optimization command for MX Player
        cmd = [
            "ffmpeg", "-y",
            "-i", source_path,
            "-strict", "-2",  # Allow experimental codecs if needed
        ]
        
        # Map all audio streams from source
        for i in range(len(source_streams["audio_streams"])):
            cmd.extend(["-map", f"0:a:{i}"])
        
        # MX Player specific audio optimization
        cmd.extend([
            "-c:a", "aac",                     # AAC is best for MX Player
            "-b:a", "192k",                    # Optimal bitrate
            "-ar", "48000",                    # Must be 48kHz for MX Player
            "-ac", "2",                        # Stereo (MX Player prefers this)
            "-async", "1",                     # Force audio resampling for sync
            "-af", "aresample=async=1000",     # Aggressive sync for MX Player
        ])
        
        # Add proper timestamp handling for MX Player
        cmd.extend([
            "-avoid_negative_ts", "make_zero",  # Fix negative timestamps
            "-fflags", "+genpts",              # Generate missing PTS
            "-max_interleave_delta", "0",      # Reduce interleaving delay
        ])
        
        # Match target duration precisely
        if target_duration > 0:
            source_duration = float(source_info.get("format", {}).get("duration", 0))
            if abs(source_duration - target_duration) > 0.1:  # If difference > 100ms
                cmd.extend(["-t", str(target_duration)])
        
        cmd.append(output_audio_path)
        
        print(f"MX Player Audio optimization command: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0
        
    except Exception as e:
        print(f"Audio optimization error: {e}")
        return False

def merge_for_mx_player_compatibility(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Merge files with MX Player compatibility fixes and proper subtitle handling
    """
    try:
        # Create temp directory for processed audio
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Step 1: Optimize audio for MX Player
            optimized_audio = str(temp_path / "mx_optimized_audio.m4a")
            
            if not analyze_and_optimize_audio(source_path, target_path, optimized_audio):
                print("Failed to optimize audio for MX Player")
                return False
            
            # Step 2: Get media info for both files
            target_info = get_media_info(target_path)
            source_info = get_media_info(source_path)
            
            target_streams_info = extract_streams_info(target_info)
            source_streams_info = extract_streams_info(source_info)
            
            audio_info = get_media_info(optimized_audio)
            audio_streams_info = extract_streams_info(audio_info)
            
            # Step 3: Check subtitle requirements
            target_has_subs = len(target_streams_info["subtitle_streams"]) > 0
            source_has_subs = len(source_streams_info["subtitle_streams"]) > 0
            
            print(f"Subtitle Info: Target has {len(target_streams_info['subtitle_streams'])} subs, "
                  f"Source has {len(source_streams_info['subtitle_streams'])} subs")
            
            # Step 4: Prepare merge command with MX Player fixes
            cmd = [
                "ffmpeg", "-y",
                "-i", target_path,          # Target file
                "-i", optimized_audio,      # Optimized audio
                "-strict", "-2",            # Allow experimental codecs
            ]
            
            # Add source file if it has subtitles and target doesn't
            if source_has_subs and not target_has_subs:
                cmd.insert(3, source_path)
                cmd.insert(3, "-i")
            
            # MX Player requires proper stream ordering
            # Always map target video first
            cmd.extend(["-map", "0:v"])              # Video from target
            
            # Map target audio if exists
            if target_streams_info["audio_streams"]:
                cmd.extend(["-map", "0:a"])
            
            # Map optimized audio (from source)
            cmd.extend(["-map", "1:a"])             # Optimized audio
            
            # Map subtitles according to logic
            if target_has_subs:
                # Target has subtitles - always keep them
                cmd.extend(["-map", "0:s"])
            
            if source_has_subs and target_has_subs:
                # Both have subtitles - add source subtitles
                if not source_path in cmd:
                    cmd.extend(["-i", source_path])
                cmd.extend(["-map", "2:s"])
            elif source_has_subs and not target_has_subs:
                # Only source has subtitles
                cmd.extend(["-map", "2:s"])
            
            # Video settings for MX Player
            cmd.extend([
                "-c:v", "copy",             # Copy video
                "-vsync", "cfr",            # Constant frame rate (better for MX)
                "-copyts",                  # Copy timestamps
                "-start_at_zero",           # Start at zero
            ])
            
            # Audio settings for MX Player
            cmd.extend([
                "-c:a", "copy",             # Copy all audio (already optimized)
                "-disposition:a", "default",  # Set default audio
                "-af", "aresample=async=1000",  # Extra sync for MX Player
            ])
            
            # Subtitle codec settings - copy all
            cmd.extend(["-c:s", "copy"])
            
            # MX Player container optimizations
            cmd.extend([
                "-movflags", "+faststart",  # Quick start for streaming
                "-f", "mp4" if output_path.endswith('.mp4') else "matroska",
                "-fflags", "+genpts+igndts",  # Generate PTS, ignore DTS
                "-max_interleave_delta", "1000000",  # Reduce buffer
            ])
            
            # Metadata for MX Player
            cmd.extend([
                "-metadata", "handler_name=MX Player Compatible",
                "-metadata:s:v", "title=Video Track",
                "-metadata:s:a", "title=Audio Track",
                "-write_tmcd", "0",         # Don't write timecode
            ])
            
            cmd.append(output_path)
            
            print(f"MX Player merge command: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print("MX Player merge successful")
                return True
            else:
                print(f"MX Player merge failed: {result.stderr[:500]}")
                return False
            
    except Exception as e:
        print(f"MX Player merge error: {e}")
        return False

def merge_audio_subtitles_v2_mx_fixed(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Improved v2 method with MX Player fixes and proper audio disposition
    Makes source audio default, target audio non-default
    """
    try:
        # Get media info
        target_info = get_media_i
