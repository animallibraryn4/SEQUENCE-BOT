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

def silent_cleanup(*file_paths):
    """
    Silently delete files without raising errors or notifying user
    Returns the number of successfully deleted files
    """
    deleted_count = 0
    for file_path in file_paths:
        if file_path and isinstance(file_path, str):
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    deleted_count += 1
                    print(f"✓ Cleaned up: {os.path.basename(file_path)}")
            except Exception as e:
                # Silent failure - don't raise, just log for debugging
                print(f"⚠️ Could not delete {file_path}: {e}")
                pass
    return deleted_count

# --- HELP TEXT UPDATE ---
def get_merging_help_text() -> str:
    """Get help text for merging commands"""
    return """
<blockquote><b>🔧 Optimized Auto File Merging Commands</b></blockquote>

<blockquote><b>/merging</b> - Start optimized file merging process
<b>/done</b> - Proceed to next step after sending files
<b>/cancel_merge</b> - Cancel current merging process</blockquote>

<blockquote><b>📝 How to use:</b>
1. Send <code>/merging</code>
2. Send all SOURCE files (base video files)
3. Send <code>/done</code>
4. Send all TARGET files (with audio/subtitles to extract)
5. Send <code>/done</code> again
6. Wait for optimized processing</blockquote>

<blockquote><b>⚡ OPTIMIZED WORKFLOW:</b>
- ✅ Target file deleted immediately after extraction
- ✅ Audio compressed if >20MB using Opus/AAC
- ✅ Tracks re-encoded to match source specifications
- ✅ Storage efficient with minimal temp files
- ✅ 20MB strict size limit for audio tracks
- ✅ Opus codec for best quality at small sizes</blockquote>

<blockquote><b>⚠️ Important Notes:</b>
- Files are matched by season and episode numbers
- <b>MKV/MP4 format works best</b>
- Long audio tracks (>2 hours) will be compressed
- Original source file quality preserved
- Server needs ffmpeg with Opus/AAC support</blockquote>"""
    
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

# ... (rest of the existing functions remain the same) ...

def get_file_extension(file_path: str) -> str:
    """Get file extension from path"""
    return Path(file_path).suffix.lower()

def merge_audio_subtitles_simple(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Simple merge function - Uses v2 method with MX Player fixes
    Makes source audio default, keeps target audio but not default
    """
    try:
        print(f"\n=== Starting Simple Merge ===")
        
        # Get media info
        target_info = get_media_info(target_path)
        source_info = get_media_info(source_path)
        
        target_streams = extract_streams_info(target_info)
        source_streams = extract_streams_info(source_info)
        
        print(f"Target audio streams: {len(target_streams['audio_streams'])}")
        print(f"Source audio streams: {len(source_streams['audio_streams'])}")
        
        # Check if we have anything to add
        if not source_streams["audio_streams"] and not source_streams["subtitle_streams"]:
            print("No audio or subtitles to add from source")
            return False
        
        # Always use v2 with MX Player fixes
        print("Using v2 method with MX Player fixes...")
        return merge_audio_subtitles_v2_mx_fixed(source_path, target_path, output_path)
            
    except Exception as e:
        print(f"Error in simple merge: {e}")
        import traceback
        traceback.print_exc()
        return False
