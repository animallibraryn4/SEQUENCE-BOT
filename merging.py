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

# Throttling system for multiple users
LAST_EDIT_TIME = {}
EDIT_INTERVAL = 1.2  # Minimum 1.2 seconds between updates

async def smart_progress_callback(current, total, msg, start_time, stage, filename, user_id, cancel_callback_data=None):
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
    - cancel_callback_data: Optional callback data for cancel button
    """
    
    now = time.time()
    
    # --- THROTTLING CHECK ---
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
    
    # Add cancel button if provided
    reply_markup = None
    if cancel_callback_data:
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel Processing", callback_data=cancel_callback_data)]
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
        target_info = get_media_info(target_path)
        source_info = get_media_info(source_path)
        
        target_streams = extract_streams_info(target_info)
        source_streams = extract_streams_info(source_info)
        
        target_has_subs = len(target_streams["subtitle_streams"]) > 0
        source_has_subs = len(source_streams["subtitle_streams"]) > 0
        
        print(f"V2 Subtitle Info: Target subs: {len(target_streams['subtitle_streams'])}, "
              f"Source subs: {len(source_streams['subtitle_streams'])}")
        
        # Build the ffmpeg command
        cmd = [
            "ffmpeg", "-y",
            "-i", target_path,  # Input 0: Target file
            "-i", source_path,  # Input 1: Source file
        ]
        
        # Map streams in order
        mappings = [
            "-map", "0:v:0",    # Target video (first video stream)
        ]
        
        # Count audio streams for proper indexing
        target_audio_count = len(target_streams["audio_streams"])
        source_audio_count = len(source_streams["audio_streams"])
        
        # Map target audio streams (non-default)
        for i in range(target_audio_count):
            mappings.extend(["-map", f"0:a:{i}"])
        
        # Map source audio streams (will be default)
        for i in range(source_audio_count):
            mappings.extend(["-map", f"1:a:{i}"])
        
        cmd.extend(mappings)
        
        # Map subtitles
        if target_has_subs:
            cmd.extend(["-map", "0:s?"])  # Target subtitles
        
        if source_has_subs and target_has_subs:
            cmd.extend(["-map", "1:s?"])  # Source subtitles (both have)
        elif source_has_subs and not target_has_subs:
            cmd.extend(["-map", "1:s?"])  # Only source has subtitles
        
        # Video settings
        cmd.extend([
            "-c:v", "copy",      # Copy video
        ])
        
        # Audio settings - AAC for compatibility
        cmd.extend([
            "-c:a", "aac",       # AAC codec
            "-b:a", "192k",      # Bitrate
            "-ar", "48000",      # Sample rate
            "-ac", "2",          # Stereo
        ])
        
        # KEY PART: Set audio dispositions
        # First, clear all default dispositions
        cmd.extend(["-disposition:a", "none"])
        
        # Then set ONLY the source audio streams as default
        # Source audio streams start at index = target_audio_count
        for i in range(source_audio_count):
            cmd.extend([f"-disposition:a:{target_audio_count + i}", "default"])
        
        # Subtitle codec - copy all
        cmd.extend(["-c:s", "copy"])
        
        # MX Player fixes
        cmd.extend([
            "-movflags", "+faststart",
            "-max_interleave_delta", "0",
        ])
        
        # Sync fixes
        cmd.extend([
            "-fflags", "+genpts",
            "-avoid_negative_ts", "make_zero",
        ])
        
        cmd.append(output_path)
        
        print(f"Merge command (with audio disposition): {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"FFmpeg error: {result.stderr[:500]}")
            return False
            
        return True
        
    except Exception as e:
        print(f"MX fixed v2 error: {e}")
        import traceback
        traceback.print_exc()
        return False

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

# --- TELEGRAM BOT HANDLERS ---
def setup_merging_handlers(app: Client):
    """Setup all merging-related handlers"""
    
    @app.on_message(filters.command("merging"))
    async def merging_command(client: Client, message: Message):
        """Start the merging process"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        # Initialize merging state
        merging_users[user_id] = MergingState(user_id)
        
        help_text = (
            "<blockquote><b>🔧 AUTO FILE MERGING MODE</b></blockquote>\n\n"
            "<blockquote>Please send the SOURCE FILES from which you want to extract audio and subtitles.</blockquote>\n\n"
            "<blockquote><b>📝 Instructions:</b>\n"
            "1. Send all source files (with desired audio/subtitle tracks)\n"
            "2. Send <code>/done</code> when finished\n"
            "3. Send all target files (to add tracks to)\n"
            "4. Send <code>/done</code> again\n"
            "5. Wait for processing</blockquote>\n\n"
            "<blockquote><b>⚠️ Requirements:</b>\n"
            "- Files should be MKV format for best results\n"
            "- Files should have similar naming for auto-matching\n"
            "- Bot needs ffmpeg installed on server</blockquote>"
        )
        
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_merge_cmd")]
        ])
        
        await message.reply_text(help_text, reply_markup=buttons)
    
    @app.on_callback_query(filters.regex(r"^cancel_merge_cmd$"))
    async def cancel_merge_callback(client, query):
        """Handle cancel button callback"""
        user_id = query.from_user.id
        
        if user_id in merging_users:
            del merging_users[user_id]
        
        await query.message.edit_text(
            "<blockquote><b>❌ Merge process cancelled.</b></blockquote>"
        )
        await query.answer("Merge cancelled")
    
    @app.on_message(filters.document | filters.video)
    async def handle_merging_files(client: Client, message: Message):
        """Handle files sent during merging process"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        if user_id not in merging_users:
            return
        
        state = merging_users[user_id]
        file_obj = message.document or message.video
        
        if not file_obj:
            return
        
        # Get filename
        filename = file_obj.file_name or f"file_{message.id}"
        mime_type = file_obj.mime_type or ""
        
        # Check if it's a video file
        if not any(x in mime_type for x in ['video', 'octet-stream', 'x-matroska']):
            await message.reply_text(
                f"<blockquote>⚠️ Skipping non-video file: {filename}</blockquote>"
            )
            return
        
        file_data = {
            "message": message,
            "filename": filename,
            "file_id": file_obj.file_id,
            "file_size": file_obj.file_size,
            "mime_type": mime_type
        }
        
        if state.state == "waiting_for_source":
            state.source_files.append(file_data)
            
            # Send confirmation
            if len(state.source_files) % 3 == 0 or len(state.source_files) == 1:
                await message.reply_text(
                    f"<blockquote>📥 Received {len(state.source_files)} source files.</blockquote>\n"
                    f"<blockquote>Send <code>/done</code> when finished with source files.</blockquote>"
                )
                
        elif state.state == "waiting_for_target":
            state.target_files.append(file_data)
            
            # Send confirmation
            if len(state.target_files) % 3 == 0 or len(state.target_files) == 1:
                await message.reply_text(
                    f"<blockquote>📥 Received {len(state.target_files)} target files.</blockquote>\n"
                    f"<blockquote>Send <code>/done</code> when finished with target files.</blockquote>"
                )
    
    @app.on_message(filters.command("done"))
    async def done_command(client: Client, message: Message):
        """Handle /done command to proceed to next step"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        if user_id not in merging_users:
            await message.reply_text(
                "<blockquote>❌ No active merging session. Use <code>/merging</code> to start.</blockquote>"
            )
            return
        
        state = merging_users[user_id]
        
        if state.state == "waiting_for_source":
            if not state.source_files:
                await message.reply_text(
                    "<blockquote>❌ No source files received yet.</blockquote>\n"
                    "<blockquote>Please send source files first.</blockquote>"
                )
                return
            
            state.state = "waiting_for_target"
            
            await message.reply_text(
                f"<blockquote><b>✅ Source files received!</b></blockquote>\n\n"
                f"<blockquote>Total source files: {len(state.source_files)}</blockquote>\n\n"
                f"<blockquote><b>Now send me the TARGET files.</b></blockquote>\n\n"
                f"<blockquote><i>📝 Note: Send the same number of target files</i></blockquote>"
            )
            
        elif state.state == "waiting_for_target":
            if not state.target_files:
                await message.reply_text(
                    "<blockquote>❌ No target files received yet.</blockquote>\n"
                    "<blockquote>Please send target files first.</blockquote>"
                )
                return
            
            # Check if counts match
            if len(state.source_files) != len(state.target_files):
                await message.reply_text(
                    f"<blockquote>⚠️ File count mismatch!</blockquote>\n\n"
                    f"<blockquote>Source files: {len(state.source_files)}\n"
                    f"Target files: {len(state.target_files)}</blockquote>\n\n"
                    f"<blockquote>You can continue anyway, but only matching episodes will be processed.</blockquote>",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Continue Anyway", callback_data="continue_merge")],
                        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_merge")]
                    ])
                )
                return
            
            # Start processing
            await start_merging_process(client, state, message)
            
        else:
            await message.reply_text(
                "<blockquote>❌ Invalid state. Use <code>/cancel_merge</code> to reset.</blockquote>"
            )
    
    @app.on_callback_query(filters.regex(r"^(continue_merge|cancel_merge)$"))
    async def merge_control_callback(client, query):
        """Handle merge control callbacks"""
        user_id = query.from_user.id
        action = query.data
        
        if user_id not in merging_users:
            await query.answer("Session expired", show_alert=True)
            return
        
        state = merging_users[user_id]
        
        if action == "continue_merge":
            await query.message.delete()
            await start_merging_process(client, state, query.message)
            
        elif action == "cancel_merge":
            if user_id in merging_users:
                del merging_users[user_id]
            await query.message.edit_text(
                "<blockquote><b>❌ Merge process cancelled.</b></blockquote>"
            )
            await query.answer("Merge cancelled")
    
    @app.on_message(filters.command("cancel_merge"))
    async def cancel_merge_command(client: Client, message: Message):
        """Cancel the merging process"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        if user_id in merging_users:
            del merging_users[user_id]
            await message.reply_text(
                "<blockquote><b>❌ Merge process cancelled.</b></blockquote>"
            )
        else:
            await message.reply_text(
                "<blockquote>❌ No active merging session to cancel.</blockquote>"
            )

async def smart_progress_callback(current, total, msg, start_time, stage, filename, user_id, cancel_callback_data=None):
    """Throttled progress callback for multiple users"""
    # Check if processing was cancelled
    if user_id in PROCESSING_STATES and PROCESSING_STATES[user_id].get("cancelled"):
        raise asyncio.CancelledError("Processing cancelled by user")
    
    now = time.time()
    
    # Check if we should update (throttle)
    if user_id in LAST_EDIT_TIME:
        time_since_last = now - LAST_EDIT_TIME[user_id]
        if time_since_last < EDIT_INTERVAL:
            return
    
    diff = now - start_time
    
    if diff == 0 or total == 0:
        return
    
    speed = current / diff
    percent = current * 100 / total
    eta = (total - current) / speed if speed > 0 else 0
    
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
    
    # Add cancel button if callback data provided
    reply_markup = None
    if cancel_callback_data:
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel Processing", callback_data=cancel_callback_data)]
        ])
    
    try:
        await msg.edit_text(text, reply_markup=reply_markup)
        LAST_EDIT_TIME[user_id] = now
    except Exception as e:
        # If message was deleted or other error, don't update last edit time
        pass


async def smart_progress_callback(current, total, msg, start_time, stage, filename, user_id, msg_id):
    """Throttled progress callback for multiple users with cancel check"""
    # Check if processing was cancelled
    if user_id in PROCESSING_STATES and PROCESSING_STATES[user_id].get("cancelled"):
        raise asyncio.CancelledError("Processing cancelled by user")
    
    now = time.time()
    
    # Check if we should update (throttle)
    if user_id in LAST_EDIT_TIME:
        time_since_last = now - LAST_EDIT_TIME[user_id]
        if time_since_last < EDIT_INTERVAL:
            return
    
    diff = now - start_time
    
    if diff == 0 or total == 0:
        return
    
    speed = current / diff
    percent = current * 100 / total
    eta = (total - current) / speed if speed > 0 else 0
    
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
    
    try:
        await msg.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
            ])
        )
        LAST_EDIT_TIME[user_id] = now
    except Exception as e:
        # If message was deleted or other error, don't update last edit time
        pass

async def process_merging(client: Client, state: MergingState, progress_msg: Message):
    """Process the merging of all files with cancellation support"""
    user_id = state.user_id
    msg_id = progress_msg.id
    
    # Initialize processing state for this user
    PROCESSING_STATES[user_id] = {
        "cancelled": False,
        "current_file": None,
        "progress_msg_id": msg_id
    }
    
    # Clear any previous edit time for this user
    if user_id in LAST_EDIT_TIME:
        del LAST_EDIT_TIME[user_id]
    
    try:  
        # Create temporary directory  
        with tempfile.TemporaryDirectory() as temp_dir:  
            temp_path = Path(temp_dir)  
              
            # Check cancellation before starting
            if PROCESSING_STATES[user_id].get("cancelled"):
                raise asyncio.CancelledError("Processing cancelled by user")
              
            # Match files by episode  
            matched_pairs = match_files_by_episode(  
                state.source_files,   
                state.target_files  
            )  
              
            # Filter out pairs without source  
            valid_pairs = [(s, t) for s, t in matched_pairs if s is not None]  
              
            if not valid_pairs:  
                await progress_msg.edit_text(  
                    "<blockquote>❌ No matching episodes found!</blockquote>\n\n"  
                    "<blockquote>Could not match source and target files by season/episode.</blockquote>"  
                )  
                return  
            
            # Send initial count info with cancel button
            await progress_msg.edit_text(
                f"<blockquote><b>📊 Files Matched</b></blockquote>\n\n"
                f"<blockquote>Total pairs: {len(valid_pairs)}</blockquote>\n"
                f"<blockquote>Skipped (no match): {len(matched_pairs) - len(valid_pairs)}</blockquote>\n\n"
                f"<blockquote>🔄 Starting processing...</blockquote>",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                ])
            )
            
            # Process each matched pair  
            for idx, (source_data, target_data) in enumerate(valid_pairs, 1):  
                try:  
                    # Check cancellation before each file
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # Update current file in processing state
                    PROCESSING_STATES[user_id]["current_file"] = target_data['filename']
                    
                    overall_progress = f"{idx}/{len(valid_pairs)}"
                    
                    # --- SOURCE DOWNLOAD ---  
                    source_filename = f"source_{idx}{get_file_extension(source_data['filename'])}"  
                    start_time = time.time()  
                      
                    await progress_msg.edit_text(  
                        f"<blockquote><b>⬇️ Downloading Source ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>📁 {source_data['filename']}</blockquote>\n\n"
                        f"<blockquote>Status: Starting download...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                      
                    source_file = await client.download_media(  
                        source_data["message"],  
                        file_name=str(temp_path / source_filename),  
                        progress=lambda c, t: asyncio.create_task(
                            smart_progress_callback(c, t, progress_msg, start_time, 
                                                   f"⬇️ Source ({overall_progress})", 
                                                   source_data["filename"], user_id, msg_id)
                        )  
                    )  
                      
                    if not source_file:  
                        print(f"Failed to download source file {idx}")  
                        await progress_msg.edit_text(
                            f"<blockquote><b>❌ Download Failed</b></blockquote>\n\n"
                            f"<blockquote>📁 {source_data['filename']}</blockquote>\n"
                            f"<blockquote>Skipping to next file...</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )
                        continue  
                    
                    # Check cancellation after source download
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # --- TARGET DOWNLOAD ---  
                    target_filename = f"target_{idx}{get_file_extension(target_data['filename'])}"  
                    start_time = time.time()  
                    
                    await progress_msg.edit_text(  
                        f"<blockquote><b>⬇️ Downloading Target ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>📁 {target_data['filename']}</blockquote>\n\n"
                        f"<blockquote>Status: Starting download...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                      
                    target_file = await client.download_media(  
                        target_data["message"],  
                        file_name=str(temp_path / target_filename),  
                        progress=lambda c, t: asyncio.create_task(
                            smart_progress_callback(c, t, progress_msg, start_time, 
                                                   f"⬇️ Target ({overall_progress})", 
                                                   target_data["filename"], user_id, msg_id)
                        )  
                    )  
                      
                    if not target_file:  
                        print(f"Failed to download target file {idx}")  
                        await progress_msg.edit_text(
                            f"<blockquote><b>❌ Download Failed</b></blockquote>\n\n"
                            f"<blockquote>📁 {target_data['filename']}</blockquote>\n"
                            f"<blockquote>Skipping to next file...</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )
                        continue  
                      
                    # Check cancellation after target download
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        raise asyncio.CancelledError("Processing cancelled by user")
                      
                    # Output file path - keep original target filename  
                    output_filename = target_data["filename"]  
                    output_file = str(temp_path / output_filename)  
                      
                    print(f"Processing pair {idx}:")  
                    print(f"  Source: {source_data['filename']}")  
                    print(f"  Target: {target_data['filename']}")  
                    print(f"  Output: {output_filename}")  
                      
                    # --- MERGE STAGE ---  
                    merge_start_time = time.time()  
                    await progress_msg.edit_text(  
                        f"<blockquote><b>🛠️ Merging ({overall_progress})</b></blockquote>\n\n"  
                        f"<blockquote>📁 {output_filename}</blockquote>\n\n"  
                        f"<blockquote>Engine : FFmpeg</blockquote>\n"  
                        f"<blockquote>Status : Processing (0%)</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                    
                    # Run merge in thread to avoid blocking
                    merge_success = False
                    try:
                        # Use threading to run ffmpeg without blocking
                        import threading
                        from queue import Queue
                        
                        result_queue = Queue()
                        
                        def run_merge():
                            try:
                                # Check cancellation during merge
                                if PROCESSING_STATES.get(user_id, {}).get("cancelled"):
                                    result_queue.put(("cancelled", None))
                                    return
                                    
                                success = merge_audio_subtitles_simple(source_file, target_file, output_file)
                                result_queue.put(("success", success))
                            except Exception as e:
                                result_queue.put(("error", str(e)))
                        
                        # Start merge thread
                        merge_thread = threading.Thread(target=run_merge)
                        merge_thread.daemon = True
                        merge_thread.start()
                        
                        # Update merge progress periodically
                        while merge_thread.is_alive():
                            # Check cancellation
                            if PROCESSING_STATES[user_id].get("cancelled"):
                                raise asyncio.CancelledError("Processing cancelled by user")
                                
                            elapsed = time.time() - merge_start_time
                            progress_text = (
                                f"<blockquote><b>🛠️ Merging ({overall_progress})</b></blockquote>\n\n"  
                                f"<blockquote>📁 {output_filename}</blockquote>\n\n"  
                                f"<blockquote>Engine : FFmpeg</blockquote>\n"  
                                f"<blockquote>Status : Processing ({elapsed:.0f}s elapsed)</blockquote>"  
                            )
                            try:
                                await progress_msg.edit_text(
                                    progress_text,
                                    reply_markup=InlineKeyboardMarkup([
                                        [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                                    ])
                                )
                            except:
                                pass
                            await asyncio.sleep(2)  # Update every 2 seconds
                        
                        # Get result
                        if not result_queue.empty():
                            result_type, result = result_queue.get()
                            if result_type == "success":
                                merge_success = result
                            elif result_type == "cancelled":
                                raise asyncio.CancelledError("Processing cancelled by user")
                            else:
                                print(f"Merge error: {result}")
                                merge_success = False
                        else:
                            merge_success = False
                            
                    except Exception as e:
                        print(f"Merge thread error: {str(e)}")
                        merge_success = False
                      
                    # Check cancellation after merge
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        raise asyncio.CancelledError("Processing cancelled by user")
                      
                    if merge_success:  
                        # --- UPLOAD STAGE ---  
                        start_time = time.time()  
                        
                        # Clear throttle for upload
                        if user_id in LAST_EDIT_TIME:
                            del LAST_EDIT_TIME[user_id]
                          
                        await progress_msg.edit_text(  
                            f"<blockquote><b>⬆️ Uploading ({overall_progress})</b></blockquote>\n\n"
                            f"<blockquote>📁 {output_filename}</blockquote>\n\n"
                            f"<blockquote>Status: Starting upload...</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )  
                          
                        await client.send_document(  
                            chat_id=user_id,  
                            document=output_file,  
                            caption=(  
                                f"<blockquote>✅ <b>Merged File</b></blockquote>\n"  
                                f"<blockquote>📁 {target_data['filename']}</blockquote>\n"  
                                f"<blockquote>🎵 Audio tracks added from source</blockquote>\n"  
                                f"<blockquote>📝 Subtitle tracks added from source</blockquote>"  
                            ),  
                            progress=lambda c, t: asyncio.create_task(
                                smart_progress_callback(c, t, progress_msg, start_time, 
                                                       f"⬆️ Upload ({overall_progress})", 
                                                       output_filename, user_id, msg_id)
                            )  
                        )  
                          
                        # --- FINAL STATUS FOR THIS FILE ---  
                        await progress_msg.edit_text(  
                            f"<blockquote><b>✅ Merge Completed ({overall_progress})</b></blockquote>\n\n"  
                            f"<blockquote>📁 {output_filename}</blockquote>\n"  
                            f"<blockquote>🎵 Source audio set as DEFAULT</blockquote>\n"  
                            f"<blockquote>🎯 No quality loss</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )  
                          
                        print(f"Successfully merged file {idx}")  
                    else:  
                        await progress_msg.edit_text(  
                            f"<blockquote><b>❌ Merge Failed ({overall_progress})</b></blockquote>\n\n"  
                            f"<blockquote>📁 {target_data['filename']}</blockquote>\n"  
                            f"<blockquote>⚠️ This file may be incompatible or corrupted</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )  
                        print(f"Failed to merge file {idx}")  
                      
                except asyncio.CancelledError as e:
                    # User cancelled processing
                    print(f"Processing cancelled by user for file {idx}")
                    raise e  # Re-raise to exit loop
                except Exception as e:  
                    print(f"Error processing file {idx}: {str(e)}")  
                    await progress_msg.edit_text(  
                        f"<blockquote><b>❌ Processing Error ({idx}/{len(valid_pairs)})</b></blockquote>\n\n"  
                        f"<blockquote>📁 {target_data['filename']}</blockquote>\n"  
                        f"<blockquote>⚠️ Error: {str(e)[:100]}</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                  
                # Clear throttle before next file
                if user_id in LAST_EDIT_TIME:
                    del LAST_EDIT_TIME[user_id]
                
                # Small delay to avoid flooding  
                await asyncio.sleep(1)  
              
            # Final completion message  
            await progress_msg.edit_text(  
                "<blockquote><b>✅ All Merges Completed</b></blockquote>\n\n"  
                "<blockquote>🎉 All merged files have been sent to you!</blockquote>"  
            )  
              
    except asyncio.CancelledError:
        # Handle cancellation
        print(f"Merging cancelled for user {user_id}")
        await progress_msg.edit_text(  
            "<blockquote><b>❌ Processing Cancelled</b></blockquote>\n\n"  
            "<blockquote>🚫 Merging process was cancelled by user.</blockquote>\n"
            "<blockquote>Use <code>/merging</code> to start again.</blockquote>"  
        )
    except Exception as e:  
        print(f"Merge process error: {str(e)}")  
        import traceback  
        traceback.print_exc()  
        try:  
            await progress_msg.edit_text(  
                "<blockquote>❌ An error occurred during merging.</blockquote>\n"  
                "<blockquote>Please try again with different files.</blockquote>"  
            )  
        except:  
            pass  
    
    finally:
        # Clean up processing state
        if user_id in PROCESSING_STATES:
            del PROCESSING_STATES[user_id]
        if user_id in LAST_EDIT_TIME:
            del LAST_EDIT_TIME[user_id]
        if user_id in merging_users:  
            del merging_users[user_id]
                      
 
