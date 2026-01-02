import os
import re
import asyncio
import tempfile
import subprocess
import json
import time
import math
from pathlib import Path
from typing import List, Dict, Tuple, Optional
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
        self.progress_msg = None  # Store progress message reference

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

<blockquote><b>🎯 NEW: Optimized Workflow</b>
- ✅ Extract tracks first, delete source to save space
- ✅ Smart compression (30MB limit)
- ✅ Target analysis before re-encoding
- ✅ Target original audio preserved
- ✅ Automatic cleanup</blockquote>

<blockquote><b>⚠️ Important Notes:</b>
- Files are matched by season and episode numbers
- <b>MKV format works best</b>
- Original target file tracks are preserved
- Only new audio/subtitle tracks are added from source
- Server needs FFmpeg installed</blockquote>"""
    
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

# --- NEW: TARGET ANALYSIS & TRACK EXTRACTION ---
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
    """Extract audio and subtitle stream information with timing"""
    audio_streams = []
    subtitle_streams = []
    
    for stream in media_info.get("streams", []):
        codec_type = stream.get("codec_type", "")
        
        if codec_type == "audio":
            # Get audio delay/timing information
            # Some formats store delay in 'start_time', others in 'start_pts'
            start_time = stream.get("start_time", 0)
            start_pts = stream.get("start_pts", 0)
            
            # Convert to seconds if not already
            try:
                if isinstance(start_time, str):
                    start_time = float(start_time)
            except:
                start_time = 0
                
            try:
                if isinstance(start_pts, str):
                    start_pts = float(start_pts)
            except:
                start_pts = 0
            
            # Use whichever timing value is available
            audio_delay = start_time if start_time != 0 else (start_pts / 1000 if start_pts != 0 else 0)
            
            audio_info = {
                "index": stream.get("index"),
                "codec": stream.get("codec_name"),
                "language": stream.get("tags", {}).get("language", "und"),
                "channels": stream.get("channels", 2),
                "sample_rate": stream.get("sample_rate", 48000),
                "bit_rate": stream.get("bit_rate", "128000"),
                "duration": stream.get("duration", 0),
                "title": stream.get("tags", {}).get("title", ""),
                "start_time": audio_delay,  # Added: Audio delay in seconds
                "start_pts": start_pts,
                "time_base": stream.get("time_base", "1/1000")
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

def analyze_target_specs(file_path: str) -> Dict:
    """Analyze target file specifications for re-encoding"""
    info = get_media_info(file_path)
    
    target_specs = {
        "format": info.get("format", {}).get("format_name", "matroska"),
        "duration": float(info.get("format", {}).get("duration", 0)),
        "size": int(info.get("format", {}).get("size", 0)),
        "audio_codec": None,
        "audio_bitrate": 192000,
        "audio_channels": 2,
        "audio_sample_rate": 48000
    }
    
    # Find audio stream specs
    for stream in info.get("streams", []):
        if stream.get("codec_type") == "audio":
            target_specs["audio_codec"] = stream.get("codec_name", "aac")
            target_specs["audio_bitrate"] = int(stream.get("bit_rate", 192000))
            target_specs["audio_channels"] = stream.get("channels", 2)
            target_specs["audio_sample_rate"] = int(stream.get("sample_rate", 48000))
            break
    
    return target_specs

def extract_tracks_from_source(source_path: str, temp_dir: Path) -> Dict:
    """Extract audio and subtitle tracks from source file with timing preservation"""
    extracted_tracks = {
        "audio_files": [],
        "subtitle_files": [],
        "success": False
    }
    
    try:
        # Get source info
        source_info = get_media_info(source_path)
        streams_info = extract_streams_info(source_info)
        
        print(f"Extracting {len(streams_info['audio_streams'])} audio tracks and "
              f"{len(streams_info['subtitle_streams'])} subtitle tracks from source")
        
        # Extract audio tracks WITH TIMING PRESERVATION
        for idx, audio_info in enumerate(streams_info["audio_streams"]):
            audio_output = temp_dir / f"audio_{idx}_{audio_info['language']}.m4a"
            
            # NEW: Extract with timing offset preserved
            cmd = [
                "ffmpeg", "-y",
                "-i", source_path,
                "-map", f"0:a:{idx}",
                "-c:a", "copy",
                "-bsf:a", "aac_adtstoasc",  # Fix AAC timing
                str(audio_output)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                extracted_tracks["audio_files"].append({
                    "path": str(audio_output),
                    "index": idx,
                    "language": audio_info["language"],
                    "codec": audio_info["codec"],
                    "original_bitrate": audio_info.get("bit_rate", "128000"),
                    "start_time": audio_info.get("start_time", 0),  # Store timing info
                    "start_pts": audio_info.get("start_pts", 0)
                })
                print(f"  ✓ Extracted audio track {idx+1} ({audio_info['language']}) "
                      f"delay: {audio_info.get('start_time', 0):.3f}s")
        
        # Extract subtitle tracks (same as before)
        for idx, sub_info in enumerate(streams_info["subtitle_streams"]):
            sub_output = temp_dir / f"subtitle_{idx}_{sub_info['language']}.srt"
            
            cmd = [
                "ffmpeg", "-y",
                "-i", source_path,
                "-map", f"0:s:{idx}",
                str(sub_output)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                extracted_tracks["subtitle_files"].append({
                    "path": str(sub_output),
                    "index": idx,
                    "language": sub_info["language"],
                    "codec": sub_info["codec"]
                })
                print(f"  ✓ Extracted subtitle track {idx+1} ({sub_info['language']})")
        
        extracted_tracks["success"] = True
        
    except Exception as e:
        print(f"Error extracting tracks: {e}")
        import traceback
        traceback.print_exc()
    
    return extracted_tracks

def calculate_bitrate_for_size(file_path: str, target_size_mb: int = 3000) -> int:
    """Calculate appropriate bitrate to keep file under target size"""
    try:
        info = get_media_info(file_path)
        duration = float(info.get("format", {}).get("duration", 0))
        
        if duration <= 0:
            return 128000  # Default
        
        # Calculate bitrate in bits per second
        # target_size_mb * 8 * 1024 * 1024 = bits
        # divided by duration = bits per second
        target_bitrate = int((target_size_mb * 8 * 1024 * 1024) / duration)
        
        # Apply reasonable limits
        if target_bitrate < 64000:  # Minimum 64kbps
            return 64000
        elif target_bitrate > 320000:  # Maximum 320kbps
            return 320000
        else:
            return target_bitrate
            
    except Exception as e:
        print(f"Error calculating bitrate: {e}")
        return 128000  # Fallback

def reencode_audio_for_target(audio_path: str, target_specs: Dict, output_path: str, audio_delay: float = 0) -> bool:
    """Re-encode audio to match target specifications with timing correction"""
    try:
        # Get audio file size
        file_size = os.path.getsize(audio_path) / (1024 * 1024)  # MB
        print(f"Audio file size: {file_size:.2f} MB, Delay: {audio_delay:.3f}s")
        
        # Calculate appropriate bitrate
        if file_size > 30:  # If audio track > 30MB
            print(f"Audio track exceeds 30MB ({file_size:.2f} MB), compressing...")
            target_bitrate = calculate_bitrate_for_size(audio_path, 30)
            print(f"Using compressed bitrate: {target_bitrate} bps")
        else:
            # Use target's bitrate or original if smaller
            target_bitrate = target_specs.get("audio_bitrate", 192000)
            print(f"Using target bitrate: {target_bitrate} bps")
        
        # Choose codec based on target
        target_codec = target_specs.get("audio_codec", "aac")
        if target_codec.lower() not in ["aac", "opus", "mp3"]:
            target_codec = "aac"  # Default to AAC for compatibility
        
        # Build command with timing adjustment
        cmd = [
            "ffmpeg", "-y",
            "-i", audio_path,
        ]
        
        # Apply audio delay if needed
        if audio_delay != 0:
            if audio_delay > 0:
                # Audio starts later than video, add silence at beginning
                cmd.extend(["-af", f"adelay={int(audio_delay*1000)}|{int(audio_delay*1000)}"])
            else:
                # Audio starts earlier than video, need to cut beginning
                cmd.extend(["-ss", str(abs(audio_delay))])
        
        cmd.extend([
            "-c:a", target_codec,
            "-b:a", str(target_bitrate),
            "-ar", str(target_specs.get("audio_sample_rate", 48000)),
            "-ac", str(target_specs.get("audio_channels", 2)),
            "-vn",  # No video
            "-sn",  # No subtitles
            output_path
        ])
        
        print(f"Re-encoding command with delay correction: {' '.join(cmd[:10])}...")
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            new_size = os.path.getsize(output_path) / (1024 * 1024)
            print(f"Re-encoded audio: {new_size:.2f} MB (compression: {(file_size - new_size)/file_size*100:.1f}%)")
            return True
        else:
            print(f"Re-encoding failed: {result.stderr[:500]}")
            return False
            
    except Exception as e:
        print(f"Error re-encoding audio: {e}")
        return False

def merge_tracks_into_target(target_path: str, reencoded_tracks: Dict, output_path: str) -> bool:
    """Merge re-encoded tracks into target file with timing"""
    try:
        # Get target info
        target_info = get_media_info(target_path)
        target_streams = extract_streams_info(target_info)
        
        # Build ffmpeg command
        inputs = [target_path]
        maps = ["-map", "0:v"]  # Target video
        
        # Map target audio (original - will be preserved)
        for i in range(len(target_streams["audio_streams"])):
            maps.extend(["-map", f"0:a:{i}"])
        
        # Add re-encoded audio tracks WITH TIMING
        audio_idx = 1
        for audio_track in reencoded_tracks.get("audio_files", []):
            if os.path.exists(audio_track["path"]):
                inputs.append(audio_track["path"])
                maps.extend(["-map", f"{audio_idx}:a:0"])
                
                # Apply timing offset using -itsoffset
                audio_delay = audio_track.get("original_delay", 0)
                if audio_delay != 0:
                    # Positive delay means audio starts later, need itsoffset
                    maps.extend([f"-itsoffset:{audio_idx}", str(audio_delay)])
                
                audio_idx += 1
        
        # Map target subtitles
        for i in range(len(target_streams["subtitle_streams"])):
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
        
        # Add all inputs
        for input_file in inputs:
            cmd.extend(["-i", input_file])
        
        # Add maps and timing offsets
        cmd.extend(maps)
        
        # Video settings - copy unchanged
        cmd.extend([
            "-c:v", "copy",
        ])
        
        # Audio settings - copy all (already re-encoded with timing)
        cmd.extend(["-c:a", "copy"])
        
        # Subtitle settings - copy all
        cmd.extend(["-c:s", "copy"])
        
        # Set source audio as default, target audio as non-default
        total_target_audio = len(target_streams["audio_streams"])
        if total_target_audio > 0:
            cmd.extend(["-disposition:a:0", "0"])  # Target audio not default
        if len(reencoded_tracks.get("audio_files", [])) > 0:
            cmd.extend([f"-disposition:a:{total_target_audio}", "default"])  # First source audio default
        
        # Container optimizations
        cmd.extend([
            "-movflags", "+faststart",
            "-max_interleave_delta", "0",
            "-async", "1",  # Force audio sync
        ])
        
        cmd.append(output_path)
        
        print(f"Merging command with timing: {' '.join(cmd[:20])}...")
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Merge successful with timing correction")
            return True
        else:
            print(f"Merge failed: {result.stderr[:500]}")
            return False
            
    except Exception as e:
        print(f"Error merging tracks: {e}")
        import traceback
        traceback.print_exc()
        return False

# Main optimized merging function
def optimized_merge(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Optimized merging workflow with audio sync fix
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        try:
            print(f"\n=== STARTING OPTIMIZED MERGE WITH SYNC FIX ===")
            print(f"Source: {os.path.basename(source_path)}")
            print(f"Target: {os.path.basename(target_path)}")
            
            # STEP 1: Extract tracks from source WITH TIMING INFO
            print("\n1. Extracting tracks from source with timing preservation...")
            extracted_tracks = extract_tracks_from_source(source_path, temp_path)
            
            if not extracted_tracks["success"]:
                print("Failed to extract tracks from source")
                return False
            
            # STEP 2: Delete source file immediately
            print("\n2. Deleting source file to save space...")
            silent_cleanup(source_path)
            
            # STEP 3: Analyze target specifications
            print("\n3. Analyzing target file specifications...")
            target_specs = analyze_target_specs(target_path)
            print(f"   Target specs: {target_specs}")
            
            # STEP 4: Re-encode extracted tracks WITH TIMING CORRECTION
            print("\n4. Re-encoding extracted tracks with timing correction...")
            reencoded_tracks = {
                "audio_files": [],
                "subtitle_files": []
            }
            
            # Re-encode audio tracks with timing info
            for audio_track in extracted_tracks["audio_files"]:
                reencoded_path = temp_path / f"reencoded_{os.path.basename(audio_track['path'])}"
                
                # NEW: Pass audio delay to re-encoding function
                audio_delay = audio_track.get("start_time", 0)
                if reencode_audio_for_target(audio_track["path"], target_specs, str(reencoded_path), audio_delay):
                    reencoded_tracks["audio_files"].append({
                        "path": str(reencoded_path),
                        "language": audio_track["language"],
                        "original_delay": audio_delay  # Keep track of original delay
                    })
                    # Delete original extracted audio
                    silent_cleanup(audio_track["path"])
                else:
                    print(f"Failed to re-encode audio track {audio_track['language']}")
            
            # Copy subtitle tracks (no re-encoding needed)
            for sub_track in extracted_tracks["subtitle_files"]:
                reencoded_tracks["subtitle_files"].append(sub_track)
            
            # STEP 5: Merge re-encoded tracks into target WITH TIMING
            print("\n5. Merging tracks into target file with timing preservation...")
            success = merge_tracks_into_target(target_path, reencoded_tracks, output_path)
            
            # STEP 6: Cleanup all temporary files
            print("\n6. Cleaning up temporary files...")
            cleanup_count = 0
            
            # Cleanup re-encoded audio files
            for audio_track in reencoded_tracks["audio_files"]:
                cleanup_count += silent_cleanup(audio_track["path"])
            
            # Cleanup subtitle files
            for sub_track in reencoded_tracks["subtitle_files"]:
                cleanup_count += silent_cleanup(sub_track["path"])
            
            print(f"   Cleaned up {cleanup_count} temporary files")
            
            return success
            
        except Exception as e:
            print(f"Error in optimized merge: {e}")
            import traceback
            traceback.print_exc()
            return False
            

def get_file_extension(file_path: str) -> str:
    """Get file extension from path"""
    return Path(file_path).suffix.lower()

def merge_audio_subtitles_simple(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Main merge function - Uses optimized workflow
    """
    return optimized_merge(source_path, target_path, output_path)
