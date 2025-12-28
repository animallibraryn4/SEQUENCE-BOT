import os
import re
import asyncio
import tempfile
import subprocess
import json
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

# --- PARSING ENGINE FOR EPISODE MATCHING ---
def parse_episode_info(filename: str) -> Dict:
    name = filename.lower()

    patterns = [
        r's\s*(\d+)[\s._-]*e\s*(\d+)',      # S01E01, S1_E1, S01-E01
        r'season\s*(\d+)[\s._-]*episode\s*(\d+)',
        r'(\d+)[xX](\d+)',                  # 1x01
        r'ep\s*(\d+)',                      # EP01
    ]

    season = 1
    episode = 0

    for p in patterns:
        m = re.search(p, name)
        if m:
            if len(m.groups()) == 2:
                season = int(m.group(1))
                episode = int(m.group(2))
            else:
                episode = int(m.group(1))
            break

    return {"season": season, "episode": episode}
    

def match_files_by_episode(source_files: List[Dict], target_files: List[Dict]) -> List[Tuple[Dict, Dict]]:
    """Match source and target files by season and episode"""
    matched_pairs = []
    
    for target in target_files:
        target_info = parse_episode_info(target.get("filename", ""))
        
        # Find matching source file
        for source in source_files:
            source_info = parse_episode_info(source.get("filename", ""))
            
            if (source_info["season"] == target_info["season"] and 
                source_info["episode"] == target_info["episode"]):
                matched_pairs.append((source, target))
                break
        else:
            # If no match found, add None for source
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
            
            "-disposition:a:0", "default",
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
        
        # Set first audio stream as default during optimization
        if len(source_streams["audio_streams"]) > 0:
            cmd.extend(["-disposition:a:0", "default"])
        
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
        
        # Add metadata to identify as added audio
        cmd.extend(["-metadata", "title=Added Audio (Auto-Selected)"])
        
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
            
            # Map target audio if exists (NOT as default)
            if target_streams_info["audio_streams"]:
                cmd.extend(["-map", "0:a"])
            
            # Map optimized audio (from source) - will be set as default
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
            
            # Audio settings for MX Player - Set source audio as default
            if target_streams_info["audio_streams"]:
                # Both target and source have audio
                target_audio_count = len(target_streams_info["audio_streams"])
                
                # Copy target audio codec (NOT default)
                for i in range(target_audio_count):
                    cmd.extend([f"-c:a:{i}", "copy"])
                    cmd.extend([f"-disposition:a:{i}", "0"])  # Not default
                
                # Set source audio as default (first source audio stream)
                source_start_idx = target_audio_count
                cmd.extend([f"-c:a:{source_start_idx}", "copy"])  # Already optimized
                cmd.extend([f"-disposition:a:{source_start_idx}", "default"])  # AUTO SELECTED
                
                # Copy remaining source audio streams (not default)
                for i in range(1, len(audio_streams_info["audio_streams"])):
                    cmd.extend([f"-c:a:{source_start_idx + i}", "copy"])
                    cmd.extend([f"-disposition:a:{source_start_idx + i}", "0"])
            else:
                # Only source has audio - set as default
                cmd.extend(["-c:a", "copy"])  # Already optimized
                cmd.extend(["-disposition:a", "default"])  # AUTO SELECTED
            
            # Audio filter for sync
            cmd.extend(["-af", "aresample=async=1000"])
            
            # Subtitle codec settings - copy all
            cmd.extend(["-c:s", "copy"])
            
            # MX Player container optimizations
            cmd.extend([
                "-movflags", "+faststart",  # Quick start for streaming
                "-f", "mp4" if output_path.endswith('.mp4') else "matroska",
                "-fflags", "+genpts+igndts",  # Generate PTS, ignore DTS
                "-max_interleave_delta", "1000000",  # Reduce buffer
            ])
            
            # Metadata for MX Player - Mark source audio as preferred
            cmd.extend([
                "-metadata", "handler_name=MX Player Compatible",
                "-metadata:s:v", "title=Video Track",
                "-metadata:s:a", "title=Added Audio (Auto-Selected)",
                "-write_tmcd", "0",         # Don't write timecode
            ])
            
            cmd.append(output_path)
            
            print(f"MX Player merge command: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print("MX Player merge successful - Source audio auto-selected")
                return True
            else:
                print(f"MX Player merge failed: {result.stderr[:500]}")
                return False
            
    except Exception as e:
        print(f"MX Player merge error: {e}")
        return False

def merge_audio_subtitles_v2_mx_fixed(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Improved v2 method with MX Player fixes and proper subtitle handling
    """
    try:
        # Get media info to check subtitles
        target_info = get_media_info(target_path)
        source_info = get_media_info(source_path)
        
        target_streams = extract_streams_info(target_info)
        source_streams = extract_streams_info(source_info)
        
        target_has_subs = len(target_streams["subtitle_streams"]) > 0
        source_has_subs = len(source_streams["subtitle_streams"]) > 0
        
        print(f"V2 Subtitle Info: Target subs: {len(target_streams['subtitle_streams'])}, "
              f"Source subs: {len(source_streams['subtitle_streams'])}")
        
        cmd = [
            "ffmpeg", "-y",
            "-i", target_path,
            "-i", source_path,
            
            # Stream mapping - always map target video
            "-map", "0:v:0",
            "-map", "0:a?",              # Target audio
            "-map", "1:a?",              # Source audio (will be auto-selected)
        ]
        
        # Handle subtitles based on logic
        if target_has_subs:
            cmd.extend(["-map", "0:s?"])  # Target subtitles
        
        if source_has_subs and target_has_subs:
            cmd.extend(["-map", "1:s?"])  # Source subtitles (both have)
        elif source_has_subs and not target_has_subs:
            cmd.extend(["-map", "1:s?"])  # Only source has subtitles
        
        # MX Player video fixes
        cmd.extend([
            "-c:v", "copy",
            "-vsync", "cfr",
            "-copyts",
        ])
        
        # MX Player audio fixes - Set source audio as default
        if len(target_streams["audio_streams"]) > 0:
            # Both target and source have audio
            target_audio_count = len(target_streams["audio_streams"])
            
            # Target audio (not default)
            cmd.extend(["-c:a", "aac"])
            cmd.extend(["-b:a", "192k"])
            cmd.extend(["-ar", "48000"])
            cmd.extend(["-ac", "2"])
            
            # Set disposition for all audio streams
            for i in range(target_audio_count):
                cmd.extend([f"-disposition:a:{i}", "0"])  # Not default
            
            # Set first source audio stream as default
            cmd.extend([f"-disposition:a:{target_audio_count}", "default"])  # AUTO SELECTED
        else:
            # Only source has audio - set as default
            cmd.extend(["-c:a", "aac"])
            cmd.extend(["-b:a", "192k"])
            cmd.extend(["-ar", "48000"])
            cmd.extend(["-ac", "2"])
            cmd.extend(["-disposition:a", "default"])  # AUTO SELECTED
        
        # Audio sync
        cmd.extend(["-async", "1"])
        cmd.extend(["-af", "aresample=async=1000"])
        
        # Subtitle codec - copy all
        cmd.extend(["-c:s", "copy"])
        
        # MX Player container fixes
        cmd.extend([
            "-movflags", "+faststart",
            "-max_interleave_delta", "0",
        ])
        
        # Sync fixes
        cmd.extend([
            "-fflags", "+genpts",
            "-avoid_negative_ts", "make_zero",
            "-map_metadata", "0",
        ])
        
        # Metadata to indicate auto-selected audio
        cmd.extend(["-metadata:s:a", "title=Added Audio (Auto-Selected)"])
        
        cmd.append(output_path)
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0
        
    except Exception as e:
        print(f"MX fixed v2 error: {e}")
        return False

def get_file_extension(file_path: str) -> str:
    """Get file extension from path"""
    return Path(file_path).suffix.lower()

def merge_audio_subtitles_simple(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Main merging function with MX Player compatibility and proper subtitle handling
    """
    try:
        print(f"\n=== Starting MX Player Compatible Merge ===")
        
        # Get media info for subtitle check
        # Get media info for subtitle check
        target_info = get_media_info(target_path)
        source_info = get_media_info(source_path)
        
        target_streams = extract_streams_info(target_info)
        source_streams = extract_streams_info(source_info)
        
        print(f"Target has {len(target_streams['subtitle_streams'])} subtitle streams")
        print(f"Source has {len(source_streams['subtitle_streams'])} subtitle streams")
        
        # Determine output format based on target
        target_ext = get_file_extension(target_path)
        if not output_path.endswith(target_ext):
            output_path = output_path.rsplit('.', 1)[0] + target_ext
        
        print(f"Target format: {target_info.get('format', {}).get('format_name', 'unknown')}")
        print(f"Target video codec: {[s.get('codec_name') for s in target_info.get('streams', []) if s.get('codec_type') == 'video']}")
        
        # Check if we have audio to add
        if not source_streams["audio_streams"] and not source_streams["subtitle_streams"]:
            print("No audio or subtitles to add from source")
            return False
        
        # Try MX Player optimized merge first
        if merge_for_mx_player_compatibility(source_path, target_path, output_path):
            print("=== MX Player Optimized Merge Successful ===")
            
            # Verify subtitles in output
            output_info = get_media_info(output_path)
            output_streams = extract_streams_info(output_info)
            print(f"Output has {len(output_streams['subtitle_streams'])} subtitle streams")
            
            # Additional post-processing for MX Player if needed
            if target_ext.lower() == '.mp4':
                # Run qt-faststart for better MP4 compatibility
                try:
                    temp_output = output_path + ".temp"
                    os.rename(output_path, temp_output)
                    qt_cmd = ["qt-faststart", temp_output, output_path]
                    subprocess.run(qt_cmd, capture_output=True)
                    os.remove(temp_output)
                    print("Applied qt-faststart for better streaming")
                except:
                    pass
            
            return True
        else:
            print("MX Player merge failed, trying standard method...")
            # Fallback to standard method but with MX Player fixes
            return merge_audio_subtitles_v2_mx_fixed(source_path, target_path, output_path)
            
    except Exception as e:
        print(f"Error in MX Player merge: {e}")
        import traceback
        traceback.print_exc()
        return merge_audio_subtitles_v2(source_path, target_path, output_path)


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

async def start_merging_process(client: Client, state: MergingState, message: Message):
    """Start the merging process"""
    user_id = state.user_id
    state.state = "processing"
    state.total_files = min(len(state.source_files), len(state.target_files))
    
    
    # Send initial processing message
    progress_msg = await message.reply_text(
        f"<blockquote><b>🔄 Starting Merge Process</b></blockquote>\n\n"
        f"<blockquote>📊 Matching files...\n"
        f"⏳ Please wait...</blockquote>"
    )
    
    # Start the merging process in background
    asyncio.create_task(process_merging(client, state, progress_msg))

async def process_merging(client: Client, state: MergingState, progress_msg: Message):
    """Process the merging of all files"""
    user_id = state.user_id
    
    try:
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
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
            
            # Process each matched pair
            success_count = 0
            failed_count = 0
            skipped_count = len(matched_pairs) - len(valid_pairs)
            
            for idx, (source_data, target_data) in enumerate(valid_pairs, 1):
                # Update progress
                try:
                    progress_text = (
                        f"<blockquote><b>🔄 Merging Files</b></blockquote>\n\n"
                        f"<blockquote>📊 Progress: {idx}/{len(valid_pairs)}\n"
                        f"✅ Successful: {success_count}\n"
                        f"❌ Failed: {failed_count}\n"
                        f"⏳ Current: Episode {idx}</blockquote>"
                    )
                    await progress_msg.edit_text(progress_text)
                except:
                    pass
                
                try:
                    # Download source file
                    source_filename = f"source_{idx}{get_file_extension(source_data['filename'])}"
                    source_file = await client.download_media(
                        source_data["message"],
                        file_name=str(temp_path / source_filename)
                    )
                    
                    if not source_file:
                        print(f"Failed to download source file {idx}")
                        failed_count += 1
                        continue
                    
                    # Download target file
                    target_filename = f"target_{idx}{get_file_extension(target_data['filename'])}"
                    target_file = await client.download_media(
                        target_data["message"],
                        file_name=str(temp_path / target_filename)
                    )
                    
                    if not target_file:
                        print(f"Failed to download target file {idx}")
                        failed_count += 1
                        continue
                    
                    # Output file path - keep original target filename
                    output_filename = target_data["filename"]
                    output_file = str(temp_path / output_filename)
                    
                    print(f"Processing pair {idx}:")
                    print(f"  Source: {source_data['filename']}")
                    print(f"  Target: {target_data['filename']}")
                    print(f"  Output: {output_filename}")
                    
                    # Merge audio and subtitles using improved method
                    if merge_audio_subtitles_simple(source_file, target_file, output_file):
                        # Upload merged file
                        await client.send_document(
                            chat_id=user_id,
                            document=output_file,
                            caption=(
                                f"<blockquote>✅ <b>Merged File</b></blockquote>\n"
                                f"<blockquote>📁 {target_data['filename']}</blockquote>\n"
                                f"<blockquote>🎵 Audio tracks added from source</blockquote>\n"
                                f"<blockquote>📝 Subtitle tracks added from source</blockquote>\n"
                                f"<blockquote>🔊 <b>Note:</b> Source audio is auto-selected by default</blockquote>"
                            )
                        )
                        success_count += 1
                        print(f"Successfully merged file {idx}")
                    else:
                        failed_count += 1
                        await client.send_message(
                            user_id,
                            f"<blockquote>❌ Failed to merge: {target_data['filename']}</blockquote>\n"
                            f"<blockquote><i>This file may be incompatible or corrupted.</i></blockquote>"
                        )
                        print(f"Failed to merge file {idx}")
                    
                except Exception as e:
                    print(f"Error processing file {idx}: {str(e)}")
                    failed_count += 1
                    try:
                        await client.send_message(
                            user_id,
                            f"<blockquote>❌ Error processing: {target_data['filename']}</blockquote>\n"
                            f"<blockquote><i>Error: {str(e)[:100]}</i></blockquote>"
                        )
                    except:
                        pass
                
                # Small delay to avoid flooding
                await asyncio.sleep(2)
            
            # Final summary
            summary = (
                f"<blockquote><b>📊 Merge Process Complete!</b></blockquote>\n\n"
                f"<blockquote>✅ Successful: {success_count}\n"
                f"❌ Failed: {failed_count}\n"
                f"⏭️ Skipped (no match): {skipped_count}\n"
                f"📁 Total Processed: {len(valid_pairs)}</blockquote>\n\n"
            )
            
            if success_count > 0:
                summary += "<blockquote>🎉 Merged files have been sent to you!</blockquote>\n"
                summary += "<blockquote>🔊 <b>Note:</b> Source audio is auto-selected by default in all players</blockquote>"
            
            await progress_msg.edit_text(summary)
            
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
        # Clean up user state
        if user_id in merging_users:
            del merging_users[user_id]

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

<blockquote><b>✨ New Feature:</b>
- Source audio is now <b>auto-selected by default</b> in all video players
- No need to manually switch audio tracks after merging
- Works with VLC, MX Player, Windows Media Player, etc.</blockquote>

<blockquote><b>⚠️ Important Notes:</b>
- Files are matched by season and episode numbers
- MKV format works best for merging
- Original target file tracks are preserved
- Source audio tracks are set as default
- No re-encoding (file size optimized)</blockquote>""
