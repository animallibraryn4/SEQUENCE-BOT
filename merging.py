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

def analyze_audio_streams(file_path: str) -> Dict:
    """Analyze audio streams for compatibility"""
    info = get_media_info(file_path)
    audio_streams = []
    
    for stream in info.get("streams", []):
        if stream.get("codec_type") == "audio":
            audio_info = {
                "index": stream.get("index"),
                "codec": stream.get("codec_name"),
                "sample_rate": stream.get("sample_rate"),
                "channels": stream.get("channels"),
                "duration": stream.get("duration"),
                "start_time": stream.get("start_time"),
                "language": stream.get("tags", {}).get("language", "und"),
                "delay": stream.get("tags", {}).get("delay", "0"),
                "title": stream.get("tags", {}).get("title", "")
            }
            audio_streams.append(audio_info)
    
    return {
        "has_audio": len(audio_streams) > 0,
        "audio_streams": audio_streams,
        "main_audio": audio_streams[0] if audio_streams else None
    }

def normalize_audio(file_path: str) -> bool:
    """Normalize audio levels to prevent silent audio"""
    try:
        temp_file = file_path + ".temp.mkv"
        
        cmd = [
            "ffmpeg", "-y",
            "-i", file_path,
            "-c:v", "copy",
            "-c:a", "aac",
            "-b:a", "192k",
            "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",  # Normalize audio
            "-c:s", "copy",
            temp_file
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        
        if result.returncode == 0 and os.path.exists(temp_file):
            os.replace(temp_file, file_path)
            print("Audio normalized successfully")
            return True
        return False
    except:
        return False

def merge_audio_subtitles_v2(source_path: str, target_path: str, output_path: str) -> bool:
    try:
        # First, analyze both files to get audio delays and formats
        source_info = get_media_info(source_path)
        target_info = get_media_info(target_path)
        
        # Extract audio delay information
        source_delay = 0
        target_delay = 0
        
        for stream in source_info.get("streams", []):
            if stream.get("codec_type") == "audio":
                delay_tag = stream.get("tags", {}).get("delay", "0")
                try:
                    source_delay = int(delay_tag)
                except:
                    pass
                break
        
        for stream in target_info.get("streams", []):
            if stream.get("codec_type") == "audio":
                delay_tag = stream.get("tags", {}).get("delay", "0")
                try:
                    target_delay = int(delay_tag)
                except:
                    pass
                break
        
        # Calculate delay difference in seconds (convert from milliseconds if needed)
        delay_diff = (target_delay - source_delay) / 1000000.0  # Convert to seconds
        
        # Build FFmpeg command with proper sync
        cmd = [
            "ffmpeg", "-y",
            "-i", target_path,     # input 0 (Target Video)
            "-i", source_path,     # input 1 (Source Audio/Subs)
            
            # Map streams properly
            "-map", "0:v:0",       # Target video
            "-map", "1:a?",        # Source audio first (priority)
            "-map", "0:a?",        # Target audio (original, optional)
            "-map", "1:s?",        # Source subtitles
            "-map", "0:s?",        # Target subtitles
            
            # Video: copy without re-encoding
            "-c:v", "copy",
            
            # Audio: ensure proper format and sync
            "-c:a", "aac",         # Convert to AAC for compatibility
            "-b:a", "192k",        # Good quality bitrate
            "-ar", "48000",        # Standard sample rate
            "-ac", "2",            # Stereo for compatibility
            
            # CRITICAL: Audio sync fixes
            "-async", "1",         # Force audio resync
            "-copyts",             # Copy timestamps
            "-muxdelay", "0",      # Remove muxing delay
            
            # Apply delay compensation if needed
            *(["-itsoffset", str(delay_diff), "-i", source_path, "-map", "2:a?"] if abs(delay_diff) > 0.01 else []),
            
            # Subtitles: copy
            "-c:s", "copy",
            
            # Metadata and disposition
            "-metadata:s:a:0", "title=Merged Audio (Source)",
            "-disposition:a:0", "default",
            "-metadata:s:a:1", "title=Original Audio (Target)",
            "-disposition:a:1", "0",
            "-map_metadata", "0",
            
            # Output
            output_path
        ]

        print(f"FFmpeg command: {' '.join(cmd[:30])}...")
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print("FFmpeg error:", result.stderr[:500])
            
            # Fallback to simpler method
            return merge_audio_subtitles_simple(source_path, target_path, output_path)
        
        # Verify the output file
        if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
            print(f"Merge successful: {output_path}")
            return True
        else:
            print("Output file not created or too small")
            return False

    except subprocess.TimeoutExpired:
        print("FFmpeg timeout - using fallback method")
        return merge_audio_subtitles_simple(source_path, target_path, output_path)
    except Exception as e:
        print("Merge failed:", str(e))
        return merge_audio_subtitles_simple(source_path, target_path, output_path)
        

def get_file_extension(file_path: str) -> str:
    """Get file extension from path"""
    return Path(file_path).suffix.lower()

def merge_audio_subtitles_simple(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Improved version with better audio handling
    """
    try:
        # First analyze audio streams
        source_audio = analyze_audio_streams(source_path)
        target_audio = analyze_audio_streams(target_path)
        
        if not source_audio["has_audio"]:
            print("No audio in source file - cannot merge")
            return False
        
        # MKVMERGE approach with audio normalization
        mkvmerge_cmd = [
            "mkvmerge",
            "-o", output_path,
            
            # Target file
            "--no-audio" if source_audio["has_audio"] else "",  # Remove target audio if we have source audio
            target_path,
            
            # Source file: take only audio and subs
            "--no-video",
            "--audio-tracks", "0",  # Take first audio track
            source_path
        ]
        
        # Filter out empty strings
        mkvmerge_cmd = [x for x in mkvmerge_cmd if x]
        
        print(f"Running mkvmerge: {' '.join(mkvmerge_cmd)}")
        result = subprocess.run(mkvmerge_cmd, capture_output=True, text=True, timeout=300)

        if result.returncode == 0 and os.path.exists(output_path):
            print("Merge successful with mkvmerge")
            
            # Optional: Normalize audio levels
            normalize_audio(output_path)
            return True
        else:
            print(f"mkvmerge failed: {result.stderr[:200]}")
            return merge_audio_subtitles_v2(source_path, target_path, output_path)

    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        print(f"mkvmerge error: {e}")
        return merge_audio_subtitles_v2(source_path, target_path, output_path)
    except Exception as e:
        print(f"Unexpected error: {e}")
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
                                f"<blockquote>📝 Subtitle tracks added from source</blockquote>"
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
                summary += "<blockquote>🎉 Merged files have been sent to you!</blockquote>"
            
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

<blockquote><b>⚠️ Important Notes:</b>
- Files are matched by season and episode numbers
- MKV format works best for merging
- Original target file tracks are preserved
- Only new audio/subtitle tracks are added from source
- No re-encoding (file size optimized)</blockquote>"""
 
