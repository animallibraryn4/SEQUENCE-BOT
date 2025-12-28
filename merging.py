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

def get_file_extension(file_path: str) -> str:
    """Get file extension from path"""
    return Path(file_path).suffix.lower()

def detect_audio_language(file_path: str) -> str:
    """Detect audio language from file"""
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-show_entries', 'stream_tags=language', 
               '-select_streams', 'a', '-of', 'csv=p=0', file_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            # Get first audio language
            languages = result.stdout.strip().split('\n')
            return languages[0] if languages[0] else 'und'
    except Exception as e:
        print(f"Error detecting language: {e}")
    return 'und'

def merge_with_mkvmerge(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Merge using MKVToolNix - NO RE-ENCODE, PERFECT QUALITY
    Target audio: keep, NOT default
    Source audio: DEFAULT
    Subtitles: copy from both
    Video: copy (original quality)
    """
    try:
        print(f"\n=== Starting MKVToolNix Merge (NO RE-ENCODE) ===")
        print(f"Source: {source_path}")
        print(f"Target: {target_path}")
        print(f"Output: {output_path}")
        
        # Get languages for better tagging
        target_lang = detect_audio_language(target_path)
        source_lang = detect_audio_language(source_path)
        
        print(f"Target audio language: {target_lang}")
        print(f"Source audio language: {source_lang}")
        
        # Ensure output is .mkv for best compatibility
        if not output_path.lower().endswith('.mkv'):
            output_path = output_path.rsplit('.', 1)[0] + '.mkv'
            print(f"Changed output to: {output_path}")
        
        # Build the MKVToolNix command
        cmd = [
            "mkvmerge", "-o", output_path,

            # TARGET FILE (video + audio + subs)
            "--video-tracks", "0",
            "--audio-tracks", "0",
            "--default-track", "0:no",
            "--language", f"0:{target_lang}",
            "--subtitle-tracks", "all",
            target_path,

            # SOURCE FILE (audio default + subs)
            "--audio-tracks", "0",
            "--default-track", "0:yes",
            "--language", f"0:{source_lang}",
            "--subtitle-tracks", "all",
            source_path,
        ]
        
        print(f"\nMKVToolNix Command:")
        print(' '.join(cmd))
        print("\n")
        
        # Execute the command
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ MKVToolNix merge successful!")
            print(f"Output created: {output_path}")
            
            # Verify the result
            verify_cmd = ['mkvinfo', output_path, '--ui-language', 'en_US']
            verify_result = subprocess.run(verify_cmd, capture_output=True, text=True)
            
            if verify_result.returncode == 0:
                print("\n=== VERIFICATION ===")
                # Check for default track info
                for line in verify_result.stdout.split('\n'):
                    if 'Default track' in line:
                        print(f"Track info: {line.strip()}")
                
                # Count tracks
                audio_count = verify_result.stdout.count('Track type: audio')
                subtitle_count = verify_result.stdout.count('Track type: subtitles')
                print(f"Total audio tracks: {audio_count}")
                print(f"Total subtitle tracks: {subtitle_count}")
            return True
        else:
            print(f"❌ MKVToolNix failed: {result.stderr[:500]}")
            return False
            
    except Exception as e:
        print(f"❌ MKVToolNix error: {e}")
        import traceback
        traceback.print_exc()
        return False

def merge_audio_subtitles_simple(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Simple merge function - Uses MKVToolNix (NO RE-ENCODE) as primary
    Falls back to FFmpeg if MKVToolNix fails
    """
    try:
        print(f"\n=== Starting Simple Merge ===")
        
        # Get media info for logging
        target_info = get_media_info(target_path)
        source_info = get_media_info(source_path)
        
        target_streams = extract_streams_info(target_info)
        source_streams = extract_streams_info(source_info)
        
        print(f"Target has {len(target_streams['audio_streams'])} audio, {len(target_streams['subtitle_streams'])} subtitle streams")
        print(f"Source has {len(source_streams['audio_streams'])} audio, {len(source_streams['subtitle_streams'])} subtitle streams")
        
        # Check if we have anything to add
        if not source_streams["audio_streams"] and not source_streams["subtitle_streams"]:
            print("No audio or subtitles to add from source")
            return False
        
        # Check if files are MKV format (best for MKVToolNix)
        source_ext = get_file_extension(source_path)
        target_ext = get_file_extension(target_path)
        
        if source_ext == '.mkv' and target_ext == '.mkv':
            print("✅ Both files are MKV format - using MKVToolNix (NO RE-ENCODE)")
            return merge_with_mkvmerge(source_path, target_path, output_path)
        else:
            print(f"⚠️ Non-MKV format detected (Source: {source_ext}, Target: {target_ext})")
            print("Will still try MKVToolNix first...")
            if merge_with_mkvmerge(source_path, target_path, output_path):
                return True
            else:
                print("MKVToolNix failed, trying FFmpeg fallback...")
                # FFmpeg fallback (keeping your existing v2 logic)
                return fallback_ffmpeg_merge(source_path, target_path, output_path)
                
    except Exception as e:
        print(f"Error in simple merge: {e}")
        import traceback
        traceback.print_exc()
        # Final fallback to FFmpeg
        print("Trying FFmpeg fallback...")
        return fallback_ffmpeg_merge(source_path, target_path, output_path)

def fallback_ffmpeg_merge(source_path: str, target_path: str, output_path: str) -> bool:
    """
    FFmpeg fallback method when MKVToolNix fails
    """
    try:
        print("Using FFmpeg fallback method...")
        
        cmd = [
            "ffmpeg", "-y",
            "-i", target_path,
            "-i", source_path,
            
            # Stream mapping
            "-map", "0:v:0",       # Target video
            "-map", "0:a?",        # Target audio
            "-map", "1:a?",        # Source audio
            "-map", "0:s?",        # Target subtitles
            "-map", "1:s?",        # Source subtitles
            
            # Codec settings
            "-c:v", "copy",        # Copy video
            "-c:a", "aac",         # Re-encode audio for compatibility
            "-b:a", "192k",
            "-ac", "2",
            "-c:s", "copy",        # Copy subtitles
            
            # Default track settings
            "-disposition:a:0", "0",        # Target audio NOT default
            "-disposition:a:1", "default",  # Source audio DEFAULT
            
            # Sync fixes
            "-af", "aresample=async=1",
            "-fflags", "+genpts",
            
            output_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ FFmpeg fallback successful")
            return True
        else:
            print(f"❌ FFmpeg fallback failed: {result.stderr[:500]}")
            return False
            
    except Exception as e:
        print(f"❌ FFmpeg fallback error: {e}")
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
            "<blockquote><b>✨ NEW: MKVToolNix Engine (NO RE-ENCODE)</b></blockquote>\n\n"
            "<blockquote>Please send the SOURCE FILES from which you want to extract audio and subtitles.</blockquote>\n\n"
            "<blockquote><b>📝 Instructions:</b>\n"
            "1. Send all source files (with desired audio/subtitle tracks)\n"
            "2. Send <code>/done</code> when finished\n"
            "3. Send all target files (to add tracks to)\n"
            "4. Send <code>/done</code> again\n"
            "5. Wait for processing</blockquote>\n\n"
            "<blockquote><b>⚠️ Requirements:</b>\n"
            "- <b>MKV format works best</b> (NO re-encode, original quality preserved)\n"
            "- Files should have similar naming for auto-matching\n"
            "- Server needs MKVToolNix installed for best results</blockquote>\n\n"
            "<blockquote><b>🎯 Benefits:</b>\n"
            "- ✅ Zero quality loss (no video re-encode)\n"
            "- ✅ Source audio set as DEFAULT\n"
            "- ✅ MX Player, VLC, Android TV compatible\n"
            "- ✅ Fast processing</blockquote>"
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
        f"<blockquote>✨ <b>Using MKVToolNix Engine (NO RE-ENCODE)</b></blockquote>\n"
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
                        f"<blockquote>✨ <b>MKVToolNix Engine Active</b></blockquote>\n"
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
                    
                    # Output file path - keep original target filename but ensure .mkv extension
                    output_filename = target_data["filename"]
                    if not output_filename.lower().endswith('.mkv'):
                        output_filename = output_filename.rsplit('.', 1)[0] + '.mkv'
                    
                    output_file = str(temp_path / output_filename)
                    
                    print(f"\n{'='*50}")
                    print(f"Processing pair {idx}:")
                    print(f"  Source: {source_data['filename']}")
                    print(f"  Target: {target_data['filename']}")
                    print(f"  Output: {output_filename}")
                    print(f"{'='*50}\n")
                    
                    # Merge audio and subtitles using MKVToolNix
                    if merge_audio_subtitles_simple(source_file, target_file, output_file):
                        # Upload merged file
                        await client.send_document(
                            chat_id=user_id,
                            document=output_file,
                            caption=(
                                f"<blockquote>✅ <b>Merged File (NO RE-ENCODE)</b></blockquote>\n\n"
                                f"<blockquote>📁 {output_filename}</blockquote>\n"
                                f"<blockquote>🎵 <b>Source audio set as DEFAULT</b></blockquote>\n"
                                f"<blockquote>🎯 Zero quality loss (video copied)</blockquote>\n"
                                f"<blockquote>📝 All subtitle tracks preserved</blockquote>\n"
                                f"<blockquote>📱 MX Player, VLC, Android TV compatible</blockquote>"
                            )
                        )
                        success_count += 1
                        print(f"✅ Successfully merged file {idx}")
                    else:
                        failed_count += 1
                        await client.send_message(
                            user_id,
                            f"<blockquote>❌ Failed to merge: {target_data['filename']}</blockquote>\n"
                            f"<blockquote><i>This file may be incompatible or corrupted.</i></blockquote>"
                        )
                        print(f"❌ Failed to merge file {idx}")
                    
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
                f"<blockquote>✨ <b>MKVToolNix Results:</b></blockquote>\n"
                f"<blockquote>✅ Successful: {success_count}\n"
                f"❌ Failed: {failed_count}\n"
                f"⏭️ Skipped (no match): {skipped_count}\n"
                f"📁 Total Processed: {len(valid_pairs)}</blockquote>\n\n"
            )
            
            if success_count > 0:
                summary += (
                    "<blockquote>🎉 Merged files have been sent to you!</blockquote>\n\n"
                    "<blockquote>🔑 Key Features:</blockquote>\n"
                    "<blockquote>• ✅ Zero quality loss (video copied)</blockquote>\n"
                    "<blockquote>• ✅ Source audio = DEFAULT track</blockquote>\n"
                    "<blockquote>• ✅ All subtitles preserved</blockquote>\n"
                    "<blockquote>• ✅ MX Player, VLC, Android TV compatible</blockquote>"
                )
            
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
       
