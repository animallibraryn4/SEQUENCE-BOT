import os
import re
import shutil
import asyncio
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
import subprocess
from datetime import datetime

# Database imports
from database import users_collection, update_user_stats

# Temporary storage for user states
user_merging_state = {}  # {user_id: {"step": 1/2/3, "source_files": [], "target_files": [], "current_mode": "file"/"caption"}}
extracted_tracks = {}    # {user_id: {"season": {"episode": {"audio": [], "subtitles": []}}}}

# Parse file info (reusing from sequence.py)
def parse_file_info(text: str) -> Dict:
    """Parse file information from text (either filename or caption)"""
    quality_match = re.search(r'(\d{3,4})[pP]', text)
    quality = int(quality_match.group(1)) if quality_match else 0
    clean_name = re.sub(r'\d{3,4}[pP]', '', text)

    season_match = re.search(r'[sS](?:eason)?\s*(\d+)', clean_name)
    season = int(season_match.group(1)) if season_match else 1
    
    ep_match = re.search(r'[eE](?:p(?:isode)?)?\s*(\d+)', clean_name)
    if ep_match:
        episode = int(ep_match.group(1))
    else:
        nums = re.findall(r'\d+', clean_name)
        episode = int(nums[-1]) if nums else 0

    return {"season": season, "episode": episode, "quality": quality}

# FFmpeg helper functions
async def extract_audio_and_subtitles(file_path: str, output_dir: str) -> Tuple[List[str], List[str]]:
    """
    Extract audio and subtitle tracks from video file using FFmpeg.
    Returns: (audio_files_list, subtitle_files_list)
    """
    audio_files = []
    subtitle_files = []
    
    try:
        # First, probe the file to see what tracks are available
        probe_cmd = [
            'ffprobe',
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_streams',
            '-show_format',
            file_path
        ]
        
        result = subprocess.run(probe_cmd, capture_output=True, text=True)
        
        # For now, we'll use a simpler approach
        # Extract audio tracks
        audio_extract_cmd = [
            'ffmpeg',
            '-i', file_path,
            '-map', '0:a:0',  # First audio track
            '-c', 'copy',
            os.path.join(output_dir, 'audio.mka'),
            '-y'
        ]
        
        try:
            subprocess.run(audio_extract_cmd, capture_output=True, check=True)
            audio_files.append(os.path.join(output_dir, 'audio.mka'))
        except subprocess.CalledProcessError:
            pass  # No audio track
        
        # Extract subtitles
        subtitle_extract_cmd = [
            'ffmpeg',
            '-i', file_path,
            '-map', '0:s:0',  # First subtitle track
            '-c', 'copy',
            os.path.join(output_dir, 'subtitles.ass'),
            '-y'
        ]
        
        try:
            subprocess.run(subtitle_extract_cmd, capture_output=True, check=True)
            subtitle_files.append(os.path.join(output_dir, 'subtitles.ass'))
        except subprocess.CalledProcessError:
            pass  # No subtitle track
            
    except Exception as e:
        print(f"Error extracting tracks: {e}")
    
    return audio_files, subtitle_files

async def merge_tracks_into_file(source_tracks: Dict, target_file: str, output_file: str) -> bool:
    """
    Merge extracted audio and subtitle tracks into target file without re-encoding.
    """
    try:
        # Build ffmpeg command
        cmd = ['ffmpeg', '-i', target_file]
        
        # Add audio tracks if available
        audio_files = source_tracks.get('audio', [])
        for audio_file in audio_files:
            cmd.extend(['-i', audio_file])
        
        # Add subtitle tracks if available
        subtitle_files = source_tracks.get('subtitles', [])
        for subtitle_file in subtitle_files:
            cmd.extend(['-i', subtitle_file])
        
        # Map streams
        cmd.extend(['-map', '0:v'])  # Keep original video
        
        # Map original audio tracks
        cmd.extend(['-map', '0:a'])
        
        # Map extracted audio tracks
        for i in range(len(audio_files)):
            cmd.extend(['-map', f'{i+1}:a'])
        
        # Map original subtitle tracks
        cmd.extend(['-map', '0:s?'])  # Optional original subtitles
        
        # Map extracted subtitle tracks
        for i in range(len(audio_files), len(audio_files) + len(subtitle_files)):
            cmd.extend(['-map', f'{i+1}:s'])
        
        # Copy all codecs (no re-encoding)
        cmd.extend(['-c', 'copy'])
        
        # Output file
        cmd.append(output_file)
        cmd.append('-y')
        
        # Run ffmpeg
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            return True
        else:
            print(f"FFmpeg error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Error merging tracks: {e}")
        return False

async def download_file(client: Client, message: Message, download_path: str) -> Optional[str]:
    """Download a file from Telegram message"""
    try:
        file_name = ""
        if message.document:
            file_name = message.document.file_name
        elif message.video:
            file_name = message.video.file_name or f"video_{message.id}.mp4"
        elif message.audio:
            file_name = message.audio.file_name or f"audio_{message.id}.mp3"
        
        if not file_name:
            file_name = f"file_{message.id}.bin"
        
        full_path = os.path.join(download_path, file_name)
        
        # Download the file
        await client.download_media(message, file_name=full_path)
        
        return full_path
    except Exception as e:
        print(f"Error downloading file: {e}")
        return None

async def process_merging_files(client: Client, user_id: int, chat_id: int):
    """Process merging operation for a user"""
    if user_id not in user_merging_state:
        return
    
    state = user_merging_state[user_id]
    source_files = state.get("source_files", [])
    target_files = state.get("target_files", [])
    extracted_tracks_data = state.get("extracted_tracks", {})
    
    if not source_files or not target_files:
        return
    
    # Create temp directory for processing
    with tempfile.TemporaryDirectory() as temp_dir:
        processed_count = 0
        failed_count = 0
        
        # Send processing message
        status_msg = await client.send_message(
            chat_id,
            f"<blockquote>🔄 Processing {len(target_files)} files...\n"
            f"Progress: 0/{len(target_files)}</blockquote>"
        )
        
        for target_file_data in target_files:
            try:
                # Parse target file info
                target_info = parse_file_info(target_file_data.get("filename", ""))
                target_season = target_info["season"]
                target_episode = target_info["episode"]
                
                # Find matching source tracks
                source_tracks = None
                if str(target_season) in extracted_tracks_data:
                    season_data = extracted_tracks_data[str(target_season)]
                    if str(target_episode) in season_data:
                        source_tracks = season_data[str(target_episode)]
                
                if source_tracks and (source_tracks.get("audio") or source_tracks.get("subtitles")):
                    # Download target file
                    target_path = await download_file(
                        client,
                        target_file_data["message"],
                        temp_dir
                    )
                    
                    if target_path:
                        # Create output file name
                        original_name = os.path.basename(target_path)
                        output_name = f"merged_{original_name}"
                        output_path = os.path.join(temp_dir, output_name)
                        
                        # Merge tracks
                        success = await merge_tracks_into_file(
                            source_tracks,
                            target_path,
                            output_path
                        )
                        
                        if success and os.path.exists(output_path):
                            # Send merged file back to user
                            await client.send_document(
                                chat_id,
                                document=output_path,
                                caption=f"✅ Merged: {original_name}"
                            )
                            processed_count += 1
                        else:
                            failed_count += 1
                            
                            # Send original file if merging failed
                            await client.send_document(
                                chat_id,
                                document=target_path,
                                caption=f"⚠️ Original (merge failed): {original_name}"
                            )
                    else:
                        failed_count += 1
                else:
                    # No matching tracks found, send original file
                    target_path = await download_file(
                        client,
                        target_file_data["message"],
                        temp_dir
                    )
                    
                    if target_path:
                        await client.send_document(
                            chat_id,
                            document=target_path,
                            caption=f"ℹ️ No matching tracks: {os.path.basename(target_path)}"
                        )
                    failed_count += 1
                
                # Update status
                if (processed_count + failed_count) % 2 == 0:  # Update every 2 files
                    await status_msg.edit_text(
                        f"<blockquote>🔄 Processing {len(target_files)} files...\n"
                        f"Progress: {processed_count + failed_count}/{len(target_files)}\n"
                        f"✅ Success: {processed_count} | ❌ Failed: {failed_count}</blockquote>"
                    )
                
                # Small delay to avoid flooding
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"Error processing file: {e}")
                failed_count += 1
                continue
        
        # Final status
        await status_msg.edit_text(
            f"<blockquote><b>✅ Merging Complete!</b></blockquote>\n"
            f"<blockquote>Total files: {len(target_files)}\n"
            f"✅ Successfully merged: {processed_count}\n"
            f"❌ Failed/skipped: {failed_count}</blockquote>"
        )
        
        # Cleanup
        if user_id in user_merging_state:
            del user_merging_state[user_id]

# Command handler
async def merging_command(client: Client, message: Message):
    """Handle /merging command"""
    user_id = message.from_user.id
    
    # Initialize merging state
    user_merging_state[user_id] = {
        "step": 1,  # 1 = waiting for source files, 2 = waiting for target files
        "source_files": [],
        "target_files": [],
        "extracted_tracks": {},
        "temp_dir": None
    }
    
    await message.reply_text(
        "<blockquote><b>🔀 MERGING MODE ACTIVATED</b></blockquote>\n\n"
        "<blockquote>Please send the <b>SOURCE FILES</b> from which you want to extract audio and subtitles.\n\n"
        "After sending all source files, click the button below to proceed.</blockquote>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Done with Source Files", callback_data=f"merging_done_source_{user_id}")],
            [InlineKeyboardButton("❌ Cancel Merging", callback_data=f"merging_cancel_{user_id}")]
        ])
    )

# File handler for merging
async def handle_merging_files(client: Client, message: Message):
    """Handle files sent during merging mode"""
    user_id = message.from_user.id
    
    if user_id not in user_merging_state:
        return
    
    state = user_merging_state[user_id]
    step = state["step"]
    
    # Check if it's a media file
    if not (message.document or message.video or message.audio):
        return
    
    # Parse file info from filename or caption
    if message.caption:
        text_to_parse = message.caption
    else:
        file_obj = message.document or message.video or message.audio
        file_name = file_obj.file_name if file_obj else f"file_{message.id}"
        text_to_parse = file_name
    
    file_info = parse_file_info(text_to_parse)
    
    if step == 1:
        # Source files
        state["source_files"].append({
            "message": message,
            "filename": text_to_parse,
            "info": file_info
        })
        
        count = len(state["source_files"])
        await message.reply_text(
            f"<blockquote>✅ Source file #{count} received.\n"
            f"Season {file_info['season']}, Episode {file_info['episode']}</blockquote>"
        )
        
    elif step == 2:
        # Target files
        state["target_files"].append({
            "message": message,
            "filename": text_to_parse,
            "info": file_info
        })
        
        count = len(state["target_files"])
        await message.reply_text(
            f"<blockquote>✅ Target file #{count} received.\n"
            f"Season {file_info['season']}, Episode {file_info['episode']}</blockquote>"
        )

# Callback handlers
async def merging_callback_handler(client: Client, query, data: str):
    """Handle merging callbacks"""
    user_id = query.from_user.id
    
    if data.startswith("merging_done_source_"):
        target_user_id = int(data.split("_")[3])
        
        if user_id != target_user_id:
            await query.answer("This is not for you!", show_alert=True)
            return
        
        if target_user_id not in user_merging_state:
            await query.answer("Session expired!", show_alert=True)
            return
        
        state = user_merging_state[target_user_id]
        
        if not state["source_files"]:
            await query.answer("No source files received!", show_alert=True)
            return
        
        # Move to step 2
        state["step"] = 2
        
        await query.message.edit_text(
            "<blockquote><b>✅ Source files received!</b></blockquote>\n\n"
            "<blockquote>Now please send the <b>TARGET FILES</b> to which you want to add the extracted audio and subtitles.\n\n"
            "The bot will match files by season and episode number.\n\n"
            "After sending all target files, click the button below.</blockquote>",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Done with Target Files", callback_data=f"merging_done_target_{target_user_id}")],
                [InlineKeyboardButton("❌ Cancel Merging", callback_data=f"merging_cancel_{target_user_id}")]
            ])
        )
        await query.answer()
        
    elif data.startswith("merging_done_target_"):
        target_user_id = int(data.split("_")[3])
        
        if user_id != target_user_id:
            await query.answer("This is not for you!", show_alert=True)
            return
        
        if target_user_id not in user_merging_state:
            await query.answer("Session expired!", show_alert=True)
            return
        
        state = user_merging_state[target_user_id]
        
        if not state["target_files"]:
            await query.answer("No target files received!", show_alert=True)
            return
        
        # Extract tracks from source files
        await query.message.edit_text(
            "<blockquote>⏳ Extracting audio and subtitles from source files...\n"
            "This may take a moment.</blockquote>"
        )
        
        # Process in temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            source_tracks_dir = os.path.join(temp_dir, "source_tracks")
            os.makedirs(source_tracks_dir, exist_ok=True)
            
            extracted_data = {}
            
            for source_file_data in state["source_files"]:
                try:
                    # Download source file
                    source_path = await download_file(
                        client,
                        source_file_data["message"],
                        source_tracks_dir
                    )
                    
                    if source_path:
                        # Extract tracks
                        file_tracks_dir = os.path.join(temp_dir, f"tracks_{os.path.basename(source_path)}")
                        os.makedirs(file_tracks_dir, exist_ok=True)
                        
                        audio_files, subtitle_files = await extract_audio_and_subtitles(
                            source_path,
                            file_tracks_dir
                        )
                        
                        # Store extracted tracks by season/episode
                        season = source_file_data["info"]["season"]
                        episode = source_file_data["info"]["episode"]
                        
                        if str(season) not in extracted_data:
                            extracted_data[str(season)] = {}
                        
                        extracted_data[str(season)][str(episode)] = {
                            "audio": audio_files,
                            "subtitles": subtitle_files
                        }
                        
                except Exception as e:
                    print(f"Error processing source file: {e}")
                    continue
            
            # Store extracted tracks
            state["extracted_tracks"] = extracted_data
            
            # Start merging process
            await query.message.edit_text(
                "<blockquote>✅ Tracks extracted successfully!</blockquote>\n\n"
                "<blockquote>Starting to merge tracks into target files...</blockquote>"
            )
            
            # Start the merging process
            await process_merging_files(client, target_user_id, query.message.chat.id)
        
    elif data.startswith("merging_cancel_"):
        target_user_id = int(data.split("_")[2])
        
        if user_id != target_user_id:
            await query.answer("This is not for you!", show_alert=True)
            return
        
        # Cleanup
        if target_user_id in user_merging_state:
            # Clean temp directories
            state = user_merging_state[target_user_id]
            if state.get("temp_dir") and os.path.exists(state["temp_dir"]):
                try:
                    shutil.rmtree(state["temp_dir"])
                except:
                    pass
            
            del user_merging_state[target_user_id]
        
        await query.message.edit_text("<blockquote>❌ Merging cancelled.</blockquote>")
        await query.answer("Merging cancelled.")
