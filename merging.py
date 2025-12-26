import os
import re
import asyncio
import tempfile
import subprocess
import json
from typing import Dict, List, Tuple, Optional

# Temporary storage for user states
user_merging_state = {}  # {user_id: {"step": 1/2/3, "source_files": [], "target_files": [], "current_mode": "file"/"caption"}}

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

def get_media_info(file_path: str) -> Dict:
    """Get detailed media information using ffprobe"""
    cmd = [
        'ffprobe',
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_streams',
        '-show_format',
        file_path
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return json.loads(result.stdout)
    except Exception as e:
        print(f"FFprobe error for {file_path}: {e}")
    return {}

# FFmpeg helper functions
async def extract_audio_and_subtitles(file_path: str, output_dir: str) -> Tuple[List[str], List[str]]:
    """
    Extract audio and subtitle tracks from video file using FFmpeg.
    Returns: (audio_files_list, subtitle_files_list)
    """
    audio_files = []
    subtitle_files = []
    
    try:
        # Get media info first
        media_info = get_media_info(file_path)
        if not media_info:
            return audio_files, subtitle_files
        
        streams = media_info.get('streams', [])
        
        # Find audio streams
        audio_streams = [i for i, stream in enumerate(streams) 
                        if stream.get('codec_type') == 'audio']
        
        # Find subtitle streams
        subtitle_streams = [i for i, stream in enumerate(streams) 
                          if stream.get('codec_type') == 'subtitle']
        
        # Extract audio tracks
        for idx, stream_idx in enumerate(audio_streams):
            audio_output = os.path.join(output_dir, f'audio_{idx}.m4a')
            audio_cmd = [
                'ffmpeg',
                '-i', file_path,
                '-map', f'0:{stream_idx}',
                '-c:a', 'aac',
                '-b:a', '192k',
                '-ac', '2',  # Convert to stereo
                audio_output,
                '-y',
                '-hide_banner',
                '-loglevel', 'error'
            ]
            
            try:
                result = subprocess.run(audio_cmd, capture_output=True, text=True, timeout=120)
                if result.returncode == 0 and os.path.exists(audio_output):
                    audio_files.append(audio_output)
                    print(f"Extracted audio track {idx} to {audio_output}")
            except Exception as e:
                print(f"Error extracting audio track {idx}: {e}")
        
        # Extract subtitle tracks
        for idx, stream_idx in enumerate(subtitle_streams):
            subtitle_output = os.path.join(output_dir, f'subtitle_{idx}.srt')
            subtitle_cmd = [
                'ffmpeg',
                '-i', file_path,
                '-map', f'0:{stream_idx}',
                subtitle_output,
                '-y',
                '-hide_banner',
                '-loglevel', 'error'
            ]
            
            try:
                result = subprocess.run(subtitle_cmd, capture_output=True, text=True, timeout=120)
                if result.returncode == 0 and os.path.exists(subtitle_output):
                    subtitle_files.append(subtitle_output)
                    print(f"Extracted subtitle track {idx} to {subtitle_output}")
            except Exception as e:
                print(f"Error extracting subtitle track {idx}: {e}")
                
    except Exception as e:
        print(f"Error extracting tracks from {file_path}: {e}")
    
    return audio_files, subtitle_files

async def merge_tracks_into_file(source_tracks: Dict, target_file: str, output_file: str) -> bool:
    """
    Merge extracted audio and subtitle tracks into target file.
    """
    try:
        # Get target file info
        target_info = get_media_info(target_file)
        if not target_info:
            print(f"Cannot get media info for {target_file}")
            return False
        
        target_streams = target_info.get('streams', [])
        
        # Build ffmpeg command
        cmd = ['ffmpeg']
        
        # Add target file as input
        cmd.extend(['-i', target_file])
        
        # Add audio inputs
        audio_files = source_tracks.get('audio', [])
        for audio_file in audio_files:
            if os.path.exists(audio_file):
                cmd.extend(['-i', audio_file])
        
        # Add subtitle inputs
        subtitle_files = source_tracks.get('subtitles', [])
        for subtitle_file in subtitle_files:
            if os.path.exists(subtitle_file):
                cmd.extend(['-i', subtitle_file])
        
        # Start mapping
        map_cmds = []
        
        # Map video stream (always first)
        map_cmds.extend(['-map', '0:v'])
        
        # Map all original audio streams
        audio_count = sum(1 for s in target_streams if s.get('codec_type') == 'audio')
        for i in range(audio_count):
            map_cmds.extend(['-map', f'0:a:{i}'])
        
        # Map extracted audio streams
        for i in range(len(audio_files)):
            map_cmds.extend(['-map', f'{i+1}:a'])
        
        # Map all original subtitle streams
        subtitle_count = sum(1 for s in target_streams if s.get('codec_type') == 'subtitle')
        for i in range(subtitle_count):
            map_cmds.extend(['-map', f'0:s:{i}'])
        
        # Map extracted subtitle streams
        offset = len(audio_files) + 1
        for i in range(len(subtitle_files)):
            map_cmds.extend(['-map', f'{offset + i}:s'])
        
        # Add mapping commands
        cmd.extend(map_cmds)
        
        # Codec settings
        # Copy video codec
        cmd.extend(['-c:v', 'copy'])
        
        # Copy original audio codecs
        for i in range(audio_count):
            cmd.extend([f'-c:a:{i}', 'copy'])
        
        # Convert extracted audio to AAC
        for i in range(len(audio_files)):
            cmd.extend([f'-c:a:{audio_count + i}', 'aac'])
            cmd.extend([f'-b:a:{audio_count + i}', '192k'])
        
        # Copy subtitle codecs
        cmd.extend(['-c:s', 'copy'])
        
        # Set default streams
        cmd.extend(['-disposition:a', '0'])
        if audio_count > 0:
            cmd.extend(['-disposition:a:0', 'default'])
        
        # Metadata
        cmd.extend(['-metadata', 'title=Merged with Sequence Bot'])
        
        # Avoid issues
        cmd.extend(['-max_interleave_delta', '0'])
        
        # Output
        cmd.append(output_file)
        cmd.append('-y')
        cmd.extend(['-hide_banner', '-loglevel', 'warning'])
        
        print(f"Running merge command for {target_file}")
        
        # Run ffmpeg
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode != 0:
            print(f"FFmpeg error: {result.stderr}")
            return False
        
        # Verify output
        if os.path.exists(output_file):
            output_info = get_media_info(output_file)
            if output_info:
                output_streams = output_info.get('streams', [])
                print(f"Output streams: {[(s.get('codec_type'), s.get('codec_name')) for s in output_streams]}")
                return True
        
        return False
            
    except subprocess.TimeoutExpired:
        print(f"Timeout merging {target_file}")
        return False
    except Exception as e:
        print(f"Error merging {target_file}: {e}")
        import traceback
        traceback.print_exc()
        return False

async def download_file(client, message, download_path: str) -> Optional[str]:
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
            file_name = f"file_{message.id}.mkv"
        
        # Clean filename
        import string
        valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
        file_name = ''.join(c for c in file_name if c in valid_chars)
        
        full_path = os.path.join(download_path, file_name)
        
        # Download the file
        await client.download_media(message, file_name=full_path)
        
        # Check if file was downloaded
        if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
            return full_path
        else:
            print(f"Downloaded file is empty or doesn't exist: {full_path}")
            return None
            
    except Exception as e:
        print(f"Error downloading file: {e}")
        import traceback
        traceback.print_exc()
        return None

async def process_merging_files(client, user_id: int, chat_id: int):
    """Process merging operation for a user"""
    if user_id not in user_merging_state:
        return
    
    state = user_merging_state[user_id]
    source_files = state.get("source_files", [])
    target_files = state.get("target_files", [])
    extracted_tracks_data = state.get("extracted_tracks", {})
    
    if not source_files or not target_files:
        await client.send_message(chat_id, "<blockquote>❌ No files to process.</blockquote>")
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
        
        for idx, target_file_data in enumerate(target_files, 1):
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
                    
                    if target_path and os.path.exists(target_path):
                        # Create output file name
                        original_name = os.path.basename(target_path)
                        name_without_ext = os.path.splitext(original_name)[0]
                        output_name = f"{name_without_ext}_merged.mp4"
                        output_path = os.path.join(temp_dir, output_name)
                        
                        print(f"\n{'='*50}")
                        print(f"Processing file {idx}/{len(target_files)}: {original_name}")
                        print(f"Matching Season {target_season}, Episode {target_episode}")
                        print(f"Source audio tracks: {len(source_tracks.get('audio', []))}")
                        print(f"Source subtitle tracks: {len(source_tracks.get('subtitles', []))}")
                        
                        # Get target file info
                        target_media_info = get_media_info(target_path)
                        if target_media_info:
                            target_streams = target_media_info.get('streams', [])
                            audio_count = sum(1 for s in target_streams if s.get('codec_type') == 'audio')
                            sub_count = sum(1 for s in target_streams if s.get('codec_type') == 'subtitle')
                            print(f"Target has {audio_count} audio and {sub_count} subtitle tracks")
                        
                        # Merge tracks
                        success = await merge_tracks_into_file(
                            source_tracks,
                            target_path,
                            output_path
                        )
                        
                        if success and os.path.exists(output_path):
                            # Verify output
                            output_info = get_media_info(output_path)
                            if output_info:
                                output_streams = output_info.get('streams', [])
                                output_audio = sum(1 for s in output_streams if s.get('codec_type') == 'audio')
                                output_subs = sum(1 for s in output_streams if s.get('codec_type') == 'subtitle')
                                print(f"Output has {output_audio} audio and {output_subs} subtitle tracks")
                            
                            # Send merged file back to user
                            await client.send_document(
                                chat_id,
                                document=output_path,
                                caption=f"✅ Merged: {original_name}\nAdded {len(source_tracks.get('audio', []))} audio track(s) and {len(source_tracks.get('subtitles', []))} subtitle track(s)"
                            )
                            processed_count += 1
                        else:
                            failed_count += 1
                            await client.send_message(
                                chat_id,
                                f"<blockquote>❌ Failed to merge file {idx}: {original_name}</blockquote>"
                            )
                    else:
                        failed_count += 1
                        await client.send_message(
                            chat_id,
                            f"<blockquote>❌ Failed to download target file {idx}</blockquote>"
                        )
                else:
                    failed_count += 1
                    await client.send_message(
                        chat_id,
                        f"<blockquote>❌ No matching tracks found for Season {target_season} Episode {target_episode}</blockquote>"
                    )
                
                # Update status
                current = processed_count + failed_count
                await status_msg.edit_text(
                    f"<blockquote>🔄 Processing {len(target_files)} files...\n"
                    f"Progress: {current}/{len(target_files)}\n"
                    f"✅ Success: {processed_count} | ❌ Failed: {failed_count}</blockquote>"
                )
                
                # Delay to avoid flooding
                await asyncio.sleep(2)
                
            except Exception as e:
                print(f"Error processing file {idx}: {e}")
                import traceback
                traceback.print_exc()
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
async def merging_command(client, message):
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
    
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    await message.reply_text(
        "<blockquote><b>🔀 MERGING MODE ACTIVATED</b></blockquote>\n\n"
        "<blockquote><b>Step 1/2:</b> Send the <b>SOURCE FILES</b> from which you want to extract audio and subtitles.\n\n"
        "ℹ️ <i>Send all source files first, then click the button below.</i></blockquote>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Done with Source Files", callback_data=f"merging_done_source_{user_id}")],
            [InlineKeyboardButton("❌ Cancel Merging", callback_data=f"merging_cancel_{user_id}")]
        ])
    )

# File handler for merging
async def handle_merging_files(client, message):
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
