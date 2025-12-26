
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
from start import is_subscribed  # Fixed import

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
    """Parse season and episode information from filename"""
    # Clean the filename
    filename = filename.lower().replace('_', ' ')
    
    # Try different patterns - using raw strings for regex
    patterns = [
        r's(\d+)\s*e(\d+)',  # S01E01
        r'season\s*(\d+)\s*episode\s*(\d+)',  # Season 1 Episode 1
        r'(\d+)x(\d+)',  # 1x01
        r'ep\s*(\d+)',  # EP 01
        r'\s(\d{2,3})\s'  # Space separated numbers like " 01 "
    ]
    
    season = 1  # Default season
    episode = 0
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            if pattern == r's(\d+)\s*e(\d+)':
                season = int(match.group(1))
                episode = int(match.group(2))
                break
            elif pattern == r'season\s*(\d+)\s*episode\s*(\d+)':
                season = int(match.group(1))
                episode = int(match.group(2))
                break
            elif pattern == r'(\d+)x(\d+)':
                season = int(match.group(1))
                episode = int(match.group(2))
                break
            elif pattern == r'ep\s*(\d+)':
                episode = int(match.group(1))
                break
            elif pattern == r'\s(\d{2,3})\s':
                episode = int(match.group(1))
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
    
    return matched_pairs

# --- FFMPEG UTILITIES ---
def get_stream_info(file_path: str) -> Dict:
    """Get audio and subtitle stream information from file"""
    cmd = [
        'ffprobe',
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_streams',
        '-select_streams', 'a,s',
        file_path
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            return {
                'audio_streams': [s for s in data.get('streams', []) if s.get('codec_type') == 'audio'],
                'subtitle_streams': [s for s in data.get('streams', []) if s.get('codec_type') == 'subtitle']
            }
    except:
        pass
    
    return {'audio_streams': [], 'subtitle_streams': []}

def merge_audio_subtitles(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Merge audio and subtitle tracks from source to target without re-encoding
    Returns True if successful, False otherwise
    """
    try:
        # Get stream info
        source_info = get_stream_info(source_path)
        target_info = get_stream_info(target_path)
        
        # Build ffmpeg command
        cmd = ['ffmpeg', '-y']
        
        # Input files
        cmd.extend(['-i', target_path])
        cmd.extend(['-i', source_path])
        
        # Map all streams from first input (target)
        cmd.extend(['-map', '0'])
        
        # Map audio streams from second input (source)
        source_audio_count = len(source_info.get('audio_streams', []))
        for i in range(source_audio_count):
            cmd.extend(['-map', f'1:a:{i}'])
        
        # Map subtitle streams from second input (source)
        source_sub_count = len(source_info.get('subtitle_streams', []))
        for i in range(source_sub_count):
            cmd.extend(['-map', f'1:s:{i}'])
        
        # Copy codecs (no re-encoding)
        cmd.extend(['-c', 'copy'])
        
        # Output file
        cmd.append(output_path)
        
        # Run ffmpeg
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            return True
        else:
            print(f"FFmpeg error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Merge error: {e}")
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
        
        await message.reply_text(
            "<blockquote><b>🔧 AUTO FILE MERGING MODE</b></blockquote>\n\n"
            "<blockquote>Please send the SOURCE FILES from which you want to extract audio and subtitles.</blockquote>\n\n"
            "<blockquote><i>📝 Note: Send all source files at once (e.g., Season 1, Episodes 1-10)</i></blockquote>"
        )
    
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
        
        file_data = {
            "message": message,
            "filename": filename,
            "file_id": file_obj.file_id,
            "file_size": file_obj.file_size
        }
        
        if state.state == "waiting_for_source":
            state.source_files.append(file_data)
            
            # Send confirmation after receiving files
            if len(state.source_files) % 5 == 0 or len(state.source_files) == 1:
                await message.reply_text(
                    f"<blockquote>📥 Received {len(state.source_files)} source files.</blockquote>\n"
                    f"<blockquote>Send more source files or send /done when finished.</blockquote>"
                )
                
        elif state.state == "waiting_for_target":
            state.target_files.append(file_data)
            
            # Send confirmation after receiving files
            if len(state.target_files) % 5 == 0 or len(state.target_files) == 1:
                await message.reply_text(
                    f"<blockquote>📥 Received {len(state.target_files)} target files.</blockquote>\n"
                    f"<blockquote>Send more target files or send /done when finished.</blockquote>"
                )
    
    @app.on_message(filters.command("done"))
    async def done_command(client: Client, message: Message):
        """Handle /done command to proceed to next step"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        if user_id not in merging_users:
            return
        
        state = merging_users[user_id]
        
        if state.state == "waiting_for_source" and state.source_files:
            state.state = "waiting_for_target"
            
            await message.reply_text(
                f"<blockquote><b>✅ Source files received!</b></blockquote>\n\n"
                f"<blockquote>Total source files: {len(state.source_files)}</blockquote>\n\n"
                f"<blockquote><b>Now send me the TARGET files.</b></blockquote>\n\n"
                f"<blockquote><i>📝 Note: Send all target files at once (same number as source files)</i></blockquote>"
            )
            
        elif state.state == "waiting_for_target" and state.target_files:
            # Check if counts match
            if len(state.source_files) != len(state.target_files):
                await message.reply_text(
                    f"<blockquote>❌ File count mismatch!</blockquote>\n\n"
                    f"<blockquote>Source files: {len(state.source_files)}\n"
                    f"Target files: {len(state.target_files)}</blockquote>\n\n"
                    f"<blockquote>Please send exactly the same number of target files as source files.</blockquote>"
                )
                return
            
            # Start processing
            state.state = "processing"
            state.total_files = len(state.source_files)
            
            # Send initial processing message
            progress_msg = await message.reply_text(
                f"<blockquote><b>🔄 Starting Merge Process</b></blockquote>\n\n"
                f"<blockquote>📊 Progress: 0/{state.total_files}\n"
                f"⏳ Status: Preparing...</blockquote>"
            )
            
            # Start the merging process
            await process_merging(client, state, progress_msg)
            
        else:
            await message.reply_text(
                "<blockquote>❌ No files received yet.</blockquote>\n"
                "<blockquote>Please send files first.</blockquote>"
            )
    
    @app.on_message(filters.command("cancel_merge"))
    async def cancel_merge_command(client: Client, message: Message):
        """Cancel the merging process"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        if user_id in merging_users:
            del merging_users[user_id]
            await message.reply_text(
                "<blockquote><b>❌ Merge process cancelled.</b></blockquote>\n"
                "<blockquote>All temporary files cleaned up.</blockquote>"
            )

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
            
            if not matched_pairs:
                await progress_msg.edit_text(
                    "<blockquote>❌ No matching episodes found!</blockquote>\n\n"
                    "<blockquote>Could not match source and target files by season/episode.</blockquote>"
                )
                return
            
            # Process each matched pair
            success_count = 0
            failed_count = 0
            
            for idx, (source_data, target_data) in enumerate(matched_pairs, 1):
                # Update progress
                try:
                    await progress_msg.edit_text(
                        f"<blockquote><b>🔄 Merging Files</b></blockquote>\n\n"
                        f"<blockquote>📊 Progress: {idx}/{len(matched_pairs)}\n"
                        f"✅ Successful: {success_count}\n"
                        f"❌ Failed: {failed_count}\n"
                        f"⏳ Current: {source_data['filename'][:30]}...</blockquote>"
                    )
                except:
                    pass
                
                try:
                    # Download source file
                    source_msg = source_data["message"]
                    source_file = await client.download_media(
                        source_msg,
                        file_name=str(temp_path / f"source_{idx}.mkv")
                    )
                    
                    if not source_file:
                        failed_count += 1
                        continue
                    
                    # Download target file
                    target_msg = target_data["message"]
                    target_file = await client.download_media(
                        target_msg,
                        file_name=str(temp_path / f"target_{idx}.mkv")
                    )
                    
                    if not target_file:
                        failed_count += 1
                        continue
                    
                    # Output file path
                    output_file = str(temp_path / f"output_{idx}.mkv")
                    
                    # Merge audio and subtitles
                    if merge_audio_subtitles(source_file, target_file, output_file):
                        # Upload merged file
                        await client.send_document(
                            chat_id=user_id,
                            document=output_file,
                            caption=f"<blockquote>✅ Merged: {target_data['filename']}</blockquote>"
                        )
                        success_count += 1
                    else:
                        failed_count += 1
                        await client.send_message(
                            user_id,
                            f"<blockquote>❌ Failed to merge: {target_data['filename']}</blockquote>"
                        )
                    
                except Exception as e:
                    print(f"Error processing file {idx}: {e}")
                    failed_count += 1
                    try:
                        await client.send_message(
                            user_id,
                            f"<blockquote>❌ Error processing: {target_data['filename']}</blockquote>"
                        )
                    except:
                        pass
                
                # Small delay to avoid flooding
                await asyncio.sleep(1)
            
            # Final summary
            summary = (
                f"<blockquote><b>📊 Merge Process Complete!</b></blockquote>\n\n"
                f"<blockquote>✅ Successful: {success_count}\n"
                f"❌ Failed: {failed_count}\n"
                f"📁 Total Processed: {len(matched_pairs)}</blockquote>"
            )
            
            await progress_msg.edit_text(summary)
            
    except Exception as e:
        print(f"Merge process error: {e}")
        try:
            await progress_msg.edit_text(
                "<blockquote>❌ An error occurred during merging.</blockquote>\n"
                "<blockquote>Please try again.</blockquote>"
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
2. Send all SOURCE files
3. Send <code>/done</code>
4. Send all TARGET files
5. Send <code>/done</code> again
6. Wait for processing to complete</blockquote>

<blockquote><b>⚠️ Important Notes:</b>
- Files are matched by season and episode numbers
- Original target file tracks are preserved
- Only new audio/subtitle tracks are added from source
- No re-encoding (file size optimized)</blockquote>
"""

