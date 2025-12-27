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
        match = re.search(p, name)
        if match:
            if len(match.groups()) == 2:
                season = int(match.group(1))
                episode = int(match.group(2))
            else:
                episode = int(match.group(1))
            break
    
    if episode == 0:
        nums = re.findall(r'\d+', name)
        if nums:
            episode = int(nums[-1])

    return {"season": season, "episode": episode}

# --- ADVANCED MERGING ENGINE (PRE-PROCESSING INCLUDED) ---

def merge_audio_subtitles_v2(source_path: str, target_path: str, output_path: str) -> bool:
    """
    VLC aur MX Player ke liye sabse stable merging method.
    Isme pehle audio ko standardize kiya jata hai taaki seek/lag issue na ho.
    """
    temp_audio = f"sync_temp_{os.getpid()}.m4a"
    try:
        # STEP 1: AUDIO PRE-PROCESSING
        # Audio ko pehle clean aur video-ready format mein convert karna
        print(f"Pre-processing audio: {source_path}")
        clean_cmd = [
            "ffmpeg", "-y",
            "-i", source_path,
            "-vn", "-sn",                # No video, No subtitles
            "-c:a", "aac",
            "-b:a", "192k",              # Stability ke liye high bitrate
            "-ar", "44100",              # Universal sample rate
            "-ac", "2",
            "-af", "aresample=async=1:min_hard_comp=0.01:first_pts=0",
            temp_audio
        ]
        
        # Audio clean-up process run karein
        subprocess.run(clean_cmd, capture_output=True, check=True)

        if not os.path.exists(temp_audio):
            return False

        # STEP 2: FINAL MERGING WITH TARGET VIDEO
        # 
        cmd = [
            "ffmpeg", "-y",
            "-i", target_path,
            "-i", temp_audio,
            
            "-map", "0:v:0",             # Target Video
            "-map", "0:a?",              # Target's original audio (if any)
            "-map", "1:a",               # Cleaned New Audio
            "-map", "0:s?",              # Target subtitles
            
            "-c:v", "copy",              # Video copy (Speed ke liye)
            "-c:a", "copy",              # Audio already cleaned hai isliye copy
            "-c:s", "copy",
            
            "-disposition:a:1", "default", # New audio track ko default banana
            
            # Timestamp aur Seek fix commands
            "-max_interleave_delta", "200M",
            "-avoid_negative_ts", "make_zero",
            "-movflags", "+faststart",
            "-bsf:a", "aac_adtstoasc",
            
            output_path
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0

    except Exception as e:
        print(f"FFmpeg Merge Error: {e}")
        return False
    finally:
        # Temp audio file delete karein
        if os.path.exists(temp_audio):
            os.remove(temp_audio)

def merge_audio_subtitles_simple(source_path: str, target_path: str, output_path: str) -> bool:
    """Muxing wrapper - Seedha stable version call karein"""
    return merge_audio_subtitles_v2(source_path, target_path, output_path)

# --- BOT COMMAND HANDLERS ---

@Client.on_message(filters.command("merging") & filters.private)
async def start_merging(client: Client, message: Message):
    if not await is_subscribed(client, message):
        return
    
    user_id = message.from_user.id
    merging_users[user_id] = MergingState(user_id)
    
    await message.reply_text(
        "<b>Step 1: Send all SOURCE files.</b>\n"
        "(Those files which have the Audio/Subtitles you want to extract)\n\n"
        "Send <code>/done</code> when finished."
    )

@Client.on_message(filters.command("done") & filters.private)
async def done_command(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in merging_users:
        return await message.reply_text("Please start with /merging first.")
    
    state = merging_users[user_id]
    
    if state.state == "waiting_for_source":
        if not state.source_files:
            return await message.reply_text("Please send at least one source file.")
        
        state.state = "waiting_for_target"
        await message.reply_text(
            f"✅ Received {len(state.source_files)} source files.\n\n"
            "<b>Step 2: Now send the TARGET files.</b>\n"
            "(The main videos where you want to add the tracks)\n\n"
            "Send <code>/done</code> when finished."
        )
        
    elif state.state == "waiting_for_target":
        if not state.target_files:
            return await message.reply_text("Please send at least one target file.")
        
        state.state = "processing"
        await handle_merging_process(client, message, user_id)

@Client.on_message(filters.document | filters.video & filters.private)
async def handle_files(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in merging_users:
        return
    
    state = merging_users[user_id]
    if state.state == "waiting_for_source":
        state.source_files.append(message)
    elif state.state == "waiting_for_target":
        state.target_files.append(message)

async def handle_merging_process(client: Client, message: Message, user_id: int):
    state = merging_users[user_id]
    progress_msg = await message.reply_text("🔄 Analyzing files and matching episodes...")
    
    try:
        source_map = {}
        for msg in state.source_files:
            filename = msg.document.file_name if msg.document else msg.video.file_name
            info = parse_episode_info(filename)
            key = (info['season'], info['episode'])
            source_map[key] = msg

        matches = []
        for msg in state.target_files:
            filename = msg.document.file_name if msg.document else msg.video.file_name
            info = parse_episode_info(filename)
            key = (info['season'], info['episode'])
            if key in source_map:
                matches.append((source_map[key], msg))

        if not matches:
            return await progress_msg.edit_text("❌ No matching episodes found between source and target files.")

        state.total_files = len(matches)
        success_count = 0
        
        for i, (src_msg, tgt_msg) in enumerate(matches):
            state.current_processing = i + 1
            await progress_msg.edit_text(f"🚀 Processing: {i+1}/{state.total_files}")
            
            with tempfile.TemporaryDirectory() as tmpdir:
                # Downloading...
                src_path = await src_msg.download(file_name=os.path.join(tmpdir, "source"))
                tgt_path = await tgt_msg.download(file_name=os.path.join(tmpdir, "target"))
                
                output_filename = tgt_msg.document.file_name if tgt_msg.document else tgt_msg.video.file_name
                # Extension fix: MKV is more stable for seekers
                if not output_filename.endswith(".mkv"):
                    output_filename = os.path.splitext(output_filename)[0] + ".mkv"
                
                out_path = os.path.join(tmpdir, output_filename)
                
                if merge_audio_subtitles_simple(src_path, tgt_path, out_path):
                    await client.send_document(
                        chat_id=user_id,
                        document=out_path,
                        caption=f"✅ Merged Successfully: {output_filename}"
                    )
                    success_count += 1
                else:
                    await client.send_message(user_id, f"❌ Failed to merge: {output_filename}")

        summary = (
            "✨ <b>Merging Completed!</b>\n\n"
            f"✅ Success: {success_count}\n"
            f"❌ Failed: {state.total_files - success_count}\n"
            f"📂 Total processed: {state.total_files}"
        )
        await progress_msg.edit_text(summary)
            
    except Exception as e:
        print(f"Merge process error: {str(e)}")
        await progress_msg.edit_text("❌ An error occurred during processing.")
    
    finally:
        if user_id in merging_users:
            del merging_users[user_id]

# --- HELP TEXT ---
def get_merging_help_text() -> str:
    return """
<b>🔧 Auto File Merging Commands</b>

<b>/merging</b> - Start process
<b>/done</b> - Next step
<b>/cancel_merge</b> - Stop

<b>📝 Note:</b> Files are matched by Season/Episode. 
This version fixes MX Player and VLC lag issues.
"""
    
