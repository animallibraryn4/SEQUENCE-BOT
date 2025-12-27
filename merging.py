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
merging_users = {}

class MergingState:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.source_files = [] 
        self.target_files = [] 
        self.state = "waiting_for_source"
        self.current_processing = 0
        self.total_files = 0

# --- PARSING ENGINE ---
def parse_episode_info(filename: str) -> Dict:
    name = filename.lower()
    patterns = [
        r's\s*(\d+)[\s._-]*e\s*(\d+)',
        r'season\s*(\d+)[\s._-]*episode\s*(\d+)',
        r'(\d+)[xX](\d+)',
        r'ep\s*(\d+)',
    ]
    season, episode = 1, 0
    for p in patterns:
        match = re.search(p, name)
        if match:
            if len(match.groups()) == 2:
                season, episode = int(match.group(1)), int(match.group(2))
            else:
                episode = int(match.group(1))
            break
    if episode == 0:
        nums = re.findall(r'\d+', name)
        if nums: episode = int(nums[-1])
    return {"season": season, "episode": episode}

# --- ADVANCED MERGING ENGINE (The Fix) ---
def merge_audio_subtitles_v2(source_path: str, target_path: str, output_path: str) -> bool:
    temp_audio = f"sync_temp_{os.getpid()}.m4a"
    try:
        # STEP 1: PRE-CODE AUDIO (Silence & Lag Fix)
        clean_cmd = [
            "ffmpeg", "-y", "-i", source_path,
            "-vn", "-sn", "-c:a", "aac", "-b:a", "192k", "-ar", "44100", "-ac", "2",
            "-af", "aresample=async=1:min_hard_comp=0.01:first_pts=0",
            temp_audio
        ]
        subprocess.run(clean_cmd, capture_output=True, check=True)

        # STEP 2: STABLE MERGE
        cmd = [
            "ffmpeg", "-y", "-i", target_path, "-i", temp_audio,
            "-map", "0:v:0", "-map", "0:a?", "-map", "1:a", "-map", "0:s?",
            "-c:v", "copy", "-c:a", "copy", "-c:s", "copy",
            "-disposition:a:1", "default",
            "-max_interleave_delta", "200M",
            "-avoid_negative_ts", "make_zero",
            "-movflags", "+faststart",
            "-bsf:a", "aac_adtstoasc",
            output_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0
    except Exception as e:
        print(f"Merge Error: {e}")
        return False
    finally:
        if os.path.exists(temp_audio): os.remove(temp_audio)

# --- HANDLERS ---
async def start_merging(client: Client, message: Message):
    if not await is_subscribed(client, message): return
    user_id = message.from_user.id
    merging_users[user_id] = MergingState(user_id)
    await message.reply_text("<b>Step 1: Send SOURCE files (Audio/Subs).</b>\nSend /done when finished.")

async def handle_files(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in merging_users: return
    state = merging_users[user_id]
    if state.state == "waiting_for_source": state.source_files.append(message)
    elif state.state == "waiting_for_target": state.target_files.append(message)

async def done_command(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in merging_users: return
    state = merging_users[user_id]
    
    if state.state == "waiting_for_source":
        if not state.source_files: return await message.reply_text("Send files first!")
        state.state = "waiting_for_target"
        await message.reply_text(f"✅ Got {len(state.source_files)} source files.\n<b>Step 2: Send TARGET videos.</b>\nSend /done when finished.")
    elif state.state == "waiting_for_target":
        if not state.target_files: return await message.reply_text("Send target files!")
        state.state = "processing"
        await handle_merging_process(client, message, user_id)

async def handle_merging_process(client: Client, message: Message, user_id: int):
    state = merging_users[user_id]
    progress = await message.reply_text("🔄 Matching episodes...")
    try:
        source_map = {(parse_episode_info(m.document.file_name if m.document else m.video.file_name)['season'], 
                       parse_episode_info(m.document.file_name if m.document else m.video.file_name)['episode']): m 
                      for m in state.source_files}
        
        matches = []
        for tgt in state.target_files:
            info = parse_episode_info(tgt.document.file_name if tgt.document else tgt.video.file_name)
            key = (info['season'], info['episode'])
            if key in source_map: matches.append((source_map[key], tgt))

        if not matches: return await progress.edit_text("❌ No matches found!")

        for i, (src_msg, tgt_msg) in enumerate(matches):
            await progress.edit_text(f"🚀 Processing {i+1}/{len(matches)}...")
            with tempfile.TemporaryDirectory() as tmp:
                s_p = await src_msg.download(os.path.join(tmp, "s"))
                t_p = await tgt_msg.download(os.path.join(tmp, "t"))
                out_name = (tgt_msg.document.file_name if tgt_msg.document else tgt_msg.video.file_name).rsplit('.', 1)[0] + ".mkv"
                out_p = os.path.join(tmp, out_name)
                
                if merge_audio_subtitles_v2(s_p, t_p, out_p):
                    await client.send_document(user_id, out_p, caption=f"✅ {out_name}")
                else:
                    await client.send_message(user_id, f"❌ Failed: {out_name}")
        await progress.edit_text("✨ Done!")
    except Exception as e:
        await progress.edit_text(f"❌ Error: {e}")
    finally:
        if user_id in merging_users: del merging_users[user_id]

# --- IMPORTANT: THIS CONNECTS TO BOT.PY ---
def setup_merging_handlers(app: Client):
    app.add_handler(filters.command("merging") & filters.private, start_merging)
    app.add_handler(filters.command("done") & filters.private, done_command)
    app.add_handler((filters.document | filters.video) & filters.private, handle_files, group=1)

def get_merging_help_text():
    return "Use /merging to start. Send source files, then /done, then target files, then /done."
    
