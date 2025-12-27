import os
import re
import asyncio
import tempfile
import subprocess
from pathlib import Path
from typing import List, Dict, Tuple

from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

from start import is_subscribed

# ===============================
# MERGING SESSION STATE
# ===============================

merging_users = {}

class MergingState:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.source_files = []
        self.target_files = []
        self.state = "waiting_for_source"


# ===============================
# EPISODE PARSER
# ===============================

def parse_episode(filename: str):
    name = filename.lower()
    patterns = [
        r"s(\d+)[\s._-]*e(\d+)",
        r"(\d+)x(\d+)",
        r"ep[\s._-]*(\d+)"
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

    return season, episode


def match_by_episode(src, tgt):
    pairs = []
    for t in tgt:
        ts, te = parse_episode(t["filename"])
        found = None
        for s in src:
            ss, se = parse_episode(s["filename"])
            if ss == ts and se == te:
                found = s
                break
        if found:
            pairs.append((found, t))
    return pairs


# ===============================
# 🔥 MX PLAYER SAFE MERGE
# ===============================
def merge_audio_subs_seek_safe(source, target, output) -> bool:
    try:
        cmd = [
            "ffmpeg", "-y",

            # inputs
            "-fflags", "+genpts",
            "-i", target,
            "-i", source,

            # mapping
            "-map", "0:v:0",
            "-map", "0:a?",
            "-map", "1:a?",
            "-map", "0:s?",
            "-map", "1:s?",

            # 🔥 VIDEO — REMUX (no re-encode)
            "-c:v", "copy",
            "-copyts", "0",
            "-vsync", "vfr",

            # 🔥 AUDIO — SEEK SAFE
            "-c:a", "aac",
            "-b:a", "192k",
            "-ac", "2",
            "-ar", "48000",
            "-af", "aresample=async=1:first_pts=0",

            # subs
            "-c:s", "copy",

            # metadata
            "-map_metadata", "0",
            "-movflags", "+faststart",

            output
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(result.stderr[:600])
            return False

        return True

    except Exception as e:
        print("Merge error:", e)
        return False


# ===============================
# TELEGRAM HANDLERS
# ===============================

def setup_merging_handlers(app: Client):

    @app.on_message(filters.command("merging"))
    async def start_merging(client, message: Message):
        if not await is_subscribed(client, message):
            return

        merging_users[message.from_user.id] = MergingState(message.from_user.id)

        await message.reply(
            "🔧 **Auto File Merging Mode ON**\n\n"
            "📥 Pehle SOURCE files bhejo\n"
            "Phir `/done`\n\n"
            "📦 Uske baad TARGET files bhejo\n"
            "Phir `/done`",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_merge")]]
            )
        )

    @app.on_callback_query(filters.regex("^cancel_merge$"))
    async def cancel_merge(client, query):
        merging_users.pop(query.from_user.id, None)
        await query.message.edit("❌ Merge cancelled")
        await query.answer()

    @app.on_message(filters.document | filters.video)
    async def collect_files(client, message: Message):
        if message.from_user.id not in merging_users:
            return

        state = merging_users[message.from_user.id]
        file = message.document or message.video
        if not file:
            return

        state_list = state.source_files if state.state == "waiting_for_source" else state.target_files
        state_list.append({
            "message": message,
            "filename": file.file_name or f"file_{message.id}"
        })

    @app.on_message(filters.command("done"))
    async def done_step(client, message: Message):
        user_id = message.from_user.id
        if user_id not in merging_users:
            return

        state = merging_users[user_id]

        if state.state == "waiting_for_source":
            state.state = "waiting_for_target"
            await message.reply("✅ Source received\n📦 Ab TARGET files bhejo")
            return

        await message.reply("🔄 Processing started…")
        asyncio.create_task(process_merge(client, state, message))


# ===============================
# MERGE PROCESS
# ===============================

async def process_merge(client, state: MergingState, message: Message):
    pairs = match_by_episode(state.source_files, state.target_files)

    if not pairs:
        await message.reply("❌ Episode match nahi hua")
        return

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)

        for i, (src, tgt) in enumerate(pairs, 1):
            src_file = await client.download_media(src["message"], tmp / f"s{i}.mkv")
            tgt_file = await client.download_media(tgt["message"], tmp / f"t{i}.mkv")

            out = tmp / tgt["filename"]

            if merge_audio_subs_safe(src_file, tgt_file, out):
                await client.send_document(
                    message.chat.id,
                    out,
                    caption=f"✅ Merged: {tgt['filename']}"
                )
            else:
                await client.send_message(
                    message.chat.id,
                    f"❌ Failed: {tgt['filename']}"
                )

    merging_users.pop(state.user_id, None)
