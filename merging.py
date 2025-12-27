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

def inject_audio_subs_method_2(source, target, output) -> bool:
    try:
        base = output.rsplit(".", 1)[0]
        audio_file = base + "_audio.aac"
        sub_file = base + "_sub.srt"

        # -------------------------
        # STEP 1: Extract & clean AUDIO
        # -------------------------
        audio_cmd = [
            "ffmpeg", "-y",
            "-i", source,
            "-map", "0:a:0",
            "-c:a", "aac",
            "-ac", "2",
            "-ar", "48000",
            "-b:a", "192k",
            "-fflags", "+genpts",
            "-avoid_negative_ts", "make_zero",
            audio_file
        ]

        r1 = subprocess.run(audio_cmd, capture_output=True, text=True)
        if r1.returncode != 0:
            print("Audio extract error:", r1.stderr[:300])
            return False

        # -------------------------
        # STEP 2: Extract SUBTITLES (convert to SRT)
        # -------------------------
        sub_cmd = [
            "ffmpeg", "-y",
            "-i", source,
            "-map", "0:s:0",
            "-c:s", "srt",
            sub_file
        ]

        r2 = subprocess.run(sub_cmd, capture_output=True, text=True)
        if r2.returncode != 0:
            print("Subtitle extract error:", r2.stderr[:300])
            sub_file = None  # subtitles optional

        # -------------------------
        # STEP 3: Inject into TARGET
        # -------------------------
        inject_cmd = [
            "ffmpeg", "-y",
            "-i", target,
            "-i", audio_file,
        ]

        if sub_file:
            inject_cmd += ["-i", sub_file]

        inject_cmd += [
            "-map", "0:v",
            "-map", "0:a?",
            "-map", "1:a",
        ]

        if sub_file:
            inject_cmd += ["-map", "2:s"]

        inject_cmd += [
            "-c", "copy",
            "-map_metadata", "0",
            "-movflags", "+faststart",
            output
        ]

        r3 = subprocess.run(inject_cmd, capture_output=True, text=True)
        if r3.returncode != 0:
            print("Inject error:", r3.stderr[:400])
            return False

        return True

    except Exception as e:
        print("Method 2 A+S error:", e)
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

            if inject_audio_subs_method_2(src_file, tgt_file, out):
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
