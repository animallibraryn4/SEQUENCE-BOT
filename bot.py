import os
import subprocess

# MKVToolNix check
if os.system("mkvmerge --version") != 0:
    print("mkvtoolnix nahi mila, install kar raha hoon...")
    os.system("apt-get update && apt-get install -y mkvtoolnix")
else:
    print("mkvtoolnix pehle se installed hai.")

# FFmpeg check bhi karo
if os.system("ffmpeg -version") != 0:
    print("⚠️ FFmpeg not found, merging may not work properly")

import asyncio
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN
from handlers import setup_all_handlers  # Unified handler setup
from start import set_bot_start_time

# Create the main bot client
app = Client(
    "sequence_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir="/content"
)

def main():
    """Initialize and run the bot with all features"""
    
    # Setup all handlers ek saath
    setup_all_handlers(app)
    
    # Set bot start time for uptime tracking
    set_bot_start_time()
    
    print("🤖 Bot starting with all features...")
    print("✅ All handlers loaded successfully")
    
    app.run()

if __name__ == "__main__":
    main()
