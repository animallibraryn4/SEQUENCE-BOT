import os
import subprocess

# Ye check karega ki mkvmerge installed hai ya nahi
if os.system("mkvmerge --version") != 0:
    print("mkvtoolnix nahi mila, install kar raha hoon...")
    os.system("apt-get update && apt-get install -y mkvtoolnix")
else:
    print("mkvtoolnix pehle se installed hai.")

import asyncio
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN
import sequence  # This will register sequence handlers
from handler_merging import setup_merging_handlers
from start import setup_start_handlers

async def main():
    """Initialize and run the bot with all features"""
    
    # Create the main bot client
    app = Client(
        name="sequence_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        workdir="/content"
    )
    
    # Setup all handlers
    setup_start_handlers(app)
    setup_merging_handlers(app)
    
    print("🤖 Bot starting with all features...")
    print("✅ Sequence mode loaded")
    print("✅ Merging mode loaded")
    print("✅ Start handlers loaded")
    
    await app.start()
    print("Bot started successfully! Use Ctrl+C to stop.")
    
    # Keep the bot running
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
