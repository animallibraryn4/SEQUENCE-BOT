import os
import subprocess

# Ye check karega ki mkvmerge installed hai ya nahi
if os.system("mkvmerge --version") != 0:
    print("mkvtoolnix nahi mila, install kar raha hoon...")
    os.system("apt-get update && apt-get install -y mkvtoolnix")
else:
    print("mkvtoolnix pehle se installed hai.")

# Baaki ka purana code iske niche rahega...

import asyncio
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN
import sequence  # This will register sequence handlers   # CHANGED: Import from handler_merging
from start import setup_start_handlers

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
    
    # Setup all handlers
    setup_start_handlers(app)
    setup_merging_handlers(app)  # Now this imports from handler_merging.py
    
    print("🤖 Bot starting with all features...")
    print("✅ Sequence mode loaded")
    print("✅ Merging mode loaded (via handler_merging)")
    print("✅ Start handlers loaded")
    
    app.run()

if __name__ == "__main__":
    main()
