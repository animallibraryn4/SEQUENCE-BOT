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

# Import handler setup functions
from handler_merging import setup_merging_handlers
from start import setup_start_handlers
from sequence import setup_sequence_handlers  # NEW: Import sequence setup

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
    setup_merging_handlers(app)
    setup_sequence_handlers(app)  # NEW: Setup sequence handlers
    
    print("🤖 Bot starting with all features...")
    print("✅ Sequence mode loaded")
    print("✅ Merging mode loaded (via handler_merging)")
    print("✅ Start handlers loaded")
    
    app.run()

if __name__ == "__main__":
    main()
