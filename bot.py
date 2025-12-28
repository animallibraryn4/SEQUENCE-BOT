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

# Create the main bot client (SINGLE INSTANCE)
app = Client(
    "sequence_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir="/content"
)

def main():
    """Initialize and run the bot with all features"""
    
    # Import and setup all handlers
    from start import setup_start_handlers
    from sequence import setup_sequence_handlers
    from handler_merging import setup_merging_handlers
    
    # Setup all handlers
    setup_start_handlers(app)
    setup_sequence_handlers(app)  # Changed from importing sequence.py
    setup_merging_handlers(app)
    
    print("🤖 Bot starting with all features...")
    print("✅ Sequence mode loaded")
    print("✅ Merging mode loaded")
    print("✅ Start handlers loaded")
    
    app.run()

if __name__ == "__main__":
    main()
