import os
import subprocess
import asyncio
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN

# Ye check karega ki mkvmerge installed hai ya nahi
if os.system("mkvmerge --version") != 0:
    print("mkvtoolnix nahi mila, install kar raha hoon...")
    os.system("apt-get update && apt-get install -y mkvtoolnix")
else:
    print("mkvtoolnix pehle se installed hai.")

# Import handlers
import sequence  # This will register sequence handlers
from handler_merging import setup_merging_handlers
from start import setup_start_handlers

# Disable Pyrogram's interactive login
os.environ['PYROGRAM_SESSION'] = 'non-interactive'

async def main():
    """Initialize and run the bot with all features"""
    
    # Check if BOT_TOKEN is set
    if not BOT_TOKEN or BOT_TOKEN == "":
        print("❌ ERROR: BOT_TOKEN is empty!")
        print("Please add your bot token to config.py")
        return
    
    print(f"Using bot token: {BOT_TOKEN[:10]}...")  # Show first 10 chars
    
    # Create the main bot client
    app = Client(
        name="sequence_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        workdir="/content",
        in_memory=True  # Don't save session files
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
