# file name: bot.py (UPDATED)
import asyncio
import os
import sys
from pyrogram import Client

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import API_ID, API_HASH, BOT_TOKEN
from start import setup_start_handlers
from sequence import setup_sequence_handlers
from merging import setup_merging_handlers

# Create single bot client
app = Client(
    "sequence_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir=os.path.dirname(os.path.abspath(__file__))
)

def setup_all_handlers():
    """Setup all bot handlers"""
    setup_start_handlers(app)
    setup_sequence_handlers(app)  # We'll create this function
    setup_merging_handlers(app)
    
    print("✅ All handlers registered successfully")

def main():
    """Main entry point"""
    print("🤖 Starting Sequence & Merge Bot...")
    print(f"📁 Working directory: {os.getcwd()}")
    
    # Setup all handlers
    setup_all_handlers()
    
    # Run the bot
    app.run()

if __name__ == "__main__":
    main()
