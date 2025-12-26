
import asyncio
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN

# Import all handlers
from sequence import app as sequence_app, main as sequence_main
from merging import setup_merging_handlers
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
    setup_merging_handlers(app)
    
    # Note: sequence.py handlers are already registered in the sequence module
    # We just need to run the app
    
    print("🤖 Bot starting with all features...")
    print("✅ Sequence mode loaded")
    print("✅ Merging mode loaded")
    print("✅ Start handlers loaded")
    
    app.run()

if __name__ == "__main__":
    main()

