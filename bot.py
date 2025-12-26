
import asyncio
import merging
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN
import sequence  # This will register sequence handlers
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
    
    print("🤖 Bot starting with all features...")
    print("✅ Sequence mode loaded")
    print("✅ Merging mode loaded")
    print("✅ Start handlers loaded")
    
    app.run()

if __name__ == "__main__":
    main()



  # Changed from 'from merging import setup_merging_handlers'

def main():
    # setup_start_handlers(app) 
    # If merging.py handles its own registration upon import, 
    # you can comment out the setup_merging_handlers(app) line.
    
