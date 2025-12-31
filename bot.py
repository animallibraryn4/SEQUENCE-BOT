


import asyncio
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN
from sequence import setup_sequence_handlers  # ADD THIS IMPORT
from handler_merging import setup_merging_handlers
from start import setup_start_handlers, set_bot_start_time

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
    setup_sequence_handlers(app)  # ADD THIS LINE - IMPORTANT!
    setup_merging_handlers(app)
    
    # Set bot start time for uptime tracking
    set_bot_start_time()
    
    print("ðŸ¤– Bot starting with all features...")
    print("âœ… Sequence mode loaded")
    print("âœ… Merging mode loaded (via handler_merging)")
    print("âœ… Start handlers loaded")
    
    app.run()

if __name__ == "__main__":
    main()
