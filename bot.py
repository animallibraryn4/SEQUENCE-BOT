
import asyncio
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN

# Create the main bot client FIRST
app = Client(
    "sequence_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir="/content"
)

def main():
    """Initialize and run the bot with all features"""
    
    # Import and setup handlers AFTER creating app
    from start import setup_start_handlers
    from sequence import setup_sequence_handlers
    
    # Try to import merging handlers if available
    try:
        from handler_merging import setup_merging_handlers
        MERGING_AVAILABLE = True
    except ImportError as e:
        print(f"Merging module not available: {e}")
        MERGING_AVAILABLE = False
    
    # Setup all handlers
    setup_start_handlers(app)
    setup_sequence_handlers(app)
    
    if MERGING_AVAILABLE:
        setup_merging_handlers(app)
        print("✅ Merging mode loaded (via handler_merging)")
    
    print("🤖 Bot starting with all features...")
    print("✅ Sequence mode loaded")
    print("✅ Start handlers loaded")
    
    # Set bot start time
    from start import set_bot_start_time
    set_bot_start_time()
    
    app.run()

if __name__ == "__main__":
    main()
