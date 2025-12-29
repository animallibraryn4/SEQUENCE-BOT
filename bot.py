
import asyncio
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN
import sequence  # This will register sequence handlers
from handler_merging import setup_merging_handlers
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
    
    # Setup all handlers in correct order
    setup_start_handlers(app)
    setup_merging_handlers(app)  # Merging handlers
    # sequence handlers are already registered via import
    
    print("🤖 Bot starting with all features...")
    print("✅ Sequence mode loaded")
    print("✅ Merging mode loaded (via handler_merging)")
    print("✅ Start handlers loaded")
    
    app.run()

if __name__ == "__main__":
    main()
