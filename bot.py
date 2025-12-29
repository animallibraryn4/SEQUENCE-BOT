import asyncio
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN
from handler_merging import setup_merging_handlers
from start import setup_start_handlers
import sequence  # This will import sequence functions but NOT run them

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
    
    # Remove the duplicate Client creation from sequence module
    # We need to pass our app instance to sequence.py handlers
    
    # First setup sequence handlers using our app instance
    from sequence import (
        # Command handlers
        quality_mode_cmd, ls_command, start_sequence, store_file,
        # Callback handlers
        mode_callback_handler, set_mode_callback, sequence_control_callback,
        ls_callback_handlers,
        # Other functions
        handle_ls_links, switch_mode_cmd
    )
    
    # Register sequence command handlers
    app.on_message(filters.command("fileseq"))(quality_mode_cmd)
    app.on_message(filters.command("ls"))(ls_command)
    app.on_message(filters.command("sequence"))(start_sequence)
    app.on_message(filters.command("sf"))(switch_mode_cmd)
    
    # Register file handler for sequence mode
    app.on_message(filters.document | filters.video | filters.audio)(store_file)
    
    # Register link handler for LS mode
    app.on_message(filters.text & filters.regex(r'https?://t\.me/'))(handle_ls_links)
    
    # Register sequence callback handlers
    app.on_callback_query(filters.regex(r'^mode_(file|caption)$|^close_mode$'))(mode_callback_handler)
    app.on_callback_query(filters.regex(r'^set_mode_(group|per_ep)$'))(set_mode_callback)
    app.on_callback_query(filters.regex(r'^(send_sequence|cancel_sequence)$'))(sequence_control_callback)
    app.on_callback_query(filters.regex(r'^ls_(chat|channel|close)_'))(ls_callback_handlers)
    
    # Setup other handlers in correct order
    setup_start_handlers(app)
    setup_merging_handlers(app)  # Merging handlers
    
    print("🤖 Bot starting with all features...")
    print("✅ Sequence mode loaded")
    print("✅ Merging mode loaded (via handler_merging)")
    print("✅ Start handlers loaded")
    print("✅ All commands registered properly")
    
    app.run()

if __name__ == "__main__":
    # Import filters here to avoid circular imports
    from pyrogram import filters
    main()
