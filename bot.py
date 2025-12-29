import asyncio
from pyrogram import Client, filters
from config import API_ID, API_HASH, BOT_TOKEN
from handler_merging import setup_merging_handlers
from start import setup_start_handlers
from file_handler import setup_file_handler
from sequence import (
    quality_mode_cmd,
    ls_command,
    start_sequence,
    mode_callback_handler,
    set_mode_callback,
    sequence_control_callback,
    ls_callback_handlers,
    handle_ls_links,
    switch_mode_cmd
)

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
    
    print("🤖 Bot starting with all features...")
    
    # Setup basic handlers
    setup_start_handlers(app)
    print("✅ Start handlers loaded")
    
    # Setup merging handlers
    setup_merging_handlers(app)
    print("✅ Merging mode loaded")
    
    # Setup file handler (routes files to correct mode)
    setup_file_handler(app)
    print("✅ File handler loaded")
    
    # Register sequence command handlers
    app.on_message(filters.command("fileseq"))(quality_mode_cmd)
    app.on_message(filters.command("ls"))(ls_command)
    app.on_message(filters.command("sequence"))(start_sequence)
    app.on_message(filters.command("sf"))(switch_mode_cmd)
    
    # Register link handler for LS mode
    app.on_message(filters.text & filters.regex(r'https?://t\.me/'))(handle_ls_links)
    
    # Register sequence callback handlers
    app.on_callback_query(filters.regex(r'^mode_(file|caption)$|^close_mode$'))(mode_callback_handler)
    app.on_callback_query(filters.regex(r'^set_mode_(group|per_ep)$'))(set_mode_callback)
    app.on_callback_query(filters.regex(r'^(send_sequence|cancel_sequence)$'))(sequence_control_callback)
    app.on_callback_query(filters.regex(r'^ls_(chat|channel|close)_'))(ls_callback_handlers)
    
    print("✅ Sequence mode loaded")
    print("✅ All handlers registered")
    
    app.run()

if __name__ == "__main__":
    main()
