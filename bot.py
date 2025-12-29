import asyncio
from pyrogram import Client, filters
from config import API_ID, API_HASH, BOT_TOKEN
from handler_merging import setup_merging_handlers
from start import setup_start_handlers
from sequence import (
    quality_mode_cmd,
    ls_command,
    start_sequence,
    store_file,
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
    
    # Setup start handlers first (basic commands)
    setup_start_handlers(app)
    print("✅ Start handlers loaded")
    
    # Setup merging handlers SECOND
    setup_merging_handlers(app)
    print("✅ Merging mode loaded")
    
    # Register sequence command handlers THIRD
    app.on_message(filters.command("fileseq"))(quality_mode_cmd)
    app.on_message(filters.command("ls"))(ls_command)
    app.on_message(filters.command("sequence"))(start_sequence)
    app.on_message(filters.command("sf"))(switch_mode_cmd)
    
    # Register file handler for sequence mode - IMPORTANT: with specific filter
    async def sequence_store_file(client, message):
        # Only handle files if user is in sequence mode
        user_id = message.from_user.id
        if user_id in user_sequences:  # We'll need to import this
            await store_file(client, message)
    
    # Import user_sequences here
    from database import user_sequences
    
    # Register with filter that only catches files when user is in sequence mode
    @app.on_message(filters.document | filters.video | filters.audio)
    async def combined_file_handler(client, message):
        user_id = message.from_user.id
        
        # First check if user is in merging mode
        from merging import merging_users  # Import from merging.py
        if user_id in merging_users:
            # Let merging handle it
            from handler_merging import handle_merging_files
            await handle_merging_files(client, message)
            return
        
        # Then check if user is in sequence mode
        if user_id in user_sequences:
            await store_file(client, message)
            return
    
    # Register link handler for LS mode
    app.on_message(filters.text & filters.regex(r'https?://t\.me/'))(handle_ls_links)
    
    # Register sequence callback handlers with specific filters
    app.on_callback_query(filters.regex(r'^mode_(file|caption)$|^close_mode$'))(mode_callback_handler)
    app.on_callback_query(filters.regex(r'^set_mode_(group|per_ep)$'))(set_mode_callback)
    app.on_callback_query(filters.regex(r'^(send_sequence|cancel_sequence)$'))(sequence_control_callback)
    app.on_callback_query(filters.regex(r'^ls_(chat|channel|close)_'))(ls_callback_handlers)
    
    print("✅ Sequence mode loaded")
    print("✅ All handlers registered")
    
    app.run()

if __name__ == "__main__":
    main()
