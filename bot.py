import asyncio
from pyrogram import Client, filters
from config import API_ID, API_HASH, BOT_TOKEN
from handler_merging import setup_merging_handlers
from start import setup_start_handlers
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
from database import user_sequences
from merging import merging_users

# Create the main bot client
app = Client(
    "sequence_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir="/content"
)

# Global error handler for MESSAGE_NOT_MODIFIED
@app.on_callback_query()
async def global_error_handler(client, query):
    """Global handler to catch and ignore MESSAGE_NOT_MODIFIED errors"""
    try:
        # Let other handlers process first
        return
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" in str(e):
            # Ignore this harmless error
            pass
        else:
            raise e

def main():
    """Initialize and run the bot with all features"""
    
    print("🤖 Bot starting with all features...")
    
    # Setup basic handlers
    setup_start_handlers(app)
    print("✅ Start handlers loaded")
    
    # Setup merging handlers
    setup_merging_handlers(app)
    print("✅ Merging mode loaded")
    
    # Combined file handler
    @app.on_message(filters.document | filters.video | filters.audio)
    async def combined_file_handler(client, message):
        user_id = message.from_user.id
        
        # Check if user is in merging mode
        if user_id in merging_users:
            from handler_merging import handle_merging_files
            await handle_merging_files(client, message)
            return
        
        # Check if user is in sequence mode
        if user_id in user_sequences:
            from sequence import store_file
            await store_file(client, message)
            return
    
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
