from pyrogram import Client, filters
from database import user_sequences
from merging import merging_users

async def route_file_handlers(client, message):
    """Route files to appropriate handler based on user state"""
    user_id = message.from_user.id
    
    # Check if user is in merging mode
    if user_id in merging_users:
        from handler_merging import handle_merging_files
        await handle_merging_files(client, message)
        return True
    
    # Check if user is in sequence mode
    elif user_id in user_sequences:
        from sequence import store_file
        await store_file(client, message)
        return True
    
    # User not in any mode, ignore
    return False

def setup_file_handler(app):
    """Setup the combined file handler"""
    @app.on_message(filters.document | filters.video | filters.audio)
    async def combined_file_handler(client, message):
        await route_file_handlers(client, message)
