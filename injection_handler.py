import os
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from injection import inject_audio_subtitles_safely
import tempfile
import shutil

class InjectionHandler:
    """Handler for audio and subtitle injection commands."""
    
    def __init__(self, app):
        self.app = app
        self.processing_users = set()
        self.register_handlers()
    
    def register_handlers(self):
        """Register bot command handlers."""
        
        @self.app.on_message(filters.command("inject"))
        async def inject_command(client, message: Message):
            """Handle /inject command."""
            user_id = message.from_user.id
            
            # Check if user is already processing
            if user_id in self.processing_users:
                await message.reply_text(
                    "⏳ You already have an injection in progress. Please wait."
                )
                return
            
            # Check if message is a reply to a video
            if not message.reply_to_message:
                await message.reply_text(
                    "📁 Please reply to a video file with `/inject` command.\n\n"
                    "Usage:\n"
                    "1. Send a video file (MKV, MP4, etc.)\n"
                    "2. Reply with `/inject`\n\n"
                    "This will create a seek-safe version with clean audio and subtitles."
                )
                return
            
            replied_msg = message.reply_to_message
            if not (replied_msg.video or replied_msg.document):
                await message.reply_text(
                    "❌ Please reply to a video file."
                )
                return
            
            # Start processing
            self.processing_users.add(user_id)
            status_msg = await message.reply_text(
                "🔧 Starting seek-safe injection...\n"
                "⏳ This may take a few moments..."
            )
            
            try:
                # Download the file
                await status_msg.edit_text("📥 Downloading video file...")
                
                temp_dir = tempfile.mkdtemp(prefix="inject_bot_")
                input_path = os.path.join(temp_dir, "input_video")
                
                # Download file
                file_path = await replied_msg.download(input_path)
                
                # Generate output path
                output_path = os.path.join(temp_dir, "output_injected.mkv")
                
                # Perform injection
                await status_msg.edit_text(
                    "🔄 Injecting audio and subtitles...\n"
                    "• Keeping video untouched\n"
                    "• Adding clean audio track\n"
                    "• Adding subtitles\n"
                    "⏳ Please wait..."
                )
                
                # Run injection in thread pool to avoid blocking
                loop = asyncio.get_event_loop()
                success = await loop.run_in_executor(
                    None,
                    inject_audio_subtitles_safely,
                    file_path,
                    output_path
                )
                
                if success and os.path.exists(output_path):
                    # Get file size
                    file_size = os.path.getsize(output_path)
                    
                    # Check if file is too large for Telegram (max 2GB)
                    if file_size > 2 * 1024 * 1024 * 1024:
                        await status_msg.edit_text(
                            "❌ File is too large (>2GB) for Telegram."
                        )
                    else:
                        await status_msg.edit_text("📤 Uploading processed file...")
                        
                        # Send the file
                        caption = (
                            "✅ Seek-safe injection complete!\n\n"
                            "📁 Video stream: Preserved (no changes)\n"
                            "🔊 Audio: Clean AAC track added\n"
                            "📝 Subtitles: English SRT added\n\n"
                            "🎬 Safe for seeking in MX Player & VLC"
                        )
                        
                        await client.send_document(
                            chat_id=message.chat.id,
                            document=output_path,
                            caption=caption,
                            reply_to_message_id=message.id
                        )
                        
                        await status_msg.delete()
                else:
                    await status_msg.edit_text(
                        "❌ Injection failed. Please try another file."
                    )
                
            except Exception as e:
                await status_msg.edit_text(
                    f"❌ Error during injection: {str(e)[:200]}"
                )
                print(f"Injection error: {e}")
            
            finally:
                # Cleanup
                if 'temp_dir' in locals() and os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir, ignore_errors=True)
                
                # Remove user from processing set
                self.processing_users.discard(user_id)
        
        @self.app.on_message(filters.command("injecthelp"))
        async def inject_help_command(client, message: Message):
            """Show help for injection command."""
            help_text = (
                "🔧 **Seek-Safe Audio & Subtitle Injection**\n\n"
                "**Problem solved:**\n"
                "• Audio goes silent after seeking in MX Player/VLC\n"
                "• Timestamp conflicts between video and audio\n\n"
                "**Our solution:**\n"
                "• Video stream: Preserved exactly as-is\n"
                "• Audio: Clean re-encoded AAC track added\n"
                "• Subtitles: English SRT track added\n\n"
                "**How to use:**\n"
                "1. Send any video file (MKV, MP4, AVI, etc.)\n"
                "2. Reply with `/inject`\n"
                "3. Wait for processed file\n\n"
                "**Benefits:**\n"
                "✅ No video quality loss\n"
                "✅ Safe for seeking\n"
                "✅ Works in all players\n"
                "✅ Preserves original tracks\n\n"
                "**Note:** Processed files may be slightly larger due to added audio track."
            )
            
            await message.reply_text(help_text)


# For integration with your existing bot
def setup_injection_handlers(app):
    """Setup injection handlers in your main bot."""
    handler = InjectionHandler(app)
    return handler
