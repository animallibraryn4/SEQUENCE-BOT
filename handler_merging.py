import os
import asyncio
import tempfile
import time
from pathlib import Path
from typing import List, Tuple, Optional
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait, MessageNotModified

from config import OWNER_ID
from start import is_subscribed

# Import from refactored merging module
from merging import (
    MergingState,
    MergeState,
    ProcessStage,
    state_manager,
    ProgressTracker,
    EpisodeParser,
    FileMatcher,
    merge_engine,
    get_merging_help_text,
    silent_cleanup,
    validate_file_size,
    sanitize_filename,
    format_size,
    format_duration,
    MediaFile,
    Config
)

# ============================
# UTILITY FUNCTIONS
# ============================

async def send_safe_edit(message: Message, text: str, reply_markup=None, **kwargs):
    """Safely edit message with flood wait handling"""
    try:
        await message.edit_text(text, reply_markup=reply_markup, **kwargs)
        return True
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await send_safe_edit(message, text, reply_markup, **kwargs)
    except MessageNotModified:
        return True
    except Exception as e:
        print(f"Error editing message: {e}")
        return False

async def download_with_progress(
    client: Client,
    message: Message,
    file_path: str,
    progress_msg: Message,
    user_id: int,
    stage: ProcessStage,
    filename: str,
    overall_progress: str
) -> Optional[str]:
    """Download file with progress updates"""
    start_time = time.time()
    
    async def progress_callback(current, total):
        # Check cancellation
        if await state_manager.is_cancelled(user_id):
            raise asyncio.CancelledError("Processing cancelled")
        
        await ProgressTracker.update(
            message=progress_msg,
            user_id=user_id,
            stage=stage,
            filename=f"{filename} ({overall_progress})",
            current=current,
            total=total,
            start_time=start_time,
            cancelled_callback_data=f"cancel_processing_{user_id}"
        )
    
    try:
        downloaded_file = await client.download_media(
            message,
            file_name=file_path,
            progress=progress_callback
        )
        
        # Final update
        await ProgressTracker.update(
            message=progress_msg,
            user_id=user_id,
            stage=stage,
            filename=f"{filename} ({overall_progress})",
            current=os.path.getsize(downloaded_file) if downloaded_file and os.path.exists(downloaded_file) else 0,
            total=message.document.file_size if message.document else message.video.file_size,
            start_time=start_time,
            cancelled_callback_data=f"cancel_processing_{user_id}"
        )
        
        return downloaded_file
        
    except asyncio.CancelledError:
        # Clean up partial download
        if os.path.exists(file_path):
            os.remove(file_path)
        raise
    except Exception as e:
        print(f"Download error: {e}")
        return None

async def upload_with_progress(
    client: Client,
    user_id: int,
    file_path: str,
    progress_msg: Message,
    overall_progress: str,
    caption: str
) -> bool:
    """Upload file with progress updates"""
    start_time = time.time()
    filename = os.path.basename(file_path)
    
    async def progress_callback(current, total):
        if await state_manager.is_cancelled(user_id):
            raise asyncio.CancelledError("Processing cancelled")
        
        await ProgressTracker.update(
            message=progress_msg,
            user_id=user_id,
            stage=ProcessStage.UPLOADING,
            filename=f"{filename} ({overall_progress})",
            current=current,
            total=total,
            start_time=start_time,
            cancelled_callback_data=f"cancel_processing_{user_id}"
        )
    
    try:
        await client.send_document(
            chat_id=user_id,
            document=file_path,
            caption=caption,
            progress=progress_callback
        )
        return True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"Upload error: {e}")
        return False

# ============================
# MERGING PROCESS
# ============================

async def start_merging_process(client: Client, state: MergingState, message: Message):
    """Start the merging process"""
    user_id = state.user_id
    
    # Create initial progress message
    progress_msg = await message.reply_text(
        f"""
<b>🔄 Starting Optimized Merge Process</b>

📊 Matching {len(state.source_files)} source files with {len(state.target_files)} target files...

<b>🎯 Workflow:</b>
• Extract tracks from source
• Delete source to save space
• Analyze target specifications
• Smart re-encoding ({Config.MAX_AUDIO_SIZE_MB}MB limit)
• Merge with timing preservation
• Automatic cleanup
        """,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")
        ]])
    )
    
    state.progress_msg = progress_msg
    state.state = MergeState.PROCESSING
    
    # Start processing in background
    asyncio.create_task(process_merging(client, state, progress_msg))

async def process_merging(client: Client, state: MergingState, progress_msg: Message):
    """Process all matched file pairs"""
    user_id = state.user_id
    
    # Set initial processing state
    await state_manager.set_user_state(user_id, {"cancelled": False, "current_file": None})
    
    try:
        # Match files
        matched_pairs = FileMatcher.match_files(state.source_files, state.target_files)
        valid_pairs = [(s, t) for s, t in matched_pairs if s is not None]
        
        if not valid_pairs:
            await send_safe_edit(
                progress_msg,
                """
<b>❌ No Matching Episodes Found</b>

Could not match source and target files by season/episode.

<b>Possible reasons:</b>
• Filenames don't contain episode numbers
• Different naming conventions
• Missing episodes
                """
            )
            return
        
        # Update with match info
        await send_safe_edit(
            progress_msg,
            f"""
<b>📊 Files Matched Successfully</b>

Total pairs: {len(valid_pairs)}
Skipped (no match): {len(matched_pairs) - len(valid_pairs)}

<b>🔄 Starting optimized processing...</b>
            """,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")
            ]])
        )
        
        # Process each pair
        for idx, (source, target) in enumerate(valid_pairs, 1):
            try:
                await state_manager.set_user_state(user_id, {
                    "cancelled": False,
                    "current_file": target.filename
                })
                
                # Process single pair
                success = await process_single_pair(
                    client=client,
                    source=source,
                    target=target,
                    progress_msg=progress_msg,
                    user_id=user_id,
                    pair_index=idx,
                    total_pairs=len(valid_pairs)
                )
                
                if success:
                    state.metrics.files_processed += 1
                else:
                    state.metrics.errors += 1
                
                # Check for too many errors
                if state.metrics.errors >= Config.MAX_CONSECUTIVE_ERRORS:
                    await send_safe_edit(
                        progress_msg,
                        f"""
<b>⚠️ Too Many Errors</b>

Stopping after {Config.MAX_CONSECUTIVE_ERRORS} consecutive errors.

Processed: {state.metrics.files_processed}
Errors: {state.metrics.errors}
                        """
                    )
                    break
                
            except asyncio.CancelledError:
                await send_safe_edit(
                    progress_msg,
                    "<b>❌ Processing Cancelled</b>\n\nCurrent operation was cancelled."
                )
                return
            except Exception as e:
                print(f"Error processing pair {idx}: {e}")
                state.metrics.errors += 1
                
                # Update error count
                await send_safe_edit(
                    progress_msg,
                    f"""
<b>⚠️ Processing Error</b>

File: {target.filename}
Error: {str(e)[:100]}

Errors so far: {state.metrics.errors}
                    """,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")
                    ]])
                )
            
            # Small delay between files
            await asyncio.sleep(1)
        
        # Final completion message
        await send_completion_message(progress_msg, state.metrics)
        
    except asyncio.CancelledError:
        await handle_cancellation(progress_msg)
    except Exception as e:
        print(f"Process error: {e}")
        await send_safe_edit(
            progress_msg,
            f"""
<b>❌ Process Error</b>

An unexpected error occurred:
{str(e)[:200]}
            """
        )
    finally:
        # Clean up
        await state_manager.cleanup_user(user_id)

async def process_single_pair(
    client: Client,
    source: MediaFile,
    target: MediaFile,
    progress_msg: Message,
    user_id: int,
    pair_index: int,
    total_pairs: int
) -> bool:
    """Process a single source-target pair"""
    overall_progress = f"{pair_index}/{total_pairs}"
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        try:
            # Download source
            await send_safe_edit(
                progress_msg,
                f"""
<b>⬇️ Downloading Source ({overall_progress})</b>

📁 {source.filename}
                """,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")
                ]])
            )
            
            source_path = str(temp_path / f"source_{pair_index}{Path(source.filename).suffix}")
            downloaded_source = await download_with_progress(
                client, source.message, source_path,
                progress_msg, user_id,
                ProcessStage.DOWNLOADING,
                f"Source: {source.filename}",
                overall_progress
            )
            
            if not downloaded_source:
                return False
            
            # Download target
            await send_safe_edit(
                progress_msg,
                f"""
<b>⬇️ Downloading Target ({overall_progress})</b>

📁 {target.filename}
                """,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")
                ]])
            )
            
            target_path = str(temp_path / f"target_{pair_index}{Path(target.filename).suffix}")
            downloaded_target = await download_with_progress(
                client, target.message, target_path,
                progress_msg, user_id,
                ProcessStage.DOWNLOADING,
                f"Target: {target.filename}",
                overall_progress
            )
            
            if not downloaded_target:
                silent_cleanup(downloaded_source)
                return False
            
            # Merge files
            output_path = str(temp_path / target.filename)
            
            await send_safe_edit(
                progress_msg,
                f"""
<b>🔄 Merging ({overall_progress})</b>

📁 {target.filename}

<b>Steps:</b>
1. Extract tracks from source
2. Delete source to save space
3. Analyze target specs
4. Smart re-encoding
5. Merge with timing
6. Cleanup
                """,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")
                ]])
            )
            
            success = await merge_engine.optimized_merge(
                downloaded_source,
                downloaded_target,
                output_path
            )
            
            if not success:
                silent_cleanup(downloaded_source, downloaded_target)
                return False
            
            # Upload result
            caption = f"""
<b>✅ Optimized Merge Completed</b>

📁 {target.filename}

<b>Workflow used:</b>
• Source deleted after track extraction ✓
• Target analysis before re-encoding ✓
• Smart compression ({Config.MAX_AUDIO_SIZE_MB}MB limit) ✓
• Original target audio preserved ✓
• Automatic cleanup ✓
            """
            
            await send_safe_edit(
                progress_msg,
                f"""
<b>⬆️ Uploading ({overall_progress})</b>

📁 {target.filename}
                """,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")
                ]])
            )
            
            upload_success = await upload_with_progress(
                client, user_id, output_path,
                progress_msg, overall_progress, caption
            )
            
            # Cleanup
            silent_cleanup(downloaded_source, downloaded_target, output_path)
            
            if upload_success:
                await send_safe_edit(
                    progress_msg,
                    f"""
<b>✅ Merge Completed ({overall_progress})</b>

📁 {target.filename}

• Target audio: Preserved ✓
• Source audio: Added ✓
• Storage optimized ✓
• Smart compression ✓
                    """,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")
                    ]])
                )
                return True
            else:
                return False
            
        except asyncio.CancelledError:
            # Cleanup any existing files
            for file in temp_path.glob("*"):
                if file.is_file():
                    file.unlink()
            raise
        except Exception as e:
            print(f"Pair processing error: {e}")
            return False

async def send_completion_message(progress_msg: Message, metrics: any):
    """Send final completion message"""
    total_time = time.time() - metrics.start_time
    
    await send_safe_edit(
        progress_msg,
        f"""
<b>✅ All Merges Completed Successfully</b>

📊 <b>Summary:</b>
• Files processed: {metrics.files_processed}
• Errors encountered: {metrics.errors}
• Files cleaned up: {metrics.cleanup_count}
• Total time: {format_duration(total_time)}

🎯 <b>Optimizations Applied:</b>
• Source files deleted after extraction
• Smart compression ({Config.MAX_AUDIO_SIZE_MB}MB limit)
• Target audio preserved
• Automatic cleanup

📦 <b>Next Steps:</b>
• Check your Telegram saved messages
• Files are ready to use
• Use /merging to start again
        """
    )

async def handle_cancellation(progress_msg: Message):
    """Handle process cancellation"""
    await send_safe_edit(
        progress_msg,
        """
<b>❌ Processing Cancelled</b>

All operations stopped.
Temporary files have been cleaned up.

Use <code>/merging</code> to start a new session.
        """
    )

# ============================
# TELEGRAM HANDLERS
# ============================

def setup_merging_handlers(app: Client):
    """Setup all merging-related handlers"""
    
    @app.on_message(filters.command("merging"))
    async def merging_command(client: Client, message: Message):
        """Start the merging process"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        # Check if already in merging state
        existing_state = await state_manager.get_user_state(user_id)
        if existing_state:
            await message.reply_text(
                "<b>⚠️ Already in merging mode</b>\n\n"
                "Use <code>/cancel_merge</code> to cancel current session.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ Cancel", callback_data="cancel_merge_cmd")
                ]])
            )
            return
        
        # Initialize new state
        state = MergingState(user_id)
        await state_manager.set_user_state(user_id, state)
        
        help_text = get_merging_help_text()
        
        await message.reply_text(
            help_text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_merge_cmd")
            ]])
        )
    
    @app.on_message(filters.command("cancel_merge"))
    async def cancel_merge_command(client: Client, message: Message):
        """Cancel the merging process"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        await state_manager.set_cancelled(user_id, True)
        await state_manager.cleanup_user(user_id)
        
        await message.reply_text("<b>❌ Merge process cancelled.</b>")
    
    @app.on_message(filters.document | filters.video)
    async def handle_merging_files(client: Client, message: Message):
        """Handle files sent during merging process"""
        if not await is_subscribed(client, message):
            return
        
        if not message.from_user:
            return
        
        user_id = message.from_user.id
        state = await state_manager.get_user_state(user_id)
        
        if not state or not isinstance(state, MergingState):
            return
        
        # Get file information
        file_obj = message.document or message.video
        if not file_obj:
            return
        
        filename = sanitize_filename(file_obj.file_name or f"file_{message.id}")
        
        # Validate file size
        if not validate_file_size(file_obj.file_size):
            await message.reply_text(
                f"<b>⚠️ File size issue</b>\n\n"
                f"<code>{filename}</code>\n"
                f"Size: {format_size(file_obj.file_size)}\n\n"
                f"Maximum allowed: {format_size(Config.MAX_FILE_SIZE_MB * 1024 * 1024)}"
            )
            return
        
        # Create media file object
        media_file = MediaFile(
            message=message,
            file_id=file_obj.file_id,
            filename=filename,
            file_size=file_obj.file_size,
            mime_type=file_obj.mime_type or ""
        )
        
        # Add to appropriate list
        if state.state == MergeState.WAITING_SOURCE:
            state.add_source_file(media_file)
            count = len(state.source_files)
            
            if count % 2 == 0 or count == 1:
                await message.reply_text(
                    f"<b>📥 Source file added</b>\n\n"
                    f"Total source files: {count}\n"
                    f"Send <code>/done</code> when finished."
                )
        
        elif state.state == MergeState.WAITING_TARGET:
            state.add_target_file(media_file)
            count = len(state.target_files)
            
            if count % 2 == 0 or count == 1:
                await message.reply_text(
                    f"<b>📥 Target file added</b>\n\n"
                    f"Total target files: {count}\n"
                    f"Send <code>/done</code> when finished."
                )
    
    @app.on_message(filters.command("done"))
    async def done_command(client: Client, message: Message):
        """Handle /done command to proceed to next step"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        state = await state_manager.get_user_state(user_id)
        
        if not state or not isinstance(state, MergingState):
            await message.reply_text(
                "<b>❌ No active merging session</b>\n\n"
                "Use <code>/merging</code> to start."
            )
            return
        
        if state.state == MergeState.WAITING_SOURCE:
            if not state.source_files:
                await message.reply_text(
                    "<b>⚠️ No source files received</b>\n\n"
                    "Please send source files first."
                )
                return
            
            state.state = MergeState.WAITING_TARGET
            
            await message.reply_text(
                f"<b>✅ Source files received!</b>\n\n"
                f"Total: {len(state.source_files)} files\n\n"
                f"<b>Now send TARGET files</b>\n\n"
                f"• Send same number of target files\n"
                f"• Original audio will be preserved\n"
                f"• Send <code>/done</code> when finished"
            )
        
        elif state.state == MergeState.WAITING_TARGET:
            if not state.target_files:
                await message.reply_text(
                    "<b>⚠️ No target files received</b>\n\n"
                    "Please send target files first."
                )
                return
            
            # Check counts
            source_count = len(state.source_files)
            target_count = len(state.target_files)
            
            if source_count != target_count:
                await message.reply_text(
                    f"<b>⚠️ File count mismatch</b>\n\n"
                    f"Source files: {source_count}\n"
                    f"Target files: {target_count}\n\n"
                    f"Continue anyway? Only matching episodes will be processed.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("✅ Continue", callback_data="continue_merge"),
                        InlineKeyboardButton("❌ Cancel", callback_data="cancel_merge")
                    ]])
                )
                return
            
            # Start processing
            await start_merging_process(client, state, message)
    
    @app.on_callback_query(filters.regex(r"^cancel_merge_cmd$"))
    async def cancel_merge_callback(client, query):
        """Handle cancel button callback"""
        user_id = query.from_user.id
        await state_manager.cleanup_user(user_id)
        
        await query.message.edit_text("<b>❌ Merge process cancelled.</b>")
        await query.answer("Merge cancelled")
    
    @app.on_callback_query(filters.regex(r"^continue_merge$"))
    async def continue_merge_callback(client, query):
        """Handle continue merge callback"""
        user_id = query.from_user.id
        state = await state_manager.get_user_state(user_id)
        
        if state and isinstance(state, MergingState):
            await query.message.delete()
            await start_merging_process(client, state, query.message)
    
    @app.on_callback_query(filters.regex(r"^cancel_merge$"))
    async def cancel_merge_callback(client, query):
        """Handle cancel merge callback"""
        user_id = query.from_user.id
        await state_manager.cleanup_user(user_id)
        
        await query.message.edit_text("<b>❌ Merge process cancelled.</b>")
        await query.answer("Merge cancelled")
    
    @app.on_callback_query(filters.regex(r"^cancel_processing_(\d+)$"))
    async def cancel_processing_callback(client, query):
        """Handle cancel processing button"""
        try:
            user_id = int(query.data.split("_")[2])
            
            if user_id != query.from_user.id:
                await query.answer("You can only cancel your own processing!", show_alert=True)
                return
            
            await state_manager.set_cancelled(user_id, True)
            await query.answer("⏹️ Processing will be cancelled...", show_alert=True)
            
        except (IndexError, ValueError):
            await query.answer("Invalid callback", show_alert=True)

# Export the setup function
__all__ = ['setup_merging_handlers'] 
