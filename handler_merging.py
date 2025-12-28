


import asyncio
import tempfile
import time
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from config import OWNER_ID
from start import is_subscribed

# Import from merging.py with ALL global states
from merging import (
    MergingState, merging_users, PROCESSING_STATES, LAST_EDIT_TIME,
    get_file_extension, match_files_by_episode, merge_audio_subtitles_simple,
    smart_progress_callback, cleanup_user_throttling,
    get_merging_help_text, MERGE_TASKS, cleanup_merging_session
)

async def start_merging_process(client: Client, state: MergingState, message: Message):
    """Start the merging process"""
    user_id = state.user_id
    state.state = "processing"
    state.total_files = min(len(state.source_files), len(state.target_files))
    
    # Send initial processing message with cancel button
    progress_msg = await message.reply_text(  
        "<blockquote><b>🔄 Starting Merge Process</b></blockquote>\n\n"  
        "<blockquote>📊 Matching files...</blockquote>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
        ])
    )
    
    # Store progress message reference in state
    state.progress_msg = progress_msg
    
    # Start the merging process in background with TASK TRACKING
    task = asyncio.create_task(process_merging(client, state, progress_msg))
    MERGE_TASKS[user_id] = task  # 🔥 Store task reference for cancellation

async def process_merging(client: Client, state: MergingState, progress_msg: Message):
    """Process the merging of all files with cancellation support"""
    user_id = state.user_id
    msg_id = progress_msg.id
    
    try:
        # Initialize processing state for this user
        PROCESSING_STATES[user_id] = {
            "cancelled": False,
            "current_file": None,
            "progress_msg_id": msg_id
        }
        
        # Clear any previous edit time for this user
        if user_id in LAST_EDIT_TIME:
            del LAST_EDIT_TIME[user_id]
        
        # Create temporary directory  
        with tempfile.TemporaryDirectory() as temp_dir:  
            temp_path = Path(temp_dir)  
              
            # Check cancellation before starting
            if PROCESSING_STATES[user_id].get("cancelled"):
                raise asyncio.CancelledError("Processing cancelled by user")
              
            # Match files by episode  
            matched_pairs = match_files_by_episode(  
                state.source_files,   
                state.target_files  
            )  
              
            # Filter out pairs without source  
            valid_pairs = [(s, t) for s, t in matched_pairs if s is not None]  
              
            if not valid_pairs:  
                await progress_msg.edit_text(  
                    "<blockquote>❌ No matching episodes found!</blockquote>\n\n"  
                    "<blockquote>Could not match source and target files by season/episode.</blockquote>"  
                )  
                return  
            
            # Send initial count info with cancel button
            await progress_msg.edit_text(
                f"<blockquote><b>📊 Files Matched</b></blockquote>\n\n"
                f"<blockquote>Total pairs: {len(valid_pairs)}</blockquote>\n"
                f"<blockquote>Skipped (no match): {len(matched_pairs) - len(valid_pairs)}</blockquote>\n\n"
                f"<blockquote>🔄 Starting processing...</blockquote>",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                ])
            )
            
            # Process each matched pair  
            for idx, (source_data, target_data) in enumerate(valid_pairs, 1):  
                try:  
                    # Check cancellation before each file
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # Update current file in processing state
                    PROCESSING_STATES[user_id]["current_file"] = target_data['filename']
                    
                    overall_progress = f"{idx}/{len(valid_pairs)}"
                    
                    # --- SOURCE DOWNLOAD ---  
                    source_filename = f"source_{idx}{get_file_extension(source_data['filename'])}"  
                    start_time = time.time()  
                      
                    await progress_msg.edit_text(  
                        f"<blockquote><b>⬇️ Downloading Source ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>📁 {source_data['filename']}</blockquote>\n\n"
                        f"<blockquote>Status: Starting download...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                      
                    # FIXED: Use a proper async callback function
                    async def source_progress(current, total):
                        await smart_progress_callback(
                            current, total, progress_msg, start_time,
                            f"⬇️ Source ({overall_progress})", 
                            source_data["filename"], user_id, msg_id
                        )
                    
                    source_file = await client.download_media(  
                        source_data["message"],  
                        file_name=str(temp_path / source_filename),  
                        progress=source_progress
                    )  
                      
                    if not source_file:  
                        print(f"Failed to download source file {idx}")  
                        await progress_msg.edit_text(
                            f"<blockquote><b>❌ Download Failed</b></blockquote>\n\n"
                            f"<blockquote>📁 {source_data['filename']}</blockquote>\n"
                            f"<blockquote>Skipping to next file...</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )
                        continue  
                    
                    # Check cancellation after source download
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # --- TARGET DOWNLOAD ---  
                    target_filename = f"target_{idx}{get_file_extension(target_data['filename'])}"  
                    start_time = time.time()  
                    
                    await progress_msg.edit_text(  
                        f"<blockquote><b>⬇️ Downloading Target ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>📁 {target_data['filename']}</blockquote>\n\n"
                        f"<blockquote>Status: Starting download...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                      
                    # FIXED: Use a proper async callback function
                    async def target_progress(current, total):
                        await smart_progress_callback(
                            current, total, progress_msg, start_time,
                            f"⬇️ Target ({overall_progress})", 
                            target_data["filename"], user_id, msg_id
                        )
                    
                    target_file = await client.download_media(  
                        target_data["message"],  
                        file_name=str(temp_path / target_filename),  
                        progress=target_progress
                    )  
                      
                    if not target_file:  
                        print(f"Failed to download target file {idx}")  
                        await progress_msg.edit_text(
                            f"<blockquote><b>❌ Download Failed</b></blockquote>\n\n"
                            f"<blockquote>📁 {target_data['filename']}</blockquote>\n"
                            f"<blockquote>Skipping to next file...</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )
                        continue  
                      
                    # Check cancellation after target download
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        raise asyncio.CancelledError("Processing cancelled by user")
                      
                    # Output file path - keep original target filename  
                    output_filename = target_data["filename"]  
                    output_file = str(temp_path / output_filename)  
                      
                    print(f"Processing pair {idx}:")  
                    print(f"  Source: {source_data['filename']}")  
                    print(f"  Target: {target_data['filename']}")  
                    print(f"  Output: {output_filename}")  
                      
                    # --- MERGE STAGE ---  
                    merge_start_time = time.time()  
                    await progress_msg.edit_text(  
                        f"<blockquote><b>🛠️ Merging ({overall_progress})</b></blockquote>\n\n"  
                        f"<blockquote>📁 {output_filename}</blockquote>\n\n"  
                        f"<blockquote>Engine : FFmpeg</blockquote>\n"  
                        f"<blockquote>Status : Processing (0%)</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                    
                    # Run merge in thread to avoid blocking
                    merge_success = False
                    try:
                        # Use threading to run ffmpeg without blocking
                        import threading
                        from queue import Queue
                        
                        result_queue = Queue()
                        
                        def run_merge():
                            try:
                                # Check cancellation during merge
                                if PROCESSING_STATES.get(user_id, {}).get("cancelled"):
                                    result_queue.put(("cancelled", None))
                                    return
                                    
                                success = merge_audio_subtitles_simple(source_file, target_file, output_file)
                                result_queue.put(("success", success))
                            except Exception as e:
                                result_queue.put(("error", str(e)))
                        
                        # Start merge thread
                        merge_thread = threading.Thread(target=run_merge)
                        merge_thread.daemon = True
                        merge_thread.start()
                        
                        # Update merge progress periodically
                        while merge_thread.is_alive():
                            # Check cancellation
                            if PROCESSING_STATES[user_id].get("cancelled"):
                                raise asyncio.CancelledError("Processing cancelled by user")
                                
                            elapsed = time.time() - merge_start_time
                            progress_text = (
                                f"<blockquote><b>🛠️ Merging ({overall_progress})</b></blockquote>\n\n"  
                                f"<blockquote>📁 {output_filename}</blockquote>\n\n"  
                                f"<blockquote>Engine : FFmpeg</blockquote>\n"  
                                f"<blockquote>Status : Processing ({elapsed:.0f}s elapsed)</blockquote>"  
                            )
                            try:
                                await progress_msg.edit_text(
                                    progress_text,
                                    reply_markup=InlineKeyboardMarkup([
                                        [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                                    ])
                                )
                            except:
                                pass
                            await asyncio.sleep(2)  # Update every 2 seconds
                        
                        # Get result
                        if not result_queue.empty():
                            result_type, result = result_queue.get()
                            if result_type == "success":
                                merge_success = result
                            elif result_type == "cancelled":
                                raise asyncio.CancelledError("Processing cancelled by user")
                            else:
                                print(f"Merge error: {result}")
                                merge_success = False
                        else:
                            merge_success = False
                            
                    except Exception as e:
                        print(f"Merge thread error: {str(e)}")
                        merge_success = False
                      
                    # Check cancellation after merge
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        raise asyncio.CancelledError("Processing cancelled by user")
                      
                    if merge_success:  
                        # --- UPLOAD STAGE ---  
                        start_time = time.time()  
                        
                        # Clear throttle for upload
                        if user_id in LAST_EDIT_TIME:
                            del LAST_EDIT_TIME[user_id]
                          
                        await progress_msg.edit_text(  
                            f"<blockquote><b>⬆️ Uploading ({overall_progress})</b></blockquote>\n\n"
                            f"<blockquote>📁 {output_filename}</blockquote>\n\n"
                            f"<blockquote>Status: Starting upload...</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )  
                          
                        # FIXED: Use a proper async callback function for upload
                        async def upload_progress(current, total):
                            await smart_progress_callback(
                                current, total, progress_msg, start_time,
                                f"⬆️ Upload ({overall_progress})", 
                                output_filename, user_id, msg_id
                            )
                          
                        await client.send_document(  
                            chat_id=user_id,  
                            document=output_file,  
                            caption=(  
                                f"<blockquote>✅ <b>Merged File</b></blockquote>\n"  
                                f"<blockquote>📁 {target_data['filename']}</blockquote>\n"  
                                f"<blockquote>🎵 Audio tracks added from source</blockquote>\n"  
                                f"<blockquote>📝 Subtitle tracks added from source</blockquote>"  
                            ),  
                            progress=upload_progress
                        )  
                          
                        # --- FINAL STATUS FOR THIS FILE ---  
                        await progress_msg.edit_text(  
                            f"<blockquote><b>✅ Merge Completed ({overall_progress})</b></blockquote>\n\n"  
                            f"<blockquote>📁 {output_filename}</blockquote>\n"  
                            f"<blockquote>🎵 Source audio set as DEFAULT</blockquote>\n"  
                            f"<blockquote>🎯 No quality loss</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )  
                          
                        print(f"Successfully merged file {idx}")  
                    else:  
                        await progress_msg.edit_text(  
                            f"<blockquote><b>❌ Merge Failed ({overall_progress})</b></blockquote>\n\n"  
                            f"<blockquote>📁 {target_data['filename']}</blockquote>\n"  
                            f"<blockquote>⚠️ This file may be incompatible or corrupted</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )  
                        print(f"Failed to merge file {idx}")  
                      
                except asyncio.CancelledError:
                    # User cancelled processing
                    print(f"Processing cancelled by user for file {idx}")
                    raise  # Re-raise to exit loop
                except Exception as e:  
                    print(f"Error processing file {idx}: {str(e)}")  
                    await progress_msg.edit_text(  
                        f"<blockquote><b>❌ Processing Error ({idx}/{len(valid_pairs)})</b></blockquote>\n\n"  
                        f"<blockquote>📁 {target_data['filename']}</blockquote>\n"  
                        f"<blockquote>⚠️ Error: {str(e)[:100]}</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                  
                # Clear throttle before next file
                if user_id in LAST_EDIT_TIME:
                    del LAST_EDIT_TIME[user_id]
                
                # Small delay to avoid flooding  
                await asyncio.sleep(1)  
              
            # Final completion message  
            await progress_msg.edit_text(  
                "<blockquote><b>✅ All Merges Completed</b></blockquote>\n\n"  
                "<blockquote>🎉 All merged files have been sent to you!</blockquote>"  
            )  
              
    except asyncio.CancelledError:
        # Handle cancellation
        # Handle cancellation
        print(f"Merging cancelled for user {user_id}")
        await progress_msg.edit_text(  
            "<blockquote><b>❌ Processing Cancelled</b></blockquote>\n\n"  
            "<blockquote>🚫 Merging process was cancelled by user.</blockquote>\n"
            "<blockquote>Use <code>/merging</code> to start again.</blockquote>"  
        )
        raise  # Re-raise to trigger cleanup in finally block
        
    except Exception as e:  
        print(f"Merge process error: {str(e)}")  
        import traceback  
        traceback.print_exc()  
        try:  
            await progress_msg.edit_text(  
                "<blockquote>❌ An error occurred during merging.</blockquote>\n"  
                "<blockquote>Please try again with different files.</blockquote>"  
            )  
        except:  
            pass  
    
    finally:
        # 🔥 CENTRAL CLEANUP - Guaranteed to run
        await cleanup_merging_session(
            user_id,
            progress_msg,
            reason="finished / crash / cancel"
        )

def setup_merging_handlers(app: Client):
    """Setup all merging-related handlers"""

    @app.on_callback_query(filters.regex(r"^cancel_processing_(\d+)$"))
    async def cancel_processing_callback(client, query):
        user_id = int(query.data.split("_")[2])

        # Security: user sirf apna hi process cancel kar sakta hai
        if user_id != query.from_user.id:
            await query.answer("Not allowed", show_alert=True)
            return

        # 🔔 Sirf CANCEL SIGNAL bhejo
        if user_id in PROCESSING_STATES:
            PROCESSING_STATES[user_id]["cancelled"] = True

        task = MERGE_TASKS.get(user_id)
        if task and not task.done():
            task.cancel()

        # ✅ Instant feedback (VERY IMPORTANT)
        await query.answer("⏹️ Cancelling…", show_alert=False)    
    
    @app.on_message(filters.command("merging"))
    async def merging_command(client: Client, message: Message):
        """Start the merging process"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        # Clean up any existing session first
        if user_id in merging_users:
            await cleanup_merging_session(
                user_id,
                reason="new session started"
            )
        
        # Initialize merging state
        merging_users[user_id] = MergingState(user_id)
        
        help_text = (
            "<blockquote><b>🔧 AUTO FILE MERGING MODE</b></blockquote>\n\n"
            "<blockquote>Please send the SOURCE FILES from which you want to extract audio and subtitles.</blockquote>\n\n"
            "<blockquote><b>📝 Instructions:</b>\n"
            "1. Send all source files (with desired audio/subtitle tracks)\n"
            "2. Send <code>/done</code> when finished\n"
            "3. Send all target files (to add tracks to)\n"
            "4. Send <code>/done</code> again\n"
            "5. Wait for processing</blockquote>\n\n"
            "<blockquote><b>⚠️ Requirements:</b>\n"
            "- Files should be MKV format for best results\n"
            "- Files should have similar naming for auto-matching\n"
            "- Bot needs ffmpeg installed on server</blockquote>"
        )
        
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_merge_cmd")]
        ])
        
        await message.reply_text(help_text, reply_markup=buttons)
    
    @app.on_callback_query(filters.regex(r"^cancel_merge_cmd$"))
    async def cancel_merge_callback(client, query):
        """Handle cancel button callback"""
        user_id = query.from_user.id
        
        await cleanup_merging_session(
            user_id,
            query.message,
            reason="cancel button"
        )
        
        await query.answer("Merge cancelled")
    
    @app.on_message(filters.document | filters.video)
    async def handle_merging_files(client: Client, message: Message):
        """Handle files sent during merging process"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        if user_id not in merging_users:
            return
        
        state = merging_users[user_id]
        file_obj = message.document or message.video
        
        if not file_obj:
            return
        
        # Get filename
        filename = file_obj.file_name or f"file_{message.id}"
        mime_type = file_obj.mime_type or ""
        
        # Check if it's a video file
        if not any(x in mime_type for x in ['video', 'octet-stream', 'x-matroska']):
            await message.reply_text(
                f"<blockquote>⚠️ Skipping non-video file: {filename}</blockquote>"
            )
            return
        
        file_data = {
            "message": message,
            "filename": filename,
            "file_id": file_obj.file_id,
            "file_size": file_obj.file_size,
            "mime_type": mime_type
        }
        
        if state.state == "waiting_for_source":
            state.source_files.append(file_data)
            
            # Send confirmation
            if len(state.source_files) % 3 == 0 or len(state.source_files) == 1:
                await message.reply_text(
                    f"<blockquote>📥 Received {len(state.source_files)} source files.</blockquote>\n"
                    f"<blockquote>Send <code>/done</code> when finished with source files.</blockquote>"
                )
                
        elif state.state == "waiting_for_target":
            state.target_files.append(file_data)
            
            # Send confirmation
            if len(state.target_files) % 3 == 0 or len(state.target_files) == 1:
                await message.reply_text(
                    f"<blockquote>📥 Received {len(state.target_files)} target files.</blockquote>\n"
                    f"<blockquote>Send <code>/done</code> when finished with target files.</blockquote>"
                )
    
    @app.on_message(filters.command("done"))
    async def done_command(client: Client, message: Message):
        """Handle /done command to proceed to next step"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        if user_id not in merging_users:
            await message.reply_text(
                "<blockquote>❌ No active merging session. Use <code>/merging</code> to start.</blockquote>"
            )
            return
        
        state = merging_users[user_id]
        
        if state.state == "waiting_for_source":
            if not state.source_files:
                await message.reply_text(
                    "<blockquote>❌ No source files received yet.</blockquote>\n"
                    "<blockquote>Please send source files first.</blockquote>"
                )
                return
            
            state.state = "waiting_for_target"
            
            await message.reply_text(
                f"<blockquote><b>✅ Source files received!</b></blockquote>\n\n"
                f"<blockquote>Total source files: {len(state.source_files)}</blockquote>\n\n"
                f"<blockquote><b>Now send me the TARGET files.</b></blockquote>\n\n"
                f"<blockquote><i>📝 Note: Send the same number of target files</i></blockquote>"
            )
            
        elif state.state == "waiting_for_target":
            if not state.target_files:
                await message.reply_text(
                    "<blockquote>❌ No target files received yet.</blockquote>\n"
                    "<blockquote>Please send target files first.</blockquote>"
                )
                return
            
            # Check if counts match
            if len(state.source_files) != len(state.target_files):
                await message.reply_text(
                    f"<blockquote>⚠️ File count mismatch!</blockquote>\n\n"
                    f"<blockquote>Source files: {len(state.source_files)}\n"
                    f"Target files: {len(state.target_files)}</blockquote>\n\n"
                    f"<blockquote>You can continue anyway, but only matching episodes will be processed.</blockquote>",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Continue Anyway", callback_data="continue_merge")],
                        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_merge")]
                    ])
                )
                return
            
            # Start processing
            await start_merging_process(client, state, message)
            
        else:
            await message.reply_text(
                "<blockquote>❌ Invalid state. Use <code>/cancel_merge</code> to reset.</blockquote>"
            )
    
    @app.on_callback_query(filters.regex(r"^(continue_merge|cancel_merge)$"))
    async def merge_control_callback(client, query):
        """Handle merge control callbacks"""
        user_id = query.from_user.id
        action = query.data
        
        if user_id not in merging_users:
            await query.answer("Session expired", show_alert=True)
            return
        
        state = merging_users[user_id]
        
        if action == "continue_merge":
            await query.message.delete()
            await start_merging_process(client, state, query.message)
            
        elif action == "cancel_merge":
            await cleanup_merging_session(
                user_id,
                query.message,
                reason="cancel callback"
            )
            await query.answer("Merge cancelled")
    
    @app.on_message(filters.command("cancel_merge"))
    async def cancel_merge_command(client: Client, message: Message):
        """Cancel the merging process"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        if user_id not in merging_users:
            await message.reply_text(
                "<blockquote>❌ No active merging session to cancel.</blockquote>"
            )
            return
        
        # 🔥 REAL CANCELLATION - Uses central cleanup function
        await cleanup_merging_session(
            user_id,
            reason="cancel command"
        )
        
        await message.reply_text(
            "<blockquote><b>❌ Merge process cancelled and cleaned up.</b></blockquote>"
        )
    
    # Add cancel processing callback handler
    @app.on_callback_query(filters.regex(r"^cancel_processing_(\d+)$"))
    async def cancel_processing_callback(client, query):
        """Handle cancel processing button callback"""
        user_id = int(query.data.split("_")[2])
        
        if user_id != query.from_user.id:
            await query.answer("You can only cancel your own processing!", show_alert=True)
            return
        
        # 🔥 REAL CANCELLATION - Uses central cleanup function
        await cleanup_merging_session(
            user_id,
            query.message,
            reason="cancel button"
        )
        
        await query.answer("⏹️ Processing cancelled", show_alert=True)

# Export the setup function
__all__ = ['setup_merging_handlers']
