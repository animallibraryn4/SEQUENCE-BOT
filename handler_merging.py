import os
import asyncio
import tempfile
import time
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from config import OWNER_ID
from start import is_subscribed

# Import from merging.py - IMPORTANT: silent_cleanup ko import karein
from merging import (
    MergingState, merging_users, PROCESSING_STATES, LAST_EDIT_TIME,
    get_file_extension, match_files_by_episode, merge_audio_subtitles_simple,
    smart_progress_callback, cleanup_user_throttling,
    get_merging_help_text,
    silent_cleanup  # ✅ YEH LINE ADD KAREIN
)

# Import new functions needed for parallel processing
import subprocess
import json
from concurrent.futures import ThreadPoolExecutor

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
    
    # Start the merging process in background  
    asyncio.create_task(process_merging_optimized(client, state, progress_msg))

async def analyze_media_file(file_path):
    """Analyze media file to get audio/subtitle details"""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json',
            '-show_streams', '-show_format', file_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            
            # Extract audio and subtitle streams
            audio_streams = []
            subtitle_streams = []
            
            for stream in data.get('streams', []):
                if stream.get('codec_type') == 'audio':
                    audio_info = {
                        'codec': stream.get('codec_name', 'unknown'),
                        'language': stream.get('tags', {}).get('language', 'und'),
                        'channels': stream.get('channels', 2),
                        'sample_rate': stream.get('sample_rate', '48000'),
                        'bit_rate': stream.get('bit_rate', '0'),
                        'index': stream.get('index')
                    }
                    audio_streams.append(audio_info)
                
                elif stream.get('codec_type') == 'subtitle':
                    subtitle_info = {
                        'codec': stream.get('codec_name', 'unknown'),
                        'language': stream.get('tags', {}).get('language', 'und'),
                        'index': stream.get('index')
                    }
                    subtitle_streams.append(subtitle_info)
            
            return {
                'success': True,
                'audio_streams': audio_streams,
                'subtitle_streams': subtitle_streams,
                'format': data.get('format', {}).get('format_name', 'unknown'),
                'duration': data.get('format', {}).get('duration', '0'),
                'size': data.get('format', {}).get('size', '0')
            }
        else:
            return {'success': False, 'error': 'FFprobe failed'}
    except Exception as e:
        return {'success': False, 'error': str(e)}

async def extract_tracks_parallel(target_file, temp_path, idx):
    """Extract audio and subtitle tracks from target file in parallel"""
    try:
        # Create extraction directory
        extract_dir = temp_path / f"extract_{idx}"
        extract_dir.mkdir(exist_ok=True)
        
        # Analyze target file first
        target_analysis = await analyze_media_file(target_file)
        
        if not target_analysis['success']:
            return {'success': False, 'error': 'Failed to analyze target file'}
        
        # Prepare extraction commands
        audio_tracks = []
        subtitle_tracks = []
        
        # Extract audio tracks
        for audio in target_analysis['audio_streams']:
            audio_output = extract_dir / f"audio_{audio['index']}.mka"
            cmd = [
                'ffmpeg', '-i', target_file,
                '-map', f'0:{audio["index"]}',
                '-c', 'copy',
                '-y', str(audio_output)
            ]
            
            result = subprocess.run(cmd, capture_output=True)
            if result.returncode == 0:
                audio_tracks.append(str(audio_output))
        
        # Extract subtitle tracks
        for sub in target_analysis['subtitle_streams']:
            sub_output = extract_dir / f"sub_{sub['index']}.ass"
            cmd = [
                'ffmpeg', '-i', target_file,
                '-map', f'0:{sub["index"]}',
                '-c', 'copy',
                '-y', str(sub_output)
            ]
            
            result = subprocess.run(cmd, capture_output=True)
            if result.returncode == 0:
                subtitle_tracks.append(str(sub_output))
        
        # Delete original target file after successful extraction
        silent_cleanup(target_file)
        
        return {
            'success': True,
            'audio_tracks': audio_tracks,
            'subtitle_tracks': subtitle_tracks,
            'analysis': target_analysis,
            'extract_dir': str(extract_dir)
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

async def reencode_tracks_if_needed(source_analysis, extracted_tracks, temp_path, idx):
    """Re-encode tracks if needed based on source file analysis"""
    try:
        reencode_dir = temp_path / f"reencode_{idx}"
        reencode_dir.mkdir(exist_ok=True)
        
        processed_audio = []
        processed_subs = []
        
        # Process audio tracks
        for audio_path in extracted_tracks['audio_tracks']:
            audio_file = Path(audio_path)
            
            # Check if re-encoding is needed
            # For simplicity, we'll re-encode to aac if not already aac
            analysis = await analyze_media_file(audio_path)
            if analysis['success'] and analysis['audio_streams']:
                audio_info = analysis['audio_streams'][0]
                
                output_path = reencode_dir / f"audio_{audio_info['index']}.m4a"
                
                # Check size and re-encode if needed
                if int(analysis.get('size', 0)) > 20 * 1024 * 1024:  # 20MB
                    # Re-encode with optimized settings
                    cmd = [
                        'ffmpeg', '-i', audio_path,
                        '-c:a', 'aac', '-b:a', '192k',
                        '-y', str(output_path)
                    ]
                else:
                    # Copy as is
                    cmd = [
                        'ffmpeg', '-i', audio_path,
                        '-c', 'copy',
                        '-y', str(output_path)
                    ]
                
                result = subprocess.run(cmd, capture_output=True)
                if result.returncode == 0:
                    processed_audio.append(str(output_path))
                    # Cleanup original extracted file
                    silent_cleanup(audio_path)
        
        # Process subtitle tracks
        for sub_path in extracted_tracks['subtitle_tracks']:
            sub_file = Path(sub_path)
            output_path = reencode_dir / f"sub_{sub_file.stem}.ass"
            
            # For subtitles, we usually just copy
            cmd = [
                'ffmpeg', '-i', sub_path,
                '-c', 'copy',
                '-y', str(output_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True)
            if result.returncode == 0:
                processed_subs.append(str(output_path))
                # Cleanup original extracted file
                silent_cleanup(sub_path)
        
        # Cleanup extraction directory
        if 'extract_dir' in extracted_tracks:
            import shutil
            shutil.rmtree(extracted_tracks['extract_dir'], ignore_errors=True)
        
        return {
            'success': True,
            'audio_tracks': processed_audio,
            'subtitle_tracks': processed_subs,
            'reencode_dir': str(reencode_dir)
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

async def merge_tracks_with_source(source_file, processed_tracks, output_file, progress_msg, overall_progress):
    """Merge processed tracks with source file"""
    try:
        # Analyze source file
        source_analysis = await analyze_media_file(source_file)
        
        if not source_analysis['success']:
            return {'success': False, 'error': 'Failed to analyze source file'}
        
        # Build ffmpeg command
        cmd = ['ffmpeg', '-i', source_file]
        
        # Add processed audio tracks
        for audio_path in processed_tracks['audio_tracks']:
            cmd.extend(['-i', audio_path])
        
        # Add processed subtitle tracks
        for sub_path in processed_tracks['subtitle_tracks']:
            cmd.extend(['-i', sub_path])
        
        # Map all streams
        cmd.extend(['-map', '0:v'])  # All video from source
        
        # Map all audio from source and processed tracks
        for i in range(len(source_analysis['audio_streams'])):
            cmd.extend(['-map', f'0:a:{i}'])
        
        audio_offset = 1
        for i in range(len(processed_tracks['audio_tracks'])):
            cmd.extend(['-map', f'{audio_offset}:a:0'])
            audio_offset += 1
        
        # Map all subtitles from source and processed tracks
        for i in range(len(source_analysis['subtitle_streams'])):
            cmd.extend(['-map', f'0:s:{i}'])
        
        sub_offset = 1 + len(processed_tracks['audio_tracks'])
        for i in range(len(processed_tracks['subtitle_tracks'])):
            cmd.extend(['-map', f'{sub_offset}:s:0'])
            sub_offset += 1
        
        # Copy all codecs
        cmd.extend(['-c', 'copy'])
        
        # Output file
        cmd.extend(['-y', output_file])
        
        # Run merge
        await progress_msg.edit_text(
            f"<blockquote><b>🛠️ Merging Tracks ({overall_progress})</b></blockquote>\n\n"
            f"<blockquote>📁 {Path(output_file).name}</blockquote>\n\n"
            f"<blockquote>Status: Combining tracks...</blockquote>"
        )
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            # Cleanup source file
            silent_cleanup(source_file)
            
            # Cleanup processed tracks directory
            if 'reencode_dir' in processed_tracks:
                import shutil
                shutil.rmtree(processed_tracks['reencode_dir'], ignore_errors=True)
            
            return {'success': True}
        else:
            return {'success': False, 'error': result.stderr}
            
    except Exception as e:
        return {'success': False, 'error': str(e)}

async def process_merging_optimized(client: Client, state: MergingState, progress_msg: Message):
    """Optimized merging process with parallel operations"""
    user_id = state.user_id
    msg_id = progress_msg.id
    
    # Initialize processing state for this user
    PROCESSING_STATES[user_id] = {
        "cancelled": False,
        "current_file": None,
        "progress_msg_id": msg_id
    }
    
    # Clear any previous edit time for this user
    if user_id in LAST_EDIT_TIME:
        del LAST_EDIT_TIME[user_id]
    
    try:  
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
                f"<blockquote>🔄 Starting optimized processing...</blockquote>",
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
                    
                    # --- DOWNLOAD BOTH FILES CONCURRENTLY ---
                    await progress_msg.edit_text(
                        f"<blockquote><b>⬇️ Downloading Files ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>Source: {source_data['filename']}</blockquote>\n"
                        f"<blockquote>Target: {target_data['filename']}</blockquote>\n\n"
                        f"<blockquote>Status: Starting concurrent downloads...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )
                    
                    # Download both files concurrently
                    source_filename = f"source_{idx}{get_file_extension(source_data['filename'])}"  
                    target_filename = f"target_{idx}{get_file_extension(target_data['filename'])}"
                    
                    source_file_path = temp_path / source_filename
                    target_file_path = temp_path / target_filename
                    
                    # Create download tasks
                    async def download_with_progress(message_data, file_path, file_type):
                        start_time = time.time()
                        async def progress_callback(current, total):
                            await smart_progress_callback(
                                current, total, progress_msg, start_time,
                                f"⬇️ {file_type} ({overall_progress})", 
                                message_data["filename"], user_id, msg_id
                            )
                        
                        return await client.download_media(
                            message_data["message"],
                            file_name=str(file_path),
                            progress=progress_callback
                        )
                    
                    # Download concurrently
                    download_tasks = [
                        download_with_progress(source_data, source_file_path, "Source"),
                        download_with_progress(target_data, target_file_path, "Target")
                    ]
                    
                    results = await asyncio.gather(*download_tasks, return_exceptions=True)
                    
                    source_file = results[0]
                    target_file = results[1]
                    
                    # Check for download failures
                    if not source_file or isinstance(source_file, Exception):
                        await progress_msg.edit_text(
                            f"<blockquote><b>❌ Source Download Failed</b></blockquote>\n\n"
                            f"<blockquote>📁 {source_data['filename']}</blockquote>\n"
                            f"<blockquote>Skipping to next file...</blockquote>"
                        )
                        continue
                    
                    if not target_file or isinstance(target_file, Exception):
                        silent_cleanup(source_file)
                        await progress_msg.edit_text(
                            f"<blockquote><b>❌ Target Download Failed</b></blockquote>\n\n"
                            f"<blockquote>📁 {target_data['filename']}</blockquote>\n"
                            f"<blockquote>Skipping to next file...</blockquote>"
                        )
                        continue
                    
                    # Check cancellation after downloads
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        silent_cleanup(source_file, target_file)
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # --- EXTRACT TRACKS FROM TARGET IN BACKGROUND ---
                    await progress_msg.edit_text(
                        f"<blockquote><b>🎵 Extracting Tracks ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>From: {target_data['filename']}</blockquote>\n\n"
                        f"<blockquote>Status: Extracting audio and subtitles...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )
                    
                    # Extract tracks from target
                    extracted_tracks = await extract_tracks_parallel(target_file, temp_path, idx)
                    
                    if not extracted_tracks['success']:
                        silent_cleanup(source_file, target_file)
                        await progress_msg.edit_text(
                            f"<blockquote><b>❌ Track Extraction Failed</b></blockquote>\n\n"
                            f"<blockquote>📁 {target_data['filename']}</blockquote>\n"
                            f"<blockquote>Error: {extracted_tracks.get('error', 'Unknown')}</blockquote>"
                        )
                        continue
                    
                    # Check cancellation after extraction
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        silent_cleanup(source_file)
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # --- ANALYZE SOURCE AND RE-ENCODE TRACKS ---
                    await progress_msg.edit_text(
                        f"<blockquote><b>🔧 Processing Tracks ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>Analyzing source and re-encoding tracks...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )
                    
                    # Analyze source file
                    source_analysis = await analyze_media_file(source_file)
                    
                    if not source_analysis['success']:
                        silent_cleanup(source_file)
                        await progress_msg.edit_text(
                            f"<blockquote><b>❌ Source Analysis Failed</b></blockquote>\n\n"
                            f"<blockquote>📁 {source_data['filename']}</blockquote>\n"
                            f"<blockquote>Error: {source_analysis.get('error', 'Unknown')}</blockquote>"
                        )
                        continue
                    
                    # Re-encode tracks based on source analysis
                    processed_tracks = await reencode_tracks_if_needed(
                        source_analysis, extracted_tracks, temp_path, idx
                    )
                    
                    if not processed_tracks['success']:
                        silent_cleanup(source_file)
                        await progress_msg.edit_text(
                            f"<blockquote><b>❌ Track Processing Failed</b></blockquote>\n\n"
                            f"<blockquote>Error: {processed_tracks.get('error', 'Unknown')}</blockquote>"
                        )
                        continue
                    
                    # Check cancellation after processing
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        silent_cleanup(source_file)
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # --- MERGE TRACKS WITH SOURCE ---
                    output_filename = target_data["filename"]  
                    output_file = str(temp_path / output_filename)
                    
                    merge_result = await merge_tracks_with_source(
                        source_file, processed_tracks, output_file, progress_msg, overall_progress
                    )
                    
                    if not merge_result['success']:
                        await progress_msg.edit_text(
                            f"<blockquote><b>❌ Merge Failed ({overall_progress})</b></blockquote>\n\n"
                            f"<blockquote>📁 {output_filename}</blockquote>\n"
                            f"<blockquote>Error: {merge_result.get('error', 'Unknown')}</blockquote>"
                        )
                        continue
                    
                    # Check cancellation after merge
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        if os.path.exists(output_file):
                            silent_cleanup(output_file)
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # --- UPLOAD MERGED FILE ---
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
                            f"<blockquote>🎵 Audio tracks added from target</blockquote>\n"
                            f"<blockquote>📝 Subtitle tracks added from target</blockquote>\n"
                            f"<blockquote>⚡ Optimized parallel processing</blockquote>"
                        ),
                        progress=upload_progress
                    )
                    
                    # Cleanup merged file after upload
                    silent_cleanup(output_file)
                    
                    # --- FINAL STATUS ---
                    await progress_msg.edit_text(
                        f"<blockquote><b>✅ Merge Completed ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>📁 {output_filename}</blockquote>\n"
                        f"<blockquote>🎵 Tracks optimized and merged</blockquote>\n"
                        f"<blockquote>⚡ Parallel processing completed</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )
                    
                    print(f"Successfully processed file {idx}")
                    
                except asyncio.CancelledError as e:
                    print(f"Processing cancelled by user for file {idx}")
                    raise e
                except Exception as e:
                    print(f"Error processing file {idx}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    
                    await progress_msg.edit_text(
                        f"<blockquote><b>❌ Processing Error ({idx}/{len(valid_pairs)})</b></blockquote>\n\n"
                        f"<blockquote>📁 {target_data['filename']}</blockquote>\n"
                        f"<blockquote>⚠️ Error: {str(e)[:100]}</blockquote>"
                    )
                
                # Clear throttle before next file
                if user_id in LAST_EDIT_TIME:
                    del LAST_EDIT_TIME[user_id]
                
                # Small delay to avoid flooding
                await asyncio.sleep(1)
            
            # Final completion message
            await progress_msg.edit_text(
                "<blockquote><b>✅ All Merges Completed</b></blockquote>\n\n"
                "<blockquote>🎉 All merged files have been sent to you!</blockquote>\n\n"
                "<blockquote>⚡ <i>Optimized parallel processing was used</i></blockquote>\n"
                "<blockquote>💾 <i>All temporary files have been cleaned up automatically</i></blockquote>"
            )
            
    except asyncio.CancelledError:
        print(f"Merging cancelled for user {user_id}")
        await progress_msg.edit_text(
            "<blockquote><b>❌ Processing Cancelled</b></blockquote>\n\n"
            "<blockquote>🚫 Merging process was cancelled by user.</blockquote>\n"
            "<blockquote>Use <code>/merging</code> to start again.</blockquote>"
        )
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
        # Clean up processing state
        if user_id in PROCESSING_STATES:
            del PROCESSING_STATES[user_id]
        if user_id in LAST_EDIT_TIME:
            del LAST_EDIT_TIME[user_id]
        if user_id in merging_users:
            del merging_users[user_id]

# Keep the rest of your existing handler functions unchanged
# (setup_merging_handlers and all the other handlers remain the same)

def setup_merging_handlers(app: Client):
    """Setup all merging-related handlers"""
    
    @app.on_message(filters.command("merging"))
    async def merging_command(client: Client, message: Message):
        """Start the merging process"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
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
        
        if user_id in merging_users:
            del merging_users[user_id]
        
        await query.message.edit_text(
            "<blockquote><b>❌ Merge process cancelled.</b></blockquote>"
        )
        await query.answer("Merge cancelled")
    
    @app.on_message(filters.document | filters.video)
    async def handle_merging_files(client: Client, message: Message):
        """Handle files sent during merging process"""
        if not await is_subscribed(client, message):
            return
        
        # FIX: Check if message has a from_user (could be from channel or anonymous)
        if not message.from_user:
            return  # Skip messages without from_user
        
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
            
            # Start processing with optimized function
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
            if user_id in merging_users:
                del merging_users[user_id]
            await query.message.edit_text(
                "<blockquote><b>❌ Merge process cancelled.</b></blockquote>"
            )
            await query.answer("Merge cancelled")
    
    @app.on_message(filters.command("cancel_merge"))
    async def cancel_merge_command(client: Client, message: Message):
        """Cancel the merging process"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        if user_id in merging_users:
            del merging_users[user_id]
            await message.reply_text(
                "<blockquote><b>❌ Merge process cancelled.</b></blockquote>"
            )
        else:
            await message.reply_text(
                "<blockquote>❌ No active merging session to cancel.</blockquote>"
            )
    
    # Add cancel processing callback handler
    @app.on_callback_query(filters.regex(r"^cancel_processing_(\d+)$"))
    async def cancel_processing_callback(client, query):
        """Handle cancel processing button callback"""
        user_id = int(query.data.split("_")[2])
        
        if user_id != query.from_user.id:
            await query.answer("You can only cancel your own processing!", show_alert=True)
            return
        
        if user_id in PROCESSING_STATES:
            PROCESSING_STATES[user_id]["cancelled"] = True
            await query.answer("⏹️ Processing will be cancelled...", show_alert=True)
        else:
            await query.answer("No active processing to cancel", show_alert=True)

# Export the setup function
__all__ = ['setup_merging_handlers']
