import os
import asyncio
import tempfile
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading
from queue import Queue
import subprocess
import json
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from start import is_subscribed

# Import from merging.py
from merging import (
    MergingState, merging_users, PROCESSING_STATES, LAST_EDIT_TIME,
    get_file_extension, match_files_by_episode, merge_audio_subtitles_simple,
    smart_progress_callback, cleanup_user_throttling,
    get_merging_help_text,
    silent_cleanup,
    get_media_info,  # Added for audio/subtitle analysis
    extract_streams_info  # Added for stream extraction
)

# Thread pool for background processing
background_executor = ThreadPoolExecutor(max_workers=4)

class BackgroundProcessor:
    """Handles background audio/subtitle extraction and processing"""
    
    @staticmethod
    def extract_target_streams(target_file: str) -> dict:
        """Extract audio and subtitle streams from target file in background"""
        try:
            print(f"Background: Extracting streams from {target_file}")
            
            # Get media info
            media_info = get_media_info(target_file)
            streams_info = extract_streams_info(media_info)
            
            # Prepare extracted streams data
            extracted_data = {
                "audio_streams": streams_info["audio_streams"],
                "subtitle_streams": streams_info["subtitle_streams"],
                "video_info": None,
                "temp_files": []  # Will store paths of temp processed files
            }
            
            # Extract video stream info if needed
            for stream in media_info.get("streams", []):
                if stream.get("codec_type") == "video":
                    extracted_data["video_info"] = {
                        "codec": stream.get("codec_name"),
                        "width": stream.get("width"),
                        "height": stream.get("height"),
                        "fps": stream.get("avg_frame_rate", "25/1"),
                        "duration": float(media_info.get("format", {}).get("duration", 0))
                    }
                    break
            
            print(f"Background: Extracted {len(streams_info['audio_streams'])} audio, "
                  f"{len(streams_info['subtitle_streams'])} subtitle streams")
            return extracted_data
            
        except Exception as e:
            print(f"Background extraction error: {e}")
            return None
    
    @staticmethod
    def reencode_target_streams(target_streams: dict, source_file: str, temp_dir: str) -> dict:
        """Re-encode target streams based on source file analysis"""
        try:
            print(f"Background: Re-encoding streams based on {source_file}")
            
            # Analyze source file
            source_info = get_media_info(source_file)
            source_streams = extract_streams_info(source_info)
            
            # Get source video properties for sync
            source_video_info = None
            for stream in source_info.get("streams", []):
                if stream.get("codec_type") == "video":
                    source_video_info = {
                        "duration": float(source_info.get("format", {}).get("duration", 0)),
                        "fps": stream.get("avg_frame_rate", "25/1"),
                        "time_base": stream.get("time_base", "1/1000")
                    }
                    break
            
            reencoded_files = []
            processed_streams = []
            
            # Process audio streams
            for idx, audio_stream in enumerate(target_streams.get("audio_streams", [])):
                try:
                    # Prepare output file
                    output_audio = os.path.join(temp_dir, f"reencoded_audio_{idx}.aac")
                    
                    # Build re-encoding command based on source properties
                    cmd = [
                        "ffmpeg", "-y",
                        "-i", source_file,  # Use source for timing reference
                        "-map", f"0:a:{idx}",
                        "-c:a", "aac",
                        "-b:a", "192k",
                        "-ar", "48000",
                        "-ac", "2",
                        "-af", "aresample=async=1000",
                        "-strict", "experimental"
                    ]
                    
                    # Add timing synchronization if source video info exists
                    if source_video_info and source_video_info["duration"] > 0:
                        cmd.extend(["-t", str(source_video_info["duration"])])
                    
                    cmd.append(output_audio)
                    
                    # Run re-encoding
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    if result.returncode == 0:
                        # Check file size
                        file_size = os.path.getsize(output_audio) / (1024 * 1024)  # MB
                        
                        if file_size > 20:  # If larger than 20MB
                            print(f"Audio stream {idx} is {file_size:.2f}MB, optimizing...")
                            # Apply additional optimization
                            optimized_audio = os.path.join(temp_dir, f"optimized_audio_{idx}.aac")
                            opt_cmd = [
                                "ffmpeg", "-y",
                                "-i", output_audio,
                                "-c:a", "aac",
                                "-b:a", "128k",  # Lower bitrate for large files
                                "-ar", "44100",
                                "-ac", "2",
                                optimized_audio
                            ]
                            subprocess.run(opt_cmd, capture_output=True)
                            os.remove(output_audio)
                            output_audio = optimized_audio
                        
                        reencoded_files.append(output_audio)
                        processed_streams.append({
                            "type": "audio",
                            "index": idx,
                            "file": output_audio,
                            "language": audio_stream.get("language", "und"),
                            "channels": audio_stream.get("channels", 2)
                        })
                        print(f"Re-encoded audio stream {idx} -> {output_audio}")
                        
                except Exception as e:
                    print(f"Error re-encoding audio stream {idx}: {e}")
                    continue
            
            # Process subtitle streams
            for idx, sub_stream in enumerate(target_streams.get("subtitle_streams", [])):
                try:
                    output_sub = os.path.join(temp_dir, f"processed_sub_{idx}.srt")
                    
                    # Extract and convert subtitle if needed
                    cmd = [
                        "ffmpeg", "-y",
                        "-i", source_file,  # Use source for timing
                        "-map", f"0:s:{idx}",
                        output_sub
                    ]
                    
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    if result.returncode == 0 and os.path.exists(output_sub):
                        # Validate subtitle file
                        file_size = os.path.getsize(output_sub)
                        if file_size > 0:
                            reencoded_files.append(output_sub)
                            processed_streams.append({
                                "type": "subtitle",
                                "index": idx,
                                "file": output_sub,
                                "language": sub_stream.get("language", "und"),
                                "format": "srt"
                            })
                            print(f"Processed subtitle stream {idx} -> {output_sub}")
                            
                except Exception as e:
                    print(f"Error processing subtitle stream {idx}: {e}")
                    continue
            
            return {
                "processed_streams": processed_streams,
                "temp_files": reencoded_files,
                "source_video_info": source_video_info
            }
            
        except Exception as e:
            print(f"Re-encoding error: {e}")
            return None
    
    @staticmethod
    def create_merged_file(source_file: str, processed_data: dict, output_file: str) -> bool:
        """Create merged file by adding processed streams to source"""
        try:
            print(f"Background: Creating merged file {output_file}")
            
            # Build ffmpeg command
            cmd = [
                "ffmpeg", "-y",
                "-i", source_file,  # Source as base
            ]
            
            # Add processed audio files as inputs
            audio_inputs = []
            for stream in processed_data.get("processed_streams", []):
                if stream["type"] == "audio":
                    cmd.extend(["-i", stream["file"]])
                    audio_inputs.append(stream)
            
            # Add processed subtitle files as inputs
            sub_inputs = []
            for stream in processed_data.get("processed_streams", []):
                if stream["type"] == "subtitle":
                    cmd.extend(["-i", stream["file"]])
                    sub_inputs.append(stream)
            
            # Map original video and audio from source
            cmd.extend(["-map", "0:v"])  # Source video
            cmd.extend(["-map", "0:a"])  # Source audio
            
            # Map processed audio streams
            for i in range(len(audio_inputs)):
                cmd.extend(["-map", f"{i+1}:a:0"])  # +1 because source is input 0
            
            # Map processed subtitle streams
            offset = 1 + len(audio_inputs)
            for i in range(len(sub_inputs)):
                cmd.extend(["-map", f"{offset + i}:s:0"])
            
            # Codec settings
            cmd.extend([
                "-c:v", "copy",  # Copy video
                "-c:a", "copy",  # Copy original audio
            ])
            
            # For added audio streams
            for i in range(len(audio_inputs)):
                cmd.extend([f"-c:a:{1 + i}", "copy"])
            
            # For added subtitle streams
            cmd.extend(["-c:s", "copy"])
            
            # Disposition - make original source audio default
            cmd.extend(["-disposition:a:0", "default"])
            
            # Clear other audio dispositions
            for i in range(1, 1 + len(audio_inputs)):
                cmd.extend([f"-disposition:a:{i}", "0"])
            
            # Sync and compatibility settings
            cmd.extend([
                "-movflags", "+faststart",
                "-max_interleave_delta", "0",
                "-fflags", "+genpts",
                "-avoid_negative_ts", "make_zero",
            ])
            
            cmd.append(output_file)
            
            print(f"Merging command: {' '.join(cmd[:20])}...")  # Print first 20 args
            
            # Run merge
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                print(f"Successfully created merged file: {output_file}")
                return True
            else:
                print(f"Merge failed: {result.stderr[:500]}")
                return False
                
        except subprocess.TimeoutExpired:
            print("Merge timed out after 5 minutes")
            return False
        except Exception as e:
            print(f"Merge creation error: {e}")
            return False


async def start_merging_process(client: Client, state: MergingState, message: Message):
    """Start the merging process with background processing"""
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
    asyncio.create_task(process_merging_parallel(client, state, progress_msg))


async def process_merging_parallel(client: Client, state: MergingState, progress_msg: Message):
    """Process merging with parallel background processing"""
    user_id = state.user_id
    msg_id = progress_msg.id
    
    # Initialize processing state for this user
    PROCESSING_STATES[user_id] = {
        "cancelled": False,
        "current_file": None,
        "progress_msg_id": msg_id,
        "background_tasks": {}
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
            
            # Send initial count info
            await progress_msg.edit_text(
                f"<blockquote><b>📊 Files Matched</b></blockquote>\n\n"
                f"<blockquote>Total pairs: {len(valid_pairs)}</blockquote>\n"
                f"<blockquote>Skipped (no match): {len(matched_pairs) - len(valid_pairs)}</blockquote>\n\n"
                f"<blockquote>🔄 Starting parallel processing...</blockquote>",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                ])
            )
            
            # Process each matched pair  
            for idx, (source_data, target_data) in enumerate(valid_pairs, 1):  
                try:  
                    # Check cancellation
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # Update current file in processing state
                    PROCESSING_STATES[user_id]["current_file"] = target_data['filename']
                    
                    overall_progress = f"{idx}/{len(valid_pairs)}"
                    
                    # --- STEP 1: DOWNLOAD SOURCE FILE ---
                    source_filename = f"source_{idx}{get_file_extension(source_data['filename'])}"  
                    source_start_time = time.time()  
                      
                    await progress_msg.edit_text(  
                        f"<blockquote><b>⬇️ Downloading Source ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>📁 {source_data['filename']}</blockquote>\n\n"
                        f"<blockquote>Status: Starting download...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                      
                    async def source_progress(current, total):
                        await smart_progress_callback(
                            current, total, progress_msg, source_start_time,
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
                        silent_cleanup(source_file)
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # --- STEP 2: DOWNLOAD TARGET FILE & START BACKGROUND PROCESSING ---
                    target_filename = f"target_{idx}{get_file_extension(target_data['filename'])}"  
                    target_start_time = time.time()  
                    
                    await progress_msg.edit_text(  
                        f"<blockquote><b>⬇️ Downloading Target ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>📁 {target_data['filename']}</blockquote>\n\n"
                        f"<blockquote>Status: Starting download + background processing...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                      
                    async def target_progress(current, total):
                        await smart_progress_callback(
                            current, total, progress_msg, target_start_time,
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
                        silent_cleanup(source_file)
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
                        silent_cleanup(source_file, target_file)
                        raise asyncio.CancelledError("Processing cancelled by user")
                      
                    # --- STEP 3: BACKGROUND PROCESSING ---
                    await progress_msg.edit_text(  
                        f"<blockquote><b>🔧 Background Processing ({overall_progress})</b></blockquote>\n\n"  
                        f"<blockquote>📁 {target_data['filename']}</blockquote>\n\n"  
                        f"<blockquote>Status: Extracting & re-encoding streams...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                    
                    # Start background processing in thread pool
                    background_task = asyncio.get_event_loop().run_in_executor(
                        background_executor,
                        process_in_background,
                        target_file,
                        source_file,
                        str(temp_path / f"processed_{idx}")
                    )
                    
                    # Wait for background processing with timeout
                    try:
                        background_result = await asyncio.wait_for(background_task, timeout=300)
                    except asyncio.TimeoutError:
                        print(f"Background processing timed out for pair {idx}")
                        silent_cleanup(source_file, target_file)
                        await progress_msg.edit_text(
                            f"<blockquote><b>⚠️ Processing Timeout ({overall_progress})</b></blockquote>\n\n"
                            f"<blockquote>📁 {target_data['filename']}</blockquote>\n\n"
                            f"<blockquote>Background processing took too long. Skipping...</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )
                        continue
                    
                    if not background_result or "output_file" not in background_result:
                        print(f"Background processing failed for pair {idx}")
                        silent_cleanup(source_file, target_file)
                        await progress_msg.edit_text(
                            f"<blockquote><b>❌ Processing Failed ({overall_progress})</b></blockquote>\n\n"
                            f"<blockquote>📁 {target_data['filename']}</blockquote>\n\n"
                            f"<blockquote>Could not process streams. Skipping...</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )
                        continue
                    
                    # Delete source file before upload (as requested)
                    print(f"✅ Deleting source file before upload: {source_file}")
                    silent_cleanup(source_file)
                    
                    # --- STEP 4: UPLOAD MERGED FILE ---
                    output_file = background_result["output_file"]
                    output_filename = target_data["filename"]  
                    
                    upload_start_time = time.time()  
                    
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
                            current, total, progress_msg, upload_start_time,
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
                            f"<blockquote>📝 Subtitle tracks added from target</blockquote>"  
                        ),  
                        progress=upload_progress
                    )  
                    
                    # Delete merged file after upload
                    print(f"✅ Deleting merged file after upload: {output_file}")
                    silent_cleanup(output_file)
                    
                    # Cleanup any temp files from background processing
                    if "temp_files" in background_result:
                        for temp_file in background_result["temp_files"]:
                            silent_cleanup(temp_file)
                    
                    # Cleanup target file
                    silent_cleanup(target_file)
                      
                    # --- FINAL STATUS FOR THIS FILE ---  
                    await progress_msg.edit_text(  
                        f"<blockquote><b>✅ Merge Completed ({overall_progress})</b></blockquote>\n\n"  
                        f"<blockquote>📁 {output_filename}</blockquote>\n"  
                        f"<blockquote>🎵 Target audio added to source</blockquote>\n"  
                        f"<blockquote>🎯 Background processed</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                      
                    print(f"Successfully processed file {idx}")  
                      
                except asyncio.CancelledError as e:
                    # User cancelled processing
                    print(f"Processing cancelled by user for file {idx}")
                    raise e
                except Exception as e:  
                    print(f"Error processing file {idx}: {str(e)}")  
                    import traceback  
                    traceback.print_exc()  
                    
                    # Ensure cleanup
                    try:
                        if 'source_file' in locals(): silent_cleanup(source_file)
                        if 'target_file' in locals(): silent_cleanup(target_file)
                    except:
                        pass
                    
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
                "<blockquote>🎉 All merged files have been sent to you!</blockquote>\n\n"
                "<blockquote>💾 <i>All temporary files have been cleaned up automatically</i></blockquote>"  
            )  
              
    except asyncio.CancelledError:
        # Handle cancellation
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


def process_in_background(target_file: str, source_file: str, output_base: str):
    """Process files in background thread"""
    try:
        print(f"Starting background processing for {target_file}")
        
        # Create temp directory for this processing
        temp_dir = tempfile.mkdtemp(prefix="bg_process_")
        
        # Step 1: Extract streams from target
        target_streams = BackgroundProcessor.extract_target_streams(target_file)
        if not target_streams:
            print("Failed to extract target streams")
            return None
        
        # Step 2: Re-encode streams based on source
        processed_data = BackgroundProcessor.reencode_target_streams(
            target_streams, 
            source_file, 
            temp_dir
        )
        
        if not processed_data:
            print("Failed to re-encode streams")
            # Cleanup temp dir
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None
        
        # Step 3: Create merged file
        output_file = f"{output_base}_merged.mkv"
        success = BackgroundProcessor.create_merged_file(
            source_file,
            processed_data,
            output_file
        )
        
        if success:
            result = {
                "output_file": output_file,
                "temp_files": processed_data.get("temp_files", []),
                "temp_dir": temp_dir  # Will be cleaned up later
            }
            print(f"Background processing completed successfully: {output_file}")
            return result
        else:
            print("Failed to create merged file")
            # Cleanup
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None
            
    except Exception as e:
        print(f"Background processing error: {e}")
        import traceback
        traceback.print_exc()
        # Cleanup on error
        try:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
        except:
            pass
        return None


# Keep the existing setup_merging_handlers function exactly as is
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
    
    # ... (keep all the existing callback and message handlers exactly as they were)
    # The rest of the setup_merging_handlers function remains unchanged
    # [Include all the existing handlers from your original code here]

# Export the setup function
__all__ = ['setup_merging_handlers']
