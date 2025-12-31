import os
import asyncio
import tempfile
import time
import math
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from config import OWNER_ID
from start import is_subscribed

# Import from merging.py
from merging import (
    MergingState, merging_users, PROCESSING_STATES, LAST_EDIT_TIME,
    get_file_extension, match_files_by_episode, merge_audio_subtitles_simple,
    smart_progress_callback, cleanup_user_throttling,
    get_merging_help_text,
    silent_cleanup,
    get_media_info,  # Added for media analysis
    extract_streams_info  # Added for stream extraction
)

# New imports for audio processing
import subprocess
import json

# Size limit for audio compression (20 MB)
MAX_AUDIO_SIZE_MB = 20
MAX_AUDIO_SIZE_BYTES = MAX_AUDIO_SIZE_MB * 1024 * 1024

async def compress_audio_if_needed(audio_path: str, target_duration: float, source_codec: str) -> str:
    """
    Compress audio if size exceeds 20MB
    Returns the path to compressed audio (same path if no compression needed)
    """
    try:
        # Get current audio size
        current_size = os.path.getsize(audio_path)
        
        if current_size <= MAX_AUDIO_SIZE_BYTES:
            print(f"Audio size {current_size/1024/1024:.2f}MB <= {MAX_AUDIO_SIZE_MB}MB, no compression needed")
            return audio_path
        
        print(f"Audio size {current_size/1024/1024:.2f}MB > {MAX_AUDIO_SIZE_MB}MB, compressing...")
        
        # Calculate target bitrate based on duration and size limit
        target_bitrate_kbps = int((MAX_AUDIO_SIZE_BYTES * 8) / (target_duration * 1000))
        
        # Ensure minimum bitrate for quality
        min_bitrate = 64 if source_codec.lower() == 'opus' else 96
        target_bitrate_kbps = max(target_bitrate_kbps, min_bitrate)
        
        # Cap maximum bitrate
        max_bitrate = 256 if source_codec.lower() == 'opus' else 320
        target_bitrate_kbps = min(target_bitrate_kbps, max_bitrate)
        
        print(f"Target bitrate: {target_bitrate_kbps}kbps for {target_duration:.1f}s duration")
        
        # Create temporary compressed file
        temp_dir = Path(audio_path).parent
        compressed_path = str(temp_dir / f"compressed_{Path(audio_path).name}")
        
        # Choose codec based on source
        if source_codec.lower() == 'opus':
            codec = 'libopus'
            codec_args = ['-c:a', 'libopus', '-b:a', f'{target_bitrate_kbps}k', '-vbr', 'on']
        else:
            codec = 'aac'
            codec_args = ['-c:a', 'aac', '-b:a', f'{target_bitrate_kbps}k', '-profile:a', 'aac_low']
        
        # Compression command
        cmd = [
            'ffmpeg', '-y',
            '-i', audio_path,
            *codec_args,
            '-ar', '48000',
            '-ac', '2',
            compressed_path
        ]
        
        print(f"Compression command: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            compressed_size = os.path.getsize(compressed_path)
            print(f"Compression successful: {compressed_size/1024/1024:.2f}MB")
            
            # Delete original and rename compressed
            silent_cleanup(audio_path)
            return compressed_path
        else:
            print(f"Compression failed: {result.stderr[:500]}")
            return audio_path
            
    except Exception as e:
        print(f"Error in audio compression: {e}")
        return audio_path

async def extract_audio_from_target(target_path: str, temp_dir: Path, audio_index: int = 0) -> str:
    """Extract audio track from target file"""
    try:
        audio_path = str(temp_dir / f"target_audio_{audio_index}.mka")
        
        cmd = [
            'ffmpeg', '-y',
            '-i', target_path,
            '-map', f'0:a:{audio_index}',
            '-c', 'copy',
            audio_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0 and os.path.exists(audio_path):
            return audio_path
        else:
            print(f"Audio extraction failed: {result.stderr[:500]}")
            return None
            
    except Exception as e:
        print(f"Error extracting audio: {e}")
        return None

async def extract_subtitles_from_target(target_path: str, temp_dir: Path, sub_index: int = 0) -> str:
    """Extract subtitle track from target file"""
    try:
        sub_path = str(temp_dir / f"target_sub_{sub_index}.srt")
        
        cmd = [
            'ffmpeg', '-y',
            '-i', target_path,
            '-map', f'0:s:{sub_index}',
            '-c', 'srt',
            sub_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0 and os.path.exists(sub_path):
            return sub_path
        else:
            print(f"Subtitle extraction failed: {result.stderr[:500]}")
            return None
            
    except Exception as e:
        print(f"Error extracting subtitles: {e}")
        return None

async def reencode_tracks_to_match_source(source_info: dict, target_audio_path: str, target_sub_path: str, temp_dir: Path) -> tuple:
    """
    Re-encode extracted tracks to match source file specifications
    Returns: (reencoded_audio_path, reencoded_sub_path)
    """
    try:
        # Analyze source audio specs
        source_audio_info = None
        source_streams = extract_streams_info(source_info)
        
        if source_streams["audio_streams"]:
            source_audio_info = source_streams["audio_streams"][0]
        
        reencoded_audio = None
        reencoded_sub = None
        
        # Re-encode audio to match source specs
        if target_audio_path and source_audio_info:
            reencoded_audio = str(temp_dir / "reencoded_audio.mka")
            
            # Get source audio codec and properties
            source_codec = source_audio_info.get("codec", "aac")
            source_channels = source_audio_info.get("channels", 2)
            
            cmd = [
                'ffmpeg', '-y',
                '-i', target_audio_path,
                '-c:a', source_codec,
                '-ar', '48000',
                '-ac', str(min(source_channels, 2)),  # Max 2 channels for compatibility
                '-b:a', '192k' if source_codec.lower() != 'opus' else '128k',
                reencoded_audio
            ]
            
            print(f"Audio re-encoding command: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Audio re-encoding failed: {result.stderr[:500]}")
                reencoded_audio = None
        
        # Re-encode subtitle to SRT format if needed
        if target_sub_path:
            reencoded_sub = str(temp_dir / "reencoded_sub.srt")
            
            cmd = [
                'ffmpeg', '-y',
                '-i', target_sub_path,
                '-c:s', 'srt',
                reencoded_sub
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Subtitle re-encoding failed: {result.stderr[:500]}")
                reencoded_sub = None
        
        return reencoded_audio, reencoded_sub
        
    except Exception as e:
        print(f"Error in track re-encoding: {e}")
        return None, None

async def merge_with_reencoded_tracks(source_path: str, audio_path: str, sub_path: str, output_path: str) -> bool:
    """Merge re-encoded tracks with source file"""
    try:
        # Build input list
        inputs = ['-i', source_path]
        if audio_path:
            inputs.extend(['-i', audio_path])
        if sub_path:
            if not audio_path:  # If only subtitle, need to add empty audio input
                inputs.extend(['-i', sub_path])
            else:
                inputs.extend(['-i', sub_path])
        
        cmd = [
            'ffmpeg', '-y',
            *inputs,
            '-map', '0:v',  # Video from source
            '-map', '0:a',  # Audio from source (original)
        ]
        
        # Add re-encoded audio if exists
        if audio_path:
            cmd.extend(['-map', '1:a'])
        
        # Add subtitle from source if exists
        source_info = get_media_info(source_path)
        source_streams = extract_streams_info(source_info)
        
        if source_streams["subtitle_streams"]:
            cmd.extend(['-map', '0:s'])
        
        # Add re-encoded subtitle if exists
        if sub_path:
            if audio_path:
                cmd.extend(['-map', '2:s'])  # Subtitle is third input
            else:
                cmd.extend(['-map', '1:s'])  # Subtitle is second input
        
        # Codec settings
        cmd.extend([
            '-c:v', 'copy',
            '-c:a', 'copy',  # Copy all audio
            '-c:s', 'copy',  # Copy all subtitles
        ])
        
        # Set default audio disposition to source audio
        cmd.extend([
            '-disposition:a:0', 'default',  # Source audio as default
        ])
        
        if audio_path:
            cmd.extend(['-disposition:a:1', '0'])  # Re-encoded audio as non-default
        
        cmd.append(output_path)
        
        print(f"Merge command: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Merge successful")
            return True
        else:
            print(f"Merge failed: {result.stderr[:500]}")
            return False
            
    except Exception as e:
        print(f"Error in merging: {e}")
        return False

async def start_merging_process(client: Client, state: MergingState, message: Message):
    """Start the merging process with optimized workflow"""
    user_id = state.user_id
    state.state = "processing"
    state.total_files = min(len(state.source_files), len(state.target_files))
    
    # Send initial processing message with cancel button
    progress_msg = await message.reply_text(  
        "<blockquote><b>🔄 Starting Optimized Merge Process</b></blockquote>\n\n"  
        "<blockquote>📊 Matching files...</blockquote>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
        ])
    )
    
    # Store progress message reference in state
    state.progress_msg = progress_msg
    
    # Start the merging process in background  
    asyncio.create_task(process_merging_optimized(client, state, progress_msg))

async def process_merging_optimized(client: Client, state: MergingState, progress_msg: Message):
    """Optimized merging process with new workflow"""
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
                f"<blockquote>🔄 Starting OPTIMIZED processing...</blockquote>",
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
                    
                    # --- STEP 1: TARGET DOWNLOAD (First as per new workflow) ---  
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
                        # Cleanup target file before exiting
                        silent_cleanup(target_file)
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # --- STEP 2: ANALYZE TARGET & EXTRACT TRACKS ---
                    await progress_msg.edit_text(  
                        f"<blockquote><b>🔍 Analyzing Target ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>📁 {target_data['filename']}</blockquote>\n\n"
                        f"<blockquote>Status: Extracting audio & subtitles...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )
                    
                    # Get target file info
                    target_info = get_media_info(target_file)
                    target_streams = extract_streams_info(target_info)
                    
                    # Extract audio tracks
                    extracted_audio_paths = []
                    for audio_idx in range(len(target_streams["audio_streams"])):
                        audio_path = await extract_audio_from_target(target_file, temp_path, audio_idx)
                        if audio_path:
                            extracted_audio_paths.append(audio_path)
                    
                    # Extract subtitle tracks
                    extracted_sub_paths = []
                    for sub_idx in range(len(target_streams["subtitle_streams"])):
                        sub_path = await extract_subtitles_from_target(target_file, temp_path, sub_idx)
                        if sub_path:
                            extracted_sub_paths.append(sub_path)
                    
                    print(f"Extracted {len(extracted_audio_paths)} audio and {len(extracted_sub_paths)} subtitle tracks")
                    
                    # --- STEP 3: DELETE TARGET FILE ---
                    silent_cleanup(target_file)
                    print(f"✅ Target file deleted to save space")
                    
                    # Check cancellation after extraction
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        # Cleanup extracted tracks
                        for path in extracted_audio_paths + extracted_sub_paths:
                            silent_cleanup(path)
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # --- STEP 4: SOURCE DOWNLOAD ---  
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
                        # Cleanup extracted tracks
                        for path in extracted_audio_paths + extracted_sub_paths:
                            silent_cleanup(path)
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
                        # Cleanup source and extracted tracks
                        silent_cleanup(source_file)
                        for path in extracted_audio_paths + extracted_sub_paths:
                            silent_cleanup(path)
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # --- STEP 5: ANALYZE SOURCE SPECIFICATIONS ---
                    await progress_msg.edit_text(  
                        f"<blockquote><b>🔍 Analyzing Source ({overall_progress})</b></blockquote>\n\n"
                        f"<blockquote>📁 {source_data['filename']}</blockquote>\n\n"
                        f"<blockquote>Status: Checking format & specifications...</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )
                    
                    source_info = get_media_info(source_file)
                    source_streams = extract_streams_info(source_info)
                    
                    # Get source duration for compression calculation
                    source_duration = float(source_info.get("format", {}).get("duration", 0))
                    
                    # --- STEP 6: RE-ENCODE & COMPRESS TRACKS ---
                    reencoded_audio_paths = []
                    reencoded_sub_paths = []
                    
                    # Process each extracted audio track
                    for audio_idx, audio_path in enumerate(extracted_audio_paths):
                        await progress_msg.edit_text(  
                            f"<blockquote><b>🎵 Processing Audio {audio_idx+1}/{len(extracted_audio_paths)} ({overall_progress})</b></blockquote>\n\n"
                            f"<blockquote>📁 {source_data['filename']}</blockquote>\n\n"
                            f"<blockquote>Status: Re-encoding to match source...</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )
                        
                        # Re-encode to match source specifications
                        reencoded_audio, _ = await reencode_tracks_to_match_source(
                            source_info, audio_path, None, temp_path
                        )
                        
                        if reencoded_audio:
                            # Check size and compress if needed
                            source_codec = "aac"
                            if source_streams["audio_streams"]:
                                source_codec = source_streams["audio_streams"][0].get("codec", "aac")
                            
                            compressed_audio = await compress_audio_if_needed(
                                reencoded_audio, source_duration, source_codec
                            )
                            reencoded_audio_paths.append(compressed_audio)
                            
                            # Delete original extracted audio
                            silent_cleanup(audio_path)
                    
                    # Process each extracted subtitle track
                    for sub_idx, sub_path in enumerate(extracted_sub_paths):
                        await progress_msg.edit_text(  
                            f"<blockquote><b>📝 Processing Subtitle {sub_idx+1}/{len(extracted_sub_paths)} ({overall_progress})</b></blockquote>\n\n"
                            f"<blockquote>📁 {source_data['filename']}</blockquote>\n\n"
                            f"<blockquote>Status: Converting format...</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )
                        
                        # Re-encode subtitle
                        _, reencoded_sub = await reencode_tracks_to_match_source(
                            source_info, None, sub_path, temp_path
                        )
                        
                        if reencoded_sub:
                            reencoded_sub_paths.append(reencoded_sub)
                            
                            # Delete original extracted subtitle
                            silent_cleanup(sub_path)
                    
                    print(f"Re-encoded {len(reencoded_audio_paths)} audio and {len(reencoded_sub_paths)} subtitle tracks")
                    
                    # Check cancellation after re-encoding
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        silent_cleanup(source_file)
                        for path in reencoded_audio_paths + reencoded_sub_paths:
                            silent_cleanup(path)
                        raise asyncio.CancelledError("Processing cancelled by user")
                    
                    # --- STEP 7: MERGE RE-ENCODED TRACKS WITH SOURCE ---
                    output_filename = source_data["filename"]  
                    output_file = str(temp_path / output_filename)  
                      
                    print(f"Processing pair {idx}:")  
                    print(f"  Source: {source_data['filename']}")  
                    print(f"  Target: {target_data['filename']}")  
                    print(f"  Output: {output_filename}")  
                      
                    merge_start_time = time.time()  
                    await progress_msg.edit_text(  
                        f"<blockquote><b>🛠️ Merging ({overall_progress})</b></blockquote>\n\n"  
                        f"<blockquote>📁 {output_filename}</blockquote>\n\n"  
                        f"<blockquote>Engine : Optimized FFmpeg</blockquote>\n"  
                        f"<blockquote>Status : Processing (0%)</blockquote>",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                        ])
                    )  
                    
                    # Use first re-encoded audio and subtitle (or None if none exist)
                    audio_to_merge = reencoded_audio_paths[0] if reencoded_audio_paths else None
                    sub_to_merge = reencoded_sub_paths[0] if reencoded_sub_paths else None
                    
                    merge_success = await merge_with_reencoded_tracks(
                        source_file, audio_to_merge, sub_to_merge, output_file
                    )
                      
                    # Check cancellation after merge
                    if PROCESSING_STATES[user_id].get("cancelled"):
                        # Cleanup all files
                        silent_cleanup(source_file, output_file if os.path.exists(output_file) else None)
                        for path in reencoded_audio_paths + reencoded_sub_paths:
                            silent_cleanup(path)
                        raise asyncio.CancelledError("Processing cancelled by user")
                      
                    if merge_success:  
                        # Cleanup source and re-encoded tracks immediately
                        print(f"✅ Merge successful. Cleaning up temporary files...")
                        files_to_cleanup = [source_file] + reencoded_audio_paths + reencoded_sub_paths
                        deleted_count = silent_cleanup(*files_to_cleanup)
                        print(f"✅ Cleaned up {deleted_count} temporary files")
                        
                        # --- STEP 8: UPLOAD FINAL FILE ---  
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
                                f"<blockquote>✅ <b>Optimized Merged File</b></blockquote>\n"  
                                f"<blockquote>📁 {source_data['filename']}</blockquote>\n"  
                                f"<blockquote>🎵 Audio tracks added from target (re-encoded)</blockquote>\n"  
                                f"<blockquote>📝 Subtitle tracks added from target (converted)</blockquote>\n"
                                f"<blockquote>💾 Optimized workflow with compression</blockquote>"  
                            ),  
                            progress=upload_progress
                        )  
                        
                        # Cleanup merged file after upload
                        print(f"✅ Upload successful. Cleaning up merged file...")
                        deleted_count = silent_cleanup(output_file)
                        print(f"✅ Cleaned up merged file")
                          
                        # Final status for this file  
                        await progress_msg.edit_text(  
                            f"<blockquote><b>✅ Merge Completed ({overall_progress})</b></blockquote>\n\n"  
                            f"<blockquote>📁 {output_filename}</blockquote>\n"  
                            f"<blockquote>🎵 Audio optimized with compression</blockquote>\n"  
                            f"<blockquote>📝 Subtitles converted to match source</blockquote>\n"
                            f"<blockquote>💾 Storage efficient workflow</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )  
                          
                        print(f"Successfully processed file {idx}")  
                    else:  
                        # Cleanup all files on failure
                        files_to_cleanup = [source_file] + reencoded_audio_paths + reencoded_sub_paths
                        if os.path.exists(output_file):
                            files_to_cleanup.append(output_file)
                        silent_cleanup(*files_to_cleanup)
                        print(f"✅ Cleaned up all files after failed merge")
                        
                        await progress_msg.edit_text(  
                            f"<blockquote><b>❌ Merge Failed ({overall_progress})</b></blockquote>\n\n"  
                            f"<blockquote>📁 {source_data['filename']}</blockquote>\n"  
                            f"<blockquote>⚠️ This file may be incompatible or corrupted</blockquote>",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Cancel Processing", callback_data=f"cancel_processing_{user_id}")]
                            ])
                        )  
                        print(f"Failed to merge file {idx}")  
                      
                except asyncio.CancelledError as e:
                    # User cancelled processing
                    print(f"Processing cancelled by user for file {idx}")
                    raise e
                except Exception as e:  
                    print(f"Error processing file {idx}: {str(e)}")  
                    import traceback  
                    traceback.print_exc()  
                    
                    # Ensure cleanup on unexpected errors
                    try:
                        # Cleanup any files that might exist
                        for var_name in ['target_file', 'source_file', 'output_file']:
                            if var_name in locals():
                                var_value = locals()[var_name]
                                if var_value and os.path.exists(var_value):
                                    silent_cleanup(var_value)
                    except:
                        pass
                    
                    await progress_msg.edit_text(  
                        f"<blockquote><b>❌ Processing Error ({idx}/{len(valid_pairs)})</b></blockquote>\n\n"  
                        f"<blockquote>📁 {source_data['filename']}</blockquote>\n"  
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
                "<blockquote><b>✅ All Optimized Merges Completed</b></blockquote>\n\n"  
                "<blockquote>🎉 All merged files have been sent to you!</blockquote>\n\n"
                "<blockquote>💾 <i>Storage-efficient workflow with automatic compression</i></blockquote>\n"
                "<blockquote>⚡ <i>Target file deleted immediately after extraction</i></blockquote>\n"
                "<blockquote>🎵 <i>Audio compressed if >20MB using Opus/AAC</i></blockquote>"  
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
            "<blockquote><b>🔧 OPTIMIZED AUTO FILE MERGING MODE</b></blockquote>\n\n"
            "<blockquote>Storage-efficient workflow with automatic compression</blockquote>\n\n"
            "<blockquote><b>📝 Instructions:</b>\n"
            "1. Send all source files (base video files)\n"
            "2. Send <code>/done</code> when finished\n"
            "3. Send all target files (with audio/subtitles to extract)\n"
            "4. Send <code>/done</code> again\n"
            "5. Wait for optimized processing</blockquote>\n\n"
            "<blockquote><b>⚡ NEW OPTIMIZED WORKFLOW:</b>\n"
            "- Target file deleted immediately after extraction\n"
            "- Audio compressed if >20MB using Opus/AAC\n"
            "- Tracks re-encoded to match source specifications\n"
            "- Storage efficient with minimal temp files</blockquote>\n\n"
            "<blockquote><b>⚠️ Requirements:</b>\n"
            "- Files should be MKV/MP4 format for best results\n"
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
                f"<blockquote><b>Now send me the TARGET files (with audio/subtitles to extract).</b></blockquote>\n\n"
                f"<blockquote><i>📝 Note: Audio >20MB will be automatically compressed</i></blockquote>"
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
            
            # Start OPTIMIZED processing
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
