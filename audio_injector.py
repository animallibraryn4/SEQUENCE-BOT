# file name: audio_injector.py
import subprocess
import json
import os
from pathlib import Path

def extract_audio_seek_safe(input_path: str, output_audio_path: str) -> bool:
    """
    Extract and re-encode audio for seek-safe injection
    """
    try:
        cmd = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-vn",  # No video
            "-acodec", "aac",  # Re-encode to AAC
            "-b:a", "192k",    # Bitrate
            "-ac", "2",        # Stereo
            "-ar", "48000",    # Sample rate
            "-async", "1",     # Resample for sync
            "-af", "aresample=async=1:first_pts=0",  # Reset timestamps
            output_audio_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0
    except Exception as e:
        print(f"Audio extraction error: {e}")
        return False

def extract_subtitles(input_path: str, output_sub_path: str) -> bool:
    """
    Extract subtitles and convert to SRT if possible
    """
    try:
        # First check if there are text subtitles
        cmd_check = [
            "ffprobe",
            "-v", "quiet",
            "-select_streams", "s",
            "-show_entries", "stream=codec_type,codec_name",
            "-of", "json",
            input_path
        ]
        
        result = subprocess.run(cmd_check, capture_output=True, text=True)
        if result.returncode != 0:
            return False
            
        streams = json.loads(result.stdout).get("streams", [])
        text_subs = [s for s in streams if s.get("codec_name") in ["srt", "ass", "ssa", "subrip"]]
        
        if not text_subs:
            return False
        
        # Extract first text subtitle
        cmd_extract = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-map", "0:s:0",  # First subtitle
            "-c:s", "srt",    # Convert to SRT
            output_sub_path
        ]
        
        result = subprocess.run(cmd_extract, capture_output=True, text=True)
        return result.returncode == 0
        
    except Exception as e:
        print(f"Subtitle extraction error: {e}")
        return False

def inject_audio_subtitles_seek_safe(video_path: str, audio_path: str, sub_path: str, output_path: str) -> bool:
    """
    Inject audio and subtitles without touching video timestamps (Method 2)
    """
    try:
        # Build ffmpeg command
        cmd = [
            "ffmpeg", "-y",
            "-i", video_path,  # Input video
        ]
        
        inputs = [video_path]
        
        # Add audio if exists
        if os.path.exists(audio_path):
            cmd.extend(["-i", audio_path])
            inputs.append(audio_path)
        
        # Add subtitle if exists
        if os.path.exists(sub_path):
            cmd.extend(["-i", sub_path])
            inputs.append(sub_path)
        
        # Map streams
        cmd.extend(["-map", "0:v:0"])  # Video from first input
        
        # Original audio tracks
        cmd.extend(["-map", "0:a?"])
        
        # New audio track
        if os.path.exists(audio_path):
            cmd.extend(["-map", "1:a:0"])
        
        # Original subtitle tracks
        cmd.extend(["-map", "0:s?"])
        
        # New subtitle track
        if os.path.exists(sub_path):
            cmd.extend(["-map", "2:s:0"])
        
        # Codec settings
        cmd.extend([
            "-c:v", "copy",          # Copy video
            "-c:a", "aac",           # Re-encode all audio to AAC
            "-b:a", "192k",
            "-ac", "2",
            "-c:s", "copy",          # Copy subtitles
            "-disposition:a", "default",
            "-max_interleave_delta", "0",  # Reduce buffering
            output_path
        ])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"FFmpeg error: {result.stderr[:500]}")
            return False
        return True
        
    except Exception as e:
        print(f"Injection error: {e}")
        return False

def merge_audio_subtitles_method2(source_path: str, target_path: str, output_path: str) -> bool:
    """
    Main Method 2: Seek-safe audio and subtitle injection
    """
    try:
        temp_dir = Path("/tmp") / "audio_injector"
        temp_dir.mkdir(exist_ok=True)
        
        audio_file = temp_dir / "extracted_audio.m4a"
        sub_file = temp_dir / "extracted_sub.srt"
        
        # Step 1: Extract and clean audio
        print("Extracting audio...")
        if not extract_audio_seek_safe(source_path, str(audio_file)):
            print("Failed to extract audio")
            return False
        
        # Step 2: Extract subtitles
        print("Extracting subtitles...")
        subtitle_extracted = extract_subtitles(source_path, str(sub_file))
        
        # Step 3: Inject into target
        print("Injecting tracks...")
        if not subtitle_extracted:
            sub_file = None
        
        success = inject_audio_subtitles_seek_safe(
            target_path,
            str(audio_file) if audio_file.exists() else None,
            str(sub_file) if sub_file and sub_file.exists() else None,
            output_path
        )
        
        # Cleanup
        if audio_file.exists():
            audio_file.unlink()
        if sub_file and sub_file.exists():
            sub_file.unlink()
        
        return success
        
    except Exception as e:
        print(f"Method 2 merge error: {e}")
        return False
