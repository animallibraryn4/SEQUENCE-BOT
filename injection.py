import os
import subprocess
import tempfile
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SeekSafeInjector:
    """
    Seek-safe audio and subtitle injector.
    Preserves original video stream without modification.
    """
    
    def __init__(self, ffmpeg_path="ffmpeg"):
        """
        Initialize the injector.
        
        Args:
            ffmpeg_path: Path to ffmpeg binary
        """
        self.ffmpeg_path = ffmpeg_path
        self.temp_dir = tempfile.mkdtemp(prefix="injector_")
        
        # Check if ffmpeg is available
        try:
            result = subprocess.run(
                [ffmpeg_path, "-version"],
                capture_output=True,
                text=True,
                check=False
            )
            if result.returncode != 0:
                raise RuntimeError(f"FFmpeg not found at {ffmpeg_path}")
            logger.info(f"FFmpeg version: {result.stdout.split('version')[1].split()[0]}")
        except Exception as e:
            logger.error(f"FFmpeg check failed: {e}")
            raise
    
    def inject_audio_subtitles(self, input_file, output_file, 
                               audio_lang="eng", sub_lang="eng",
                               keep_original_audio=True,
                               keep_original_subs=True):
        """
        Inject audio and subtitles without modifying video stream.
        
        Args:
            input_file: Path to source file
            output_file: Path to output file
            audio_lang: Language code for audio to extract
            sub_lang: Language code for subtitles to extract
            keep_original_audio: Whether to keep original audio tracks
            keep_original_subs: Whether to keep original subtitle tracks
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create temp files
            temp_audio = os.path.join(self.temp_dir, "clean_audio.aac")
            temp_subtitle = os.path.join(self.temp_dir, "extracted_sub.srt")
            
            # Step 1: Analyze source file
            logger.info(f"Analyzing source file: {input_file}")
            source_info = self._analyze_source(input_file)
            
            # Step 2: Extract and clean audio
            logger.info("Extracting and cleaning audio...")
            audio_extracted = self._extract_and_clean_audio(
                input_file, temp_audio, audio_lang, source_info
            )
            
            # Step 3: Extract subtitles
            logger.info("Extracting subtitles...")
            subtitle_extracted = self._extract_subtitles(
                input_file, temp_subtitle, sub_lang, source_info
            )
            
            # Step 4: Build FFmpeg command for injection
            cmd = self._build_injection_command(
                input_file, output_file, temp_audio, temp_subtitle,
                audio_extracted, subtitle_extracted,
                keep_original_audio, keep_original_subs
            )
            
            # Step 5: Execute injection
            logger.info("Injecting audio and subtitles...")
            result = self._run_ffmpeg_command(cmd, "injection")
            
            # Step 6: Clean up temp files
            self._cleanup_temp_files([temp_audio, temp_subtitle])
            
            if result:
                logger.info(f"Successfully created: {output_file}")
                return True
            else:
                logger.error("Injection failed")
                return False
                
        except Exception as e:
            logger.error(f"Injection failed: {e}")
            self._cleanup_temp_files()
            return False
    
    def _analyze_source(self, input_file):
        """Analyze source file to get stream information."""
        cmd = [
            self.ffmpeg_path, "-i", input_file,
            "-hide_banner"
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )
            
            info = {
                "duration": "N/A",
                "video_streams": 0,
                "audio_streams": 0,
                "subtitle_streams": 0,
                "has_audio": False,
                "has_subs": False
            }
            
            # Parse output for stream info
            output = result.stderr
            
            # Check for duration
            import re
            duration_match = re.search(r"Duration: (\d{2}:\d{2}:\d{2}\.\d{2})", output)
            if duration_match:
                info["duration"] = duration_match.group(1)
            
            # Count streams
            info["video_streams"] = output.count("Video:")
            info["audio_streams"] = output.count("Audio:")
            info["subtitle_streams"] = output.count("Subtitle:")
            info["has_audio"] = info["audio_streams"] > 0
            info["has_subs"] = info["subtitle_streams"] > 0
            
            logger.info(f"Source analysis: {info}")
            return info
            
        except Exception as e:
            logger.error(f"Source analysis failed: {e}")
            return {}
    
    def _extract_and_clean_audio(self, input_file, output_audio, 
                                audio_lang, source_info):
        """
        Extract and clean audio for seek-safe injection.
        
        Returns True if audio was extracted, False otherwise.
        """
        if not source_info.get("has_audio", False):
            logger.warning("No audio streams found in source")
            return False
        
        try:
            # Build command to extract and clean audio
            # Key points for seek-safety:
            # 1. Use -acodec aac (re-encode to clean AAC)
            # 2. Use -ar 48000 (standardize sample rate)
            # 3. Use -ac 2 (standardize to stereo)
            # 4. Use -async 1 for timestamp normalization
            
            cmd = [
                self.ffmpeg_path, "-i", input_file,
                "-map", "0:a:m:language:" + audio_lang + "?",
                "-acodec", "aac",
                "-ar", "48000",
                "-ac", "2",
                "-b:a", "192k",
                "-async", "1",
                "-y",  # Overwrite output
                output_audio
            ]
            
            # If no language-specific audio found, extract first audio track
            if any("?] No match" in line or "No stream" in line 
                   for line in subprocess.run(cmd, capture_output=True, text=True).stderr.split('\n')):
                cmd = [
                    self.ffmpeg_path, "-i", input_file,
                    "-map", "0:a:0",  # First audio track
                    "-acodec", "aac",
                    "-ar", "48000",
                    "-ac", "2",
                    "-b:a", "192k",
                    "-async", "1",
                    "-y",
                    output_audio
                ]
            
            result = self._run_ffmpeg_command(cmd, "audio extraction")
            
            # Check if audio file was created and has content
            if result and os.path.exists(output_audio) and os.path.getsize(output_audio) > 0:
                logger.info(f"Audio extracted and cleaned: {output_audio}")
                return True
            else:
                logger.warning("Audio extraction failed or produced empty file")
                return False
                
        except Exception as e:
            logger.error(f"Audio extraction failed: {e}")
            return False
    
    def _extract_subtitles(self, input_file, output_sub, sub_lang, source_info):
        """
        Extract subtitles in SRT format.
        
        Returns True if subtitles were extracted, False otherwise.
        """
        if not source_info.get("has_subs", False):
            logger.warning("No subtitle streams found in source")
            return False
        
        try:
            # Try to extract language-specific subtitles
            cmd = [
                self.ffmpeg_path, "-i", input_file,
                "-map", "0:s:m:language:" + sub_lang + "?",
                "-c:s", "srt",
                "-y",
                output_sub
            ]
            
            result = self._run_ffmpeg_command(cmd, "subtitle extraction")
            
            # If language-specific not found, try first subtitle track
            if not result or not os.path.exists(output_sub) or os.path.getsize(output_sub) == 0:
                cmd = [
                    self.ffmpeg_path, "-i", input_file,
                    "-map", "0:s:0",  # First subtitle track
                    "-c:s", "srt",
                    "-y",
                    output_sub
                ]
                result = self._run_ffmpeg_command(cmd, "subtitle extraction")
            
            if result and os.path.exists(output_sub) and os.path.getsize(output_sub) > 0:
                logger.info(f"Subtitles extracted: {output_sub}")
                
                # Verify SRT format (basic check)
                with open(output_sub, 'r', encoding='utf-8') as f:
                    content = f.read(1000)
                    if "--> " in content or "WEBVTT" in content:
                        return True
                    else:
                        logger.warning("Extracted subtitles don't appear to be valid SRT format")
                        return False
            else:
                logger.warning("Subtitle extraction failed")
                return False
                
        except Exception as e:
            logger.error(f"Subtitle extraction failed: {e}")
            return False
    
    def _build_injection_command(self, input_file, output_file,
                                temp_audio, temp_subtitle,
                                audio_extracted, subtitle_extracted,
                                keep_original_audio, keep_original_subs):
        """
        Build FFmpeg command for injection without video re-encoding.
        """
        cmd = [self.ffmpeg_path]
        
        # Input files
        cmd.extend(["-i", input_file])
        
        if audio_extracted:
            cmd.extend(["-i", temp_audio])
        
        if subtitle_extracted:
            cmd.extend(["-i", temp_subtitle])
        
        # Map streams
        stream_index = 0
        
        # Always copy video stream (unchanged)
        cmd.extend(["-map", "0:v", "-c:v", "copy"])
        
        # Handle original audio
        if keep_original_audio:
            cmd.extend(["-map", "0:a?", "-c:a", "copy"])
        
        # Handle injected audio
        if audio_extracted:
            cmd.extend(["-map", f"{1 if keep_original_audio else 1}:a", "-c:a", "copy"])
        
        # Handle original subtitles
        if keep_original_subs:
            cmd.extend(["-map", "0:s?", "-c:s", "copy"])
        
        # Handle injected subtitles
        if subtitle_extracted:
            input_idx = 1  # Base index
            if audio_extracted:
                input_idx += 1
            if keep_original_audio:
                input_idx += 1  # Adjust for audio input
            
            cmd.extend(["-map", f"{input_idx}:s", "-c:s", "copy"])
        
        # Metadata settings
        cmd.extend(["-metadata:s:a:0", "language=eng"])
        cmd.extend(["-metadata:s:s:0", "language=eng"])
        
        # Important: Disable automatic timestamp adjustments
        cmd.extend(["-avoid_negative_ts", "make_zero"])
        cmd.extend(["-fflags", "+genpts"])
        
        # Output file
        cmd.extend(["-y", output_file])
        
        logger.info(f"Injection command: {' '.join(cmd)}")
        return cmd
    
    def _run_ffmpeg_command(self, cmd, operation_name):
        """Run FFmpeg command and log output."""
        try:
            logger.info(f"Running {operation_name}...")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Monitor progress
            progress_lines = []
            while True:
                output_line = process.stderr.readline()
                if output_line == '' and process.poll() is not None:
                    break
                if output_line:
                    # Log progress every 5 seconds
                    if "time=" in output_line:
                        progress_lines.append(output_line.strip())
                        if len(progress_lines) % 10 == 0:
                            logger.info(f"Progress: {progress_lines[-1]}")
            
            # Wait for completion
            stdout, stderr = process.communicate()
            return_code = process.returncode
            
            if return_code == 0:
                logger.info(f"{operation_name} completed successfully")
                return True
            else:
                logger.error(f"{operation_name} failed with code {return_code}")
                logger.error(f"FFmpeg stderr: {stderr[:500]}")
                return False
                
        except Exception as e:
            logger.error(f"Error running FFmpeg command: {e}")
            return False
    
    def _cleanup_temp_files(self, additional_files=None):
        """Clean up temporary files."""
        try:
            if additional_files:
                for file in additional_files:
                    if os.path.exists(file):
                        os.remove(file)
                        logger.debug(f"Removed temp file: {file}")
            
            # Clean temp directory if empty
            if os.path.exists(self.temp_dir) and not os.listdir(self.temp_dir):
                os.rmdir(self.temp_dir)
        except Exception as e:
            logger.warning(f"Failed to clean temp files: {e}")
    
    def __del__(self):
        """Cleanup on destruction."""
        self._cleanup_temp_files()


# Simple function for direct use
def inject_audio_subtitles_safely(source_file, target_file, 
                                 audio_lang="eng", sub_lang="eng"):
    """
    Simple wrapper for seek-safe audio and subtitle injection.
    
    Args:
        source_file: Path to source video file
        target_file: Path to output file
        audio_lang: Audio language code (default: "eng")
        sub_lang: Subtitle language code (default: "eng")
    
    Returns:
        bool: True if successful, False otherwise
    """
    injector = SeekSafeInjector()
    return injector.inject_audio_subtitles(
        source_file, target_file,
        audio_lang=audio_lang,
        sub_lang=sub_lang,
        keep_original_audio=True,
        keep_original_subs=True
    )


# Test function
if __name__ == "__main__":
    # Example usage
    source = "input.mkv"
    target = "output_injected.mkv"
    
    if os.path.exists(source):
        success = inject_audio_subtitles_safely(source, target)
        if success:
            print(f"✓ Successfully created: {target}")
        else:
            print("✗ Injection failed")
    else:
        print(f"Source file not found: {source}")
