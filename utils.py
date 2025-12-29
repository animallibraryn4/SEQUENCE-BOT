# utils.py
import re
import subprocess
from pathlib import Path

def parse_file_info(text):
    """Parse file information from text (either filename or caption)"""
    quality_match = re.search(r'(\d{3,4})[pP]', text)
    quality = int(quality_match.group(1)) if quality_match else 0
    clean_name = re.sub(r'\d{3,4}[pP]', '', text)

    season_match = re.search(r'[sS](?:eason)?\s*(\d+)', clean_name)
    season = int(season_match.group(1)) if season_match else 1
    
    ep_match = re.search(r'[eE](?:p(?:isode)?)?\s*(\d+)', clean_name)
    if ep_match:
        episode = int(ep_match.group(1))
    else:
        nums = re.findall(r'\d+', clean_name)
        episode = int(nums[-1]) if nums else 0

    return {"season": season, "episode": episode, "quality": quality}

def check_ffmpeg_available():
    """Check if FFmpeg is installed and available"""
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
        return result.returncode == 0
    except:
        return False
