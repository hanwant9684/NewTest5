# Copyright (C) @TheSmartBisnu

import os
import glob
import time
from typing import Optional

from logger import LOGGER

SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]

def get_download_path(folder_id: int, filename: str, root_dir: str = "downloads") -> str:
    folder = os.path.join(root_dir, str(folder_id))
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, filename)


def cleanup_download(path: str) -> None:
    try:
        if not path or path is None:
            LOGGER(__name__).debug("Cleanup skipped: path is None or empty")
            return
        
        LOGGER(__name__).info(f"Cleaning Download: {path}")
        
        if os.path.exists(path):
            os.remove(path)
        if os.path.exists(path + ".temp"):
            os.remove(path + ".temp")

        folder = os.path.dirname(path)
        if os.path.isdir(folder) and not os.listdir(folder):
            os.rmdir(folder)

    except Exception as e:
        LOGGER(__name__).error(f"Cleanup failed for {path}: {e}")


def get_readable_file_size(size_in_bytes: Optional[float]) -> str:
    if size_in_bytes is None or size_in_bytes < 0:
        return "0B"

    for unit in SIZE_UNITS:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024

    return "File too large"


def get_readable_time(seconds: int) -> str:
    result = ""
    (days, remainder) = divmod(seconds, 86400)
    days = int(days)
    if days:
        result += f"{days}d"
    (hours, remainder) = divmod(remainder, 3600)
    hours = int(hours)
    if hours:
        result += f"{hours}h"
    (minutes, seconds) = divmod(remainder, 60)
    minutes = int(minutes)
    if minutes:
        result += f"{minutes}m"
    seconds = int(seconds)
    result += f"{seconds}s"
    return result


async def fileSizeLimit(file_size, message, action_type="download", is_premium=False):
    MAX_FILE_SIZE = 2 * 2097152000 if is_premium else 2097152000
    if file_size > MAX_FILE_SIZE:
        await message.reply(
            f"The file size exceeds the {get_readable_file_size(MAX_FILE_SIZE)} limit and cannot be {action_type}ed."
        )
        return False
    return True


def cleanup_orphaned_files() -> tuple[int, int]:
    """
    Emergency cleanup of orphaned files from crashes.
    Removes:
    - All files in downloads/ folder
    - Media files in root directory (MOV, MP4, MKV, AVI, JPG, PNG)
    - Temp files
    
    Returns: (files_removed, bytes_freed)
    """
    try:
        files_removed = 0
        bytes_freed = 0
        
        # Cleanup downloads folder
        if os.path.exists("downloads"):
            for root, dirs, files in os.walk("downloads", topdown=False):
                for file in files:
                    filepath = os.path.join(root, file)
                    try:
                        size = os.path.getsize(filepath)
                        os.remove(filepath)
                        files_removed += 1
                        bytes_freed += size
                        LOGGER(__name__).debug(f"Removed orphaned file: {filepath}")
                    except Exception as e:
                        LOGGER(__name__).warning(f"Failed to remove {filepath}: {e}")
                
                # Remove empty folders
                for dir in dirs:
                    dirpath = os.path.join(root, dir)
                    try:
                        if not os.listdir(dirpath):
                            os.rmdir(dirpath)
                    except:
                        pass
        
        # Cleanup media files in root directory (from crashes)
        media_extensions = ['*.MOV', '*.mov', '*.MP4', '*.mp4', '*.MKV', '*.mkv', 
                          '*.AVI', '*.avi', '*.JPG', '*.jpg', '*.JPEG', '*.jpeg',
                          '*.PNG', '*.png', '*.temp', '*.tmp']
        
        for pattern in media_extensions:
            for filepath in glob.glob(pattern):
                # Don't delete important files
                if any(x in filepath.lower() for x in ['config', 'database', 'log', 'backup', 'main', 'server']):
                    continue
                
                try:
                    size = os.path.getsize(filepath)
                    os.remove(filepath)
                    files_removed += 1
                    bytes_freed += size
                    LOGGER(__name__).info(f"Removed orphaned media file from root: {filepath}")
                except Exception as e:
                    LOGGER(__name__).warning(f"Failed to remove {filepath}: {e}")
        
        if files_removed > 0:
            LOGGER(__name__).warning(
                f"🧹 Emergency cleanup: Removed {files_removed} orphaned files, "
                f"freed {get_readable_file_size(bytes_freed)}"
            )
        
        return files_removed, bytes_freed
        
    except Exception as e:
        LOGGER(__name__).error(f"Error during orphaned files cleanup: {e}")
        return 0, 0
