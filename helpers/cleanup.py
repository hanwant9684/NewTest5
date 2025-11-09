# Automatic cleanup for temporary files and downloads
# Prevents memory and disk usage from growing indefinitely

import os
import shutil
import time
import asyncio
from logger import LOGGER

async def cleanup_old_downloads(max_age_minutes=30):
    """
    Clean up download folders older than max_age_minutes
    This prevents disk space and potential memory issues
    """
    downloads_dir = "downloads"
    
    if not os.path.exists(downloads_dir):
        return
    
    try:
        current_time = time.time()
        max_age_seconds = max_age_minutes * 60
        cleaned_count = 0
        
        for folder_name in os.listdir(downloads_dir):
            folder_path = os.path.join(downloads_dir, folder_name)
            
            if not os.path.isdir(folder_path):
                continue
            
            # Check folder age
            folder_age = current_time - os.path.getmtime(folder_path)
            
            if folder_age > max_age_seconds:
                try:
                    shutil.rmtree(folder_path)
                    cleaned_count += 1
                    LOGGER(__name__).info(f"Cleaned up old download folder: {folder_name}")
                except Exception as e:
                    LOGGER(__name__).error(f"Failed to clean folder {folder_name}: {e}")
        
        if cleaned_count > 0:
            LOGGER(__name__).info(f"Cleanup complete: removed {cleaned_count} old download folder(s)")
            
    except Exception as e:
        LOGGER(__name__).error(f"Error during cleanup: {e}")

async def start_periodic_cleanup(interval_minutes=30):
    """
    Background task that periodically cleans up old downloads
    Runs every interval_minutes to keep disk usage low
    """
    while True:
        try:
            await asyncio.sleep(interval_minutes * 60)
            await cleanup_old_downloads(max_age_minutes=30)
        except Exception as e:
            LOGGER(__name__).error(f"Error in periodic cleanup task: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retry
