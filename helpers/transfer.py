import os
import asyncio
import io
from typing import Optional, Callable
from telethon import TelegramClient
from telethon.tl.types import Message
from logger import LOGGER


def _drop_file_cache(file_path: str) -> None:
    """
    Helper to drop OS cache for a file using posix_fadvise.
    Safely handles systems where posix_fadvise is unavailable or file doesn't exist.
    """
    if not hasattr(os, 'posix_fadvise') or not os.path.exists(file_path):
        return
    
    fd = None
    try:
        fd = os.open(file_path, os.O_RDONLY)
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        LOGGER(__name__).debug(f"Dropped OS cache for: {file_path}")
    except OSError as e:
        LOGGER(__name__).debug(f"Cache drop skipped for {file_path}: {e}")
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass

async def download_media_streaming(
    client: TelegramClient,
    message: Message,
    file: str,
    progress_callback: Optional[Callable] = None
) -> str:
    """
    Memory-efficient streaming download using Telethon's iter_download()
    Downloads chunk-by-chunk without loading entire file into RAM
    RAM usage: ~5-10MB vs FastTelethon's 40-160MB
    """
    if not message.media:
        raise ValueError("Message has no media")
    
    try:
        file_size = message.file.size if message.file else 0
        LOGGER(__name__).info(f"Streaming download starting: {file} ({file_size} bytes, chunk-by-chunk)")
        
        downloaded_bytes = 0
        chunk_size = 524288  # 512KB chunks for optimal speed
        
        with open(file, 'wb') as f:
            async for chunk in client.iter_download(
                message.media,
                chunk_size=chunk_size,
                request_size=chunk_size
            ):
                f.write(chunk)
                downloaded_bytes += len(chunk)
                
                if progress_callback and file_size > 0:
                    r = progress_callback(downloaded_bytes, file_size)
                    if asyncio.iscoroutine(r):
                        await r
        
        LOGGER(__name__).info(f"Streaming download complete: {file} ({downloaded_bytes} bytes)")
        return file
        
    except Exception as e:
        LOGGER(__name__).error(f"Streaming download failed: {e}")
        raise


async def upload_media_streaming(
    client: TelegramClient,
    file_path: str,
    progress_callback: Optional[Callable] = None
):
    """
    FASTEST streaming upload using Telethon's upload_file() with maximum chunk size
    Uses 512KB parts (Telegram's maximum) for optimal speed with minimal protocol overhead
    RAM usage: ~5-10MB while maintaining full upload speed
    Returns: InputFile object ready for sending
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        file_size = os.path.getsize(file_path)
        LOGGER(__name__).info(f"Fast streaming upload starting: {file_path} ({file_size} bytes, 512KB chunks)")
        
        with open(file_path, 'rb') as f:
            result = await client.upload_file(
                f,
                file_size=file_size,
                part_size_kb=512,  # Maximum chunk size for fastest uploads
                file_name=os.path.basename(file_path),
                progress_callback=progress_callback
            )
        
        # Drop OS cache after upload to free RAM immediately
        _drop_file_cache(file_path)
        
        LOGGER(__name__).info(f"Fast streaming upload complete: {file_path}")
        return result
        
    except Exception as e:
        LOGGER(__name__).error(f"Fast streaming upload failed: {e}")
        raise
