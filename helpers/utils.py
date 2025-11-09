# Copyright (C) @TheSmartBisnu
# Telethon-compatible version

import os
from time import time
from logger import LOGGER
from typing import Optional
from asyncio.subprocess import PIPE
from asyncio import create_subprocess_exec, create_subprocess_shell, wait_for

from telethon.tl.types import (
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    DocumentAttributeFilename,
)

from helpers.files import (
    fileSizeLimit,
    cleanup_download,
    get_download_path
)

from helpers.msg import (
    get_parsed_msg,
    get_file_name
)

from helpers.transfer import download_media_streaming

# Try to import PIL for thumbnail processing (optional)
try:
    from PIL import Image as PILImage
    PIL_AVAILABLE = True
except ImportError:
    PILImage = None
    PIL_AVAILABLE = False
    LOGGER(__name__).info("PIL not available - thumbnails will be skipped for better RAM efficiency")

async def process_thumbnail(thumb_path, max_size_kb=200):
    """
    Process thumbnail to meet Telegram requirements (optional - requires PIL):
    - JPEG format
    - <= 200 KB
    - Max 320px width/height
    
    Returns False if PIL is not available or processing fails.
    """
    if not PIL_AVAILABLE or PILImage is None:
        return False
    
    try:
        with PILImage.open(thumb_path) as img:
            # Convert to RGB (remove alpha channel if present)
            if img.mode != 'RGB':
                img = img.convert('RGB')
            
            # Resize to fit within 320x320 while maintaining aspect ratio
            img.thumbnail((320, 320), PILImage.Resampling.LANCZOS)
            
            # Save with compression, iteratively reduce quality if needed
            quality = 95
            while quality > 10:
                img.save(thumb_path, 'JPEG', quality=quality, optimize=True)
                
                # Check file size
                file_size_kb = os.path.getsize(thumb_path) / 1024
                if file_size_kb <= max_size_kb:
                    return True
                
                quality -= 10
            
            # If still too large after minimum quality, return False
            file_size_kb = os.path.getsize(thumb_path) / 1024
            if file_size_kb > max_size_kb:
                LOGGER(__name__).warning(f"Thumbnail still {file_size_kb:.2f} KB after compression")
                return False
            
            return True
    except Exception as e:
        LOGGER(__name__).error(f"Error processing thumbnail: {e}")
        return False

# Simplified progress bar template (reduced RAM usage)
PROGRESS_BAR = "{percentage:.0f}% | {speed}/s"

async def cmd_exec(cmd, shell=False):
    if shell:
        proc = await create_subprocess_shell(cmd, stdout=PIPE, stderr=PIPE)
    else:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = await proc.communicate()
    try:
        stdout = stdout.decode().strip()
    except:
        stdout = "Unable to decode the response!"
    try:
        stderr = stderr.decode().strip()
    except:
        stderr = "Unable to decode the error!"
    return stdout, stderr, proc.returncode


async def get_media_info(path):
    try:
        result = await cmd_exec([
            "ffprobe", "-hide_banner", "-loglevel", "error",
            "-print_format", "json", "-show_format", "-show_streams", path,
        ])
    except Exception as e:
        print(f"Get Media Info: {e}. Mostly File not found! - File: {path}")
        return 0, None, None
    
    if result[0] and result[2] == 0:
        try:
            try:
                import orjson
                data = orjson.loads(result[0])
            except ImportError:
                import json
                data = json.loads(result[0])
        except Exception as e:
            LOGGER(__name__).error(f"Failed to parse ffprobe JSON: {e}")
            return 0, None, None
        
        duration = 0
        artist = None
        title = None
        
        # Try to get duration from format first
        format_info = data.get("format", {})
        if format_info:
            try:
                duration_str = format_info.get("duration", "0")
                if duration_str and duration_str != "N/A":
                    duration = round(float(duration_str))
            except (ValueError, TypeError):
                pass
            
            # Get tags from format
            tags = format_info.get("tags", {})
            artist = tags.get("artist") or tags.get("ARTIST") or tags.get("Artist")
            title = tags.get("title") or tags.get("TITLE") or tags.get("Title")
        
        # If format duration is 0 or missing, try to get from video stream
        if duration == 0:
            streams = data.get("streams", [])
            for stream in streams:
                if stream.get("codec_type") == "video":
                    try:
                        stream_duration = stream.get("duration")
                        if stream_duration and stream_duration != "N/A":
                            duration = round(float(stream_duration))
                            LOGGER(__name__).info(f"Got duration from video stream: {duration}s")
                            break
                    except (ValueError, TypeError):
                        continue
        
        return duration, artist, title
    return 0, None, None


async def get_video_thumbnail(video_file, duration):
    output = os.path.join("Assets", "video_thumb.jpg")
    if duration is None:
        duration = (await get_media_info(video_file))[0]
    if not duration:
        duration = 3
    duration //= 2
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-ss", str(duration), "-i", video_file,
        "-vf", "thumbnail", "-q:v", "1", "-frames:v", "1",
        "-threads", str((os.cpu_count() or 4) // 2), output,
    ]
    try:
        _, err, code = await wait_for(cmd_exec(cmd), timeout=60)
        if code != 0 or not os.path.exists(output):
            return None
    except:
        return None
    return output


# Progress Throttle Helper to prevent Telegram API rate limits
class ProgressThrottle:
    """
    Centralized progress throttling to prevent Telegram API rate limits.
    Enforces minimum time between updates and handles rate limit errors gracefully.
    """
    def __init__(self):
        self.message_throttles = {}  # message_id -> throttle data
    
    def should_update(self, message_id, current, total, now):
        """
        Determine if progress should be updated based on throttle rules.
        
        Rules:
        - Minimum 5 seconds between updates (or 10% progress change)
        - If rate limited, exponential backoff up to 60 seconds
        - Always allow 100% completion
        """
        if message_id not in self.message_throttles:
            self.message_throttles[message_id] = {
                'last_update_time': 0,
                'last_percentage': 0,
                'rate_limited': False,
                'backoff_duration': 5,  # Start with 5 seconds
                'cooldown_until': 0
            }
        
        throttle = self.message_throttles[message_id]
        percentage = (current / total) * 100 if total > 0 else 0
        
        # Always allow 100% completion
        if percentage >= 100:
            return True
        
        # If we're in cooldown from rate limiting, don't update
        if throttle['cooldown_until'] > now:
            return False
        
        # Check time and percentage thresholds
        time_diff = now - throttle['last_update_time']
        percentage_diff = percentage - throttle['last_percentage']
        
        # Require minimum 5 seconds OR 10% progress
        min_time = throttle['backoff_duration']
        return time_diff >= min_time or percentage_diff >= 10
    
    def mark_updated(self, message_id, percentage, now):
        """Mark that an update was successfully sent"""
        if message_id in self.message_throttles:
            throttle = self.message_throttles[message_id]
            throttle['last_update_time'] = now
            throttle['last_percentage'] = percentage
            # Reset backoff on successful update
            throttle['rate_limited'] = False
            throttle['backoff_duration'] = 5
    
    def mark_rate_limited(self, message_id, now):
        """Mark that we hit a rate limit and implement exponential backoff"""
        if message_id in self.message_throttles:
            throttle = self.message_throttles[message_id]
            throttle['rate_limited'] = True
            # Exponential backoff: 5s -> 10s -> 20s -> 40s -> 60s (max)
            throttle['backoff_duration'] = min(throttle['backoff_duration'] * 2, 60)
            throttle['cooldown_until'] = now + throttle['backoff_duration']
            LOGGER(__name__).info(f"Rate limited - backing off for {throttle['backoff_duration']}s")
    
    def cleanup(self, message_id):
        """Remove throttle data when done"""
        if message_id in self.message_throttles:
            del self.message_throttles[message_id]

# Global throttle instance
_progress_throttle = ProgressThrottle()

# Native Telethon progress callback (replaces Pyleaves to reduce RAM)
async def safe_progress_callback(current, total, *args):
    """
    Native Telethon progress callback - lightweight and RAM-efficient
    Telethon progress callback signature: callback(current, total)
    
    Args:
        current: Current bytes transferred
        total: Total bytes to transfer
        *args: (action, progress_message, start_time, progress_bar_template, filled_char, empty_char)
    """
    try:
        # Unpack args
        action = args[0] if len(args) > 0 else "Progress"
        progress_message = args[1] if len(args) > 1 else None
        start_time = args[2] if len(args) > 2 else time()
        
        # Guard against None progress_message
        if not progress_message:
            return
        
        now = time()
        percentage = (current / total) * 100 if total > 0 else 0
        message_id = progress_message.id
        
        # Check throttle - only update if allowed
        if not _progress_throttle.should_update(message_id, current, total, now):
            return
        
        # Calculate speed and ETA
        elapsed_time = now - start_time
        speed = current / elapsed_time if elapsed_time > 0 else 0
        eta = (total - current) / speed if speed > 0 else 0
        
        # Import here to avoid circular dependency
        from helpers.files import get_readable_file_size, get_readable_time
        
        # Format progress bar (simple and RAM-efficient)
        filled_length = int(10 * current / total) if total > 0 else 0
        bar = "▓" * filled_length + "░" * (10 - filled_length)
        
        # Build progress message
        progress_text = (
            f"{action}\n\n"
            f"{bar} {percentage:.1f}%\n\n"
            f"📊 {get_readable_file_size(current)} / {get_readable_file_size(total)}\n"
            f"⚡ Speed: {get_readable_file_size(speed)}/s\n"
            f"⏱️ ETA: {get_readable_time(int(eta))}"
        )
        
        # Try to update message
        await progress_message.edit(progress_text)
        # Mark successful update
        _progress_throttle.mark_updated(message_id, percentage, now)
            
    except Exception as e:
        error_str = str(e).lower()
        
        # Check if it's a rate limit error
        if 'wait of' in error_str and 'seconds is required' in error_str:
            # This is a rate limit error - mark it and back off
            if progress_message:
                _progress_throttle.mark_rate_limited(progress_message.id, time())
            LOGGER(__name__).warning(f"Rate limited by Telegram API - backing off")
        # Silently ignore errors related to deleted or invalid messages
        elif any(err in error_str for err in ['message_id_invalid', 'message not found', 'message to edit not found', 'message can\'t be edited']):
            LOGGER(__name__).debug(f"Progress message was deleted or invalid, ignoring: {e}")
        else:
            # Log other errors but don't raise to avoid interrupting downloads
            LOGGER(__name__).warning(f"Progress callback error: {e}")


async def forward_to_dump_channel(bot, sent_message, user_id, caption=None):
    """
    Send media to dump channel for monitoring (if configured).
    Uses the media from sent_message (no re-upload) with custom caption showing user ID.
    
    Args:
        bot: Telethon Client instance
        sent_message: The message object that was sent to the user
        user_id: User ID who downloaded this
        caption: Original caption (optional, added below user ID)
    """
    from config import PyroConf
    
    # Only send if dump channel is configured
    if not PyroConf.DUMP_CHANNEL_ID:
        return
    
    try:
        # Convert channel ID to integer format
        channel_id = int(PyroConf.DUMP_CHANNEL_ID)
        
        # Build custom caption with User ID at the top
        custom_caption = f"👤 User ID: {user_id}"
        
        # Add original caption below if present
        if caption:
            custom_caption += f"\n\n📝 Original Caption:\n{caption[:4000]}"  # Telegram limit is 4096
        
        # Send media using the media from sent_message (no re-upload!)
        # Telethon reuses the file reference, so this is RAM-efficient
        await bot.send_file(
            channel_id,
            sent_message.media,
            caption=custom_caption
        )
        
        LOGGER(__name__).info(f"✅ Sent media to dump channel for user {user_id} (RAM-efficient, no re-upload, no 'Forwarded from' label)")
    except Exception as e:
        # Silently log errors - don't interrupt user's download
        LOGGER(__name__).warning(f"Failed to send to dump channel: {e}")

# Generate progress bar for downloading/uploading
def progressArgs(action: str, progress_message, start_time):
    return (action, progress_message, start_time, PROGRESS_BAR, "▓", "░")


async def send_media(
    bot, message, media_path, media_type, caption, progress_message, start_time, user_id=None
):
    """Upload media with all safeguards (size checks, fast uploads, thumbnails, dump channel).
    
    Returns:
        bool: True if upload succeeded, False if it was rejected or failed
    """
    file_size = os.path.getsize(media_path)

    if not await fileSizeLimit(file_size, message, "upload"):
        return False

    from memory_monitor import memory_monitor
    memory_monitor.log_memory_snapshot("Upload Start", f"User {user_id or 'unknown'}: {os.path.basename(media_path)} ({media_type})")
    
    progress_args = progressArgs("📤 Streaming Upload", progress_message, start_time)
    LOGGER(__name__).info(f"Uploading media: {media_path} ({media_type})")

    if media_type == "photo":
        from helpers.transfer import upload_media_streaming
        
        fast_file = await upload_media_streaming(
            bot, 
            media_path, 
            progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args)
        )
        
        sent_message = None
        if fast_file:
            # Streaming upload: Use explicit filename to preserve extension
            sent_message = await bot.send_file(
                message.chat_id,
                fast_file,
                caption=caption or "",
                force_document=False,
                file_name=os.path.basename(media_path)
            )
        else:
            sent_message = await bot.send_file(
                message.chat_id,
                media_path,
                caption=caption or "",
                progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args),
                force_document=False
            )
        
        # Forward to dump channel if configured (RAM-efficient, no re-upload)
        if user_id and sent_message:
            await forward_to_dump_channel(bot, sent_message, user_id, caption)
        
        memory_monitor.log_memory_snapshot("Upload Complete", f"User {user_id or 'unknown'}: {os.path.basename(media_path)} (photo)")
        return True
    elif media_type == "video":
        # Check for custom thumbnail first
        thumb = None
        custom_thumb_path = None
        fallback_thumb = None
        
        if user_id:
            try:
                from database_sqlite import db
            except ImportError:
                from database import db
            custom_thumb_file_id = db.get_custom_thumbnail(user_id)
            if custom_thumb_file_id:
                try:
                    # Use unique temp path to avoid race conditions
                    import time as time_module
                    timestamp = int(time_module.time() * 1000)
                    os.makedirs("Assets/thumbs", exist_ok=True)
                    custom_thumb_path = f"Assets/thumbs/user_{user_id}_{timestamp}.jpg"
                    
                    # Download the thumbnail from Telegram
                    await bot.download_media(custom_thumb_file_id, file=custom_thumb_path)
                    
                    # Process thumbnail to meet Telegram requirements
                    if await process_thumbnail(custom_thumb_path):
                        thumb = custom_thumb_path
                        LOGGER(__name__).info(f"Using custom thumbnail for user {user_id}")
                    else:
                        LOGGER(__name__).warning(f"Failed to process custom thumbnail for user {user_id}, will try fallback")
                        thumb = None
                except Exception as e:
                    LOGGER(__name__).error(f"Failed to download custom thumbnail for user {user_id}: {e}")
                    thumb = None
        
        # Get video duration and dimensions
        duration = (await get_media_info(media_path))[0]
        
        # If no custom thumbnail, prepare unique fallback thumbnail
        if not thumb:
            import time as time_module
            timestamp = int(time_module.time() * 1000)
            os.makedirs("Assets/thumbs", exist_ok=True)
            fallback_thumb = f"Assets/thumbs/fb_{user_id or 0}_{timestamp}.jpg"
            
            # Extract thumbnail from video to unique path
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-ss", str(duration // 2 if duration else 3), "-i", media_path,
                "-vf", "thumbnail", "-q:v", "1", "-frames:v", "1",
                "-threads", str((os.cpu_count() or 4) // 2), fallback_thumb,
            ]
            try:
                _, err, code = await wait_for(cmd_exec(cmd), timeout=60)
                if code == 0 and os.path.exists(fallback_thumb):
                    thumb = fallback_thumb
                else:
                    thumb = None
            except:
                thumb = None
        
        # Get video dimensions
        if thumb and thumb != "none" and os.path.exists(str(thumb)) and PIL_AVAILABLE and PILImage:
            try:
                with PILImage.open(thumb) as img:
                    width, height = img.size
            except:
                width = 480
                height = 320
        else:
            width = 480
            height = 320

        if thumb == "none":
            thumb = None

        # Prepare video attributes
        attributes = []
        if duration and duration > 0:
            attributes.append(DocumentAttributeVideo(
                duration=duration,
                w=width,
                h=height,
                supports_streaming=True
            ))
        
        sent_successfully = False
        sent_message = None
        try:
            from helpers.transfer import upload_media_streaming
            
            fast_file = await upload_media_streaming(
                bot,
                media_path,
                progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args)
            )
            
            if fast_file:
                # Streaming upload: Use the file with explicit filename to preserve extension
                sent_message = await bot.send_file(
                    message.chat_id,
                    fast_file,
                    caption=caption or "",
                    thumb=thumb,
                    attributes=attributes if attributes else None,
                    force_document=False,
                    file_name=os.path.basename(media_path)
                )
            else:
                sent_message = await bot.send_file(
                    message.chat_id,
                    media_path,
                    caption=caption or "",
                    thumb=thumb,
                    attributes=attributes if attributes else None,
                    progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args),
                    force_document=False
                )
            sent_successfully = True
        except Exception as e:
            # If thumbnail causes error, try with fallback or no thumb
            LOGGER(__name__).error(f"Upload failed with thumbnail: {e}")
            
            # If custom thumbnail was used, generate fallback now
            if custom_thumb_path and not fallback_thumb:
                LOGGER(__name__).info("Custom thumbnail failed, generating fallback thumbnail")
                try:
                    import time as time_module
                    timestamp = int(time_module.time() * 1000)
                    fallback_thumb = f"Assets/thumbs/fb_{user_id or 0}_{timestamp}.jpg"
                    
                    cmd = [
                        "ffmpeg", "-hide_banner", "-loglevel", "error",
                        "-ss", str(duration // 2 if duration else 3), "-i", media_path,
                        "-vf", "thumbnail", "-q:v", "1", "-frames:v", "1",
                        "-threads", str((os.cpu_count() or 4) // 2), fallback_thumb,
                    ]
                    _, err, code = await wait_for(cmd_exec(cmd), timeout=60)
                    if code != 0 or not os.path.exists(fallback_thumb):
                        fallback_thumb = None
                except:
                    fallback_thumb = None
            
            # Try with fallback thumbnail
            if fallback_thumb:
                LOGGER(__name__).info("Retrying with auto-extracted thumbnail")
                try:
                    sent_message = await bot.send_file(
                        message.chat_id,
                        media_path,
                        caption=caption or "",
                        thumb=fallback_thumb,
                        attributes=attributes if attributes else None,
                        progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args),
                        force_document=False
                    )
                    thumb = fallback_thumb
                    sent_successfully = True
                except Exception as e2:
                    LOGGER(__name__).error(f"Upload failed with fallback: {e2}, trying without thumbnail")
                    sent_message = await bot.send_file(
                        message.chat_id,
                        media_path,
                        caption=caption or "",
                        thumb=None,
                        attributes=attributes if attributes else None,
                        progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args),
                        force_document=False
                    )
                    thumb = None
                    sent_successfully = True
            else:
                LOGGER(__name__).info("Retrying without thumbnail")
                sent_message = await bot.send_file(
                    message.chat_id,
                    media_path,
                    caption=caption or "",
                    thumb=None,
                    attributes=attributes if attributes else None,
                    progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args),
                    force_document=False
                )
                thumb = None
                sent_successfully = True
        
        # Forward to dump channel if upload was successful (RAM-efficient, no re-upload)
        if sent_successfully and user_id and sent_message:
            await forward_to_dump_channel(bot, sent_message, user_id, caption)
        
        if sent_successfully:
            memory_monitor.log_memory_snapshot("Upload Complete", f"User {user_id or 'unknown'}: {os.path.basename(media_path)} (video)")
        
        # Clean up thumbnails after upload
        if custom_thumb_path and os.path.exists(custom_thumb_path):
            try:
                os.remove(custom_thumb_path)
            except:
                pass
        if fallback_thumb and os.path.exists(fallback_thumb):
            try:
                os.remove(fallback_thumb)
            except:
                pass
        return True
    elif media_type == "audio":
        duration, artist, title = await get_media_info(media_path)
        
        # Prepare audio attributes
        attributes = []
        if duration and duration > 0:
            attributes.append(DocumentAttributeAudio(
                duration=duration,
                performer=artist,
                title=title,
                voice=False
            ))
        
        from helpers.transfer import upload_media_streaming
        
        fast_file = await upload_media_streaming(
            bot,
            media_path,
            progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args)
        )
        
        sent_message = None
        if fast_file:
            # Streaming upload: Use explicit filename to preserve extension
            sent_message = await bot.send_file(
                message.chat_id,
                fast_file,
                caption=caption or "",
                attributes=attributes if attributes else None,
                force_document=False,
                file_name=os.path.basename(media_path)
            )
        else:
            sent_message = await bot.send_file(
                message.chat_id,
                media_path,
                caption=caption or "",
                attributes=attributes if attributes else None,
                progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args),
                force_document=False
            )
        
        # Forward to dump channel if configured (RAM-efficient, no re-upload)
        if user_id and sent_message:
            await forward_to_dump_channel(bot, sent_message, user_id, caption)
        
        memory_monitor.log_memory_snapshot("Upload Complete", f"User {user_id or 'unknown'}: {os.path.basename(media_path)} (audio)")
        return True
    elif media_type == "document":
        from helpers.transfer import upload_media_streaming
        
        fast_file = await upload_media_streaming(
            bot,
            media_path,
            progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args)
        )
        
        sent_message = None
        if fast_file:
            # Streaming upload: Use explicit filename to preserve extension
            sent_message = await bot.send_file(
                message.chat_id,
                fast_file,
                caption=caption or "",
                force_document=True,
                file_name=os.path.basename(media_path)
            )
        else:
            sent_message = await bot.send_file(
                message.chat_id,
                media_path,
                caption=caption or "",
                progress_callback=lambda c, t: safe_progress_callback(c, t, *progress_args),
                force_document=True
            )
        
        # Forward to dump channel if configured (RAM-efficient, no re-upload)
        if user_id and sent_message:
            await forward_to_dump_channel(bot, sent_message, user_id, caption)
        
        memory_monitor.log_memory_snapshot("Upload Complete", f"User {user_id or 'unknown'}: {os.path.basename(media_path)} (document)")
        return True


async def processMediaGroup(chat_message, bot, message, user_id=None, user_client=None):
    """Process and download a media group (multiple files in one post)
    
    ONE-AT-A-TIME APPROACH: Downloads and uploads each file sequentially to minimize RAM usage.
    Uses send_media() helper to preserve all safeguards (size checks, fast uploads, thumbnails, dump channel).
    Files are deleted immediately after upload to free memory.
    
    Args:
        chat_message: The Telegram message containing the media group
        bot: Bot client (for sending to user)
        message: User's message
        user_id: User ID for dump channel tracking
        user_client: User's Telegram client (for downloading from private channels)
        
    Returns:
        int: Number of files successfully downloaded and sent (0 if failed)
    """
    # Use user_client to fetch messages from private/public channels
    # Fall back to bot if user_client is not provided (backward compatibility)
    client_for_download = user_client if user_client else bot
    
    # Get all messages in the media group
    media_group_messages = await client_for_download.get_messages(
        chat_message.chat_id,
        ids=[chat_message.id + i for i in range(-10, 11)]
    )
    
    # Filter to only messages in the same grouped_id
    grouped_messages = []
    if chat_message.grouped_id:
        for msg in media_group_messages:
            if msg and hasattr(msg, 'grouped_id') and msg.grouped_id == chat_message.grouped_id:
                grouped_messages.append(msg)
    else:
        grouped_messages = [chat_message]
    
    # Sort by ID to maintain order
    grouped_messages.sort(key=lambda m: m.id)
    
    total_files = len(grouped_messages)
    files_sent_count = 0
    
    start_time = time()
    progress_message = await message.reply(f"📥 Processing media group ({total_files} files)...")
    LOGGER(__name__).info(
        f"Processing media group with {total_files} items (one-at-a-time mode for low RAM usage)..."
    )

    # Process each file one at a time: download → upload (via send_media) → delete → next
    for idx, msg in enumerate(grouped_messages, 1):
        if msg.media or msg.photo or msg.video or msg.document or msg.audio:
            media_path = None
            try:
                # Update progress
                await progress_message.edit(
                    f"📥 Processing file {idx}/{total_files}..."
                )
                
                # Get filename from message
                filename = get_file_name(msg.id, msg)
                # Use message.id as folder_id to group all media group files together
                download_path = get_download_path(message.id, filename)
                
                # STEP 1: Download this file
                LOGGER(__name__).info(f"Downloading file {idx}/{total_files}: {filename}")
                media_path = await download_media_streaming(
                    client=client_for_download,
                    message=msg,
                    file=download_path,
                    progress_callback=lambda c, t: safe_progress_callback(
                        c, t, *progressArgs(f"📥 Download {idx}/{total_files}", progress_message, start_time)
                    )
                )
                
                if not media_path:
                    LOGGER(__name__).warning(f"File {idx}/{total_files} download failed: no media path returned")
                    continue
                
                # Determine media type (same logic as in main.py)
                media_type = (
                    "photo"
                    if msg.photo
                    else "video"
                    if msg.video
                    else "audio"
                    if msg.audio
                    else "document"
                )
                
                # Get caption (preserve formatting)
                caption_text = msg.text or ""
                upload_caption = caption_text
                
                # STEP 2: Upload this file using send_media (preserves all safeguards)
                # send_media handles: file size checks, fast uploads, thumbnails, dump channel forwarding
                LOGGER(__name__).info(f"Uploading file {idx}/{total_files} to user (via send_media)")
                upload_success = await send_media(
                    bot=bot,
                    message=message,
                    media_path=media_path,
                    media_type=media_type,
                    caption=upload_caption,
                    progress_message=progress_message,
                    start_time=start_time,
                    user_id=user_id
                )
                
                # Only count as sent if upload succeeded
                if upload_success:
                    files_sent_count += 1
                    LOGGER(__name__).info(f"Successfully processed file {idx}/{total_files}")
                else:
                    LOGGER(__name__).warning(f"File {idx}/{total_files} was not sent (rejected by size limit or other error)")
                
                # STEP 3: Delete the file immediately to free RAM
                try:
                    cleanup_download(media_path)
                    LOGGER(__name__).info(f"Cleaned up file {idx}/{total_files}: {media_path}")
                except Exception as cleanup_err:
                    LOGGER(__name__).warning(f"Failed to cleanup file {idx}/{total_files}: {cleanup_err}")
                
            except Exception as e:
                LOGGER(__name__).error(f"Error processing file {idx}/{total_files} from message {msg.id}: {e}")
                # Clean up on error
                if media_path:
                    try:
                        cleanup_download(media_path)
                    except:
                        pass
                continue

    # Cleanup throttle data for this progress message
    _progress_throttle.cleanup(progress_message.id)
    
    # Delete progress message
    await progress_message.delete()
    
    if files_sent_count == 0:
        await message.reply("**❌ No valid media found in the group**")
        return 0
    
    # Don't send completion message here - let main.py handle it based on user type
    # This allows customized messages for free vs premium users
    LOGGER(__name__).info(f"Media group complete: {files_sent_count}/{total_files} files sent successfully")
    return files_sent_count
