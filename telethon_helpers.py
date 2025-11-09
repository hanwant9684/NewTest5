# Telethon Helper Utilities
# Adapter functions to ease migration from Pyrogram to Telethon

from telethon import Button
from telethon.tl.types import (
    KeyboardButtonCallback, 
    KeyboardButtonUrl,
    MessageEntityBold,
    MessageEntityItalic,
    MessageEntityCode,
    MessageEntityPre,
    MessageEntityTextUrl
)
from typing import List, Optional, Tuple
import re

def parse_command(text: str) -> List[str]:
    """
    Parse command and arguments from message text
    Mimics Pyrogram's message.command behavior
    
    Args:
        text: Message text
        
    Returns:
        List of command parts (command is first element)
    """
    if not text or not text.startswith('/'):
        return []
    return text.split()

def get_command_args(text: str) -> List[str]:
    """Get command arguments only (without command itself)"""
    parts = parse_command(text)
    return parts[1:] if len(parts) > 1 else []

class InlineKeyboardButton:
    """Wrapper to create inline keyboard buttons similar to Pyrogram"""
    
    @staticmethod
    def callback(text: str, callback_data: str):
        """Create callback button"""
        return Button.inline(text, data=callback_data.encode('utf-8'))
    
    @staticmethod
    def url(text: str, url: str):
        """Create URL button"""
        return Button.url(text, url)

class InlineKeyboardMarkup:
    """Wrapper to create inline keyboard markup similar to Pyrogram"""
    
    def __init__(self, rows: List[List]):
        """
        Create inline keyboard from rows of buttons
        
        Args:
            rows: List of button rows, each row is a list of buttons
        """
        self.rows = rows
    
    def to_telethon(self):
        """Convert to Telethon button format"""
        return self.rows

def create_inline_keyboard(buttons: List[List]) -> List[List]:
    """
    Create inline keyboard from button layout
    
    Args:
        buttons: List of button rows
        
    Returns:
        Telethon-compatible button layout
    """
    return buttons

def get_message_link(chat_id: int, message_id: int, username: Optional[str] = None) -> str:
    """
    Generate Telegram message link
    
    Args:
        chat_id: Chat ID
        message_id: Message ID  
        username: Chat username (if public)
        
    Returns:
        Message link URL
    """
    if username:
        return f"https://t.me/{username}/{message_id}"
    else:
        # For private chats/channels, use c/ format
        # Remove the -100 prefix if present
        chat_id_str = str(chat_id)
        if chat_id_str.startswith('-100'):
            chat_id_str = chat_id_str[4:]
        return f"https://t.me/c/{chat_id_str}/{message_id}"

def parse_message_link(link: str) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    """
    Parse Telegram message link to extract chat and message IDs
    
    Args:
        link: Telegram message link
        
    Returns:
        Tuple of (chat_id_or_username, message_thread_id, message_id)
    """
    link = link.strip()
    
    # Remove query parameters
    if '?' in link:
        link = link.split('?')[0]
    
    parts = link.rstrip('/').split('/')
    
    try:
        # Format: https://t.me/c/CHANNEL_ID/MESSAGE_ID or /c/CHANNEL_ID/THREAD_ID/MESSAGE_ID
        if '/c/' in link:
            if len(parts) >= 7:  # With thread (https://t.me/c/CHANNEL_ID/THREAD_ID/MESSAGE_ID)
                channel_id = int(parts[-3])
                thread_id = int(parts[-2])
                message_id = int(parts[-1])
                # Add -100 prefix for channels
                return f"-100{channel_id}", thread_id, message_id
            elif len(parts) >= 6:  # Without thread (https://t.me/c/CHANNEL_ID/MESSAGE_ID)
                channel_id = int(parts[-2])
                message_id = int(parts[-1])
                return f"-100{channel_id}", None, message_id
        
        # Format: https://t.me/USERNAME/MESSAGE_ID or /USERNAME/THREAD_ID/MESSAGE_ID
        else:
            # Check if this is a threaded message by trying to parse the middle part as int
            # If it fails, it's a simple username/message format
            if len(parts) >= 6:  # Potentially with thread
                try:
                    username = parts[-3]
                    thread_id = int(parts[-2])
                    message_id = int(parts[-1])
                    return username, thread_id, message_id
                except ValueError:
                    pass  # Fall through to simple format
            
            # Simple format: username/message_id
            if len(parts) >= 4:  # Without thread
                username = parts[-2]
                message_id = int(parts[-1])
                return username, None, message_id
    
    except (ValueError, IndexError):
        pass
    
    return None, None, None

def format_time(seconds: int) -> str:
    """
    Format seconds into readable time string
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Formatted time string (e.g., "1h 23m 45s")
    """
    if seconds < 0:
        return "0s"
    
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    
    return " ".join(parts)

def format_size(bytes_size: int) -> str:
    """
    Format bytes into readable size string
    
    Args:
        bytes_size: Size in bytes
        
    Returns:
        Formatted size string (e.g., "1.23 GB")
    """
    if bytes_size < 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(bytes_size)
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.2f} {units[unit_index]}"

async def get_display_name(entity) -> str:
    """
    Get display name for a user/chat entity
    
    Args:
        entity: Telethon entity (User, Chat, Channel)
        
    Returns:
        Display name
    """
    if hasattr(entity, 'first_name'):
        name = entity.first_name or ""
        if hasattr(entity, 'last_name') and entity.last_name:
            name += f" {entity.last_name}"
        return name.strip() or "Unknown"
    elif hasattr(entity, 'title'):
        return entity.title or "Unknown"
    return "Unknown"

def extract_code_from_message(text: str) -> Optional[str]:
    """
    Extract code/OTP from message text
    
    Args:
        text: Message text
        
    Returns:
        Extracted code or None
    """
    if not text:
        return None
    
    # Look for numeric codes
    match = re.search(r'\b(\d{5,6})\b', text)
    if match:
        return match.group(1)
    
    return None
