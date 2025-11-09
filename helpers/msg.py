# Copyright (C) @TheSmartBisnu
# Migrated to Telethon

from typing import Optional, List
from telethon_helpers import parse_message_link

def get_parsed_msg(text: str, entities: Optional[List] = None) -> str:
    """
    Parse message text with entities
    
    Args:
        text: Message text
        entities: List of MessageEntity objects
        
    Returns:
        Parsed text (plain text, entities are preserved by Telethon)
    """
    if not text:
        return ""
    return text

def getChatMsgID(link: str):
    """
    Parse Telegram message link to extract chat ID and message ID
    
    Args:
        link: Telegram message link
        
    Returns:
        Tuple of (chat_id, message_id)
    Raises:
        ValueError: If link is invalid
    """
    chat_id, thread_id, message_id = parse_message_link(link)
    
    if not chat_id or not message_id:
        raise ValueError("Please send a valid Telegram post URL.")
    
    # Convert username to chat ID if needed (will be resolved by Telethon later)
    # For now, just return the chat_id as-is
    return chat_id, message_id

def get_file_name(message_id: int, message) -> str:
    """
    Get filename from message media
    
    Args:
        message_id: Message ID (used as fallback)
        message: Telethon Message object
        
    Returns:
        Filename string
    """
    from telethon.tl.types import (
        MessageMediaDocument,
        MessageMediaPhoto,
        DocumentAttributeFilename,
        DocumentAttributeVideo,
        DocumentAttributeAudio,
        DocumentAttributeAnimated,
        DocumentAttributeSticker
    )
    
    if not message or not message.media:
        return f"{message_id}"
    
    # Document (file, video, audio, etc.)
    if isinstance(message.media, MessageMediaDocument):
        doc = message.media.document
        
        # Check for filename attribute
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                return attr.file_name
            elif isinstance(attr, DocumentAttributeVideo):
                return f"{message_id}.mp4"
            elif isinstance(attr, DocumentAttributeAudio):
                if attr.voice:
                    return f"{message_id}.ogg"
                return f"{message_id}.mp3"
            elif isinstance(attr, DocumentAttributeAnimated):
                return f"{message_id}.gif"
            elif isinstance(attr, DocumentAttributeSticker):
                if attr.stickerset:
                    return f"{message_id}.webp"
        
        # Fallback based on mime type
        if doc.mime_type:
            if 'video' in doc.mime_type:
                return f"{message_id}.mp4"
            elif 'audio' in doc.mime_type:
                return f"{message_id}.mp3"
            elif 'image' in doc.mime_type:
                return f"{message_id}.jpg"
        
        return f"{message_id}"
    
    # Photo
    elif isinstance(message.media, MessageMediaPhoto):
        return f"{message_id}.jpg"
    
    return f"{message_id}"
