# Copyright (C) @Wolfy004
# Channel: https://t.me/Wolfy004

import os
import psutil
import asyncio
from time import time
from attribution import verify_attribution, get_channel_link, get_creator_username

try:
    import uvloop
    # Only set uvloop policy if not already set (prevents overwriting thread-local loops)
    if not isinstance(asyncio.get_event_loop_policy(), uvloop.EventLoopPolicy):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from telethon import TelegramClient, events
from telethon.errors import PeerIdInvalidError, BadRequestError
from telethon.sessions import StringSession
from telethon_helpers import InlineKeyboardButton, InlineKeyboardMarkup, parse_command, get_command_args

from helpers.utils import (
    processMediaGroup,
    progressArgs,
    send_media,
    safe_progress_callback
)

from helpers.transfer import download_media_streaming

from helpers.files import (
    get_download_path,
    fileSizeLimit,
    get_readable_file_size,
    get_readable_time,
    cleanup_download,
    cleanup_orphaned_files
)

from helpers.msg import (
    getChatMsgID,
    get_file_name,
    get_parsed_msg
)

from config import PyroConf
from logger import LOGGER
try:
    from database_sqlite import db
except ImportError:
    from database import db
from phone_auth import PhoneAuthHandler
from ad_monetization import ad_monetization, PREMIUM_DOWNLOADS
from access_control import admin_only, paid_or_admin_only, check_download_limit, register_user, check_user_session, get_user_client, force_subscribe
from memory_monitor import memory_monitor
from admin_commands import (
    add_admin_command,
    remove_admin_command,
    set_premium_command,
    remove_premium_command,
    ban_user_command,
    unban_user_command,
    broadcast_command,
    admin_stats_command,
    user_info_command,
    broadcast_callback_handler
)
from queue_manager import download_queue
from legal_acceptance import show_legal_acceptance, handle_legal_callback

# Initialize the bot client with Telethon
# Telethon handles connection pooling and performance optimization automatically
bot = TelegramClient(
    'media_bot',
    PyroConf.API_ID,
    PyroConf.API_HASH
)

# REMOVED: Global user client was bypassing SessionManager and wasting 30-100MB RAM
# All users (including admins) must login with /login command to use SessionManager
# This ensures proper memory limits (max 3 sessions on Render = 300MB)

# Phone authentication handler
phone_auth_handler = PhoneAuthHandler(PyroConf.API_ID, PyroConf.API_HASH)

RUNNING_TASKS = set()
USER_TASKS = {}

# Track bot start time for filtering old updates
bot.start_time = None

def is_new_update(event):
    """Filter function to ignore messages older than bot start time"""
    if not bot.start_time:
        return True  # If start_time not set yet, allow all messages
    
    # Check if message date is newer than bot start time
    if event.date:
        return event.date.timestamp() >= bot.start_time
    return True  # Allow messages without date

def track_task(coro, user_id=None):
    task = asyncio.create_task(coro)
    RUNNING_TASKS.add(task)
    
    if user_id:
        if user_id not in USER_TASKS:
            USER_TASKS[user_id] = set()
        USER_TASKS[user_id].add(task)
    
    def _remove(_):
        RUNNING_TASKS.discard(task)
        if user_id and user_id in USER_TASKS:
            USER_TASKS[user_id].discard(task)
            if not USER_TASKS[user_id]:
                del USER_TASKS[user_id]
    
    task.add_done_callback(_remove)
    return task

def get_user_tasks(user_id):
    return USER_TASKS.get(user_id, set())

def cancel_user_tasks(user_id):
    tasks = get_user_tasks(user_id)
    cancelled = 0
    for task in list(tasks):
        if not task.done():
            task.cancel()
            cancelled += 1
    return cancelled

async def send_video_message(event, video_message_id: int, caption: str, markup=None, log_context: str = ""):
    """Helper function to send video message with fallback to text"""
    try:
        video_message = await bot.get_messages("Wolfy004", ids=video_message_id)
        if video_message and video_message.video:
            buttons = markup.to_telethon() if markup else None
            return await event.respond(caption, file=video_message.video, buttons=buttons)
        else:
            buttons = markup.to_telethon() if markup else None
            return await event.respond(caption, buttons=buttons, link_preview=False)
    except Exception as e:
        LOGGER(__name__).warning(f"Could not send video in {log_context}: {e}")
        buttons = markup.to_telethon() if markup else None
        return await event.respond(caption, buttons=buttons, link_preview=False)

# Auto-add OWNER_ID as admin on startup
@bot.on(events.NewMessage(pattern='/start', incoming=True, func=lambda e: e.is_private and e.sender_id == PyroConf.OWNER_ID))
async def auto_add_owner_as_admin(event):
    if PyroConf.OWNER_ID and not db.is_admin(PyroConf.OWNER_ID):
        db.add_admin(PyroConf.OWNER_ID, PyroConf.OWNER_ID)
        LOGGER(__name__).info(f"Auto-added owner {PyroConf.OWNER_ID} as admin")

@bot.on(events.NewMessage(pattern='/start', incoming=True, func=lambda e: e.is_private and is_new_update(e)))
@register_user
async def start(event):
    sender = await event.get_sender()
    username = f"@{sender.username}" if sender.username else "No username"
    name = sender.first_name if sender.first_name else "Unknown"
    LOGGER(__name__).info(f"👤 USER STARTED BOT | ID: {event.sender_id} | Username: {username} | Name: {name}")
    
    if not db.check_legal_acceptance(event.sender_id):
        LOGGER(__name__).info(f"User {event.sender_id} needs to accept legal terms")
        await show_legal_acceptance(event)
        return
    
    # Check if this is a verification deep link (format: /start verify_CODE)
    command = parse_command(event.text)
    if len(command) > 1 and command[1].startswith("verify_"):
        verification_code = command[1].replace("verify_", "").strip()
        LOGGER(__name__).info(f"🔗 AUTO-VERIFICATION | User: {event.sender_id} ({username}) | Code: {verification_code}")
        
        success, msg = ad_monetization.verify_code(verification_code, event.sender_id)
        
        if success:
            await event.respond(
                f"✅ **Automatic Verification Successful!**\n\n{msg}\n\n"
                "🎉 You can now start downloading!\n"
                "📥 Just paste any Telegram link to begin."
            )
            LOGGER(__name__).info(f"✅ AUTO-VERIFICATION SUCCESS | User: {event.sender_id} ({username}) | Got premium access")
        else:
            await event.respond(
                f"❌ **Verification Failed**\n\n{msg}\n\n"
                "Please try getting a new code with `/getpremium`"
            )
            LOGGER(__name__).warning(f"❌ AUTO-VERIFICATION FAILED | User: {event.sender_id} ({username}) | Reason: {msg}")
        return
    
    welcome_text = (
        "🎉 **Welcome to Save Restricted Content Bot!**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🚀 **Quick Start Guide:**\n\n"
        "**Step 1:** Login with your phone\n"
        "   📱 Use: `/login +1234567890`\n\n"
        "**Step 2:** Verify with OTP\n"
        "   🔐 Enter the code you receive\n\n"
        "**Step 3:** Start downloading!\n"
        "   📥 Just paste any Telegram link\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💎 **Get Free Downloads:**\n\n"
        "🎁 **Option 1: FREE (Watch Ads)**\n"
        "   📥 5 free download per ad session\n"
        "   📺 Complete quick verification steps\n"
        "   ♻️ Repeat anytime!\n"
        "   👉 Use: `/getpremium`\n\n"
        "💰 **Option 2: Paid ($1/month)**\n"
        "   ⭐ 30 days unlimited access\n"
        "   🚀 Priority downloads\n"
        "   📦 Batch download support\n"
        "   👉 Use: `/upgrade`\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "ℹ️ **Need help?** Use `/help` for all commands\n\n"
        "🔑 **Ready to start?** Login now with `/login <phone>`"
    )

    # Verify attribution
    verify_attribution()
    
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton.url("📢 Update Channel", get_channel_link(primary=True))]]
    )
    
    # Add creator attribution to welcome message
    welcome_text += f"\n\n💡 **Created by:** {get_creator_username()}"
    
    await send_video_message(event, 41, welcome_text, markup, "start command")

@bot.on(events.NewMessage(pattern='/help', incoming=True, func=lambda e: e.is_private))
@register_user
async def help_command(event):
    user_id = event.sender_id
    user_type = db.get_user_type(user_id)
    is_premium = user_type == 'paid'
    
    if is_premium:
        help_text = (
            "👑 **Premium User - Help Guide**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📥 **Download Commands:**\n\n"
            "**Single Download:**\n"
            "   `/dl <link>` or just paste a link\n"
            "   📺 Videos • 🖼️ Photos • 🎵 Audio • 📄 Documents\n\n"
            "**Batch Download:**\n"
            "   `/bdl <start_link> <end_link>`\n"
            "   💡 Example: `/bdl https://t.me/channel/100 https://t.me/channel/120`\n"
            "   📦 Downloads all posts from 100 to 120 (max 20)\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🚀 **Queue System:**\n\n"
            "   👑 **Premium Priority** - Jump ahead in queue!\n"
            "   `/queue` - Check your position\n"
            "   `/canceldownload` - Cancel current download\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔐 **Authentication:**\n\n"
            "   `/login +1234567890` - Login with phone\n"
            "   `/verify 1 2 3 4 5` - Enter OTP code\n"
            "   `/password <2FA>` - Enter 2FA password\n"
            "   `/logout` - Logout from account\n"
            "   `/cancel` - Cancel pending auth\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "ℹ️ **Other Commands:**\n\n"
            "   `/myinfo` - View account details\n"
            "   `/stats` - Bot statistics\n\n"
            "💡 **Your Benefits:**\n"
            "   ✅ Unlimited downloads\n"
            "   ✅ Priority queue access\n"
            "   ✅ Batch download (up to 20 posts)\n"
            "   ✅ No daily limits"
        )
    else:
        help_text = (
            "🆓 **Free User - Help Guide**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📥 **Download Commands:**\n\n"
            "**Single Download:**\n"
            "   `/dl <link>` or just paste a link\n"
            "   📺 Videos • 🖼️ Photos • 🎵 Audio • 📄 Documents\n\n"
            "⚠️ **Your Limits:**\n"
            "   📊 1 download per day\n"
            "   ⏳ Normal queue priority\n"
            "   ❌ No batch downloads\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "💎 **Get More Downloads:**\n\n"
            "🎁 **FREE Downloads (Watch Ads):**\n"
            "   `/getpremium` - Get 5 free download\n"
            "   📺 Complete verification steps\n"
            "   ♻️ Repeat anytime!\n\n"
            "💰 **Paid Premium ($1/month):**\n"
            "   `/upgrade` - View payment options\n"
            "   ⭐ 30 days unlimited access\n"
            "   🚀 Priority downloads\n"
            "   📦 Batch download support\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🚀 **Queue System:**\n\n"
            "   `/queue` - Check your position\n"
            "   `/canceldownload` - Cancel download\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔐 **Authentication:**\n\n"
            "   `/login +1234567890` - Login with phone\n"
            "   `/verify 1 2 3 4 5` - Enter OTP code\n"
            "   `/password <2FA>` - Enter 2FA password\n"
            "   `/logout` - Logout from account\n"
            "   `/cancel` - Cancel pending auth\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "ℹ️ **Other Commands:**\n\n"
            "   `/myinfo` - View account details\n"
            "   `/stats` - Bot statistics"
        )

    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton.url("📢 Update Channel", get_channel_link(primary=True))]]
    )
    
    help_text += f"\n\n💡 **Bot by:** {get_creator_username()} | {get_channel_link(primary=True)}"
    
    await event.respond(help_text, buttons=markup.to_telethon(), link_preview=False)

async def handle_download(bot_client, event, post_url: str, user_client=None, increment_usage=True):
    """
    Handle downloading media from Telegram posts
    
    IMPORTANT: user_client is managed by SessionManager - DO NOT call .stop() on it!
    The SessionManager will automatically reuse and cleanup sessions to prevent memory leaks.
    """
    # Cut off URL at '?' if present
    if "?" in post_url:
        post_url = post_url.split("?", 1)[0]

    try:
        LOGGER(__name__).info(f"Attempting to parse URL: {post_url}")
        chat_id, message_id = getChatMsgID(post_url)
        
        # Convert chat_id to int if it's a numeric string (Telethon requirement)
        # Telethon needs integers for numeric IDs, unlike Pyrogram which accepted strings
        if isinstance(chat_id, str) and (chat_id.lstrip('-').isdigit()):
            chat_id = int(chat_id)
            LOGGER(__name__).info(f"Converted chat_id to integer: {chat_id}")
        
        LOGGER(__name__).info(f"Successfully parsed - Chat ID: {chat_id} (type: {type(chat_id).__name__}), Message ID: {message_id}")

        # Use user's personal session (required for all users, including admins)
        client_to_use = user_client
        
        if not client_to_use:
                await event.respond(
                    "❌ **No active session found.**\n\n"
                    "Please login with your phone number:\n"
                    "`/login +1234567890`"
                )
                return

        # Try to resolve the entity first (this helps Telethon find private channels)
        try:
            LOGGER(__name__).info(f"Attempting to resolve entity for chat: {chat_id}")
            entity = await client_to_use.get_entity(chat_id)
            LOGGER(__name__).info(f"Successfully resolved entity for chat: {chat_id}, entity type: {type(entity).__name__}")
        except ValueError as e:
            LOGGER(__name__).error(f"Cannot find entity {chat_id}: {e}")
            
            # Try to load all dialogs to populate entity cache, then try again
            try:
                LOGGER(__name__).info(f"Fetching dialogs to populate entity cache for user {event.sender_id}")
                status_msg = await event.respond("🔄 **Loading your channels... Please wait.**")
                
                # Get all dialogs (chats/channels) - this populates Telethon's entity cache
                dialogs = await client_to_use.get_dialogs(limit=None)
                LOGGER(__name__).info(f"Loaded {len(dialogs)} dialogs for user {event.sender_id}")
                
                # Try to resolve entity again after loading dialogs
                entity = await client_to_use.get_entity(chat_id)
                await status_msg.delete()
                LOGGER(__name__).info(f"Successfully resolved entity after loading dialogs: {chat_id}")
            except ValueError as e2:
                await status_msg.delete()
                LOGGER(__name__).error(f"Still cannot find entity {chat_id} after loading dialogs: {e2}")
                await event.respond(
                    f"❌ **Cannot access this channel/chat.**\n\n"
                    f"**Possible reasons:**\n"
                    f"1️⃣ You're not a member of this private channel\n"
                    f"2️⃣ The channel/chat doesn't exist\n"
                    f"3️⃣ You logged in with a different account\n\n"
                    f"**To fix:**\n"
                    f"• Make sure you joined the channel with your logged-in phone number\n"
                    f"• Make sure you're using the same account that joined the channel\n"
                    f"• Try again after joining\n\n"
                    f"Chat ID: `{chat_id}`"
                )
                return
            except Exception as e3:
                try:
                    await status_msg.delete()
                except:
                    pass
                LOGGER(__name__).error(f"Error loading dialogs: {e3}")
                await event.respond(f"❌ **Error accessing channel:**\n\n`{str(e3)}`")
                return
        except Exception as e:
            LOGGER(__name__).error(f"Error resolving entity {chat_id}: {e}")
            await event.respond(f"❌ **Error accessing channel:**\n\n`{str(e)}`\n\nMake sure you've joined this channel with your Telegram account.")
            return

        chat_message = await client_to_use.get_messages(chat_id, ids=message_id)

        LOGGER(__name__).info(f"Downloading media from URL: {post_url}")

        if chat_message.document or chat_message.video or chat_message.audio:
            # Telethon uses .size instead of .file_size (Pyrogram compatibility)
            # Use message.file.size as universal way to get file size in Telethon
            file_size = chat_message.file.size if chat_message.file else 0

            # Check file size limit based on actual client being used
            try:
                # Check if user's Telegram account has premium
                me = await client_to_use.get_me()
                is_premium = getattr(me, 'is_premium', False)
            except:
                is_premium = False

            if not await fileSizeLimit(file_size, event, "download", is_premium):
                return

        # Telethon uses .message for both text and captions (no separate .caption attribute)
        # For media with caption or text messages, .message contains the text
        message_text = getattr(chat_message, 'message', None) or getattr(chat_message, 'text', '') or ''
        parsed_caption = get_parsed_msg(message_text, chat_message.entities)
        parsed_text = get_parsed_msg(message_text, chat_message.entities)

        if hasattr(chat_message, 'grouped_id') and chat_message.grouped_id:
            # Count files in media group first for quota check
            # Get messages around the current message to find all in the group
            media_group_messages = await client_to_use.get_messages(
                chat_id, 
                ids=[message_id + i for i in range(-10, 11)]
            )
            
            # Filter to only messages in the same grouped_id
            grouped_msgs = []
            for msg in media_group_messages:
                if msg and hasattr(msg, 'grouped_id') and msg.grouped_id == chat_message.grouped_id:
                    if msg.photo or msg.video or msg.document or msg.audio:
                        grouped_msgs.append(msg)
            
            file_count = len(grouped_msgs)
            
            LOGGER(__name__).info(f"Media group detected with {file_count} files for user {event.sender_id}")
            
            # Pre-flight quota check before downloading
            if increment_usage:
                can_dl, quota_msg = db.can_download(event.sender_id, file_count)
                if not can_dl:
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton.callback(f"🎁 Watch Ad & Get {PREMIUM_DOWNLOADS} Downloads", "watch_ad_now")],
                        [InlineKeyboardButton.callback("💰 Upgrade to Premium", "upgrade_premium")]
                    ])
                    await event.respond(quota_msg, buttons=keyboard.to_telethon())
                    return
            
            # Download media group (pass user_client for private channel access)
            files_sent = await processMediaGroup(chat_message, bot_client, event, event.sender_id, user_client=client_to_use)
            
            if files_sent == 0:
                await event.respond("**Could not extract any valid media from the media group.**")
                return
            
            # Increment usage by actual file count after successful download
            if increment_usage:
                success = db.increment_usage(event.sender_id, files_sent)
                if not success:
                    LOGGER(__name__).error(f"Failed to increment usage for user {event.sender_id} after media group download")
                
                # Show completion message based on user type
                user_type = db.get_user_type(event.sender_id)
                if user_type == 'free':
                    # Free users: show buttons for ads and upgrade with remaining downloads count
                    user = db.get_user(event.sender_id)
                    ad_downloads_left = user.get('ad_downloads', 0) if user else 0
                    
                    upgrade_keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton.callback(f"🎁 Watch Ad & Get {PREMIUM_DOWNLOADS} Downloads", "watch_ad_now")],
                        [InlineKeyboardButton.callback("💰 Upgrade to Premium", "upgrade_premium")]
                    ])
                    
                    remaining_msg = f"\n📊 **{ad_downloads_left} free download(s) remaining**" if ad_downloads_left > 0 else "\n📊 **0 free downloads remaining**"
                    
                    await event.respond(
                        f"✅ **Download complete**{remaining_msg}",
                        buttons=upgrade_keyboard.to_telethon()
                    )
                else:
                    # Premium/Admin users: simple completion message without buttons
                    await event.respond("✅ **Download complete**")
            
            return

        elif chat_message.media:
            start_time = time()
            progress_message = await event.respond("**📥 Downloading Progress...**")

            filename = get_file_name(message_id, chat_message)
            download_path = get_download_path(event.id, filename)

            memory_monitor.log_memory_snapshot("Download Start", f"User {event.sender_id}: {filename}")
            
            media_path = await download_media_streaming(
                client_to_use,
                chat_message,
                download_path,
                progress_callback=lambda c, t: safe_progress_callback(c, t, *progressArgs("📥 Streaming Download", progress_message, start_time))
            )

            memory_monitor.log_memory_snapshot("Download Complete", f"User {event.sender_id}: {filename}")
            LOGGER(__name__).info(f"Downloaded media: {media_path}")

            try:
                media_type = (
                    "photo"
                    if chat_message.photo
                    else "video"
                    if chat_message.video
                    else "audio"
                    if chat_message.audio
                    else "document"
                )
                await send_media(
                    bot_client,
                    event,
                    media_path,
                    media_type,
                    parsed_caption,
                    progress_message,
                    start_time,
                    event.sender_id,
                )

                # Cleanup throttle data for this progress message
                from helpers.utils import _progress_throttle
                _progress_throttle.cleanup(progress_message.id)
                
                await progress_message.delete()

                # Only increment usage after successful download
                if increment_usage:
                    db.increment_usage(event.sender_id)
                    
                    # Show completion message with buttons for all users
                    user_type = db.get_user_type(event.sender_id)
                    if user_type == 'free':
                        # Get remaining downloads count
                        user = db.get_user(event.sender_id)
                        ad_downloads_left = user.get('ad_downloads', 0) if user else 0
                        
                        upgrade_markup = InlineKeyboardMarkup([
                            [InlineKeyboardButton.callback(f"🎁 Watch Ad & Get {PREMIUM_DOWNLOADS} Downloads", "watch_ad_now")],
                            [InlineKeyboardButton.callback("💰 Upgrade to Premium", "upgrade_premium")]
                        ])
                        
                        remaining_msg = f"\n📊 **{ad_downloads_left} free download(s) remaining**" if ad_downloads_left > 0 else "\n📊 **0 free downloads remaining**"
                        
                        await event.respond(
                            f"✅ **Download complete**{remaining_msg}",
                            buttons=upgrade_markup.to_telethon()
                        )
                    else:
                        # Send simple completion message for premium/admin users
                        await event.respond("✅ **Download complete**")
            finally:
                # CRITICAL: Always cleanup downloaded file, even if errors occur during upload
                cleanup_download(media_path)

        elif chat_message.text or chat_message.message:
            await event.respond(parsed_text or parsed_caption)
        else:
            await event.respond("**No media or text found in the post URL.**")

    except (PeerIdInvalidError, BadRequestError, KeyError):
        await event.respond("**Make sure the user client is part of the chat.**")
    except Exception as e:
        error_message = f"**❌ {str(e)}**"
        await event.respond(error_message)
        LOGGER(__name__).error(e)

@bot.on(events.NewMessage(pattern='/dl', incoming=True, func=lambda e: e.is_private))
@force_subscribe
@check_download_limit
async def download_media(event):
    command = parse_command(event.text)
    if len(command) < 2:
        await event.respond("**Provide a post URL after the /dl command.**")
        return

    post_url = command[1]

    # Check if user has personal session
    user_client, error_code = await get_user_client(event.sender_id)
    
    # Handle session errors
    if error_code == 'no_session':
        await event.respond(
            "❌ **No active session found.**\n\n"
            "Please login with your phone number:\n"
            "`/login +1234567890`"
        )
        return
    elif error_code == 'slots_full':
        from queue_manager import download_queue
        active_count = len(download_queue.active_downloads)
        await event.respond(
            "⏳ **All session slots are currently busy!**\n\n"
            f"👥 **Active users downloading:** {active_count}/3\n\n"
            "💡 **Please wait a few minutes** and try again.\n"
            "Your session will be created automatically when a slot becomes available."
        )
        return
    elif error_code == 'error':
        await event.respond(
            "❌ **Session error occurred.**\n\n"
            "Please try logging in again:\n"
            "`/login +1234567890`"
        )
        return
    
    # Check if user is premium for queue priority
    is_premium = db.get_user_type(event.sender_id) in ['premium', 'admin']
    
    # Add to download queue
    download_coro = handle_download(bot, event, post_url, user_client, True)
    success, msg = await download_queue.add_to_queue(
        event.sender_id,
        download_coro,
        event,
        post_url,
        is_premium
    )
    
    await event.respond(msg)

@bot.on(events.NewMessage(pattern='/bdl', incoming=True, func=lambda e: e.is_private))
@force_subscribe
@paid_or_admin_only
async def download_range(event):
    args = event.text.split()

    if len(args) != 3 or not all(arg.startswith("https://t.me/") for arg in args[1:]):
        await event.respond(
            "🚀 **Batch Download Process**\n"
            "`/bdl start_link end_link`\n\n"
            "💡 **Example:**\n"
            "`/bdl https://t.me/mychannel/100 https://t.me/mychannel/120`"
        )
        return

    # Check if user already has a batch running
    user_tasks = get_user_tasks(event.sender_id)
    if user_tasks:
        running_count = sum(1 for task in user_tasks if not task.done())
        if running_count > 0:
            await event.respond(
                f"❌ **You already have {running_count} download(s) running!**\n\n"
                "Please wait for them to finish or use `/canceldownload` to cancel them."
            )
            return

    try:
        start_chat, start_id = getChatMsgID(args[1])
        end_chat,   end_id   = getChatMsgID(args[2])
    except Exception as e:
        return await event.respond(f"**❌ Error parsing links:\n{e}**")

    if start_chat != end_chat:
        return await event.respond("**❌ Both links must be from the same channel.**")
    if start_id > end_id:
        return await event.respond("**❌ Invalid range: start ID cannot exceed end ID.**")
    
    # Limit batch to 20 posts at a time
    batch_count = end_id - start_id + 1
    if batch_count > 20:
        return await event.respond(
            f"**❌ Batch limit exceeded!**\n\n"
            f"You requested `{batch_count}` posts, but the maximum is **20 posts** at a time.\n\n"
            f"Please reduce your range and try again."
        )

    # Check if user has personal session (required for all users, including admins)
    user_client, error_code = await get_user_client(event.sender_id)
    
    # Handle session errors
    if error_code == 'no_session':
        await event.respond(
            "❌ **No active session found.**\n\n"
            "Please login with your phone number:\n"
            "`/login +1234567890`"
        )
        return
    elif error_code == 'slots_full':
        from queue_manager import download_queue
        active_count = len(download_queue.active_downloads)
        await event.respond(
            "⏳ **All session slots are currently busy!**\n\n"
            f"👥 **Active users downloading:** {active_count}/3\n\n"
            "💡 **Please wait a few minutes** and try again.\n"
            "Batch downloads require an active session slot."
        )
        return
    elif error_code == 'error':
        await event.respond(
            "❌ **Session error occurred.**\n\n"
            "Please try logging in again:\n"
            "`/login +1234567890`"
        )
        return
    
    client_to_use = user_client

    try:
        await client_to_use.get_entity(start_chat)
    except Exception:
        pass

    prefix = args[1].rsplit("/", 1)[0]
    loading = await event.respond(f"📥 **Downloading posts {start_id}–{end_id}…**")

    downloaded = skipped = failed = 0

    for msg_id in range(start_id, end_id + 1):
        url = f"{prefix}/{msg_id}"
        try:
            chat_msg = await client_to_use.get_messages(start_chat, ids=msg_id)
            if not chat_msg:
                skipped += 1
                continue

            has_media = bool(chat_msg.grouped_id or chat_msg.media)
            has_text  = bool(chat_msg.text or chat_msg.message)
            if not (has_media or has_text):
                skipped += 1
                continue

            task = track_task(handle_download(bot, event, url, client_to_use, False), event.sender_id)
            try:
                await task
                downloaded += 1
                # Increment usage count for batch downloads after success
                db.increment_usage(event.sender_id)
            except asyncio.CancelledError:
                await loading.delete()
                # SessionManager will handle client cleanup - no need to stop() here
                return await event.respond(
                    f"**❌ Batch canceled** after downloading `{downloaded}` posts."
                )

        except Exception as e:
            failed += 1
            LOGGER(__name__).error(f"Error at {url}: {e}")

        await asyncio.sleep(3)

    await loading.delete()
    
    # SessionManager will handle client cleanup - no need to stop() here
    
    await event.respond(
        "**✅ Batch Process Complete!**\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"📥 **Downloaded** : `{downloaded}` post(s)\n"
        f"⏭️ **Skipped**    : `{skipped}` (no content)\n"
        f"❌ **Failed**     : `{failed}` error(s)"
    )

# Phone authentication commands
@bot.on(events.NewMessage(pattern='/login', incoming=True, func=lambda e: e.is_private))
@register_user
async def login_command(event):
    """Start login process with phone number"""
    try:
        command = parse_command(event.text)
        if len(command) < 2:
            await event.respond(
                "**Usage:** `/login +1234567890`\n\n"
                "**Example:** `/login +919876543210`\n\n"
                "Make sure to include country code with +"
            )
            return

        phone_number = command[1].strip()

        if not phone_number.startswith('+'):
            await event.respond("❌ **Please include country code with + sign.**\n\n**Example:** `/login +1234567890`")
            return

        # Send OTP
        success, msg, _ = await phone_auth_handler.send_otp(event.sender_id, phone_number)
        await event.respond(msg)

    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")
        LOGGER(__name__).error(f"Error in login_command: {e}")

@bot.on(events.NewMessage(pattern=r'^/verify(\s|$)', incoming=True, func=lambda e: e.is_private))
@register_user
async def verify_command(event):
    """Verify OTP code"""
    try:
        command = parse_command(event.text)
        if len(command) < 2:
            await event.respond(
                "**Usage:** `/verify 1 2 3 4 5` (with spaces between digits)\n\n"
                "**Example:** If code is 12345, send:\n"
                "`/verify 1 2 3 4 5`"
            )
            return

        # Get OTP code (all arguments after /verify)
        otp_code = ' '.join(command[1:])

        # Verify OTP
        LOGGER(__name__).info(f"Calling verify_otp for user {event.sender_id}")
        result = await phone_auth_handler.verify_otp(event.sender_id, otp_code)
        LOGGER(__name__).info(f"verify_otp returned {len(result)} items for user {event.sender_id}")

        if len(result) == 4:
            success, msg, needs_2fa, session_string = result
            LOGGER(__name__).info(f"Received session_string for user {event.sender_id}, length: {len(session_string) if session_string else 0}")
        else:
            success, msg, needs_2fa = result
            session_string = None
            LOGGER(__name__).warning(f"No session_string in result for user {event.sender_id}")

        await event.respond(msg)

        # Save session string if authentication successful
        if success and session_string:
            LOGGER(__name__).info(f"Attempting to save session for user {event.sender_id}")
            result = db.set_user_session(event.sender_id, session_string)
            LOGGER(__name__).info(f"Session save result for user {event.sender_id}: {result}")
            # Verify it was saved
            saved_session = db.get_user_session(event.sender_id)
            if saved_session:
                LOGGER(__name__).info(f"✅ Verified: Session successfully saved and retrieved for user {event.sender_id}")
            else:
                LOGGER(__name__).error(f"❌ ERROR: Session save failed! Could not retrieve session for user {event.sender_id}")
        else:
            LOGGER(__name__).info(f"Not saving session for user {event.sender_id} - success: {success}, has_session_string: {session_string is not None}")

    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")
        LOGGER(__name__).error(f"Error in verify_command: {e}")

@bot.on(events.NewMessage(pattern='/password', incoming=True, func=lambda e: e.is_private))
@register_user
async def password_command(event):
    """Enter 2FA password"""
    try:
        command = parse_command(event.text)
        if len(command) < 2:
            await event.respond(
                "**Usage:** `/password <YOUR_2FA_PASSWORD>`\n\n"
                "**Example:** `/password MySecretPassword123`"
            )
            return

        # Get password (everything after /password)
        password = event.text.split(' ', 1)[1]

        # Verify 2FA
        success, msg, session_string = await phone_auth_handler.verify_2fa_password(event.sender_id, password)
        await event.respond(msg)

        # Save session string if successful
        if success and session_string:
            db.set_user_session(event.sender_id, session_string)
            LOGGER(__name__).info(f"Saved session for user {event.sender_id} after 2FA")

    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")
        LOGGER(__name__).error(f"Error in password_command: {e}")

@bot.on(events.NewMessage(pattern='/logout', incoming=True, func=lambda e: e.is_private))
@register_user
async def logout_command(event):
    """Logout from account"""
    try:
        if db.set_user_session(event.sender_id, None):
            # Also remove from SessionManager to free memory immediately
            from helpers.session_manager import session_manager
            await session_manager.remove_session(event.sender_id)
            
            await event.respond(
                "✅ **Successfully logged out!**\n\n"
                "Use `/login <phone_number>` to login again."
            )
            LOGGER(__name__).info(f"User {event.sender_id} logged out")
        else:
            await event.respond("❌ **You are not logged in.**")

    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")

@bot.on(events.NewMessage(pattern='/cancel', incoming=True, func=lambda e: e.is_private))
@register_user
async def cancel_command(event):
    """Cancel pending authentication"""
    success, msg = await phone_auth_handler.cancel_auth(event.sender_id)
    await event.respond(msg)

@bot.on(events.NewMessage(pattern='/canceldownload', incoming=True, func=lambda e: e.is_private))
@register_user
async def cancel_download_command(event):
    """Cancel user's running downloads"""
    success, msg = await download_queue.cancel_user_download(event.sender_id)
    await event.respond(msg)
    if success:
        LOGGER(__name__).info(f"User {event.sender_id} cancelled download")

@bot.on(events.NewMessage(pattern='/queue', incoming=True, func=lambda e: e.is_private))
@register_user
async def queue_status_command(event):
    """Check your download queue status"""
    status = await download_queue.get_queue_status(event.sender_id)
    await event.respond(status)

@bot.on(events.NewMessage(pattern='/qstatus', incoming=True, func=lambda e: e.is_private))
@admin_only
async def global_queue_status_command(event):
    """Check global download queue status (admin only)"""
    status = await download_queue.get_global_status()
    await event.respond(status)

@bot.on(events.NewMessage(incoming=True, func=lambda e: e.is_private and e.text and not e.text.startswith('/') and is_new_update(e)))
@force_subscribe
@check_download_limit
async def handle_any_message(event):
    if event.text and not event.text.startswith("/"):
        # Check if user is premium for queue priority
        is_premium = db.get_user_type(event.sender_id) in ['premium', 'admin']
        
        # Check if user already has an active download (quick check before getting client)
        async with download_queue._lock:
            if event.sender_id in download_queue.user_queue_positions or event.sender_id in download_queue.active_downloads:
                position = download_queue.get_queue_position(event.sender_id)
                if event.sender_id in download_queue.active_downloads:
                    await event.respond(
                        "❌ **You already have a download in progress!**\n\n"
                        "⏳ Please wait for it to complete.\n\n"
                        "💡 **Want to download this instead?**\n"
                        "Use `/canceldownload` to cancel the current download."
                    )
                    return
                else:
                    await event.respond(
                        f"❌ **You already have a download in the queue!**\n\n"
                        f"📍 **Position:** #{position}/{len(download_queue.waiting_queue)}\n\n"
                        f"💡 **Want to cancel it?**\n"
                        f"Use `/canceldownload` to remove from queue."
                    )
                    return
        
        # Check if user has personal session
        user_client, error_code = await get_user_client(event.sender_id)
        
        # Handle session errors
        if error_code == 'no_session':
            await event.respond(
                "❌ **No active session found.**\n\n"
                "Please login with your phone number:\n"
                "`/login +1234567890`"
            )
            return
        elif error_code == 'slots_full':
            active_count = len(download_queue.active_downloads)
            await event.respond(
                "⏳ **All session slots are currently busy!**\n\n"
                f"👥 **Active users downloading:** {active_count}/3\n\n"
                "💡 **Please wait a few minutes** and try again.\n"
                "Your session will be created automatically when a slot becomes available."
            )
            return
        elif error_code == 'error':
            await event.respond(
                "❌ **Session error occurred.**\n\n"
                "Please try logging in again:\n"
                "`/login +1234567890`"
            )
            return
        
        # Add to download queue
        download_coro = handle_download(bot, event, event.text, user_client, True)
        success, msg = await download_queue.add_to_queue(
            event.sender_id,
            download_coro,
            event,
            event.text,
            is_premium
        )
        
        if msg:  # Only reply if there's a message to send
            await event.respond(msg)

@bot.on(events.NewMessage(pattern='/stats', incoming=True, func=lambda e: e.is_private))
@register_user
async def stats(event):
    currentTime = get_readable_time(int(time() - PyroConf.BOT_START_TIME))
    process = psutil.Process(os.getpid())
    
    bot_memory_mb = round(process.memory_info()[0] / 1024**2)
    cpu_percent = process.cpu_percent(interval=0.1)

    stats_text = (
        "🤖 **BOT STATUS**\n"
        "—————————————————————\n\n"
        "✨ **Status:** Online & Running\n\n"
        "📊 **System Metrics:**\n"
        f"⏱️ Uptime: `{currentTime}`\n"
        f"💾 Memory: `{bot_memory_mb} MiB`\n"
        f"⚡ CPU: `{cpu_percent}%`\n\n"
        "—————————————————————\n\n"
        "💡 **Quick Access:**\n"
        "• `/queue` - Check downloads\n"
        "• `/myinfo` - Your account\n"
        "• `/help` - All commands"
    )
    await event.respond(stats_text)

@bot.on(events.NewMessage(pattern='/logs', incoming=True, func=lambda e: e.is_private))
@admin_only
async def logs(event):
    await event.respond(
        "**📋 Bot Logging**\n\n"
        "Logs are stored in MongoDB and can be viewed via:\n"
        "• Database admin panel\n"
        "• Cloud hosting logs (Render/Railway dashboard)\n\n"
        "Use `/adminstats` for bot statistics."
    )

@bot.on(events.NewMessage(pattern='/killall', incoming=True, func=lambda e: e.is_private))
@admin_only
async def cancel_all_tasks(event):
    queue_cancelled = await download_queue.cancel_all_downloads()
    task_cancelled = 0
    for task in list(RUNNING_TASKS):
        if not task.done():
            task.cancel()
            task_cancelled += 1
    total_cancelled = queue_cancelled + task_cancelled
    await event.respond(
        f"✅ **All downloads cancelled!**\n\n"
        f"📊 **Queue downloads:** {queue_cancelled}\n"
        f"📊 **Other tasks:** {task_cancelled}\n"
        f"📊 **Total:** {total_cancelled}"
    )

# Thumbnail commands
@bot.on(events.NewMessage(pattern='/setthumb', incoming=True, func=lambda e: e.is_private))
@register_user
async def set_thumbnail(event):
    """Set custom thumbnail for video uploads"""
    reply_msg = await event.get_reply_message()
    if reply_msg and reply_msg.photo:
        # User replied to a photo
        photo = reply_msg.photo
        file_id = photo.id
        
        if db.set_custom_thumbnail(event.sender_id, file_id):
            await event.respond(
                "✅ **Custom thumbnail saved successfully!**\n\n"
                "This thumbnail will be used for all your video downloads.\n\n"
                "Use `/delthumb` to remove it."
            )
            LOGGER(__name__).info(f"User {event.sender_id} set custom thumbnail")
        else:
            await event.respond("❌ **Failed to save thumbnail. Please try again.**")
    else:
        await event.respond(
            "📸 **How to set a custom thumbnail:**\n\n"
            "1. Send or forward a photo to the bot\n"
            "2. Reply to that photo with `/setthumb`\n\n"
            "The photo will be used as thumbnail for all your video downloads."
        )

@bot.on(events.NewMessage(pattern='/delthumb', incoming=True, func=lambda e: e.is_private))
@register_user
async def delete_thumbnail(event):
    """Delete custom thumbnail"""
    if db.delete_custom_thumbnail(event.sender_id):
        await event.respond(
            "✅ **Custom thumbnail removed!**\n\n"
            "Videos will now use auto-generated thumbnails from the video itself."
        )
        LOGGER(__name__).info(f"User {event.sender_id} deleted custom thumbnail")
    else:
        await event.respond("ℹ️ **You don't have a custom thumbnail set.**")

@bot.on(events.NewMessage(pattern='/viewthumb', incoming=True, func=lambda e: e.is_private))
@register_user
async def view_thumbnail(event):
    """View current custom thumbnail"""
    thumb_id = db.get_custom_thumbnail(event.sender_id)
    if thumb_id:
        try:
            await event.respond(
                "**Your current custom thumbnail**\n\nUse `/delthumb` to remove it.",
                file=thumb_id
            )
        except:
            await event.respond(
                "⚠️ **Thumbnail exists but couldn't be displayed.**\n\n"
                "It might have expired. Please set a new one with `/setthumb`"
            )
    else:
        await event.respond(
            "ℹ️ **You don't have a custom thumbnail set.**\n\n"
            "Use `/setthumb` to set one."
        )

# Admin commands
@bot.on(events.NewMessage(pattern='/addadmin', incoming=True, func=lambda e: e.is_private))
async def add_admin_handler(event):
    await add_admin_command(event)

@bot.on(events.NewMessage(pattern='/removeadmin', incoming=True, func=lambda e: e.is_private))
async def remove_admin_handler(event):
    await remove_admin_command(event)

@bot.on(events.NewMessage(pattern='/setpremium', incoming=True, func=lambda e: e.is_private))
async def set_premium_handler(event):
    await set_premium_command(event)

@bot.on(events.NewMessage(pattern='/removepremium', incoming=True, func=lambda e: e.is_private))
async def remove_premium_handler(event):
    await remove_premium_command(event)

@bot.on(events.NewMessage(pattern='/ban', incoming=True, func=lambda e: e.is_private))
async def ban_user_handler(event):
    await ban_user_command(event)

@bot.on(events.NewMessage(pattern='/unban', incoming=True, func=lambda e: e.is_private))
async def unban_user_handler(event):
    await unban_user_command(event)

@bot.on(events.NewMessage(pattern='/broadcast', incoming=True, func=lambda e: e.is_private))
async def broadcast_handler(event):
    await broadcast_command(event)

@bot.on(events.NewMessage(pattern='/testdump', incoming=True, func=lambda e: e.is_private))
@admin_only
async def test_dump_channel(event):
    """Test dump channel configuration (admin only)"""
    from config import PyroConf
    
    if not PyroConf.DUMP_CHANNEL_ID:
        await event.respond("❌ **Dump channel not configured**\n\nSet DUMP_CHANNEL_ID in your environment variables.")
        return
    
    try:
        channel_id = int(PyroConf.DUMP_CHANNEL_ID)
        # Try to get chat info
        chat = await bot.get_entity(channel_id)
        
        # Try sending a test message
        test_msg = await bot.send_message(
            channel_id,
            f"✅ **Dump Channel Test**\n\n👤 Test by Admin: {event.sender_id}\n\nDump channel is working correctly!"
        )
        
        await event.respond(
            f"✅ **Dump Channel Working!**\n\n"
            f"📱 **Channel:** {chat.title}\n"
            f"🆔 **ID:** `{channel_id}`\n"
            f"✉️ **Test message sent successfully**\n\n"
            f"All downloaded media will be forwarded to this channel."
        )
    except Exception as e:
        await event.respond(
            f"❌ **Dump Channel Error**\n\n"
            f"**Error:** {str(e)}\n\n"
            f"**How to fix:**\n"
            f"1. Forward any message from your channel to @userinfobot to get the correct channel ID\n"
            f"2. Make sure bot is added to the channel\n"
            f"3. Make bot an administrator with 'Post Messages' permission\n"
            f"4. Update DUMP_CHANNEL_ID in Replit Secrets"
        )

@bot.on(events.NewMessage(pattern='/adminstats', incoming=True, func=lambda e: e.is_private))
async def admin_stats_handler(event):
    await admin_stats_command(event, queue_manager=download_queue)

@bot.on(events.NewMessage(pattern='/getpremium', incoming=True, func=lambda e: e.is_private))
@register_user
async def get_premium_command(event):
    """Generate ad link for temporary premium access"""
    LOGGER(__name__).info(f"get_premium_command triggered by user {event.sender_id}")
    try:
        user_type = db.get_user_type(event.sender_id)
        
        if user_type == 'paid':
            user = db.get_user(event.sender_id)
            expiry_date_str = user.get('subscription_end', 'N/A')
            
            # Calculate time remaining
            time_left_msg = ""
            if expiry_date_str != 'N/A':
                try:
                    from datetime import datetime
                    expiry_date = datetime.strptime(expiry_date_str, '%Y-%m-%d %H:%M:%S')
                    time_remaining = expiry_date - datetime.now()
                    
                    days = time_remaining.days
                    hours = time_remaining.seconds // 3600
                    minutes = (time_remaining.seconds % 3600) // 60
                    
                    if days > 0:
                        time_left_msg = f"⏱️ **Expires in:** {days} days, {hours} hours"
                    elif hours > 0:
                        time_left_msg = f"⏱️ **Expires in:** {hours} hours, {minutes} minutes"
                    else:
                        time_left_msg = f"⏱️ **Expires in:** {minutes} minutes"
                except:
                    time_left_msg = f"📅 **Valid until:** {expiry_date_str}"
            else:
                time_left_msg = "📅 **Permanent premium**"
            
            await event.respond(
                f"✅ **You already have premium subscription!**\n\n"
                f"{time_left_msg}\n\n"
                f"No need to watch ads! Enjoy your unlimited downloads."
            )
            return
        
        bot_domain = PyroConf.get_app_url()
        
        session_id, ad_url = ad_monetization.generate_ad_link(event.sender_id, bot_domain)
        
        premium_text = (
            f"🎬 **Get {PREMIUM_DOWNLOADS} FREE downloads!**\n\n"
            "**How it works:**\n"
            "1️⃣ Click the button below\n"
            "2️⃣ Navigate through pages 1-5 on our blog (2.5 minutes total)\n"
            "3️⃣ A timer will show your progress at the top\n"
            "4️⃣ After completing all pages, get your verification code\n"
            "5️⃣ Send: `/verifypremium <code>`\n\n"
            "⚠️ **Note:** You must visit 5 different pages within the time limit!\n\n"
            "⏱️ Session expires in 30 minutes"
        )
        
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton.url(f"🎁 Start Verification (Visit Pages 1-5)", ad_url)],
            [InlineKeyboardButton.url("❓ Don't know How to Verify", "https://t.me/Wolfy004/43")]
        ])
        
        # Send with video (message ID 42)
        await send_video_message(event, 42, premium_text, markup, "getpremium command")
        LOGGER(__name__).info(f"User {event.sender_id} requested ad-based premium")
        
    except Exception as e:
        await event.respond(f"❌ **Error generating premium link:** {str(e)}")
        LOGGER(__name__).error(f"Error in get_premium_command: {e}")

@bot.on(events.NewMessage(pattern='/verifypremium', incoming=True, func=lambda e: e.is_private))
@register_user
async def verify_premium_command(event):
    """Verify ad completion code and grant temporary premium"""
    LOGGER(__name__).info(f"verify_premium_command triggered by user {event.sender_id}")
    try:
        command = parse_command(event.text)
        if len(command) < 2:
            await event.respond(
                "**Usage:** `/verifypremium <code>`\n\n"
                "**Example:** `/verifypremium ABC123DEF456`\n\n"
                "Get your code by using `/getpremium` first!"
            )
            return
        
        verification_code = command[1].strip()
        
        success, msg = ad_monetization.verify_code(verification_code, event.sender_id)
        
        if success:
            await event.respond(msg)
            LOGGER(__name__).info(f"User {event.sender_id} successfully verified ad code and received downloads")
        else:
            await event.respond(msg)
            
    except Exception as e:
        await event.respond(f"❌ **Error verifying code:** {str(e)}")
        LOGGER(__name__).error(f"Error in verify_premium_command: {e}")

@bot.on(events.NewMessage(pattern='/upgrade', incoming=True, func=lambda e: e.is_private))
@register_user
async def upgrade_command(event):
    """Show premium upgrade information with pricing and payment details"""
    upgrade_text = (
        "💎 **Upgrade to Premium**\n\n"
        "**Premium Features:**\n"
        "✅ Unlimited downloads per day\n"
        "✅ Batch download support (/bdl command)\n"
        "✅ Download up to 20 posts at once\n"
        "✅ Priority support\n"
        "✅ No daily limits\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🎯 Option 1: Watch Ads (FREE)**\n"
        f"📥 **{PREMIUM_DOWNLOADS} Free Downloads**\n"
        "📺 Complete quick verification steps!\n\n"
        "**How it works:**\n"
        "1️⃣ Use `/getpremium` command\n"
        "2️⃣ Click the link and complete 3 steps\n"
        "3️⃣ Get verification code\n"
        "4️⃣ Send code back to bot\n"
        f"5️⃣ Enjoy {PREMIUM_DOWNLOADS} free downloads! 🎉\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**💰 Option 2: Monthly Subscription**\n"
        "💵 **30 Days Premium = $1 USD**\n\n"
        "**How to Subscribe:**\n"
    )
    
    # Add payment information if configured
    payment_methods_available = PyroConf.PAYPAL_URL or PyroConf.UPI_ID or PyroConf.TELEGRAM_TON or PyroConf.CRYPTO_ADDRESS
    
    if payment_methods_available:
        upgrade_text += "1️⃣ **Make Payment (Choose any method):**\n"
        
        if PyroConf.PAYPAL_URL:
            upgrade_text += f"   💳 **PayPal:** {PyroConf.PAYPAL_URL}\n"
        
        if PyroConf.UPI_ID:
            upgrade_text += f"   📱 **UPI (India):** `{PyroConf.UPI_ID}`\n"
        
        if PyroConf.TELEGRAM_TON:
            upgrade_text += f"   🛒 **Telegram Pay (TON):** `{PyroConf.TELEGRAM_TON}`\n"
        
        if PyroConf.CRYPTO_ADDRESS:
            upgrade_text += f"   ₿ **Crypto (USDT/BTC/ETH):** `{PyroConf.CRYPTO_ADDRESS}`\n"
        
        upgrade_text += "\n"
    
    # Add contact information
    if PyroConf.ADMIN_USERNAME:
        upgrade_text += f"2️⃣ **Contact Admin:**\n   👤 @{PyroConf.ADMIN_USERNAME}\n\n"
    else:
        upgrade_text += f"2️⃣ **Contact Admin:**\n   👤 Contact the bot owner\n\n"
    
    upgrade_text += (
        "3️⃣ **Send Payment Proof:**\n"
        "   Send screenshot/transaction ID to admin\n\n"
        "4️⃣ **Get Activated:**\n"
        "   Admin will activate your premium within 24 hours!"
    )
    
    await event.respond(upgrade_text, link_preview=False)

@bot.on(events.NewMessage(pattern='/premiumlist', incoming=True, func=lambda e: e.is_private))
async def premium_list_command(event):
    """Show list of all premium users (Owner only)"""
    if event.sender_id != PyroConf.OWNER_ID:
        await event.respond("❌ **This command is only available to the bot owner.**")
        return
    
    premium_users = db.get_premium_users()
    
    if not premium_users:
        await event.respond("ℹ️ **No premium users found.**")
        return
    
    premium_text = "💎 **Premium Users List**\n\n"
    
    for idx, user in enumerate(premium_users, 1):
        user_id = user.get('user_id', 'Unknown')
        username = user.get('username', 'N/A')
        expiry_date = user.get('premium_expiry', 'N/A')
        
        premium_text += f"{idx}. **User ID:** `{user_id}`\n"
        if username and username != 'N/A':
            premium_text += f"   **Username:** @{username}\n"
        premium_text += f"   **Expires:** {expiry_date}\n\n"
    
    premium_text += f"**Total Premium Users:** {len(premium_users)}"
    
    await event.respond(premium_text)

@bot.on(events.NewMessage(pattern='/myinfo', incoming=True, func=lambda e: e.is_private))
async def myinfo_handler(event):
    await user_info_command(event)

# Callback query handler
@bot.on(events.CallbackQuery())
async def callback_handler(event):
    data = event.data
    
    if isinstance(data, bytes) and data.startswith(b"legal_"):
        await handle_legal_callback(event)
        return
    
    data = event.data.decode('utf-8') if isinstance(event.data, bytes) else event.data
    
    if data == "get_free_premium":
        user_id = event.sender_id
        user_type = db.get_user_type(user_id)
        
        if user_type == 'paid':
            await event.answer("You already have premium subscription!", alert=True)
            return
        
        bot_domain = PyroConf.get_app_url()
        verification_code, ad_url = ad_monetization.generate_ad_link(user_id, bot_domain)
        
        premium_text = (
            f"🎬 **Get {PREMIUM_DOWNLOADS} FREE downloads!**\n\n"
            "**How it works:**\n"
            "1️⃣ Click the button below\n"
            "2️⃣ Navigate through pages 1-5 on our blog (2.5 minutes total)\n"
            "3️⃣ A timer will show your progress at the top\n"
            "4️⃣ After completing all pages, get your verification code\n"
            "5️⃣ Send: `/verifypremium <code>`\n\n"
            "⚠️ **Note:** You must visit 5 different pages within the time limit!\n\n"
            "⏱️ Session expires in 30 minutes"
        )
        
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton.url(f"🎁 Watch Ad & Get {PREMIUM_DOWNLOADS} Downloads", ad_url)],
            [InlineKeyboardButton.url("❓ Don't know How to Verify", "https://t.me/Wolfy004/43")]
        ])
        
        await event.answer()
        
        # Send with video (message ID 42) - create a mock event from the message
        class MessageEvent:
            def __init__(self, message):
                self.message = message
                self.sender_id = message.peer_id.user_id if hasattr(message.peer_id, 'user_id') else user_id
            async def respond(self, *args, **kwargs):
                return await bot.send_message(self.message.peer_id, *args, **kwargs)
        
        msg_event = MessageEvent(event.message if hasattr(event, 'message') else event)
        await send_video_message(msg_event, 42, premium_text, markup, "get_free_premium callback")
        LOGGER(__name__).info(f"User {user_id} requested ad-based premium via button")
        
    elif data == "get_paid_premium":
        await event.answer()
        
        upgrade_text = (
            "💎 **Upgrade to Premium**\n\n"
            "**Premium Features:**\n"
            "✅ Unlimited downloads per day\n"
            "✅ Batch download support (/bdl command)\n"
            "✅ Download up to 20 posts at once\n"
            "✅ Priority support\n"
            "✅ No daily limits\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**🎯 Option 1: Watch Ads (FREE)**\n"
            f"🎁 **Get {PREMIUM_DOWNLOADS} FREE Downloads**\n"
            "📺 Just watch a short ad!\n\n"
            "**How it works:**\n"
            "1️⃣ Use `/getpremium` command\n"
            "2️⃣ Complete 3 verification steps\n"
            "3️⃣ Get verification code\n"
            "4️⃣ Send code back to bot\n"
            f"5️⃣ Enjoy {PREMIUM_DOWNLOADS} free downloads! 🎉\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**💰 Option 2: Monthly Subscription**\n"
            "💵 **30 Days Premium = $1 USD**\n\n"
            "**How to Subscribe:**\n"
        )
        
        payment_methods_available = PyroConf.PAYPAL_URL or PyroConf.UPI_ID or PyroConf.TELEGRAM_TON or PyroConf.CRYPTO_ADDRESS
        
        if payment_methods_available:
            upgrade_text += "1️⃣ **Make Payment (Choose any method):**\n"
            
            if PyroConf.PAYPAL_URL:
                upgrade_text += f"   💳 **PayPal:** {PyroConf.PAYPAL_URL}\n"
            
            if PyroConf.UPI_ID:
                upgrade_text += f"   📱 **UPI (India):** `{PyroConf.UPI_ID}`\n"
            
            if PyroConf.TELEGRAM_TON:
                upgrade_text += f"   🛒 **Telegram Pay (TON):** `{PyroConf.TELEGRAM_TON}`\n"
            
            if PyroConf.CRYPTO_ADDRESS:
                upgrade_text += f"   ₿ **Crypto (USDT/BTC/ETH):** `{PyroConf.CRYPTO_ADDRESS}`\n"
            
            upgrade_text += "\n"
        
        if PyroConf.ADMIN_USERNAME:
            upgrade_text += f"2️⃣ **Contact Admin:**\n   👤 @{PyroConf.ADMIN_USERNAME}\n\n"
        else:
            upgrade_text += f"2️⃣ **Contact Admin:**\n   👤 Contact the bot owner\n\n"
        
        upgrade_text += (
            "3️⃣ **Send Payment Proof:**\n"
            "   Send screenshot/transaction ID to admin\n\n"
            "4️⃣ **Get Activated:**\n"
            "   Admin will activate your premium within 24 hours!"
        )
        
        await bot.send_message(event.chat_id, upgrade_text, link_preview=False)
    
    elif data == "watch_ad_now":
        user_id = event.sender_id
        user_type = db.get_user_type(user_id)
        
        if user_type == 'paid':
            await event.answer("You already have premium subscription!", alert=True)
            return
        
        bot_domain = PyroConf.get_app_url()
        verification_code, ad_url = ad_monetization.generate_ad_link(user_id, bot_domain)
        
        premium_text = (
            f"🎬 **Get {PREMIUM_DOWNLOADS} FREE downloads!**\n\n"
            "**How it works:**\n"
            "1️⃣ Click the button below\n"
            "2️⃣ Navigate through pages 1-5 on our blog (2.5 minutes total)\n"
            "3️⃣ A timer will show your progress at the top\n"
            "4️⃣ After completing all pages, get your verification code\n"
            "5️⃣ Send: `/verifypremium <code>`\n\n"
            "⚠️ **Note:** You must visit 5 different pages within the time limit!\n\n"
            "⏱️ Session expires in 30 minutes"
        )
        
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton.url(f"🎁 Watch Ad & Get {PREMIUM_DOWNLOADS} Downloads", ad_url)],
            [InlineKeyboardButton.url("❓ Don't know How to Verify", "https://t.me/Wolfy004/43")]
        ])
        
        await event.answer()
        
        # Send with video (message ID 42) to the user's chat
        try:
            video_message = await bot.get_messages("Wolfy004", ids=42)
            if video_message and video_message.video:
                await bot.send_message(
                    user_id,
                    premium_text,
                    file=video_message.video,
                    buttons=markup.to_telethon()
                )
            else:
                await bot.send_message(
                    user_id,
                    premium_text,
                    buttons=markup.to_telethon(),
                    link_preview=False
                )
        except Exception as e:
            LOGGER(__name__).warning(f"Could not send video in watch_ad_now callback: {e}")
            await bot.send_message(
                user_id,
                premium_text,
                buttons=markup.to_telethon(),
                link_preview=False
            )
        
        LOGGER(__name__).info(f"User {user_id} requested ad-based download via button")
    
    elif data == "upgrade_premium":
        await event.answer()
        
        upgrade_text = (
            "💎 **Upgrade to Premium**\n\n"
            "**Premium Features:**\n"
            "✅ Unlimited downloads per day\n"
            "✅ Batch download support (/bdl command)\n"
            "✅ Download up to 20 posts at once\n"
            "✅ Priority support\n"
            "✅ No daily limits\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**🎯 Option 1: Watch Ads (FREE)**\n"
            f"🎁 **Get {PREMIUM_DOWNLOADS} FREE Download**\n"
            "📺 Just watch a short ad!\n\n"
            "**How it works:**\n"
            "1️⃣ Use `/getpremium` command\n"
            "2️⃣ Complete 3 verification steps\n"
            "3️⃣ Get verification code\n"
            "4️⃣ Send code back to bot\n"
            f"5️⃣ Enjoy {PREMIUM_DOWNLOADS} free download! 🎉\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**💰 Option 2: Monthly Subscription**\n"
            "💵 **30 Days Premium = $1 USD**\n\n"
            "**How to Subscribe:**\n"
        )
        
        payment_methods_available = PyroConf.PAYPAL_URL or PyroConf.UPI_ID or PyroConf.TELEGRAM_TON or PyroConf.CRYPTO_ADDRESS
        
        if payment_methods_available:
            upgrade_text += "1️⃣ **Make Payment (Choose any method):**\n"
            
            if PyroConf.PAYPAL_URL:
                upgrade_text += f"   💳 **PayPal:** {PyroConf.PAYPAL_URL}\n"
            
            if PyroConf.UPI_ID:
                upgrade_text += f"   📱 **UPI (India):** `{PyroConf.UPI_ID}`\n"
            
            if PyroConf.TELEGRAM_TON:
                upgrade_text += f"   🛒 **Telegram Pay (TON):** `{PyroConf.TELEGRAM_TON}`\n"
            
            if PyroConf.CRYPTO_ADDRESS:
                upgrade_text += f"   ₿ **Crypto (USDT/BTC/ETH):** `{PyroConf.CRYPTO_ADDRESS}`\n"
            
            upgrade_text += "\n"
        
        if PyroConf.ADMIN_USERNAME:
            upgrade_text += f"2️⃣ **Contact Admin:**\n   👤 @{PyroConf.ADMIN_USERNAME}\n\n"
        else:
            upgrade_text += f"2️⃣ **Contact Admin:**\n   👤 Contact the bot owner\n\n"
        
        upgrade_text += (
            "3️⃣ **Send Payment Proof:**\n"
            "   Send screenshot/transaction ID to admin\n\n"
            "4️⃣ **Get Activated:**\n"
            "   Admin will activate your premium within 24 hours!"
        )
        
        await bot.send_message(event.chat_id, upgrade_text, link_preview=False)
        
    else:
        await broadcast_callback_handler(event)

# Queue processor will be started by server_wsgi.py using asyncio (not threading)
# This avoids duplicate initialization and saves RAM by not creating extra threads

# Verify bot attribution on startup
verify_attribution()

# Verify dump channel configuration on startup
async def verify_dump_channel():
    """Verify that dump channel is accessible if configured"""
    from config import PyroConf
    
    if not PyroConf.DUMP_CHANNEL_ID:
        LOGGER(__name__).info("Dump channel not configured (optional feature)")
        return
    
    try:
        channel_id = int(PyroConf.DUMP_CHANNEL_ID)
        # Try to get channel info to verify bot has access
        chat = await bot.get_entity(channel_id)
        chat_title = getattr(chat, 'title', 'Unknown')
        LOGGER(__name__).info(f"✅ Dump channel verified: {chat_title} (ID: {channel_id})")
        LOGGER(__name__).info("All downloaded media will be forwarded to dump channel")
    except Exception as e:
        LOGGER(__name__).error(f"❌ Dump channel configuration error: {e}")
        LOGGER(__name__).error(f"Make sure:")
        LOGGER(__name__).error(f"  1. DUMP_CHANNEL_ID is correct (e.g., -1001234567890)")
        LOGGER(__name__).error(f"  2. Bot is added to the channel as administrator")
        LOGGER(__name__).error(f"  3. Bot has permission to post messages")
        LOGGER(__name__).error(f"Dump channel feature will be disabled until fixed")

# Note: Periodic cleanup task is started from server.py when bot initializes
# This ensures downloaded files are cleaned up every 30 minutes to prevent memory/disk leaks

if __name__ == "__main__":
    try:
        # When running main.py directly (not through server_wsgi.py),
        # we need to start the queue processor using asyncio
        async def start_with_queue():
            from queue_manager import download_queue
            await bot.start(bot_token=PyroConf.BOT_TOKEN)
            await download_queue.start_processor()
            LOGGER(__name__).info("Download queue processor initialized")
            LOGGER(__name__).info("Bot Started!")
            # Wait for the bot to be disconnected (keeps running until stopped)
            await bot.disconnected
        
        import asyncio
        loop = asyncio.get_event_loop()
        loop.run_until_complete(start_with_queue())
    except KeyboardInterrupt:
        pass
    except Exception as err:
        LOGGER(__name__).error(err)
    finally:
        # Gracefully disconnect all user sessions before shutdown
        try:
            import asyncio
            from helpers.session_manager import session_manager
            loop = asyncio.get_event_loop()
            if not loop.is_closed():
                loop.run_until_complete(session_manager.disconnect_all())
                LOGGER(__name__).info("Disconnected all user sessions")
        except Exception as e:
            LOGGER(__name__).error(f"Error disconnecting sessions: {e}")
        
        LOGGER(__name__).info("Bot Stopped")
