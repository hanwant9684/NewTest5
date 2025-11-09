# Copyright (C) @Wolfy004
# Channel: https://t.me/Wolfy004

import asyncio
from telethon import events
from telethon_helpers import InlineKeyboardButton, InlineKeyboardMarkup, parse_command, get_command_args
from access_control import admin_only, register_user
try:
    from database_sqlite import db
except ImportError:
    from database import db
from logger import LOGGER

@admin_only
async def add_admin_command(event):
    """Add a new admin"""
    try:
        args = get_command_args(event.text)
        if len(args) < 1:
            await event.respond("**Usage:** `/addadmin <user_id>`")
            return

        target_user_id = int(args[0])
        admin_user_id = event.sender_id

        if db.add_admin(target_user_id, admin_user_id):
            try:
                user_info = await event.client.get_entity(target_user_id)
                user_name = user_info.first_name or "Unknown"
            except:
                user_name = str(target_user_id)

            await event.respond(f"✅ **Successfully added {user_name} as admin.**")
            LOGGER(__name__).info(f"Admin {admin_user_id} added {target_user_id} as admin")
        else:
            await event.respond("❌ **Failed to add admin. User might already be an admin.**")

    except ValueError:
        await event.respond("❌ **Invalid user ID. Please provide a numeric user ID.**")
    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")
        LOGGER(__name__).error(f"Error in add_admin_command: {e}")

@admin_only
async def remove_admin_command(event):
    """Remove admin privileges"""
    try:
        args = get_command_args(event.text)
        if len(args) < 1:
            await event.respond("**Usage:** `/removeadmin <user_id>`")
            return

        target_user_id = int(args[0])

        if db.remove_admin(target_user_id):
            await event.respond(f"✅ **Successfully removed admin privileges from user {target_user_id}.**")
            LOGGER(__name__).info(f"Admin {event.sender_id} removed admin privileges from {target_user_id}")
        else:
            await event.respond("❌ **User is not an admin or error occurred.**")

    except ValueError:
        await event.respond("❌ **Invalid user ID. Please provide a numeric user ID.**")
    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")

@admin_only
async def set_premium_command(event):
    """Set user as premium"""
    try:
        args = get_command_args(event.text)

        if len(args) < 1:
            await event.respond("**Usage:** `/setpremium <user_id> [days]`\n\n**Default:** 30 days")
            return

        target_user_id = int(args[0])
        days = int(args[1]) if len(args) > 1 else 30

        if db.set_user_type(target_user_id, 'paid', days):
            await event.respond(f"✅ **Successfully upgraded user {target_user_id} to premium for {days} days.**")
            LOGGER(__name__).info(f"Admin {event.sender_id} set {target_user_id} as premium for {days} days")
        else:
            await event.respond("❌ **Failed to upgrade user.**")

    except ValueError:
        await event.respond("❌ **Invalid input. Use numeric values only.**")
    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")

@admin_only
async def remove_premium_command(event):
    """Remove premium subscription"""
    try:
        args = get_command_args(event.text)
        if len(args) < 1:
            await event.respond("**Usage:** `/removepremium <user_id>`")
            return

        target_user_id = int(args[0])

        if db.set_user_type(target_user_id, 'free'):
            await event.respond(f"✅ **Successfully downgraded user {target_user_id} to free plan.**")
            LOGGER(__name__).info(f"Admin {event.sender_id} removed premium from {target_user_id}")
        else:
            await event.respond("❌ **Failed to downgrade user.**")

    except ValueError:
        await event.respond("❌ **Invalid user ID. Please provide a numeric user ID.**")
    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")

@admin_only
async def ban_user_command(event):
    """Ban a user"""
    try:
        args = get_command_args(event.text)
        if len(args) < 1:
            await event.respond("**Usage:** `/ban <user_id>`")
            return

        target_user_id = int(args[0])

        if target_user_id == event.sender_id:
            await event.respond("❌ **You cannot ban yourself.**")
            return

        if db.is_admin(target_user_id):
            await event.respond("❌ **Cannot ban another admin.**")
            return

        if db.ban_user(target_user_id):
            await event.respond(f"✅ **Successfully banned user {target_user_id}.**")
            LOGGER(__name__).info(f"Admin {event.sender_id} banned {target_user_id}")
        else:
            await event.respond("❌ **Failed to ban user.**")

    except ValueError:
        await event.respond("❌ **Invalid user ID. Please provide a numeric user ID.**")
    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")

@admin_only
async def unban_user_command(event):
    """Unban a user"""
    try:
        args = get_command_args(event.text)
        if len(args) < 1:
            await event.respond("**Usage:** `/unban <user_id>`")
            return

        target_user_id = int(args[0])

        if db.unban_user(target_user_id):
            await event.respond(f"✅ **Successfully unbanned user {target_user_id}.**")
            LOGGER(__name__).info(f"Admin {event.sender_id} unbanned {target_user_id}")
        else:
            await event.respond("❌ **Failed to unban user or user was not banned.**")

    except ValueError:
        await event.respond("❌ **Invalid user ID. Please provide a numeric user ID.**")
    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")

@admin_only
async def broadcast_command(event):
    """Broadcast message/media to all users
    
    Usage:
    - Text: /broadcast <message>
    - Media: Reply to a photo/video/audio/document/GIF with /broadcast <optional caption>
    """
    try:
        broadcast_data = {}
        
        replied_msg = await event.get_reply_message()
        
        if replied_msg:
            caption = None
            args = get_command_args(event.text)
            if len(args) > 0:
                caption = event.text.split(' ', 1)[1]
            elif replied_msg.text:
                caption = replied_msg.text
            
            if replied_msg.photo:
                broadcast_data = {
                    'type': 'photo',
                    'file': replied_msg.photo,
                    'caption': caption
                }
            elif replied_msg.video:
                broadcast_data = {
                    'type': 'video',
                    'file': replied_msg.video,
                    'caption': caption
                }
            elif replied_msg.audio:
                broadcast_data = {
                    'type': 'audio',
                    'file': replied_msg.audio,
                    'caption': caption
                }
            elif replied_msg.voice:
                broadcast_data = {
                    'type': 'voice',
                    'file': replied_msg.voice,
                    'caption': caption
                }
            elif replied_msg.document:
                if replied_msg.gif:
                    broadcast_data = {
                        'type': 'animation',
                        'file': replied_msg.document,
                        'caption': caption
                    }
                else:
                    broadcast_data = {
                        'type': 'document',
                        'file': replied_msg.document,
                        'caption': caption
                    }
            elif replied_msg.sticker:
                broadcast_data = {
                    'type': 'sticker',
                    'file': replied_msg.sticker,
                    'caption': None
                }
            else:
                await event.respond("❌ **Unsupported media type or no media found in the replied message.**")
                return
        else:
            args = get_command_args(event.text)
            if len(args) < 1:
                await event.respond(
                    "**📢 Broadcast Usage:**\n\n"
                    "**Text:** `/broadcast <message>`\n"
                    "**Media:** Reply to a photo/video/audio/document/GIF with `/broadcast <optional caption>`\n\n"
                    "**Examples:**\n"
                    "• `/broadcast Hello everyone! New features available.`\n"
                    "• Reply to a photo: `/broadcast Check out this new update!`\n"
                    "• Reply to a video (no caption): `/broadcast`"
                )
                return
            
            broadcast_data = {
                'type': 'text',
                'message': event.text.split(' ', 1)[1]
            }
        
        if broadcast_data['type'] == 'text':
            preview = broadcast_data['message'][:100] + "..." if len(broadcast_data['message']) > 100 else broadcast_data['message']
            preview_text = f"**📢 Broadcast Preview (Text):**\n\n{preview}"
        else:
            media_type = broadcast_data['type'].upper()
            caption_preview = broadcast_data.get('caption', 'No caption')
            if caption_preview and len(caption_preview) > 100:
                caption_preview = caption_preview[:100] + "..."
            preview_text = f"**📢 Broadcast Preview ({media_type}):**\n\n{caption_preview or 'No caption'}"
        
        confirm_markup = InlineKeyboardMarkup([
            [
                InlineKeyboardButton.callback("✅ Send Broadcast", f"broadcast_confirm:{event.sender_id}"),
                InlineKeyboardButton.callback("❌ Cancel", "broadcast_cancel")
            ]
        ])
        
        await event.respond(
            f"{preview_text}\n\n"
            f"**Are you sure you want to send this to all users?**",
            buttons=confirm_markup.to_telethon()
        )
        
        setattr(event.client, f'pending_broadcast_{event.sender_id}', broadcast_data)
        
    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")
        LOGGER(__name__).error(f"Error in broadcast_command: {e}")

async def execute_broadcast(client, admin_id: int, broadcast_data: dict):
    """Execute the actual broadcast - supports text and all media types"""
    all_users = db.get_all_users()
    total_users = len(all_users)
    successful_sends = 0

    if total_users == 0:
        return 0, 0

    broadcast_type = broadcast_data.get('type', 'text')
    
    for user_id in all_users:
        try:
            if broadcast_type == 'text':
                await client.send_message(user_id, broadcast_data['message'])
            elif broadcast_type == 'photo':
                await client.send_file(
                    user_id, 
                    broadcast_data['file'],
                    caption=broadcast_data.get('caption')
                )
            elif broadcast_type == 'video':
                await client.send_file(
                    user_id, 
                    broadcast_data['file'],
                    caption=broadcast_data.get('caption')
                )
            elif broadcast_type == 'audio':
                await client.send_file(
                    user_id, 
                    broadcast_data['file'],
                    caption=broadcast_data.get('caption'),
                    voice_note=False
                )
            elif broadcast_type == 'voice':
                await client.send_file(
                    user_id, 
                    broadcast_data['file'],
                    caption=broadcast_data.get('caption'),
                    voice_note=True
                )
            elif broadcast_type == 'document':
                await client.send_file(
                    user_id, 
                    broadcast_data['file'],
                    caption=broadcast_data.get('caption')
                )
            elif broadcast_type == 'animation':
                await client.send_file(
                    user_id, 
                    broadcast_data['file'],
                    caption=broadcast_data.get('caption')
                )
            elif broadcast_type == 'sticker':
                await client.send_file(user_id, broadcast_data['file'])
            
            successful_sends += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            LOGGER(__name__).debug(f"Failed to send broadcast to {user_id}: {e}")
            continue

    broadcast_content = broadcast_data.get('message') or broadcast_data.get('caption') or f"[{broadcast_type.upper()} broadcast]"
    db.save_broadcast(broadcast_content, admin_id, total_users, successful_sends)

    return total_users, successful_sends

@admin_only
async def admin_stats_command(event, queue_manager=None):
    """Show detailed admin statistics"""
    try:
        stats = db.get_stats()
        
        active_downloads = 0
        queue_size = 0
        if queue_manager:
            active_downloads = len(queue_manager.active_downloads)
            queue_size = len(queue_manager.waiting_queue)

        stats_text = (
            "👑 **ADMIN DASHBOARD**\n"
            "——————————————————————————\n\n"
            "👥 **User Analytics:**\n"
            f"📊 Total Users: `{stats.get('total_users', 0)}`\n"
            f"💎 Premium Users: `{stats.get('paid_users', 0)}`\n"
            f"🟢 Active (7d): `{stats.get('active_users', 0)}`\n"
            f"🆕 New Today: `{stats.get('today_new_users', 0)}`\n"
            f"🔐 Admins: `{stats.get('admin_count', 0)}`\n\n"
            "📈 **Download Activity:**\n"
            f"📥 Today: `{stats.get('today_downloads', 0)}`\n"
            f"⚡ Active: `{active_downloads}`\n"
            f"📋 Queue: `{queue_size}`\n\n"
            "——————————————————————————\n\n"
            "⚙️ **Quick Admin Actions:**\n"
            "• `/killall` - Cancel all downloads\n"
            "• `/broadcast` - Send message to all\n"
            "• `/logs` - View bot logs"
        )

        await event.respond(stats_text)

    except Exception as e:
        await event.respond(f"❌ **Error getting stats: {str(e)}**")
        LOGGER(__name__).error(f"Error in admin_stats_command: {e}")

@register_user
async def user_info_command(event):
    """Show user information"""
    try:
        user_id = event.sender_id
        user_type = db.get_user_type(user_id)
        daily_usage = db.get_daily_usage(user_id)

        user_info_text = (
            f"**👤 Your Account Information**\n\n"
            f"**User ID:** `{user_id}`\n"
            f"**Account Type:** `{user_type.title()}`\n"
        )

        if user_type == 'free':
            ad_downloads = db.get_ad_downloads(user_id)
            remaining = 1 - daily_usage
            user_info_text += (
                f"**Today's Downloads:** `{daily_usage}/1`\n"
                f"**Remaining:** `{remaining}`\n"
                f"**Ad Downloads:** `{ad_downloads}`\n\n"
                "💎 **Upgrade to Premium for unlimited downloads!**\n"
                "🎁 **Or use** `/getpremium` **to watch ads and get more downloads!**"
            )
        elif user_type == 'paid':
            user = db.get_user(user_id)
            if user and user['subscription_end']:
                user_info_text += f"**Subscription Valid Until:** `{user['subscription_end']}`\n"
            user_info_text += f"**Today's Downloads:** `{daily_usage}` (unlimited)\n"
        else:
            user_info_text += f"**Today's Downloads:** `{daily_usage}` (unlimited)\n**Privileges:** `Administrator`\n"

        await event.respond(user_info_text)

    except Exception as e:
        await event.respond(f"❌ **Error: {str(e)}**")
        LOGGER(__name__).error(f"Error in user_info_command: {e}")

async def broadcast_callback_handler(event):
    """Handle broadcast confirmation callbacks"""
    data = event.data.decode('utf-8') if isinstance(event.data, bytes) else event.data
    user_id = event.sender_id

    if data == "broadcast_cancel":
        await event.edit("❌ **Broadcast cancelled.**")
        return

    if data.startswith("broadcast_confirm:"):
        admin_id = int(data.split(":")[1])

        if user_id != admin_id:
            await event.answer("❌ You are not authorized to confirm this broadcast.", alert=True)
            return

        broadcast_data = getattr(event.client, f'pending_broadcast_{admin_id}', None)

        if not broadcast_data:
            await event.edit("❌ **Broadcast data not found. Please try again.**")
            return

        await event.edit("📡 **Sending broadcast... Please wait.**")

        total_users, successful_sends = await execute_broadcast(event.client, admin_id, broadcast_data)

        if hasattr(event.client, f'pending_broadcast_{admin_id}'):
            delattr(event.client, f'pending_broadcast_{admin_id}')

        result_text = (
            f"✅ **Broadcast Completed!**\n\n"
            f"**Total Users:** `{total_users}`\n"
            f"**Successful Sends:** `{successful_sends}`\n"
            f"**Failed Sends:** `{total_users - successful_sends}`\n"
            f"**Success Rate:** `{(successful_sends/total_users*100):.1f}%`" if total_users > 0 else "**Success Rate:** `0%`"
        )

        await event.edit(result_text)
