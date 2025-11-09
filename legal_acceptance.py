# Copyright (C) @Wolfy004
# Channel: https://t.me/Wolfy004
# Legal acceptance handler for Terms & Conditions and Privacy Policy

import os
from functools import wraps
from telethon_helpers import InlineKeyboardButton, InlineKeyboardMarkup
from logger import LOGGER

try:
    from database_sqlite import db
except ImportError:
    from database import db

LEGAL_DIR = "legal"
TERMS_FILE = os.path.join(LEGAL_DIR, "terms_and_conditions.txt")
PRIVACY_FILE = os.path.join(LEGAL_DIR, "privacy_policy.txt")

def load_legal_document(file_path: str) -> str:
    """Load legal document from file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        LOGGER(__name__).error(f"Error loading legal document {file_path}: {e}")
        return ""

def get_legal_summary() -> str:
    """Get a summary of legal terms for display"""
    return (
        "⚖️ **TERMS & CONDITIONS AND PRIVACY POLICY**\n\n"
        "📜 **Before using this bot, you must accept our legal terms.**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔴 **IMPORTANT DISCLAIMERS:**\n\n"
        "1️⃣ **User Responsibility:**\n"
        "   • You are SOLELY responsible for the content you download\n"
        "   • You must ensure your use complies with all applicable laws\n"
        "   • The bot owner is NOT liable for your actions\n\n"
        "2️⃣ **Legal Compliance:**\n"
        "   • You must comply with copyright laws\n"
        "   • You must comply with the IT Act, 2000 (India)\n"
        "   • You must comply with GDPR (if applicable)\n"
        "   • You must NOT use the service for illegal purposes\n\n"
        "3️⃣ **Age Restriction:**\n"
        "   • You must be 18 years or older to use this service\n\n"
        "4️⃣ **Data Collection:**\n"
        "   • We collect: User ID, username, download logs\n"
        "   • Data is stored securely and not sold to third parties\n"
        "   • You have the right to request data deletion\n\n"
        "5️⃣ **No Warranty:**\n"
        "   • Service provided \"AS IS\" without warranties\n"
        "   • Bot owner not responsible for service availability\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📄 **Full Documents:**\n\n"
        "Click the buttons below to view complete documents:\n"
        "• Terms and Conditions\n"
        "• Privacy Policy\n\n"
        "⚠️ **By clicking 'I Accept', you confirm that:**\n"
        "   ✅ You have read and understood both documents\n"
        "   ✅ You are 18 years or older\n"
        "   ✅ You agree to all terms and conditions\n"
        "   ✅ You acknowledge sole responsibility for your actions\n\n"
        "❌ **Declining means you cannot use this bot.**"
    )

def get_terms_preview() -> str:
    """Get a preview of Terms and Conditions"""
    terms = load_legal_document(TERMS_FILE)
    if not terms:
        return "❌ Terms and Conditions document not found."
    
    lines = terms.split('\n')
    preview = '\n'.join(lines[:100])
    
    if len(lines) > 100:
        preview += "\n\n... (document continues)\n\nClick 'View Full Terms' to read the complete document."
    
    return f"📜 **TERMS AND CONDITIONS**\n\n{preview}"

def get_privacy_preview() -> str:
    """Get a preview of Privacy Policy"""
    privacy = load_legal_document(PRIVACY_FILE)
    if not privacy:
        return "❌ Privacy Policy document not found."
    
    lines = privacy.split('\n')
    preview = '\n'.join(lines[:100])
    
    if len(lines) > 100:
        preview += "\n\n... (document continues)\n\nClick 'View Full Privacy Policy' to read the complete document."
    
    return f"🔒 **PRIVACY POLICY**\n\n{preview}"

def get_full_terms() -> str:
    """Get full Terms and Conditions"""
    terms = load_legal_document(TERMS_FILE)
    if not terms:
        return "❌ Terms and Conditions document not found."
    return f"📜 **TERMS AND CONDITIONS (FULL)**\n\n{terms}"

def get_full_privacy() -> str:
    """Get full Privacy Policy"""
    privacy = load_legal_document(PRIVACY_FILE)
    if not privacy:
        return "❌ Privacy Policy document not found."
    return f"🔒 **PRIVACY POLICY (FULL)**\n\n{privacy}"

async def show_legal_acceptance(event):
    """Show legal acceptance screen to user"""
    try:
        summary = get_legal_summary()
        
        markup = InlineKeyboardMarkup([
            [
                InlineKeyboardButton.callback("📜 View Terms & Conditions", "legal_view_terms"),
                InlineKeyboardButton.callback("🔒 View Privacy Policy", "legal_view_privacy")
            ],
            [
                InlineKeyboardButton.callback("✅ I Accept (Age 18+)", "legal_accept"),
                InlineKeyboardButton.callback("❌ I Decline", "legal_decline")
            ]
        ])
        
        await event.respond(summary, buttons=markup.to_telethon(), link_preview=False)
        LOGGER(__name__).info(f"Shown legal acceptance screen to user {event.sender_id}")
        
    except Exception as e:
        LOGGER(__name__).error(f"Error showing legal acceptance: {e}")
        await event.respond(
            "❌ Error displaying legal terms. Please contact support."
        )

def require_legal_acceptance(func):
    """Decorator to check if user has accepted legal terms before executing command"""
    @wraps(func)
    async def wrapper(event):
        user_id = event.sender_id
        
        if db.check_legal_acceptance(user_id):
            return await func(event)
        
        LOGGER(__name__).info(f"User {user_id} attempted to use bot without accepting legal terms")
        
        await event.respond(
            "⚠️ **Legal Acceptance Required**\n\n"
            "You must accept our Terms & Conditions and Privacy Policy before using this bot.\n\n"
            "Please use /start to view and accept the legal terms."
        )
        
        await show_legal_acceptance(event)
        
    return wrapper

async def handle_legal_callback(event):
    """Handle callback queries for legal acceptance"""
    data = event.data
    user_id = event.sender_id
    
    try:
        if data == b"legal_view_terms":
            terms_preview = get_terms_preview()
            
            if len(terms_preview) > 4000:
                chunks = [terms_preview[i:i+4000] for i in range(0, len(terms_preview), 4000)]
                for i, chunk in enumerate(chunks):
                    if i == 0:
                        markup = InlineKeyboardMarkup([[
                            InlineKeyboardButton.callback("⬅️ Back", "legal_back"),
                            InlineKeyboardButton.callback("📄 Full Document", "legal_full_terms")
                        ]])
                        await event.respond(chunk, buttons=markup.to_telethon(), link_preview=False)
                    else:
                        await event.respond(chunk, link_preview=False)
            else:
                markup = InlineKeyboardMarkup([[
                    InlineKeyboardButton.callback("⬅️ Back", "legal_back"),
                    InlineKeyboardButton.callback("📄 Full Document", "legal_full_terms")
                ]])
                await event.respond(terms_preview, buttons=markup.to_telethon(), link_preview=False)
            
            await event.answer()
            LOGGER(__name__).info(f"User {user_id} viewed Terms & Conditions preview")
        
        elif data == b"legal_view_privacy":
            privacy_preview = get_privacy_preview()
            
            if len(privacy_preview) > 4000:
                chunks = [privacy_preview[i:i+4000] for i in range(0, len(privacy_preview), 4000)]
                for i, chunk in enumerate(chunks):
                    if i == 0:
                        markup = InlineKeyboardMarkup([[
                            InlineKeyboardButton.callback("⬅️ Back", "legal_back"),
                            InlineKeyboardButton.callback("📄 Full Document", "legal_full_privacy")
                        ]])
                        await event.respond(chunk, buttons=markup.to_telethon(), link_preview=False)
                    else:
                        await event.respond(chunk, link_preview=False)
            else:
                markup = InlineKeyboardMarkup([[
                    InlineKeyboardButton.callback("⬅️ Back", "legal_back"),
                    InlineKeyboardButton.callback("📄 Full Document", "legal_full_privacy")
                ]])
                await event.respond(privacy_preview, buttons=markup.to_telethon(), link_preview=False)
            
            await event.answer()
            LOGGER(__name__).info(f"User {user_id} viewed Privacy Policy preview")
        
        elif data == b"legal_full_terms":
            full_terms = get_full_terms()
            
            chunks = [full_terms[i:i+4000] for i in range(0, len(full_terms), 4000)]
            for i, chunk in enumerate(chunks):
                if i == len(chunks) - 1:
                    markup = InlineKeyboardMarkup([[
                        InlineKeyboardButton.callback("⬅️ Back", "legal_back")
                    ]])
                    await event.respond(chunk, buttons=markup.to_telethon(), link_preview=False)
                else:
                    await event.respond(chunk, link_preview=False)
            
            await event.answer()
            LOGGER(__name__).info(f"User {user_id} viewed full Terms & Conditions")
        
        elif data == b"legal_full_privacy":
            full_privacy = get_full_privacy()
            
            chunks = [full_privacy[i:i+4000] for i in range(0, len(full_privacy), 4000)]
            for i, chunk in enumerate(chunks):
                if i == len(chunks) - 1:
                    markup = InlineKeyboardMarkup([[
                        InlineKeyboardButton.callback("⬅️ Back", "legal_back")
                    ]])
                    await event.respond(chunk, buttons=markup.to_telethon(), link_preview=False)
                else:
                    await event.respond(chunk, link_preview=False)
            
            await event.answer()
            LOGGER(__name__).info(f"User {user_id} viewed full Privacy Policy")
        
        elif data == b"legal_back":
            await show_legal_acceptance(event)
            await event.answer()
        
        elif data == b"legal_accept":
            success = db.record_legal_acceptance(user_id)
            
            if success:
                await event.answer("✅ Accepted!")
                await event.respond(
                    "✅ **Thank you for accepting!**\n\n"
                    "You can now use the bot. Use /start to see the welcome message and get started!\n\n"
                    "💡 **Quick Start:**\n"
                    "1. Login with `/login +your_phone`\n"
                    "2. Verify with OTP\n"
                    "3. Start downloading by pasting any Telegram link!"
                )
                LOGGER(__name__).info(f"✅ User {user_id} ACCEPTED legal terms")
            else:
                await event.answer("❌ Error recording acceptance")
                await event.respond(
                    "❌ Error recording your acceptance. Please try again or contact support."
                )
                LOGGER(__name__).error(f"Failed to record legal acceptance for user {user_id}")
        
        elif data == b"legal_decline":
            await event.answer("❌ Declined")
            await event.respond(
                "❌ **Terms Declined**\n\n"
                "You have declined the Terms & Conditions and Privacy Policy.\n\n"
                "Unfortunately, you cannot use this bot without accepting our legal terms.\n\n"
                "If you change your mind, use /start again to review and accept the terms."
            )
            LOGGER(__name__).info(f"❌ User {user_id} DECLINED legal terms")
    
    except Exception as e:
        LOGGER(__name__).error(f"Error handling legal callback for user {user_id}: {e}")
        try:
            await event.answer("❌ An error occurred")
        except:
            pass
