import secrets
from datetime import datetime, timedelta

from logger import LOGGER
try:
    from database_sqlite import db
except ImportError:
    from database import db

PREMIUM_DOWNLOADS = 5
SESSION_VALIDITY_MINUTES = 30

class AdMonetization:
    def __init__(self):
        # All ads are on website only - no URL shorteners needed
        self.blog_url = "https://socialhub00.blogspot.com/"
        
        LOGGER(__name__).info("Ad Monetization initialized - using Blogspot with Adsterra ads")
    
    def create_ad_session(self, user_id: int) -> str:
        """Create a temporary session for ad watching"""
        session_id = secrets.token_hex(16)
        db.create_ad_session(session_id, user_id)
        
        LOGGER(__name__).info(f"Created ad session {session_id} for user {user_id}")
        return session_id
    
    def verify_ad_completion(self, session_id: str) -> tuple[bool, str, str]:
        """Verify that user clicked through URL shortener and generate verification code"""
        session_data = db.get_ad_session(session_id)
        
        if not session_data:
            return False, "", "❌ Invalid or expired session. Please start over with /getpremium"
        
        # Check if session expired (30 minutes max)
        elapsed_time = datetime.now() - session_data['created_at']
        if elapsed_time > timedelta(minutes=SESSION_VALIDITY_MINUTES):
            db.delete_ad_session(session_id)
            return False, "", "⏰ Session expired. Please start over with /getpremium"
        
        # Atomically mark session as used (prevents race condition)
        success = db.mark_ad_session_used(session_id)
        if not success:
            return False, "", "❌ This session has already been used. Please use /getpremium to get a new link."
        
        # Generate verification code
        verification_code = self._generate_verification_code(session_data['user_id'])
        
        # Delete session after successful verification
        db.delete_ad_session(session_id)
        
        LOGGER(__name__).info(f"User {session_data['user_id']} completed ad session {session_id}, generated code {verification_code}")
        return True, verification_code, "✅ Ad completed! Here's your verification code"
    
    def _generate_verification_code(self, user_id: int) -> str:
        """Generate verification code after ad is watched"""
        code = secrets.token_hex(4).upper()
        db.create_verification_code(code, user_id)
        
        LOGGER(__name__).info(f"Generated verification code {code} for user {user_id}")
        return code
    
    def verify_code(self, code: str, user_id: int) -> tuple[bool, str]:
        """Verify user's code and grant free downloads"""
        code = code.upper().strip()
        
        verification_data = db.get_verification_code(code)
        
        if not verification_data:
            return False, "❌ **Invalid verification code.**\n\nPlease make sure you entered the code correctly or get a new one with `/getpremium`"
        
        if verification_data['user_id'] != user_id:
            return False, "❌ **This verification code belongs to another user.**"
        
        created_at = verification_data['created_at']
        if datetime.now() - created_at > timedelta(minutes=30):
            db.delete_verification_code(code)
            return False, "⏰ **Verification code has expired.**\n\nCodes expire after 30 minutes. Please get a new one with `/getpremium`"
        
        db.delete_verification_code(code)
        
        # Grant ad downloads
        db.add_ad_downloads(user_id, PREMIUM_DOWNLOADS)
        
        LOGGER(__name__).info(f"User {user_id} successfully verified code {code}, granted {PREMIUM_DOWNLOADS} ad downloads")
        return True, f"✅ **Verification successful!**\n\nYou now have **{PREMIUM_DOWNLOADS} free download(s)**!"
    
    def generate_ad_link(self, user_id: int, bot_domain: str | None = None) -> tuple[str, str]:
        """
        Generate ad link - sends user to blog homepage with session
        Blog's JavaScript will automatically redirect to first verification page
        This way you can change verification pages in theme without updating bot code
        """
        session_id = self.create_ad_session(user_id)
        
        # Send to blog homepage - theme will handle redirect to first page
        first_page_url = f"{self.blog_url}?session={session_id}"
        
        # Add app_url parameter if bot domain is available
        if bot_domain:
            from urllib.parse import quote
            first_page_url += f"&app_url={quote(bot_domain)}"
        
        LOGGER(__name__).info(f"User {user_id}: Sending to blog homepage for ad verification - app_url: {bot_domain}")
        
        return session_id, first_page_url
    
    def get_premium_downloads(self) -> int:
        """Get number of downloads given for watching ads"""
        return PREMIUM_DOWNLOADS

ad_monetization = AdMonetization()
