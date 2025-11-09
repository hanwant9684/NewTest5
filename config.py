# Copyright (C) @Wolfy004
# Channel: https://t.me/Wolfy004

import os
from time import time

class PyroConf:
    try:
        API_ID = int(os.getenv("API_ID", "0"))
    except ValueError:
        API_ID = 0

    API_HASH = os.getenv("API_HASH", "")
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    BOT_USERNAME = os.getenv("BOT_USERNAME", "")
    SESSION_STRING = os.getenv("SESSION_STRING", "")
    
    # MongoDB Configuration
    MONGODB_URI = os.getenv("MONGODB_URI", "")

    try:
        OWNER_ID = int(os.getenv("OWNER_ID", "0"))
    except ValueError:
        OWNER_ID = 0

    FORCE_SUBSCRIBE_CHANNEL = os.getenv("FORCE_SUBSCRIBE_CHANNEL", "")
    
    # Optional Dump Channel - Bot will forward all downloaded media here for monitoring
    # Set this to your channel ID (e.g., -1001234567890) to enable
    # Leave empty to disable
    try:
        dump_channel = os.getenv("DUMP_CHANNEL_ID", "")
        DUMP_CHANNEL_ID = int(dump_channel) if dump_channel else None
    except (ValueError, TypeError):
        DUMP_CHANNEL_ID = None

    # Payment and Contact Configuration
    ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "")
    PAYPAL_URL = os.getenv("PAYPAL_URL", "")
    UPI_ID = os.getenv("UPI_ID", "")
    TELEGRAM_TON= os.getenv("TELEGRAM_TON", "")
    CRYPTO_ADDRESS = os.getenv("CRYPTO_ADDRESS", "")
    
    # Ad Monetization - Droplink.co
    # API key is stored in .env file (DROPLINK_API_KEY)
    
    # Cloud Backup Configuration (GitHub)
    CLOUD_BACKUP_SERVICE = os.getenv("CLOUD_BACKUP_SERVICE", "").lower().strip()
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
    GITHUB_BACKUP_REPO = os.getenv("GITHUB_BACKUP_REPO", "")
    
    try:
        CLOUD_BACKUP_INTERVAL_HOURS = int(os.getenv("CLOUD_BACKUP_INTERVAL_HOURS", "6"))
    except ValueError:
        CLOUD_BACKUP_INTERVAL_HOURS = 6

    BOT_START_TIME = time()
    
    @staticmethod
    def get_app_url() -> str:
        """
        Get the application URL dynamically based on the hosting platform.
        Supports: Railway, Render, Heroku, VPS, Replit, and custom deployments.
        
        Priority order:
        1. APP_URL (custom/manual override)
        2. Railway environment variables
        3. Render environment variables
        4. Heroku environment variables
        5. Replit environment variables
        6. Fallback to empty string
        """
        from logger import LOGGER
        
        def normalize_url(url: str) -> str:
            """Ensure URL has https:// scheme and no trailing slash"""
            if not url:
                return ""
            # Add https:// if no scheme present
            if not url.startswith(("http://", "https://")):
                url = f"https://{url}"
            # Strip trailing slash
            return url.rstrip("/")
        
        # 1. Custom/Manual override (highest priority)
        app_url = os.getenv("APP_URL", "")
        if app_url:
            normalized = normalize_url(app_url)
            LOGGER(__name__).info(f"Using custom APP_URL: {normalized}")
            return normalized
        
        # 2. Railway platform
        railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
        if railway_domain:
            normalized = normalize_url(railway_domain)
            LOGGER(__name__).info(f"Detected Railway domain: {normalized}")
            return normalized
        
        railway_url = os.getenv("RAILWAY_STATIC_URL", "")
        if railway_url:
            normalized = normalize_url(railway_url)
            LOGGER(__name__).info(f"Detected Railway static URL: {normalized}")
            return normalized
        
        # 3. Render platform
        render_url = os.getenv("RENDER_EXTERNAL_URL", "")
        if render_url:
            normalized = normalize_url(render_url)
            LOGGER(__name__).info(f"Detected Render URL: {normalized}")
            return normalized
        
        # 4. Heroku platform
        heroku_app = os.getenv("HEROKU_APP_NAME", "")
        if heroku_app:
            normalized = normalize_url(f"{heroku_app}.herokuapp.com")
            LOGGER(__name__).info(f"Detected Heroku app: {normalized}")
            return normalized
        
        # 5. Replit platform
        replit_domain = os.getenv("REPLIT_DEV_DOMAIN", "")
        if replit_domain:
            normalized = normalize_url(replit_domain)
            LOGGER(__name__).info(f"Detected Replit domain: {normalized}")
            return normalized
        
        # 6. Fallback (empty string will make bot use direct ad URLs)
        LOGGER(__name__).warning("No platform domain detected! Ad verification may use direct ad URLs. Set APP_URL environment variable for custom domains.")
        return ""
