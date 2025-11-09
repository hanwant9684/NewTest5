# Telegram Media Bot

A Telegram bot for downloading and forwarding media from private/public channels with ad monetization support.

## Features

- Download and forward media from Telegram channels
- Support for multiple media types (photos, videos, documents)
- Queue management for downloads
- Ad monetization with URL shorteners (SwiftLnx, GPLinks, Shrtfly, UpShrink)
- SQLite database for user and session management (portable & lightweight)
- Premium user system
- Admin controls and broadcasting
- Phone authentication for accessing restricted content

## Setup Instructions

### Required Environment Variables

The bot requires the following environment variables to run. You can set them in Replit Secrets:

1. **API_ID** - Telegram API ID
   - Get from: https://my.telegram.org/apps

2. **API_HASH** - Telegram API Hash
   - Get from: https://my.telegram.org/apps

3. **BOT_TOKEN** - Telegram Bot Token
   - Get from: @BotFather on Telegram

4. **OWNER_ID** - Your Telegram user ID (will be auto-added as admin)
   - Get from: @userinfobot on Telegram

### Optional Environment Variables

- **FORCE_SUBSCRIBE_CHANNEL** - Channel username or ID for forced subscription
- **ADMIN_USERNAME** - Bot admin username for contact
- **PAYPAL_URL** - PayPal payment URL for premium subscriptions
- **UPI_ID** - UPI ID for payments
- **SWIFTLNX_API_KEY**, **GPLINKS_API_KEY**, **SHRTFLY_API_KEY**, **UPSHRINK_API_KEY** - URL shortener API keys for ad monetization
- **SESSION_STRING** - Session string for admin downloads (optional)

## How to Run

1. Set all required environment variables in Replit Secrets
2. The bot will start automatically once all required variables are configured
3. The WSGI server will run on port 5000 for ad verification (uses Waitress for optimal performance)

## Project Structure

- `main.py` - Main bot logic and message handlers
- `server_wsgi.py` - Minimal WSGI server for ad verification (replaced Flask)
- `database_sqlite.py` - SQLite database manager (replaced MongoDB)
- `config.py` - Configuration and environment variable loader
- `ad_monetization.py` - Ad monetization logic
- `access_control.py` - User authentication and access control
- `admin_commands.py` - Admin-only commands
- `queue_manager.py` - Download queue management
- `helpers/` - Utility functions for media, files, and messages

## Credits

Created by @Wolfy004
Channel: https://t.me/Wolfy004
