# Telegram Media Bot - Replit Setup

## Overview
This project is a Telegram bot designed to download and forward media from private and public Telegram channels. Its primary purpose is to offer media forwarding with advanced features such as queue management, a premium user system, and ad monetization integration. The bot leverages Telethon for Telegram interaction and includes a WSGI server for handling ad verification, aiming to provide a robust and monetizable media sharing solution.

## User Preferences
None specified yet. Add preferences as they are expressed.

## System Architecture

### Technology Stack
- **Language:** Python 3.12
- **Telegram Client:** Telethon
- **Web Server:** Waitress (WSGI server)
- **Database:** SQLite
- **Event Loop:** uvloop

### Core Features & Design Decisions

1.  **Media Handling:**
    *   **Queue Management:** Implements a download queue with concurrency control.
    *   **Efficient Media Group Downloads:** Processes media group files sequentially (download, upload, delete) to prevent high RAM usage, uploading them as individual messages rather than grouped albums for memory efficiency.
    *   **Streaming Transfers:** Uses Telethon's built-in `iter_download()` for downloads and `upload_file()` for uploads, processing chunk-by-chunk (512KB chunks) without loading entire files into RAM.
    *   **In-Flight Page Cache Eviction:** Evicts kernel page cache during BOTH download and upload phases using `os.posix_fadvise(POSIX_FADV_DONTNEED)` after each 512KB chunk. Prevents page cache from accumulating during large file transfers.
    *   **Maximum Upload Speed:** Configured with `part_size_kb=512` (Telegram's maximum) for fastest possible uploads with minimal protocol overhead.
    *   **Removed FastTelethon:** Replaced parallel connection system with streaming approach for massive RAM reduction (~5-10MB per transfer vs 40-160MB with FastTelethon).

2.  **User & Session Management:**
    *   **Authentication & Access Control:** Features a user authentication and permission system, including phone-based authentication for restricted content.
    *   **Session Pooling:** Manages user sessions with a maximum of 3 concurrent sessions and a 30-minute idle timeout.
    *   **Smart Session Eviction:** Protects active downloads by only evicting idle sessions when all slots are busy, ensuring uninterrupted user experience.
    *   **Legal Acceptance System:** Requires users to accept Terms & Conditions and a Privacy Policy (compliant with Indian and international laws) before using bot features, with acceptance stored persistently in the database.

3.  **Monetization & Ads:**
    *   **Ad Verification System:** Integrates various URL shorteners (e.g., SwiftLnx, UpShrink, Shrtfly, GPLinks) for ad monetization.
    *   **Two-Step Verification:** Employs a landing page with a "Get Verification Code" button to prevent premature code generation by URL shortener validation.

4.  **System Stability & Optimization:**
    *   **RAM Optimization:** Implemented comprehensive memory optimizations including streaming downloads/uploads (~5-10MB per transfer), asynchronous background tasks (using `asyncio.create_task`), and optimized data structures for memory monitoring.
    *   **In-Flight Page Cache Eviction (Downloads & Uploads):** Critical fix for Render/container deployments - uses `os.posix_fadvise(POSIX_FADV_DONTNEED)` to evict kernel page cache chunks immediately after writing during downloads and reading during uploads. Prevents page cache from accumulating during large file transfers (which was causing OOM crashes even when process RSS was only ~67MB).
    *   **Container Memory Monitoring:** Enhanced memory monitoring to track cgroup memory usage (`/sys/fs/cgroup/memory.current`) in addition to process RSS. This shows the actual memory usage that Render/container platforms count against limits (includes both process memory + kernel page cache).
    *   **Cloud-Only Backup:** Simplifies backup strategy to use only GitHub for database persistence, with automatic backups every 10 minutes.
    *   **Robust Error Handling:** Includes graceful shutdown mechanisms and proper background task tracking to prevent resource leaks and errors like "Task was destroyed but it is pending!".
    *   **SQLite Database:** Chosen for its portability and low resource footprint.
    *   **Waitress WSGI Server:** Selected over Flask for its minimal RAM consumption.
    *   **uvloop:** Used for performance enhancement of the event loop.

### Core Modules
-   `main.py`: Main bot logic and Telethon event handlers.
-   `server_wsgi.py`: WSGI server entry point and bot orchestration.
-   `database_sqlite.py`: Manages SQLite database operations.
-   `ad_monetization.py`: Handles ad verification and URL shortener integration.
-   `access_control.py`: Manages user permissions and authentication.
-   `queue_manager.py`: Controls media download queues.
-   `session_manager.py`: Manages user session lifecycles.
-   `cloud_backup.py`: Implements the GitHub-based database backup system.
-   `legal_acceptance.py`: Manages the legal terms acceptance process.

## External Dependencies

-   **Telegram API:** Accessed via the Telethon library using `API_ID` and `API_HASH`.
-   **BotFather:** For obtaining the `BOT_TOKEN`.
-   **URL Shorteners:**
    *   SwiftLnx (`SWIFTLNX_API_KEY`)
    *   UpShrink (`UPSHRINK_API_KEY`)
    *   Shrtfly (`SHRTFLY_API_KEY`)
    *   GPLinks (`GPLINKS_API_KEY`)
-   **GitHub:** Used for cloud-only database backups (`GITHUB_TOKEN`, `GITHUB_BACKUP_REPO`).
-   **Payment Gateways (Optional):** Supports integration with `PAYPAL_URL`, `UPI_ID`, `TELEGRAM_TON`, `CRYPTO_ADDRESS` for premium features.