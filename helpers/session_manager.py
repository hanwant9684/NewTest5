# Session Manager for Telethon Client instances
# Limits active user sessions to reduce memory usage
# Each Telethon Client uses less RAM than Pyrogram (~60-80MB vs ~100MB)

import asyncio
from typing import Dict, Optional
from collections import OrderedDict
from time import time
from telethon import TelegramClient
from telethon.sessions import StringSession
from logger import LOGGER

class SessionManager:
    """
    Manages Telethon Client instances with a maximum limit
    Automatically disconnects oldest sessions when limit is reached
    Also disconnects idle sessions after timeout to prevent memory leaks
    This prevents memory exhaustion from too many active user sessions
    """
    
    def __init__(self, max_sessions: int = 5, idle_timeout_minutes: int = 30):
        """
        Args:
            max_sessions: Maximum number of concurrent user sessions
                         Each session uses ~60-80MB with Telethon
            idle_timeout_minutes: Minutes of inactivity before session is disconnected
        """
        self.max_sessions = max_sessions
        self.idle_timeout_minutes = idle_timeout_minutes
        self.idle_timeout_seconds = idle_timeout_minutes * 60
        self.active_sessions: OrderedDict[int, TelegramClient] = OrderedDict()
        self.last_activity: Dict[int, float] = {}  # Track last activity time per user
        self._lock = asyncio.Lock()
        self._cleanup_task = None
        LOGGER(__name__).info(f"Session Manager initialized: max {max_sessions} concurrent sessions, {idle_timeout_minutes}min idle timeout")
    
    async def get_or_create_session(
        self, 
        user_id: int, 
        session_string: str,
        api_id: int,
        api_hash: str
    ):
        """
        Get existing session or create new one
        If max sessions reached, disconnects oldest IDLE session first
        IMPORTANT: Never disconnects sessions with active downloads to prevent interrupted downloads
        
        Returns:
            tuple: (client, error_code) where:
                - (TelegramClient, None) if successful
                - (None, 'slots_full') if all slots have active downloads
                - (None, 'invalid_session') if session is not authorized
                - (None, 'creation_failed') if session creation failed
        """
        async with self._lock:
            # Check if user already has active session
            if user_id in self.active_sessions:
                # Move to end (most recently used)
                self.active_sessions.move_to_end(user_id)
                # Update last activity time
                self.last_activity[user_id] = time()
                LOGGER(__name__).debug(f"Reusing existing session for user {user_id}")
                return (self.active_sessions[user_id], None)
            
            # If at capacity, try to disconnect oldest IDLE session (no active downloads)
            if len(self.active_sessions) >= self.max_sessions:
                from queue_manager import download_queue
                
                # Find sessions without active downloads (safe to evict)
                evictable_sessions = []
                for uid in self.active_sessions.keys():
                    if uid not in download_queue.active_downloads:
                        evictable_sessions.append(uid)
                
                # If we have sessions that can be safely evicted, evict the oldest one
                if evictable_sessions:
                    oldest_idle_user = evictable_sessions[0]
                    oldest_client = self.active_sessions.pop(oldest_idle_user)
                    try:
                        from memory_monitor import memory_monitor
                        memory_monitor.track_session_cleanup(oldest_idle_user)
                        await oldest_client.disconnect()
                        # Clear activity timestamp for evicted session
                        self.last_activity.pop(oldest_idle_user, None)
                        LOGGER(__name__).info(f"Disconnected oldest idle session: user {oldest_idle_user} (no active downloads)")
                        memory_monitor.log_memory_snapshot("Session Disconnected", f"Freed idle session for user {oldest_idle_user}")
                    except Exception as e:
                        LOGGER(__name__).error(f"Error disconnecting session {oldest_idle_user}: {e}")
                else:
                    # All sessions have active downloads - cannot evict safely
                    LOGGER(__name__).warning(
                        f"Cannot create session for user {user_id}: all {self.max_sessions} sessions "
                        f"have active downloads. User must wait."
                    )
                    return (None, 'slots_full')
            
            # Create new session
            try:
                from memory_monitor import memory_monitor
                
                memory_monitor.track_session_creation(user_id)
                
                # Create Telethon client with StringSession
                client = TelegramClient(
                    StringSession(session_string),
                    api_id,
                    api_hash,
                    connection_retries=3,
                    retry_delay=1,
                    auto_reconnect=True,
                    timeout=10
                )
                
                # Connect the client
                await client.connect()
                
                # Verify the session is valid
                if not await client.is_user_authorized():
                    LOGGER(__name__).error(f"Session for user {user_id} is not authorized")
                    await client.disconnect()
                    return (None, 'invalid_session')
                
                self.active_sessions[user_id] = client
                # Track activity time
                self.last_activity[user_id] = time()
                LOGGER(__name__).info(f"Created new session for user {user_id} ({len(self.active_sessions)}/{self.max_sessions})")
                
                memory_monitor.log_memory_snapshot("Session Created", f"User {user_id} - Total sessions: {len(self.active_sessions)}")
                
                return (client, None)
                
            except Exception as e:
                LOGGER(__name__).error(f"Failed to create session for user {user_id}: {e}")
                return (None, 'creation_failed')
    
    async def remove_session(self, user_id: int):
        """Remove and disconnect a specific user session"""
        async with self._lock:
            if user_id in self.active_sessions:
                try:
                    from memory_monitor import memory_monitor
                    memory_monitor.track_session_cleanup(user_id)
                    await self.active_sessions[user_id].disconnect()
                    del self.active_sessions[user_id]
                    self.last_activity.pop(user_id, None)
                    LOGGER(__name__).info(f"Removed session for user {user_id}")
                    memory_monitor.log_memory_snapshot("Session Removed", f"User {user_id}")
                except Exception as e:
                    LOGGER(__name__).error(f"Error removing session {user_id}: {e}")
    
    async def disconnect_all(self):
        """Disconnect all active sessions (for shutdown)"""
        async with self._lock:
            for user_id, client in list(self.active_sessions.items()):
                try:
                    await client.disconnect()
                except:
                    pass
            self.active_sessions.clear()
            self.last_activity.clear()
            LOGGER(__name__).info("All sessions disconnected")
    
    async def cleanup_idle_sessions(self):
        """Disconnect sessions that have been idle for too long"""
        current_time = time()
        disconnected_count = 0
        
        async with self._lock:
            # Find idle sessions
            idle_users = []
            for user_id, last_active in list(self.last_activity.items()):
                idle_seconds = current_time - last_active
                if idle_seconds > self.idle_timeout_seconds:
                    idle_users.append(user_id)
            
            # Disconnect idle sessions
            for user_id in idle_users:
                if user_id in self.active_sessions:
                    try:
                        from memory_monitor import memory_monitor
                        idle_minutes = (current_time - self.last_activity[user_id]) / 60
                        LOGGER(__name__).info(f"Disconnecting idle session for user {user_id} (idle for {idle_minutes:.1f} minutes)")
                        
                        memory_monitor.track_session_cleanup(user_id)
                        await self.active_sessions[user_id].disconnect()
                        del self.active_sessions[user_id]
                        del self.last_activity[user_id]
                        disconnected_count += 1
                        
                        memory_monitor.log_memory_snapshot("Idle Session Cleanup", f"User {user_id} idle for {idle_minutes:.1f}min")
                    except Exception as e:
                        LOGGER(__name__).error(f"Error disconnecting idle session {user_id}: {e}")
        
        if disconnected_count > 0:
            LOGGER(__name__).info(f"Cleaned up {disconnected_count} idle sessions. Active sessions: {len(self.active_sessions)}")
        
        return disconnected_count
    
    async def start_cleanup_task(self):
        """Start periodic cleanup of idle sessions"""
        if self._cleanup_task is not None:
            return
        
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
        LOGGER(__name__).info(f"Started periodic session cleanup (every 10 minutes)")
    
    async def _periodic_cleanup(self):
        """Background task that periodically cleans up idle sessions"""
        while True:
            try:
                # Run cleanup every 10 minutes
                await asyncio.sleep(600)
                await self.cleanup_idle_sessions()
            except asyncio.CancelledError:
                break
            except Exception as e:
                LOGGER(__name__).error(f"Error in periodic session cleanup: {e}")
    
    def get_active_count(self) -> int:
        """Get number of currently active sessions"""
        return len(self.active_sessions)

# Global session manager instance (import this in other modules)
# Limit to 3 sessions on Render/Replit (3 * 70MB = ~210MB)
# Limit to 5 sessions on normal deployment (5 * 70MB = ~350MB)
import os
IS_CONSTRAINED = bool(
    os.getenv('RENDER') or 
    os.getenv('RENDER_EXTERNAL_URL') or 
    os.getenv('REPLIT_DEPLOYMENT') or 
    os.getenv('REPL_ID')
)

MAX_SESSIONS = 3 if IS_CONSTRAINED else 5
session_manager = SessionManager(max_sessions=MAX_SESSIONS)
