import os
import asyncio
from datetime import datetime
from typing import Dict, Set, Optional, Tuple
from dataclasses import dataclass, field
from enum import IntEnum
from logger import LOGGER

class Priority(IntEnum):
    PREMIUM = 1
    FREE = 2

@dataclass(order=True)
class QueueItem:
    priority: int
    timestamp: float = field(compare=True)
    user_id: int = field(compare=False)
    download_coro: any = field(compare=False)
    message: any = field(compare=False)
    post_url: str = field(compare=False)

class DownloadQueueManager:
    def __init__(self, max_concurrent: int = 20, max_queue: int = 100):
        self.max_concurrent = max_concurrent
        self.max_queue = max_queue
        
        self.active_downloads: Set[int] = set()
        self.waiting_queue: list[QueueItem] = []
        
        self.user_queue_positions: Dict[int, QueueItem] = {}
        self.active_tasks: Dict[int, asyncio.Task] = {}
        
        self._lock = asyncio.Lock()
        self._processing = False
        self._processor_task: Optional[asyncio.Task] = None
        
        LOGGER(__name__).info(f"Queue Manager initialized: {max_concurrent} concurrent, {max_queue} max queue")
    
    async def start_processor(self):
        if not self._processing:
            self._processing = True
            self._processor_task = asyncio.create_task(self._process_queue())
            LOGGER(__name__).info("Queue processor started")
    
    async def stop_processor(self):
        self._processing = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        LOGGER(__name__).info("Queue processor stopped")
    
    async def add_to_queue(
        self, 
        user_id: int, 
        download_coro, 
        message,
        post_url: str,
        is_premium: bool = False
    ) -> Tuple[bool, str]:
        async with self._lock:
            if user_id in self.user_queue_positions or user_id in self.active_downloads:
                position = self.get_queue_position(user_id)
                if user_id in self.active_downloads:
                    return False, (
                        "❌ **You already have a download in progress!**\n\n"
                        "⏳ Please wait for it to complete.\n\n"
                        "💡 **Want to download this instead?**\n"
                        "Use `/canceldownload` to cancel the current download."
                    )
                else:
                    return False, (
                        f"❌ **You already have a download in the queue!**\n\n"
                        f"📍 **Position:** #{position}/{len(self.waiting_queue)}\n\n"
                        f"💡 **Want to cancel it?**\n"
                        f"Use `/canceldownload` to remove from queue."
                    )
            
            if len(self.active_downloads) >= self.max_concurrent:
                if len(self.waiting_queue) >= self.max_queue:
                    return False, (
                        f"❌ **Download queue is full!**\n\n"
                        f"🔄 **Active Downloads:** {len(self.active_downloads)}/{self.max_concurrent}\n"
                        f"⏳ **Waiting in Queue:** {len(self.waiting_queue)}/{self.max_queue}\n\n"
                        f"Please try again later."
                    )
                
                priority = Priority.PREMIUM if is_premium else Priority.FREE
                queue_item = QueueItem(
                    priority=priority,
                    timestamp=datetime.now().timestamp(),
                    user_id=user_id,
                    download_coro=download_coro,
                    message=message,
                    post_url=post_url
                )
                
                self.waiting_queue.append(queue_item)
                self.waiting_queue.sort()
                self.user_queue_positions[user_id] = queue_item
                
                position = self.get_queue_position(user_id)
                premium_badge = "👑 **PREMIUM**" if is_premium else "🆓 **FREE**"
                
                # Don't send queue message - only show completion message
                return True, None
            else:
                self.active_downloads.add(user_id)
                task = asyncio.create_task(self._execute_download(user_id, download_coro, message))
                self.active_tasks[user_id] = task
                
                # Don't send download start message - only show completion message
                # status_msg = f"✅ **Download started!**\n\n🔄 **Active Downloads:** {len(self.active_downloads)}/{self.max_concurrent}"
                # asyncio.create_task(self._send_auto_delete_message(message, status_msg, 10))
                
                return True, None
    
    async def _send_auto_delete_message(self, message, text: str, delete_after: int):
        """Send a message and auto-delete it after specified seconds"""
        try:
            sent_msg = await message.reply(text)
            await asyncio.sleep(delete_after)
            await sent_msg.delete()
        except Exception as e:
            LOGGER(__name__).debug(f"Failed to auto-delete message: {e}")
    
    async def _execute_download(self, user_id: int, download_coro, message):
        import gc
        try:
            from memory_monitor import memory_monitor
            memory_monitor.log_memory_snapshot("Download Started", f"User {user_id} | Active: {len(self.active_downloads)}")
            
            # Properly await the coroutine with timeout to prevent hanging
            try:
                await asyncio.wait_for(download_coro, timeout=1800)  # 30 minute timeout
            except asyncio.TimeoutError:
                LOGGER(__name__).error(f"Download timeout for user {user_id} after 30 minutes")
                try:
                    await message.reply("❌ **Download failed:** Timeout after 30 minutes")
                except:
                    pass
                return
            except asyncio.CancelledError:
                LOGGER(__name__).info(f"Download cancelled for user {user_id}")
                raise  # Re-raise to properly handle cancellation
            
            memory_monitor.log_memory_snapshot("Download Completed", f"User {user_id} | Active: {len(self.active_downloads)}")
        except asyncio.CancelledError:
            # Handle cancellation gracefully - don't log as error
            LOGGER(__name__).info(f"Download task cancelled for user {user_id}")
            try:
                await message.reply("❌ **Download cancelled**")
            except:
                pass
        except Exception as e:
            LOGGER(__name__).error(f"Download error for user {user_id}: {e}")
            import traceback
            LOGGER(__name__).error(f"Full traceback: {traceback.format_exc()}")
            try:
                await message.reply(f"❌ **Download failed:** {str(e)}")
            except:
                pass
        finally:
            async with self._lock:
                self.active_downloads.discard(user_id)
                self.active_tasks.pop(user_id, None)
            
            # Force garbage collection to free memory immediately after download
            gc.collect()
            LOGGER(__name__).info(f"Download completed for user {user_id}. Active: {len(self.active_downloads)}. GC triggered.")
    
    async def _process_queue(self):
        while self._processing:
            try:
                await asyncio.sleep(1)
                
                async with self._lock:
                    while len(self.active_downloads) < self.max_concurrent and self.waiting_queue:
                        queue_item = self.waiting_queue.pop(0)
                        user_id = queue_item.user_id
                        
                        self.user_queue_positions.pop(user_id, None)
                        
                        if user_id in self.active_downloads:
                            continue
                        
                        self.active_downloads.add(user_id)
                        
                        # Don't send download start message - only show completion message
                        # try:
                        #     status_msg = f"🚀 **Your download is starting now!**\n\n📥 Downloading: `{queue_item.post_url}`"
                        #     asyncio.create_task(self._send_auto_delete_message(queue_item.message, status_msg, 10))
                        # except:
                        #     pass
                        
                        task = asyncio.create_task(
                            self._execute_download(user_id, queue_item.download_coro, queue_item.message)
                        )
                        self.active_tasks[user_id] = task
                        
                        LOGGER(__name__).info(
                            f"Started queued download for user {user_id}. "
                            f"Active: {len(self.active_downloads)}, Queue: {len(self.waiting_queue)}"
                        )
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                LOGGER(__name__).error(f"Queue processor error: {e}")
    
    def get_queue_position(self, user_id: int) -> int:
        for idx, item in enumerate(self.waiting_queue, 1):
            if item.user_id == user_id:
                return idx
        return 0
    
    async def get_queue_status(self, user_id: int) -> str:
        async with self._lock:
            if user_id in self.active_downloads:
                return (
                    f"📥 **Your download is currently active!**\n\n"
                    f"🔄 **Active Downloads:** {len(self.active_downloads)}/{self.max_concurrent}\n"
                    f"⏳ **Waiting in Queue:** {len(self.waiting_queue)}/{self.max_queue}"
                )
            
            position = self.get_queue_position(user_id)
            if position > 0:
                queue_item = self.user_queue_positions.get(user_id)
                priority_text = "👑 **PREMIUM**" if queue_item and queue_item.priority == Priority.PREMIUM else "🆓 **FREE**"
                
                return (
                    f"⏳ **You're in the queue!**\n\n"
                    f"{priority_text}\n"
                    f"📍 **Your Position:** #{position}/{len(self.waiting_queue)}\n"
                    f"🔄 **Active Downloads:** {len(self.active_downloads)}/{self.max_concurrent}\n\n"
                    f"💡 Estimated wait: ~{position * 2} minutes"
                )
            
            return (
                f"✅ **No active downloads**\n\n"
                f"🔄 **Active Downloads:** {len(self.active_downloads)}/{self.max_concurrent}\n"
                f"⏳ **Waiting in Queue:** {len(self.waiting_queue)}/{self.max_queue}\n\n"
                f"💡 Send a download link to get started!"
            )
    
    async def get_global_status(self) -> str:
        async with self._lock:
            premium_in_queue = sum(1 for item in self.waiting_queue if item.priority == Priority.PREMIUM)
            free_in_queue = len(self.waiting_queue) - premium_in_queue
            
            return (
                f"📊 **Queue System Status**\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"🔄 **Active Downloads:** {len(self.active_downloads)}/{self.max_concurrent}\n"
                f"⏳ **Waiting in Queue:** {len(self.waiting_queue)}/{self.max_queue}\n\n"
                f"👑 Premium in queue: {premium_in_queue}\n"
                f"🆓 Free in queue: {free_in_queue}\n\n"
                f"💡 Premium users get priority!"
            )
    
    async def cancel_user_download(self, user_id: int) -> Tuple[bool, str]:
        async with self._lock:
            if user_id in self.active_downloads:
                task = self.active_tasks.get(user_id)
                if task and not task.done():
                    task.cancel()
                self.active_downloads.discard(user_id)
                self.active_tasks.pop(user_id, None)
                return True, "✅ **Active download cancelled!**"
            
            queue_item = self.user_queue_positions.get(user_id)
            if queue_item and queue_item in self.waiting_queue:
                self.waiting_queue.remove(queue_item)
                self.user_queue_positions.pop(user_id, None)
                return True, "✅ **Removed from download queue!**"
            
            return False, "❌ **No active download or queue entry found.**"
    
    async def cancel_all_downloads(self) -> int:
        async with self._lock:
            cancelled = 0
            
            for task in self.active_tasks.values():
                if not task.done():
                    task.cancel()
                    cancelled += 1
            
            self.active_downloads.clear()
            self.active_tasks.clear()
            
            cancelled += len(self.waiting_queue)
            self.waiting_queue.clear()
            self.user_queue_positions.clear()
            
            LOGGER(__name__).info(f"Cancelled all downloads: {cancelled} total")
            return cancelled
    
    async def sweep_stale_items(self, max_age_minutes: int = 60) -> Dict[str, int]:
        """Remove orphaned queue items and tasks that are no longer running.
        This prevents memory leaks from aborted downloads.
        Returns counts of cleaned items."""
        async with self._lock:
            import gc
            from datetime import datetime
            
            cleanup_count = 0
            task_cleanup_count = 0
            
            # Remove queue items older than max_age_minutes
            cutoff_timestamp = (datetime.now().timestamp() - (max_age_minutes * 60))
            stale_items = [item for item in self.waiting_queue if item.timestamp < cutoff_timestamp]
            
            for stale_item in stale_items:
                try:
                    self.waiting_queue.remove(stale_item)
                    self.user_queue_positions.pop(stale_item.user_id, None)
                    cleanup_count += 1
                    LOGGER(__name__).warning(
                        f"Cleaned up stale queue item for user {stale_item.user_id} "
                        f"(age: {int((datetime.now().timestamp() - stale_item.timestamp) / 60)} minutes)"
                    )
                except Exception as e:
                    LOGGER(__name__).error(f"Error removing stale queue item: {e}")
            
            # Remove orphaned tasks (done/cancelled but not cleaned up)
            orphaned_users = []
            for user_id, task in list(self.active_tasks.items()):
                if task.done() or task.cancelled():
                    orphaned_users.append(user_id)
                    self.active_tasks.pop(user_id, None)
                    self.active_downloads.discard(user_id)
                    task_cleanup_count += 1
                    LOGGER(__name__).warning(f"Cleaned up orphaned task for user {user_id}")
            
            if cleanup_count > 0 or task_cleanup_count > 0:
                LOGGER(__name__).info(
                    f"Queue sweep: cleaned {cleanup_count} stale items and "
                    f"{task_cleanup_count} orphaned tasks"
                )
                # Force garbage collection after cleanup
                gc.collect()
            
            return {
                'stale_items': cleanup_count,
                'orphaned_tasks': task_cleanup_count
            }

# Detect constrained environments (Render, Replit) and reduce queue size
IS_CONSTRAINED = bool(
    os.getenv('RENDER') or 
    os.getenv('RENDER_EXTERNAL_URL') or 
    os.getenv('REPLIT_DEPLOYMENT') or 
    os.getenv('REPL_ID')
)

# ULTRA-aggressive settings for Render's 512MB RAM limit
# Render free tier: 3 concurrent downloads (prevents OOM), 20 max queue
# Normal deployment: 20 concurrent downloads, 100 max queue
# Note: Large video downloads can use 100-150MB each when buffering
# With 3 concurrent: 3*150MB + 60MB base = 510MB max (safe for 512MB limit)
MAX_CONCURRENT = 3 if IS_CONSTRAINED else 20
MAX_QUEUE = 20 if IS_CONSTRAINED else 100

download_queue = DownloadQueueManager(max_concurrent=MAX_CONCURRENT, max_queue=MAX_QUEUE)
