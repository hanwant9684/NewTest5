import os
import psutil
import asyncio
import traceback
from datetime import datetime
from logger import LOGGER

class MemoryMonitor:
    def __init__(self):
        self.process = psutil.Process()
        self.logger = LOGGER(__name__)
        self.last_memory_mb = 0
        self.memory_threshold_mb = 400  # Alert if memory exceeds 400MB on 512MB plan
        self.spike_threshold_mb = 50  # Alert if memory increases by 50MB suddenly
        # Use collections.deque for memory-efficient circular buffer
        from collections import deque
        self.operation_history = deque(maxlen=20)  # Auto-discards old items, saves RAM
        self.max_history = 20
        
        # Dedicated memory log file for debugging OOM issues on Render
        self.memory_log_file = "memory_debug.log"
        self._init_memory_log()
    
    def _init_memory_log(self):
        """Initialize dedicated memory log file"""
        try:
            # Check if file exists (indicates recovery from crash)
            recovering_from_crash = os.path.exists(self.memory_log_file)
            
            if recovering_from_crash:
                # Append recovery message instead of overwriting
                with open(self.memory_log_file, 'a') as f:
                    f.write("\n\n")
                    f.write("=" * 80 + "\n")
                    f.write(f"🔄 BOT RESTARTED at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("Previous session may have crashed - check logs above\n")
                    f.write("=" * 80 + "\n\n")
                self.logger.warning("⚠️ Found existing memory log - bot may have crashed previously")
            else:
                # Write header to new memory log file
                with open(self.memory_log_file, 'w') as f:
                    f.write("=" * 80 + "\n")
                    f.write("MEMORY DEBUG LOG - Telegram Bot on Render 512MB Plan\n")
                    f.write(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("=" * 80 + "\n\n")
                    f.write("This file captures critical memory events to help debug OOM crashes.\n")
                    f.write("Check this file after crashes to see what happened before running out of RAM.\n\n")
                    f.write("-" * 80 + "\n\n")
                self.logger.info(f"Memory debug log initialized: {self.memory_log_file}")
        except Exception as e:
            self.logger.error(f"Failed to initialize memory log file: {e}")
    
    def _write_to_memory_log(self, message, force_write=False):
        """Write critical memory events to dedicated log file.
        Only writes when:
        - force_write=True (when /memory-debug is accessed)
        - Memory >= 400MB
        - Memory > 480MB (about to crash)
        """
        try:
            # Only write if forced or memory is critical
            if not force_write:
                mem = self.get_memory_info()
                if mem['rss_mb'] < 400:
                    return  # Skip writing for normal memory usage
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(self.memory_log_file, 'a') as f:
                f.write(f"[{timestamp}] {message}\n")
                f.flush()  # Force write to disk immediately
        except Exception as e:
            self.logger.error(f"Failed to write to memory log: {e}")
        
    def get_memory_info(self):
        memory_info = self.process.memory_info()
        rss_mb = memory_info.rss / 1024 / 1024  # Convert to MB
        vms_mb = memory_info.vms / 1024 / 1024
        
        # Get system memory
        system_memory = psutil.virtual_memory()
        system_total_mb = system_memory.total / 1024 / 1024
        system_available_mb = system_memory.available / 1024 / 1024
        system_percent = system_memory.percent
        
        return {
            'rss_mb': round(rss_mb, 2),
            'vms_mb': round(vms_mb, 2),
            'system_total_mb': round(system_total_mb, 2),
            'system_available_mb': round(system_available_mb, 2),
            'system_percent': system_percent
        }
    
    def get_detailed_state(self):
        try:
            from helpers.session_manager import session_manager
            active_sessions = len(session_manager.sessions) if hasattr(session_manager, 'sessions') else 0
        except:
            active_sessions = 0
        
        try:
            from queue_manager import download_queue
            queue_size = len(download_queue.waiting_queue) if hasattr(download_queue, 'waiting_queue') else 0
            active_downloads = len(download_queue.active_downloads) if hasattr(download_queue, 'active_downloads') else 0
        except:
            queue_size = 0
            active_downloads = 0
        
        try:
            try:
                from database_sqlite import db
            except ImportError:
                from database import db
            cached_items = len(db.cache.cache) if hasattr(db, 'cache') and hasattr(db.cache, 'cache') else 0
            ad_sessions = db.get_ad_sessions_count() if hasattr(db, 'get_ad_sessions_count') else 0
        except:
            cached_items = 0
            ad_sessions = 0
        
        return {
            'active_sessions': active_sessions,
            'queue_size': queue_size,
            'active_downloads': active_downloads,
            'cached_items': cached_items,
            'ad_sessions': ad_sessions,
            'thread_count': self.process.num_threads(),
            'open_files': len(self.process.open_files()) if hasattr(self.process, 'open_files') else 0
        }
    
    def log_memory_snapshot(self, operation="", context=""):
        mem = self.get_memory_info()
        state = self.get_detailed_state()
        
        # Store as tuple instead of dict to save ~60% RAM per entry
        # Each dict entry ~200 bytes, tuple ~80 bytes = saves 120 bytes * 20 entries = 2.4KB
        snapshot = (
            datetime.now().strftime("%H:%M:%S"),
            operation or '',
            round(mem['rss_mb'], 1),
            context or ''
        )
        
        self.operation_history.append(snapshot)
        
        log_msg = (
            f"📊 MEMORY SNAPSHOT | Operation: {operation or 'General'}\n"
            f"├─ RAM Usage: {mem['rss_mb']:.1f} MB (Virtual: {mem['vms_mb']:.1f} MB)\n"
            f"├─ System: {mem['system_percent']:.1f}% used ({mem['system_available_mb']:.1f} MB available)\n"
            f"├─ Sessions: {state['active_sessions']} | Queue: {state['queue_size']} | Active DLs: {state['active_downloads']}\n"
            f"├─ Cache: {state['cached_items']} items | Ad Sessions: {state['ad_sessions']}\n"
            f"├─ Threads: {state['thread_count']} | Open files: {state['open_files']}\n"
            f"└─ Context: {context or 'N/A'}"
        )
        
        # Check for critical memory (near crash on 512MB plan)
        if mem['rss_mb'] > 480:  # 93% of 512MB - crash imminent!
            critical_msg = (
                f"🚨 CRITICAL: CRASH IMMINENT! {mem['rss_mb']:.1f} MB / 512 MB\n"
                f"Sessions: {state['active_sessions']} | Queue: {state['queue_size']} | "
                f"Active DLs: {state['active_downloads']} | Cache: {state['cached_items']} | "
                f"Ad Sessions: {state['ad_sessions']}\n"
                f"Current Operation: {operation or 'Unknown'}\n"
                f"Context: {context or 'N/A'}\n"
                f"Last 5 operations before crash:"
            )
            self.logger.error(critical_msg)
            self._write_to_memory_log("🚨" * 40, force_write=True)
            self._write_to_memory_log(f"🚨 CRITICAL MEMORY - CRASH IMMINENT: {mem['rss_mb']:.1f} MB / 512 MB", force_write=True)
            self._write_to_memory_log(critical_msg, force_write=True)
            for idx, op in enumerate(list(self.operation_history)[-5:], 1):
                # Unpack tuple: (timestamp, operation, memory_mb, context)
                self._write_to_memory_log(f"  {idx}. [{op[0]}] {op[1]} - {op[2]:.1f} MB - {op[3]}", force_write=True)
            self._write_to_memory_log("🚨" * 40 + "\n", force_write=True)
        
        # Check for memory spike
        memory_increase = mem['rss_mb'] - self.last_memory_mb
        if memory_increase > self.spike_threshold_mb:
            self.logger.warning(f"⚠️ MEMORY SPIKE DETECTED: +{memory_increase:.1f} MB increase!")
            self.logger.warning(log_msg)
            self.log_recent_operations()
            
            # Write to dedicated memory log file (only if >= 400MB or forced)
            spike_reason = f"Memory spike caused by: {operation or 'Unknown operation'} - {context or 'No context'}"
            self._write_to_memory_log(f"⚠️ MEMORY SPIKE: +{memory_increase:.1f} MB")
            self._write_to_memory_log(spike_reason)
            self._write_to_memory_log(log_msg)
            self._write_to_memory_log("Recent operations:")
            for idx, op in enumerate(list(self.operation_history)[-10:], 1):
                # Unpack tuple: (timestamp, operation, memory_mb, context)
                self._write_to_memory_log(f"  {idx}. [{op[0]}] {op[1]} - {op[2]:.1f} MB - {op[3]}")
            self._write_to_memory_log("-" * 80 + "\n")
            
        elif mem['rss_mb'] > self.memory_threshold_mb:
            self.logger.warning(f"⚠️ HIGH MEMORY USAGE: {mem['rss_mb']:.1f} MB / 512 MB")
            self.logger.warning(log_msg)
            
            # Write to dedicated memory log file with reason
            high_mem_reason = f"High memory caused by: {operation or 'Unknown operation'} - {context or 'No context'}"
            self._write_to_memory_log(f"⚠️ HIGH MEMORY: {mem['rss_mb']:.1f} MB / 512 MB")
            self._write_to_memory_log(high_mem_reason)
            self._write_to_memory_log(log_msg)
            self._write_to_memory_log("-" * 80 + "\n")
            
        else:
            self.logger.info(log_msg)
        
        # Periodic snapshots - only written if memory >= 400MB
        if operation == "Periodic Check" and mem['rss_mb'] >= 400:
            self._write_to_memory_log(f"📊 Periodic Snapshot (High Memory): {mem['rss_mb']:.1f} MB")
            self._write_to_memory_log(f"   Sessions: {state['active_sessions']} | Queue: {state['queue_size']} | Active DLs: {state['active_downloads']} | Cache: {state['cached_items']} | Ad Sessions: {state['ad_sessions']}")
        
        self.last_memory_mb = mem['rss_mb']
        return mem
    
    def log_recent_operations(self):
        if not self.operation_history:
            return
        
        self.logger.info("📜 Recent operations (last 20):")
        for idx, op in enumerate(list(self.operation_history)[-20:], 1):
            # Unpack tuple: (timestamp, operation, memory_mb, context)
            self.logger.info(
                f"  {idx}. [{op[0]}] {op[1]} - "
                f"{op[2]:.1f} MB - {op[3]}"
            )
    
    async def log_operation(self, operation_name, func, *args, **kwargs):
        user_id = kwargs.get('user_id', 'unknown')
        context = kwargs.pop('memory_context', '')
        
        mem_before = self.get_memory_info()
        self.logger.info(f"🔵 START: {operation_name} | Memory: {mem_before['rss_mb']:.1f} MB | Context: {context}")
        
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            mem_after = self.get_memory_info()
            mem_diff = mem_after['rss_mb'] - mem_before['rss_mb']
            
            log_msg = (
                f"✅ COMPLETE: {operation_name}\n"
                f"├─ Memory Before: {mem_before['rss_mb']:.1f} MB\n"
                f"├─ Memory After: {mem_after['rss_mb']:.1f} MB\n"
                f"├─ Memory Change: {'+' if mem_diff >= 0 else ''}{mem_diff:.1f} MB\n"
                f"└─ Context: {context}"
            )
            
            if abs(mem_diff) > 10:
                self.logger.warning(f"⚠️ Significant memory change ({mem_diff:+.1f} MB):")
                self.logger.warning(log_msg)
                self.log_memory_snapshot(operation_name, f"After completion (changed {mem_diff:+.1f} MB)")
            else:
                self.logger.info(log_msg)
            
            return result
            
        except Exception as e:
            mem_error = self.get_memory_info()
            self.logger.error(
                f"❌ ERROR in {operation_name}: {str(e)}\n"
                f"Memory at error: {mem_error['rss_mb']:.1f} MB\n"
                f"Traceback: {traceback.format_exc()}"
            )
            raise
    
    def track_download(self, file_size_mb, user_id):
        context = f"User {user_id} | File size: {file_size_mb:.1f} MB"
        self.log_memory_snapshot("Download Started", context)
    
    def track_upload(self, file_size_mb, user_id):
        context = f"User {user_id} | File size: {file_size_mb:.1f} MB"
        self.log_memory_snapshot("Upload Started", context)
    
    def track_session_creation(self, user_id):
        context = f"User {user_id} creating new session"
        self.log_memory_snapshot("Session Creation", context)
    
    def track_session_cleanup(self, user_id):
        context = f"User {user_id} session cleanup"
        self.log_memory_snapshot("Session Cleanup", context)
    
    def get_memory_state_for_endpoint(self):
        """Get current memory state for /memory-debug endpoint and log it to file.
        Returns a dictionary with all memory metrics."""
        from datetime import datetime
        
        mem = self.get_memory_info()
        state = self.get_detailed_state()
        
        # Create detailed response
        response = {
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "memory": {
                "ram_usage_mb": mem['rss_mb'],
                "virtual_memory_mb": mem['vms_mb'],
                "system_total_mb": mem['system_total_mb'],
                "system_available_mb": mem['system_available_mb'],
                "system_percent_used": mem['system_percent']
            },
            "application_state": {
                "active_sessions": state['active_sessions'],
                "queue_size": state['queue_size'],
                "active_downloads": state['active_downloads'],
                "cached_items": state['cached_items'],
                "ad_sessions": state['ad_sessions'],
                "thread_count": state['thread_count'],
                "open_files": state['open_files']
            },
            "status": self._get_memory_status(mem['rss_mb']),
            "recent_operations": [
                {
                    "timestamp": op[0],
                    "operation": op[1],
                    "memory_mb": op[2],
                    "context": op[3]
                }
                for op in list(self.operation_history)[-10:]
            ]
        }
        
        # Log this access to memory debug file (forced write)
        log_msg = (
            f"📊 /memory-debug accessed\n"
            f"├─ RAM Usage: {mem['rss_mb']:.1f} MB (Virtual: {mem['vms_mb']:.1f} MB)\n"
            f"├─ System: {mem['system_percent']:.1f}% used ({mem['system_available_mb']:.1f} MB available)\n"
            f"├─ Sessions: {state['active_sessions']} | Queue: {state['queue_size']} | Active DLs: {state['active_downloads']}\n"
            f"├─ Cache: {state['cached_items']} items | Ad Sessions: {state['ad_sessions']}\n"
            f"├─ Threads: {state['thread_count']} | Open files: {state['open_files']}\n"
            f"└─ Status: {response['status']}"
        )
        
        self._write_to_memory_log(log_msg, force_write=True)
        self._write_to_memory_log("-" * 80, force_write=True)
        
        return response
    
    def _get_memory_status(self, rss_mb):
        """Get human-readable memory status"""
        if rss_mb > 480:
            return "🚨 CRITICAL - Crash Imminent!"
        elif rss_mb >= 400:
            return "⚠️ HIGH - Needs Attention"
        elif rss_mb >= 300:
            return "⚡ ELEVATED - Monitor Closely"
        elif rss_mb >= 200:
            return "✅ NORMAL - Healthy"
        else:
            return "✅ LOW - Excellent"
    
    async def periodic_monitor(self, interval=300):
        while True:
            try:
                await asyncio.sleep(interval)
                self.log_memory_snapshot("Periodic Check", f"Auto-check every {interval}s")
                
                # Force garbage collection if memory is high
                mem = self.get_memory_info()
                if mem['rss_mb'] > self.memory_threshold_mb:
                    self.logger.warning(f"⚠️ Memory above threshold, forcing garbage collection...")
                    self._write_to_memory_log(f"🗑️ Auto GC triggered at {mem['rss_mb']:.1f} MB")
                    
                    import gc
                    collected = gc.collect()
                    mem_after = self.get_memory_info()
                    freed = mem['rss_mb'] - mem_after['rss_mb']
                    
                    self.logger.info(
                        f"🗑️ GC collected {collected} objects. "
                        f"Memory: {mem['rss_mb']:.1f} MB → {mem_after['rss_mb']:.1f} MB "
                        f"(freed {freed:.1f} MB)"
                    )
                    self._write_to_memory_log(f"   Collected {collected} objects, freed {freed:.1f} MB → now {mem_after['rss_mb']:.1f} MB")
                    self._write_to_memory_log("-" * 80 + "\n")
            except Exception as e:
                self.logger.error(f"Error in periodic monitor: {e}")

memory_monitor = MemoryMonitor()
