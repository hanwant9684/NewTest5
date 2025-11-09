# Copyright (C) @Wolfy004
# Channel: https://t.me/Wolfy004

"""
In-memory cache to reduce database queries and improve response time
Especially important on Render's 512MB RAM limit
"""

import os
import time
from typing import Optional, Dict, Any
from collections import OrderedDict
from logger import LOGGER

class LRUCache:
    """Simple LRU cache with TTL (Time To Live) support"""
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 300):
        """
        Initialize cache
        
        Args:
            max_size: Maximum number of items to cache
            default_ttl: Default time-to-live in seconds (5 minutes)
        """
        self.cache: OrderedDict = OrderedDict()
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.hits = 0
        self.misses = 0
        LOGGER(__name__).info(f"Cache initialized: max_size={max_size}, ttl={default_ttl}s")
    
    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        """Check if cache entry is expired"""
        return time.time() > entry['expires_at']
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        if key in self.cache:
            entry = self.cache[key]
            
            # Check if expired
            if self._is_expired(entry):
                del self.cache[key]
                self.misses += 1
                return None
            
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            self.hits += 1
            return entry['value']
        
        self.misses += 1
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in cache with optional custom TTL"""
        if ttl is None:
            ttl = self.default_ttl
        
        # Remove oldest if at capacity
        if len(self.cache) >= self.max_size and key not in self.cache:
            self.cache.popitem(last=False)
        
        self.cache[key] = {
            'value': value,
            'expires_at': time.time() + ttl
        }
        self.cache.move_to_end(key)
    
    def delete(self, key: str):
        """Remove specific key from cache"""
        if key in self.cache:
            del self.cache[key]
    
    def clear_pattern(self, pattern: str):
        """Clear all keys matching pattern (e.g., 'user_123_*')"""
        keys_to_delete = [k for k in self.cache.keys() if pattern in k]
        for key in keys_to_delete:
            del self.cache[key]
    
    def clear(self):
        """Clear entire cache"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': f"{hit_rate:.1f}%"
        }


# Global cache instance
# Using smaller cache for Render's 512MB RAM
# Detect constrained environments (Render, Replit) and reduce cache size
IS_CONSTRAINED = bool(
    os.getenv('RENDER') or 
    os.getenv('RENDER_EXTERNAL_URL') or 
    os.getenv('REPLIT_DEPLOYMENT') or 
    os.getenv('REPL_ID')
)

# ULTRA-aggressive cache reduction for Render's 512MB RAM
# Each cache entry can be 1-10KB, so 50 items = ~50-500KB max
CACHE_SIZE = 50 if IS_CONSTRAINED else 500
_cache = LRUCache(max_size=CACHE_SIZE, default_ttl=120)  # Shorter TTL (2 min) to free memory faster


def get_cache() -> LRUCache:
    """Get global cache instance"""
    return _cache
