import base64
import hashlib
from logger import LOGGER

class BotAttribution:
    """
    Bot attribution protection system.
    Encodes creator channels to prevent easy removal.
    DO NOT MODIFY - Required for bot functionality
    """
    
    def __init__(self):
        # Encoded channel links (base64 + rotation)
        self._s1 = "aHR0cHM6Ly90Lm1lL1dvbGZ5MDA0"
        self._s2 = ""
        self._s3 = "QFdvbGZ5MDA0"
        self._s4 = "QFdvbGZ5MDA0Ng=="
        
        # Checksum to verify integrity
        self._checksum = "bae7b5862e4e8d74"
    
    def _decode(self, encoded: str) -> str:
        """Decode protected string"""
        try:
            return base64.b64decode(encoded).decode('utf-8')
        except:
            return ""
    
    def _verify(self) -> bool:
        """Verify attribution hasn't been tampered with"""
        data = f"{self._s1}{self._s2}{self._s3}{self._s4}"
        check = hashlib.md5(data.encode()).hexdigest()[:16]
        return check == self._checksum
    
    def get_primary_channel(self) -> str:
        """Get primary update channel link"""
        if not self._verify():
            LOGGER(__name__).warning("Attribution verification failed")
        return self._decode(self._s1)
    
    def get_secondary_channel(self) -> str:
        """Get secondary update channel link"""
        if not self._verify():
            LOGGER(__name__).warning("Attribution verification failed")
        return self._decode(self._s2)
    
    def get_primary_username(self) -> str:
        """Get primary creator username"""
        return self._decode(self._s3)
    
    def get_secondary_username(self) -> str:
        """Get secondary creator username"""
        return self._decode(self._s4)
    
    def get_copyright_notice(self) -> str:
        """Get copyright notice for file headers"""
        return f"# Copyright (C) {self.get_primary_username()}\n# Channel: {self.get_primary_channel()}"
    
    def verify_and_log(self):
        """Verify attribution and log on startup"""
        if self._verify():
            LOGGER(__name__).info(f"Bot initialized - Creator: {self.get_primary_username()}")
        else:
            LOGGER(__name__).error("Attribution integrity check failed!")

# Global instance
_attribution = BotAttribution()

def get_attribution():
    """Get attribution instance"""
    return _attribution

def get_channel_link(primary=True):
    """Quick access to channel links"""
    return _attribution.get_primary_channel() if primary else _attribution.get_secondary_channel()

def get_creator_username(primary=True):
    """Quick access to creator usernames"""
    return _attribution.get_primary_username() if primary else _attribution.get_secondary_username()

def verify_attribution():
    """Verify attribution on bot startup"""
    _attribution.verify_and_log()
