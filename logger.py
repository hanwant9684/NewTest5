import logging
import os
import glob
from logging.handlers import RotatingFileHandler

# Optimized for Render free plan (512MB RAM constraint)
# Total log storage: ~3MB max (1MB current + 1MB x 2 backups)
# Old logs automatically deleted when rotation happens

def cleanup_old_logs():
    """Clean up old backup logs beyond the backupCount to save storage"""
    try:
        # Remove backup files beyond logs.txt.1 and logs.txt.2
        old_backups = glob.glob("logs.txt.*")
        for backup in old_backups:
            try:
                # Extract the number from logs.txt.N
                backup_num = int(backup.split(".")[-1])
                if backup_num > 2:  # Keep only .1 and .2
                    os.remove(backup)
            except (ValueError, OSError):
                pass  # Skip files that don't match the pattern
    except Exception:
        pass  # Fail silently if cleanup fails

# Clean up old logs on startup
cleanup_old_logs()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(levelname)s] - %(funcName)s() - Line %(lineno)d: %(name)s - %(message)s",
    datefmt="%d-%b-%y %I:%M:%S %p",
    handlers=[
        RotatingFileHandler(
            "logs.txt",
            mode="a",  # Append mode instead of w+ to preserve logs across restarts
            maxBytes=1000000,  # 1MB per file (reduced from 5MB)
            backupCount=2,  # Keep only 2 backup files (reduced from 10)
        ),
        logging.StreamHandler(),
    ],
)

logging.getLogger("telethon").setLevel(logging.ERROR)


def LOGGER(name: str) -> logging.Logger:
    return logging.getLogger(name)
