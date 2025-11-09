# Copyright (C) @Wolfy004
# Channel: https://t.me/Wolfy004
# SQLite-based database (replaces MongoDB for ~50-95MB RAM savings)

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from logger import LOGGER
from cache import get_cache
from threading import Lock

class DatabaseManager:
    def __init__(self, db_path: Optional[str] = None):
        if not db_path:
            db_path = os.getenv("DATABASE_PATH", "telegram_bot.db")
        
        self.db_path = db_path
        self.cache = get_cache()
        self.lock = Lock()
        
        try:
            self._init_database()
            LOGGER(__name__).info(f"Successfully connected to SQLite database: {db_path}")
        except Exception as e:
            LOGGER(__name__).error(f"SQLite initialization error: {e}")
            raise

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_database(self):
        with self.lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    user_type TEXT DEFAULT 'free',
                    subscription_end TEXT,
                    premium_source TEXT,
                    joined_date TEXT NOT NULL,
                    last_activity TEXT NOT NULL,
                    is_banned INTEGER DEFAULT 0,
                    session_string TEXT,
                    custom_thumbnail TEXT,
                    ad_downloads INTEGER DEFAULT 0,
                    ad_downloads_reset_date TEXT,
                    shortener_index INTEGER DEFAULT 0
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    files_downloaded INTEGER DEFAULT 0,
                    UNIQUE(user_id, date)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER PRIMARY KEY,
                    added_by INTEGER NOT NULL,
                    added_date TEXT NOT NULL
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS broadcasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT NOT NULL,
                    sent_by INTEGER NOT NULL,
                    sent_date TEXT NOT NULL,
                    total_users INTEGER NOT NULL,
                    successful_sends INTEGER NOT NULL
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ad_sessions (
                    session_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    used INTEGER DEFAULT 0
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ad_verifications (
                    code TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS legal_acceptance (
                    user_id INTEGER PRIMARY KEY,
                    accepted_terms INTEGER DEFAULT 0,
                    accepted_privacy INTEGER DEFAULT 0,
                    acceptance_date TEXT NOT NULL,
                    ip_address TEXT,
                    version TEXT DEFAULT '1.0'
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_usage_user_date ON daily_usage(user_id, date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ad_sessions_created ON ad_sessions(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ad_verifications_created ON ad_verifications(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_legal_acceptance_date ON legal_acceptance(acceptance_date)')
            
            conn.commit()
            conn.close()
            
            LOGGER(__name__).info("Database tables and indexes created successfully")

    def add_user(self, user_id: int, username: Optional[str] = None, first_name: Optional[str] = None,
                 last_name: Optional[str] = None, user_type: str = 'free') -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                now = datetime.now().isoformat()
                
                cursor.execute('SELECT 1 FROM users WHERE user_id = ?', (user_id,))
                exists = cursor.fetchone()
                
                if not exists:
                    cursor.execute('''
                        INSERT INTO users (user_id, username, first_name, last_name, user_type, joined_date, last_activity, ad_downloads_reset_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (user_id, username, first_name, last_name, user_type, now, now, datetime.now().strftime('%Y-%m-%d')))
                else:
                    updates = ['last_activity = ?']
                    params = [now]
                    if username:
                        updates.append('username = ?')
                        params.append(username)
                    if first_name:
                        updates.append('first_name = ?')
                        params.append(first_name)
                    if last_name:
                        updates.append('last_name = ?')
                        params.append(last_name)
                    params.append(user_id)
                    
                    cursor.execute(f'UPDATE users SET {", ".join(updates)} WHERE user_id = ?', params)
                
                conn.commit()
                conn.close()
            
            if not exists:
                try:
                    from cloud_backup import trigger_backup_on_critical_change
                    trigger_backup_on_critical_change("add_user", user_id)
                except Exception as e:
                    LOGGER(__name__).warning(f"Backup trigger failed for add_user: {e}")
            
            return True
        except Exception as e:
            LOGGER(__name__).error(f"Error adding user {user_id}: {e}")
            return False

    def get_user(self, user_id: int) -> Optional[Dict]:
        cache_key = f"user_{user_id}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                user = dict(row)
                user['is_banned'] = bool(user['is_banned'])
                self.cache.set(cache_key, user, ttl=180)
                return user
            return None
        except Exception as e:
            LOGGER(__name__).error(f"Error getting user {user_id}: {e}")
            return None

    def get_user_type(self, user_id: int) -> str:
        user = self.get_user(user_id)
        if not user:
            return 'free'

        if self.is_admin(user_id):
            return 'admin'

        if user.get('user_type') == 'paid' and user.get('subscription_end'):
            try:
                sub_end = datetime.fromisoformat(user['subscription_end'])
            except:
                try:
                    sub_end = datetime.strptime(user['subscription_end'], '%Y-%m-%d')
                except:
                    return 'free'
            
            if sub_end > datetime.now():
                return 'paid'
            else:
                with self.lock:
                    conn = self._get_connection()
                    cursor = conn.cursor()
                    cursor.execute('UPDATE users SET user_type = ?, subscription_end = NULL, premium_source = NULL WHERE user_id = ?', 
                                   ('free', user_id))
                    conn.commit()
                    conn.close()
                LOGGER(__name__).info(f"User {user_id} premium expired, downgraded to free")

        return 'free'

    def is_admin(self, user_id: int) -> bool:
        cache_key = f"admin_{user_id}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM admins WHERE user_id = ?', (user_id,))
            is_admin = cursor.fetchone() is not None
            conn.close()
            self.cache.set(cache_key, is_admin, ttl=300)
            return is_admin
        except Exception as e:
            LOGGER(__name__).error(f"Error checking admin status for {user_id}: {e}")
            return False

    def add_admin(self, user_id: int, added_by: int) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('INSERT OR REPLACE INTO admins (user_id, added_by, added_date) VALUES (?, ?, ?)',
                               (user_id, added_by, datetime.now().isoformat()))
                conn.commit()
                conn.close()
            self.cache.delete(f"admin_{user_id}")
            self.cache.delete(f"user_{user_id}")
            return True
        except Exception as e:
            LOGGER(__name__).error(f"Error adding admin {user_id}: {e}")
            return False

    def remove_admin(self, user_id: int) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('DELETE FROM admins WHERE user_id = ?', (user_id,))
                deleted = cursor.rowcount > 0
                conn.commit()
                conn.close()
            self.cache.delete(f"admin_{user_id}")
            self.cache.delete(f"user_{user_id}")
            return deleted
        except Exception as e:
            LOGGER(__name__).error(f"Error removing admin {user_id}: {e}")
            return False

    def set_user_type(self, user_id: int, user_type: str, days: int = 30) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                if user_type == 'paid':
                    subscription_end = (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d')
                    cursor.execute('UPDATE users SET user_type = ?, subscription_end = ?, premium_source = ? WHERE user_id = ?',
                                   (user_type, subscription_end, 'paid', user_id))
                else:
                    cursor.execute('UPDATE users SET user_type = ?, subscription_end = NULL, premium_source = NULL WHERE user_id = ?',
                                   (user_type, user_id))
                success = cursor.rowcount > 0
                conn.commit()
                conn.close()
            return success
        except Exception as e:
            LOGGER(__name__).error(f"Error setting user type for {user_id}: {e}")
            return False

    def set_premium(self, user_id: int, expiry_datetime: str, source: str = "ads") -> bool:
        try:
            user = self.get_user(user_id)
            
            if user and user.get('user_type') == 'paid':
                existing_end = user.get('subscription_end')
                if existing_end:
                    try:
                        existing_expiry = datetime.fromisoformat(existing_end)
                    except:
                        try:
                            existing_expiry = datetime.strptime(existing_end, '%Y-%m-%d')
                        except:
                            existing_expiry = datetime.now()
                    
                    if existing_expiry > datetime.now():
                        existing_source = user.get('premium_source')
                        if source == 'ads' and existing_source != 'ads':
                            LOGGER(__name__).warning(
                                f"User {user_id} has active premium until {existing_end}. Skipping ad-based premium.")
                            return False
            
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET user_type = ?, subscription_end = ?, premium_source = ? WHERE user_id = ?',
                               ('paid', expiry_datetime, source, user_id))
                success = cursor.rowcount > 0
                conn.commit()
                conn.close()
            
            if success:
                try:
                    from cloud_backup import trigger_backup_on_critical_change
                    trigger_backup_on_critical_change("set_premium", user_id)
                except Exception as e:
                    LOGGER(__name__).warning(f"Backup trigger failed for set_premium: {e}")
            
            return success
        except Exception as e:
            LOGGER(__name__).error(f"Error setting premium for {user_id}: {e}")
            return False

    def get_daily_usage(self, user_id: int, date: Optional[str] = None) -> int:
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT files_downloaded FROM daily_usage WHERE user_id = ? AND date = ?', (user_id, date))
            row = cursor.fetchone()
            conn.close()
            return row['files_downloaded'] if row else 0
        except Exception as e:
            LOGGER(__name__).error(f"Error getting daily usage for {user_id}: {e}")
            return 0

    def increment_usage(self, user_id: int, count: int = 1) -> bool:
        try:
            user_type = self.get_user_type(user_id)
            
            if user_type in ['admin', 'paid']:
                for _ in range(count):
                    self.increment_shortener_rotation()
                return True
            
            self.reset_ad_downloads_if_needed(user_id)
            
            user = self.get_user(user_id)
            ad_downloads = user.get('ad_downloads', 0) if user else 0
            
            if ad_downloads > 0:
                if count > ad_downloads:
                    LOGGER(__name__).warning(f"User {user_id} has only {ad_downloads} ad downloads but needs {count}")
                    return False
                
                with self.lock:
                    conn = self._get_connection()
                    cursor = conn.cursor()
                    cursor.execute('UPDATE users SET ad_downloads = ad_downloads - ? WHERE user_id = ? AND ad_downloads >= ?',
                                   (count, user_id, count))
                    success = cursor.rowcount > 0
                    conn.commit()
                    conn.close()
                
                if success:
                    LOGGER(__name__).info(f"User {user_id} used {count} ad download(s), {ad_downloads - count} remaining")
                    self.cache.delete(f"user_{user_id}")
                    for _ in range(count):
                        self.increment_shortener_rotation()
                    return True
                else:
                    LOGGER(__name__).error(f"Failed to deduct {count} ad downloads for user {user_id}")
                    return False
            
            daily_usage = self.get_daily_usage(user_id)
            if daily_usage + count > 1:
                LOGGER(__name__).warning(f"User {user_id} tried to exceed daily limit: {daily_usage} + {count} > 1")
                return False
            
            date = datetime.now().strftime('%Y-%m-%d')
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('INSERT OR IGNORE INTO daily_usage (user_id, date, files_downloaded) VALUES (?, ?, 0)', (user_id, date))
                cursor.execute('UPDATE daily_usage SET files_downloaded = files_downloaded + ? WHERE user_id = ? AND date = ?',
                               (count, user_id, date))
                conn.commit()
                conn.close()
            
            for _ in range(count):
                self.increment_shortener_rotation()
            
            return True
        except Exception as e:
            LOGGER(__name__).error(f"Error incrementing usage for {user_id}: {e}")
            return False

    def can_download(self, user_id: int, count: int = 1) -> tuple[bool, str]:
        user_type = self.get_user_type(user_id)

        if user_type in ['admin', 'paid']:
            return True, ""

        self.reset_ad_downloads_if_needed(user_id)

        user = self.get_user(user_id)
        ad_downloads = user.get('ad_downloads', 0) if user else 0
        
        if ad_downloads > 0:
            if ad_downloads < count:
                quota_message = f"❌ **Insufficient ad downloads**\n\n📊 You have {ad_downloads} ad download(s) but need {count} for this media group."
                return False, quota_message
            return True, ""

        daily_usage = self.get_daily_usage(user_id)
        if daily_usage + count > 1:
            quota_message = f"📊 **Daily limit reached**"
            return False, quota_message

        return True, ""

    def get_all_users(self) -> List[int]:
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT user_id FROM users WHERE is_banned = 0')
            users = [row['user_id'] for row in cursor.fetchall()]
            conn.close()
            return users
        except Exception as e:
            LOGGER(__name__).error(f"Error getting all users: {e}")
            return []

    def save_broadcast(self, message: str, sent_by: int, total_users: int, successful_sends: int) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('INSERT INTO broadcasts (message, sent_by, sent_date, total_users, successful_sends) VALUES (?, ?, ?, ?, ?)',
                               (message, sent_by, datetime.now().isoformat(), total_users, successful_sends))
                conn.commit()
                conn.close()
            return True
        except Exception as e:
            LOGGER(__name__).error(f"Error saving broadcast: {e}")
            return False

    def ban_user(self, user_id: int) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
                success = cursor.rowcount > 0
                conn.commit()
                conn.close()
            self.cache.delete(f"banned_{user_id}")
            self.cache.delete(f"user_{user_id}")
            return success
        except Exception as e:
            LOGGER(__name__).error(f"Error banning user {user_id}: {e}")
            return False

    def unban_user(self, user_id: int) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET is_banned = 0 WHERE user_id = ?', (user_id,))
                success = cursor.rowcount > 0
                conn.commit()
                conn.close()
            self.cache.delete(f"banned_{user_id}")
            self.cache.delete(f"user_{user_id}")
            return success
        except Exception as e:
            LOGGER(__name__).error(f"Error unbanning user {user_id}: {e}")
            return False

    def is_banned(self, user_id: int) -> bool:
        cache_key = f"banned_{user_id}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached
        
        user = self.get_user(user_id)
        is_banned = bool(user and user.get('is_banned', False))
        self.cache.set(cache_key, is_banned, ttl=300)
        return is_banned

    def set_user_session(self, user_id: int, session_string: Optional[str] = None) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                cursor.execute('SELECT session_string FROM users WHERE user_id = ?', (user_id,))
                row = cursor.fetchone()
                had_session = bool(row and row['session_string'])
                
                cursor.execute('UPDATE users SET session_string = ? WHERE user_id = ?', (session_string, user_id))
                success = cursor.rowcount > 0
                conn.commit()
                conn.close()
            
            self.cache.delete(f"user_{user_id}")
            
            if success and session_string and not had_session:
                try:
                    from cloud_backup import trigger_backup_on_session
                    trigger_backup_on_session(user_id)
                except Exception as e:
                    LOGGER(__name__).warning(f"Backup trigger failed for user {user_id}: {e}")
            
            return success
        except Exception as e:
            LOGGER(__name__).error(f"Error setting session for {user_id}: {e}")
            return False

    def get_user_session(self, user_id: int) -> Optional[str]:
        user = self.get_user(user_id)
        return user.get('session_string') if user else None

    def get_stats(self) -> Dict:
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) as count FROM users')
            total_users = cursor.fetchone()['count']
            
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            cursor.execute('SELECT COUNT(*) as count FROM users WHERE last_activity > ?', (week_ago,))
            active_users = cursor.fetchone()['count']
            
            now = datetime.now().strftime('%Y-%m-%d')
            cursor.execute('SELECT COUNT(*) as count FROM users WHERE user_type = ? AND subscription_end > ?', ('paid', now))
            paid_users = cursor.fetchone()['count']
            
            cursor.execute('SELECT COUNT(*) as count FROM admins')
            admin_count = cursor.fetchone()['count']
            
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute('SELECT SUM(files_downloaded) as total FROM daily_usage WHERE date = ?', (today,))
            result = cursor.fetchone()
            today_downloads = result['total'] if result['total'] else 0
            
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            cursor.execute('SELECT COUNT(*) as count FROM users WHERE joined_date >= ?', (today_start,))
            today_new_users = cursor.fetchone()['count']
            
            conn.close()
            
            return {
                'total_users': total_users,
                'active_users': active_users,
                'paid_users': paid_users,
                'admin_count': admin_count,
                'today_downloads': today_downloads,
                'today_new_users': today_new_users
            }
        except Exception as e:
            LOGGER(__name__).error(f"Error getting stats: {e}")
            return {}
    
    def set_custom_thumbnail(self, user_id: int, file_id: str) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET custom_thumbnail = ? WHERE user_id = ?', (file_id, user_id))
                success = cursor.rowcount > 0
                conn.commit()
                conn.close()
            return success
        except Exception as e:
            LOGGER(__name__).error(f"Error setting custom thumbnail for {user_id}: {e}")
            return False

    def get_custom_thumbnail(self, user_id: int) -> Optional[str]:
        user = self.get_user(user_id)
        return user.get('custom_thumbnail') if user else None

    def delete_custom_thumbnail(self, user_id: int) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET custom_thumbnail = NULL WHERE user_id = ?', (user_id,))
                success = cursor.rowcount > 0
                conn.commit()
                conn.close()
            return success
        except Exception as e:
            LOGGER(__name__).error(f"Error deleting custom thumbnail for {user_id}: {e}")
            return False

    def add_ad_downloads(self, user_id: int, count: int) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET ad_downloads = ad_downloads + ? WHERE user_id = ?', (count, user_id))
                success = cursor.rowcount > 0
                conn.commit()
                conn.close()
            self.cache.delete(f"user_{user_id}")
            
            if success:
                try:
                    from cloud_backup import trigger_backup_on_critical_change
                    trigger_backup_on_critical_change("add_ad_downloads", user_id)
                except Exception as e:
                    LOGGER(__name__).warning(f"Backup trigger failed for add_ad_downloads: {e}")
            
            return success
        except Exception as e:
            LOGGER(__name__).error(f"Error adding ad downloads for {user_id}: {e}")
            return False

    def reset_ad_downloads_if_needed(self, user_id: int):
        user = self.get_user(user_id)
        if not user:
            return
        
        reset_date = user.get('ad_downloads_reset_date')
        today = datetime.now().strftime('%Y-%m-%d')
        
        if reset_date != today:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET ad_downloads = 0, ad_downloads_reset_date = ? WHERE user_id = ?', (today, user_id))
                conn.commit()
                conn.close()
            self.cache.delete(f"user_{user_id}")

    def create_ad_session(self, session_id: str, user_id: int) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('INSERT INTO ad_sessions (session_id, user_id, created_at) VALUES (?, ?, ?)',
                               (session_id, user_id, datetime.now().isoformat()))
                conn.commit()
                conn.close()
            return True
        except Exception as e:
            LOGGER(__name__).error(f"Error creating ad session: {e}")
            return False

    def get_ad_session(self, session_id: str) -> Optional[Dict]:
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM ad_sessions WHERE session_id = ?', (session_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                session = dict(row)
                session['created_at'] = datetime.fromisoformat(session['created_at'])
                session['used'] = bool(session['used'])
                return session
            return None
        except Exception as e:
            LOGGER(__name__).error(f"Error getting ad session: {e}")
            return None

    def mark_ad_session_used(self, session_id: str) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE ad_sessions SET used = 1 WHERE session_id = ? AND used = 0', (session_id,))
                success = cursor.rowcount > 0
                conn.commit()
                conn.close()
            return success
        except Exception as e:
            LOGGER(__name__).error(f"Error marking ad session used: {e}")
            return False

    def delete_ad_session(self, session_id: str) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('DELETE FROM ad_sessions WHERE session_id = ?', (session_id,))
                conn.commit()
                conn.close()
            return True
        except Exception as e:
            LOGGER(__name__).error(f"Error deleting ad session: {e}")
            return False

    def create_verification_code(self, code: str, user_id: int) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('INSERT INTO ad_verifications (code, user_id, created_at) VALUES (?, ?, ?)',
                               (code, user_id, datetime.now().isoformat()))
                conn.commit()
                conn.close()
            return True
        except Exception as e:
            LOGGER(__name__).error(f"Error creating verification code: {e}")
            return False

    def get_verification_code(self, code: str) -> Optional[Dict]:
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM ad_verifications WHERE code = ?', (code,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                verification = dict(row)
                verification['created_at'] = datetime.fromisoformat(verification['created_at'])
                return verification
            return None
        except Exception as e:
            LOGGER(__name__).error(f"Error getting verification code: {e}")
            return None

    def delete_verification_code(self, code: str) -> bool:
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('DELETE FROM ad_verifications WHERE code = ?', (code,))
                conn.commit()
                conn.close()
            return True
        except Exception as e:
            LOGGER(__name__).error(f"Error deleting verification code: {e}")
            return False

    def increment_shortener_rotation(self) -> bool:
        return True

    def get_user_shortener_index(self, user_id: int) -> int:
        user = self.get_user(user_id)
        return user.get('shortener_index', 0) if user else 0

    def rotate_user_shortener(self, user_id: int) -> bool:
        try:
            current_index = self.get_user_shortener_index(user_id)
            next_index = (current_index + 1) % 4
            
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET shortener_index = ? WHERE user_id = ?', (next_index, user_id))
                success = cursor.rowcount > 0
                conn.commit()
                conn.close()
            self.cache.delete(f"user_{user_id}")
            return success
        except Exception as e:
            LOGGER(__name__).error(f"Error rotating shortener for {user_id}: {e}")
            return False

    def cleanup_expired_sessions(self) -> Dict[str, int]:
        """Clean up expired ad sessions and verification codes (older than 60 minutes).
        Also invalidates any cached session data.
        Returns counts of deleted items."""
        try:
            cutoff_time = (datetime.now() - timedelta(minutes=60)).isoformat()
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Get session IDs before deleting to clear cache
                cursor.execute('SELECT session_id, user_id FROM ad_sessions WHERE created_at < ?', (cutoff_time,))
                expired_sessions = cursor.fetchall()
                
                # Delete expired ad sessions
                cursor.execute('DELETE FROM ad_sessions WHERE created_at < ?', (cutoff_time,))
                deleted_sessions = cursor.rowcount
                
                # Delete expired verification codes
                cursor.execute('DELETE FROM ad_verifications WHERE created_at < ?', (cutoff_time,))
                deleted_verifications = cursor.rowcount
                
                conn.commit()
                conn.close()
            
            # Clear cache entries for affected users
            for session in expired_sessions:
                user_id = session['user_id']
                self.cache.delete(f"user_{user_id}")
            
            if deleted_sessions > 0 or deleted_verifications > 0:
                LOGGER(__name__).info(
                    f"Cleaned up {deleted_sessions} expired ad sessions and "
                    f"{deleted_verifications} verification codes"
                )
            
            return {
                'sessions': deleted_sessions,
                'verifications': deleted_verifications
            }
        except Exception as e:
            LOGGER(__name__).error(f"Error cleaning up expired sessions: {e}")
            return {'sessions': 0, 'verifications': 0}

    def get_ad_downloads(self, user_id: int) -> int:
        """Get the number of ad downloads remaining for a user"""
        try:
            self.reset_ad_downloads_if_needed(user_id)
            user = self.get_user(user_id)
            return user.get('ad_downloads', 0) if user else 0
        except Exception as e:
            LOGGER(__name__).error(f"Error getting ad downloads for {user_id}: {e}")
            return 0

    def get_premium_users(self) -> List[Dict]:
        """Get list of all active premium users"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            now = datetime.now().strftime('%Y-%m-%d')
            cursor.execute('''
                SELECT user_id, username, subscription_end as premium_expiry 
                FROM users 
                WHERE user_type = ? AND subscription_end > ?
                ORDER BY subscription_end DESC
            ''', ('paid', now))
            users = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return users
        except Exception as e:
            LOGGER(__name__).error(f"Error getting premium users: {e}")
            return []
    
    def get_ad_sessions_count(self) -> int:
        """Get count of active ad sessions (for memory monitoring)"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) as count FROM ad_sessions')
            count = cursor.fetchone()['count']
            conn.close()
            return count
        except Exception as e:
            LOGGER(__name__).error(f"Error getting ad sessions count: {e}")
            return 0
    
    def check_legal_acceptance(self, user_id: int) -> bool:
        """Check if user has accepted both Terms & Conditions and Privacy Policy"""
        cache_key = f"legal_{user_id}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                'SELECT accepted_terms, accepted_privacy FROM legal_acceptance WHERE user_id = ?',
                (user_id,)
            )
            row = cursor.fetchone()
            conn.close()
            
            if row and row['accepted_terms'] and row['accepted_privacy']:
                self.cache.set(cache_key, True)
                return True
            
            self.cache.set(cache_key, False)
            return False
        except Exception as e:
            LOGGER(__name__).error(f"Error checking legal acceptance for {user_id}: {e}")
            return False
    
    def record_legal_acceptance(self, user_id: int, ip_address: Optional[str] = None) -> bool:
        """Record that user has accepted Terms & Conditions and Privacy Policy"""
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                now = datetime.now().isoformat()
                
                cursor.execute(
                    '''INSERT OR REPLACE INTO legal_acceptance 
                       (user_id, accepted_terms, accepted_privacy, acceptance_date, ip_address, version)
                       VALUES (?, 1, 1, ?, ?, '1.0')''',
                    (user_id, now, ip_address)
                )
                
                conn.commit()
                conn.close()
            
            self.cache.delete(f"legal_{user_id}")
            LOGGER(__name__).info(f"Legal acceptance recorded for user {user_id}")
            
            try:
                from cloud_backup import trigger_backup_on_critical_change
                trigger_backup_on_critical_change("legal_acceptance", user_id)
            except Exception as e:
                LOGGER(__name__).warning(f"Backup trigger failed for legal_acceptance: {e}")
            
            return True
        except Exception as e:
            LOGGER(__name__).error(f"Error recording legal acceptance for {user_id}: {e}")
            return False
    
    def get_legal_acceptance_stats(self) -> Dict:
        """Get statistics about legal acceptance"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) as total FROM users')
            total_users = cursor.fetchone()['total']
            
            cursor.execute('SELECT COUNT(*) as accepted FROM legal_acceptance WHERE accepted_terms = 1 AND accepted_privacy = 1')
            accepted_users = cursor.fetchone()['accepted']
            
            conn.close()
            
            return {
                'total_users': total_users,
                'accepted_users': accepted_users,
                'pending_users': total_users - accepted_users
            }
        except Exception as e:
            LOGGER(__name__).error(f"Error getting legal acceptance stats: {e}")
            return {'total_users': 0, 'accepted_users': 0, 'pending_users': 0}

db = DatabaseManager()
