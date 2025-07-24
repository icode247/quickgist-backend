# integrations/session_manager.py - Updated with Better Integration
import pickle
import os
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import logging
import time
from typing import Dict, Any, Optional
import hashlib

logger = logging.getLogger(__name__)

class SocialSessionManager:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.sessions_dir = "sessions"
        os.makedirs(self.sessions_dir, exist_ok=True)
        
        # Session expiry times (in hours)
        self.session_expiry = {
            'twitter': 24,      # Twitter sessions last ~24 hours
            'linkedin': 48,     # LinkedIn sessions can last longer
            'facebook': 24,     # Facebook sessions
            'instagram': 12     # Instagram sessions shorter due to restrictions
        }

    def test_connection_with_session(self, user_id: str, platform: str, 
                                   username: str, password: str) -> Dict[str, Any]:
        """Test connection and save session for future use"""
        driver = None
        try:
            logger.info(f"Testing {platform} connection for user {user_id}")
            driver = self._setup_driver()
            
            # Try to restore existing session first
            session_data = self.load_session(user_id, platform)
            if session_data:
                restored = self.restore_session(driver, session_data)
                if restored and self._verify_platform_login(driver, platform):
                    logger.info(f"✅ Existing session valid for {platform}")
                    return {'success': True, 'message': 'Existing session is valid'}
            
            # If no session or restoration failed, do fresh login
            logger.info(f"Performing fresh login for {platform}")
            login_result = self._perform_platform_login(driver, platform, username, password)
            
            if login_result['success']:
                # Save session for future use
                session_saved = self.save_session(user_id, platform, driver)
                if session_saved:
                    logger.info(f"✅ Session saved for {platform}")
                    return {'success': True, 'message': 'Login successful, session saved'}
                else:
                    return {'success': True, 'message': 'Login successful but session save failed'}
            else:
                return login_result
                
        except Exception as e:
            logger.error(f"Error testing {platform} connection: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            if driver:
                driver.quit()

    def save_session(self, user_id: str, platform: str, driver: webdriver.Chrome) -> bool:
        """Save browser session for reuse"""
        try:
            session_id = self._generate_session_id(user_id, platform)
            session_file = os.path.join(self.sessions_dir, f"{session_id}.pkl")
            
            # Collect session data
            session_data = {
                'cookies': driver.get_cookies(),
                'current_url': driver.current_url,
                'user_agent': driver.execute_script("return navigator.userAgent;"),
                'saved_at': datetime.now().isoformat(),
                'platform': platform,
                'user_id': user_id
            }
            
            # Save to file
            with open(session_file, 'wb') as f:
                pickle.dump(session_data, f)
            
            # Store session metadata in Redis
            session_key = f"session:{user_id}:{platform}"
            session_metadata = {
                'session_file': session_file,
                'session_id': session_id,
                'saved_at': session_data['saved_at'],
                'platform': platform,
                'user_id': user_id,
                'status': 'active',
                'expires_at': (datetime.now() + timedelta(hours=self.session_expiry.get(platform, 24))).isoformat()
            }
            
            self.redis.hset(session_key, mapping=session_metadata)
            
            # Set expiry on Redis key
            expiry_hours = self.session_expiry.get(platform, 24)
            self.redis.expire(session_key, expiry_hours * 3600)
            
            logger.info(f"Session saved for {user_id} on {platform} (expires in {expiry_hours}h)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save session: {e}")
            return False

    def load_session(self, user_id: str, platform: str) -> Optional[Dict]:
        """Load existing browser session"""
        try:
            session_key = f"session:{user_id}:{platform}"
            session_metadata = self.redis.hgetall(session_key)
            
            if not session_metadata:
                logger.debug(f"No session metadata found for {user_id}:{platform}")
                return None
            
            # Check if session has expired
            expires_at_str = session_metadata.get('expires_at')
            if expires_at_str:
                expires_at = datetime.fromisoformat(expires_at_str)
                if datetime.now() > expires_at:
                    logger.info(f"Session expired for {user_id}:{platform}")
                    self.invalidate_session(user_id, platform)
                    return None
            
            # Load session data from file
            session_file = session_metadata.get('session_file')
            if not session_file or not os.path.exists(session_file):
                logger.warning(f"Session file not found: {session_file}")
                self.invalidate_session(user_id, platform)
                return None
            
            with open(session_file, 'rb') as f:
                session_data = pickle.load(f)
            
            logger.debug(f"Loaded session for {user_id}:{platform}")
            return session_data
            
        except Exception as e:
            logger.error(f"Failed to load session: {e}")
            return None

    def restore_session(self, driver: webdriver.Chrome, session_data: Dict) -> bool:
        """Restore session to a browser instance"""
        try:
            platform = session_data.get('platform')
            current_url = session_data.get('current_url')
            
            # Navigate to platform first
            platform_urls = {
                'twitter': 'https://twitter.com',
                'linkedin': 'https://www.linkedin.com',
                'facebook': 'https://www.facebook.com',
                'instagram': 'https://www.instagram.com'
            }
            
            start_url = platform_urls.get(platform, current_url)
            driver.get(start_url)
            time.sleep(2)
            
            # Add cookies
            cookies_added = 0
            for cookie in session_data.get('cookies', []):
                try:
                    # Clean cookie data
                    cookie_data = {
                        'name': cookie['name'],
                        'value': cookie['value'],
                        'domain': cookie.get('domain'),
                        'path': cookie.get('path', '/'),
                        'secure': cookie.get('secure', False),
                        'httpOnly': cookie.get('httpOnly', False)
                    }
                    
                    # Remove None values
                    cookie_data = {k: v for k, v in cookie_data.items() if v is not None}
                    
                    driver.add_cookie(cookie_data)
                    cookies_added += 1
                    
                except Exception as e:
                    logger.debug(f"Failed to add cookie {cookie.get('name')}: {e}")
                    continue
            
            logger.debug(f"Added {cookies_added} cookies")
            
            # Refresh to apply session data
            driver.refresh()
            time.sleep(3)
            
            logger.info("Session restoration completed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore session: {e}")
            return False

    def invalidate_session(self, user_id: str, platform: str):
        """Remove invalid session"""
        try:
            session_key = f"session:{user_id}:{platform}"
            session_metadata = self.redis.hgetall(session_key)
            
            # Delete session file
            if session_metadata:
                session_file = session_metadata.get('session_file')
                if session_file and os.path.exists(session_file):
                    os.remove(session_file)
                    logger.debug(f"Deleted session file: {session_file}")
            
            # Remove from Redis
            self.redis.delete(session_key)
            
            logger.info(f"Session invalidated for {user_id}:{platform}")
            
        except Exception as e:
            logger.error(f"Failed to invalidate session: {e}")

    def is_session_valid(self, user_id: str, platform: str) -> bool:
        """Check if user has a valid session"""
        try:
            session_key = f"session:{user_id}:{platform}"
            session_metadata = self.redis.hgetall(session_key)
            
            if not session_metadata:
                return False
            
            # Check expiry
            expires_at_str = session_metadata.get('expires_at')
            if expires_at_str:
                expires_at = datetime.fromisoformat(expires_at_str)
                return datetime.now() < expires_at
            
            # Fallback to saved_at check
            saved_at_str = session_metadata.get('saved_at')
            if saved_at_str:
                saved_at = datetime.fromisoformat(saved_at_str)
                expiry_hours = self.session_expiry.get(platform, 24)
                return datetime.now() - saved_at < timedelta(hours=expiry_hours)
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking session validity: {e}")
            return False

    def cleanup_old_sessions(self):
        """Clean up sessions older than their expiry time"""
        try:
            cleaned_count = 0
            pattern = "session:*"
            
            for key in self.redis.scan_iter(pattern):
                try:
                    session_metadata = self.redis.hgetall(key)
                    if not session_metadata:
                        continue
                    
                    platform = session_metadata.get('platform')
                    expires_at_str = session_metadata.get('expires_at')
                    
                    if expires_at_str:
                        expires_at = datetime.fromisoformat(expires_at_str)
                        if datetime.now() > expires_at:
                            user_id, platform = key.split(':')[1:3]
                            self.invalidate_session(user_id, platform)
                            cleaned_count += 1
                    else:
                        # Fallback cleanup based on saved_at
                        saved_at_str = session_metadata.get('saved_at')
                        if saved_at_str:
                            saved_at = datetime.fromisoformat(saved_at_str)
                            expiry_hours = self.session_expiry.get(platform, 24)
                            if datetime.now() - saved_at > timedelta(hours=expiry_hours):
                                user_id, platform = key.split(':')[1:3]
                                self.invalidate_session(user_id, platform)
                                cleaned_count += 1
                
                except Exception as e:
                    logger.warning(f"Error processing session key {key}: {e}")
                    # Delete corrupted session metadata
                    self.redis.delete(key)
                    cleaned_count += 1
            
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} old/invalid sessions")
                
        except Exception as e:
            logger.error(f"Session cleanup failed: {e}")

    def _setup_driver(self) -> webdriver.Chrome:
        """Setup Chrome WebDriver for session management"""
        chrome_options = Options()
        # chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.implicitly_wait(10)
        return driver

    def _generate_session_id(self, user_id: str, platform: str) -> str:
        """Generate unique session ID"""
        timestamp = datetime.now().isoformat()
        data = f"{user_id}:{platform}:{timestamp}"
        return hashlib.md5(data.encode()).hexdigest()[:16]

    def _verify_platform_login(self, driver: webdriver.Chrome, platform: str) -> bool:
        """Verify if logged into specific platform"""
        try:
            current_url = driver.current_url.lower()
            
            verification_checks = {
                'twitter': lambda: 'home' in current_url or ('twitter.com' in current_url and 'login' not in current_url),
                'linkedin': lambda: 'feed' in current_url or ('linkedin.com' in current_url and 'login' not in current_url),
                'facebook': lambda: 'facebook.com' in current_url and 'login' not in current_url,
                'instagram': lambda: 'instagram.com' in current_url and 'login' not in current_url
            }
            
            check_func = verification_checks.get(platform)
            if check_func:
                return check_func()
            
            return False
            
        except Exception as e:
            logger.debug(f"Error verifying {platform} login: {e}")
            return False

    def _perform_platform_login(self, driver: webdriver.Chrome, platform: str, 
                               username: str, password: str) -> Dict[str, Any]:
        """Perform fresh login to platform - simplified for testing"""
        try:
            # For now, just simulate successful login
            # In production, you'd implement actual login logic here
            logger.info(f"Simulating login to {platform} for {username}")
            time.sleep(2)  # Simulate login time
            
            return {'success': True, 'message': f'{platform} login simulated successfully'}
                
        except Exception as e:
            return {'success': False, 'error': f'Login error: {str(e)}'}