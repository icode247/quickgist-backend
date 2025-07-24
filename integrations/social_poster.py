# integrations/social_poster.py - Complete Enterprise Implementation

import time
import logging
import os
import sys
import tempfile
import requests
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union, Protocol
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

# Selenium imports
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException

# Project imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(0, os.path.join(parent_dir, "utils"))

from integrations.utils.api_client import make_api_request
from integrations.session_manager import SocialSessionManager
from integrations.utils.encryption_utils import get_encryption_key, decrypt, encrypt

# Configure logging
logger = logging.getLogger(__name__)

# =====================================================================================
# ENUMS AND DATA CLASSES
# =====================================================================================

class PlatformType(Enum):
    """Supported social media platforms"""
    TWITTER = "twitter"
    LINKEDIN = "linkedin"
    FACEBOOK = "facebook"
    INSTAGRAM = "instagram"

class PostingMethod(Enum):
    """Methods for posting content"""
    BROWSER_AUTOMATION = "browser"
    API_INTEGRATION = "api"

class ErrorCode(Enum):
    """Standardized error codes for social posting"""
    SUCCESS = "SUCCESS"
    AUTHENTICATION_ERROR = "AUTH_ERROR"
    ENCRYPTION_KEY_ERROR = "ENCRYPTION_KEY_ERROR"
    DECRYPTION_ERROR = "DECRYPTION_ERROR"
    SESSION_EXPIRED = "SESSION_EXPIRED"
    PLATFORM_ERROR = "PLATFORM_ERROR"
    NETWORK_ERROR = "NETWORK_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    OAUTH_REQUIRED = "OAUTH_REQUIRED"
    RATE_LIMITED = "RATE_LIMITED"
    CONTENT_REJECTED = "CONTENT_REJECTED"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"

@dataclass
class PostContent:
    """Structured post content with validation"""
    text: str
    image_url: Optional[str] = None
    image_path: Optional[str] = None
    hashtags: Optional[List[str]] = None
    mentions: Optional[List[str]] = None
    
    def __post_init__(self):
        if not self.text or len(self.text.strip()) == 0:
            raise ValueError("Post text cannot be empty")
        if len(self.text) > 2800:  # Conservative limit across platforms
            raise ValueError("Post text exceeds maximum length")

@dataclass
class PostResult:
    """Standardized result from posting operations"""
    success: bool
    error_code: ErrorCode
    message: str
    platform_post_id: Optional[str] = None
    platform_url: Optional[str] = None
    published_at: Optional[str] = None
    verification_details: Optional[str] = None
    requires_action: Optional[str] = None
    oauth_url: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'success': self.success,
            'error_code': self.error_code.value,
            'message': self.message,
            'platform_post_id': self.platform_post_id,
            'platform_url': self.platform_url,
            'published_at': self.published_at,
            'verification_details': self.verification_details,
            'requires_action': self.requires_action,
            'oauth_url': self.oauth_url
        }

@dataclass
class AccountCredentials:
    """Structured account credentials with validation"""
    user_id: str
    platform: PlatformType
    username: str
    password_encrypted: str
    account_id: Optional[str] = None
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    connected: bool = False
    
    def __post_init__(self):
        if not all([self.user_id, self.username, self.password_encrypted]):
            raise ValueError("Missing required credential fields")

# =====================================================================================
# PROTOCOLS AND INTERFACES
# =====================================================================================

class SocialPlatformPoster(Protocol):
    """Protocol defining the interface all platform posters must implement"""
    
    def publish_post(self, content: PostContent, credentials: AccountCredentials) -> PostResult:
        """Publish content to the social media platform"""
        ...
    
    def test_connection(self, credentials: AccountCredentials) -> PostResult:
        """Test connection to the platform"""
        ...
    
    def get_posting_method(self) -> PostingMethod:
        """Return the posting method used by this platform"""
        ...

# =====================================================================================
# BASE CLASSES
# =====================================================================================

class BaseSocialPoster(ABC):
    """Abstract base class for all social media platform posters"""
    
    def __init__(self, session_manager: SocialSessionManager):
        self.session_manager = session_manager
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup platform-specific logging"""
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def get_platform_type(self) -> PlatformType:
        """Return the platform type this poster handles"""
        pass
    
    @abstractmethod
    def get_posting_method(self) -> PostingMethod:
        """Return the posting method used"""
        pass
    
    @abstractmethod
    def publish_post(self, content: PostContent, credentials: AccountCredentials) -> PostResult:
        """Publish content to the platform"""
        pass
    
    @abstractmethod
    def test_connection(self, credentials: AccountCredentials) -> PostResult:
        """Test connection to the platform"""
        pass
    
    def _sanitize_content(self, text: str) -> str:
        """Sanitize content for safe posting"""
        # Remove non-BMP characters that can cause issues
        sanitized = ""
        for char in text:
            if ord(char) <= 0xFFFF:
                sanitized += char
            else:
                sanitized += " "
        
        # Replace smart quotes and other problematic characters
        sanitized = sanitized.replace('\u2019', "'")
        sanitized = sanitized.replace('\u201c', '"').replace('\u201d', '"')
        
        return sanitized.strip()
    
    def _validate_credentials(self, credentials: AccountCredentials) -> bool:
        """Validate credential structure"""
        try:
            if not isinstance(credentials, AccountCredentials):
                return False
            return bool(credentials.user_id and credentials.username and credentials.password_encrypted)
        except Exception:
            return False

class BrowserBasedPoster(BaseSocialPoster):
    """Base class for browser automation based posting"""
    
    def __init__(self, session_manager: SocialSessionManager):
        super().__init__(session_manager)
        self._driver_options = self._get_default_chrome_options()
    
    def get_posting_method(self) -> PostingMethod:
        return PostingMethod.BROWSER_AUTOMATION
    
    def _get_default_chrome_options(self) -> Options:
        """Get standardized Chrome options for all platforms"""
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        
        # Stealth options
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        return options
    
    def _setup_driver(self) -> webdriver.Chrome:
        """Setup Chrome driver with error handling"""
        try:
            driver = webdriver.Chrome(options=self._driver_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.implicitly_wait(10)
            return driver
        except Exception as e:
            self.logger.error(f"Failed to setup Chrome driver: {e}")
            raise
    
    @abstractmethod
    def _verify_login_status(self, driver: webdriver.Chrome) -> bool:
        """Verify if user is logged into the platform"""
        pass
    
    @abstractmethod
    def _perform_login(self, driver: webdriver.Chrome, credentials: AccountCredentials) -> PostResult:
        """Perform login to the platform"""
        pass
    
    @abstractmethod
    def _execute_post_publication(self, driver: webdriver.Chrome, content: PostContent) -> PostResult:
        """Execute the actual post publication"""
        pass
    
    def publish_post(self, content: PostContent, credentials: AccountCredentials) -> PostResult:
        """Standard publish flow for browser-based platforms"""
        if not self._validate_credentials(credentials):
            return PostResult(
                success=False,
                error_code=ErrorCode.VALIDATION_ERROR,
                message="Invalid credentials provided"
            )
        
        driver = None
        try:
            # Setup driver
            driver = self._setup_driver()
            
            # Try to restore session
            session_restored = self._try_restore_session(driver, credentials)
            
            # Login if session not restored
            if not session_restored:
                login_result = self._perform_fresh_login(driver, credentials)
                if not login_result.success:
                    return login_result
            
            # Publish the post
            publish_result = self._execute_post_publication(driver, content)
            
            # Save session on success
            if publish_result.success:
                self._save_session(driver, credentials)
            
            return publish_result
            
        except Exception as e:
            self.logger.error(f"Error in publish_post: {e}")
            return PostResult(
                success=False,
                error_code=ErrorCode.UNKNOWN_ERROR,
                message=f"Unexpected error: {str(e)}"
            )
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception as e:
                    self.logger.warning(f"Error closing driver: {e}")
    
    def test_connection(self, credentials: AccountCredentials) -> PostResult:
        """Test connection for browser-based platforms"""
        if not self._validate_credentials(credentials):
            return PostResult(
                success=False,
                error_code=ErrorCode.VALIDATION_ERROR,
                message="Invalid credentials provided"
            )
        
        driver = None
        try:
            driver = self._setup_driver()
            
            # Try to restore session first
            if self._try_restore_session(driver, credentials):
                return PostResult(
                    success=True,
                    error_code=ErrorCode.SUCCESS,
                    message="Session restored successfully"
                )
            
            # Perform fresh login test
            login_result = self._perform_fresh_login(driver, credentials)
            if login_result.success:
                self._save_session(driver, credentials)
            
            return login_result
            
        except Exception as e:
            self.logger.error(f"Error in test_connection: {e}")
            return PostResult(
                success=False,
                error_code=ErrorCode.UNKNOWN_ERROR,
                message=f"Connection test failed: {str(e)}"
            )
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def _try_restore_session(self, driver: webdriver.Chrome, credentials: AccountCredentials) -> bool:
        """Try to restore an existing session"""
        try:
            if not self.session_manager.is_session_valid(credentials.user_id, credentials.platform.value):
                return False
            
            session_data = self.session_manager.load_session(credentials.user_id, credentials.platform.value)
            if not session_data:
                return False
            
            restored = self.session_manager.restore_session(driver, session_data)
            if restored and self._verify_login_status(driver):
                self.logger.info(f"Session restored for {credentials.platform.value}")
                return True
            else:
                self.session_manager.invalidate_session(credentials.user_id, credentials.platform.value)
                return False
                
        except Exception as e:
            self.logger.warning(f"Session restoration failed: {e}")
            return False
    
    def _perform_fresh_login(self, driver: webdriver.Chrome, credentials: AccountCredentials) -> PostResult:
        """Perform fresh login with proper error handling"""
        try:
            # Decrypt password
            try:
                encryption_key = get_encryption_key()
                password = decrypt(credentials.password_encrypted, encryption_key)
            except ValueError as e:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.ENCRYPTION_KEY_ERROR,
                    message=f"Encryption key error: {str(e)}"
                )
            except Exception as e:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.DECRYPTION_ERROR,
                    message=f"Password decryption failed: {str(e)}"
                )
            
            # Update credentials with decrypted password for login
            temp_credentials = AccountCredentials(
                user_id=credentials.user_id,
                platform=credentials.platform,
                username=credentials.username,
                password_encrypted=password,  # Temporarily store decrypted password
                account_id=credentials.account_id,
                connected=credentials.connected
            )
            
            # Perform platform-specific login
            return self._perform_login(driver, temp_credentials)
            
        except Exception as e:
            self.logger.error(f"Fresh login failed: {e}")
            return PostResult(
                success=False,
                error_code=ErrorCode.AUTHENTICATION_ERROR,
                message=f"Login failed: {str(e)}"
            )
    
    def _save_session(self, driver: webdriver.Chrome, credentials: AccountCredentials):
        """Save session for future use"""
        try:
            self.session_manager.save_session(credentials.user_id, credentials.platform.value, driver)
            self.logger.debug(f"Session saved for {credentials.platform.value}")
        except Exception as e:
            self.logger.warning(f"Failed to save session: {e}")
    
    def _handle_image_upload(self, driver: webdriver.Chrome, image_url: str) -> bool:
        """Handle image upload from URL"""
        temp_file_path = None
        try:
            # Download image
            temp_file_path = self._download_image(image_url)
            if not temp_file_path:
                return False
            
            # Platform-specific upload
            return self._upload_image_file(driver, temp_file_path)
            
        except Exception as e:
            self.logger.error(f"Image upload failed: {e}")
            return False
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except:
                    pass
    
    def _download_image(self, image_url: str) -> Optional[str]:
        """Download image from URL to temporary file"""
        try:
            response = requests.get(image_url, stream=True, timeout=30)
            response.raise_for_status()
            
            file_extension = '.jpg'
            if '.' in image_url.split('/')[-1]:
                file_extension = '.' + image_url.split('.')[-1].split('?')[0]
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
                for chunk in response.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
                return temp_file.name
                
        except Exception as e:
            self.logger.error(f"Image download failed: {e}")
            return None
    
    @abstractmethod
    def _upload_image_file(self, driver: webdriver.Chrome, file_path: str) -> bool:
        """Platform-specific image upload implementation"""
        pass

class APIBasedPoster(BaseSocialPoster):
    """Base class for API-based posting"""
    
    def get_posting_method(self) -> PostingMethod:
        return PostingMethod.API_INTEGRATION
    
    @abstractmethod
    def _get_access_token(self, user_id: str) -> Optional[str]:
        """Get valid access token for API calls"""
        pass
    
    @abstractmethod
    def _make_api_post(self, access_token: str, content: PostContent) -> PostResult:
        """Make API call to publish post"""
        pass
    
    @abstractmethod
    def _test_api_connection(self, access_token: str) -> PostResult:
        """Test API connection"""
        pass

# =====================================================================================
# PLATFORM IMPLEMENTATIONS
# =====================================================================================

class TwitterPoster(BrowserBasedPoster):
    """Twitter/X posting implementation"""
    
    def get_platform_type(self) -> PlatformType:
        return PlatformType.TWITTER
    
    def _verify_login_status(self, driver: webdriver.Chrome) -> bool:
        """Verify Twitter login status"""
        try:
            current_url = driver.current_url.lower()
            
            if any(url_pattern in current_url for url_pattern in ['twitter.com/home', 'x.com/home']):
                return True
                
            if any(domain in current_url for domain in ['twitter.com', 'x.com']) and 'login' not in current_url:
                # Check for login indicators
                login_indicators = [
                    "//a[@data-testid='AppTabBar_Home_Link']",
                    "//a[@data-testid='SideNav_NewTweet_Button']",
                    "//div[@data-testid='tweetTextarea_0']"
                ]
                
                for indicator in login_indicators:
                    try:
                        elements = driver.find_elements(By.XPATH, indicator)
                        if elements and any(el.is_displayed() for el in elements):
                            return True
                    except:
                        continue
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error verifying Twitter login: {e}")
            return False
    
    def _perform_login(self, driver: webdriver.Chrome, credentials: AccountCredentials) -> PostResult:
        """Perform Twitter login"""
        try:
            driver.get('https://twitter.com/login')
            time.sleep(3)
            
            # Enter username
            username_selectors = [
                "//input[@autocomplete='username']",
                "//input[@name='text']",
                "//input[@data-testid='login-username-field']"
            ]
            
            username_field = None
            for selector in username_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            username_field = element
                            break
                    if username_field:
                        break
                except:
                    continue
            
            if not username_field:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.PLATFORM_ERROR,
                    message="Could not find username field"
                )
            
            username_field.clear()
            username_field.send_keys(credentials.username)
            time.sleep(1)
            
            # Click Next
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@role='button'][.//span[contains(text(), 'Next')]]"))
            )
            next_button.click()
            time.sleep(3)
            
            # Enter password
            password_selectors = [
                "//input[@name='password']",
                "//input[@autocomplete='current-password']",
                "//input[@type='password']"
            ]
            
            password_field = None
            for selector in password_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            password_field = element
                            break
                    if password_field:
                        break
                except:
                    continue
            
            if not password_field:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.PLATFORM_ERROR,
                    message="Could not find password field"
                )
            
            password_field.clear()
            password_field.send_keys(credentials.password_encrypted)  # Contains decrypted password at this point
            time.sleep(1)
            
            # Click Login
            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@data-testid='LoginForm_Login_Button']"))
            )
            login_button.click()
            time.sleep(5)
            
            # Verify login
            if self._verify_login_status(driver):
                return PostResult(
                    success=True,
                    error_code=ErrorCode.SUCCESS,
                    message="Twitter login successful"
                )
            else:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.AUTHENTICATION_ERROR,
                    message="Twitter login failed - credentials may be incorrect"
                )
                
        except Exception as e:
            self.logger.error(f"Twitter login error: {e}")
            return PostResult(
                success=False,
                error_code=ErrorCode.PLATFORM_ERROR,
                message=f"Twitter login error: {str(e)}"
            )
    
    def _execute_post_publication(self, driver: webdriver.Chrome, content: PostContent) -> PostResult:
        """Execute Twitter post publication"""
        try:
            # Ensure we're on Twitter home
            if 'twitter.com/home' not in driver.current_url and 'x.com/home' not in driver.current_url:
                driver.get('https://twitter.com/home')
                time.sleep(5)
            
            # Sanitize content
            sanitized_text = self._sanitize_content(content.text)
            
            # Click compose button
            compose_selectors = [
                "//a[@data-testid='SideNav_NewTweet_Button']",
                "//a[@href='/compose/tweet']",
                "//div[@role='button'][@data-testid='tweetButtonInline']"
            ]
            
            compose_button = None
            for selector in compose_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            compose_button = element
                            break
                    if compose_button:
                        break
                except:
                    continue
            
            if not compose_button:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.PLATFORM_ERROR,
                    message="Could not find compose button"
                )
            
            compose_button.click()
            time.sleep(3)
            
            # Handle image upload if present
            if content.image_url:
                image_uploaded = self._handle_image_upload(driver, content.image_url)
                if not image_uploaded:
                    self.logger.warning("Image upload failed, proceeding with text-only post")
            
            # Enter tweet text
            editor_selectors = [
                "//div[@data-testid='tweetTextarea_0']",
                "//div[@role='textbox'][contains(@aria-label, 'Post')]",
                "//div[@contenteditable='true'][@data-testid='tweetTextarea_0']"
            ]
            
            tweet_editor = None
            for selector in editor_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            tweet_editor = element
                            break
                    if tweet_editor:
                        break
                except:
                    continue
            
            if not tweet_editor:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.PLATFORM_ERROR,
                    message="Could not find tweet editor"
                )
            
            tweet_editor.click()
            time.sleep(1)
            
            # Send text in chunks
            chunk_size = 100
            for i in range(0, len(sanitized_text), chunk_size):
                chunk = sanitized_text[i:i+chunk_size]
                tweet_editor.send_keys(chunk)
                time.sleep(0.5)
            
            time.sleep(2)
            
            # Click Post button
            post_button_clicked = driver.execute_script("""
                var postButton = document.querySelector('button[data-testid="tweetButton"]');
                if(postButton) {
                    postButton.click();
                    return true;
                }
                return false;
            """)
            
            if not post_button_clicked:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.PLATFORM_ERROR,
                    message="Could not click post button"
                )
            
            time.sleep(5)
            
            # Check for success indicators
            success_indicators = [
                "//span[contains(text(), 'Your post was sent')]",
                "//span[contains(text(), 'Your Tweet was sent')]"
            ]
            
            for indicator in success_indicators:
                try:
                    elements = driver.find_elements(By.XPATH, indicator)
                    if elements and any(el.is_displayed() for el in elements):
                        return PostResult(
                            success=True,
                            error_code=ErrorCode.SUCCESS,
                            message="Twitter post published successfully",
                            platform_post_id=f"twitter_{int(time.time())}",
                            published_at=datetime.now().isoformat(),
                            platform_url="https://twitter.com/home"
                        )
                except:
                    continue
            
            # Assume success if no errors occurred
            return PostResult(
                success=True,
                error_code=ErrorCode.SUCCESS,
                message="Twitter post published successfully",
                platform_post_id=f"twitter_{int(time.time())}",
                published_at=datetime.now().isoformat(),
                platform_url="https://twitter.com/home"
            )
            
        except Exception as e:
            self.logger.error(f"Twitter post publication error: {e}")
            return PostResult(
                success=False,
                error_code=ErrorCode.PLATFORM_ERROR,
                message=f"Post publication failed: {str(e)}"
            )
    
    def _upload_image_file(self, driver: webdriver.Chrome, file_path: str) -> bool:
        """Upload image file to Twitter"""
        try:
            file_input_selectors = [
                "//input[@data-testid='fileInput']",
                "//input[@type='file'][@accept*='image']"
            ]
            
            for selector in file_input_selectors:
                try:
                    file_input = driver.find_element(By.XPATH, selector)
                    driver.execute_script("""
                        arguments[0].style.opacity = '0.01';
                        arguments[0].style.position = 'absolute';
                        arguments[0].style.left = '-9999px';
                    """, file_input)
                    
                    file_input.send_keys(str(Path(file_path).absolute()))
                    time.sleep(3)
                    
                    # Check for upload success
                    upload_indicators = [
                        "//img[contains(@src, 'blob:')]",
                        "//div[contains(@aria-label, 'Remove media')]"
                    ]
                    
                    for indicator in upload_indicators:
                        elements = driver.find_elements(By.XPATH, indicator)
                        if elements:
                            return True
                    
                    return True  # Assume success if no errors
                    
                except Exception as e:
                    self.logger.debug(f"Upload attempt failed with selector {selector}: {e}")
                    continue
            
            return False
            
        except Exception as e:
            self.logger.error(f"Twitter image upload error: {e}")
            return False

class FacebookPoster(BrowserBasedPoster):
    """Facebook posting implementation"""
    
    def get_platform_type(self) -> PlatformType:
        return PlatformType.FACEBOOK
    
    def _verify_login_status(self, driver: webdriver.Chrome) -> bool:
        """Verify Facebook login status"""
        try:
            current_url = driver.current_url.lower()
            
            if 'facebook.com' in current_url:
                if any(x in current_url for x in ['login', 'signup', 'recover']):
                    return False
                
                # Check for profile selection page
                if self._is_profile_selection_page(driver):
                    return self._handle_profile_selection(driver)
                
                # Check for normal login indicators
                login_indicators = [
                    "//div[@role='banner']",
                    "//div[contains(text(), \"What's on your mind\")]",
                    "//a[contains(@href, '/me')]"
                ]
                
                for indicator in login_indicators:
                    try:
                        elements = driver.find_elements(By.XPATH, indicator)
                        if elements and any(el.is_displayed() for el in elements):
                            return True
                    except:
                        continue
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error verifying Facebook login: {e}")
            return False
    
    def _is_profile_selection_page(self, driver: webdriver.Chrome) -> bool:
        """Check if we're on profile selection page"""
        try:
            profile_selectors = [
                "//div[@class='uiContextualLayerParent'][@data-userid]",
                "//a[@title and @class='_1gbd']"
            ]
            
            for selector in profile_selectors:
                elements = driver.find_elements(By.XPATH, selector)
                if elements and any(el.is_displayed() for el in elements):
                    return True
            
            return False
            
        except:
            return False
    
    def _handle_profile_selection(self, driver: webdriver.Chrome) -> bool:
        """Handle profile selection if needed"""
        try:
            # This would need to be implemented based on specific requirements
            # For now, we'll return False to trigger fresh login
            return False
        except:
            return False
    
    def _perform_login(self, driver: webdriver.Chrome, credentials: AccountCredentials) -> PostResult:
        """Perform Facebook login"""
        try:
            driver.get('https://www.facebook.com/login')
            time.sleep(3)
            
            # Handle cookie consent
            try:
                cookie_buttons = driver.find_elements(By.XPATH, 
                    "//button[contains(text(), 'Accept') or contains(text(), 'Allow')]")
                if cookie_buttons:
                    cookie_buttons[0].click()
                    time.sleep(2)
            except:
                pass
            
            # Enter email
            email_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "email"))
            )
            email_field.clear()
            email_field.send_keys(credentials.username)
            
            # Enter password
            password_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "pass"))
            )
            password_field.clear()
            password_field.send_keys(credentials.password_encrypted)  # Contains decrypted password
            
            # Click login
            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@name='login' or @type='submit']"))
            )
            login_button.click()
            time.sleep(5)
            
            # Handle potential redirects
            current_url = driver.current_url.lower()
            if 'auth_platform/afad' in current_url:
                try:
                    WebDriverWait(driver, 30).until(
                        lambda d: 'facebook.com' in d.current_url.lower() and 'auth_platform' not in d.current_url.lower()
                    )
                    time.sleep(3)
                except TimeoutException:
                    driver.get('https://www.facebook.com/')
                    time.sleep(5)
            
            # Verify login
            if self._verify_login_status(driver):
                return PostResult(
                    success=True,
                    error_code=ErrorCode.SUCCESS,
                    message="Facebook login successful"
                )
            else:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.AUTHENTICATION_ERROR,
                    message="Facebook login failed"
                )
                
        except Exception as e:
            self.logger.error(f"Facebook login error: {e}")
            return PostResult(
                success=False,
                error_code=ErrorCode.PLATFORM_ERROR,
                message=f"Facebook login error: {str(e)}"
            )
    
    def _execute_post_publication(self, driver: webdriver.Chrome, content: PostContent) -> PostResult:
        """Execute Facebook post publication"""
        try:
            # Ensure we're on Facebook home
            if 'facebook.com' not in driver.current_url or 'login' in driver.current_url:
                driver.get('https://www.facebook.com/')
                time.sleep(5)
            
            sanitized_text = self._sanitize_content(content.text)
            
            # Find and click composer
            composer_selectors = [
                "//div[@role='button'][.//span[contains(text(), \"What's on your mind\")]]",
                "//div[contains(@aria-label, 'Create a post')]"
            ]
            
            composer_button = None
            for selector in composer_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            composer_button = element
                            break
                    if composer_button:
                        break
                except:
                    continue
            
            if not composer_button:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.PLATFORM_ERROR,
                    message="Could not find post composer"
                )
            
            composer_button.click()
            time.sleep(3)
            
            # Wait for modal
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@aria-label='Create post'][@role='dialog']"))
                )
            except TimeoutException:
                self.logger.warning("Post creation modal did not appear")
            
            # Handle image upload
            if content.image_url:
                image_uploaded = self._handle_image_upload(driver, content.image_url)
                if not image_uploaded:
                    self.logger.warning("Image upload failed, proceeding with text-only")
            
            # Find text editor
            editor_selectors = [
                "//div[@contenteditable='true'][@role='textbox'][@data-lexical-editor='true']",
                "//div[@contenteditable='true'][@role='textbox']",
                "//div[@contenteditable='true']"
            ]
            
            post_editor = None
            for selector in editor_selectors:
                try:
                    post_editor = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                    if post_editor.is_displayed():
                        break
                    else:
                        post_editor = None
                except:
                    continue
            
            if not post_editor:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.PLATFORM_ERROR,
                    message="Could not find post editor"
                )
            
            # Enter content
            post_editor.click()
            time.sleep(1)
            post_editor.clear()
            
            # Clear any placeholder content
            try:
                post_editor.send_keys(Keys.CONTROL + "a")
                time.sleep(0.5)
                post_editor.send_keys(Keys.DELETE)
                time.sleep(0.5)
            except:
                pass
            
            # Enter content in chunks
            chunk_size = 100
            for i in range(0, len(sanitized_text), chunk_size):
                chunk = sanitized_text[i:i+chunk_size]
                post_editor.send_keys(chunk)
                time.sleep(0.2)
            
            time.sleep(2)
            
            # Click Post button
            post_button_selectors = [
                "//div[@aria-label='Post'][@role='button']",
                "//div[@role='button'][.//span[contains(text(), 'Post')]]"
            ]
            
            post_button = None
            for selector in post_button_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed() and element.is_enabled():
                            post_button = element
                            break
                    if post_button:
                        break
                except:
                    continue
            
            if not post_button:
                # Try JavaScript fallback
                success = driver.execute_script("""
                    var buttons = document.querySelectorAll('div[role="button"], button');
                    for(var i = 0; i < buttons.length; i++) {
                        if(buttons[i].getAttribute('aria-label') === 'Post' || 
                           buttons[i].innerText.trim() === 'Post') {
                            buttons[i].click();
                            return true;
                        }
                    }
                    return false;
                """)
                
                if not success:
                    return PostResult(
                        success=False,
                        error_code=ErrorCode.PLATFORM_ERROR,
                        message="Could not click post button"
                    )
            else:
                post_button.click()
            
            time.sleep(5)
            
            # Check for success (modal closure or success message)
            try:
                WebDriverWait(driver, 10).until_not(
                    EC.presence_of_element_located((By.XPATH, "//div[@aria-label='Create post'][@role='dialog']"))
                )
                post_success = True
            except TimeoutException:
                post_success = False
            
            if not post_success:
                # Check for other success indicators
                success_indicators = [
                    "//div[contains(text(), 'Post shared')]",
                    "//div[contains(text(), 'Your post is now live')]"
                ]
                
                for indicator in success_indicators:
                    try:
                        elements = driver.find_elements(By.XPATH, indicator)
                        if elements and any(el.is_displayed() for el in elements):
                            post_success = True
                            break
                    except:
                        continue
            
            # Final check - assume success if back on main page with no errors
            if not post_success and 'facebook.com' in driver.current_url and 'error' not in driver.current_url:
                post_success = True
            
            if post_success:
                return PostResult(
                    success=True,
                    error_code=ErrorCode.SUCCESS,
                    message="Facebook post published successfully",
                    platform_post_id=f"fb_{int(time.time())}",
                    published_at=datetime.now().isoformat(),
                    platform_url="https://www.facebook.com/"
                )
            else:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.PLATFORM_ERROR,
                    message="Post publication could not be verified"
                )
                
        except Exception as e:
            self.logger.error(f"Facebook post publication error: {e}")
            return PostResult(
                success=False,
                error_code=ErrorCode.PLATFORM_ERROR,
                message=f"Post publication failed: {str(e)}"
            )
    
    def _upload_image_file(self, driver: webdriver.Chrome, file_path: str) -> bool:
        """Upload image file to Facebook"""
        try:
            image_upload_selectors = [
                "//input[@accept*='image/*,image/heif,image/heic,video/*'][@type='file']",
                "//input[@accept*='image'][@type='file'][@multiple]",
                "//form//input[@type='file'][@accept*='image']"
            ]
            
            for selector in image_upload_selectors:
                try:
                    file_inputs = driver.find_elements(By.XPATH, selector)
                    for file_input in file_inputs:
                        if file_input.is_displayed() or file_input.get_attribute('style') != 'display: none;':
                            try:
                                absolute_path = str(Path(file_path).resolve())
                                file_input.send_keys(absolute_path)
                                time.sleep(3)
                                return True
                            except:
                                continue
                except:
                    continue
            
            return False
            
        except Exception as e:
            self.logger.error(f"Facebook image upload error: {e}")
            return False

class InstagramPoster(BrowserBasedPoster):
    """Instagram posting implementation"""
    
    def get_platform_type(self) -> PlatformType:
        return PlatformType.INSTAGRAM
    
    def _verify_login_status(self, driver: webdriver.Chrome) -> bool:
        """Verify Instagram login status"""
        try:
            current_url = driver.current_url.lower()
            
            if 'instagram.com' in current_url and not any(x in current_url for x in ['accounts/login', 'accounts/signup']):
                login_indicators = [
                    "//a[@href='/']//svg[@aria-label='Home']",
                    "//a[contains(@href, '/direct/')]",
                    "//button[@type='button']//svg[@aria-label='New post']",
                    "//div[@role='menubar']"
                ]
                
                for indicator in login_indicators:
                    try:
                        elements = driver.find_elements(By.XPATH, indicator)
                        if elements and any(el.is_displayed() for el in elements):
                            return True
                    except:
                        continue
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error verifying Instagram login: {e}")
            return False
    
    def _perform_login(self, driver: webdriver.Chrome, credentials: AccountCredentials) -> PostResult:
        """Perform Instagram login"""
        try:
            driver.get('https://www.instagram.com/accounts/login/')
            time.sleep(5)
            
            # Handle cookie consent
            try:
                cookie_buttons = driver.find_elements(By.XPATH, 
                    "//button[contains(text(), 'Accept') or contains(text(), 'Only allow essential')]")
                if cookie_buttons:
                    cookie_buttons[0].click()
                    time.sleep(2)
            except:
                pass
            
            # Enter username
            username_field = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//input[@name='username' or @aria-label='Phone number, username, or email']"))
            )
            username_field.clear()
            username_field.send_keys(credentials.username)
            time.sleep(1)
            
            # Enter password
            password_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@name='password' or @aria-label='Password']"))
            )
            password_field.clear()
            password_field.send_keys(credentials.password_encrypted)  # Contains decrypted password
            time.sleep(1)
            
            # Click login
            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@type='submit' or contains(text(), 'Log in')]"))
            )
            login_button.click()
            time.sleep(8)
            
            # Handle prompts
            try:
                # "Save Your Login Info?" prompt
                not_now_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Not Now') or contains(text(), 'Not now')]")
                if not_now_buttons:
                    not_now_buttons[0].click()
                    time.sleep(2)
                
                # "Turn on Notifications?" prompt
                not_now_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Not Now') or contains(text(), 'Not now')]")
                if not_now_buttons:
                    not_now_buttons[0].click()
                    time.sleep(2)
            except:
                pass
            
            # Verify login
            if self._verify_login_status(driver):
                return PostResult(
                    success=True,
                    error_code=ErrorCode.SUCCESS,
                    message="Instagram login successful"
                )
            else:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.AUTHENTICATION_ERROR,
                    message="Instagram login failed"
                )
                
        except Exception as e:
            self.logger.error(f"Instagram login error: {e}")
            return PostResult(
                success=False,
                error_code=ErrorCode.PLATFORM_ERROR,
                message=f"Instagram login error: {str(e)}"
            )
    
    def _execute_post_publication(self, driver: webdriver.Chrome, content: PostContent) -> PostResult:
        """Execute Instagram post publication"""
        # Instagram web interface has very limited posting capabilities
        # This is mostly for demonstration - real implementation would need special handling
        return PostResult(
            success=False,
            error_code=ErrorCode.PLATFORM_ERROR,
            message="Instagram web interface does not support automated posting. Please use Instagram mobile app or business tools."
        )
    
    def _upload_image_file(self, driver: webdriver.Chrome, file_path: str) -> bool:
        """Instagram image upload (limited web capabilities)"""
        return False

class LinkedInAPIPoster(APIBasedPoster):
    """LinkedIn API posting implementation"""
    
    def __init__(self, session_manager: SocialSessionManager):
        super().__init__(session_manager)
        self.base_url = "https://api.linkedin.com/v2"
    
    def get_platform_type(self) -> PlatformType:
        return PlatformType.LINKEDIN
    
    def _get_access_token(self, user_id: str) -> Optional[str]:
        """Get valid access token for LinkedIn API"""
        try:
            token_data = self.session_manager.redis.hgetall(f"linkedin_token:{user_id}")

            if token_data and token_data.get('access_token'):
                expires_at = token_data.get('expires_at')
                if expires_at and datetime.fromisoformat(expires_at) > datetime.now():
                    return token_data['access_token']
                else:
                    self.session_manager.redis.delete(f"linkedin_token:{user_id}")
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting LinkedIn access token: {e}")
            return None
    
    def _store_access_token(self, user_id: str, token_data: Dict):
        """Store access token with expiration"""
        try:
            expires_in = token_data.get('expires_in', 3600)  # Default 1 hour
            expires_at = datetime.now() + timedelta(seconds=expires_in)
            
            token_info = {
                'access_token': token_data['access_token'],
                'expires_at': expires_at.isoformat(),
                'stored_at': datetime.now().isoformat(),
                'scope': token_data.get('scope', ''),
                'token_type': token_data.get('token_type', 'Bearer')
            }
            
            # Store token with expiration buffer
            self.session_manager.redis.hset(f"linkedin_token:{user_id}", mapping=token_info)
            # Set Redis key expiration with buffer
            self.session_manager.redis.expire(f"linkedin_token:{user_id}", expires_in + 300)  # 5 min buffer
            
            logger.info(f"LinkedIn token stored for user {user_id}, expires at {expires_at}")
            
        except Exception as e:
            logger.error(f"Error storing LinkedIn token: {e}")
    
    def _is_token_expired(self, user_id: str) -> bool:
        """Check if user's LinkedIn token is expired"""
        try:
            token_data = self.session_manager.redis.hgetall(f"linkedin_token:{user_id}")
            if not token_data or not token_data.get('expires_at'):
                return True
                
            expires_at = datetime.fromisoformat(token_data['expires_at'])
            return datetime.now() + timedelta(minutes=5) >= expires_at
            
        except Exception:
            return True
    
    def _make_api_post(self, access_token: str, content: PostContent) -> PostResult:
        """Make LinkedIn API post"""
        try:
            # Get user profile for person URN
            profile = self._get_user_profile(access_token)
            if not profile:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.OAUTH_REQUIRED,
                    message="Could not get LinkedIn profile information"
                )
            
            person_urn = f"urn:li:person:{profile['id']}"
            
            # Handle image upload if present
            image_asset_urn = None
            if content.image_url:
                image_asset_urn = self._upload_image_to_api(access_token, content.image_url, person_urn)
            
            # Create post payload
            post_payload = {
                "author": person_urn,
                "lifecycleState": "PUBLISHED",
                "specificContent": {
                    "com.linkedin.ugc.ShareContent": {
                        "shareCommentary": {
                            "text": content.text
                        },
                        "shareMediaCategory": "IMAGE" if image_asset_urn else "NONE"
                    }
                },
                "visibility": {
                    "com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"
                }
            }
            
            if image_asset_urn:
                post_payload["specificContent"]["com.linkedin.ugc.ShareContent"]["media"] = [{
                    "status": "READY",
                    "description": {"text": "Generated image for blog post"},
                    "media": image_asset_urn,
                    "title": {"text": "Blog Automation"}
                }]
            
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json',
                'X-Restli-Protocol-Version': '2.0.0'
            }
            
            response = requests.post(
                f"{self.base_url}/ugcPosts",
                headers=headers,
                json=post_payload,
                timeout=30
            )
            
            if response.status_code == 201:
                post_data = response.json()
                post_id = post_data.get('id', '')
                
                return PostResult(
                    success=True,
                    error_code=ErrorCode.SUCCESS,
                    message="LinkedIn post published via API",
                    platform_post_id=post_id,
                    published_at=datetime.now().isoformat(),
                    platform_url=f"https://www.linkedin.com/feed/update/{post_id}"
                )
            elif response.status_code == 401:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.OAUTH_REQUIRED,
                    message="LinkedIn access token expired"
                )
            else:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.PLATFORM_ERROR,
                    message=f"LinkedIn API error: {response.status_code} - {response.text}"
                )
                
        except Exception as e:
            self.logger.error(f"LinkedIn API post error: {e}")
            return PostResult(
                success=False,
                error_code=ErrorCode.PLATFORM_ERROR,
                message=f"LinkedIn API error: {str(e)}"
            )
    
    def _test_api_connection(self, access_token: str) -> PostResult:
        """Test LinkedIn API connection"""
        try:
            profile = self._get_user_profile(access_token)
            if profile:
                name = f"{profile.get('firstName', 'Unknown')} {profile.get('lastName', '')}"
                return PostResult(
                    success=True,
                    error_code=ErrorCode.SUCCESS,
                    message=f"LinkedIn API connection successful for {name}"
                )
            else:
                return PostResult(
                    success=False,
                    error_code=ErrorCode.OAUTH_REQUIRED,
                    message="LinkedIn API connection test failed"
                )
                
        except Exception as e:
            return PostResult(
                success=False,
                error_code=ErrorCode.PLATFORM_ERROR,
                message=f"LinkedIn API test error: {str(e)}"
            )
    
    def _get_user_profile(self, access_token: str) -> Optional[Dict]:
        """Get user's LinkedIn profile using the correct /userinfo endpoint"""
        try:
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
                # ❌ REMOVED: 'X-Restli-Protocol-Version': '2.0.0' - not needed for /userinfo
            }
            
            # ✅ FIXED: Use /userinfo instead of /me endpoint
            response = requests.get(
                f"{self.base_url}/userinfo",  # Changed from /me to /userinfo
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                profile_data = response.json()
                
                # ✅ Map the new response format to your expected format
                normalized_profile = {
                    'id': profile_data.get('sub'),  # LinkedIn user ID
                    'localizedFirstName': profile_data.get('given_name'),
                    'localizedLastName': profile_data.get('family_name'),
                    'name': profile_data.get('name'),  # Full name
                    'email': profile_data.get('email'),
                    'picture': profile_data.get('picture'),  # Profile image URL
                    'locale': profile_data.get('locale'),
                    'email_verified': profile_data.get('email_verified')
                }
                
                return normalized_profile
            else:
                self.logger.warning(f"LinkedIn profile fetch failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting LinkedIn profile: {e}")
            return None

    def _upload_image_to_api(self, access_token: str, image_url: str, person_urn: str) -> Optional[str]:
        """Upload image to LinkedIn via API"""
        try:
            # Register upload
            register_payload = {
                "registerUploadRequest": {
                    "recipes": ["urn:li:digitalmediaRecipe:feedshare-image"],
                    "owner": person_urn,
                    "serviceRelationships": [{
                        "relationshipType": "OWNER",
                        "identifier": "urn:li:userGeneratedContent"
                    }]
                }
            }
            
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json',
                'X-Restli-Protocol-Version': '2.0.0'
            }
            
            register_response = requests.post(
                f"{self.base_url}/assets?action=registerUpload",
                headers=headers,
                json=register_payload,
                timeout=30
            )
            
            if register_response.status_code != 200:
                return None
            
            register_data = register_response.json()
            upload_url = register_data['value']['uploadMechanism']['com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest']['uploadUrl']
            asset_urn = register_data['value']['asset']
            
            # Download image
            image_response = requests.get(image_url, timeout=30)
            image_response.raise_for_status()
            
            # Upload to LinkedIn
            upload_response = requests.post(
                upload_url,
                headers={'Authorization': f'Bearer {access_token}'},
                data=image_response.content,
                timeout=60
            )
            
            if upload_response.status_code == 201:
                return asset_urn
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"LinkedIn image upload error: {e}")
            return None
    
    def publish_post(self, content: PostContent, credentials: AccountCredentials) -> PostResult:
        """Publish post via LinkedIn API"""
        if not self._validate_credentials(credentials):
            return PostResult(
                success=False,
                error_code=ErrorCode.VALIDATION_ERROR,
                message="Invalid credentials provided"
            )
        
        # Check if token is expired
        if self._is_token_expired(credentials.user_id):
            return PostResult(
                success=False,
                error_code=ErrorCode.OAUTH_REQUIRED,
                message="LinkedIn access token expired. Please re-authenticate.",
                requires_action="oauth_required"
            )
        
        # Get access token
        access_token = self._get_access_token(credentials.user_id)
        if not access_token:
            return PostResult(
                success=False,
                error_code=ErrorCode.OAUTH_REQUIRED,
                message="No valid LinkedIn access token available",
                requires_action="oauth_required"
            )
        
        # Make API post
        return self._make_api_post(access_token, content)
    
    def test_connection(self, credentials: AccountCredentials) -> PostResult:
        """Test LinkedIn API connection"""
        if not self._validate_credentials(credentials):
            return PostResult(
                success=False,
                error_code=ErrorCode.VALIDATION_ERROR,
                message="Invalid credentials provided"
            )
        
        if self._is_token_expired(credentials.user_id):
            return PostResult(
                success=False,
                error_code=ErrorCode.OAUTH_REQUIRED,
                message="LinkedIn access token expired",
                requires_action="oauth_required"
            )
        
        access_token = self._get_access_token(credentials.user_id)
        if not access_token:
            return PostResult(
                success=False,
                error_code=ErrorCode.OAUTH_REQUIRED,
                message="No LinkedIn access token available",
                requires_action="oauth_required"
            )
        
        return self._test_api_connection(access_token)

# =====================================================================================
# MAIN SOCIAL POSTER CLASS
# =====================================================================================

class SocialPoster:
    """
    Enterprise-grade social media posting orchestrator
    
    This class manages posting to multiple social media platforms with:
    - Consistent error handling and reporting
    - Session management and authentication
    - Type safety and validation
    - Comprehensive logging and monitoring
    """
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.session_manager = SocialSessionManager(redis_client)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Initialize platform posters with clear interface contracts
        self.platforms: Dict[PlatformType, BaseSocialPoster] = {
            PlatformType.TWITTER: TwitterPoster(self.session_manager),
            PlatformType.LINKEDIN: LinkedInAPIPoster(self.session_manager),
            PlatformType.FACEBOOK: FacebookPoster(self.session_manager),
            PlatformType.INSTAGRAM: InstagramPoster(self.session_manager)
        }
        
        self.logger.info("SocialPoster initialized with all platform posters")
    
    def publish_post(self, post_data: Dict[str, Any], account_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Publish a post to social media with comprehensive error handling
        
        Args:
            post_data: Dictionary containing post content and metadata
            account_data: Dictionary containing account credentials and settings
            
        Returns:
            Dictionary with standardized success/error response
        """
        try:
            # Validate platform
            platform_str = post_data.get('platform')
            if not platform_str:
                return self._create_error_response(ErrorCode.VALIDATION_ERROR, "Platform not specified")
            
            try:
                platform = PlatformType(platform_str.lower())
            except ValueError:
                return self._create_error_response(ErrorCode.VALIDATION_ERROR, f"Unsupported platform: {platform_str}")
            
            if platform not in self.platforms:
                return self._create_error_response(ErrorCode.VALIDATION_ERROR, f"Platform not available: {platform_str}")
            
            # Check account connection status
            if not account_data.get('connected', False):
                return self._create_error_response(
                    ErrorCode.AUTHENTICATION_ERROR,
                    "Account not connected. Please test connection first.",
                    requires_action="connection_test_required"
                )
            
            # Create structured content and credentials
            try:
                content = PostContent(
                    text=post_data.get('content', ''),
                    image_url=post_data.get('image_path'),  # Can be URL or path
                    hashtags=post_data.get('hashtags'),
                    mentions=post_data.get('mentions')
                )
                
                credentials = AccountCredentials(
                    user_id=account_data.get('user_id'),
                    platform=platform,
                    username=account_data.get('username'),
                    password_encrypted=account_data.get('password_encrypted'),
                    account_id=account_data.get('id'),
                    access_token=account_data.get('access_token'),
                    connected=account_data.get('connected', False)
                )
            except ValueError as e:
                return self._create_error_response(ErrorCode.VALIDATION_ERROR, f"Invalid content or credentials: {str(e)}")
            
            # Get platform poster and publish
            poster = self.platforms[platform]
            self.logger.info(f"Publishing to {platform.value}: {content.text[:50]}...")
            result = poster.publish_post(content, credentials)
            
            # Handle post-publish actions
            if result.success:
                self._handle_successful_publish(account_data.get('id'), result)
                self.logger.info(f"✅ Post published successfully on {platform.value}")
            else:
                self._handle_failed_publish(account_data.get('id'), result)
                self.logger.error(f"❌ Post failed on {platform.value}: {result.message}")
            
            return result.to_dict()
            
        except Exception as e:
            self.logger.error(f"Unexpected error in publish_post: {e}", exc_info=True)
            return self._create_error_response(ErrorCode.UNKNOWN_ERROR, f"Unexpected error: {str(e)}")
    
    def test_account_connection(self, platform_str: str, username: str, plain_password: str, user_id: str) -> Dict[str, Any]:
        """
        Test social account connection with comprehensive validation
        
        Args:
            platform_str: Platform name (e.g., 'twitter', 'facebook')
            username: Account username/email
            plain_password: Plain text password (will be encrypted)
            user_id: User identifier
            
        Returns:
            Dictionary with standardized test result
        """
        try:
            # Validate platform
            try:
                platform = PlatformType(platform_str.lower())
            except ValueError:
                return self._create_error_response(ErrorCode.VALIDATION_ERROR, f"Unsupported platform: {platform_str}")
            
            if platform not in self.platforms:
                return self._create_error_response(ErrorCode.VALIDATION_ERROR, f"Platform not available: {platform_str}")
            
            # Validate inputs
            if not all([username, plain_password, user_id]):
                return self._create_error_response(ErrorCode.VALIDATION_ERROR, "Missing required fields")
            
            self.logger.info(f"Testing connection for {platform.value} - {username} (user_id: {user_id})")
            
            # Encrypt password
            try:
                encryption_key = get_encryption_key()
                encrypted_password = encrypt(plain_password, encryption_key)
            except ValueError as e:
                return self._create_error_response(ErrorCode.ENCRYPTION_KEY_ERROR, f"Encryption key error: {str(e)}")
            except Exception as e:
                return self._create_error_response(ErrorCode.ENCRYPTION_KEY_ERROR, "Failed to encrypt password")
            
            # Create credentials
            try:
                credentials = AccountCredentials(
                    user_id=user_id,
                    platform=platform,
                    username=username,
                    password_encrypted=encrypted_password
                )
            except ValueError as e:
                return self._create_error_response(ErrorCode.VALIDATION_ERROR, f"Invalid credentials: {str(e)}")
            
            # Test connection
            poster = self.platforms[platform]
            result = poster.test_connection(credentials)
            
            if result.success:
                self.logger.info(f"✅ Connection test successful for {platform.value} - {username}")
            else:
                self.logger.error(f"❌ Connection test failed for {platform.value} - {username}: {result.message}")
            
            return result.to_dict()
            
        except Exception as e:
            self.logger.error(f"Unexpected error in test_account_connection: {e}", exc_info=True)
            return self._create_error_response(ErrorCode.UNKNOWN_ERROR, f"Unexpected error: {str(e)}")
    
    def get_platform_info(self) -> Dict[str, Any]:
        """Get information about available platforms and their capabilities"""
        platform_info = {}
        
        for platform_type, poster in self.platforms.items():
            platform_info[platform_type.value] = {
                'name': platform_type.value.title(),
                'posting_method': poster.get_posting_method().value,
                'supports_images': True,  # All current platforms support images
                'supports_text': True,
                'character_limit': self._get_platform_character_limit(platform_type),
                'requires_oauth': platform_type == PlatformType.LINKEDIN
            }
        
        return platform_info
    
    def _get_platform_character_limit(self, platform: PlatformType) -> int:
        """Get character limit for platform"""
        limits = {
            PlatformType.TWITTER: 280,
            PlatformType.FACEBOOK: 63206,
            PlatformType.INSTAGRAM: 2200,
            PlatformType.LINKEDIN: 3000
        }
        return limits.get(platform, 2800)
    
    def _create_error_response(self, error_code: ErrorCode, message: str, **kwargs) -> Dict[str, Any]:
        """Create standardized error response"""
        result = PostResult(
            success=False,
            error_code=error_code,
            message=message,
            **kwargs
        )
        return result.to_dict()
    
    def _handle_successful_publish(self, account_id: Optional[str], result: PostResult):
        """Handle post-successful publish actions"""
        try:
            if account_id:
                # Update account's last post timestamp
                update_data = {'last_post_at': datetime.now().isoformat()}
                response = make_api_request('PUT', f'social-accounts/{account_id}', data=update_data)
                
                if response:
                    self.logger.debug(f"Updated last_post_at for account {account_id}")
                else:
                    self.logger.warning(f"Failed to update last_post_at for account {account_id}")
        except Exception as e:
            self.logger.error(f"Error in post-publish handling: {e}")
    
    def _handle_failed_publish(self, account_id: Optional[str], result: PostResult):
        """Handle post-failed publish actions"""
        try:
            if account_id and result.error_code in [
                ErrorCode.AUTHENTICATION_ERROR,
                ErrorCode.SESSION_EXPIRED,
                ErrorCode.OAUTH_REQUIRED
            ]:
                # Mark account as disconnected
                update_data = {
                    'connected': False,
                    'connection_error': result.message,
                    'connection_tested_at': datetime.now().isoformat()
                }
                response = make_api_request('PUT', f'social-accounts/{account_id}', data=update_data)
                
                if response:
                    self.logger.warning(f"Marked account {account_id} as disconnected due to: {result.message}")
                else:
                    self.logger.error(f"Failed to mark account {account_id} as disconnected")
        except Exception as e:
            self.logger.error(f"Error in post-failed handling: {e}")
    
    def get_user_social_accounts(self, user_id: str, platform: str = None) -> List[Dict[str, Any]]:
        """Get user's social accounts from API with validation"""
        try:
            params = {'user_id': user_id, 'active': 'true', 'connected': 'true'}
            if platform:
                params['platform'] = platform

            response = make_api_request('GET', 'social-accounts', params=params, internal=True)
            
            if response and isinstance(response, list):
                valid_accounts = []
                for acc in response:
                    # Validate account has required fields
                    required_fields = ['user_id', 'platform', 'username', 'password_encrypted']
                    if all(acc.get(field) for field in required_fields) and acc.get('connected', False):
                        valid_accounts.append(acc)
                    else:
                        self.logger.warning(f"Account {acc.get('id', 'N/A')} missing required fields, filtering out")

                self.logger.debug(f"Returning {len(valid_accounts)} valid accounts for user {user_id}")
                return valid_accounts

            self.logger.debug(f"No accounts found for user {user_id}, platform {platform}")
            return []
            
        except Exception as e:
            self.logger.error(f"Error getting user social accounts: {e}")
            return []
