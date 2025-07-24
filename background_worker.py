# Final Enhanced User ID Handling - All Issues Resolved

import asyncio
import json
import logging
import os
import sys
import time
import threading
import signal
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import redis
import schedule

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('background_worker.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Import automation components
try:
    from automation.blog_monitor import BlogMonitor
    from integrations.content_processor import ContentProcessor
    from integrations.social_poster import SocialPoster
    from integrations.utils.api_client import make_api_request
    logger.info("✅ All automation modules imported successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import automation modules: {e}")
    sys.exit(1)

class UnifiedBackgroundWorker:
    def __init__(self):
        # Initialize Redis
        try:
            self.redis = redis.Redis(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                password=os.getenv('REDIS_PASSWORD', ''),
                db=0,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=30,
                socket_keepalive=True,
                socket_keepalive_options={},
                retry_on_timeout=True,
                health_check_interval=30
            )
            self.redis.ping()
            logger.info("✅ Connected to Redis")
        except redis.ConnectionError as e:
            logger.error(f"❌ Redis connection failed: {e}")
            raise

        # Initialize automation components
        self.blog_monitor = BlogMonitor(self.redis)
        self.content_processor = ContentProcessor()
        self.social_poster = SocialPoster(self.redis)

        # Control flags
        self.running = True
        self.paused = False
        
        # Worker threads
        self.threads = []
        
        # Statistics
        self.stats = {
            'start_time': datetime.now().isoformat(),
            'blogs_checked': 0,
            'posts_discovered': 0,
            'posts_generated': 0,
            'posts_published': 0,
            'errors': 0,
            'last_heartbeat': None,
            'last_queue_log': None
        }

        # Queue names
        self.queues = {
            'blog_monitoring': 'blog_monitoring_queue',
            'content_processing': 'content_processing_queue',
            'publishing': 'publishing_queue'
        }

    def start(self):
        """Start the unified background worker"""
        logger.info("🚀 Starting Unified Background Worker")
        logger.info("Powered by fastApppy & Limiai")
        logger.info("=" * 60)

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            # Start worker threads
            self._start_worker_threads()

            # Start scheduled tasks
            self._start_scheduler()

            # Main loop with heartbeat
            self._main_loop()

        except Exception as e:
            logger.error(f"❌ Worker startup failed: {e}")
            self.stop()

    def _start_worker_threads(self):
        """Start all worker threads"""
        workers = [
            ('Blog Monitor Queue', self._blog_monitoring_worker),
            ('Content Processing Queue', self._content_processing_worker),
            ('Publishing Queue', self._publishing_worker),
            ('Control Signal Handler', self._control_signal_worker)
        ]

        for name, worker_func in workers:
            thread = threading.Thread(target=worker_func, name=name, daemon=True)
            thread.start()
            self.threads.append(thread)
            logger.info(f"✅ Started {name} worker")

    def _start_scheduler(self):
        """Start scheduled tasks"""
        # Schedule regular blog monitoring every 30 minutes
        schedule.every(30).minutes.do(self._schedule_blog_monitoring)
        
        # Schedule cleanup tasks
        schedule.every(1).hours.do(self._schedule_cleanup)
        
        # Schedule stats logging - Updated to use new method signature
        schedule.every(15).minutes.do(lambda: self._log_stats())  # Overall stats
        
        # Optional: Schedule user-specific stats every hour
        schedule.every(1).hours.do(self._log_stats_for_active_users)

        # Start scheduler thread
        scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        scheduler_thread.start()
        self.threads.append(scheduler_thread)
        logger.info("✅ Started scheduler thread")
        
        # Run initial blog monitoring immediately
        threading.Thread(target=self._run_initial_blog_check, daemon=True).start()

    def _main_loop(self):
        """Main worker loop with heartbeat"""
        logger.info("✅ All workers started successfully")
        
        while self.running:
            try:
                # Send heartbeat
                self._send_heartbeat()
                
                # Check for pause/resume signals
                if self.paused:
                    logger.info("⏸️  Worker is paused")
                    time.sleep(10)
                    continue
                
                # Sleep for heartbeat interval
                time.sleep(60)  # 1 minute heartbeat
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                self.stats['errors'] += 1
                time.sleep(10)

    def _blog_monitoring_worker(self):
        """Worker for processing blog monitoring queue"""
        logger.info("📡 Blog monitoring worker started")
        
        while self.running:
            try:
                if self.paused:
                    time.sleep(10)
                    continue

                # Get task from queue (blocking with timeout)
                result = self._safe_redis_brpop(self.queues['blog_monitoring'], timeout=30)
                
                if result:
                    _, task_json = result
                    task = json.loads(task_json)
                    
                    logger.info(f"Processing blog monitoring task: {task.get('type', 'unknown')}")
                    
                    self._process_blog_monitoring_task(task)
                
                # Log queue status occasionally (every 5 minutes)
                self._log_queue_status_if_needed('blog_monitoring_queue')
                    
            except redis.ConnectionError as e:
                logger.error(f"Redis connection error in blog monitoring worker: {e}")
                self.stats['errors'] += 1
                time.sleep(30)  # Wait longer for connection issues
            except redis.TimeoutError:
                # Timeouts are normal when there are no tasks - don't log as error
                logger.debug("Blog monitoring queue timeout (normal - no tasks)")
                continue
            except Exception as e:
                logger.error(f"Error in blog monitoring worker: {e}")
                self.stats['errors'] += 1
                time.sleep(10)

    def _content_processing_worker(self):
        """Worker for processing content generation queue"""
        logger.info("⚙️ Content processing worker started")
        
        while self.running:
            try:
                if self.paused:
                    time.sleep(10)
                    continue

                # Get task from queue
                result = self._safe_redis_brpop(self.queues['content_processing'], timeout=30)
                if result:
                    queue_name, task_json = result
                    task = json.loads(task_json)
                    
                    logger.info(f"Processing content task: {task.get('type', 'unknown')}")
                    
                    self._process_content_task(task)
                
                # Log queue status occasionally
                self._log_queue_status_if_needed('content_processing_queue')
                    
            except redis.ConnectionError as e:
                logger.error(f"Redis connection error in content processing worker: {e}")
                self.stats['errors'] += 1
                time.sleep(30)
            except redis.TimeoutError:
                # Timeouts are normal when there are no tasks - don't log as error
                logger.debug("Content processing queue timeout (normal - no tasks)")
                continue
            except Exception as e:
                logger.error(f"Error in content processing worker: {e}")
                self.stats['errors'] += 1
                time.sleep(10)

    def _publishing_worker(self):
        """Worker for processing publishing queue with proper scheduling"""
        logger.info("📤 Publishing worker started")
        
        while self.running:
            try:
                if self.paused:
                    time.sleep(10)
                    continue

                # First, check for scheduled posts that are now ready
                self._check_pending_posts()

                # Then process immediate publishing queue
                result = self._safe_redis_brpop(self.queues['publishing'], timeout=10)
                
                if result:
                    queue_name, task_json = result
                    task = json.loads(task_json)
                    
                    logger.info(f"Processing publishing task: {task.get('type', 'unknown')}")
                    
                    # Process the task (this should now only contain ready-to-publish posts)
                    self._process_publishing_task_immediate(task)
                
                # Log queue status occasionally
                self._log_queue_status_if_needed('publishing_queue')
                    
            except redis.ConnectionError as e:
                logger.error(f"Redis connection error in publishing worker: {e}")
                self.stats['errors'] += 1
                time.sleep(30)
            except redis.TimeoutError:
                # Timeouts are normal when there are no tasks - don't log as error
                logger.debug("Publishing queue timeout (normal - no tasks)")
                continue
            except Exception as e:
                logger.error(f"Error in publishing worker: {e}")
                self.stats['errors'] += 1
                time.sleep(10)

    def _control_signal_worker(self):
        """Worker for handling control signals from API"""
        logger.info("🎛️ Control signal worker started")
        
        while self.running:
            try:
                # Check for control signals
                control_signal = self.redis.get('worker:control')
                
                if control_signal:
                    signal_data = json.loads(control_signal)
                    action = signal_data.get('action')
                    
                    logger.info(f"Received control signal: {action}")
                    
                    if action == 'pause':
                        self.paused = True
                        logger.info("⏸️  Worker paused")
                    elif action == 'resume':
                        self.paused = False
                        logger.info("▶️  Worker resumed")
                    elif action == 'restart':
                        logger.info("🔄 Worker restart requested")
                        self.stop()
                        break
                    
                    # Clear the signal
                    self.redis.delete('worker:control')
                
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error in control signal worker: {e}")
                time.sleep(10)

    def _check_pending_posts(self):
        """Check pending posts queue and move ready posts to publishing queue"""
        try:
            current_timestamp = datetime.now().timestamp()
            
            # Get all posts that are ready to publish (score <= current timestamp)
            ready_posts = self._safe_redis_operation(
                self.redis.zrangebyscore,
                'pending_posts_queue',
                0,
                current_timestamp,
                withscores=True
            )
            
            if ready_posts:
                logger.info(f"Found {len(ready_posts)} posts ready to publish")
                
                for post_json, scheduled_timestamp in ready_posts:
                    try:
                        # Move to immediate publishing queue
                        self._safe_redis_operation(
                            self.redis.lpush,
                            self.queues['publishing'],
                            post_json
                        )
                        
                        # Remove from pending queue
                        self._safe_redis_operation(
                            self.redis.zrem,
                            'pending_posts_queue',
                            post_json
                        )
                        
                        # Parse the task to get post info for logging
                        task = json.loads(post_json)
                        post_data = task.get('post_data', {})
                        platform = post_data.get('platform', 'unknown')
                        
                        logger.info(f"✅ Moved {platform} post to publishing queue (was scheduled for {datetime.fromtimestamp(scheduled_timestamp)})")
                        
                    except Exception as e:
                        logger.error(f"Error moving pending post to publishing queue: {e}")
                        
        except Exception as e:
            logger.error(f"Error checking pending posts: {e}")

    def _schedule_blog_monitoring(self):
        """Schedule blog monitoring tasks for each user, prioritizing pending posts."""
        try:
            logger.info("📅 Starting scheduled blog monitoring process...")
            
            # Get all users who have blog monitors (active or inactive)
            # This gives us a comprehensive list of users to check
            all_users_response = make_api_request('GET', 'blog-monitors', params={'has_blog_monitors': 'true'})
            
            if not all_users_response or not isinstance(all_users_response, list):
                logger.info("No users with blog monitors found.")
                return

            logger.info(f"Found {len(all_users_response)} users with blog monitors. Processing each user...")
            
            for user_data in all_users_response:
                if not isinstance(user_data, dict):
                    continue
                    
                user_id = user_data.get('id') or user_data.get('user_id')
                if not user_id:
                    continue
                    
                # Check if enough time has passed since the user's last check
                last_check_key = f"user:{user_id}:last_scheduled_blog_check_time"
                last_check_time_str = self._safe_redis_operation(self.redis.get, last_check_key)

                user_settings = self._get_user_settings(user_id)
                check_interval_minutes = user_settings.get('blog_check_interval_minutes', 60)

                if last_check_time_str:
                    last_check_time = datetime.fromisoformat(last_check_time_str)
                    if datetime.now() - last_check_time < timedelta(minutes=check_interval_minutes):
                        logger.debug(f"Skipping scheduled check for user {user_id}, interval not yet passed.")
                        continue

                # Create task for this user
                task = {
                    'type': 'user_scheduled_blog_check',
                    'user_id': user_id,
                    'created_at': datetime.now().isoformat(),
                    'priority': 'normal'
                }
                
                self._safe_redis_operation(
                    self.redis.lpush,
                    self.queues['blog_monitoring'],
                    json.dumps(task)
                )
                
                # Update last check time
                self._safe_redis_operation(
                    self.redis.set, 
                    last_check_key, 
                    datetime.now().isoformat(), 
                    ex=int(timedelta(days=2).total_seconds())
                )
                
                logger.info(f"📅 Enqueued user-specific blog check for user_id: {user_id}")

            logger.info("✅ Finished enqueuing user-specific scheduled blog checks.")

        except Exception as e:
            logger.error(f"Error in _schedule_blog_monitoring: {e}", exc_info=True)

    def _schedule_cleanup(self):
        """Schedule cleanup tasks"""
        try:
            # Clean up old queue items
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            for queue_name in self.queues.values():
                queue_length = self.redis.llen(queue_name)
                if queue_length > 1000:  # If queue is too long, clean old items
                    self.redis.ltrim(queue_name, 0, 500)  # Keep only 500 most recent
                    logger.info(f"Cleaned up queue: {queue_name}")
            
            # Clean up old session data
            session_keys = self.redis.keys('session:*')
            cleaned_sessions = 0
            
            for key in session_keys:
                session_data = self.redis.hgetall(key)
                if session_data.get('saved_at'):
                    try:
                        saved_at = datetime.fromisoformat(session_data['saved_at'])
                        if datetime.now() - saved_at > timedelta(days=7):
                            self.redis.delete(key)
                            cleaned_sessions += 1
                    except:
                        continue
            
            if cleaned_sessions > 0:
                logger.info(f"🧹 Cleaned up {cleaned_sessions} old sessions")
                
        except Exception as e:
            logger.error(f"Error in cleanup: {e}")

    def _run_scheduler(self):
        """Run the scheduled tasks"""
        while self.running:
            try:
                if not self.paused:
                    schedule.run_pending()
                time.sleep(30)
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                time.sleep(60)

    def _send_heartbeat(self):
        """Send heartbeat to indicate worker is alive"""
        try:
            heartbeat_time = datetime.now().isoformat()
            self.stats['last_heartbeat'] = heartbeat_time
            
            # Store heartbeat in Redis (convert datetime to string)
            self.redis.set('worker:heartbeat', heartbeat_time, ex=300)
            
            # Store stats (ensure all values are JSON serializable)
            stats_for_redis = {}
            for key, value in self.stats.items():
                if isinstance(value, datetime):
                    stats_for_redis[key] = value.isoformat()
                else:
                    stats_for_redis[key] = str(value)
            
            self.redis.hset('worker:stats', mapping=stats_for_redis)
            
        except redis.ConnectionError as e:
            logger.error(f"Redis connection error during heartbeat: {e}")
        except redis.TimeoutError as e:
            logger.error(f"Redis timeout during heartbeat: {e}")
        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()

    def _log_queue_status_if_needed(self, queue_name: str):
        """Log queue status only occasionally to reduce spam"""
        try:
            current_time = datetime.now()
            last_log_time = self.stats.get('last_queue_log')
            
            # Only log every 5 minutes
            if not last_log_time or (current_time - datetime.fromisoformat(last_log_time)).total_seconds() > 300:
                queue_size = self._safe_redis_operation(self.redis.llen, queue_name) or 0
                if queue_size > 0:
                    logger.info(f"Queue '{queue_name}' has {queue_size} items")
                
                self.stats['last_queue_log'] = current_time.isoformat()
                
        except Exception as e:
            logger.debug(f"Error logging queue status: {e}")

    def _run_initial_blog_check(self):
        """Run initial blog monitoring immediately after startup"""
        try:
            logger.info("🔄 Running initial blog monitoring check...")
            time.sleep(5)  # Give other workers time to start
            
            # Schedule initial blog check
            task = {
                'type': 'scheduled_blog_check', # This will now iterate through users
                'created_at': datetime.now().isoformat(),
                'priority': 'high',
                'initial_run': True # This flag helps identify it as an initial run
            }
            
            self._safe_redis_operation(
                self.redis.lpush,
                self.queues['blog_monitoring'], 
                json.dumps(task)
            )
            
            logger.info("✅ Initial (all users) blog check (type: scheduled_blog_check) scheduled successfully. It will be processed user by user.")
            
        except Exception as e:
            logger.error(f"Error scheduling initial blog check: {e}")

    def stop(self):
        """Stop the worker gracefully"""
        logger.info("🛑 Stopping background worker...")
        self.running = False
        
        # Wait for threads to finish
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=5)
        
        # Clear heartbeat
        try:
            self.redis.delete('worker:heartbeat')
        except:
            pass
        
        logger.info("✅ Background worker stopped successfully")
    
    def _validate_user_exists(self, user_id: str) -> bool:
        """Validate that user exists and is active"""
        try:
            if not user_id or user_id == 'system':
                return True
                
            # Check if user exists in Redis cache first
            user_cache_key = f"user:{user_id}"
            cached_user = self.redis.get(user_cache_key)
            
            if cached_user:
                try:
                    user_data = json.loads(cached_user)
                    return isinstance(user_data, dict) and user_data.get('active', False)
                except (json.JSONDecodeError, TypeError):
                    # Invalid cached data, remove it
                    self.redis.delete(user_cache_key)
            
            # Fallback to API check
            response = make_api_request('GET', f'users/{user_id}')
            if response and isinstance(response, dict) and response.get('active'):
                # Cache for 5 minutes - ensure it's JSON serializable
                cache_data = {
                    'active': response.get('active', False),
                    'user_id': user_id,
                    'cached_at': datetime.now().isoformat()
                }
                self.redis.setex(user_cache_key, 300, json.dumps(cache_data))
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Error validating user {user_id}: {e}")
            return False
    
    def _get_user_settings(self, user_id: str) -> Dict[str, Any]:
        """Get user settings with caching and validation"""
        try:
            if not user_id or user_id == 'system':
                return self._get_default_settings()
                
            # Check cache first
            settings_cache_key = f"user_settings:{user_id}"
            cached_settings = self.redis.get(settings_cache_key)
            
            if cached_settings:
                try:
                    settings = json.loads(cached_settings)
                    if isinstance(settings, dict):
                        return settings
                    else:
                        # Invalid cached data, remove it
                        self.redis.delete(settings_cache_key)
                except (json.JSONDecodeError, TypeError):
                    self.redis.delete(settings_cache_key)
            
            # Fetch settings from API
            response = make_api_request('GET', f'settings/{user_id}')
            
            # CRITICAL: Also fetch connected social accounts
            connected_platforms = []
            try:
                accounts_response = make_api_request(
                    'GET', 
                    'social-accounts', 
                    params={
                        'user_id': user_id,
                        'active': 'true',
                        'connected': 'true'
                    }
                )
                
                if accounts_response and isinstance(accounts_response, list):
                    connected_platforms = [acc['platform'].lower() for acc in accounts_response 
                                        if isinstance(acc, dict) and acc.get('platform')]
                    logger.info(f"Found connected platforms for user {user_id}: {connected_platforms}")
            except Exception as e:
                logger.warning(f"Could not fetch connected accounts for user {user_id}: {e}")
            
            if response and isinstance(response, dict):
                # Add connected platforms to the response
                response['_connected_platforms'] = connected_platforms
                
                # Validate and sanitize settings before caching
                validated_settings = self._validate_user_settings(response)
                
                # Cache for 10 minutes
                try:
                    self.redis.setex(settings_cache_key, 600, json.dumps(validated_settings))
                except (TypeError, ValueError) as e:
                    logger.warning(f"Could not cache user settings for {user_id}: {e}")
                
                return validated_settings
                
            # Return default settings if API fails
            logger.warning(f"Could not fetch settings for user {user_id}, using defaults with connected platforms")
            default_settings = self._get_default_settings()
            
            # Even with defaults, enable connected platforms
            for platform in connected_platforms:
                if platform in default_settings['platforms']:
                    default_settings['platforms'][platform]['enabled'] = True
            
            return default_settings
            
        except Exception as e:
            logger.error(f"Error getting user settings for {user_id}: {e}")
            return self._get_default_settings()

    def _validate_user_settings(self, settings: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and sanitize user settings - FIXED to handle exact API response structure"""
        try:
            # Start with defaults
            validated = self._get_default_settings()
            
            if not isinstance(settings, dict):
                return validated
            
            # Get connected platforms from the API response
            connected_platforms = settings.get('_connected_platforms', [])
            
            # Map automation settings
            if 'automation' in settings:
                automation = settings['automation']
                validated['content_generation'].update({
                    'enabled': automation.get('autoPublish', True),
                    'max_posts_per_day': automation.get('maxPostsPerDay', 3)
                })
                validated['publishing'].update({
                    'auto_publish': automation.get('autoPublish', True)
                })
                # Enable images if specified
                if 'includeImages' in automation:
                    validated['include_images'] = automation['includeImages']
            
            # Map content settings  
            if 'content' in settings:
                content = settings['content']
                validated.update({
                    'max_twitter_length': content.get('maxWordCount', 280),
                    'max_linkedin_length': content.get('maxWordCount', 1500),
                    'include_emojis': content.get('includeEmojis', True),
                    'custom_prompt': content.get('customPrompt', '')
                })
            
            # Map social settings
            if 'social' in settings:
                social = settings['social'] 
                validated.update({
                    'include_hashtags': bool(social.get('defaultHashtags', '')),
                    'post_template': social.get('postTemplate', ''),
                    'schedule_delay': social.get('scheduleDelay', 30),
                    'posting_schedule': social.get('postingSchedule', 'smart_spread')
                })
            
            # Map general settings
            if 'general' in settings:
                general = settings['general']
                validated.update({
                    'tone': general.get('theme', 'professional')
                })
            
            # CRITICAL FIX: Enable platforms based on connected accounts AND autoPublish setting
            auto_publish_enabled = validated['publishing']['auto_publish']
            
            if auto_publish_enabled and connected_platforms:
                for platform in connected_platforms:
                    valid_platforms = ['twitter', 'linkedin', 'facebook', 'instagram', 'youtube', 'tiktok']
                    if platform in valid_platforms:
                        if platform not in validated['platforms']:
                            validated['platforms'][platform] = {}
                        validated['platforms'][platform]['enabled'] = True
                        logger.info(f"✅ Enabled platform {platform} for publishing (autoPublish=true + connected account)")
            else:
                logger.warning(f"Platform publishing disabled: autoPublish={auto_publish_enabled}, connected_platforms={connected_platforms}")
            
            logger.info(f"✅ Final validated settings for user {settings.get('user_id', 'unknown')}: platforms={validated['platforms']}, auto_publish={validated['publishing']['auto_publish']}")
            return validated
            
        except Exception as e:
            logger.error(f"Error validating user settings: {e}")
            return self._get_default_settings()

    def _get_default_settings(self) -> Dict[str, Any]:
        """Get default user settings"""
        return {
            'blog_monitoring': {
                'blog_check_interval_minutes': 60,
            },
            'content_generation': {
                'enabled': True,
                'tone': 'professional',
                'max_posts_per_day': 3
            },
            'publishing': {
                'auto_publish': True,
                'default_delay_minutes': 30
            },
            'platforms': {
                'twitter': {'enabled': False},
                'linkedin': {'enabled': False},
                'facebook': {'enabled': False},
                'instagram': {'enabled': False},
                'youtube': {'enabled': False},
                'tiktok': {'enabled': False}
            },
            # Additional settings from content processor
            'tone': 'professional',
            'include_hashtags': True,
            'include_emojis': True,
            'max_twitter_length': 280,
            'max_linkedin_length': 1500,
            'branding_enabled': True,
            'max_posts_per_day': 3,
            'auto_publish': True,
            'post_template': '',
            'schedule_delay': 30,
            'custom_prompt': '',
            'posting_schedule': 'smart_spread',
            'respect_posting_hours': True,
            'posting_start_hour': 8,
            'posting_end_hour': 22,
            'include_images': True,
            'image_style': 'professional',
            'brand_colors': 'professional blue and white',
            'generate_images_for_platforms': ['twitter', 'linkedin', 'facebook'],
            'image_source': 'unsplash'
        }

    def _validate_monitor_access(self, monitor_id: str, user_id: str) -> bool:
        """Validate that user has access to monitor"""
        try:
            if user_id == 'system':
                return True
                
            monitor_user_id = self._get_monitor_user_id(monitor_id)
            return monitor_user_id == user_id
            
        except Exception as e:
            logger.error(f"Error validating monitor access: {e}")
            return False
    
    def _queue_content_processing(self, post_data: Dict[str, Any], user_id: str = None, user_settings: Dict = None):
        """Enhanced content processing queue with comprehensive validation"""
        try:
            # Extract user_id from post_data if not provided
            if not user_id:
                user_id = post_data.get('user_id') or post_data.get('discovered_by_user', 'system')
            
            # Validate user_id
            if not user_id or user_id == 'system':
                user_id = 'system'
                user_settings = self._get_default_settings()
            else:

                
                # Get user settings if not provided
                if not user_settings:
                    user_settings = self._get_user_settings(user_id)
            
            # Ensure post_data is valid
            if not isinstance(post_data, dict):
                logger.error(f"Invalid post_data for user {user_id}: post_data must be a dictionary")
                return
            
            # Check if content generation is enabled for this user
            if not user_settings.get('content_generation', {}).get('enabled', True):
                logger.info(f"Content generation disabled for user {user_id}, skipping post: {post_data.get('title')}")
                return
            
            task = {
                'type': 'discovered_post',
                'post_data': post_data,
                'user_id': user_id,
                'user_settings': user_settings,
                'created_at': datetime.now().isoformat(),
                'priority': 'normal'
            }
            
            # Validate task can be serialized
            try:
                json.dumps(task)
            except (TypeError, ValueError) as e:
                logger.error(f"Task not JSON serializable for user {user_id}: {e}")
                return
            
            self._safe_redis_operation(
                self.redis.lpush, 
                self.queues['content_processing'], 
                json.dumps(task)
            )
            logger.debug(f"Queued content processing for user {user_id}: {post_data.get('title')}")
            
        except Exception as e:
            logger.error(f"Error queueing content processing for user {user_id}: {e}")
    
    def _process_publishing_task_immediate(self, task: Dict[str, Any]):
        """Enhanced publishing with comprehensive validation"""
        user_id = 'system'  # Default for error handling
        
        try:
            task_type = task.get('type')
            
            if task_type in ['scheduled_post', 'manual_publish']:
                post_data = task.get('post_data')
                
                if not post_data or not isinstance(post_data, dict):
                    logger.error("Invalid or missing post data in publishing task")
                    return
                
                user_id = post_data.get('user_id')
                platform = post_data.get('platform')
                
                # Validate required fields
                if not user_id: # Should not happen if queued correctly
                    logger.error("No user_id in post data for publishing task")
                    return
                
                if not platform: # Should not happen
                    logger.error(f"No platform specified in publishing task for user {user_id}")
                    return
                    
                # Get fresh user settings
                user_settings = self._get_user_settings(user_id)

                # Check if platform is enabled for publishing
                platform_settings = user_settings.get('platforms', {}).get(platform, {})
                if not platform_settings.get('enabled', False):
                    logger.info(f"Platform {platform} disabled for user {user_id}, skipping publish task.")
                    self._log_activity(
                        'publishing_skipped_platform_disabled',
                        f'Platform {platform} disabled, skipping publish.',
                        'info', user_id, {'platform': platform, 'post_id': post_data.get('id')}
                    )
                    return

                # Check daily post limit (especially for manual triggers, auto-queued should be pre-checked)
                if not self._can_publish_for_user(user_id, user_settings):
                    logger.info(f"Publishing limit reached for user {user_id}. Task for '{post_data.get('original_title')}' will not be processed now.")
                    self._log_activity(
                        'publishing_skipped',
                        f'Platform {platform} disabled',
                        'info',
                        user_id,
                        {'platform': platform, 'post_id': post_data.get('id')}
                    )
                    return
                
                logger.info(f"Publishing post to {platform} for user {user_id}")
                
                try:
                    accounts = self.social_poster.get_user_social_accounts(user_id, platform)
                    
                    if not accounts:
                        logger.warning(f"No {platform} account found for user {user_id}")
                        self._log_activity(
                            'publishing_error', 
                            f'No {platform} account configured', 
                            'warning', 
                            user_id,
                            {'platform': platform, 'post_id': post_data.get('id')}
                        )
                        self._update_post_status(post_data.get('id'), 'failed', {'error': 'No account configured'})
                        return
                    
                    account = accounts[0]  # Use first active account
                    
                    # Validate account is still active/valid
                    if not account.get('active', True):
                        logger.warning(f"Account for {platform} is inactive for user {user_id}")
                        self._log_activity(
                            'publishing_error',
                            f'{platform} account is inactive',
                            'warning',
                            user_id,
                            {'platform': platform, 'account_id': account.get('id')}
                        )
                        return
                    
                    # Add user context to post data
                    post_data['publishing_timestamp'] = datetime.now().isoformat()
                    post_data['account_id'] = account.get('id')
                    
                    # Publish the post
                    result = self.social_poster.publish_post(post_data, account)
                    
                    if result and result.get('success'):
                        self.stats['posts_published'] += 1
                        # Increment daily post count for the user
                        self._increment_user_daily_post_count(user_id)
                        logger.info(f"✅ Published to {platform} for user {user_id}: {post_data.get('original_title', 'Post')}")
                        
                        self._log_activity(
                            'post_published', 
                            f'Successfully published to {platform}', 
                            'success', 
                            user_id,
                            {
                                'platform': platform, 
                                'post_id': post_data.get('id'),
                                'platform_url': result.get('platform_url'),
                                'platform_post_id': result.get('platform_post_id')
                            }
                        )
                        
                        # Update post status in API
                        self._update_post_status(post_data.get('id'), 'published', result)
                    else:
                        error_msg = result.get('error', 'Unknown error') if result else 'No result returned'
                        logger.error(f"❌ Failed to publish to {platform} for user {user_id}: {error_msg}")
                        self._log_activity(
                            'publishing_error', 
                            f'Failed to publish to {platform}: {error_msg}', 
                            'error', 
                            user_id,
                            {'platform': platform, 'post_id': post_data.get('id')}
                        )
                        self._update_post_status(post_data.get('id'), 'failed', result or {'error': error_msg})
                        
                except Exception as e:
                    logger.error(f"Error during publishing for user {user_id}: {e}")
                    self._log_activity('error', f'Publishing failed: {str(e)}', 'error', user_id)
                    self._update_post_status(post_data.get('id'), 'failed', {'error': str(e)})
                    
        except Exception as e:
            logger.error(f"Error processing immediate publishing task for user {user_id}: {e}")
            self.stats['errors'] += 1
            self._log_activity('error', f'Publishing task processing failed: {str(e)}', 'error', user_id)

    def _get_user_daily_post_count(self, user_id: str) -> int:
        """Get the number of posts published by the user today."""
        if not user_id or user_id == 'system':
            return 0 # System posts are not limited
        today_str = datetime.now().strftime('%Y-%m-%d')
        counter_key = f"user:{user_id}:{today_str}:published_count"
        count = self._safe_redis_operation(self.redis.get, counter_key)
        return int(count) if count else 0

    def _increment_user_daily_post_count(self, user_id: str):
        if not user_id or user_id == 'system':
            return
        today = datetime.now().strftime('%Y-%m-%d')
        key = f"user:{user_id}:{today}:published_count"

        val = self.redis.get(key)
        # val is str if decode_responses=True, else bytes or None
        if isinstance(val, bytes):
            val_str = val.decode()
        else:
            val_str = val

        if val_str is None or not val_str.lstrip('-').isdigit():
            self.redis.set(key, 0)
        self.redis.incr(key)
        self.redis.expire(key, int(timedelta(hours=25).total_seconds()))

    def _can_publish_for_user(self, user_id: str, user_settings: Dict[str, Any]) -> bool:
        """Check if the user can publish another post based on their daily limit."""
        if not user_id or user_id == 'system':
            return True # System can always publish

        max_posts_per_day = user_settings.get('content_generation', {}).get('max_posts_per_day', 3) # Default from settings
        if max_posts_per_day <= 0: # 0 or negative means unlimited
            return True

        current_daily_count = self._get_user_daily_post_count(user_id)
        can_publish = current_daily_count < max_posts_per_day

        if not can_publish:
            logger.info(f"User {user_id} has reached their daily post limit of {max_posts_per_day} (published: {current_daily_count}).")
        return can_publish
    
    def _get_monitor_user_id(self, monitor_id: str) -> Optional[str]:
        """Get user_id for a monitor with validation"""
        try:
            if not monitor_id:
                return None
                
            # Check cache first
            monitor_cache_key = f"monitor_user:{monitor_id}"
            cached_user_id = self.redis.get(monitor_cache_key)
            
            if cached_user_id and cached_user_id != 'None':
                return cached_user_id
            
            # Fallback to API
            response = make_api_request('GET', f'monitors/{monitor_id}')
            if response and isinstance(response, dict):
                user_id = response.get('user_id')
                
                # Validate user_id format (assuming UUIDs or similar)
                if user_id and isinstance(user_id, str) and len(user_id) > 0:
                    # Cache for 1 hour
                    self.redis.setex(monitor_cache_key, 3600, user_id)
                    return user_id
            
            # Cache negative result to avoid repeated API calls
            self.redis.setex(monitor_cache_key, 300, 'None')  # Cache for 5 minutes
            return None
            
        except Exception as e:
            logger.error(f"Error getting user_id for monitor {monitor_id}: {e}")
            return None
    
    def _queue_publishing(self, post_data: Dict[str, Any]):
        """Enhanced publishing queue with user validation"""
        try:
            user_id = post_data.get('user_id')
            
            if not user_id:
                logger.error("Cannot queue publishing: no user_id in post_data")
                return
            
            # Validate required fields
            if not post_data.get('platform'):
                logger.error(f"Cannot queue publishing for user {user_id}: no platform specified")
                return
            
            scheduled_time_str = post_data.get('scheduled_time')
            
            if scheduled_time_str:
                try:
                    scheduled_time = datetime.fromisoformat(scheduled_time_str)
                    now = datetime.now()
                    
                    if scheduled_time <= now:
                        # Post is ready to publish now
                        self._queue_immediate_publishing(post_data)
                    else:
                        # Post is scheduled for later
                        self._queue_delayed_publishing(post_data, scheduled_time)
                        
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid scheduled_time format for user {user_id}: {scheduled_time_str}, error: {e}")
                    # Default to immediate publishing if time format is invalid
                    self._queue_immediate_publishing(post_data)
            else:
                # No scheduled time, publish immediately
                self._queue_immediate_publishing(post_data)
                
        except Exception as e:
            logger.error(f"Error queueing publishing: {e}")
    
    def _queue_immediate_publishing(self, post_data: Dict[str, Any]):
        """Queue post for immediate publishing"""
        task = {
            'type': 'scheduled_post',
            'post_data': post_data,
            'created_at': datetime.now().isoformat(),
            'priority': 'normal'
        }
        
        try:
            self._safe_redis_operation(
                self.redis.lpush,
                self.queues['publishing'], 
                json.dumps(task)
            )
            logger.debug(f"Queued immediate publishing for user {post_data.get('user_id')}: {post_data.get('platform')} post")
        except Exception as e:
            logger.error(f"Error queueing immediate publishing: {e}")
    
    def _queue_delayed_publishing(self, post_data: Dict[str, Any], scheduled_time: datetime):
        """Queue post for delayed publishing"""
        now = datetime.now()
        seconds_until_publish = (scheduled_time - now).total_seconds()
        
        task = {
            'type': 'scheduled_post',
            'post_data': post_data,
            'created_at': now.isoformat(),
            'scheduled_time': scheduled_time.isoformat(),
            'priority': 'normal'
        }
        
        try:
            # Use Redis sorted set with timestamp as score for delayed publishing
            self._safe_redis_operation(
                self.redis.zadd,
                'pending_posts_queue',
                {json.dumps(task): scheduled_time.timestamp()}
            )
            logger.info(f"Scheduled {post_data.get('platform')} post for user {post_data.get('user_id')} at {scheduled_time} (in {seconds_until_publish/60:.1f} minutes)")
        except Exception as e:
            logger.error(f"Error scheduling post: {e}")
            # Fallback to immediate publishing
            self._queue_immediate_publishing(post_data)

    def _log_activity(self, activity_type: str, message: str, status: str = 'info', user_id: str = 'system', metadata: Dict = None):
        """Log activity for dashboard"""
        try:
            activity = {
                'type': activity_type,
                'message': message,
                'status': status,
                'timestamp': datetime.now().isoformat(),
                'user_id': user_id,
                'metadata': metadata or {}
            }
            
            # Store in Redis
            self.redis.lpush('activities', json.dumps(activity))
            self.redis.ltrim('activities', 0, 100)
            
            logger.debug(f"Logged activity: {activity_type} - {message}")
            
        except Exception as e:
            logger.error(f"Error logging activity: {e}")

    def _update_post_status(self, post_id: Optional[str], status: str, result_data: Dict):
        """Update post status via API"""
        if not post_id:
            return
            
        try:
            update_data = {
                'status': status,
                'updated_at': datetime.now().isoformat()
            }
            
            if status == 'published':
                update_data.update({
                    'published_at': result_data.get('published_at'),
                    'platform_post_id': result_data.get('platform_post_id'),
                    'platform_url': result_data.get('platform_url'),
                    'verified': result_data.get('verified', False)
                })
            elif status == 'failed':
                update_data['error_message'] = result_data.get('error')
            
            response = make_api_request('PUT', f'posts/{post_id}', data=update_data)
            
            if response:
                logger.debug(f"Updated post {post_id} status to {status}")
            else:
                logger.warning(f"Failed to update post {post_id} status")
                
        except Exception as e:
            logger.error(f"Error updating post status: {e}")

    def _safe_redis_operation(self, operation_func, *args, **kwargs):
        """Safely perform any Redis operation with retry logic"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                return operation_func(*args, **kwargs)
            except redis.ConnectionError as e:
                retry_count += 1
                logger.warning(f"Redis connection error in operation (attempt {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    time.sleep(2 * retry_count)
                    try:
                        self.redis.ping()
                    except:
                        pass
                else:
                    raise
            except redis.TimeoutError as e:
                logger.warning(f"Redis timeout in operation: {e}")
                return None
        
        return None

    def _safe_redis_brpop(self, queue_name: str, timeout: int = 30):
        """Safely perform Redis BRPOP with retry logic"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                result = self.redis.brpop(queue_name, timeout=timeout)
                return result
            except redis.ConnectionError as e:
                retry_count += 1
                logger.warning(f"Redis connection error (attempt {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    time.sleep(5 * retry_count)  # Exponential backoff
                    try:
                        self.redis.ping()
                    except:
                        pass
                else:
                    raise
            except redis.TimeoutError:
                return None
        
        return None

    def _get_pending_discovered_posts(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all pending discovered posts for a user that haven't been processed yet"""
        try:
            if not user_id or user_id == 'system':
                return []
            
            # Check for posts with status 'discovered' for this user
            response = make_api_request(
                'GET', 
                'posts', 
                params={
                    'user_id': user_id,
                    'status': 'discovered',
                    'limit': 50  # Process up to 50 pending posts at a time
                }
            )
            
            if response and isinstance(response, list):
                logger.info(f"Found {len(response)} pending discovered posts for user {user_id}")
                return response
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting pending discovered posts for user {user_id}: {e}")
            return []

    def _process_pending_discovered_posts(self, user_id: str, user_settings: Dict[str, Any]) -> int:
        """Process all pending discovered posts for a user and return count processed"""
        try:
            pending_posts = self._get_pending_discovered_posts(user_id)
            
            if not pending_posts:
                return 0
            
            logger.info(f"🔄 Processing {len(pending_posts)} pending discovered posts for user {user_id}")
            
            processed_count = 0
            for post_data in pending_posts:
                try:
                    # Ensure the post has user_id
                    post_data['user_id'] = user_id
                    post_data['discovered_by_user'] = user_id
                    
                    # Queue for content processing
                    self._queue_content_processing(post_data, user_id, user_settings)
                    processed_count += 1
                    
                    # Update post status to 'processing' to avoid reprocessing
                    self._update_post_status(post_data.get('id'), 'processing', {
                        'queued_for_content_generation_at': datetime.now().isoformat()
                    })
                    
                except Exception as e:
                    logger.error(f"Error processing pending post {post_data.get('id', 'unknown')} for user {user_id}: {e}")
                    continue
            
            logger.info(f"✅ Queued {processed_count} pending discovered posts for content generation (user {user_id})")
            
            self._log_activity(
                'pending_posts_processed',
                f'Processed {processed_count} pending discovered posts',
                'success',
                user_id,
                {'posts_processed': processed_count}
            )
            
            return processed_count
            
        except Exception as e:
            logger.error(f"Error processing pending discovered posts for user {user_id}: {e}")
            return 0

    def _has_pending_content_generation(self, user_id: str) -> bool:
        """Check if user has posts pending content generation"""
        try:
            if not user_id or user_id == 'system':
                return False
            
            # Check for posts with status 'processing' (queued for content generation)
            response = make_api_request(
                'GET', 
                'posts', 
                params={
                    'user_id': user_id,
                    'status': 'processing',
                    'limit': 1  # Just check if any exist
                }
            )
            
            has_pending = response and isinstance(response, list) and len(response) > 0
            
            if has_pending:
                logger.debug(f"User {user_id} has pending content generation tasks")
            
            return has_pending
            
        except Exception as e:
            logger.error(f"Error checking pending content generation for user {user_id}: {e}")
            return False

    def _process_blog_monitoring_task(self, task: Dict[str, Any]):
        """
        Enhanced blog monitoring with strict priority for pending posts.
        """
        task_user_id = task.get('user_id', 'system')
        
        try:
            task_type = task.get('type')
            user_id = task_user_id
            
            # Get user settings once for efficiency
            user_settings = self._get_user_settings(user_id)
            
            if task_type == 'manual_blog_check':
                # Manual checks bypass pending post priority (user-triggered)
                monitor_id = task.get('monitor_id')
                
                self._log_activity('blog_check', 'Starting manual blog monitoring check', 'info', user_id)
                
                if monitor_id:
                    # Check single monitor
                    if not self._validate_monitor_access(monitor_id, user_id):
                        logger.error(f"User {user_id} does not have access to monitor {monitor_id}")
                        self._log_activity('error', f'Access denied to monitor {monitor_id}', 'error', user_id)
                        return
                    
                    self.blog_monitor.configure_processing_limits(
                        max_posts_to_analyze=50,
                        max_posts_to_return=30
                    )
                    
                    new_posts = self.blog_monitor.check_monitor(monitor_id)
                    self.stats['blogs_checked'] += 1
                    self.stats['posts_discovered'] += len(new_posts)
                    
                    self._log_activity(
                        'blog_check',
                        f'Checked monitor {monitor_id} - found {len(new_posts)} new posts',
                        'success' if new_posts else 'info',
                        user_id,
                        {'monitor_id': monitor_id, 'posts_found': len(new_posts)}
                    )
                    
                    # Queue posts with user context
                    for post in new_posts:
                        post['discovered_by_user'] = user_id
                        self._queue_content_processing(post, user_id, user_settings)
                        
                else:
                    # Check all monitors for user
                    self.blog_monitor.configure_processing_limits(
                        max_posts_to_analyze=50,
                        max_posts_to_return=30
                    )
                    
                    results = self.blog_monitor.check_all_monitors(user_id)
                    total_posts = sum(len(posts) for posts in results.values())
                    
                    self.stats['blogs_checked'] += len(results)
                    self.stats['posts_discovered'] += total_posts
                    
                    self._log_activity(
                        'blog_check',
                        f'Checked {len(results)} monitors - found {total_posts} total new posts',
                        'success' if total_posts > 0 else 'info',
                        user_id,
                        {'monitors_checked': len(results), 'total_posts': total_posts}
                    )
                    
                    # Queue all posts with user context
                    for monitor_id, posts in results.items():
                        for post in posts:
                            post['discovered_by_user'] = user_id
                            self._queue_content_processing(post, user_id, user_settings)
                            
            elif task_type == 'scheduled_blog_check':
                # Process all users with pending posts priority
                logger.info("Processing 'scheduled_blog_check' with pending posts priority...")
                initial_run_flag = task.get('initial_run', False)

                active_monitors_response = make_api_request('GET', 'blog-monitors', params={'active': 'true', 'fields': 'user_id'})
                if not active_monitors_response or not isinstance(active_monitors_response, list):
                    logger.info("No active monitors found or API error during 'scheduled_blog_check' processing.")
                    return

                user_ids_with_active_monitors = set()
                for monitor in active_monitors_response:
                    if isinstance(monitor, dict) and monitor.get('user_id'):
                        user_ids_with_active_monitors.add(monitor['user_id'])

                if not user_ids_with_active_monitors:
                    logger.info("No users with active monitors found for 'scheduled_blog_check'.")
                    return

                logger.info(f"Found {len(user_ids_with_active_monitors)} users with active monitors. Processing with pending posts priority...")
                
                # Process each user with pending posts priority
                for user_id in user_ids_with_active_monitors:
                    try:
                        logger.info(f"Processing scheduled check for user {user_id}...")
                        
                        # STEP 1: PRIORITY CHECK - Process pending discovered posts first
                        user_settings = self._get_user_settings(user_id)
                        pending_processed = self._process_pending_discovered_posts(user_id, user_settings)
                        print("pending_processed", pending_processed)
                        if pending_processed > 0:
                            logger.info(f"✅ Processed {pending_processed} pending discovered posts for user {user_id}. Skipping new blog monitoring this cycle.")
                            self._log_activity(
                                'pending_posts_priority',
                                f'Processed {pending_processed} pending posts, skipped new blog monitoring',
                                'success',
                                user_id,
                                {'pending_posts_processed': pending_processed}
                            )
                            continue  # Skip to next user
                        
                        # STEP 2: Check if user still has content generation tasks pending
                        if self._has_pending_content_generation(user_id):
                            logger.info(f"User {user_id} has pending content generation tasks. Skipping new blog monitoring this cycle.")
                            self._log_activity(
                                'pending_content_generation',
                                'Skipped blog monitoring due to pending content generation',
                                'info',
                                user_id
                            )
                            continue  # Skip to next user
                        
                        # STEP 3: No pending posts - create user-specific task for new monitoring
                        logger.info(f"🔍 No pending posts for user {user_id}. Enqueuing new blog monitoring check.")
                        user_task = {
                            'type': 'user_scheduled_blog_check',
                            'user_id': user_id,
                            'created_at': datetime.now().isoformat(),
                            'priority': 'normal',
                            'triggered_by_initial_run': initial_run_flag,
                            'pending_posts_already_checked': True  # Flag to avoid double-checking
                        }
                        self._safe_redis_operation(
                            self.redis.lpush,
                            self.queues['blog_monitoring'],
                            json.dumps(user_task)
                        )
                        logger.info(f"📅 Enqueued new blog monitoring for user_id: {user_id}")
                        
                    except Exception as e:
                        logger.error(f"Error processing scheduled check for user {user_id}: {e}")
                        continue
                
                self._log_activity(
                    'master_scheduled_blog_check_completed',
                    f"Scheduled check completed for {len(user_ids_with_active_monitors)} users. Initial run: {initial_run_flag}",
                    'info', 'system',
                    {'users_processed': len(user_ids_with_active_monitors), 'initial_run': initial_run_flag}
                )

            elif task_type == 'user_scheduled_blog_check':
                # MAIN PROCESSING FLOW WITH PENDING POST PRIORITY
                user_id_to_check = task.get('user_id')
                if not user_id_to_check:
                    logger.error("User ID missing in user_scheduled_blog_check task.")
                    return

                logger.info(f"Processing user-specific scheduled blog check for user_id: {user_id_to_check}")
                
                # Check if pending posts were already processed by parent scheduled_blog_check
                pending_posts_already_checked = task.get('pending_posts_already_checked', False)
                
                if not pending_posts_already_checked:
                    # STEP 1: PRIORITY CHECK - Process pending discovered posts first
                    user_settings = self._get_user_settings(user_id_to_check)
                    pending_processed = self._process_pending_discovered_posts(user_id_to_check, user_settings)
                    
                    if pending_processed > 0:
                        logger.info(f"✅ Processed {pending_processed} pending discovered posts for user {user_id_to_check}. Skipping new blog monitoring this cycle.")
                        self._log_activity(
                            'pending_posts_priority',
                            f'Processed {pending_processed} pending posts, skipped new blog monitoring',
                            'success',
                            user_id_to_check,
                            {'pending_posts_processed': pending_processed}
                        )
                        return
                    
                    # STEP 2: Check if user still has content generation tasks pending
                    if self._has_pending_content_generation(user_id_to_check):
                        logger.info(f"User {user_id_to_check} has pending content generation tasks. Skipping new blog monitoring this cycle.")
                        self._log_activity(
                            'pending_content_generation',
                            'Skipped blog monitoring due to pending content generation',
                            'info',
                            user_id_to_check
                        )
                        return
                else:
                    logger.info(f"Pending posts already checked for user {user_id_to_check} by parent task. Proceeding with new blog monitoring.")
                    user_settings = self._get_user_settings(user_id_to_check)
                
                # STEP 3 (or immediate if already checked): Get user's active monitors specifically
                logger.info(f"🔍 {'Proceeding with' if pending_posts_already_checked else 'No pending posts for user'} {user_id_to_check}. Getting active monitors...")
                
                # FIXED: Get monitors for specific user instead of all active monitors
                user_monitors_response = make_api_request(
                    'GET', 
                    'blog-monitors', 
                    params={
                        'user_id': user_id_to_check,
                        'active': 'true'
                    }
                )
                
                if not user_monitors_response or not isinstance(user_monitors_response, list):
                    logger.info(f"No active monitors found for user {user_id_to_check}")
                    return
                
                if len(user_monitors_response) == 0:
                    logger.info(f"User {user_id_to_check} has no active monitors")
                    return
                
                logger.info(f"Found {len(user_monitors_response)} active monitors for user {user_id_to_check}")
                
                # STEP 4 (or 2 if already checked): Process each monitor for new content
                self.blog_monitor.configure_processing_limits(
                    max_posts_to_analyze=50,
                    max_posts_to_return=30
                )

                # Process monitors using the BlogMonitor class
                results = self.blog_monitor.check_all_monitors(user_id=user_id_to_check)
                total_posts_for_user = sum(len(posts) for posts in results.values())
                
                self.stats['blogs_checked'] += len(results)
                self.stats['posts_discovered'] += total_posts_for_user

                # STEP 5 (or 3 if already checked): Queue discovered posts for content processing
                for monitor_id, posts in results.items():
                    for post in posts:
                        post['discovered_by_user'] = user_id_to_check
                        self._queue_content_processing(post, user_id_to_check, user_settings)
                
                logger.info(f"✅ User scheduled blog check for {user_id_to_check}: {len(results)} monitors processed, {total_posts_for_user} new posts discovered.")
                
                self._log_activity(
                    'user_scheduled_blog_check',
                    f'Completed check for user {user_id_to_check}: {len(results)} monitors, {total_posts_for_user} new posts.',
                    'success',
                    user_id_to_check,
                    {
                        'monitors_checked_for_user': len(results),
                        'posts_found_for_user': total_posts_for_user,
                        'active_monitors_count': len(user_monitors_response)
                    }
                )

                if task_user_id != 'system':
                    self._log_user_specific_stats(task_user_id) 

        except Exception as e:
            logger.error(f"Error processing blog monitoring task: {e}")
            self.stats['errors'] += 1
            self._log_activity('error', f'Blog monitoring task failed: {str(e)}', 'error', task_user_id)
            
    def _process_content_task(self, task: Dict[str, Any]):
        """Enhanced content processing with status updates"""
        user_id = 'system'
        
        try:
            task_type = task.get('type')
            user_id = task.get('user_id', 'system')
            
            if task_type in ['discovered_post', 'manual_post_generation']:
                post_data = task.get('post_data')
                user_settings = task.get('user_settings')
                
                if not post_data or not isinstance(post_data, dict):
                    logger.error(f"Invalid or missing post data in content processing task for user {user_id}")
                    return
                
                # Ensure user settings are available and valid
                if not user_settings or not isinstance(user_settings, dict):
                    logger.warning(f"Invalid user settings in task for user {user_id}, fetching fresh settings")
                    user_settings = self._get_user_settings(user_id)
                
                # # Double-check content generation is enabled
                # if not user_settings.get('content_generation', {}).get('enabled', True):
                #     logger.info(f"Content generation disabled for user {user_id}, skipping")
                #     # Update post status to indicate it was skipped
                #     self._update_post_status(post_data.get('id'), 'skipped', {
                #         'reason': 'Content generation disabled',
                #         'skipped_at': datetime.now().isoformat()
                #     })
                #     return
                
                # Ensure post_data has user_id for downstream processing
                post_data['user_id'] = user_id
                post_data['processing_timestamp'] = datetime.now().isoformat()
                
                try:
                    # Update status to 'generating' before processing
                    self._update_post_status(post_data.get('id'), 'generating', {
                        'started_content_generation_at': datetime.now().isoformat()
                    })
                    
                    # Generate social media posts
                    generated_posts = self.content_processor.process_blog_post(post_data, user_settings)
                    
                    if not generated_posts or not isinstance(generated_posts, (list, tuple)):
                        logger.warning(f"No posts generated for user {user_id} from: {post_data.get('title', 'Unknown')}")
                        # Update status to indicate generation failed
                        self._update_post_status(post_data.get('id'), 'generation_failed', {
                            'reason': 'No posts generated',
                            'failed_at': datetime.now().isoformat()
                        })
                        return
                    
                    self.stats['posts_generated'] += len(generated_posts)
                    
                    # Queue posts for publishing with user context, respecting daily limits
                    queued_for_publishing_count = 0
                    for post in generated_posts:
                        if isinstance(post, dict):
                            post['user_id'] = user_id
                            post['original_title'] = post_data.get('title', 'Unknown')
                            post['source_post_id'] = post_data.get('id')  # Link back to original discovered post

                            # Check daily post limit before queueing for publishing
                            if self._can_publish_for_user(user_id, user_settings):
                                self._queue_publishing(post)
                                queued_for_publishing_count += 1
                            else:
                                logger.info(f"Skipping publishing for post '{post.get('original_title')}' for user {user_id} due to daily limit.")
                                self._log_activity(
                                    'publishing_skipped_limit',
                                    f"Skipped publishing for '{post.get('original_title')}' due to daily limit.",
                                    'info', user_id, {'title': post.get('original_title')}
                                )
                        else:
                            logger.error(f"Invalid generated post type for user {user_id}: {type(post)}")
                    
                    # Update original post status to 'generated' since we successfully created social posts
                    self._update_post_status(post_data.get('id'), 'generated', {
                        'social_posts_generated': len(generated_posts),
                        'social_posts_queued_for_publishing': queued_for_publishing_count,
                        'generated_at': datetime.now().isoformat()
                    })
                    
                    logger.info(f"✅ Generated {len(generated_posts)} social posts for user {user_id} from: {post_data.get('title', 'Unknown')}. Queued {queued_for_publishing_count} for publishing.")
                    
                    self._log_activity(
                        'content_generated',
                        f'Generated {len(generated_posts)} social posts',
                        'success',
                        user_id,
                        {
                            'original_title': post_data.get('title'),
                            'posts_count': len(generated_posts),
                            'platforms': [post.get('platform') for post in generated_posts if isinstance(post, dict)]
                        }
                    )
                    
                except Exception as e:
                    logger.error(f"Error in content generation for user {user_id}: {e}")
                    # Update post status to indicate generation failed
                    self._update_post_status(post_data.get('id'), 'generation_failed', {
                        'error': str(e),
                        'failed_at': datetime.now().isoformat()
                    })
                    self._log_activity('error', f'Content generation failed: {str(e)}', 'error', user_id)
                    
        except Exception as e:
            logger.error(f"Error processing content task for user {user_id}: {e}")
            self.stats['errors'] += 1
            self._log_activity('error', f'Content task processing failed: {str(e)}', 'error', user_id)

    def _log_stats(self, user_id: str = None):
        """Enhanced worker statistics including pending posts by status"""
        try:
            start_time_str = self.stats['start_time']
            if isinstance(start_time_str, str):
                start_time = datetime.fromisoformat(start_time_str)
            else:
                start_time = start_time_str
            
            uptime = datetime.now() - start_time
            
            logger.info("📊 Worker Statistics:")
            logger.info(f"   Uptime: {uptime}")
            logger.info(f"   Status: {'Paused' if self.paused else 'Running'}")
            logger.info(f"   Blogs checked: {self.stats['blogs_checked']}")
            logger.info(f"   Posts discovered: {self.stats['posts_discovered']}")
            logger.info(f"   Posts generated: {self.stats['posts_generated']}")
            logger.info(f"   Posts published: {self.stats['posts_published']}")
            logger.info(f"   Errors: {self.stats['errors']}")
            
            # Log queue sizes
            for name, queue in self.queues.items():
                size = self._safe_redis_operation(self.redis.llen, queue) or 0
                logger.info(f"   {name.replace('_', ' ').title()} queue: {size}")
            
            # Log pending posts count
            pending_count = self._safe_redis_operation(self.redis.zcard, 'pending_posts_queue') or 0
            logger.info(f"   Pending scheduled posts: {pending_count}")
            
            # Show status distribution of posts
            try:
                # Prepare params
                params = {'include_counts': 'true'}
                if user_id:
                    params['user_id'] = user_id
                    logger.info(f"📋 Post Status Summary for user {user_id}:")
                else:
                    logger.info("📋 Post Status Summary (All Users):")
                
                status_response = make_api_request(
                    'GET', 
                    'posts/status-summary',
                    params=params
                )
                
                if status_response and isinstance(status_response, dict):
                    # Filter out metadata and focus on actual counts
                    status_fields = ['discovered', 'processing', 'generating', 'generated', 
                                'scheduled', 'published', 'failed', 'skipped', 'queued', 
                                'generation_failed', 'total']
                    
                    for status in status_fields:
                        count = status_response.get(status, 0)
                        if count > 0 or status == 'total':  # Always show total
                            logger.info(f"   {status.replace('_', ' ').title()}: {count}")
                    
                    # Show pipeline summary if available
                    pipeline_summary = status_response.get('pipeline_summary')
                    if pipeline_summary:
                        logger.info("📊 Pipeline Summary:")
                        logger.info(f"   Pending Processing: {pipeline_summary.get('pending_processing', 0)}")
                        logger.info(f"   Ready for Publishing: {pipeline_summary.get('ready_for_publishing', 0)}")
                        logger.info(f"   Completed: {pipeline_summary.get('completed', 0)}")
                        logger.info(f"   Errors: {pipeline_summary.get('errors', 0)}")
                        
            except Exception as e:
                logger.debug(f"Could not fetch post status summary: {e}")
            
            # Show next few pending posts
            if pending_count > 0:
                next_posts = self._safe_redis_operation(
                    self.redis.zrange,
                    'pending_posts_queue',
                    0, 2, 
                    withscores=True
                )
                
                if next_posts:
                    logger.info("🕒 Next scheduled posts:")
                    for post_json, timestamp in next_posts:
                        try:
                            scheduled_time = datetime.fromtimestamp(timestamp)
                            task = json.loads(post_json)
                            platform = task.get('post_data', {}).get('platform', 'unknown')
                            task_user_id = task.get('post_data', {}).get('user_id', 'unknown')
                            logger.info(f"   {platform} for user {task_user_id} at {scheduled_time}")
                        except:
                            pass
                    
        except Exception as e:
            logger.error(f"Error logging stats: {e}")

    def _log_user_specific_stats(self, user_id: str):
        """Log statistics for a specific user"""
        try:
            if not user_id or user_id == 'system':
                return
            
            logger.info(f"👤 User-Specific Stats for {user_id}:")
            
            # Get user's daily publish count
            daily_published = self._get_user_daily_post_count(user_id)
            user_settings = self._get_user_settings(user_id)
            daily_limit = user_settings.get('content_generation', {}).get('max_posts_per_day', 3)
            
            logger.info(f"   Daily Posts: {daily_published}/{daily_limit}")
            logger.info(f"   Daily Remaining: {max(0, daily_limit - daily_published)}")
            
            # Get pipeline status for user
            pipeline_status = self.get_pipeline_status_for_user(user_id)
            if pipeline_status:
                post_counts = pipeline_status.get('post_counts_by_status', {})
                has_pending = pipeline_status.get('has_pending_pipeline_items', False)
                
                logger.info(f"   Has Pending Items: {has_pending}")
                logger.info(f"   Connected Platforms: {pipeline_status.get('connected_platforms', [])}")
                logger.info(f"   Content Generation: {'Enabled' if pipeline_status.get('content_generation_enabled') else 'Disabled'}")
                logger.info(f"   Auto Publish: {'Enabled' if pipeline_status.get('auto_publish_enabled') else 'Disabled'}")
                
                # Show non-zero counts
                for status, count in post_counts.items():
                    if count > 0:
                        logger.info(f"   {status.replace('_', ' ').title()}: {count}")
            
        except Exception as e:
            logger.error(f"Error logging user-specific stats for {user_id}: {e}")

    def _log_stats_for_active_users(self):
        """Log statistics broken down by active users"""
        try:
            # Get users with any posts in the system
            response = make_api_request(
                'GET', 
                'posts',
                params={'limit': '100'}  # Get recent posts to find active users
            )
            
            if response and isinstance(response, list):
                active_users = set()
                for post in response:
                    if isinstance(post, dict) and post.get('user_id'):
                        active_users.add(post['user_id'])
                
                if active_users:
                    logger.info(f"📊 Active Users: {len(active_users)}")
                    
                    # Show stats for each active user (limit to top 5 to avoid spam)
                    for user_id in list(active_users)[:5]:
                        self._log_user_specific_stats(user_id)
                        
                    if len(active_users) > 5:
                        logger.info(f"   ... and {len(active_users) - 5} more users")
                        
        except Exception as e:
            logger.debug(f"Could not fetch active users stats: {e}")

    def get_pipeline_status_for_user(self, user_id: str) -> Dict[str, Any]:
        """Get detailed pipeline status for a specific user"""
        try:
            if not user_id or user_id == 'system':
                return {}
            
            # Get post counts by status
            status_counts = {}
            statuses = ['discovered', 'processing', 'generating', 'generated', 'scheduled', 'published', 'failed', 'skipped']
            
            for status in statuses:
                try:
                    response = make_api_request(
                        'GET', 
                        'posts', 
                        params={
                            'user_id': user_id,
                            'status': status,
                            'count_only': 'true'
                        }
                    )
                    status_counts[status] = response.get('count', 0) if isinstance(response, dict) else len(response) if response else 0
                except:
                    status_counts[status] = 0
            
            # Get daily publish count
            daily_published = self._get_user_daily_post_count(user_id)
            user_settings = self._get_user_settings(user_id)
            daily_limit = user_settings.get('content_generation', {}).get('max_posts_per_day', 3)
            
            return {
                'user_id': user_id,
                'post_counts_by_status': status_counts,
                'daily_published_count': daily_published,
                'daily_limit': daily_limit,
                'daily_remaining': max(0, daily_limit - daily_published),
                'has_pending_pipeline_items': any(status_counts[s] > 0 for s in ['discovered', 'processing', 'generating']),
                'connected_platforms': user_settings.get('_connected_platforms', []),
                'content_generation_enabled': user_settings.get('content_generation', {}).get('enabled', True),
                'auto_publish_enabled': user_settings.get('publishing', {}).get('auto_publish', True)
            }
            
        except Exception as e:
            logger.error(f"Error getting pipeline status for user {user_id}: {e}")
            return {}



    # <----------------------->
    def _get_all_users_with_monitors(self) -> List[str]:
        """
        Get all user IDs who have blog monitors (active or inactive).
        
        Fetches all blog monitors and extracts unique user_ids.
        
        Returns: List of user_id strings
        """
        try:
            logger.info("Getting all users with blog monitors...")
            
            # Get all monitors and extract unique user_ids
            monitors_response = make_api_request('GET', 'blog-monitors', params={'active': 'true'})
            
            if not monitors_response or not isinstance(monitors_response, list):
                logger.warning("No blog monitors found or API error")
                return []
            
            # Extract unique user IDs
            user_ids = set()
            for monitor in monitors_response:
                if isinstance(monitor, dict) and monitor.get('user_id'):
                    user_ids.add(monitor['user_id'])
            
            user_ids_list = list(user_ids)
            logger.info(f"Found {len(user_ids_list)} users with blog monitors: {user_ids_list}")
            return user_ids_list
            
        except Exception as e:
            logger.error(f"Error getting users with monitors: {e}")
            return []

    def _process_user_pending_posts_priority(self, user_id: str) -> bool:
        """
        Process pending posts for a user with priority.
        
        This method ensures that existing discovered posts are processed before 
        checking for new blog content. It handles the complete pipeline:
        1. Discovered posts (status: 'discovered') 
        2. Content generation queue (status: 'processing')
        
        Args:
            user_id: The user ID to process
            
        Returns:
            bool: True if pending posts were found and processed (skip new monitoring)
                False if no pending posts (proceed with new monitoring)
        """
        try:
            logger.info(f"🔍 Checking pending posts priority for user {user_id}")
            
            # Get user settings
            user_settings = self._get_user_settings(user_id)
            
            # STEP 1: Process pending discovered posts first
            pending_processed = self._process_pending_discovered_posts(user_id, user_settings)
            print("pending_processed",pending_processed)
            if pending_processed > 0:
                logger.info(f"✅ Processed {pending_processed} pending discovered posts for user {user_id}. Skipping new blog monitoring.")
                self._log_activity(
                    'pending_posts_priority',
                    f'Processed {pending_processed} pending posts, skipped new blog monitoring',
                    'success',
                    user_id,
                    {'pending_posts_processed': pending_processed}
                )
                return True  # Skip new monitoring
            
            # STEP 2: Check if user still has content generation tasks pending
            if self._has_pending_content_generation(user_id):
                logger.info(f"User {user_id} has pending content generation tasks. Skipping new blog monitoring.")
                self._log_activity(
                    'pending_content_generation',
                    'Skipped blog monitoring due to pending content generation',
                    'info',
                    user_id
                )
                return True  # Skip new monitoring
            
            # No pending posts - can proceed with new monitoring
            logger.info(f"✅ No pending posts for user {user_id}. Can proceed with new blog monitoring.")
            return False
            
        except Exception as e:
            logger.error(f"Error processing pending posts priority for user {user_id}: {e}")
            return False  # On error, proceed with new monitoring

    def _process_new_blog_monitoring_for_user(self, user_id: str, user_settings: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Process new blog monitoring for a specific user.
        
        This method handles the actual blog monitoring process:
        1. Gets user's active monitors
        2. Configures processing limits
        3. Checks all monitors for new content
        4. Queues discovered posts for content processing
        5. Updates statistics and logs activity
        
        Args:
            user_id: The user ID to process
            user_settings: Optional user settings (will fetch if not provided)
            
        Returns:
            Dict containing:
            - monitors_processed: Number of monitors checked
            - posts_discovered: Number of new posts found
            - active_monitors_count: Total active monitors for user
            - error: Error message if any
        """
        try:
            if not user_settings:
                user_settings = self._get_user_settings(user_id)
            
            logger.info(f"🔍 Processing new blog monitoring for user {user_id}")
            
            # Get user's active monitors
            user_monitors_response = make_api_request(
                'GET', 
                'blog-monitors', 
                params={
                    'user_id': user_id,
                    'active': 'true'
                }
            )
            
            if not user_monitors_response or not isinstance(user_monitors_response, list):
                logger.info(f"No active monitors found for user {user_id}")
                return {'monitors_processed': 0, 'posts_discovered': 0}
            
            if len(user_monitors_response) == 0:
                logger.info(f"User {user_id} has no active monitors")
                return {'monitors_processed': 0, 'posts_discovered': 0}
            
            logger.info(f"Found {len(user_monitors_response)} active monitors for user {user_id}")
            
            # Configure processing limits
            self.blog_monitor.configure_processing_limits(
                max_posts_to_analyze=50,
                max_posts_to_return=30
            )

            # Process monitors using the BlogMonitor class
            results = self.blog_monitor.check_all_monitors(user_id=user_id)
            total_posts_for_user = sum(len(posts) for posts in results.values())
            
            # Update stats
            self.stats['blogs_checked'] += len(results)
            self.stats['posts_discovered'] += total_posts_for_user

            # Queue discovered posts for content processing
            for monitor_id, posts in results.items():
                for post in posts:
                    post['discovered_by_user'] = user_id
                    self._queue_content_processing(post, user_id, user_settings)
            
            logger.info(f"✅ New blog monitoring for user {user_id}: {len(results)} monitors processed, {total_posts_for_user} new posts discovered.")
            
            self._log_activity(
                'new_blog_monitoring',
                f'Processed {len(results)} monitors, found {total_posts_for_user} new posts',
                'success',
                user_id,
                {
                    'monitors_processed': len(results),
                    'posts_discovered': total_posts_for_user,
                    'active_monitors_count': len(user_monitors_response)
                }
            )
            
            return {
                'monitors_processed': len(results),
                'posts_discovered': total_posts_for_user,
                'active_monitors_count': len(user_monitors_response)
            }
            
        except Exception as e:
            logger.error(f"Error processing new blog monitoring for user {user_id}: {e}")
            return {'monitors_processed': 0, 'posts_discovered': 0, 'error': str(e)}

    def _process_blog_monitoring_task(self, task: Dict[str, Any]):
        """
        Enhanced blog monitoring with strict priority for pending posts FOR ALL TASK TYPES.
        
        FLOW EXPLANATION:
        1. Extract task type and user_id from task
        2. Get all users with monitors (not just active monitors)
        3. For each user: ALWAYS check pending posts first
        4. Only if no pending posts: process new blog monitoring
        
        ALL TASK TYPES now prioritize pending posts:
        - manual_blog_check: Check pending posts first, then process new monitoring
        - scheduled_blog_check: Check pending posts for all users, then new monitoring
        - user_scheduled_blog_check: Check pending posts for specific user, then new monitoring
        """
        task_user_id = task.get('user_id', 'system')
        
        try:
            task_type = task.get('type')
            
            if task_type == 'manual_blog_check':
                # Manual checks now ALSO prioritize pending posts (FIXED)
                user_id = task_user_id if task_user_id != 'system' else None
                monitor_id = task.get('monitor_id')
                
                self._log_activity('blog_check', 'Starting manual blog monitoring check', 'info', user_id or 'system')
                
                if user_id:
                    # Single user manual check
                    logger.info(f"Manual blog check for user {user_id}")
                    
                    # STEP 1: Check pending posts first (NEW - was bypassed before)
                    should_skip = self._process_user_pending_posts_priority(user_id)
                    print(should_skip)
                    
                    if should_skip:
                        logger.info(f"Manual check for user {user_id} handled pending posts only")
                        return
                    
                    # STEP 2: Process new monitoring
                    if monitor_id:
                        # Single monitor check
                        if not self._validate_monitor_access(monitor_id, user_id):
                            logger.error(f"User {user_id} does not have access to monitor {monitor_id}")
                            self._log_activity('error', f'Access denied to monitor {monitor_id}', 'error', user_id)
                            return
                        
                        self.blog_monitor.configure_processing_limits(
                            max_posts_to_analyze=50,
                            max_posts_to_return=30
                        )
                        
                        new_posts = self.blog_monitor.check_monitor(monitor_id)
                        self.stats['blogs_checked'] += 1
                        self.stats['posts_discovered'] += len(new_posts)
                        
                        # Queue posts with user context
                        user_settings = self._get_user_settings(user_id)
                        for post in new_posts:
                            post['discovered_by_user'] = user_id
                            self._queue_content_processing(post, user_id, user_settings)
                        
                        self._log_activity(
                            'blog_check',
                            f'Manual check: monitor {monitor_id} - found {len(new_posts)} new posts',
                            'success' if new_posts else 'info',
                            user_id,
                            {'monitor_id': monitor_id, 'posts_found': len(new_posts)}
                        )
                    else:
                        # All monitors for user
                        result = self._process_new_blog_monitoring_for_user(user_id)
                        self._log_activity(
                            'blog_check',
                            f'Manual check: {result["monitors_processed"]} monitors - found {result["posts_discovered"]} new posts',
                            'success' if result["posts_discovered"] > 0 else 'info',
                            user_id,
                            result
                        )
                else:
                    # System-wide manual check (no specific user)
                    logger.info("Manual blog check for all users")
                    all_users = self._get_all_users_with_monitors()
                    
                    if not all_users:
                        logger.info("No users with monitors found for manual check")
                        return
                    
                    total_users_processed = 0
                    total_posts_discovered = 0
                    
                    for user_id in all_users:
                        try:
                            logger.info(f"Processing manual check for user {user_id}")
                            
                            # STEP 1: Check pending posts first
                            should_skip = self._process_user_pending_posts_priority(user_id)
                            
                            if should_skip:
                                total_users_processed += 1
                                continue
                            
                            # STEP 2: Process new monitoring
                            result = self._process_new_blog_monitoring_for_user(user_id)
                            total_posts_discovered += result["posts_discovered"]
                            total_users_processed += 1
                            
                        except Exception as e:
                            logger.error(f"Error in manual check for user {user_id}: {e}")
                            continue
                    
                    self._log_activity(
                        'blog_check',
                        f'Manual check completed: {total_users_processed} users processed, {total_posts_discovered} total posts discovered',
                        'success',
                        'system',
                        {'users_processed': total_users_processed, 'total_posts_discovered': total_posts_discovered}
                    )
                                
            elif task_type == 'scheduled_blog_check':
                # Scheduled check for all users with pending posts priority
                logger.info("Processing 'scheduled_blog_check' with pending posts priority for all users...")
                initial_run_flag = task.get('initial_run', False)

                # Get all users with monitors (FIXED - was trying to get monitors for 'system' user)
                all_users = self._get_all_users_with_monitors()
                
                if not all_users:
                    logger.info("No users with monitors found for scheduled check")
                    return

                logger.info(f"Found {len(all_users)} users with monitors. Processing with pending posts priority...")
                
                total_users_processed = 0
                total_posts_discovered = 0
                users_with_pending_posts = 0
                users_with_new_monitoring = 0
                
                # Process each user with pending posts priority
                for user_id in all_users:
                    try:
                        logger.info(f"Processing scheduled check for user {user_id}...")
                        
                        # STEP 1: Check pending posts first
                        should_skip = self._process_user_pending_posts_priority(user_id)
                        
                        if should_skip:
                            users_with_pending_posts += 1
                            total_users_processed += 1
                            continue
                        
                        # STEP 2: Process new monitoring
                        result = self._process_new_blog_monitoring_for_user(user_id)
                        total_posts_discovered += result["posts_discovered"]
                        
                        if result["posts_discovered"] > 0:
                            users_with_new_monitoring += 1
                        
                        total_users_processed += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing scheduled check for user {user_id}: {e}")
                        continue
                
                logger.info(f"✅ Scheduled check completed: {total_users_processed} users processed, {users_with_pending_posts} had pending posts, {users_with_new_monitoring} had new posts, {total_posts_discovered} total new posts discovered")
                
                self._log_activity(
                    'scheduled_blog_check_completed',
                    f"Scheduled check completed: {total_users_processed} users, {users_with_pending_posts} with pending posts, {total_posts_discovered} new posts discovered",
                    'success',
                    'system',
                    {
                        'users_processed': total_users_processed,
                        'users_with_pending_posts': users_with_pending_posts,
                        'users_with_new_monitoring': users_with_new_monitoring,
                        'total_posts_discovered': total_posts_discovered,
                        'initial_run': initial_run_flag
                    }
                )

            elif task_type == 'user_scheduled_blog_check':
                # User-specific scheduled check with pending posts priority
                user_id_to_check = task.get('user_id')
                if not user_id_to_check:
                    logger.error("User ID missing in user_scheduled_blog_check task.")
                    return

                logger.info(f"Processing user-specific scheduled blog check for user_id: {user_id_to_check}")
                
                # STEP 1: Check pending posts first
                should_skip = self._process_user_pending_posts_priority(user_id_to_check)
                
                if should_skip:
                    logger.info(f"User scheduled check for {user_id_to_check} handled pending posts only")
                    return
                
                # STEP 2: Process new monitoring
                result = self._process_new_blog_monitoring_for_user(user_id_to_check)
                
                self._log_activity(
                    'user_scheduled_blog_check',
                    f'User scheduled check completed: {result["monitors_processed"]} monitors, {result["posts_discovered"]} new posts',
                    'success',
                    user_id_to_check,
                    result
                )

                # Log user-specific stats
                if user_id_to_check != 'system':
                    self._log_user_specific_stats(user_id_to_check)

            else:
                logger.warning(f"Unknown task type: {task_type}")

        except Exception as e:
            logger.error(f"Error processing blog monitoring task: {e}")
            self.stats['errors'] += 1
            self._log_activity('error', f'Blog monitoring task failed: {str(e)}', 'error', task_user_id)
            
def main():
    """Main function to start the worker"""
    if not os.getenv('REDIS_HOST', 'localhost'):
        logger.warning("REDIS_HOST not set, using localhost")
    
    worker = UnifiedBackgroundWorker()
    
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        worker.stop()
    except Exception as e:
        logger.error(f"Worker error: {e}")
        worker.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()