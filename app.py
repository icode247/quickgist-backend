from flask import Flask, request, jsonify
from flask_cors import CORS
import redis
import json
import logging
import os
import sys
from datetime import datetime

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass 

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('blog_automation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins=["http://localhost:3000", "https://quickgist-alpha.vercel.app", "http://localhost:3001",])

# ======= ROLLBAR CONFIGURATION =======
import rollbar
import rollbar.contrib.flask
from flask import got_request_exception

# Initialize Rollbar only in production or when ROLLBAR_TOKEN is set
ROLLBAR_TOKEN = os.getenv('ROLLBAR_POST_SERVER_ITEM_ACCESS_TOKEN')
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

if ROLLBAR_TOKEN:
    with app.app_context():
        """Initialize rollbar module"""
        rollbar.init(
            # Access token from environment variable
            ROLLBAR_TOKEN,
            # Environment name
            ENVIRONMENT,
            # Server root directory, makes tracebacks prettier
            root=os.path.dirname(os.path.realpath(__file__)),
            # Flask already sets up logging
            allow_logging_basic_config=False,
            # Additional configuration
            capture_uncaught=True,  # Capture uncaught exceptions
            capture_unhandled_rejections=True,
            # Custom person tracking (optional)
            person_fn=lambda req: {
                'id': req.json.get('user_id') if req.json else None,
                'email': req.json.get('email') if req.json else None
            } if req and hasattr(req, 'json') else None
        )
        
        # Send exceptions from `app` to rollbar, using flask's signal system
        got_request_exception.connect(rollbar.contrib.flask.report_exception, app)
        
        logger.info("✅ Rollbar error tracking initialized")
else:
    logger.warning("⚠️ Rollbar token not found - error tracking disabled")

# ======= END ROLLBAR CONFIGURATION =======

# Environment variables
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', '')

# Redis connection
try:
    redis_client = redis.Redis(
        host=REDIS_HOST, 
        port=REDIS_PORT,
        password=REDIS_PASSWORD, 
        db=0, 
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True
    )
    redis_client.ping()
    logger.info("✅ Connected to Redis successfully")
except redis.ConnectionError as e:
    logger.error(f"❌ Failed to connect to Redis: {e}")
    # Send critical error to Rollbar if available
    if ROLLBAR_TOKEN:
        rollbar.report_exc_info(extra_data={'component': 'redis_connection'})
    sys.exit(1)

# Import automation components
try:
    from integrations.social_poster import SocialPoster, LinkedInAPIPoster
    from integrations.social_poster import LinkedInAPIPoster, AccountCredentials, PlatformType
    from integrations.session_manager import SocialSessionManager
    logger.info("✅ Automation modules imported successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import automation modules: {e}")
    # Send critical error to Rollbar if available
    if ROLLBAR_TOKEN:
        rollbar.report_exc_info(extra_data={'component': 'module_import'})
    sys.exit(1)

# Initialize components
social_poster = SocialPoster(redis_client)
session_manager = SocialSessionManager(redis_client)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Production health check endpoint"""
    try:
        redis_connected = redis_client.ping()
        
        health_data = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'version': '2.0.0',
            'environment': os.getenv('ENVIRONMENT', 'development'),
            'services': {
                'redis': redis_connected,
                'background_worker': _check_worker_health(),
                'ai_enabled': bool(os.getenv('OPENAI_API_KEY')),
                'rollbar_enabled': bool(ROLLBAR_TOKEN)
            },
            'queue_stats': {
                'blog_monitoring_queue': redis_client.llen('blog_monitoring_queue') or 0,
                'content_processing_queue': redis_client.llen('content_processing_queue') or 0,
                'publishing_queue': redis_client.llen('publishing_queue') or 0,
                'published_posts': redis_client.llen('published_posts') or 0
            }
        }
        
        return jsonify(health_data)
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        # Report to Rollbar with context
        if ROLLBAR_TOKEN:
            rollbar.report_exc_info(extra_data={
                'endpoint': '/api/health',
                'component': 'health_check'
            })
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

def _check_worker_health():
    """Check if background worker is running"""
    try:
        last_heartbeat = redis_client.get('worker:heartbeat')
        if last_heartbeat:
            last_time = datetime.fromisoformat(last_heartbeat)
            time_diff = (datetime.now() - last_time).total_seconds()
            return time_diff < 300  # Worker should heartbeat every 5 minutes
        return False
    except Exception as e:
        # Report worker health check errors to Rollbar
        if ROLLBAR_TOKEN:
            rollbar.report_exc_info(extra_data={
                'component': 'worker_health_check',
                'function': '_check_worker_health'
            })
        return False

@app.route('/api/test-social-connection', methods=['POST'])
def test_social_connection():
    """Test social media account connection and save session"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        platform = data.get('platform', '').lower().strip()
        username = data.get('username', '').strip()
        password = data.get('password', '')
        user_id = data.get('user_id', 'default_user')
        
        if not all([platform, username, password]):
            return jsonify({'error': 'Platform, username, and password are required'}), 400
        
        if platform not in ['twitter', 'linkedin', 'facebook', 'instagram']:
            return jsonify({'error': 'Unsupported platform'}), 400
        
        # Test connection and save session
        connection_result = session_manager.test_connection_with_session(
            user_id, platform, username, password
        )
        
        if connection_result['success']:
            logger.info(f"✅ Successfully tested connection: {platform} - {username}")
            # Log successful connection to Rollbar for monitoring
            if ROLLBAR_TOKEN:
                rollbar.report_message(
                    f"Successful social connection: {platform}",
                    level='info',
                    extra_data={
                        'user_id': user_id,
                        'platform': platform,
                        'username': username
                    }
                )
            return jsonify({
                'success': True,
                'message': 'Connection successful and session saved',
                'platform': platform,
                'username': username
            })
        else:
            logger.error(f"❌ Connection test failed: {platform} - {username}")
            # Report failed connections to Rollbar
            if ROLLBAR_TOKEN:
                rollbar.report_message(
                    f"Failed social connection: {platform}",
                    level='warning',
                    extra_data={
                        'user_id': user_id,
                        'platform': platform,
                        'username': username,
                        'error': connection_result.get('error')
                    }
                )
            return jsonify({
                'success': False,
                'error': connection_result.get('error', 'Connection failed')
            }), 401
        
    except Exception as e:
        logger.error(f"Error testing social connection: {str(e)}")
        # Rollbar will automatically capture this via the signal handler
        return jsonify({'error': 'Failed to test connection', 'details': str(e)}), 500

# Add a custom error handler that also reports to Rollbar
@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {str(error)}")
    # Additional context for Rollbar
    if ROLLBAR_TOKEN:
        rollbar.report_exc_info(extra_data={
            'endpoint': request.endpoint if request else None,
            'method': request.method if request else None,
            'url': request.url if request else None,
            'user_agent': request.headers.get('User-Agent') if request else None
        })
    return jsonify({'error': 'Internal server error'}), 500

@app.errorhandler(404)
def not_found(error):
    # Report 404s to Rollbar for monitoring (optional)
    if ROLLBAR_TOKEN:
        rollbar.report_message(
            f"404 Not Found: {request.url if request else 'Unknown URL'}",
            level='warning',
            extra_data={
                'endpoint': request.endpoint if request else None,
                'method': request.method if request else None,
                'url': request.url if request else None
            }
        )
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(400)
def bad_request(error):
    return jsonify({'error': 'Bad request'}), 400

# Add a test endpoint to verify Rollbar is working
@app.route('/api/test-rollbar', methods=['POST'])
def test_rollbar():
    """Test endpoint to verify Rollbar error reporting"""
    if not ROLLBAR_TOKEN:
        return jsonify({'error': 'Rollbar not configured'}), 400
    
    try:
        # Send a test message
        rollbar.report_message(
            "Rollbar test message from Flask API",
            level='info',
            extra_data={
                'test': True,
                'timestamp': datetime.now().isoformat(),
                'endpoint': '/api/test-rollbar'
            }
        )
        
        # Optionally test an exception
        test_type = request.json.get('type', 'message') if request.json else 'message'
        
        if test_type == 'exception':
            try:
                # This will raise an exception
                x = 1 / 0
            except ZeroDivisionError:
                rollbar.report_exc_info(extra_data={'test_exception': True})
                
        return jsonify({
            'success': True,
            'message': 'Rollbar test completed',
            'type': test_type
        })
        
    except Exception as e:
        logger.error(f"Error testing Rollbar: {str(e)}")
        return jsonify({'error': 'Failed to test Rollbar', 'details': str(e)}), 500

@app.route('/api/trigger-blog-check', methods=['POST'])
def trigger_blog_check():
    """Manually trigger blog monitoring for specific monitor or all"""
    try:
        data = request.get_json() or {}
        monitor_id = data.get('monitor_id')
        user_id = data.get('user_id', 'default_user')
        
        # Add to blog monitoring queue
        task = {
            'type': 'manual_blog_check',
            'monitor_id': monitor_id,  # None means check all
            'user_id': user_id,
            'priority': 'high',
            'created_at': datetime.now().isoformat()
        }
        
        redis_client.lpush('blog_monitoring_queue', json.dumps(task))
        
        message = f"Triggered blog check for monitor {monitor_id}" if monitor_id else "Triggered blog check for all monitors"
        logger.info(message)
        
        return jsonify({
            'success': True,
            'message': message,
            'task_id': task['created_at']
        })
        
    except Exception as e:
        logger.error(f"Error triggering blog check: {str(e)}")
        return jsonify({'error': 'Failed to trigger blog check', 'details': str(e)}), 500

@app.route('/api/trigger-post-generation', methods=['POST'])
def trigger_post_generation():
    """Manually trigger post generation for a specific blog post"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        required_fields = ['title', 'content', 'url', 'user_id']
        if not all(field in data for field in required_fields):
            return jsonify({'error': 'Missing required fields: title, content, url, user_id'}), 400
        
        # Add to content processing queue
        task = {
            'type': 'manual_post_generation',
            'post_data': {
                'title': data['title'],
                'content': data['content'],
                'url': data['url'],
                'user_id': data['user_id'],
                'author': data.get('author', 'Unknown'),
                'published_at': data.get('published_at', datetime.now().isoformat())
            },
            'priority': 'high',
            'created_at': datetime.now().isoformat()
        }
        
        redis_client.lpush('content_processing_queue', json.dumps(task))
        
        logger.info(f"Triggered post generation for: {data['title']}")
        
        return jsonify({
            'success': True,
            'message': 'Post generation triggered successfully',
            'task_id': task['created_at']
        })
        
    except Exception as e:
        logger.error(f"Error triggering post generation: {str(e)}")
        return jsonify({'error': 'Failed to trigger post generation', 'details': str(e)}), 500

@app.route('/api/publish-post', methods=['POST'])
def publish_post():
    """Manually trigger publishing of a specific post"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        required_fields = ['content', 'platform', 'user_id']
        if not all(field in data for field in required_fields):
            return jsonify({'error': 'Missing required fields: content, platform, user_id'}), 400
        
        # Add to publishing queue
        task = {
            'type': 'manual_publish',
            'post_data': {
                'content': data['content'],
                'platform': data['platform'],
                'user_id': data['user_id'],
                'original_title': data.get('original_title', ''),
                'original_url': data.get('original_url', ''),
                'scheduled_time': data.get('scheduled_time', datetime.now().isoformat())
            },
            'priority': 'high',
            'created_at': datetime.now().isoformat()
        }
        
        redis_client.lpush('publishing_queue', json.dumps(task))
        
        logger.info(f"Triggered post publishing: {data['platform']} - {data.get('original_title', 'Manual post')}")
        
        return jsonify({
            'success': True,
            'message': 'Post publishing triggered successfully',
            'task_id': task['created_at']
        })
        
    except Exception as e:
        logger.error(f"Error triggering post publishing: {str(e)}")
        return jsonify({'error': 'Failed to trigger post publishing', 'details': str(e)}), 500

@app.route('/api/queue-stats', methods=['GET'])
def get_queue_stats():
    """Get current queue statistics"""
    try:
        stats = {
            'queues': {
                'blog_monitoring': redis_client.llen('blog_monitoring_queue') or 0,
                'content_processing': redis_client.llen('content_processing_queue') or 0,
                'publishing': redis_client.llen('publishing_queue') or 0
            },
            'completed': {
                'published_posts': redis_client.llen('published_posts') or 0,
                'processed_posts': redis_client.llen('processed_posts') or 0
            },
            'worker': {
                'status': 'running' if _check_worker_health() else 'stopped',
                'last_heartbeat': redis_client.get('worker:heartbeat')
            },
            'sessions': {
                'active_sessions': len(redis_client.keys('session:*')) or 0
            }
        }
        
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Error getting queue stats: {str(e)}")
        return jsonify({'error': 'Failed to get queue stats', 'details': str(e)}), 500

@app.route('/api/clear-queue', methods=['POST'])
def clear_queue():
    """Clear specific queue (for maintenance)"""
    try:
        data = request.get_json()
        queue_name = data.get('queue_name') if data else None
        
        if not queue_name:
            return jsonify({'error': 'Queue name is required'}), 400
        
        valid_queues = ['blog_monitoring_queue', 'content_processing_queue', 'publishing_queue']
        if queue_name not in valid_queues:
            return jsonify({'error': f'Invalid queue name. Valid queues: {valid_queues}'}), 400
        
        # Clear the queue
        cleared_count = redis_client.llen(queue_name)
        redis_client.delete(queue_name)
        
        logger.info(f"Cleared {cleared_count} items from {queue_name}")
        
        return jsonify({
            'success': True,
            'message': f'Cleared {cleared_count} items from {queue_name}',
            'queue_name': queue_name,
            'items_cleared': cleared_count
        })
        
    except Exception as e:
        logger.error(f"Error clearing queue: {str(e)}")
        return jsonify({'error': 'Failed to clear queue', 'details': str(e)}), 500

@app.route('/api/clear-session', methods=['POST'])
def clear_session():
    """Clear session data for a specific user and platform"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        user_id = data.get('user_id')
        platform = data.get('platform')
        
        if not user_id or not platform:
            return jsonify({'error': 'user_id and platform are required'}), 400
        
        # Clear session data
        session_manager.invalidate_session(user_id, platform)
        
        logger.info(f"Cleared session for {user_id}:{platform}")
        
        return jsonify({
            'success': True,
            'message': f'Session cleared for {user_id}:{platform}'
        })
        
    except Exception as e:
        logger.error(f"Error clearing session: {str(e)}")
        return jsonify({'error': 'Failed to clear session', 'details': str(e)}), 500

@app.route('/api/retest-connection', methods=['POST'])
def retest_connection():
    """Retest social media account connection"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        account_id = data.get('account_id')
        if not account_id:
            return jsonify({'error': 'account_id is required'}), 400
        
        # Get account details from Next.js API
        from integrations.utils.api_client import make_api_request
        account_data = make_api_request('GET', f'social-accounts/{account_id}')
        
        if not account_data:
            return jsonify({'error': 'Account not found'}), 404
        
        # We can't test with hashed password, so return instruction
        return jsonify({
            'success': False,
            'message': 'To test connection, please update the account with the password and set test_connection: true',
            'instructions': {
                'method': 'PUT',
                'url': f'/api/social-accounts/{account_id}',
                'body': {
                    'password': 'your_actual_password',
                    'test_connection': True
                }
            }
        })
        
    except Exception as e:
        logger.error(f"Error retesting connection: {str(e)}")
        return jsonify({'error': 'Failed to retest connection', 'details': str(e)}), 500

@app.route('/api/worker-control', methods=['POST'])
def worker_control():
    """Send control signals to background worker"""
    try:
        data = request.get_json()
        action = data.get('action') if data else None
        
        if not action:
            return jsonify({'error': 'Action is required'}), 400
        
        if action not in ['pause', 'resume', 'restart']:
            return jsonify({'error': 'Invalid action. Valid actions: pause, resume, restart'}), 400
        
        # Send control signal via Redis
        control_signal = {
            'action': action,
            'timestamp': datetime.now().isoformat(),
            'sender': 'api'
        }
        
        redis_client.set('worker:control', json.dumps(control_signal), ex=300)  # Expire in 5 minutes
        
        logger.info(f"Sent {action} signal to background worker")
        
        return jsonify({
            'success': True,
            'message': f'Sent {action} signal to background worker',
            'action': action
        })
        
    except Exception as e:
        logger.error(f"Error sending worker control signal: {str(e)}")
        return jsonify({'error': 'Failed to send control signal', 'details': str(e)}), 500
        
@app.route('/api/log-activity', methods=['POST'])
def log_activity():
    """Log automation activity for the dashboard"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        activity = {
            'type': data.get('type', 'system'),
            'message': data.get('message', ''),
            'status': data.get('status', 'info'),
            'timestamp': data.get('timestamp', datetime.now().isoformat()),
            'user_id': data.get('user_id', 'system'),
            'metadata': data.get('metadata', {})
        }
        
        # Store activity in Redis for retrieval
        activity_key = f"activity:{datetime.now().timestamp()}"
        redis_client.hset(activity_key, mapping=activity)
        redis_client.expire(activity_key, 86400)  # Expire after 24 hours
        
        # Add to activities list
        redis_client.lpush('activities', json.dumps(activity))
        redis_client.ltrim('activities', 0, 100)  # Keep only last 100 activities
        
        logger.info(f"Logged activity: {activity['type']} - {activity['message']}")
        
        return jsonify({'success': True, 'message': 'Activity logged successfully'})
        
    except Exception as e:
        logger.error(f"Error logging activity: {str(e)}")
        return jsonify({'error': 'Failed to log activity', 'details': str(e)}), 500

@app.route('/api/activities', methods=['GET'])
def get_activities():
    """Get recent automation activities"""
    try:
        # Get activities from Redis
        activities = []
        activity_list = redis_client.lrange('activities', 0, 50)  # Get last 50 activities
        
        for activity_json in activity_list:
            try:
                activity = json.loads(activity_json)
                activities.append(activity)
            except:
                continue
        
        return jsonify(activities)
        
    except Exception as e:
        logger.error(f"Error getting activities: {str(e)}")
        return jsonify({'error': 'Failed to get activities', 'details': str(e)}), 500

@app.route('/api/linkedin-token-status', methods=['POST'])
def linkedin_token_status():
    """Check if user has a valid LinkedIn token"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        
        if not user_id:
            return jsonify({'error': 'user_id is required'}), 400
        
        token_data = session_manager.redis.hgetall(f"linkedin_token:{user_id}")
        
        if token_data:
            access_token = token_data.get('access_token')
            expires_at = token_data.get('expires_at')

            if access_token and expires_at:
                try:
                    expires_datetime = datetime.fromisoformat(expires_at)
                    if expires_datetime > datetime.now():
                        return jsonify({
                            'has_valid_token': True,
                            'expires_at': expires_at
                        })
                except Exception as e:
                    logger.warning(f"Invalid date format in token for user {user_id}: {e}")
        
        return jsonify({
            'has_valid_token': False,
            'expires_at': None
        })
        
    except Exception as e:
        logger.error(f"Error checking LinkedIn token status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/linkedin-token-remove', methods=['POST'])
def linkedin_token_remove():
    """Remove LinkedIn token for user"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        
        if not user_id:
            return jsonify({'error': 'user_id is required'}), 400
        
        # Remove token from Redis
        token_key = f"linkedin_token:{user_id}"
        session_manager.redis.delete(token_key)
        
        logger.info(f"LinkedIn token removed for user {user_id}")
        
        return jsonify({
            'success': True,
            'message': 'LinkedIn token removed successfully'
        })
        
    except Exception as e:
        logger.error(f"Error removing LinkedIn token: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/linkedin-test-api', methods=['POST'])
def linkedin_test_api():
    """Test LinkedIn API connection for a user"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        
        if not user_id:
            return jsonify({'error': 'user_id is required'}), 400
        
        # Initialize LinkedIn API poster
        linkedin_poster = LinkedInAPIPoster(session_manager)
        
        # Test connection
        result = linkedin_poster.test_connection_with_session(
            user_id=user_id,
            platform=PlatformType.LINKEDIN,
            username='',  # Not needed for API
            password_encrypted=''  # Not needed for API
        )
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error testing LinkedIn API: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/store-linkedin-token', methods=['POST'])
def store_linkedin_token():
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        token_data = data.get('token_data')
        
        if not user_id or not token_data:
            return jsonify({'error': 'Missing user_id or token_data'}), 400
        
        # Store token using the LinkedIn API poster
        linkedin_poster = LinkedInAPIPoster(session_manager)
        linkedin_poster._store_access_token(user_id, token_data)
        
        return jsonify({'success': True, 'message': 'LinkedIn token stored successfully'})
        
    except Exception as e:
        logger.error(f"Error storing LinkedIn token: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/linkedin/profile', methods=['GET'])
def get_linkedin_profile():
    """Get LinkedIn user profile information"""
    try:
        # Get user_id from query parameters or JWT token
        user_id = request.args.get('user_id')
        if not user_id:
            return jsonify({'error': 'user_id parameter is required'}), 400
        
        # Initialize LinkedIn API poster
        session_manager = SocialSessionManager(redis_client)
        linkedin_poster = LinkedInAPIPoster(session_manager)
        
        # Get access token for the user
        access_token = linkedin_poster._get_access_token(user_id)
        print(access_token)
        if not access_token:
            return jsonify({
                'error': 'No valid LinkedIn access token found',
                'requires_oauth': True,
                'message': 'Please connect your LinkedIn account first'
            }), 401
        
        # Check if token is expired
        if linkedin_poster._is_token_expired(user_id):
            return jsonify({
                'error': 'LinkedIn access token has expired',
                'requires_oauth': True,
                'message': 'Please re-authenticate your LinkedIn account'
            }), 401
        
        # Get user profile
        profile_data = linkedin_poster._get_user_profile(access_token)
        print(profile_data)
        if profile_data:
            # Add some metadata
            response_data = {
                'success': True,
                'profile': profile_data,
                'retrieved_at': datetime.now().isoformat(),
                'user_id': user_id
            }
            
            logger.info(f"Successfully retrieved LinkedIn profile for user {user_id}")
            return jsonify(response_data)
        else:
            logger.warning(f"Failed to retrieve LinkedIn profile for user {user_id}")
            return jsonify({
                'error': 'Failed to retrieve LinkedIn profile',
                'message': 'Could not fetch profile data from LinkedIn API',
                'requires_oauth': True
            }), 404
            
    except Exception as e:
        logger.error(f"Error getting LinkedIn profile: {str(e)}")
        return jsonify({
            'error': 'Internal server error', 
            'details': str(e)
        }), 500


@app.route('/api/linkedin/profile/<user_id>', methods=['GET'])
def get_linkedin_profile_by_user_id(user_id):
    """Get LinkedIn user profile information by user ID (alternative endpoint)"""
    try:
        if not user_id:
            return jsonify({'error': 'user_id is required'}), 400
        
        # Initialize LinkedIn API poster
        session_manager = SocialSessionManager(redis_client)
        linkedin_poster = LinkedInAPIPoster(session_manager)
        
        # Check if token is expired first
        if linkedin_poster._is_token_expired(user_id):
            return jsonify({
                'error': 'LinkedIn access token has expired',
                'requires_oauth': True,
                'oauth_url': linkedin_poster.get_oauth_url(user_id, request.host_url + 'api/linkedin/callback'),
                'message': 'Please re-authenticate your LinkedIn account'
            }), 401
        
        # Get access token
        access_token = linkedin_poster._get_access_token(user_id)
        
        if not access_token:
            return jsonify({
                'error': 'No valid LinkedIn access token found',
                'requires_oauth': True,
                'oauth_url': linkedin_poster.get_oauth_url(user_id, request.host_url + 'api/linkedin/callback'),
                'message': 'Please connect your LinkedIn account first'
            }), 401
        
        # Get user profile
        profile_data = linkedin_poster._get_user_profile(access_token)
        
        if profile_data:
            # Enhance the response with additional metadata
            enhanced_profile = {
                'success': True,
                'profile': {
                    'id': profile_data.get('id'),
                    'firstName': profile_data.get('firstName'),
                    'lastName': profile_data.get('lastName'),
                    'fullName': f"{profile_data.get('firstName', '')} {profile_data.get('lastName', '')}".strip(),
                    'linkedinId': profile_data.get('id')
                },
                'metadata': {
                    'retrieved_at': datetime.now().isoformat(),
                    'user_id': user_id,
                    'api_version': 'v2',
                    'source': 'linkedin_api'
                }
            }
            
            logger.info(f"Successfully retrieved LinkedIn profile for user {user_id}: {enhanced_profile['profile']['fullName']}")
            return jsonify(enhanced_profile)
        else:
            logger.warning(f"LinkedIn API returned empty profile for user {user_id}")
            return jsonify({
                'error': 'Profile not found',
                'message': 'LinkedIn profile could not be retrieved',
                'user_id': user_id,
                'requires_oauth': True
            }), 404
            
    except Exception as e:
        logger.error(f"Error getting LinkedIn profile for user {user_id}: {str(e)}")
        return jsonify({
            'error': 'Internal server error', 
            'details': str(e),
            'user_id': user_id
        }), 500

@app.route('/api/linkedin/profile/batch', methods=['POST'])
def get_linkedin_profiles_batch():
    """Get LinkedIn profiles for multiple users"""
    try:
        data = request.get_json()
        if not data or 'user_ids' not in data:
            return jsonify({'error': 'user_ids array is required in request body'}), 400
        
        user_ids = data['user_ids']
        if not isinstance(user_ids, list) or len(user_ids) == 0:
            return jsonify({'error': 'user_ids must be a non-empty array'}), 400
        
        if len(user_ids) > 50:  # Limit batch size
            return jsonify({'error': 'Maximum 50 user_ids allowed per batch request'}), 400
        
        # Initialize LinkedIn API poster
        session_manager = SocialSessionManager(redis_client)
        linkedin_poster = LinkedInAPIPoster(session_manager)
        
        results = []
        
        for user_id in user_ids:
            try:
                # Check token expiration
                if linkedin_poster._is_token_expired(user_id):
                    results.append({
                        'user_id': user_id,
                        'success': False,
                        'error': 'Token expired',
                        'requires_oauth': True
                    })
                    continue
                
                # Get access token
                access_token = linkedin_poster._get_access_token(user_id)
                
                if not access_token:
                    results.append({
                        'user_id': user_id,
                        'success': False,
                        'error': 'No access token',
                        'requires_oauth': True
                    })
                    continue
                
                # Get profile
                profile_data = linkedin_poster._get_user_profile(access_token)
                
                if profile_data:
                    results.append({
                        'user_id': user_id,
                        'success': True,
                        'profile': {
                            'id': profile_data.get('id'),
                            'firstName': profile_data.get('firstName'),
                            'lastName': profile_data.get('lastName'),
                            'fullName': f"{profile_data.get('firstName', '')} {profile_data.get('lastName', '')}".strip()
                        }
                    })
                else:
                    results.append({
                        'user_id': user_id,
                        'success': False,
                        'error': 'Profile not found'
                    })
                    
                # Add small delay to avoid rate limiting
                time.sleep(0.1)
                
            except Exception as user_error:
                logger.error(f"Error processing user {user_id}: {str(user_error)}")
                results.append({
                    'user_id': user_id,
                    'success': False,
                    'error': f'Processing error: {str(user_error)}'
                })
        
        # Summary statistics
        successful = len([r for r in results if r.get('success')])
        failed = len(results) - successful
        
        response = {
            'success': True,
            'total_requested': len(user_ids),
            'successful': successful,
            'failed': failed,
            'results': results,
            'processed_at': datetime.now().isoformat()
        }
        
        logger.info(f"Batch profile retrieval completed: {successful}/{len(user_ids)} successful")
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error in batch LinkedIn profile retrieval: {str(e)}")
        return jsonify({
            'error': 'Internal server error', 
            'details': str(e)
        }), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({'error': 'Internal server error'}), 500

@app.errorhandler(400)
def bad_request(error):
    return jsonify({'error': 'Bad request'}), 400

if __name__ == '__main__':
    try:
        logger.info("🚀 Starting Blog Automation Flask API...")
        logger.info("Powered by fastApppy & Limiai")
        
        port = int(os.getenv('PORT', 5001))
        logger.info(f"🌐 Starting server on port {port}")
        
        # Send startup message to Rollbar
        if ROLLBAR_TOKEN:
            rollbar.report_message(
                f"Flask API started successfully on port {port}",
                level='info',
                extra_data={
                    'environment': ENVIRONMENT,
                    'port': port,
                    'startup': True
                }
            )
        
        app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
        
    except Exception as e:
        logger.error(f"❌ Failed to start application: {str(e)}")
        # Report startup failure to Rollbar
        if ROLLBAR_TOKEN:
            rollbar.report_exc_info(extra_data={
                'component': 'application_startup',
                'port': port
            })
        sys.exit(1)