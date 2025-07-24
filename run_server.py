# run_server.py - Unified Server Startup (Flask API + Background Worker)
import os
import sys
import logging
import threading
import time
import signal
from datetime import datetime
import subprocess

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, skip

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server_startup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class UnifiedServerManager:
    def __init__(self):
        self.flask_process = None
        self.worker_process = None
        self.running = True
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def start(self):
        """Start both Flask API and Background Worker"""
        logger.info("🚀 Starting Unified Blog Automation System")
        logger.info("Powered by fastApppy & Limiai")
        logger.info("=" * 60)
        
        try:
            # Run pre-flight checks
            if not self._run_preflight_checks():
                logger.error("❌ Pre-flight checks failed")
                return False
            
            # Start background worker first
            if not self._start_background_worker():
                logger.error("❌ Failed to start background worker")
                return False
            
            # Start Flask API
            if not self._start_flask_api():
                logger.error("❌ Failed to start Flask API")
                self._stop_background_worker()
                return False
            
            # Monitor both processes
            self._monitor_processes()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Server startup failed: {e}")
            self._cleanup()
            return False

    def _run_preflight_checks(self) -> bool:
        """Run pre-flight checks"""
        logger.info("🔍 Running pre-flight checks...")
        
        # Check Python dependencies
        required_packages = [
            'redis', 'flask', 'selenium', 'feedparser', 
            'requests', 'bs4', 'openai', 'flask_cors'
        ]
        
        missing_packages = []
        for package in required_packages:
            try:
                __import__(package.replace('-', '_'))
            except ImportError:
                missing_packages.append(package)
        
        if missing_packages:
            logger.error(f"❌ Missing packages: {', '.join(missing_packages)}")
            logger.error("Install with: pip install " + " ".join(missing_packages))
            return False
        
        logger.info("✅ All required packages are installed")
        
        # Check Redis connection
        try:
            import redis
            redis_client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                password=os.getenv('REDIS_PASSWORD', ''),
                db=0
            )
            redis_client.ping()
            logger.info("✅ Redis server is accessible")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            logger.error("Start Redis with: redis-server")
            return False
        
        # Check environment variables
        env_vars = {
            'OPENAI_API_KEY': 'OpenAI API key for content generation',
            'NEXTJS_API_URL': 'Next.js API base URL (default: http://localhost:3001/api)'
        }
        
        missing_env = []
        for var, description in env_vars.items():
            if not os.getenv(var):
                if var == 'OPENAI_API_KEY':
                    logger.warning(f"⚠️ {var} not set - AI content generation will use fallback")
                else:
                    missing_env.append(f"{var} ({description})")
        
        if missing_env:
            logger.warning(f"⚠️ Missing environment variables: {missing_env}")
        
        # Check file structure
        required_files = [
            'app.py',
            'background_worker.py',
            'automation/blog_monitor.py',
            'integrations/content_processor.py',
            'integrations/social_poster.py',
            'integrations/session_manager.py',
            'integrations/utils/api_client.py'
        ]
        
        missing_files = []
        for file_path in required_files:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
        
        if missing_files:
            logger.error(f"❌ Missing required files: {missing_files}")
            return False
        
        logger.info("✅ All required files are present")
        
        # Test imports
        try:
            from automation.blog_monitor import BlogMonitor
            from integrations.content_processor import ContentProcessor
            from integrations.social_poster import SocialPoster
            from integrations.utils.api_client import make_api_request
            logger.info("✅ All automation modules import successfully")
        except ImportError as e:
            logger.error(f"❌ Module import failed: {e}")
            return False
        
        logger.info("✅ All pre-flight checks passed")
        return True

    def _start_background_worker(self) -> bool:
        """Start the background worker process"""
        try:
            logger.info("🔄 Starting background worker...")
            
            # Start worker as separate process
            self.worker_process = subprocess.Popen(
                [sys.executable, 'background_worker.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=os.getcwd()
            )
            
            # Give worker time to start
            time.sleep(3)
            
            # Check if worker is still running
            if self.worker_process.poll() is None:
                logger.info("✅ Background worker started successfully")
                return True
            else:
                stdout, stderr = self.worker_process.communicate()
                logger.error(f"❌ Background worker failed to start:")
                logger.error(f"STDOUT: {stdout}")
                logger.error(f"STDERR: {stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error starting background worker: {e}")
            return False

    def _start_flask_api(self) -> bool:
        """Start the Flask API server"""
        try:
            logger.info("🌐 Starting Flask API server...")
            
            # Import and start Flask app in a thread
            from app import app
            
            port = int(os.getenv('PORT', 5001))
            host = os.getenv('HOST', '0.0.0.0')
            
            # Start Flask in a separate thread
            flask_thread = threading.Thread(
                target=lambda: app.run(host=host, port=port, debug=False, threaded=True),
                daemon=True
            )
            flask_thread.start()
            
            # Give Flask time to start
            time.sleep(2)
            
            # Test if Flask is responding
            import requests
            try:
                response = requests.get(f'http://localhost:{port}/api/health', timeout=5)
                if response.status_code == 200:
                    logger.info(f"✅ Flask API server started on http://localhost:{port}")
                    logger.info(f"📊 Health check: http://localhost:{port}/api/health")
                    return True
                else:
                    logger.error(f"❌ Flask health check failed: {response.status_code}")
                    return False
            except requests.RequestException as e:
                logger.error(f"❌ Flask API not responding: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error starting Flask API: {e}")
            return False

    def _monitor_processes(self):
        """Monitor both processes and restart if needed"""
        logger.info("👁️ Monitoring processes...")
        logger.info("📋 System Status:")
        logger.info("   - Flask API: Running")
        logger.info("   - Background Worker: Running")
        logger.info("   - Press Ctrl+C to stop")
        
        try:
            
            while self.running:
                # Check background worker
                if self.worker_process and self.worker_process.poll() is not None:
                    logger.error("❌ Background worker has stopped unexpectedly")
                    
                    # Try to restart worker
                    logger.info("🔄 Attempting to restart background worker...")
                    if self._start_background_worker():
                        logger.info("✅ Background worker restarted successfully")
                    else:
                        logger.error("❌ Failed to restart background worker")
                        break
                
                # Check worker health via Redis
                try:
                    import redis
                    redis_client = redis.Redis(
                        host=os.getenv('REDIS_HOST', 'localhost'),
                        port=int(os.getenv('REDIS_PORT', 6379)),
                        password=os.getenv('REDIS_PASSWORD', ''),
                        decode_responses=True
                    )
                    
                    last_heartbeat = redis_client.get('worker:heartbeat')
                    if last_heartbeat:
                        from datetime import datetime
                        last_time = datetime.fromisoformat(last_heartbeat)
                        time_diff = (datetime.now() - last_time).total_seconds()
                        
                        if time_diff > 300:  # 5 minutes
                            logger.warning("⚠️ Background worker heartbeat is stale")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Could not check worker heartbeat: {e}")
                
                # Sleep for monitoring interval
                time.sleep(30)  # Check every 30 seconds
                
        except KeyboardInterrupt:
            logger.info("🛑 Received shutdown signal")
        finally:
            self._cleanup()

    def _stop_background_worker(self):
        """Stop the background worker process"""
        if self.worker_process:
            try:
                logger.info("🛑 Stopping background worker...")
                self.worker_process.terminate()
                
                # Wait for graceful shutdown
                try:
                    self.worker_process.wait(timeout=10)
                    logger.info("✅ Background worker stopped gracefully")
                except subprocess.TimeoutExpired:
                    logger.warning("⚠️ Force killing background worker...")
                    self.worker_process.kill()
                    self.worker_process.wait()
                    logger.info("✅ Background worker force stopped")
                    
            except Exception as e:
                logger.error(f"Error stopping background worker: {e}")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}")
        self.running = False

    def _cleanup(self):
        """Clean up processes"""
        logger.info("🧹 Cleaning up...")
        self.running = False
        
        # Stop background worker
        self._stop_background_worker()
        
        # Flask will stop when main thread ends
        logger.info("✅ Cleanup completed")

    def show_status(self):
        """Show system status"""
        try:
            import redis
            import requests
            
            logger.info("📊 System Status Check:")
            
            # Check Redis
            try:
                redis_client = redis.Redis(
                    host=os.getenv('REDIS_HOST', 'localhost'),
                    port=int(os.getenv('REDIS_PORT', 6379)),
                    password=os.getenv('REDIS_PASSWORD', ''),
                )
                redis_client.ping()
                logger.info("   ✅ Redis: Connected")
            except:
                logger.info("   ❌ Redis: Disconnected")
            
            # Check Flask API
            try:
                port = int(os.getenv('PORT', 5001))
                response = requests.get(f'http://localhost:{port}/api/health', timeout=5)
                if response.status_code == 200:
                    health_data = response.json()
                    logger.info("   ✅ Flask API: Running")
                    logger.info(f"      - Queue stats: {health_data.get('queue_stats', {})}")
                else:
                    logger.info("   ❌ Flask API: Not responding")
            except:
                logger.info("   ❌ Flask API: Not accessible")
            
            # Check Background Worker
            try:
                redis_client = redis.Redis(
                    host=os.getenv('REDIS_HOST', 'localhost'),
                    port=int(os.getenv('REDIS_PORT', 6379)),
                    password=os.getenv('REDIS_PASSWORD', ''),
                    decode_responses=True
                )
                
                last_heartbeat = redis_client.get('worker:heartbeat')
                if last_heartbeat:
                    from datetime import datetime
                    last_time = datetime.fromisoformat(last_heartbeat)
                    time_diff = (datetime.now() - last_time).total_seconds()
                    
                    if time_diff < 120:  # 2 minutes
                        logger.info("   ✅ Background Worker: Running")
                        
                        # Show worker stats
                        stats = redis_client.hgetall('worker:stats')
                        if stats:
                            logger.info(f"      - Posts discovered: {stats.get('posts_discovered', 0)}")
                            logger.info(f"      - Posts generated: {stats.get('posts_generated', 0)}")
                            logger.info(f"      - Posts published: {stats.get('posts_published', 0)}")
                    else:
                        logger.info(f"   ⚠️ Background Worker: Stale heartbeat ({time_diff:.0f}s ago)")
                else:
                    logger.info("   ❌ Background Worker: No heartbeat")
            except:
                logger.info("   ❌ Background Worker: Status unknown")
                
        except Exception as e:
            logger.error(f"Error checking status: {e}")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Blog Automation Server')
    parser.add_argument('--status', action='store_true', help='Show system status only')
    parser.add_argument('--check', action='store_true', help='Run pre-flight checks only')
    
    args = parser.parse_args()
    
    manager = UnifiedServerManager()
    
    if args.status:
        manager.show_status()
        return
    
    if args.check:
        if manager._run_preflight_checks():
            logger.info("✅ All checks passed - system ready to start")
            sys.exit(0)
        else:
            logger.error("❌ Pre-flight checks failed")
            sys.exit(1)
    
    # Normal startup
    try:
        success = manager.start()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("🛑 Shutdown requested")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Server error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()