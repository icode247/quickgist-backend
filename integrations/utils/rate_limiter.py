"""
Rate limiting utilities to prevent API abuse
"""

import time
from typing import Dict, Optional
import redis
from datetime import datetime, timedelta

class RateLimiter:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        
        # Rate limits per platform (posts per hour)
        self.platform_limits = {
            'twitter': 50,    # Twitter allows ~300 tweets per 3 hours
            'linkedin': 20,   # LinkedIn is more restrictive
            'facebook': 25,   # Facebook has various limits
            'instagram': 10   # Instagram is very restrictive
        }
    
    def can_post(self, user_id: str, platform: str) -> Dict[str, any]:
        """Check if user can post to platform"""
        key = f"rate_limit:{user_id}:{platform}"
        current_hour = datetime.now().strftime("%Y-%m-%d-%H")
        hour_key = f"{key}:{current_hour}"
        
        # Get current count for this hour
        current_count = self.redis.get(hour_key)
        current_count = int(current_count) if current_count else 0
        
        limit = self.platform_limits.get(platform, 10)
        
        if current_count >= limit:
            return {
                'allowed': False,
                'reason': f'Rate limit exceeded for {platform}',
                'limit': limit,
                'current': current_count,
                'reset_time': self._get_next_hour()
            }
        
        return {
            'allowed': True,
            'limit': limit,
            'current': current_count,
            'remaining': limit - current_count
        }
    
    def record_post(self, user_id: str, platform: str):
        """Record a post for rate limiting"""
        key = f"rate_limit:{user_id}:{platform}"
        current_hour = datetime.now().strftime("%Y-%m-%d-%H")
        hour_key = f"{key}:{current_hour}"
        
        # Increment counter
        self.redis.incr(hour_key)
        # Set expiry for 2 hours (cleanup)
        self.redis.expire(hour_key, 7200)
    
    def _get_next_hour(self) -> str:
        """Get timestamp for next hour"""
        next_hour = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return next_hour.isoformat()
    
    def get_user_stats(self, user_id: str) -> Dict[str, Dict]:
        """Get rate limiting stats for user"""
        stats = {}
        current_hour = datetime.now().strftime("%Y-%m-%d-%H")
        
        for platform in self.platform_limits:
            key = f"rate_limit:{user_id}:{platform}:{current_hour}"
            current_count = self.redis.get(key)
            current_count = int(current_count) if current_count else 0
            
            stats[platform] = {
                'limit': self.platform_limits[platform],
                'used': current_count,
                'remaining': self.platform_limits[platform] - current_count
            }
        
        return stats
