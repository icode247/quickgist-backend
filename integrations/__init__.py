# integrations/__init__.py
"""
Blog automation modules

This package contains the core automation functionality:
- BlogMonitor: Monitors blogs for new posts
- ContentProcessor: Processes blog posts into social media content  
- SocialPoster: Posts content to social media platforms
"""

import sys
import os

# Add the parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Import main classes
try:
    from .content_processor import ContentProcessor
    from .social_poster import SocialPoster
    
    __all__ = ['ContentProcessor', 'SocialPoster']
    
except ImportError as e:
    print(f"Warning: Could not import automation modules: {e}")
    __all__ = []

# Version info
__version__ = "1.0.0"
__author__ = "Blog Automation System"
__license__ = "MIT"