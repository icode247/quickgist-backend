# integrations/content_processor.py - Updated with Engaging Content Prompts
import openai
import re
import random
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging
import os
import sys
import cloudinary
import cloudinary.uploader
import hashlib
import tempfile
from integrations.utils.unsplash_image_searcher import UnsplashDownloader

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from integrations.utils.api_client import make_api_request

# Import the image generator
try:
    from integrations.utils.image_generator import CoverImageGenerator
    IMAGE_GENERATION_AVAILABLE = True
except ImportError:
    IMAGE_GENERATION_AVAILABLE = False
    CoverImageGenerator = None

logger = logging.getLogger(__name__)

class ContentProcessor:
    def __init__(self):
        # Initialize OpenAI with new API
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.unsplash_api_key = os.getenv('UNSPLASH_API_KEY')
        if self.openai_api_key:
            openai.api_key = self.openai_api_key
            self.use_ai = True
            logger.info("✅ OpenAI API initialized")
        else:
            self.use_ai = False
            logger.warning("⚠️ No OpenAI API key found - using fallback content generation")
        
        # Initialize image generator
        self.image_generator = None
        if IMAGE_GENERATION_AVAILABLE and self.openai_api_key:
            try:
                self.image_generator = CoverImageGenerator(api_key=self.openai_api_key)
                logger.info("✅ Image generator initialized")
            except Exception as e:
                logger.warning(f"⚠️ Could not initialize image generator: {e}")
        else:
            logger.warning("⚠️ Image generation not available - missing dependencies or API key")
        
        self.unsplash_downloader = None
        if self.unsplash_api_key:
            try:
                self.unsplash_downloader = UnsplashDownloader(self.unsplash_api_key)
                logger.info("✅ Unsplash downloader initialized")
            except Exception as e:
                logger.warning(f"⚠️ Could not initialize Unsplash downloader: {e}")
        else:
            logger.warning("⚠️ Unsplash downloader not available - missing API key")
        
        # Get user settings from API (default values as fallback)
        self.default_settings = {
            'tone': 'professional',
            'include_hashtags': True,
            'include_emojis': True,
            'max_twitter_length': 250,
            'max_linkedin_length': 1500,
            'branding_enabled': True,
            'max_posts_per_day': 10,
            'auto_publish': True, 
            'post_template': '',
            'schedule_delay': 10,
            'custom_prompt': '',
            'posting_schedule': 'smart_spread',  
            'respect_posting_hours': True, 
            'posting_start_hour': 8,        
            'posting_end_hour': 10,
            'include_images': True,
            'image_style': 'professional',
            'brand_colors': 'professional blue and white',
            'generate_images_for_platforms': ['twitter', 'linkedin', 'facebook'],  
            'image_source': 'unsplash',      
            'platforms': {
                'twitter': {'enabled': False},
                'linkedin': {'enabled': True}, 
                'facebook': {'enabled': False}
            } 
        }
        
        # # Promotional messages for branding
        # self.promotional_messages = [
        #     "🔗 Apply to 100+ jobs instantly with FastApply: https://fastapply.co",
        #     "🧠 Create a stunning portfolio in 3 seconds using Limiai: https://limiai.vercel.app/",
        #     "🚀 FastApply: Apply to 150+ jobs daily across LinkedIn, Indeed & Glassdoor: https://fastapply.co",
        #     "💼 Land interviews faster with FastApply – job automation that works: https://fastapply.co",
        #     "🌐 Limiai turns your GitHub profile into a beautiful portfolio site in seconds: https://limiai.vercel.app/",
        # ]

    def _get_user_branding_message(self, settings: Dict) -> str:
        """Get user-specific branding message from settings"""
        try:
            # Check if user has custom branding enabled
            if not settings.get('branding_enabled', True):
                return ""
            
            # Get custom branding message from user settings
            custom_message = settings.get('custom_branding_message', '')
            if custom_message:
                return f"\n\n{custom_message}"
            
            # Get brand info for automatic message generation
            brand_name = settings.get('brand_name', '')
            brand_website = settings.get('brand_website', '')
            
            if brand_name and brand_website:
                return f"\n\n🌐 Learn more about {brand_name}: {brand_website}"
            elif brand_website:
                return f"\n\n🌐 Learn more: {brand_website}"
            
            # No branding if no info provided
            return ""
            
        except Exception as e:
            logger.error(f"Error getting user branding: {e}")
            return ""

    def _extract_image_concepts(self, title: str, content: str, settings: Dict) -> Dict[str, Any]:
        """Enhanced concept extraction with full content context"""
        try:
            smart_generator = SmartImagePromptGenerator()
            analysis = smart_generator.analyze_content_for_visuals(title, content)
            
            return {
                'main_theme': analysis['industry'],
                'visual_pattern': analysis['visual_pattern'],
                'key_concepts': analysis['key_concepts'],
                'emotional_tone': analysis['emotional_tone'],
                'target_audience': analysis['target_audience'],
                'description': content  # Include full content for prompt generation
            }
            
        except Exception as e:
            logger.error(f"Error in smart concept extraction: {e}")
            return self._fallback_extract_concepts(title, content)
            
    def _create_image_prompt(self, title: str, concepts: Dict, platform: str, 
                       content: str, settings: Dict) -> str:
        """Updated to use smart prompt generation"""
        
        # Initialize the smart generator
        smart_generator = SmartImagePromptGenerator()
        
        # Use the smart prompt generation (you'll need to pass content here)
        # For now, using the existing concepts, but ideally pass the full content
        content_text = content  
        
        return smart_generator.generate_smart_prompt(title, content_text, platform, settings)

    def _get_dalle_compatible_size(self, platform_size: str) -> str:
        """Convert platform size requirements to DALL-E compatible sizes"""
        # DALL-E 3 supports: 1024x1024, 1792x1024, 1024x1792
        
        size_mappings = {
            '1200x675': '1792x1024',   # Twitter/YouTube (landscape)
            '1200x627': '1792x1024',   # LinkedIn/Facebook (landscape)
            '1200x630': '1792x1024',   # Facebook (landscape)
            '1080x1080': '1024x1024',  # Instagram (square)
            '1280x720': '1792x1024',   # YouTube (landscape)
            '1080x1920': '1024x1792',  # TikTok (portrait)
        }
        
        return size_mappings.get(platform_size, '1024x1024')

    def _ensure_cloudinary_config(self):
        """Ensure Cloudinary is configured once"""
        if not hasattr(self, '_cloudinary_configured'):
            cloudinary.config(
                cloud_name=os.getenv('CLOUDINARY_CLOUD_NAME'),
                api_key=os.getenv('CLOUDINARY_API_KEY'),
                api_secret=os.getenv('CLOUDINARY_API_SECRET')
            )
            self._cloudinary_configured = True

    def _generate_images_for_platforms(self, content: str, platform: str, 
                                 user_id: str) -> Dict[str, Dict]:
        """Generate platform-specific images and upload directly to Cloudinary"""
        generated_images = {}
        
        if not self.image_generator:
            logger.warning("Image generator not available")
            return generated_images
        
        try:
            self._ensure_cloudinary_config()
            
            # Generate unique identifier for this content
            content_hash = hashlib.md5(f"{content}".encode()).hexdigest()[:8]
            
           
            try:
                prompt = f"Generate a catchy cover image that best describes this post: {content}"
                platform_spec = self.platform_image_specs.get(platform, self.platform_image_specs['twitter'])
                
                logger.info(f"🎨 Generating image for {platform}...")
                
                # Determine image size based on platform
                image_size = self._get_dalle_compatible_size(platform_spec['size'])
                    
                    # Generate image using temporary file
                with tempfile.TemporaryDirectory() as temp_dir:
                    image_paths = self.image_generator.generate_cover_image(
                        prompt=prompt,
                        size=image_size,
                        quality="standard",
                        style="natural",
                            n=1,
                            save_dir=temp_dir
                        )
                        
                if image_paths:
                    temp_image_path = image_paths[0]
                    logger.info(f"✅ Generated {platform} image")
                            
                    # Create filename for Cloudinary
                    filename = f"{user_id}_{platform}_{content_hash}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            
                    # Upload directly to Cloudinary from temp file
                    result = cloudinary.uploader.upload(
                        temp_image_path,
                        public_id=filename,
                        folder=f"social_media/{user_id}/{datetime.now().strftime('%Y-%m-%d')}",
                        resource_type="image"
                    )
                    # Temp file is automatically deleted when exiting the context
                        
                    image_url = result['secure_url']
                    logger.info(f"✅ Uploaded {platform} image to Cloudinary: {image_url}")
                            
                    generated_images[platform] = {
                                'path': None,
                                'url': image_url,
                                'description': content,
                                'prompt': prompt,
                             }
                else:
                    logger.warning(f"⚠️ Failed to generate image for {platform}")
                        
            except Exception as e:
                logger.error(f"Error generating image for {platform}: {e}")
                
            
            logger.info(f"🎨 Image generation complete. Generated {len(generated_images)} images.")
            return generated_images
            
        except Exception as e:
            logger.error(f"Error in image generation process: {e}")
            return {}

    def generate_unsplash_description(self, content: str) -> str:
        """Generate simple, descriptive image captions for Unsplash-style searches"""
        try:
            # Define the prompt for generating simple descriptions
            prompt = f"""
            Based on this content, generate a simple image description suitable for Unsplash search.
            
            Content: {content}
            
            Rules:
            1. Keep it under 10 words
            2. Use simple, descriptive language
            3. Focus on visual elements: people, objects, settings, colors
            4. Follow this pattern: [color/style] + [subject] + [action] + [setting]
            5. Examples:
            - "a black and white image of a person coding"
            - "developers working on laptops in modern office"
            - "woman typing on computer at desk"
            - "team collaborating around conference table"
            - "close up of hands on keyboard"
            
            Generate ONE simple description (no quotes, no explanations):
            """
            
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                max_tokens=50,
                temperature=0.3
            )
            
            description = response.choices[0].message.content
            return description
            
        except Exception as e:
            logger.error(f"Error generating Unsplash description: {e}")
            
            # Fallback descriptions based on common content types
            content_lower = content.lower()
            if any(word in content_lower for word in ['code', 'coding', 'developer', 'programming']):
                return "person coding on laptop"
            elif any(word in content_lower for word in ['business', 'meeting', 'team']):
                return "business team working together"
            elif any(word in content_lower for word in ['design', 'creative', 'art']):
                return "designer working on computer"
            elif any(word in content_lower for word in ['office', 'work', 'professional']):
                return "professional working at desk"
            else:
                return "person working on computer"

    def _generate_image_with_unsplash(self, content: str, platform: List[str], 
                                user_id: str) -> Dict[str, Dict]:
        try:
            content_hash = hashlib.md5(f"{content}".encode()).hexdigest()[:8]

            if not self.unsplash_downloader:
                logger.warning("❌ Unsplash downloader not initialized - missing API key")
                return {}
            
            # ✅ FIXED: Use actual Unsplash search instead of hardcoded URLs
            description = self.generate_unsplash_description(content)
            logger.info(f"🎨 Searching Unsplash for: {description}")
            
            # Generate image using Unsplash downloader with actual search
            photo_results = self.unsplash_downloader.get_search_urls(
                description, 
                count=1, 
                quality="regular",
                orientation="landscape"  # Default to landscape for most platforms
            )
            
            if not photo_results:
                logger.warning("❌ No image URLs found from Unsplash search")
                # ✅ FALLBACK: Use a generic business/tech search as last resort
                fallback_searches = [
                    "business team working together",
                    "modern office workspace", 
                    "professional meeting",
                    "technology and innovation",
                    "minimal office setup"
                ]
                
                for fallback_query in fallback_searches:
                    photo_results = self.unsplash_downloader.get_search_urls(
                        fallback_query, count=1, quality="regular"
                    )
                    if photo_results:
                        logger.info(f"✅ Found fallback image with query: {fallback_query}")
                        break
                
                if not photo_results:
                    logger.error("❌ No fallback images found")
                    return {}

            photo_url = photo_results[0].get('url')
            if not photo_url:
                logger.warning("❌ Unsplash result did not contain an image URL")
                return {}
                
            # Upload to Cloudinary
            result = self._upload_image_to_storage(photo_url, user_id, platform[0], content_hash)
            
            if not result:
                logger.warning("❌ Failed to upload image to Cloudinary")
                return {}
                
            # Return the image URL with metadata
            return {
                platform[0]: {
                    'path': None,
                    'url': result,
                    'description': photo_results[0].get('description', content),
                    'prompt': description,
                    'photographer': photo_results[0].get('photographer', 'Unsplash'),
                    'source': 'unsplash_search'
                }
            }
        except Exception as e:
            logger.error(f"Error generating image with Unsplash: {e}")
            return {}
    
    # def _generate_image_with_unsplash(self, content: str, platform: List[str], 
    #                                 user_id: str) -> Dict[str, Dict]:
    #     try:
    #         # Predefined image URLs to randomly select from
    #         image_urls = [
    #             "https://images.unsplash.com/photo-1630673489068-d329fa4e2767?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3NjkxODJ8MHwxfHNlYXJjaHwxfHxhJTIwZGl2ZXJzZSUyMHRlYW0lMjBicmFpbnN0b3JtaW5nJTIwYXJvdW5kJTIwYSUyMHdoaXRlYm9hcmR8ZW58MHx8fHwxNzUwOTU0NzQ4fDA&ixlib=rb-4.1.0&q=80&w=1080",
    #             "https://images.unsplash.com/photo-1657727534676-cac1bb160d64?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3NjkxODJ8MHwxfHNlYXJjaHwxfHxhJTIwcGVyc29uJTIwZm9jdXNpbmclMjBvbiUyMGN1c3RvbWVyJTIwdmFsdWUlMjBhdCUyMGRlc2t8ZW58MHx8fHwxNzUwOTU0NzU3fDA&ixlib=rb-4.1.0&q=80&w=1080",
    #             "https://images.unsplash.com/photo-1550309533-b1f8d1ff6112?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3NjkxODJ8MHwxfHNlYXJjaHwxfHxBJTIwbWluaW1hbGlzdCUyMG9mZmljZSUyMHdpdGglMjBkZXNpZ25lcnMlMjBjb2xsYWJvcmF0aW5nJTIwb24lMjBhJTIwcHJvamVjdC58ZW58MHx8fHwxNzUwOTU0Nzc3fDA&ixlib=rb-4.1.0&q=80&w=1080",
    #             "https://images.unsplash.com/photo-1717994818193-266ff93e3396?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3NjkxODJ8MHwxfHNlYXJjaHwxfHxtaW5pbWFsaXN0JTIwZGVzaWduJTIwdGVzdGluZyUyMG9uJTIwZGlnaXRhbCUyMGludGVyZmFjZXxlbnwwfHx8fDE3NTA5NTQ3ODN8MA&ixlib=rb-4.1.0&q=80&w=1080",
    #             "https://images.unsplash.com/photo-1629787177096-9fbe3e2ef6f3?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3NjkxODJ8MHwxfHNlYXJjaHwxfHxBJTIwZnV0dXJpc3RpYyUyMFVJJTIwZGVzaWduJTIwdGVhbSUyMGNvbGxhYm9yYXRpbmclMjBpbiUyMG1vZGVybiUyMG9mZmljZXxlbnwwfHx8fDE3NTA5NTQ4MDd8MA&ixlib=rb-4.1.0&q=80&w=1080",
    #             "https://images.unsplash.com/photo-1593852852535-4e895055d150?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3NjkxODJ8MHwxfHNlYXJjaHwxfHxBSSUyMGFnZW50cyUyMHJldm9sdXRpb25pemluZyUyMFVJJTIwZGVzaWduJTIwd2l0aCUyMGV2b2x2aW5nJTIwZnJvbnRlbmQlMjBwYXR0ZXJucy58ZW58MHx8fHwxNzUwOTU0ODE0fDA&ixlib=rb-4.1.0&q=80&w=1080",
    #             "https://images.unsplash.com/photo-1716703373020-17ff360924ee?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3NjkxODJ8MHwxfHNlYXJjaHwxfHxBJTIwZnV0dXJpc3RpYyUyMG9mZmljZSUyMHdpdGglMjBBSSUyMHRlY2hub2xvZ3klMjBpbiUyMGFjdGlvbnxlbnwwfHx8fDE3NTA5NTQ4MzV8MA&ixlib=rb-4.1.0&q=80&w=1080"
    #         ]
    #         content_hash = hashlib.md5(f"{content}".encode()).hexdigest()[:8]

    #         # Randomly select one of the predefined URLs
    #         selected_url = random.choice(image_urls)
    #         if not selected_url:
    #             logger.warning("❌ Unsplash result did not contain an image URL")
    #             return {}
                
    #         # Upload to Cloudinary
    #         result = self._upload_image_to_storage(selected_url, user_id, platform[0], content_hash)
                
    #         if not result:
    #             logger.warning("❌ Failed to upload image to Cloudinary")
    #             return {}
                
    #         # Return the image URL
    #         return {
    #             platform[0]: {
    #                 'path': None,
    #                 'url': result,
    #                 'description': content,
    #                 'prompt': content
    #             }
    #         }
    #     except Exception as e:
    #         logger.error(f"Error generating image with Unsplash: {e}")
    #         return {}
    
    def _upload_image_to_storage(self, image_path: str, user_id: str, platform: str, content_hash: str) -> str:
        """
        This method is now deprecated since we upload directly to Cloudinary
        Keeping for backward compatibility if needed
        """
        try:
            self._ensure_cloudinary_config()
            
            filename = f"{user_id}_{platform}_{content_hash}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            result = cloudinary.uploader.upload(
                image_path, 
                public_id=filename,
                folder=f"social_media/{user_id}/{datetime.now().strftime('%Y-%m-%d')}"
            )
            return result['secure_url']
            
        except Exception as e:
            logger.error(f"Error uploading image: {e}")
            return ""     

    def process_blog_post(self, post_data: Dict[str, Any], user_settings: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Process a blog post into multiple social media posts with optional image generation"""
        try:
            # Get user settings
            settings = self._get_user_settings(post_data.get('user_id'), user_settings)
            title = post_data.get('title', '')
            content = post_data.get('content', '')
            url = post_data.get('url', '')
            user_id = post_data.get('user_id', 'default')
            
            # CRITICAL: Check if we should generate posts at all
            if not self._should_generate_posts(settings, user_id, title, content):
                logger.info(f"❌ Skipping post generation for user {user_id} - validation failed")
                return []
            
            # Get connected platforms
            connected_platforms = self._get_connected_social_accounts(user_id)
            if not connected_platforms:
                logger.warning(f"❌ No connected platforms for user {user_id}")
                return []
            
            logger.info(f"Connected platforms for user {user_id}: {connected_platforms}")
            
            # CRITICAL: Check daily quota BEFORE generating any posts
            posts_to_generate = len(connected_platforms)  # One post per platform
            if not self._check_daily_quota(user_id, posts_to_generate, settings):
                logger.warning(f"❌ Daily quota exceeded for user {user_id}. Would generate {posts_to_generate} posts.")
                return []
            
            logger.info(f"✅ Daily quota check passed for user {user_id}. Generating {posts_to_generate} posts.")
            
            # Generate posts for each enabled platform
            generated_posts = []
            
            for platform in connected_platforms:
                try:
                    if self.use_ai:
                        post = self._generate_ai_post(title, content, url, platform, settings, user_id)
                    else:
                        post = self._generate_fallback_post(title, content, url, platform, settings, user_id)
                    
                    if post:
                        generated_posts.append(post)
                        
                except Exception as e:
                    logger.error(f"Error generating {platform} post: {e}")
                    # Generate fallback post
                    fallback_post = self._generate_fallback_post(title, content, url, platform, settings, user_id)
                    if fallback_post:
                        generated_posts.append(fallback_post)
            
            # Only proceed with scheduling if we actually generated posts
            if not generated_posts:
                logger.warning(f"❌ No posts were generated for: {title}")
                return []
            
            logger.info(f"✅ Generated {len(generated_posts)} posts for: {title}")
            
            # Schedule posts based on user preferences
            scheduled_posts = self._schedule_posts(generated_posts, settings)
            
            # Save posts to API
            saved_posts = []
            for post in scheduled_posts:
                saved_post = self._save_generated_post_to_api(post)
                if saved_post:
                    saved_posts.append(saved_post)
            
            logger.info(f"✅ Successfully processed blog post '{title}': {len(saved_posts)} posts saved and scheduled")
            return saved_posts
            
        except Exception as e:
            logger.error(f"Error processing blog post: {e}")
            return []
    
    def _should_generate_posts(self, settings: Dict, user_id: str, title: str, content: str) -> bool:
        """Enhanced validation with comprehensive settings check"""
        try:
            # Check if content generation is enabled
            # content_generation_enabled = settings.get('content_generation', {}).get('enabled', True)
            # logger.info(content_generation_enabled)
            # if not content_generation_enabled:
            #     logger.info(f"Content generation disabled for user {user_id}")
            #     return False
            
            # Check if auto-publish is enabled
            auto_publish = settings.get('auto_publish', False)
            if not auto_publish:
                logger.info(f"Auto-publish disabled for user {user_id}")
                return False
            
            # Check if any platforms are enabled
            enabled_platforms = [p for p, config in settings.get('platforms', {}).items() 
                            if config.get('enabled', False)]
            if not enabled_platforms:
                logger.info(f"No platforms enabled for user {user_id}")
                return False
            
            # Check posting time restrictions
            if not self._validate_posting_time(settings):
                logger.info(f"Outside posting hours for user {user_id}")
                return False
            
            # Check content length
            min_words = settings.get('min_word_count', 50)
            if not content or len(content.strip().split()) < min_words:
                logger.warning(f"Content too short for user {user_id}: {len(content.strip().split())} words < {min_words}")
                return False
            
            # Check for spam/inappropriate content patterns
            spam_patterns = [
                r'(?i)(buy now|click here|limited time|act fast)',
                r'(?i)(make money|get rich|earn \$)',
                r'(?i)(free money|100% guaranteed|no risk)',
            ]
            
            full_text = f"{title} {content}".lower()
            for pattern in spam_patterns:
                if re.search(pattern, full_text):
                    logger.warning(f"Content appears to be spam/promotional for user {user_id}")
                    return False
            
            logger.debug(f"✅ Content validation passed for user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error validating post generation: {e}")
            return False

    def _check_daily_quota(self, user_id: str, posts_to_generate: int, settings: Dict) -> bool:
        """Check if user hasn't exceeded daily posting quota"""
        try:
            max_posts_per_day = settings.get('max_posts_per_day', 10)
            
            logger.debug(f"Checking daily quota for user {user_id}: want to generate {posts_to_generate}, max per day: {max_posts_per_day}")
            
            # Get today's existing posts count from API
            today = datetime.now().strftime('%Y-%m-%d')
            response = make_api_request(
                'GET', 
                'posts', 
                params={
                    'user_id': user_id,
                    'date': today,
                    'count_only': 'true',
                    'status': 'published'
                }
            )
            
            existing_posts_today = 0
            if response:
                if isinstance(response, dict) and 'count' in response:
                    existing_posts_today = response['count']
                elif isinstance(response, list):
                    existing_posts_today = len(response)
                elif isinstance(response, int):
                    existing_posts_today = response
            
            total_posts_after = existing_posts_today + posts_to_generate
            
            if total_posts_after > max_posts_per_day:
                logger.warning(
                    f"❌ Daily quota exceeded for user {user_id}: "
                    f"{existing_posts_today} existing + {posts_to_generate} new = {total_posts_after} "
                    f"(max: {max_posts_per_day})"
                )
                return False
            
            logger.info(
                f"✅ Daily quota OK for user {user_id}: "
                f"{existing_posts_today} existing + {posts_to_generate} new = {total_posts_after} "
                f"(max: {max_posts_per_day})"
            )
            return True
            
        except Exception as e:
            logger.warning(f"Error checking daily quota: {e}, allowing post generation")
            return True  # Allow on error to avoid blocking legitimate usage

    def _get_connected_social_accounts(self, user_id: str) -> List[str]:
        """Enhanced social accounts fetching with platform validation"""
        try:
            # Call the API to get connected social accounts
            response = make_api_request(
                'GET', 
                'social-accounts', 
                params={
                    'user_id': user_id,
                    'active': 'true',
                    'connected': 'true'
                }
            )
            
            if response and isinstance(response, list):
                # Extract platform names from the response with validation
                platforms = []
                valid_social_platforms = ['twitter', 'linkedin', 'facebook', 'instagram', 'youtube', 'tiktok', 'threads', 'pinterest']
                
                for account in response:
                    if isinstance(account, dict) and 'platform' in account:
                        platform = account['platform'].lower()
                        
                        # Validate platform and ensure it's not already added
                        if platform in valid_social_platforms and platform not in platforms:
                            # Additional validation - check if account is actually connected
                            if account.get('connected', False) and account.get('active', True):
                                platforms.append(platform)
                            else:
                                logger.debug(f"Account {account.get('id')} for {platform} not properly connected")
                        else:
                            logger.debug(f"Filtered out invalid/duplicate platform: {platform}")
                
                logger.info(f"Found {len(platforms)} valid connected platforms for user {user_id}: {platforms}")
                return platforms
            
            elif response and isinstance(response, dict):
                # Handle single account response
                if 'platform' in response and response.get('connected', False):
                    platform = response['platform'].lower()
                    valid_social_platforms = ['twitter', 'linkedin', 'facebook', 'instagram', 'youtube', 'tiktok', 'threads', 'pinterest']
                    
                    if platform in valid_social_platforms:
                        return [platform]
            
            logger.warning(f"No connected social accounts found for user {user_id}")
            return []
            
        except Exception as e:
            logger.error(f"Error fetching connected social accounts for user {user_id}: {e}")
            return []
   
    def _get_user_settings(self, user_id: str, override_settings: Optional[Dict] = None) -> Dict[str, Any]:
        """Enhanced user settings mapping with comprehensive coverage"""
        settings = self.default_settings.copy()
        
        if override_settings:
            settings.update(override_settings)
            return settings
        
        try:
            # Get user settings from API
            response = make_api_request('GET', 'settings', params={'user_id': user_id})
            
            if response and isinstance(response, dict):
                # =====================================================================================
                # AUTOMATION SETTINGS MAPPING (Enhanced)
                # =====================================================================================
                if 'automation' in response:
                    automation = response['automation']
                    settings.update({
                        'max_posts_per_day': automation.get('maxPostsPerDay', settings['max_posts_per_day']),
                        'auto_publish': automation.get('autoPublish', settings['auto_publish']),
                        'include_images': automation.get('includeImages', settings['include_images']),
                        'blog_check_interval': automation.get('blogCheckInterval', 30),
                        'content_generation_enabled': automation.get('contentGenerationEnabled', True),
                        'retry_failed_posts': automation.get('retryFailedPosts', True),
                        'max_retry_attempts': automation.get('maxRetryAttempts', 3),
                    })
                
                # =====================================================================================
                # CONTENT SETTINGS MAPPING (Enhanced)
                # =====================================================================================
                if 'content' in response:
                    content_settings = response['content']
                    settings.update({
                        'max_twitter_length': content_settings.get('maxWordCount', settings['max_twitter_length']),
                        'max_linkedin_length': content_settings.get('maxWordCount', settings['max_linkedin_length']),
                        'min_word_count': content_settings.get('minWordCount', 50),
                        'include_emojis': content_settings.get('includeEmojis', settings['include_emojis']),
                        'custom_prompt': content_settings.get('customPrompt', settings['custom_prompt']),
                        'tone': content_settings.get('tone', settings['tone']),
                        'writing_style': content_settings.get('writingStyle', 'informative'),
                        'branding_enabled': content_settings.get('brandingEnabled', True),
                        'brand_name': content_settings.get('brandName', ''),
                        'brand_voice': content_settings.get('brandVoice', ''),
                        
                        # ✅ NEW: Enhanced branding settings
                        'brand_website': content_settings.get('brandWebsite', ''),
                        'custom_branding_message': content_settings.get('customBrandingMessage', ''),
                        'branding_placement': content_settings.get('brandingPlacement', 'end'),
                        'branding_frequency': content_settings.get('brandingFrequency', 'sometimes'),
                        
                        'include_questions': content_settings.get('includeQuestions', True),
                        'include_call_to_action': content_settings.get('includeCallToAction', True),
                        'content_categories': content_settings.get('contentCategories', [])
                    })
                
                # =====================================================================================
                # SOCIAL SETTINGS MAPPING (Enhanced)
                # =====================================================================================
                if 'social' in response:
                    social_settings = response['social']
                    settings.update({
                        'include_hashtags': bool(social_settings.get('defaultHashtags', '')),
                        'default_hashtags': social_settings.get('defaultHashtags', ''),
                        'post_template': social_settings.get('postTemplate', settings['post_template']),
                        'schedule_delay': social_settings.get('scheduleDelay', settings['schedule_delay']),
                        'posting_schedule': social_settings.get('postingSchedule', settings['posting_schedule']),
                        'respect_posting_hours': social_settings.get('respectPostingHours', True),
                        'posting_start_hour': social_settings.get('postingStartHour', 8),
                        'posting_end_hour': social_settings.get('postingEndHour', 22),
                    })
                    
                    # Platform settings mapping
                    if 'platforms' in social_settings:
                        settings['platforms'] = {}
                        for platform, config in social_settings['platforms'].items():
                            settings['platforms'][platform] = {
                                'enabled': config.get('enabled', False),
                                'character_limit': config.get('characterLimit', self._get_default_character_limit(platform)),
                            }
                
                # =====================================================================================
                # IMAGE SETTINGS MAPPING (Enhanced)
                # =====================================================================================
                if 'images' in response:
                    image_settings = response['images']
                    settings.update({
                        'image_generation_enabled': image_settings.get('enabled', True),
                        'image_source': image_settings.get('source', 'unsplash'),
                        'image_style': image_settings.get('style', 'professional'),
                        'brand_colors': image_settings.get('brandColors', 'professional blue and white'),
                        'generate_images_for_platforms': image_settings.get('generateForPlatforms', ['twitter', 'linkedin', 'facebook']),
                        'image_prompt_style': image_settings.get('imagePromptStyle', 'descriptive'),
                        'preferred_orientation': image_settings.get('preferredOrientation', 'landscape')
                    })
                    
                    # Override include_images with image generation setting
                    settings['include_images'] = settings['image_generation_enabled']
                
                # =====================================================================================
                # LIMITS SETTINGS MAPPING (Enhanced)
                # =====================================================================================
                if 'limits' in response:
                    limits_settings = response['limits']
                    settings.update({
                        'daily_post_limit': limits_settings.get('dailyPostLimit', 10),
                        'hourly_post_limit': limits_settings.get('hourlyPostLimit', 5),
                        'enable_rate_limiting': limits_settings.get('enableRateLimiting', True),
                        'respect_platform_limits': limits_settings.get('respectPlatformLimits', True)
                    })
                    
                    # Sync daily limits
                    if settings['daily_post_limit'] < settings['max_posts_per_day']:
                        settings['max_posts_per_day'] = settings['daily_post_limit']
                
                # =====================================================================================
                # GENERAL SETTINGS MAPPING (Enhanced)
                # =====================================================================================
                if 'general' in response:
                    general_settings = response['general']
                    settings.update({
                        'timezone': general_settings.get('timezone', 'UTC'),
                        'language': general_settings.get('language', 'en'),
                        'theme': general_settings.get('theme', settings['tone']),  # Map theme to tone
                        'notifications_enabled': general_settings.get('notifications', True)
                    })
                
                # =====================================================================================
                # CONNECTED PLATFORMS DETECTION (Critical for Multi-User)
                # =====================================================================================
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
                        connected_platforms = [
                            acc['platform'].lower() for acc in accounts_response 
                            if isinstance(acc, dict) and acc.get('platform') and acc.get('connected')
                        ]
                        logger.info(f"Connected platforms for user {user_id}: {connected_platforms}")
                except Exception as e:
                    logger.warning(f"Could not fetch connected accounts for user {user_id}: {e}")
                
                # Enable platforms based on connections AND auto_publish setting
                if settings['auto_publish'] and connected_platforms:
                    for platform in connected_platforms:
                        if platform in settings['platforms']:
                            settings['platforms'][platform]['enabled'] = True
                            logger.info(f"✅ Enabled {platform} for user {user_id}")
                
                logger.info(f"✅ Mapped comprehensive settings for user {user_id}")
                
            else:
                logger.info(f"No custom settings found for user {user_id}, using defaults")
        
        except Exception as e:
            logger.warning(f"Error loading settings for user {user_id}: {e}, using defaults")
        
        return settings
    
    def _get_default_character_limit(self, platform: str) -> int:
        """Get default character limit for platform"""
        limits = {
            'twitter': 280,
            'linkedin': 3000,
            'facebook': 63206,
            'instagram': 2200,
            'youtube': 5000,
            'tiktok': 2200
        }
        return limits.get(platform.lower(), 2800)

    def _validate_posting_time(self, user_settings: Dict[str, Any]) -> bool:
        """Check if current time is within allowed posting hours"""
        try:
            if not user_settings.get('respect_posting_hours', True):
                return True
            
            import pytz
            from datetime import datetime
            
            # Get user timezone
            timezone_str = user_settings.get('timezone', 'UTC')
            try:
                user_tz = pytz.timezone(timezone_str)
            except:
                user_tz = pytz.UTC
            
            # Get current hour in user timezone
            now = datetime.now(user_tz)
            current_hour = now.hour
            
            start_hour = user_settings.get('posting_start_hour', 8)
            end_hour = user_settings.get('posting_end_hour', 22)
            
            # Handle overnight posting windows (e.g., 22 to 6)
            if start_hour <= end_hour:
                return start_hour <= current_hour <= end_hour
            else:
                return current_hour >= start_hour or current_hour <= end_hour
            
        except Exception as e:
            logger.warning(f"Error validating posting time: {e}, allowing post")
            return True
    
    def _generate_ai_post(self, title: str, content: str, url: str, platform: str, 
                         settings: Dict, user_id: str) -> Optional[Dict[str, Any]]:
        """Generate social media post using OpenAI"""
        try:
            # Prepare content for AI
            content_preview = content[:1200] if content else ""  # Increased for better context
            
            # Create platform-specific prompt
            prompt = self._create_platform_prompt(title, content_preview, platform, settings)

            # Call OpenAI API (new format)
            response = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": self._get_system_prompt(platform, settings)},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self._get_max_tokens_for_platform(platform),
                temperature=0.7,
                presence_penalty=0.1,
                frequency_penalty=0.1
            )
            
            generated_content = response.choices[0].message.content.strip()
            
            # Post-process the generated content
            final_content = self._post_process_content(generated_content, url, platform, settings)
            
            # Create post data structure
            post_data = {
                'content': final_content,
                'platform': platform,
                'user_id': user_id,
                'original_title': title,
                'original_url': url,
                'generation_type': 'ai',
                'status': 'generated',
                'created_at': datetime.now().isoformat(),
                'has_image': settings.get('generate_images', False) and settings.get('include_images', False)
            }
            
            # Generate images AFTER generating post, using the actual post content
            if settings.get('generate_images', True) and settings.get('include_images', True) and (self.image_generator or self.unsplash_downloader):
                logger.info(f"🎨 Generating image for {platform} post...")
                try:
                    image_source = settings.get('image_source', 'unsplash')
                    generated_image = None

                    if image_source == 'unsplash':
                        if self.unsplash_downloader:
                            logger.info(f"Using Unsplash to generate image for {platform} post...")
                            generated_image = self._generate_image_with_unsplash(final_content, [platform], user_id)
                        else:
                            logger.warning("Unsplash image source selected but downloader not available. Skipping image generation.")
                    else:  # dalle or other
                        if self.image_generator:
                            logger.info(f"Using DALL-E to generate image for {platform} post...")
                            generated_image = self._generate_images_for_platforms(final_content, platform, user_id)
                        else:
                            logger.warning("DALL-E (or other AI) image source selected but image_generator not available. Skipping image generation.")

                    if generated_image:
                        image_details = generated_image.get(platform)
                        if image_details:
                            post_data['image_path'] = image_details.get('url')
                            post_data['image_description'] = image_details.get('description')
                            post_data['has_image'] = bool(post_data.get('image_path'))
                        else:
                            logger.warning(f"Image generation for platform {platform} did not return image details.")

                except Exception as e:
                    logger.error(f"Error generating image for {platform} post: {e}")
            
            logger.debug(f"Generated AI post for {platform}: {final_content[:50]}...")
            return post_data
            
        except Exception as e:
            logger.error(f"OpenAI generation failed for {platform}: {e}")
            return None

    def _create_platform_prompt(self, title: str, content: str, platform: str, settings: Dict) -> str:
        """Enhanced platform prompt with comprehensive settings integration"""
        tone = settings.get('tone', 'professional')
        writing_style = settings.get('writing_style', 'informative')
        include_hashtags = settings.get('include_hashtags', True)
        include_emojis = settings.get('include_emojis', True)
        include_questions = settings.get('include_questions', True)
        include_cta = settings.get('include_call_to_action', True)
        custom_prompt = settings.get('custom_prompt', '')
        brand_name = settings.get('brand_name', '')
        brand_voice = settings.get('brand_voice', '')
        
        platform_lower = platform.lower()
        
        # Get platform-specific character limit
        platform_config = settings.get('platforms', {}).get(platform_lower, {})
        char_limit = platform_config.get('character_limit', self._get_default_character_limit(platform_lower))
        
        # Build enhanced base prompt based on writing style
        if writing_style == 'storytelling':
            style_instruction = "Write like you're telling a compelling story with a clear beginning, middle, and end"
        elif writing_style == 'engaging':
            style_instruction = "Write in an engaging, conversational style that encourages interaction"
        elif writing_style == 'listicle':
            style_instruction = "Structure your content as clear, numbered points or tips"
        elif writing_style == 'educational':
            style_instruction = "Write in an educational style that teaches and informs"
        else:  # informative
            style_instruction = "Write in a clear, informative style that provides valuable insights"
        
        # Platform-specific optimization
        if platform_lower == 'linkedin':
            base_prompt = f"""
    Transform this blog post into a {tone} LinkedIn post using a {writing_style} approach.

    Blog Title: {title}
    Blog Content: {content}

    {style_instruction}. Write for professionals and business leaders.

    Requirements:
    - Maximum {char_limit} characters
    - {tone} tone throughout
    - Include specific examples or insights
    - Make it valuable for business professionals
    """
            
        elif platform_lower == 'twitter':
            base_prompt = f"""
    Transform this blog post into a {tone} Twitter post using a {writing_style} approach.

    Blog Title: {title}
    Blog Content: {content}

    {style_instruction}. Write for a broad social media audience.

    Requirements:
    - Maximum {char_limit} characters ONLY
    - {tone} tone
    - Concise and impactful
    - Twitter-optimized format
    """
            
        elif platform_lower == 'facebook':
            base_prompt = f"""
    Transform this blog post into a {tone} Facebook post using a {writing_style} approach.

    Blog Title: {title}
    Blog Content: {content}

    {style_instruction}. Write for a diverse Facebook audience.

    Requirements:
    - Maximum {char_limit} characters
    - {tone} and {writing_style} style
    - Encourage discussion and engagement
    - Facebook-friendly format
    """
            
        else:
            # Generic platform
            base_prompt = f"""
    Transform this blog post into a {tone} {platform} post using a {writing_style} approach.

    Blog Title: {title}
    Blog Content: {content}

    {style_instruction}.

    Requirements:
    - Maximum {char_limit} characters
    - {tone} tone
    - {writing_style} writing style
    - Platform: {platform}
    """
        
        # Add branding instructions
        if settings.get('branding_enabled', True) and brand_name:
            base_prompt += f"\n\nBrand Context: You're posting for {brand_name}"
            if brand_voice:
                base_prompt += f" with a {brand_voice} brand voice"
        
        # Add content enhancement instructions
        enhancements = []
        if include_questions and include_cta:
            enhancements.append("Include an engaging question or call-to-action")
        elif include_questions:
            enhancements.append("Include an engaging question")
        elif include_cta:
            enhancements.append("Include a call-to-action")
        
        if include_hashtags:
            enhancements.append("Include relevant hashtags naturally")
        
        if include_emojis:
            enhancements.append("Use emojis where they enhance the message naturally")
        
        if enhancements:
            base_prompt += f"\n\nEnhancements: {', '.join(enhancements)}"
        
        # Add custom instructions if provided
        if custom_prompt:
            base_prompt += f"\n\nAdditional Instructions: {custom_prompt}"
        
        base_prompt += "\n\nIMPORTANT: Do NOT include any URLs in your response - they will be added separately."
        
        return base_prompt

    def _get_system_prompt(self, platform: str, settings: Dict) -> str:
        """Get system prompt for AI"""
        return f"""You are an expert content creator who specializes in transforming blog content into engaging {platform} posts. 

You excel at:
- Creating authentic, founder-style content that resonates
- Extracting key insights and lessons from longer content
- Writing in a conversational, no-fluff style
- Making content that drives genuine engagement
- Adapting tone and format for each platform's audience

Your content feels like it comes from someone who has real experience and is sharing genuine insights, not marketing copy. You focus on providing value and practical takeaways that readers can actually use.

Never include URLs in your generated content - they will be added separately.
"""

    def _get_max_tokens_for_platform(self, platform: str) -> int:
        """Get appropriate max tokens for platform"""
        token_limits = {
            'twitter': 150,      # Increased for better quality
            'linkedin': 400,     # Increased for detailed posts
            'facebook': 250,
            'instagram': 200,
            'youtube': 300,
            'tiktok': 150
        }
        return token_limits.get(platform.lower(), 150)

    def _post_process_content(self, generated_content: str, url: str, platform: str, settings: Dict) -> str:
        """Post-process AI-generated content - FIXED"""
        content = generated_content.strip()
        
        # Clean up formatting
        content = re.sub(r'\*\*', '', content)
        content = re.sub(r'—', '-', content)
        content = re.sub(r'️', '', content)
        
        # Remove any URLs that might have been generated
        content = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', content)
        
        # ✅ FIXED: Use user-configurable branding instead of hardcoded
        branding_message = self._get_user_branding_message(settings)
        if branding_message:
            content += branding_message
            
        return content

    def _generate_fallback_post(self, title: str, content: str, url: str, platform: str, 
                              settings: Dict, user_id: str) -> Dict[str, Any]:
        """Generate post without AI (fallback method)"""
        try:
            platform_lower = platform.lower()
            
            # Platform-specific templates
            if platform_lower == 'twitter':
                post_content = self._generate_twitter_fallback(title, content, url, settings)
            elif platform_lower == 'linkedin':
                post_content = self._generate_linkedin_fallback(title, content, url, settings)
            elif platform_lower == 'facebook':
                post_content = self._generate_facebook_fallback(title, content, url, settings)
            elif platform_lower == 'instagram':
                post_content = self._generate_instagram_fallback(title, content, url, settings)
            elif platform_lower == 'youtube':
                post_content = self._generate_youtube_fallback(title, content, url, settings)
            elif platform_lower == 'tiktok':
                post_content = self._generate_tiktok_fallback(title, content, url, settings)
            else:
                post_content = self._generate_generic_fallback(title, content, url, settings)
            
             # Generate image for fallback post too
            image_path = None
            if settings.get('include_images', True):
                image_source = settings.get('image_source', 'unsplash')
                generated_image = None
                if image_source == 'unsplash':
                    if self.unsplash_downloader:
                        logger.info(f"Using Unsplash to generate image for fallback {platform} post...")
                        generated_image = self._generate_image_with_unsplash(post_content, [platform], user_id)
                    else:
                        logger.warning("Unsplash selected but not available for fallback image.")
                else:
                    if self.image_generator:
                        logger.info(f"Using DALL-E to generate image for fallback {platform} post...")
                        generated_image = self._generate_images_for_platforms(post_content, platform, user_id)
                    else:
                        logger.warning("DALL-E selected but not available for fallback image.")

                if generated_image and generated_image.get(platform):
                    image_path = generated_image[platform].get('url')
         
            post_data = {
                'content': post_content,
                'platform': platform,
                'user_id': user_id,
                'original_title': title,
                'original_url': url,
                'generation_type': 'fallback',
                'status': 'generated',
                'created_at': datetime.now().isoformat(),
                'image_path': image_path, 
                'has_image': bool(image_path) 
            }
            
            logger.debug(f"Generated fallback post for {platform}")
            return post_data
            
        except Exception as e:
            logger.error(f"Error generating fallback post for {platform}: {e}")
            return None

    def _generate_twitter_fallback(self, title: str, content: str, url: str, settings: Dict) -> str:
        """Generate Twitter fallback post"""
        emojis = ["🚀", "✨", "💡", "🔥", "⚡"] if settings.get('include_emojis') else [""]
        hashtags = ["#Tech", "#Innovation", "#Blog"] if settings.get('include_hashtags') else []
        
        emoji = random.choice(emojis)
        
        post = f"{emoji} {title}"
        
        if len(post) > 200:  # Leave room for URL and hashtags
            post = f"{emoji} {title[:180]}..."
        
        if hashtags:
            post += f"\n\n{' '.join(hashtags)}"
        
        post += f"\n\n{url}"
        
        if settings.get('branding_enabled', True):
            post += f"\n\n{self._get_random_promo()}"
        
        return post

    def _generate_linkedin_fallback(self, title: str, content: str, url: str, settings: Dict) -> str:
        """Generate LinkedIn fallback post"""
        emoji = "📖" if settings.get('include_emojis') else ""
        
        post = f"{emoji} {title}\n\n"
        
        # Add content preview
        if content:
            preview = content[:300] + "..." if len(content) > 300 else content
            post += f"{preview}\n\n"
        
        post += "What are your thoughts on this?\n\n"
        
        if settings.get('include_hashtags'):
            post += "#Technology #Innovation #Insights\n\n"
        
        post += f"Read the full article: {url}"
        
        if settings.get('branding_enabled', True):
            post += f"\n\n{self._get_random_promo()}"
        
        return post

    def _generate_facebook_fallback(self, title: str, content: str, url: str, settings: Dict) -> str:
        """Generate Facebook fallback post"""
        emoji = "🎯" if settings.get('include_emojis') else ""
        
        post = f"{emoji} {title}\n\n"
        
        if content:
            preview = content[:200] + "..." if len(content) > 200 else content
            post += f"{preview}\n\n"
        
        post += f"Check it out: {url}"
        
        if settings.get('branding_enabled', True):
            post += f"\n\n{self._get_random_promo()}"
        
        return post

    def _generate_instagram_fallback(self, title: str, content: str, url: str, settings: Dict) -> str:
        """Generate Instagram fallback post"""
        emoji = "📸" if settings.get('include_emojis') else ""
        
        post = f"{emoji} {title}\n\n"
        
        if content:
            preview = content[:150] + "..." if len(content) > 150 else content
            post += f"{preview}\n\n"
        
        if settings.get('include_hashtags'):
            post += "#Instagram #Content #Blog\n\n"
        
        post += f"Link in bio: {url}"
        
        if settings.get('branding_enabled', True):
            post += f"\n\n{self._get_random_promo()}"
        
        return post

    def _generate_youtube_fallback(self, title: str, content: str, url: str, settings: Dict) -> str:
        """Generate YouTube fallback post"""
        emoji = "🎥" if settings.get('include_emojis') else ""
        
        post = f"{emoji} {title}\n\n"
        
        if content:
            preview = content[:400] + "..." if len(content) > 400 else content
            post += f"{preview}\n\n"
        
        post += "Don't forget to like and subscribe!\n\n"
        
        if settings.get('include_hashtags'):
            post += "#YouTube #Content #Subscribe\n\n"
        
        post += f"Watch here: {url}"
        
        if settings.get('branding_enabled', True):
            post += f"\n\n{self._get_random_promo()}"
        
        return post

    def _generate_tiktok_fallback(self, title: str, content: str, url: str, settings: Dict) -> str:
        """Generate TikTok fallback post"""
        emoji = "🎵" if settings.get('include_emojis') else ""
        
        post = f"{emoji} {title}\n\n"
        
        if content:
            preview = content[:100] + "..." if len(content) > 100 else content
            post += f"{preview}\n\n"
        
        if settings.get('include_hashtags'):
            post += "#TikTok #Viral #Content\n\n"
        
        post += f"Check it out: {url}"
        
        if settings.get('branding_enabled', True):
            post += f"\n\n{self._get_random_promo()}"
        
        return post

    def _generate_generic_fallback(self, title: str, content: str, url: str, settings: Dict) -> str:
        """Generate generic fallback post"""
        post = f"{title}\n\n"
        
        if content:
            preview = content[:300] + "..." if len(content) > 300 else content
            post += f"{preview}\n\n"
        
        post += f"Read more: {url}"
        
        if settings.get('branding_enabled', True):
            post += f"\n\n{self._get_random_promo()}"
        
        return post

    def _schedule_posts(self, posts: List[Dict], settings: Dict) -> List[Dict]:
        """Enhanced post scheduling with comprehensive time management"""
        schedule_type = settings.get('posting_schedule', 'smart_spread')
        schedule_delay = settings.get('schedule_delay', 30)
        max_posts_per_day = settings.get('max_posts_per_day', 10)
        respect_posting_hours = settings.get('respect_posting_hours', True)
        posting_start_hour = settings.get('posting_start_hour', 8)
        posting_end_hour = settings.get('posting_end_hour', 22)
        timezone_str = settings.get('timezone', 'UTC')
        
        # Get user timezone
        try:
            import pytz
            user_tz = pytz.timezone(timezone_str)
        except:
            user_tz = pytz.UTC
        
        base_time = datetime.now(user_tz)
        
        # If outside posting hours and we respect them, schedule for next allowed time
        if respect_posting_hours:
            current_hour = base_time.hour
            if not (posting_start_hour <= current_hour <= posting_end_hour):
                # Schedule for next allowed time
                if current_hour < posting_start_hour:
                    base_time = base_time.replace(hour=posting_start_hour, minute=0, second=0, microsecond=0)
                else:  # current_hour > posting_end_hour
                    base_time = (base_time + timedelta(days=1)).replace(hour=posting_start_hour, minute=0, second=0, microsecond=0)
        
        for i, post in enumerate(posts):
            if schedule_type == 'immediate':
                # Immediate posting with small delays between platforms
                scheduled_time = base_time + timedelta(minutes=i * 2)
                
            elif schedule_type == 'staggered':
                # Staggered over hours using schedule_delay
                scheduled_time = base_time + timedelta(minutes=i * schedule_delay)
                
            elif schedule_type == 'daily':
                # One per day
                scheduled_time = base_time + timedelta(days=i)
                # Ensure it's within posting hours
                if respect_posting_hours:
                    scheduled_time = scheduled_time.replace(hour=posting_start_hour, minute=0)
                
            elif schedule_type == 'auto_spread':
                # Automatically spread posts throughout the day
                posting_hours = posting_end_hour - posting_start_hour if respect_posting_hours else 24
                if posting_hours <= 0:
                    posting_hours = 14  # Default 14 hours
                
                posts_per_day = min(max_posts_per_day, len(posts))
                interval_hours = posting_hours / posts_per_day if posts_per_day > 0 else 1
                
                # Calculate day and position within day
                day_offset = i // posts_per_day
                post_index_in_day = i % posts_per_day
                
                target_date = base_time + timedelta(days=day_offset)
                hour_offset = post_index_in_day * interval_hours
                
                scheduled_time = target_date.replace(
                    hour=posting_start_hour if respect_posting_hours else 0,
                    minute=0,
                    second=0,
                    microsecond=0
                ) + timedelta(hours=hour_offset)
                
            elif schedule_type == 'smart_spread':
                # Smart scheduling based on optimal posting times for each platform
                platform = post.get('platform', 'twitter').lower()
                
                # Platform-specific optimal hours
                optimal_hours_by_platform = {
                    'linkedin': [9, 12, 17],  # Business hours
                    'twitter': [12, 15, 18],  # Peak social media times
                    'facebook': [13, 15, 20],  # Facebook peak times
                    'instagram': [11, 14, 17], # Instagram peak times
                    'youtube': [14, 16, 20],   # Video consumption times
                    'tiktok': [15, 18, 21]     # TikTok peak times
                }
                
                optimal_hours = optimal_hours_by_platform.get(platform, [9, 12, 15, 18, 20])
                
                # Filter optimal hours by posting restrictions
                if respect_posting_hours:
                    optimal_hours = [h for h in optimal_hours if posting_start_hour <= h <= posting_end_hour]
                
                if not optimal_hours:
                    optimal_hours = [posting_start_hour] if respect_posting_hours else [12]
                
                # Determine which day this post should go on
                posts_per_day = min(len(optimal_hours), max_posts_per_day)
                day_offset = i // posts_per_day
                post_index_in_day = i % posts_per_day
                
                # Get the optimal hour for this post
                hour = optimal_hours[post_index_in_day % len(optimal_hours)]
                
                # Calculate the target date
                target_date = base_time + timedelta(days=day_offset)
                
                # Set the scheduled time
                scheduled_time = target_date.replace(
                    hour=hour,
                    minute=random.randint(0, 59),  # Random minute for variety
                    second=0,
                    microsecond=0
                )
                
                # If this time has already passed today, move to tomorrow
                if scheduled_time <= base_time:
                    scheduled_time += timedelta(days=1)
                
                # Apply minimum schedule_delay if posts are too close together
                if i > 0:
                    previous_post_time = datetime.fromisoformat(posts[i-1]['scheduled_time'].replace('Z', '+00:00'))
                    min_next_time = previous_post_time + timedelta(minutes=schedule_delay)
                    if scheduled_time < min_next_time:
                        scheduled_time = min_next_time
            
            else:
                # Default to immediate
                scheduled_time = base_time + timedelta(minutes=i * 2)
            
            # Ensure scheduled time is in the future
            if scheduled_time <= datetime.now(user_tz):
                scheduled_time = datetime.now(user_tz) + timedelta(minutes=schedule_delay)
            
            # Convert to ISO format for storage
            post['scheduled_time'] = scheduled_time.isoformat()
            
            # Log the scheduling for debugging
            logger.debug(f"Post {i+1} scheduled for {scheduled_time} (mode: {schedule_type}, platform: {post.get('platform')})")
        
        return posts
    
    def _save_generated_post_to_api(self, post_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Save generated post to Next.js API"""
        try:

            # Ensure required fields are present
            if not post_data.get('content') or not post_data.get('platform'):
                logger.error("Missing required fields for post")
                return None
            response = make_api_request('POST', 'posts', data=post_data)
            
            if response and 'id' in response:
                logger.info(f"Saved generated post to API: {post_data['platform']} (ID: {response['id']})")
                return response
            else:
                logger.error(f"Failed to save generated post to API: {post_data.get('platform')}")
                return None
                
        except Exception as e:
            logger.error(f"Error saving generated post to API: {e}")
            return None

    # def _get_random_promo(self) -> str:
    #     """Get random promotional message"""
    #     return random.choice(self.promotional_messages)

