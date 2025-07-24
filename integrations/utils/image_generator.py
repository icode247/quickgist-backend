# integrations/utils/image_generator.py - Social Media Image Generator
"""
Intelligent Image Generator for Social Media Posts
Integrates with OpenAI's DALL-E API to generate platform-specific images
"""

from cgi import print_form
import os
import requests
from datetime import datetime
from pathlib import Path
import logging

# Try importing the new OpenAI client first, fall back to old version
try:
    from openai import OpenAI
    OPENAI_V1 = True
except ImportError:
    import openai
    OPENAI_V1 = False

logger = logging.getLogger(__name__)

class CoverImageGenerator:
    def __init__(self, api_key=None):
        """
        Initialize the cover image generator
        
        Args:
            api_key (str): OpenAI API key. If None, will try to get from environment
        """
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable or pass api_key parameter")
        
        if OPENAI_V1:
            self.client = OpenAI(api_key=self.api_key)
        else:
            openai.api_key = self.api_key
            self.client = None
        
        logger.info(f"Image generator initialized with OpenAI library version: {'v1.0+' if OPENAI_V1 else 'legacy (<1.0)'}")
    
    def generate_cover_image(self, 
                           prompt, 
                           size="1024x1024", 
                           quality="standard", 
                           style="natural",
                           n=1,
                           save_dir="cover_images"):
        """
        Generate a cover image using DALL-E
        
        Args:
            prompt (str): Description of the image to generate
            size (str): Image size - "1024x1024", "1792x1024", or "1024x1792"
            quality (str): Image quality - "standard" or "hd"
            style (str): Image style - "vivid" or "natural"
            n (int): Number of images to generate (1-10)
            save_dir (str): Directory to save images
            
        Returns:
            list: Paths to saved images
        """
        try:
            logger.info(f"Generating {n} cover image(s) with prompt: '{prompt[:100]}...'")
            
            # Generate image using DALL-E (compatible with both old and new API)
            if OPENAI_V1:
                # New API (openai >= 1.0)
                response = self.client.images.generate(
                    model="dall-e-3",
                    prompt=prompt,
                    size=size,
                    quality=quality,
                    style=style,
                    n=n
                )
            else:
                # Old API (openai < 1.0)
                # Note: DALL-E 3 and advanced parameters may not be available in older versions
                # Falls back to DALL-E 2 with basic parameters
                response = openai.Image.create(
                    prompt=prompt,
                    size=size if size in ["256x256", "512x512", "1024x1024"] else "1024x1024",
                    n=min(n, 10)  # Ensure n is within limits
                )
            
            # Create save directory if it doesn't exist
            save_path = Path(save_dir)
            save_path.mkdir(parents=True, exist_ok=True)
            
            saved_files = []
            
            # Handle response format differences between API versions
            if OPENAI_V1:
                images = response.data
            else:
                images = response['data']
            
            for i, image_data in enumerate(images):
                # Generate filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"cover_image_{timestamp}_{i+1}.png"
                file_path = save_path / filename
                
                # Get image URL (different attribute names in old vs new API)
                if OPENAI_V1:
                    image_url = image_data.url
                else:
                    image_url = image_data['url']
                
                # Download and save the image
                image_response = requests.get(image_url, timeout=30)
                image_response.raise_for_status()
                
                with open(file_path, 'wb') as f:
                    f.write(image_response.content)
                
                saved_files.append(str(file_path))
                logger.info(f"✓ Image saved: {file_path}")
                
                # Print the revised prompt if available (only in new API)
                if OPENAI_V1 and hasattr(image_data, 'revised_prompt'):
                    logger.debug(f"  Revised prompt: {image_data.revised_prompt}")
            
            return saved_files
            
        except Exception as e:
            logger.error(f"Error generating image: {str(e)}")
            return []
    
    def generate_social_media_image(self, title, platform, theme="", brand_colors="", style="professional"):
        """
        Generate social media specific image with platform optimizations
        
        Args:
            title (str): Main text or title for the image
            platform (str): Target platform (twitter, linkedin, facebook, etc.)
            theme (str): Theme or topic
            brand_colors (str): Brand color preferences
            style (str): Visual style preference
            
        Returns:
            list: Paths to saved images
        """
        # Platform-specific dimensions and optimizations
        platform_configs = {
            "twitter": {
                "size": "1792x1024",
                "style_suffix": "eye-catching social media graphic optimized for Twitter engagement",
                "text_emphasis": "prominent, bold text that's readable in Twitter feed"
            },
            "linkedin": {
                "size": "1792x1024", 
                "style_suffix": "professional business graphic suitable for LinkedIn",
                "text_emphasis": "clean, professional typography appropriate for business audience"
            },
            "facebook": {
                "size": "1792x1024",
                "style_suffix": "engaging social media visual optimized for Facebook",
                "text_emphasis": "friendly, approachable text design for diverse Facebook audience"
            },
            "instagram": {
                "size": "1024x1024",
                "style_suffix": "visually striking square image perfect for Instagram",
                "text_emphasis": "aesthetic typography that complements visual elements"
            },
            "youtube": {
                "size": "1792x1024",
                "style_suffix": "compelling thumbnail design that encourages clicks",
                "text_emphasis": "bold, attention-grabbing text for video thumbnails"
            },
            "tiktok": {
                "size": "1024x1792",
                "style_suffix": "trendy vertical design optimized for TikTok",
                "text_emphasis": "modern, youth-oriented typography"
            }
        }
        
        config = platform_configs.get(platform.lower(), platform_configs["twitter"])
        
        # Build the prompt
        prompt = f"Create a {style} {config['style_suffix']} with the text '{title}' prominently displayed using {config['text_emphasis']}."
        
        if theme:
            prompt += f" Theme: {theme}."
        
        if brand_colors:
            prompt += f" Color scheme: {brand_colors}."
        else:
            prompt += " Use modern, professional colors."
        
        prompt += " High quality design with clear hierarchy and excellent readability."
        
        return self.generate_cover_image(
            prompt=prompt, 
            size=config["size"],
            quality="standard",
            style="natural"
        )
    
    def generate_concept_image(self, main_concept, description, text_elements=None, style="modern illustration"):
        """
        Generate a conceptual image with integrated text and messaging
        
        Args:
            main_concept (str): The main concept or theme
            description (str): Detailed description of the visual concept
            text_elements (list): List of text elements to include in the image
            style (str): Visual style (e.g., "modern illustration", "infographic", "split-screen comparison")
            
        Returns:
            list: Paths to saved images
        """
        prompt = f"Conceptual illustration showing {main_concept}. {description}"
        
        if text_elements:
            text_list = ", ".join([f'"{text}"' for text in text_elements])
            prompt += f". Include visible text elements: {text_list}"
        
        prompt += f". {style} style with clean typography and professional design."
        
        return self.generate_cover_image(prompt, size="1024x1024")
    
    def generate_comparison_image(self, left_concept, right_concept, left_title, right_title, description=""):
        """
        Generate a split-screen comparison image like "PAPERS vs AUTOMATION"
        
        Args:
            left_concept (str): Description of left side concept
            right_concept (str): Description of right side concept  
            left_title (str): Title text for left side
            right_title (str): Title text for right side
            description (str): Additional context
            
        Returns:
            list: Paths to saved images
        """
        prompt = f'Split-screen comparison illustration. Left side: {left_concept} with large text "{left_title}" at bottom. '
        prompt += f'Right side: {right_concept} with large text "{right_title}" at bottom. '
        prompt += f'Clean, professional illustration style with clear visual contrast between concepts. {description}'
        
        return self.generate_cover_image(prompt, size="1792x1024")
    
    def generate_business_concept_image(self, concept_title, visual_description, key_message="", style="professional illustration"):
        """
        Generate a business/tech concept image with integrated text
        
        Args:
            concept_title (str): Main title to be prominently displayed
            visual_description (str): Detailed description of the visual elements
            key_message (str): Additional text or message to include
            style (str): Visual style description
            
        Returns:
            list: Paths to saved images
        """
        prompt = f'Professional {style} with large, prominent text "{concept_title}". '
        prompt += f'{visual_description}. '
        
        if key_message:
            prompt += f'Include additional text: "{key_message}". '
        
        prompt += 'Clean typography, modern design, suitable for business presentations and marketing materials.'
        
        return self.generate_cover_image(prompt, size="1024x1024")

    def generate_infographic_cover(self, title, key_points, theme, color_scheme="professional blue"):
        """
        Generate an infographic-style cover image
        
        Args:
            title (str): Main title for the infographic
            key_points (list): Key points or statistics to highlight
            theme (str): Theme or subject matter
            color_scheme (str): Color scheme description
            
        Returns:
            list: Paths to saved images
        """
        points_text = ", ".join([f'"{point}"' for point in key_points])
        prompt = f'Infographic cover design with title "{title}" prominently displayed. '
        prompt += f'Theme: {theme}. Include key text elements: {points_text}. '
        prompt += f'{color_scheme} color scheme. Clean, modern infographic style with icons and visual elements.'
        
        return self.generate_cover_image(prompt, size="1024x1792")
    
    def generate_process_flow_image(self, process_title, steps, description="", style="modern workflow diagram"):
        """
        Generate a process flow or workflow image
        
        Args:
            process_title (str): Title of the process
            steps (list): List of process steps
            description (str): Additional context
            style (str): Visual style
            
        Returns:
            list: Paths to saved images
        """
        steps_text = " → ".join([f'"{step}"' for step in steps])
        prompt = f'{style} showing "{process_title}". '
        prompt += f'Process flow: {steps_text}. '
        
        if description:
            prompt += f'{description}. '
        
        prompt += 'Clean, professional design with clear step progression and modern typography.'
        
        return self.generate_cover_image(prompt, size="1792x1024")
    
    def generate_stat_highlight_image(self, main_stat, stat_description, context="", style="data visualization"):
        """
        Generate an image highlighting a key statistic or metric
        
        Args:
            main_stat (str): The main statistic to highlight (e.g., "300%", "$1M", "10x")
            stat_description (str): Description of what the stat represents
            context (str): Additional context
            style (str): Visual style
            
        Returns:
            list: Paths to saved images
        """
        prompt = f'{style} prominently featuring the statistic "{main_stat}". '
        prompt += f'Context: {stat_description}. '
        
        if context:
            prompt += f'Additional context: {context}. '
        
        prompt += 'Bold, impactful design that makes the statistic the focal point with supporting visual elements.'
        
        return self.generate_cover_image(prompt, size="1024x1024")

    def generate_quote_image(self, quote_text, author="", context="", style="inspirational quote design"):
        """
        Generate an image featuring a quote or key message
        
        Args:
            quote_text (str): The quote or message to feature
            author (str): Quote author (optional)
            context (str): Additional context
            style (str): Visual style
            
        Returns:
            list: Paths to saved images
        """
        prompt = f'{style} featuring the quote: "{quote_text}". '
        
        if author:
            prompt += f'Attribution to: {author}. '
        
        if context:
            prompt += f'Context: {context}. '
        
        prompt += 'Elegant typography with complementary visual elements and professional design.'
        
        return self.generate_cover_image(prompt, size="1024x1024")

    def generate_before_after_image(self, before_concept, after_concept, transformation_title="", description=""):
        """
        Generate a before/after transformation image
        
        Args:
            before_concept (str): Description of the "before" state
            after_concept (str): Description of the "after" state
            transformation_title (str): Title for the transformation
            description (str): Additional context
            
        Returns:
            list: Paths to saved images
        """
        prompt = f'Before and after comparison illustration. '
        prompt += f'Before: {before_concept}. After: {after_concept}. '
        
        if transformation_title:
            prompt += f'Title: "{transformation_title}". '
        
        if description:
            prompt += f'{description}. '
        
        prompt += 'Clear visual contrast showing transformation with professional design and clear labeling.'
        
        return self.generate_cover_image(prompt, size="1792x1024")

    def generate_tip_or_insight_image(self, tip_title, tip_content, category="", style="educational graphic"):
        """
        Generate an image for a tip, insight, or educational content
        
        Args:
            tip_title (str): Title of the tip
            tip_content (str): Content of the tip
            category (str): Category or topic area
            style (str): Visual style
            
        Returns:
            list: Paths to saved images
        """
        prompt = f'{style} with title "{tip_title}". '
        prompt += f'Tip content: {tip_content}. '
        
        if category:
            prompt += f'Category: {category}. '
        
        prompt += 'Clean, educational design with clear hierarchy and easy-to-read typography.'
        
        return self.generate_cover_image(prompt, size="1024x1024")

    def batch_generate_platform_images(self, title, content_theme, platforms, style="professional", brand_colors=""):
        """
        Generate images for multiple platforms at once
        
        Args:
            title (str): Main title/message
            content_theme (str): Theme or topic
            platforms (list): List of platform names
            style (str): Visual style preference
            brand_colors (str): Brand color scheme
            
        Returns:
            dict: Dictionary mapping platform names to image paths
        """
        generated_images = {}
        
        for platform in platforms:
            try:
                logger.info(f"Generating image for {platform}...")
                
                images = self.generate_social_media_image(
                    title=title,
                    platform=platform,
                    theme=content_theme,
                    brand_colors=brand_colors,
                    style=style
                )
                
                if images:
                    generated_images[platform] = images[0]  # Take first generated image
                    logger.info(f"✓ Generated {platform} image: {images[0]}")
                else:
                    logger.warning(f"✗ Failed to generate image for {platform}")
                    
            except Exception as e:
                logger.error(f"Error generating image for {platform}: {e}")
                continue
        
        return generated_images

# Example usage and testing
if __name__ == "__main__":
    # Example usage
    try:
        generator = CoverImageGenerator()
        
        # Test basic image generation
        print("=== Testing Basic Image Generation ===")
        images = generator.generate_social_media_image(
            title="The Future of Work",
            platform="linkedin",
            theme="digital transformation and remote work",
            brand_colors="professional blue and white",
            style="modern and clean"
        )
        
        if images:
            print(f"Generated image: {images[0]}")
        else:
            print("Failed to generate image")
            
    except ValueError as e:
        print(f"Error: {e}")
        print("Please set your OpenAI API key as an environment variable:")
        print("export OPENAI_API_KEY='your-api-key-here'")
