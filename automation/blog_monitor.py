# automation/blog_monitor.py - Enhanced with LinkedIn API integration
import feedparser
import requests
import json
import hashlib
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional
import sys
import os
import time
import re
from urllib.parse import urljoin, urlparse

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from integrations.utils.api_client import make_api_request
from automation.linkedin_scraper import ContentAnalyzer

logger = logging.getLogger(__name__)

class BlogMonitor:
   
    def __init__(self, redis_client):
        self.redis = redis_client 
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        })
        
        # Initialize content analyzer for LinkedIn posts
        self.content_analyzer = ContentAnalyzer()
        
        # LinkedIn API configuration
        self.linkedin_api_config = {
            'host': 'linkedin-api8.p.rapidapi.com',
            'key': os.getenv('RAPIDAPI_KEY', ''),
            'base_url': 'https://linkedin-api8.p.rapidapi.com'
        }

    def _extract_linkedin_username(self, url: str) -> Optional[str]:
        """Extract LinkedIn username from various LinkedIn URL formats"""
        try:
            # Clean the URL
            url = url.strip().rstrip('/')
            
            # Common LinkedIn URL patterns
            patterns = [
                r'linkedin\.com/in/([^/?]+)',           # /in/username
                r'linkedin\.com/posts/([^-]+)-',       # /posts/username-
                r'linkedin\.com/feed/update/.*',       # Activity feed URLs
                r'linkedin\.com/(?:pub/)?([^/?]+)',    # Legacy /pub/username
            ]
            
            for pattern in patterns:
                match = re.search(pattern, url, re.IGNORECASE)
                if match:
                    username = match.group(1)
                    # Clean username
                    username = username.split('/')[0].split('?')[0].split('#')[0]
                    logger.debug(f"Extracted LinkedIn username: {username} from URL: {url}")
                    return username
            
            # If no pattern matches, try to extract from path
            parsed = urlparse(url)
            path_parts = [part for part in parsed.path.split('/') if part]
            
            if path_parts:
                # Look for username-like parts (not 'in', 'posts', etc.)
                for part in path_parts:
                    if part not in ['in', 'posts', 'feed', 'update', 'company', 'school'] and len(part) > 2:
                        logger.debug(f"Extracted username from path: {part}")
                        return part
            
            logger.warning(f"Could not extract LinkedIn username from URL: {url}")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting LinkedIn username from {url}: {e}")
            return None

    def _call_linkedin_api(self, username: str) -> Optional[Dict[str, Any]]:
        """Call the RapidAPI LinkedIn API to get profile posts"""
        try:
            url = f"{self.linkedin_api_config['base_url']}/get-profile-posts"
            
            headers = {
                'x-rapidapi-key': self.linkedin_api_config['key'],
                'x-rapidapi-host': self.linkedin_api_config['host']
            }
            
            params = {
                'username': username
            }
            
            logger.info(f"Calling LinkedIn API for username: {username}")
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('success') and data.get('data'):
                posts = data['data']
                logger.info(f"✅ LinkedIn API returned {len(posts)} posts for {username}")
                return data
            else:
                logger.warning(f"LinkedIn API returned no posts or failed for {username}: {data}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"LinkedIn API request failed for {username}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LinkedIn API response for {username}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error calling LinkedIn API for {username}: {e}")
            return None

    def _convert_api_post_to_monitor_format(self, api_post: Dict[str, Any], monitor_id: str, user_id: str, source_url: str) -> Optional[Dict[str, Any]]:
        """Convert LinkedIn API post to monitor format with content analysis"""
        try:
            # Extract basic post data
            post_text = api_post.get('text', '').strip()
            
            if not post_text or len(post_text) < 30:
                logger.debug("Skipping post with insufficient content")
                return None
            
            # Analyze content using ContentAnalyzer
            analysis_result = self.content_analyzer.analyze_content(post_text)
            
            # Only process knowledge posts
            if not analysis_result.get('is_knowledge_post', False):
                logger.debug(f"Filtering out non-knowledge post: {analysis_result.get('reasoning', 'Not a knowledge post')}")
                return None
            
            # Get cleaned content
            cleaned_content = analysis_result.get('final_content', post_text)
            
            # Extract engagement metrics
            engagement_summary = self._format_engagement_summary(api_post)
            
            # Create content hash for deduplication
            post_url = api_post.get('postUrl', '')
            urn = api_post.get('urn', '')
            unique_identifier = post_url or urn or post_text
            content_hash = hashlib.md5(unique_identifier.encode()).hexdigest()
            
            # Extract author information
            author_data = api_post.get('author', {})
            author_name = f"{author_data.get('firstName', '')} {author_data.get('lastName', '')}".strip()
            if not author_name:
                author_name = author_data.get('username', 'LinkedIn User')
            
            # Parse posted date
            posted_date = self._parse_linkedin_date(api_post)
            
            # Calculate engagement score
            engagement_score = self._calculate_engagement_score_from_api(api_post)
            
            # Create monitor post format
            monitor_post = {
                'title': f"LinkedIn Knowledge Post - {analysis_result['category'].replace('_', ' ').title()}",
                'content': cleaned_content,
                'original_content': post_text,
                'url': post_url,
                'published_at': posted_date,
                'author': author_name,
                'monitor_id': monitor_id,
                'user_id': user_id,
                'platform': 'linkedin',
                'status': 'discovered',
                'source_type': 'linkedin_api',
                'engagement_summary': engagement_summary,
                'nlp_analysis': analysis_result,
                'content_hash': content_hash,
                'calculated_engagement_score': engagement_score,
                'api_data': {
                    'urn': urn,
                    'total_reactions': api_post.get('totalReactionCount', 0),
                    'comments_count': api_post.get('commentsCount', 0),
                    'reposts_count': api_post.get('repostsCount', 0),
                    'author_headline': author_data.get('headline', ''),
                    'author_username': author_data.get('username', ''),
                    'posted_at_relative': api_post.get('postedAt', ''),
                    'content_type': api_post.get('contentType', 'post')
                }
            }
            
            logger.info(f"✅ Converted LinkedIn API post to monitor format")
            logger.info(f"   Category: {analysis_result['category']}")
            logger.info(f"   Confidence: {analysis_result['confidence']:.2f}")
            logger.info(f"   Engagement Score: {engagement_score}")
            logger.debug(f"   Content: {cleaned_content[:100]}...")
            
            return monitor_post
            
        except Exception as e:
            logger.error(f"Error converting API post to monitor format: {e}")
            return None

    def _format_engagement_summary(self, api_post: Dict[str, Any]) -> str:
        """Format engagement metrics into a summary string"""
        try:
            metrics = []
            
            total_reactions = api_post.get('totalReactionCount', 0)
            if total_reactions > 0:
                metrics.append(f"{total_reactions} reactions")
            
            comments = api_post.get('commentsCount', 0)
            if comments > 0:
                metrics.append(f"{comments} comments")
            
            reposts = api_post.get('repostsCount', 0)
            if reposts > 0:
                metrics.append(f"{reposts} reposts")
            
            return ', '.join(metrics) if metrics else 'No engagement data'
            
        except Exception:
            return 'Engagement data unavailable'

    def _parse_linkedin_date(self, api_post: Dict[str, Any]) -> str:
        """Parse LinkedIn post date from API response"""
        try:
            # Try posted date timestamp first
            timestamp = api_post.get('postedDateTimestamp')
            if timestamp:
                # Convert from milliseconds to seconds
                if timestamp > 1000000000000:  # If in milliseconds
                    timestamp = timestamp / 1000
                return datetime.fromtimestamp(timestamp).isoformat()
            
            # Try posted date string
            posted_date = api_post.get('postedDate')
            if posted_date:
                # Try to parse various date formats
                date_formats = [
                    '%Y-%m-%d %H:%M:%S.%f %z %Z',
                    '%Y-%m-%d %H:%M:%S %z %Z',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d'
                ]
                
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(posted_date.split(' UTC')[0], fmt.replace(' %z %Z', ''))
                        return parsed_date.isoformat()
                    except ValueError:
                        continue
            
            # Fallback to current time
            logger.warning("Could not parse LinkedIn post date, using current time")
            return datetime.now().isoformat()
            
        except Exception as e:
            logger.warning(f"Error parsing LinkedIn date: {e}")
            return datetime.now().isoformat()

    def _is_linkedin_url(self, url: str) -> bool:
        """Check if the URL is a LinkedIn profile/activity URL"""
        return 'linkedin.com' in url.lower()

    def get_active_monitors(self, user_id: str = None) -> List[Dict[str, Any]]:
        """Fetch all active blog monitors from Next.js API"""
        try:
            params = {'active': 'true'}
            if user_id:
                params['user_id'] = user_id
                
            response = make_api_request('GET', 'blog-monitors', params=params)
            
            if response and isinstance(response, list):
                logger.info(f"Retrieved {len(response)} active monitors from API")
                return response
            else:
                logger.warning("No active monitors found or API error")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching active monitors: {e}")
            return []

    def get_monitor_by_id(self, monitor_id: str) -> Optional[Dict[str, Any]]:
        """Fetch specific monitor details from Next.js API"""
        try:
            response = make_api_request('GET', f'blog-monitors/{monitor_id}')
            
            if response:
                logger.debug(f"Successfully fetched monitor {monitor_id}")
                return response
            else:
                logger.warning(f"Monitor {monitor_id} not found")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching monitor {monitor_id}: {e}")
            return None

    def check_monitor(self, monitor_id: str) -> List[Dict[str, Any]]:
        """Check a specific monitor for new posts (blog or LinkedIn via API)"""
        monitor_data = self.get_monitor_by_id(monitor_id)
        if not monitor_data:
            logger.error(f"Monitor {monitor_id} not found")
            return []
        
        if not monitor_data.get('active', False):
            logger.info(f"Monitor {monitor_id} is not active")
            return []
        
        url = monitor_data.get('url')
        if not url:
            logger.error(f"Monitor {monitor_id} has no URL")
            return []
        
        monitor_name = monitor_data.get('name', 'Unknown Monitor')
        user_id = monitor_data.get('user_id')
        
        logger.info(f"Checking monitor: {monitor_name} ({url})")
        
        try:
            # Route to appropriate method based on URL type
            if self._is_linkedin_url(url):
                new_posts = self._check_linkedin_activity_api(url, monitor_id, user_id)
            else:
                new_posts = self._check_blog_content(url, monitor_id, user_id)
            
            if new_posts:
                logger.info(f"Found {len(new_posts)} new posts from {monitor_name}")
                
                # Save discovered posts to Next.js API
                saved_posts = []
                for post in new_posts:
                    saved_post = self._save_post_to_api(post)
                    if saved_post:
                        saved_posts.append(saved_post)
                
                # Update monitor's last_checked timestamp
                self._update_monitor_last_checked(monitor_id)
                
                return saved_posts
            else:
                logger.info(f"No new posts found for {monitor_name}")
                # Still update last_checked even if no posts found
                self._update_monitor_last_checked(monitor_id)
                return []
            
        except Exception as e:
            logger.error(f"Error checking monitor {monitor_id}: {e}")
            return []

    def check_all_monitors(self, user_id: str = None) -> Dict[str, List[Dict]]:
        """Check all active monitors for new posts (blogs and LinkedIn via API)"""
        monitors = self.get_active_monitors(user_id)
        results = {}
        
        if not monitors:
            logger.info("No active monitors to check")
            return results
        
        linkedin_monitors = []
        blog_monitors = []
        
        # Separate LinkedIn and blog monitors
        for monitor in monitors:
            monitor_id = monitor.get('id')
            url = monitor.get('url', '')
            
            if not monitor_id:
                continue
                
            if self._is_linkedin_url(url):
                linkedin_monitors.append(monitor)
            else:
                blog_monitors.append(monitor)
        
        logger.info(f"Processing {len(blog_monitors)} blog monitors and {len(linkedin_monitors)} LinkedIn monitors")
        
        # Process blog monitors
        for monitor in blog_monitors:
            monitor_id = monitor.get('id')
            try:
                new_posts = self.check_monitor(monitor_id)
                results[monitor_id] = new_posts
            except Exception as e:
                logger.error(f"Error checking blog monitor {monitor_id}: {e}")
                results[monitor_id] = []
        
        # Process LinkedIn monitors using API (no credentials needed!)
        for monitor in linkedin_monitors:
            monitor_id = monitor.get('id')
            try:
                logger.info(f"🔗 Processing LinkedIn monitor via API: {monitor.get('name')}")
                new_posts = self.check_monitor(monitor_id)
                results[monitor_id] = new_posts
            except Exception as e:
                logger.error(f"Error checking LinkedIn monitor {monitor_id}: {e}")
                results[monitor_id] = []
        
        total_posts = sum(len(posts) for posts in results.values())
        logger.info(f"Checked {len(monitors)} total monitors, found {total_posts} total new posts")
        
        return results

    def test_linkedin_api(self, username: str) -> Dict[str, Any]:
        """Test the LinkedIn API with a specific username"""
        try:
            logger.info(f"🧪 Testing LinkedIn API for username: {username}")
            
            api_response = self._call_linkedin_api(username)
            
            if api_response and api_response.get('data'):
                posts = api_response['data']
                
                # Analyze posts with content analyzer
                knowledge_posts = 0
                total_posts = len(posts)
                
                for post in posts[:5]:  # Analyze first 5 posts
                    post_text = post.get('text', '')
                    if post_text:
                        analysis = self.content_analyzer.analyze_content(post_text)
                        if analysis.get('is_knowledge_post', False):
                            knowledge_posts += 1
                
                return {
                    'success': True,
                    'message': f'LinkedIn API test successful for {username}',
                    'stats': {
                        'total_posts_returned': total_posts,
                        'knowledge_posts_detected': knowledge_posts,
                        'api_response_size': len(str(api_response))
                    },
                    'sample_post': posts[0] if posts else None
                }
            else:
                return {
                    'success': False,
                    'error': f'LinkedIn API returned no data for {username}',
                    'response': api_response
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': f'LinkedIn API test failed: {str(e)}'
            }

    def _check_blog_content(self, url: str, monitor_id: str, user_id: str) -> List[Dict[str, Any]]:
        """Check blog content (original functionality)"""
        try:
            # Try RSS/Atom feed first
            new_posts = self._check_rss_feed(url, monitor_id, user_id)
            
            # If no RSS feed found, try web scraping
            if not new_posts:
                new_posts = self._scrape_blog_posts(url, monitor_id, user_id)
            
            return new_posts
            
        except Exception as e:
            logger.error(f"Error checking blog content: {e}")
            return []

    def _check_rss_feed(self, url: str, monitor_id: str, user_id: str) -> List[Dict[str, Any]]:
        """Check RSS/Atom feed for new posts"""
        feed_urls = [
            url,
            f"{url.rstrip('/')}/feed",
            f"{url.rstrip('/')}/rss",
            f"{url.rstrip('/')}/atom.xml",
            f"{url.rstrip('/')}/feed.xml",
            f"{url.rstrip('/')}/index.xml"  # Hugo/Jekyll sites
        ]
        
        for feed_url in feed_urls:
            try:
                logger.debug(f"Trying RSS feed: {feed_url}")
                feed = feedparser.parse(feed_url)
                
                if feed.entries:
                    logger.info(f"Found RSS feed with {len(feed.entries)} entries: {feed_url}")
                    return self._process_feed_entries(feed.entries, monitor_id, user_id)
                    
            except Exception as e:
                logger.debug(f"Failed to parse feed {feed_url}: {e}")
                continue
        
        logger.info(f"No RSS feed found for {url}")
        return []

    def _process_feed_entries(self, entries: List, monitor_id: str, user_id: str) -> List[Dict[str, Any]]:
        """Process RSS feed entries into post data with full content scraping"""
        new_posts = []
        
        for entry in entries[:10]:  # Limit to 10 most recent posts
            try:
                # Get unique identifier for the post
                post_url = entry.get('link') or entry.get('id', '')
                if not post_url:
                    logger.warning("Skipping entry with no URL/ID")
                    continue
                
                # Create hash for deduplication
                post_hash = hashlib.md5(post_url.encode()).hexdigest()
                
                # Check if we've already processed this post (Redis cache)
                cache_key = f"processed_posts:{monitor_id}"
                if self.redis.sismember(cache_key, post_hash):
                    logger.debug(f"Post already processed: {post_url}")
                    continue
                
                # Try to get full content by scraping the individual post page
                full_content = self._scrape_full_post_content(post_url)
                
                # Fallback to RSS content if scraping fails
                if not full_content:
                    full_content = self._extract_feed_content(entry)
                
                # Build post data
                post_data = {
                    'title': entry.get('title', 'Untitled Post').strip(),
                    'content': full_content,
                    'url': post_url,
                    'published_at': self._parse_published_date(entry),
                    'author': entry.get('author', 'Unknown'),
                    'monitor_id': monitor_id,
                    'user_id': user_id,
                    'platform': 'blog',
                    'status': 'discovered',
                    'source_type': 'rss_full_content'
                }
                
                new_posts.append(post_data)
                
                # Mark as processed in Redis cache
                self.redis.sadd(cache_key, post_hash)
                # Set expiry for cache (30 days)
                self.redis.expire(cache_key, 2592000)
                
                logger.debug(f"Processed RSS entry with full content: {post_data['title']}")
                
                # Add delay to be respectful to the server
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing RSS entry: {e}")
                continue
        
        return new_posts

    def _scrape_full_post_content(self, post_url: str) -> str:
        """Scrape the full content from an individual blog post page"""
        try:
            logger.debug(f"Scraping full content from: {post_url}")
            
            response = self.session.get(post_url, timeout=30)
            response.raise_for_status()
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove unwanted elements
            for unwanted in soup(['script', 'style', 'nav', 'header', 'footer', 
                                'sidebar', 'aside', '.sidebar', '.navigation', 
                                '.comments', '.related', '.social-share']):
                unwanted.decompose()
            
            # Try different content selectors
            content_selectors = [
                'article .content',
                'article .post-content', 
                'article .entry-content',
                '.post-content',
                '.entry-content',
                '.article-content',
                '.content',
                'article .prose',
                '.prose',
                'main article',
                'article',
                '[role="main"] article',
                '.post-body',
                '.blog-content'
            ]
            
            content_text = ''
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # Extract text content, preserving paragraph breaks
                    paragraphs = content_elem.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li'])
                    if paragraphs:
                        content_text = '\n\n'.join(
                            p.get_text(strip=True) for p in paragraphs 
                            if p.get_text(strip=True)
                        )
                    else:
                        content_text = content_elem.get_text(separator='\n\n', strip=True)
                    
                    if content_text and len(content_text) > 200:
                        logger.debug(f"Successfully extracted full content using selector: {selector}")
                        break
            
            # Clean up and limit content
            if content_text:
                content_text = ' '.join(content_text.split())
                return content_text[:5000] if len(content_text) > 5000 else content_text
            
            logger.warning(f"Could not extract meaningful content from: {post_url}")
            return ''
            
        except Exception as e:
            logger.warning(f"Error scraping full content from {post_url}: {e}")
            return ''

    def _extract_feed_content(self, entry) -> str:
        """Extract clean content from RSS entry (fallback)"""
        content_parts = []
        
        # Try different content fields
        if hasattr(entry, 'content') and entry.content:
            for content_item in entry.content:
                if hasattr(content_item, 'value'):
                    content_parts.append(content_item.value)
        elif hasattr(entry, 'summary') and entry.summary:
            content_parts.append(entry.summary)
        elif hasattr(entry, 'description') and entry.description:
            content_parts.append(entry.description)
        
        full_content = "\n".join(content_parts)
        
        # Clean HTML tags if present
        if full_content:
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(full_content, 'html.parser')
                text_content = soup.get_text(separator=' ', strip=True)
            except Exception:
                text_content = full_content
        else:
            text_content = ''
        
        return text_content[:2000] if text_content else ''

    def _parse_published_date(self, entry) -> str:
        """Parse published date from RSS entry"""
        try:
            date_fields = ['published', 'updated', 'created']
            
            for field in date_fields:
                if hasattr(entry, field) and getattr(entry, field):
                    return getattr(entry, field)
            
            return datetime.now().isoformat()
            
        except Exception as e:
            logger.warning(f"Error parsing date: {e}")
            return datetime.now().isoformat()

    def _scrape_blog_posts(self, url: str, monitor_id: str, user_id: str) -> List[Dict[str, Any]]:
        """Scrape blog posts from HTML"""
        try:
            logger.info(f"Scraping blog posts from: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try common blog post selectors
            post_selectors = [
                'article',
                '.post',
                '.blog-post', 
                '.entry',
                '.article',
                '[class*="post"]',
                '[id*="post"]',
                '.content article',
                'main article'
            ]
            
            for selector in post_selectors:
                elements = soup.select(selector)
                if elements:
                    logger.info(f"Found {len(elements)} post elements using selector: {selector}")
                    return self._extract_posts_from_elements(elements, url, monitor_id, user_id)
            
            logger.warning(f"No blog post elements found on {url}")
            return []
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return []

    def _extract_posts_from_elements(self, elements: List, base_url: str, 
                                   monitor_id: str, user_id: str) -> List[Dict[str, Any]]:
        """Extract post data from HTML elements"""
        posts = []
        
        for element in elements[:5]:  # Limit to 5 posts
            try:
                # Extract title
                title_selectors = ['h1', 'h2', 'h3', '.title', '.post-title', '[class*="title"]']
                title = 'Untitled Scraped Post'
                
                for selector in title_selectors:
                    title_elem = element.find(selector)
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        break
                
                # Extract URL
                post_url = self._extract_post_url(element, base_url)
                if not post_url:
                    continue
                
                # Create hash for deduplication
                post_hash = hashlib.md5(post_url.encode()).hexdigest()
                
                # Check if already processed
                cache_key = f"processed_posts:{monitor_id}"
                if self.redis.sismember(cache_key, post_hash):
                    continue
                
                # Scrape full content
                full_content = self._scrape_full_post_content(post_url)
                if not full_content:
                    full_content = self._extract_element_content(element)
                
                post_data = {
                    'title': title,
                    'content': full_content,
                    'url': post_url,
                    'published_at': datetime.now().isoformat(),
                    'author': 'Unknown',
                    'monitor_id': monitor_id,
                    'user_id': user_id,
                    'platform': 'blog',
                    'status': 'discovered',
                    'source_type': 'scrape_full_content'
                }
                
                posts.append(post_data)
                
                # Mark as processed
                self.redis.sadd(cache_key, post_hash)
                self.redis.expire(cache_key, 2592000)
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error extracting post from element: {e}")
                continue
        
        return posts

    def _extract_element_content(self, element) -> str:
        """Extract content from HTML element"""
        try:
            content_selectors = [
                '.content', '.entry-content', '.post-content', 
                '.article-body', '.prose', 'p'
            ]
            
            content_text = ''
            
            for selector in content_selectors:
                content_elem = element.find(selector)
                if content_elem:
                    content_text = content_elem.get_text(separator=' ', strip=True)
                    break
            
            if not content_text:
                paragraphs = element.find_all('p', limit=3)
                content_text = ' '.join(p.get_text(strip=True) for p in paragraphs)
            
            return content_text[:1000] if content_text else ''
            
        except Exception:
            return ''

    def _extract_post_url(self, element, base_url: str) -> Optional[str]:
        """Extract post URL from element"""
        try:
            link_elem = element.find('a', href=True)
            
            if not link_elem and element.name == 'a' and element.has_attr('href'):
                link_elem = element
            
            if not link_elem:
                parent_link = element.find_parent('a', href=True)
                if parent_link:
                    link_elem = parent_link
            
            if not link_elem:
                return None
            
            post_url = link_elem['href']
            
            # Handle relative URLs
            if post_url.startswith('/'):
                post_url = urljoin(base_url, post_url)
            elif not post_url.startswith(('http://', 'https://')):
                post_url = f"https://{post_url}"
            
            return post_url
            
        except Exception:
            return None

    def _save_post_to_api(self, post_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Save discovered post to Next.js API"""
        try:
            response = make_api_request('POST', 'posts', data=post_data)
            
            if response and 'id' in response:
                logger.info(f"Saved post to API: {post_data['title']} (ID: {response['id']})")
                return response
            else:
                logger.error(f"Failed to save post to API: {post_data['title']}")
                return None
                
        except Exception as e:
            logger.error(f"Error saving post to API: {e}")
            return None

    def _update_monitor_last_checked(self, monitor_id: str):
        """Update monitor's last_checked timestamp via API"""
        try:
            update_data = {
                'last_checked': datetime.now().isoformat()
            }
            
            response = make_api_request('PUT', f'blog-monitors/{monitor_id}', data=update_data)
            
            if response:
                logger.debug(f"✅ Updated last_checked for monitor {monitor_id}")
            else:
                logger.warning(f"⚠️ Failed to update last_checked for monitor {monitor_id}")
                
        except Exception as e:
            logger.error(f"❌ Error updating monitor last_checked for {monitor_id}: {e}")

    def get_monitor_stats(self, monitor_id: str) -> Dict[str, Any]:
        """Get statistics for a specific monitor"""
        try:
            monitor = self.get_monitor_by_id(monitor_id)
            if not monitor:
                return {}
            
            posts_response = make_api_request('GET', 'posts', params={'monitor_id': monitor_id})
            posts_count = len(posts_response) if posts_response else 0
            
            cache_key = f"processed_posts:{monitor_id}"
            processed_count = self.redis.scard(cache_key) or 0
            
            # Determine monitor type
            url = monitor.get('url', '')
            monitor_type = 'linkedin' if self._is_linkedin_url(url) else 'blog'
            
            return {
                'monitor_id': monitor_id,
                'name': monitor.get('name'),
                'url': url,
                'type': monitor_type,
                'active': monitor.get('active'),
                'last_checked': monitor.get('last_checked'),
                'total_posts_found': posts_count,
                'posts_processed': processed_count,
                'cache_size': processed_count
            }
            
        except Exception as e:
            logger.error(f"Error getting monitor stats: {e}")
            return {}


    # Key modifications to _check_linkedin_activity_api method
    def _check_linkedin_activity_api(self, url: str, monitor_id: str, user_id: str) -> List[Dict[str, Any]]:
        """Check LinkedIn activity using RapidAPI with enhanced filtering and processing"""
        try:
            logger.info(f"🔗 Checking LinkedIn activity via API: {url}")
            
            # Extract username from URL
            username = self._extract_linkedin_username(url)
            if not username:
                logger.error(f"Could not extract LinkedIn username from URL: {url}")
                return []
            
            # Call LinkedIn API
            api_response = self._call_linkedin_api(username)
            if not api_response or not api_response.get('data'):
                logger.warning(f"No data returned from LinkedIn API for {username}")
                return []
            
            api_posts = api_response['data']
            logger.info(f"Retrieved {len(api_posts)} total posts from LinkedIn API")
            
            # Process ALL posts first, then filter and sort
            candidate_posts = []
            processed_hashes = set()
            
            # Get existing processed posts to avoid duplicates
            cache_key = f"processed_posts:{monitor_id}"
            existing_hashes = self.redis.smembers(cache_key)
            existing_hashes = {h.decode() if isinstance(h, bytes) else h for h in existing_hashes}
            
            logger.info(f"Processing all {len(api_posts)} posts for content analysis...")
            
            for i, api_post in enumerate(api_posts):  # Process ALL posts, no limit
                try:
                    # Quick duplicate check first
                    post_url = api_post.get('postUrl', '')
                    urn = api_post.get('urn', '')
                    post_text = api_post.get('text', '').strip()
                    
                    unique_identifier = post_url or urn or post_text
                    content_hash = hashlib.md5(unique_identifier.encode()).hexdigest()
                    
                    # Skip if already processed or duplicate in this batch
                    if content_hash in existing_hashes or content_hash in processed_hashes:
                        logger.debug(f"Skipping duplicate post {i+1}")
                        continue
                    
                    processed_hashes.add(content_hash)
                    
                    # Skip posts with insufficient content early
                    if not post_text or len(post_text) < 30:
                        logger.debug(f"Skipping post {i+1} with insufficient content")
                        continue
                    
                    # Analyze content using ContentAnalyzer
                    analysis_result = self.content_analyzer.analyze_content(post_text)
                    
                    # Only process knowledge posts
                    if not analysis_result.get('is_knowledge_post', False):
                        logger.debug(f"Post {i+1} filtered out: {analysis_result.get('reasoning', 'Not a knowledge post')}")
                        continue
                    
                    # Calculate engagement score for ranking
                    engagement_score = self._calculate_engagement_score_from_api(api_post)
                    
                    # Create candidate post with all data
                    candidate_post = {
                        'api_post': api_post,
                        'analysis_result': analysis_result,
                        'engagement_score': engagement_score,
                        'content_hash': content_hash,
                        'post_index': i + 1
                    }
                    
                    candidate_posts.append(candidate_post)
                    logger.debug(f"✅ Post {i+1} qualified - Category: {analysis_result['category']}, "
                            f"Confidence: {analysis_result['confidence']:.2f}, "
                            f"Engagement: {engagement_score}")
                    
                except Exception as e:
                    logger.error(f"Error processing LinkedIn API post {i+1}: {e}")
                    continue
            
            logger.info(f"Found {len(candidate_posts)} knowledge posts after content filtering")
            
            if not candidate_posts:
                logger.info("No qualifying knowledge posts found")
                return []
            
            # Sort by multiple criteria for best quality posts
            def sort_key(post):
                analysis = post['analysis_result']
                return (
                    analysis.get('confidence', 0) * 100,      # Confidence score (weighted heavily)
                    post['engagement_score'],                  # Engagement score
                    len(analysis.get('final_content', '')),   # Content length (more substantial posts)
                    -post['post_index']                       # Recency (negative for reverse order)
                )
            
            candidate_posts.sort(key=sort_key, reverse=True)
            
            # Log top candidates for debugging
            logger.info("Top 5 candidates after sorting:")
            for i, post in enumerate(candidate_posts[:5]):
                analysis = post['analysis_result']
                logger.info(f"  {i+1}. Confidence: {analysis.get('confidence', 0):.2f}, "
                        f"Engagement: {post['engagement_score']}, "
                        f"Category: {analysis.get('category', 'unknown')}")
            
            # Take top N posts (configurable limit)
            max_posts_to_process = 25  # Increased from 15, configurable
            selected_candidates = candidate_posts[:max_posts_to_process]
            
            logger.info(f"Processing top {len(selected_candidates)} posts...")
            
            # Convert selected candidates to monitor format
            posts_data = []
            for candidate in selected_candidates:
                try:
                    monitor_post = self._convert_candidate_to_monitor_format(
                        candidate, monitor_id, user_id, url
                    )
                    
                    if monitor_post:
                        posts_data.append(monitor_post)
                        
                        # Mark as processed in cache
                        self.redis.sadd(cache_key, candidate['content_hash'])
                        
                        logger.info(f"✅ Added high-quality LinkedIn post {len(posts_data)}: "
                                f"{candidate['analysis_result']['category']}")
                    
                except Exception as e:
                    logger.error(f"Error converting candidate to monitor format: {e}")
                    continue
            
            # Set cache expiry
            if posts_data:
                self.redis.expire(cache_key, 2592000)  # 30 days
            
            logger.info(f"Successfully processed {len(posts_data)} high-quality knowledge posts from LinkedIn API")
            
            # Log summary statistics
            if posts_data:
                categories = [post['nlp_analysis']['category'] for post in posts_data]
                category_counts = {}
                for cat in categories:
                    category_counts[cat] = category_counts.get(cat, 0) + 1
                
                avg_confidence = sum(post['nlp_analysis']['confidence'] for post in posts_data) / len(posts_data)
                avg_engagement = sum(post['calculated_engagement_score'] for post in posts_data) / len(posts_data)
                
                logger.info(f"📊 Results Summary:")
                logger.info(f"   Average Confidence: {avg_confidence:.2f}")
                logger.info(f"   Average Engagement: {avg_engagement:.1f}")
                logger.info(f"   Categories: {category_counts}")
            
            return posts_data
            
        except Exception as e:
            logger.error(f"Error checking LinkedIn activity via API: {e}")
            return []

    def _convert_candidate_to_monitor_format(self, candidate: Dict[str, Any], monitor_id: str, 
                                        user_id: str, source_url: str) -> Optional[Dict[str, Any]]:
        """Convert candidate post to monitor format (optimized version)"""
        try:
            api_post = candidate['api_post']
            analysis_result = candidate['analysis_result']
            engagement_score = candidate['engagement_score']
            content_hash = candidate['content_hash']
            
            # Extract basic post data
            post_text = api_post.get('text', '').strip()
            cleaned_content = analysis_result.get('final_content', post_text)
            
            # Extract engagement metrics
            engagement_summary = self._format_engagement_summary(api_post)
            
            # Extract author information
            author_data = api_post.get('author', {})
            author_name = f"{author_data.get('firstName', '')} {author_data.get('lastName', '')}".strip()
            if not author_name:
                author_name = author_data.get('username', 'LinkedIn User')
            
            # Parse posted date
            posted_date = self._parse_linkedin_date(api_post)
            
            # Create monitor post format
            monitor_post = {
                'title': f"LinkedIn Knowledge Post - {analysis_result['category'].replace('_', ' ').title()}",
                'content': cleaned_content,
                'original_content': post_text,
                'url': api_post.get('postUrl', ''),
                'published_at': posted_date,
                'author': author_name,
                'monitor_id': monitor_id,
                'user_id': user_id,
                'platform': 'linkedin',
                'status': 'discovered',
                'source_type': 'linkedin_api_enhanced',
                'engagement_summary': engagement_summary,
                'nlp_analysis': analysis_result,
                'content_hash': content_hash,
                'calculated_engagement_score': engagement_score,
                'quality_score': analysis_result.get('confidence', 0) * 100 + engagement_score,  # Combined quality metric
                'api_data': {
                    'urn': api_post.get('urn', ''),
                    'total_reactions': api_post.get('totalReactionCount', 0),
                    'comments_count': api_post.get('commentsCount', 0),
                    'reposts_count': api_post.get('repostsCount', 0),
                    'author_headline': author_data.get('headline', ''),
                    'author_username': author_data.get('username', ''),
                    'posted_at_relative': api_post.get('postedAt', ''),
                    'content_type': api_post.get('contentType', 'post'),
                    'processing_rank': candidate.get('post_index', 0)  # Original position in API results
                }
            }
            
            return monitor_post
            
        except Exception as e:
            logger.error(f"Error converting candidate to monitor format: {e}")
            return None

    def _calculate_engagement_score_from_api(self, api_post: Dict[str, Any]) -> int:
        """Calculate weighted engagement score from API data with better weighting"""
        try:
            # More sophisticated weighting based on engagement value
            reactions = api_post.get('totalReactionCount', 0) * 1
            comments = api_post.get('commentsCount', 0) * 5  # Comments are very valuable
            reposts = api_post.get('repostsCount', 0) * 3    # Shares/reposts are valuable
            
            # Bonus for posts with multiple engagement types
            engagement_types = sum([
                1 if reactions > 0 else 0,
                1 if comments > 0 else 0,
                1 if reposts > 0 else 0
            ])
            
            diversity_bonus = engagement_types * 2  # Bonus for diverse engagement
            
            base_score = reactions + comments + reposts + diversity_bonus
            
            # Apply logarithmic scaling for very high engagement to prevent outliers
            if base_score > 100:
                import math
                scaled_score = 100 + math.log10(base_score - 99) * 20
                return int(scaled_score)
            
            return base_score
            
        except Exception as e:
            logger.error(f"Error calculating engagement score: {e}")
            return 0

    def configure_processing_limits(self, max_posts_to_analyze: int = None, 
                                max_posts_to_return: int = None):
        """Configure processing limits for LinkedIn posts"""
        if max_posts_to_analyze is not None:
            self.max_posts_to_analyze = max_posts_to_analyze
        else:
            self.max_posts_to_analyze = 50  # Default: analyze all available posts
        
        if max_posts_to_return is not None:
            self.max_posts_to_return = max_posts_to_return
        else:
            self.max_posts_to_return = 25   # Default: return top 25 quality posts
        
        logger.info(f"Configured processing limits: analyze={self.max_posts_to_analyze}, return={self.max_posts_to_return}")

    def analyze_post_quality_distribution(self, posts_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze the quality distribution of processed posts"""
        if not posts_data:
            return {}
        
        confidences = [post['nlp_analysis']['confidence'] for post in posts_data]
        engagement_scores = [post['calculated_engagement_score'] for post in posts_data]
        quality_scores = [post.get('quality_score', 0) for post in posts_data]
        
        return {
            'total_posts': len(posts_data),
            'confidence_stats': {
                'min': min(confidences),
                'max': max(confidences),
                'avg': sum(confidences) / len(confidences)
            },
            'engagement_stats': {
                'min': min(engagement_scores),
                'max': max(engagement_scores),
                'avg': sum(engagement_scores) / len(engagement_scores)
            },
            'quality_stats': {
                'min': min(quality_scores),
                'max': max(quality_scores),
                'avg': sum(quality_scores) / len(quality_scores)
            },
            'category_distribution': self._get_category_distribution(posts_data)
        }

    def _get_category_distribution(self, posts_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Get distribution of post categories"""
        categories = {}
        for post in posts_data:
            category = post['nlp_analysis'].get('category', 'unknown')
            categories[category] = categories.get(category, 0) + 1
        return categories