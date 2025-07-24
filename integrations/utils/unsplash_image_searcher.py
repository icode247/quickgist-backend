import requests
import os
import time
from urllib.parse import urlparse
import json
from typing import List, Dict, Optional

class UnsplashDownloader:
    def __init__(self, access_key: str):
        """
        Initialize the Unsplash downloader with your API access key.
        
        Args:
            access_key: Your Unsplash API access key
        """
        self.access_key = access_key
        self.base_url = "https://api.unsplash.com"
        self.headers = {
            "Authorization": f"Client-ID {access_key}",
            "Accept-Version": "v1"
        }
        
    def test_api_connection(self) -> bool:
        """
        Test if the API key is valid and connection works.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            print("🔧 Testing API connection...")
            response = requests.get(
                f"{self.base_url}/photos", 
                headers=self.headers,
                params={"per_page": 1}
            )
            
            if response.status_code == 200:
                print("✅ API connection successful!")
                return True
            elif response.status_code == 401:
                print("❌ API key is invalid or missing")
                print("Make sure you're using the 'Access Key' from https://unsplash.com/oauth/applications")
                return False
            else:
                print(f"❌ API test failed with status code: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
        
    def search_photos(self, query: str, per_page: int = 10, page: int = 1, 
                     orientation: str = None) -> List[Dict]:
        """
        Search for photos on Unsplash.
        
        Args:
            query: Search keyword(s)
            per_page: Number of results per page (max 30)
            page: Page number
            orientation: 'landscape', 'portrait', 'squarish' (optional)
            
        Returns:
            List of photo dictionaries
        """
        url = f"{self.base_url}/search/photos"
        params = {
            "query": query,
            "per_page": min(per_page, 30),  # API limit
            "page": page
        }
        
        # Only add orientation if specified (API doesn't accept "all")
        if orientation and orientation in ['landscape', 'portrait', 'squarish']:
            params["orientation"] = orientation
        
        try:
            print(f"🔧 Debug: Making request to {url}")
            print(f"🔧 Debug: Headers: {self.headers}")
            print(f"🔧 Debug: Params: {params}")
            
            response = requests.get(url, headers=self.headers, params=params)
            
            print(f"🔧 Debug: Response status: {response.status_code}")
            
            if response.status_code == 401:
                print("❌ Error 401: Unauthorized - Check your API key")
                print("Make sure you're using the 'Access Key' (not 'Secret Key')")
                print("Get your API key at: https://unsplash.com/oauth/applications")
                return []
            elif response.status_code == 403:
                print("❌ Error 403: Forbidden - API rate limit exceeded or invalid permissions")
                return []
            elif response.status_code == 400:
                print("❌ Error 400: Bad Request")
                try:
                    error_data = response.json()
                    print(f"Error details: {error_data}")
                except:
                    print(f"Response text: {response.text}")
                return []
            
            response.raise_for_status()
            
            data = response.json()
            results = data.get("results", [])
            total = data.get("total", 0)
            print(f"🔍 Found {total} total results, returning {len(results)} photos")
            
            return results
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error: {e}")
            return []
    
    def get_photo_url(self, photo: Dict, quality: str = "regular") -> Optional[str]:
        """
        Get the direct URL for a photo without downloading it.
        
        Args:
            photo: Photo dictionary from search results
            quality: 'raw', 'full', 'regular', 'small', 'thumb'
            
        Returns:
            Direct URL to the image or None if not available
        """
        try:
            # Trigger download endpoint (required by Unsplash API terms)
            self._trigger_download(photo["links"]["download_location"])
            
            # Return the image URL
            return photo["urls"].get(quality, photo["urls"]["regular"])
            
        except Exception as e:
            print(f"❌ Error getting URL for photo {photo.get('id', 'unknown')}: {e}")
            return None
    
    def get_search_urls(self, query: str, count: int = 5, 
                       quality: str = "regular",
                       orientation: str = None) -> List[Dict]:
        """
        Search and return image URLs instead of downloading.
        
        Args:
            query: Search keyword(s)
            count: Number of photo URLs to return
            quality: Image quality URL to return
            orientation: Filter by orientation ('landscape', 'portrait', 'squarish')
            
        Returns:
            List of dictionaries containing photo info and URLs
        """
        print(f"🔍 Searching for '{query}' image URLs...")
        
        photo_urls = []
        photos_needed = count
        page = 1
        
        while photos_needed > 0:
            per_page = min(photos_needed, 30)
            photos = self.search_photos(query, per_page=per_page, page=page, orientation=orientation)
            
            if not photos:
                print(f"No more photos found. Got {len(photo_urls)} URLs.")
                break
            
            for photo in photos:
                if photos_needed <= 0:
                    break
                    
                photo_url = self.get_photo_url(photo, quality)
                if photo_url:
                    photo_info = {
                        "id": photo["id"],
                        "url": photo_url,
                        "description": photo.get("alt_description", "No description"),
                        "photographer": photo["user"]["name"],
                        "photographer_username": photo["user"]["username"],
                        "likes": photo["likes"],
                        "width": photo["width"],
                        "height": photo["height"],
                        "color": photo.get("color", "#000000"),
                        "all_urls": photo["urls"]  # All available quality URLs
                    }
                    photo_urls.append(photo_info)
                    photos_needed -= 1
                
                # Be respectful to the API
                time.sleep(0.5)
            
            page += 1
        
        print(f"✅ Retrieved {len(photo_urls)} image URLs")
        return photo_urls
    
    def download_photo(self, photo: Dict, download_dir: str = "downloads", 
                      quality: str = "regular") -> Optional[str]:
        """
        Download a single photo.
        
        Args:
            photo: Photo dictionary from search results
            download_dir: Directory to save the image
            quality: 'raw', 'full', 'regular', 'small', 'thumb'
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            # Create download directory if it doesn't exist
            os.makedirs(download_dir, exist_ok=True)
            
            # Get the download URL
            download_url = photo["urls"].get(quality, photo["urls"]["regular"])
            
            # Create filename
            photo_id = photo["id"]
            photographer = photo["user"]["username"]
            description = photo.get("alt_description", "unsplash_image")
            
            # Clean filename
            safe_description = "".join(c for c in description if c.isalnum() or c in (' ', '-', '_')).rstrip()
            safe_description = safe_description.replace(' ', '_')[:50]  # Limit length
            
            filename = f"{photo_id}_{photographer}_{safe_description}.jpg"
            filepath = os.path.join(download_dir, filename)
            
            # Download the image
            print(f"Downloading: {filename}")
            img_response = requests.get(download_url, stream=True)
            img_response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in img_response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Trigger download endpoint (required by Unsplash API terms)
            self._trigger_download(photo["links"]["download_location"])
            
            print(f"✅ Successfully downloaded: {filename}")
            return filepath
            
        except Exception as e:
            print(f"❌ Error downloading photo {photo.get('id', 'unknown')}: {e}")
            return None
    
    def _trigger_download(self, download_location: str):
        """
        Trigger the download endpoint as required by Unsplash API terms.
        """
        try:
            requests.get(download_location, headers=self.headers)
        except:
            pass  # Non-critical if this fails
    
    def download_search_results(self, query: str, count: int = 5, 
                               download_dir: str = "downloads",
                               quality: str = "regular",
                               orientation: str = None) -> List[str]:
        """
        Search and download multiple photos.
        
        Args:
            query: Search keyword(s)
            count: Number of photos to download
            download_dir: Directory to save images
            quality: Image quality to download
            orientation: Filter by orientation ('landscape', 'portrait', 'squarish')
            
        Returns:
            List of downloaded file paths
        """
        print(f"🔍 Searching for '{query}' images...")
        
        downloaded_files = []
        photos_needed = count
        page = 1
        
        while photos_needed > 0:
            per_page = min(photos_needed, 30)
            photos = self.search_photos(query, per_page=per_page, page=page, orientation=orientation)
            
            if not photos:
                print(f"No more photos found. Downloaded {len(downloaded_files)} images.")
                break
            
            for photo in photos:
                if photos_needed <= 0:
                    break
                    
                filepath = self.download_photo(photo, download_dir, quality)
                if filepath:
                    downloaded_files.append(filepath)
                    photos_needed -= 1
                
                # Be respectful to the API
                time.sleep(0.5)
            
            page += 1
        
        print(f"✅ Downloaded {len(downloaded_files)} images to '{download_dir}' folder")
        return downloaded_files
    
    def get_photo_info(self, photo: Dict) -> Dict:
        """
        Extract useful information from a photo dictionary.
        """
        return {
            "id": photo["id"],
            "description": photo.get("alt_description", "No description"),
            "photographer": photo["user"]["name"],
            "photographer_username": photo["user"]["username"],
            "likes": photo["likes"],
            "downloads": photo.get("downloads", "N/A"),
            "width": photo["width"],
            "height": photo["height"],
            "color": photo.get("color", "#000000"),
            "urls": photo["urls"]
        }


# Example usage for getting URLs instead of downloading:
if __name__ == "__main__":
    # Initialize with your API key
    downloader = UnsplashDownloader("ABbiKf3vTJzkPdGpaEmh9M6M5vU7PD-LR-zgoxlaCGE")
    
    # Test connection
    if not downloader.test_api_connection():
        exit(1)
    
    # Get image URLs instead of downloading
    photo_url = downloader.get_search_urls("Stressed designer working at cluttered desk in chaotic office", count=1, quality="regular")
    
    print(photo_url)