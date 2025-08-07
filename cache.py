"""
API caching system for Upstox API responses
"""

import json
import os
import hashlib
import time
from typing import Dict, Any, Optional
from datetime import datetime, timedelta


class APICache:
    """
    File-based cache for API responses with TTL (Time To Live) support
    """
    
    def __init__(self, cache_dir: str = ".cache", ttl_hours: int = 1):
        """
        Initialize the cache
        
        Args:
            cache_dir: Directory to store cache files
            ttl_hours: Time to live for cached data in hours
        """
        self.cache_dir = cache_dir
        self.ttl_seconds = ttl_hours * 3600
        
        # Create cache directory if it doesn't exist
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
    
    def _get_cache_key(self, url: str, params: Dict[str, Any] = None) -> str:
        """
        Generate a unique cache key for the request
        
        Args:
            url: The API URL
            params: Request parameters
            
        Returns:
            Hexadecimal cache key
        """
        # Combine URL and parameters for unique key
        cache_data = {
            'url': url,
            'params': params or {}
        }
        
        # Create hash of the request
        cache_str = json.dumps(cache_data, sort_keys=True)
        return hashlib.md5(cache_str.encode()).hexdigest()
    
    def _get_cache_file_path(self, cache_key: str) -> str:
        """Get the full path for a cache file"""
        return os.path.join(self.cache_dir, f"{cache_key}.json")
    
    def get(self, url: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """
        Get cached response if available and not expired
        
        Args:
            url: The API URL
            params: Request parameters
            
        Returns:
            Cached response data or None if not found/expired
        """
        cache_key = self._get_cache_key(url, params)
        cache_file = self._get_cache_file_path(cache_key)
        
        if not os.path.exists(cache_file):
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Check if cache has expired
            cached_time = cache_data.get('timestamp', 0)
            current_time = time.time()
            
            if current_time - cached_time > self.ttl_seconds:
                # Cache expired, remove the file
                os.remove(cache_file)
                return None
            
            print(f"Cache HIT for {url} (age: {int((current_time - cached_time)/60)} minutes)")
            return cache_data.get('response')
            
        except (json.JSONDecodeError, FileNotFoundError, KeyError):
            # Corrupted cache file, remove it
            if os.path.exists(cache_file):
                os.remove(cache_file)
            return None
    
    def set(self, url: str, response_data: Dict[str, Any], params: Dict[str, Any] = None) -> None:
        """
        Store response in cache
        
        Args:
            url: The API URL
            response_data: The API response data
            params: Request parameters
        """
        cache_key = self._get_cache_key(url, params)
        cache_file = self._get_cache_file_path(cache_key)
        
        cache_data = {
            'timestamp': time.time(),
            'url': url,
            'params': params or {},
            'response': response_data
        }
        
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)
            
            print(f"Cache SET for {url}")
            
        except Exception as e:
            print(f"Warning: Failed to cache response for {url}: {e}")
    
    def clear_expired(self) -> int:
        """
        Remove all expired cache files
        
        Returns:
            Number of files removed
        """
        if not os.path.exists(self.cache_dir):
            return 0
        
        removed_count = 0
        current_time = time.time()
        
        for filename in os.listdir(self.cache_dir):
            if not filename.endswith('.json'):
                continue
                
            file_path = os.path.join(self.cache_dir, filename)
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                cached_time = cache_data.get('timestamp', 0)
                
                if current_time - cached_time > self.ttl_seconds:
                    os.remove(file_path)
                    removed_count += 1
                    
            except (json.JSONDecodeError, FileNotFoundError, KeyError):
                # Corrupted file, remove it
                os.remove(file_path)
                removed_count += 1
        
        if removed_count > 0:
            print(f"Cleaned up {removed_count} expired cache files")
        
        return removed_count
    
    def clear_all(self) -> int:
        """
        Remove all cache files
        
        Returns:
            Number of files removed
        """
        if not os.path.exists(self.cache_dir):
            return 0
        
        removed_count = 0
        
        for filename in os.listdir(self.cache_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(self.cache_dir, filename)
                os.remove(file_path)
                removed_count += 1
        
        if removed_count > 0:
            print(f"Cleared {removed_count} cache files")
        
        return removed_count
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics
        
        Returns:
            Dictionary with cache stats
        """
        if not os.path.exists(self.cache_dir):
            return {'total_files': 0, 'expired_files': 0, 'valid_files': 0}
        
        current_time = time.time()
        total_files = 0
        expired_files = 0
        valid_files = 0
        
        for filename in os.listdir(self.cache_dir):
            if not filename.endswith('.json'):
                continue
                
            total_files += 1
            file_path = os.path.join(self.cache_dir, filename)
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                cached_time = cache_data.get('timestamp', 0)
                
                if current_time - cached_time > self.ttl_seconds:
                    expired_files += 1
                else:
                    valid_files += 1
                    
            except (json.JSONDecodeError, FileNotFoundError, KeyError):
                expired_files += 1
        
        return {
            'total_files': total_files,
            'expired_files': expired_files,
            'valid_files': valid_files,
            'cache_dir': self.cache_dir,
            'ttl_hours': self.ttl_seconds / 3600
        }


# Global cache instance
api_cache = APICache()


def clear_cache():
    """Convenience function to clear all cached data"""
    return api_cache.clear_all()


def get_cache_stats():
    """Convenience function to get cache statistics"""
    return api_cache.get_cache_stats()