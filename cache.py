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
    
    def __init__(self, cache_dir: str = ".cache", ttl_hours: int = 1, verbose: bool = False):
        """
        Initialize the cache
        
        Args:
            cache_dir: Directory to store cache files
            ttl_hours: Time to live for cached data in hours
            verbose: Enable verbose output (default: False for clean progress bars)
        """
        self.cache_dir = cache_dir
        self.ttl_seconds = ttl_hours * 3600
        self.verbose = verbose
        self._cache_memory = {}  # In-memory cache for frequently accessed items
        self._cache_timestamps = {}  # Track timestamps to avoid file I/O
        
        # Create cache directory if it doesn't exist
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        
        # Preload cache metadata for performance
        self._load_cache_metadata()
    
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
    
    def _load_cache_metadata(self) -> None:
        """Preload cache metadata to avoid repeated file I/O"""
        if not os.path.exists(self.cache_dir):
            return
        
        current_time = time.time()
        
        for filename in os.listdir(self.cache_dir):
            if not filename.endswith('.json'):
                continue
            
            cache_key = filename[:-5]  # Remove .json extension
            file_path = os.path.join(self.cache_dir, filename)
            
            try:
                # Just read the timestamp, not the full data
                with open(file_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                cached_time = cache_data.get('timestamp', 0)
                
                # Only track non-expired entries
                if current_time - cached_time <= self.ttl_seconds:
                    self._cache_timestamps[cache_key] = cached_time
                else:
                    # Clean up expired file immediately
                    os.remove(file_path)
                    
            except (json.JSONDecodeError, FileNotFoundError, KeyError):
                # Remove corrupted files
                try:
                    os.remove(file_path)
                except:
                    pass
    
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
        
        # Fast path: Check in-memory cache first
        if cache_key in self._cache_memory:
            cached_time = self._cache_timestamps.get(cache_key, 0)
            current_time = time.time()
            
            if current_time - cached_time <= self.ttl_seconds:
                if self.verbose:
                    print(f"Cache HIT (memory) for {url} (age: {int((current_time - cached_time)/60)} minutes)")
                return self._cache_memory[cache_key]
            else:
                # Expired, remove from memory
                del self._cache_memory[cache_key]
                del self._cache_timestamps[cache_key]
        
        # Check metadata before file I/O
        if cache_key not in self._cache_timestamps:
            return None
        
        cached_time = self._cache_timestamps[cache_key]
        current_time = time.time()
        
        if current_time - cached_time > self.ttl_seconds:
            # Expired
            del self._cache_timestamps[cache_key]
            cache_file = self._get_cache_file_path(cache_key)
            try:
                os.remove(cache_file)
            except:
                pass
            return None
        
        # Load from file and cache in memory
        cache_file = self._get_cache_file_path(cache_key)
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            response_data = cache_data.get('response')
            
            # Cache in memory for faster future access
            self._cache_memory[cache_key] = response_data
            
            if self.verbose:
                print(f"Cache HIT (file) for {url} (age: {int((current_time - cached_time)/60)} minutes)")
            
            return response_data
            
        except (json.JSONDecodeError, FileNotFoundError, KeyError):
            # Corrupted cache file, clean up
            if cache_key in self._cache_timestamps:
                del self._cache_timestamps[cache_key]
            try:
                os.remove(cache_file)
            except:
                pass
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
        current_time = time.time()
        
        cache_data = {
            'timestamp': current_time,
            'url': url,
            'params': params or {},
            'response': response_data
        }
        
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, separators=(',', ':'))  # Compact JSON for speed
            
            # Update in-memory cache
            self._cache_memory[cache_key] = response_data
            self._cache_timestamps[cache_key] = current_time
            
            if self.verbose:
                print(f"Cache SET for {url}")
            
        except Exception as e:
            if self.verbose:
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
        
        if removed_count > 0 and self.verbose:
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
        
        if removed_count > 0 and self.verbose:
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


# Global cache instance (non-verbose by default for clean progress bars)
api_cache = APICache(verbose=False)


def clear_cache():
    """Convenience function to clear all cached data"""
    return api_cache.clear_all()


def get_cache_stats():
    """Convenience function to get cache statistics"""
    return api_cache.get_cache_stats()