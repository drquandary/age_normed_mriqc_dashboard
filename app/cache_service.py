"""
Redis caching service for performance optimization.

This module provides caching functionality for normative data, computed results,
and frequently accessed data to improve application performance.
"""

import json
import logging
import pickle
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
import hashlib

import redis
from redis.exceptions import ConnectionError, TimeoutError

from .config import REDIS_URL
from .common_utils.logging_config import setup_logging

logger = setup_logging(__name__)


class CacheService:
    """Redis-based caching service for performance optimization."""
    
    def __init__(self, redis_url: str = REDIS_URL):
        """Initialize cache service with Redis connection."""
        try:
            self.redis_client = redis.Redis.from_url(redis_url, decode_responses=False)
            # Test connection
            self.redis_client.ping()
            logger.info("Redis cache service initialized successfully")
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None
    
    def is_available(self) -> bool:
        """Check if Redis cache is available."""
        if not self.redis_client:
            return False
        try:
            self.redis_client.ping()
            return True
        except Exception:
            return False
    
    def _generate_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate a cache key from prefix and arguments."""
        key_parts = [prefix]
        
        # Add positional arguments
        for arg in args:
            if isinstance(arg, (str, int, float)):
                key_parts.append(str(arg))
            else:
                # Hash complex objects
                key_parts.append(hashlib.md5(str(arg).encode()).hexdigest()[:8])
        
        # Add keyword arguments (sorted for consistency)
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{v}")
        
        return ":".join(key_parts)    

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if not self.is_available():
            return None
        
        try:
            value = self.redis_client.get(key)
            if value is None:
                return None
            
            # Try to deserialize as JSON first, then pickle
            try:
                return json.loads(value.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                return pickle.loads(value)
        except Exception as e:
            logger.warning(f"Cache get failed for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """Set value in cache with TTL."""
        if not self.is_available():
            return False
        
        try:
            # Try to serialize as JSON first, then pickle
            try:
                serialized = json.dumps(value, default=str)
            except (TypeError, ValueError):
                serialized = pickle.dumps(value)
            
            return self.redis_client.setex(key, ttl, serialized)
        except Exception as e:
            logger.warning(f"Cache set failed for key {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        if not self.is_available():
            return False
        
        try:
            return bool(self.redis_client.delete(key))
        except Exception as e:
            logger.warning(f"Cache delete failed for key {key}: {e}")
            return False
    
    def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern."""
        if not self.is_available():
            return 0
        
        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                return self.redis_client.delete(*keys)
            return 0
        except Exception as e:
            logger.warning(f"Cache clear pattern failed for {pattern}: {e}")
            return 0  
  # Normative Data Caching Methods
    
    def get_normative_data(self, metric_name: str, age_group_id: int) -> Optional[Dict]:
        """Get cached normative data."""
        key = self._generate_key("normative", metric_name, age_group_id)
        return self.get(key)
    
    def set_normative_data(self, metric_name: str, age_group_id: int, 
                          data: Dict, ttl: int = 86400) -> bool:
        """Cache normative data (24 hour TTL by default)."""
        key = self._generate_key("normative", metric_name, age_group_id)
        return self.set(key, data, ttl)
    
    def get_age_groups(self) -> Optional[List[Dict]]:
        """Get cached age groups."""
        return self.get("age_groups")
    
    def set_age_groups(self, age_groups: List[Dict], ttl: int = 86400) -> bool:
        """Cache age groups (24 hour TTL by default)."""
        return self.set("age_groups", age_groups, ttl)
    
    def get_quality_thresholds(self, metric_name: str, age_group_id: int) -> Optional[Dict]:
        """Get cached quality thresholds."""
        key = self._generate_key("thresholds", metric_name, age_group_id)
        return self.get(key)
    
    def set_quality_thresholds(self, metric_name: str, age_group_id: int, 
                              thresholds: Dict, ttl: int = 86400) -> bool:
        """Cache quality thresholds (24 hour TTL by default)."""
        key = self._generate_key("thresholds", metric_name, age_group_id)
        return self.set(key, thresholds, ttl)
    
    # Computed Results Caching Methods
    
    def get_normalized_metrics(self, metrics_hash: str, age: float) -> Optional[Dict]:
        """Get cached normalized metrics."""
        key = self._generate_key("normalized", metrics_hash, age)
        return self.get(key)
    
    def set_normalized_metrics(self, metrics_hash: str, age: float, 
                              normalized_data: Dict, ttl: int = 3600) -> bool:
        """Cache normalized metrics (1 hour TTL by default)."""
        key = self._generate_key("normalized", metrics_hash, age)
        return self.set(key, normalized_data, ttl)
    
    def get_quality_assessment(self, metrics_hash: str, age: float, 
                              thresholds_hash: str) -> Optional[Dict]:
        """Get cached quality assessment."""
        key = self._generate_key("assessment", metrics_hash, age, thresholds_hash)
        return self.get(key)
    
    def set_quality_assessment(self, metrics_hash: str, age: float, 
                              thresholds_hash: str, assessment: Dict, 
                              ttl: int = 3600) -> bool:
        """Cache quality assessment (1 hour TTL by default)."""
        key = self._generate_key("assessment", metrics_hash, age, thresholds_hash)
        return self.set(key, assessment, ttl)
    
    # Batch Processing Caching Methods
    
    def get_batch_status(self, batch_id: str) -> Optional[Dict]:
        """Get cached batch processing status."""
        key = self._generate_key("batch_status", batch_id)
        return self.get(key)
    
    def set_batch_status(self, batch_id: str, status: Dict, ttl: int = 7200) -> bool:
        """Cache batch processing status (2 hour TTL by default)."""
        key = self._generate_key("batch_status", batch_id)
        return self.set(key, status, ttl)
    
    def get_processed_subject(self, subject_id: str, session: str = None) -> Optional[Dict]:
        """Get cached processed subject data."""
        key = self._generate_key("subject", subject_id, session or "default")
        return self.get(key)
    
    def set_processed_subject(self, subject_id: str, subject_data: Dict, 
                             session: str = None, ttl: int = 3600) -> bool:
        """Cache processed subject data (1 hour TTL by default)."""
        key = self._generate_key("subject", subject_id, session or "default")
        return self.set(key, subject_data, ttl)
    
    # Study Configuration Caching Methods
    
    def get_study_config(self, study_name: str) -> Optional[Dict]:
        """Get cached study configuration."""
        key = self._generate_key("study_config", study_name)
        return self.get(key)
    
    def set_study_config(self, study_name: str, config: Dict, ttl: int = 86400) -> bool:
        """Cache study configuration (24 hour TTL by default)."""
        key = self._generate_key("study_config", study_name)
        return self.set(key, config, ttl)
    
    def invalidate_study_config(self, study_name: str) -> bool:
        """Invalidate cached study configuration."""
        key = self._generate_key("study_config", study_name)
        return self.delete(key)
    
    # Utility Methods
    
    def generate_hash(self, data: Any) -> str:
        """Generate hash for data to use as cache key component."""
        if isinstance(data, dict):
            # Sort dict for consistent hashing
            data_str = json.dumps(data, sort_keys=True, default=str)
        else:
            data_str = str(data)
        
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        if not self.is_available():
            return {"available": False}
        
        try:
            info = self.redis_client.info()
            return {
                "available": True,
                "used_memory": info.get("used_memory_human", "N/A"),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "hit_rate": (info.get("keyspace_hits", 0) / 
                           max(info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0), 1)) * 100
            }
        except Exception as e:
            logger.warning(f"Failed to get cache stats: {e}")
            return {"available": False, "error": str(e)}


# Global cache service instance
cache_service = CacheService()