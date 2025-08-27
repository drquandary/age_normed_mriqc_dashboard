"""
Performance monitoring and metrics collection.

This module provides utilities for monitoring application performance,
collecting metrics, and identifying optimization opportunities.
"""

import time
import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
import json

from .cache_service import cache_service
from .connection_pool import get_connection_pool
from .common_utils.logging_config import setup_logging

logger = setup_logging(__name__)


@dataclass
class PerformanceMetric:
    """Individual performance metric."""
    name: str
    value: float
    unit: str
    timestamp: datetime = field(default_factory=datetime.now)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class OperationStats:
    """Statistics for a specific operation."""
    operation_name: str
    total_calls: int = 0
    total_time: float = 0.0
    min_time: float = float('inf')
    max_time: float = 0.0
    avg_time: float = 0.0
    error_count: int = 0
    last_called: Optional[datetime] = None
    
    def update(self, execution_time: float, error: bool = False):
        """Update statistics with new execution."""
        self.total_calls += 1
        self.total_time += execution_time
        self.min_time = min(self.min_time, execution_time)
        self.max_time = max(self.max_time, execution_time)
        self.avg_time = self.total_time / self.total_calls
        self.last_called = datetime.now()
        
        if error:
            self.error_count += 1


class PerformanceMonitor:
    """Performance monitoring and metrics collection."""
    
    def __init__(self):
        self.metrics: List[PerformanceMetric] = []
        self.operation_stats: Dict[str, OperationStats] = {}
        self._lock = threading.RLock()
        self.start_time = datetime.now()
    
    @contextmanager
    def measure_operation(self, operation_name: str, tags: Dict[str, str] = None):
        """Context manager to measure operation performance."""
        start_time = time.time()
        error_occurred = False
        
        try:
            yield
        except Exception as e:
            error_occurred = True
            logger.error(f"Error in operation {operation_name}: {e}")
            raise
        finally:
            execution_time = time.time() - start_time
            self.record_operation(operation_name, execution_time, error_occurred, tags)
    
    def record_operation(self, operation_name: str, execution_time: float, 
                        error: bool = False, tags: Dict[str, str] = None):
        """Record performance data for an operation."""
        with self._lock:
            # Update operation statistics
            if operation_name not in self.operation_stats:
                self.operation_stats[operation_name] = OperationStats(operation_name)
            
            self.operation_stats[operation_name].update(execution_time, error)
            
            # Add performance metric
            metric = PerformanceMetric(
                name=f"{operation_name}_duration",
                value=execution_time,
                unit="seconds",
                tags=tags or {}
            )
            self.metrics.append(metric)
            
            # Keep only recent metrics (last 1000)
            if len(self.metrics) > 1000:
                self.metrics = self.metrics[-1000:]
    
    def record_metric(self, name: str, value: float, unit: str = "", 
                     tags: Dict[str, str] = None):
        """Record a custom metric."""
        with self._lock:
            metric = PerformanceMetric(
                name=name,
                value=value,
                unit=unit,
                tags=tags or {}
            )
            self.metrics.append(metric)
    
    def get_operation_stats(self, operation_name: str = None) -> Dict[str, Any]:
        """Get statistics for operations."""
        with self._lock:
            if operation_name:
                stats = self.operation_stats.get(operation_name)
                return stats.__dict__ if stats else {}
            else:
                return {name: stats.__dict__ for name, stats in self.operation_stats.items()}
    
    def get_recent_metrics(self, minutes: int = 60) -> List[PerformanceMetric]:
        """Get metrics from the last N minutes."""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        with self._lock:
            return [m for m in self.metrics if m.timestamp >= cutoff_time]
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get current system performance metrics."""
        metrics = {
            "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
            "total_operations": sum(stats.total_calls for stats in self.operation_stats.values()),
            "total_errors": sum(stats.error_count for stats in self.operation_stats.values())
        }
        
        # Add cache metrics if available
        cache_stats = cache_service.get_cache_stats()
        if cache_stats.get("available"):
            metrics.update({
                "cache_hit_rate": cache_stats.get("hit_rate", 0),
                "cache_memory_usage": cache_stats.get("used_memory", "N/A")
            })
        
        # Add database connection pool metrics
        try:
            pool = get_connection_pool()
            pool_stats = pool.get_stats()
            metrics.update({
                "db_pool_active_connections": pool_stats.get("active_connections", 0),
                "db_pool_available_connections": pool_stats.get("available_connections", 0),
                "db_pool_total_connections": pool_stats.get("total_connections", 0)
            })
        except Exception as e:
            logger.warning(f"Could not get database pool stats: {e}")
        
        # Add memory usage if available
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            metrics.update({
                "memory_rss_mb": memory_info.rss / 1024 / 1024,
                "memory_vms_mb": memory_info.vms / 1024 / 1024,
                "cpu_percent": process.cpu_percent()
            })
        except ImportError:
            pass
        
        return metrics
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary."""
        with self._lock:
            summary = {
                "system_metrics": self.get_system_metrics(),
                "operation_stats": self.get_operation_stats(),
                "recent_metrics_count": len(self.get_recent_metrics(60)),
                "slowest_operations": self._get_slowest_operations(),
                "most_frequent_operations": self._get_most_frequent_operations(),
                "error_rates": self._get_error_rates()
            }
        
        return summary
    
    def _get_slowest_operations(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get the slowest operations by average time."""
        operations = list(self.operation_stats.values())
        operations.sort(key=lambda x: x.avg_time, reverse=True)
        
        return [
            {
                "name": op.operation_name,
                "avg_time": op.avg_time,
                "max_time": op.max_time,
                "total_calls": op.total_calls
            }
            for op in operations[:limit]
        ]
    
    def _get_most_frequent_operations(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get the most frequently called operations."""
        operations = list(self.operation_stats.values())
        operations.sort(key=lambda x: x.total_calls, reverse=True)
        
        return [
            {
                "name": op.operation_name,
                "total_calls": op.total_calls,
                "avg_time": op.avg_time,
                "total_time": op.total_time
            }
            for op in operations[:limit]
        ]
    
    def _get_error_rates(self) -> List[Dict[str, Any]]:
        """Get error rates for operations."""
        error_rates = []
        
        for op in self.operation_stats.values():
            if op.total_calls > 0:
                error_rate = (op.error_count / op.total_calls) * 100
                if error_rate > 0:
                    error_rates.append({
                        "name": op.operation_name,
                        "error_rate_percent": error_rate,
                        "error_count": op.error_count,
                        "total_calls": op.total_calls
                    })
        
        error_rates.sort(key=lambda x: x["error_rate_percent"], reverse=True)
        return error_rates
    
    def export_metrics(self, format: str = "json") -> str:
        """Export metrics in specified format."""
        data = self.get_performance_summary()
        
        if format.lower() == "json":
            return json.dumps(data, indent=2, default=str)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def reset_stats(self):
        """Reset all performance statistics."""
        with self._lock:
            self.metrics.clear()
            self.operation_stats.clear()
            self.start_time = datetime.now()
            logger.info("Performance statistics reset")


# Global performance monitor instance
performance_monitor = PerformanceMonitor()


def monitor_performance(operation_name: str, tags: Dict[str, str] = None):
    """Decorator to monitor function performance."""
    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            with performance_monitor.measure_operation(operation_name, tags):
                return func(*args, **kwargs)
        return wrapper
    return decorator