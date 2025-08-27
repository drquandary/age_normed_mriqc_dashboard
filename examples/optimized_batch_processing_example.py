"""
Example usage of optimized batch processing features.

This script demonstrates how to use the performance optimization features
including caching, connection pooling, and optimized batch processing.
"""

import asyncio
import time
from pathlib import Path
from typing import List

from app.optimized_batch_processor import OptimizedBatchProcessor, BatchConfig
from app.cache_service import cache_service
from app.connection_pool import get_connection_pool
from app.performance_monitor import performance_monitor, monitor_performance
from app.database import NormativeDatabase


def progress_callback(progress_info):
    """Progress callback for batch processing."""
    print(f"Progress: {progress_info['processed']}/{progress_info['total']} "
          f"({progress_info['progress_percent']:.1f}%)")


@monitor_performance("example_batch_processing")
def example_batch_processing():
    """Example of optimized batch processing."""
    print("=== Optimized Batch Processing Example ===")
    
    # Configure batch processor
    config = BatchConfig(
        chunk_size=50,  # Process 50 files per chunk
        max_workers=4,  # Use 4 worker processes
        use_multiprocessing=True,  # Use multiprocessing for CPU-bound tasks
        memory_limit_mb=512,  # 512MB memory limit per worker
        cache_results=True,  # Enable result caching
        progress_callback=progress_callback  # Progress updates
    )
    
    processor = OptimizedBatchProcessor(config)
    
    # Example file paths (in practice, these would be real MRIQC files)
    file_paths = [f"data/sample_mriqc_{i:03d}.csv" for i in range(200)]
    
    print(f"Processing {len(file_paths)} files with optimized batch processor...")
    
    start_time = time.time()
    
    # Process files in batches
    try:
        result = processor.process_files_batch(
            file_paths,
            apply_quality_assessment=True,
            custom_thresholds=None
        )
        
        processing_time = time.time() - start_time
        
        print(f"\nBatch Processing Results:")
        print(f"  Total files: {result.total_files}")
        print(f"  Successful: {result.successful}")
        print(f"  Failed: {result.failed}")
        print(f"  Processing time: {processing_time:.2f}s")
        print(f"  Memory usage: {result.memory_usage_mb:.1f} MB")
        print(f"  Throughput: {result.total_files / processing_time:.1f} files/second")
        
        if result.errors:
            print(f"\nErrors encountered:")
            for error in result.errors[:5]:  # Show first 5 errors
                print(f"  - {error.file_path}: {error.message}")
    
    except Exception as e:
        print(f"Error in batch processing: {e}")


@monitor_performance("example_cache_usage")
def example_cache_usage():
    """Example of cache service usage."""
    print("\n=== Cache Service Example ===")
    
    if not cache_service.is_available():
        print("Redis cache is not available - skipping cache examples")
        return
    
    # Test basic caching
    print("Testing basic cache operations...")
    
    test_data = {
        "metric": "snr",
        "age_group": "young_adult",
        "mean": 18.5,
        "std": 2.7,
        "percentiles": [14.2, 16.8, 18.4, 20.3, 23.2]
    }
    
    # Set cache entry
    cache_service.set("test_normative_data", test_data, ttl=300)
    print("  ✓ Cached normative data")
    
    # Get cache entry
    retrieved_data = cache_service.get("test_normative_data")
    if retrieved_data == test_data:
        print("  ✓ Successfully retrieved cached data")
    else:
        print("  ✗ Cache retrieval failed")
    
    # Test normative data caching
    print("Testing normative data caching...")
    
    normative_data = {
        "mean_value": 15.2,
        "std_value": 3.1,
        "percentile_5": 10.5,
        "percentile_95": 20.8
    }
    
    cache_service.set_normative_data("snr", 1, normative_data)
    retrieved = cache_service.get_normative_data("snr", 1)
    
    if retrieved == normative_data:
        print("  ✓ Normative data caching works correctly")
    else:
        print("  ✗ Normative data caching failed")
    
    # Get cache statistics
    stats = cache_service.get_cache_stats()
    if stats.get("available"):
        print(f"Cache statistics:")
        print(f"  Memory usage: {stats.get('used_memory', 'N/A')}")
        print(f"  Hit rate: {stats.get('hit_rate', 0):.1f}%")
        print(f"  Connected clients: {stats.get('connected_clients', 0)}")


@monitor_performance("example_connection_pool")
def example_connection_pool():
    """Example of database connection pooling."""
    print("\n=== Connection Pool Example ===")
    
    try:
        # Get connection pool
        pool = get_connection_pool()
        
        print("Connection pool statistics:")
        stats = pool.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # Test multiple concurrent connections
        print("\nTesting concurrent database access...")
        
        def database_operation():
            with pool.get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM age_groups")
                result = cursor.fetchone()
                return result[0]
        
        # Simulate concurrent operations
        import threading
        results = []
        threads = []
        
        def worker():
            try:
                result = database_operation()
                results.append(result)
            except Exception as e:
                print(f"Database operation failed: {e}")
        
        # Start multiple threads
        for _ in range(10):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        print(f"  Completed {len(results)} concurrent database operations")
        print(f"  All results consistent: {len(set(results)) == 1}")
        
        # Show updated pool statistics
        final_stats = pool.get_stats()
        print("Final connection pool statistics:")
        for key, value in final_stats.items():
            print(f"  {key}: {value}")
    
    except Exception as e:
        print(f"Connection pool example failed: {e}")


@monitor_performance("example_performance_monitoring")
def example_performance_monitoring():
    """Example of performance monitoring."""
    print("\n=== Performance Monitoring Example ===")
    
    # Simulate some operations with performance monitoring
    with performance_monitor.measure_operation("database_query", {"table": "age_groups"}):
        time.sleep(0.1)  # Simulate database query
    
    with performance_monitor.measure_operation("cache_lookup", {"key": "test"}):
        time.sleep(0.05)  # Simulate cache lookup
    
    with performance_monitor.measure_operation("file_processing", {"type": "mriqc"}):
        time.sleep(0.2)  # Simulate file processing
    
    # Record custom metrics
    performance_monitor.record_metric("memory_usage", 256.5, "MB")
    performance_monitor.record_metric("cpu_usage", 45.2, "percent")
    
    # Get performance summary
    summary = performance_monitor.get_performance_summary()
    
    print("Performance Summary:")
    print(f"  System uptime: {summary['system_metrics']['uptime_seconds']:.1f}s")
    print(f"  Total operations: {summary['system_metrics']['total_operations']}")
    print(f"  Total errors: {summary['system_metrics']['total_errors']}")
    
    if summary['slowest_operations']:
        print("  Slowest operations:")
        for op in summary['slowest_operations']:
            print(f"    {op['name']}: {op['avg_time']:.3f}s avg")
    
    if summary['most_frequent_operations']:
        print("  Most frequent operations:")
        for op in summary['most_frequent_operations']:
            print(f"    {op['name']}: {op['total_calls']} calls")


async def example_async_batch_processing():
    """Example of asynchronous batch processing."""
    print("\n=== Async Batch Processing Example ===")
    
    config = BatchConfig(
        chunk_size=25,
        max_workers=3,
        use_multiprocessing=False,  # Use threading for async
        progress_callback=progress_callback
    )
    
    processor = OptimizedBatchProcessor(config)
    
    # Example file paths
    file_paths = [f"data/async_sample_{i:03d}.csv" for i in range(50)]
    
    print(f"Processing {len(file_paths)} files asynchronously...")
    
    start_time = time.time()
    
    try:
        result = await processor.process_files_async(
            file_paths,
            apply_quality_assessment=True
        )
        
        processing_time = time.time() - start_time
        
        print(f"\nAsync Batch Processing Results:")
        print(f"  Total files: {result.total_files}")
        print(f"  Successful: {result.successful}")
        print(f"  Failed: {result.failed}")
        print(f"  Processing time: {processing_time:.2f}s")
        print(f"  Throughput: {result.total_files / processing_time:.1f} files/second")
    
    except Exception as e:
        print(f"Error in async batch processing: {e}")


def main():
    """Run all examples."""
    print("Performance Optimization Examples")
    print("=" * 50)
    
    # Run synchronous examples
    example_cache_usage()
    example_connection_pool()
    example_performance_monitoring()
    example_batch_processing()
    
    # Run asynchronous example
    print("\nRunning async example...")
    asyncio.run(example_async_batch_processing())
    
    # Final performance summary
    print("\n=== Final Performance Summary ===")
    final_summary = performance_monitor.get_performance_summary()
    
    print(f"Total operations monitored: {final_summary['system_metrics']['total_operations']}")
    print(f"Total errors: {final_summary['system_metrics']['total_errors']}")
    
    if final_summary['slowest_operations']:
        print("\nSlowest operations overall:")
        for op in final_summary['slowest_operations']:
            print(f"  {op['name']}: {op['avg_time']:.3f}s avg ({op['total_calls']} calls)")
    
    # Export performance metrics
    try:
        metrics_json = performance_monitor.export_metrics("json")
        print(f"\nPerformance metrics exported ({len(metrics_json)} characters)")
    except Exception as e:
        print(f"Failed to export metrics: {e}")


if __name__ == "__main__":
    main()