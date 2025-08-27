"""
Example usage of batch processing and file monitoring features.

This script demonstrates how to use the batch processing service
and file monitoring capabilities for automated MRIQC processing.
"""

import asyncio
import time
from pathlib import Path
import pandas as pd

from app.batch_service import batch_service
from app.file_monitor import file_monitor


def create_sample_mriqc_files(directory: Path, num_files: int = 3):
    """Create sample MRIQC files for testing."""
    directory.mkdir(parents=True, exist_ok=True)
    
    file_paths = []
    
    for i in range(num_files):
        # Create sample MRIQC data
        data = pd.DataFrame({
            'bids_name': [f'sub-{j:03d}_T1w' for j in range(i*10, (i+1)*10)],
            'subject_id': [f'sub-{j:03d}' for j in range(i*10, (i+1)*10)],
            'session_id': ['ses-01'] * 10,
            'snr': [12.5 + j*0.1 for j in range(10)],
            'cnr': [3.2 + j*0.05 for j in range(10)],
            'fber': [1500.0 + j*10 for j in range(10)],
            'efc': [0.45 + j*0.01 for j in range(10)],
            'fwhm_avg': [2.8 + j*0.02 for j in range(10)]
        })
        
        file_path = directory / f'mriqc_batch_{i+1}.csv'
        data.to_csv(file_path, index=False)
        file_paths.append(str(file_path))
        
        print(f"Created sample file: {file_path}")
    
    return file_paths


async def demonstrate_batch_processing():
    """Demonstrate batch processing functionality."""
    print("=== Batch Processing Example ===")
    
    # Create sample files
    sample_dir = Path('data/samples')
    file_paths = create_sample_mriqc_files(sample_dir, num_files=3)
    
    try:
        # Submit batch processing job
        print(f"\nSubmitting batch processing for {len(file_paths)} files...")
        batch_id, task_id = batch_service.submit_batch_processing(
            file_paths,
            apply_quality_assessment=True
        )
        
        print(f"Batch ID: {batch_id}")
        print(f"Task ID: {task_id}")
        
        # Monitor batch progress
        print("\nMonitoring batch progress...")
        while True:
            status = batch_service.get_batch_status(batch_id)
            
            if not status:
                print("Batch not found")
                break
            
            print(f"Status: {status.get('status', 'unknown')}")
            print(f"Progress: {status.get('completed_items', 0)}/{status.get('total_items', 0)}")
            
            if status.get('status') in ['completed', 'failed', 'cancelled']:
                break
            
            await asyncio.sleep(2)
        
        # Get final results
        if status and status.get('status') == 'completed':
            print("\nBatch processing completed!")
            
            results = batch_service.get_batch_results(batch_id)
            if results:
                print(f"Total subjects processed: {results.get('total_subjects', 0)}")
                print(f"Processing errors: {len(results.get('processing_errors', []))}")
            
            # Get processed subjects
            subjects = batch_service.get_processed_subjects(batch_id)
            print(f"Retrieved {len(subjects)} processed subjects")
            
            # Show sample subject data
            if subjects:
                sample_subject = subjects[0]
                print(f"\nSample subject: {sample_subject.subject_info.subject_id}")
                print(f"Quality status: {sample_subject.quality_assessment.overall_status}")
                print(f"Composite score: {sample_subject.quality_assessment.composite_score:.2f}")
        
    except Exception as e:
        print(f"Error in batch processing: {e}")
    
    finally:
        # Cleanup sample files
        for file_path in file_paths:
            Path(file_path).unlink(missing_ok=True)
        sample_dir.rmdir()


def demonstrate_file_monitoring():
    """Demonstrate file monitoring functionality."""
    print("\n=== File Monitoring Example ===")
    
    # Create watch directory
    watch_dir = Path('data/watch')
    watch_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Start monitoring
        print(f"Starting file monitoring for: {watch_dir}")
        success = file_monitor.start_monitoring(
            str(watch_dir),
            auto_process=True,
            recursive=False,
            file_extensions=['.csv']
        )
        
        if not success:
            print("Failed to start monitoring")
            return
        
        print("Monitoring started successfully")
        
        # Check monitoring status
        status = file_monitor.get_monitoring_status(str(watch_dir))
        if status:
            print(f"Auto-process: {status['auto_process']}")
            print(f"File extensions: {status['file_extensions']}")
        
        # Create a test file to trigger processing
        print("\nCreating test file to trigger automatic processing...")
        test_data = pd.DataFrame({
            'bids_name': ['sub-test_T1w'],
            'subject_id': ['sub-test'],
            'session_id': ['ses-01'],
            'snr': [14.2],
            'cnr': [3.8],
            'fber': [1650.0],
            'efc': [0.42],
            'fwhm_avg': [2.7]
        })
        
        test_file = watch_dir / 'auto_test.csv'
        test_data.to_csv(test_file, index=False)
        print(f"Created test file: {test_file}")
        
        # Wait for file to be detected and processed
        print("Waiting for automatic processing...")
        time.sleep(3)
        
        # Check updated status
        status = file_monitor.get_monitoring_status(str(watch_dir))
        if status:
            print(f"Processed files count: {status['processed_files_count']}")
        
        # Get list of all monitored directories
        monitored = file_monitor.get_monitored_directories()
        print(f"\nCurrently monitoring {len(monitored)} directories:")
        for dir_info in monitored:
            print(f"  - {dir_info['directory']} (auto_process: {dir_info['auto_process']})")
        
    except Exception as e:
        print(f"Error in file monitoring: {e}")
    
    finally:
        # Stop monitoring and cleanup
        file_monitor.stop_monitoring(str(watch_dir))
        print(f"\nStopped monitoring: {watch_dir}")
        
        # Cleanup test files
        for file in watch_dir.glob('*.csv'):
            file.unlink()
        watch_dir.rmdir()


def demonstrate_worker_management():
    """Demonstrate worker status and management."""
    print("\n=== Worker Management Example ===")
    
    try:
        # Get worker status
        worker_status = batch_service.get_worker_status()
        
        print("Worker Status:")
        print(f"Broker URL: {worker_status.get('broker_url', 'N/A')}")
        print(f"Result Backend: {worker_status.get('result_backend', 'N/A')}")
        
        active_workers = worker_status.get('active_workers', {})
        print(f"Active Workers: {len(active_workers)}")
        
        for worker_name, tasks in active_workers.items():
            print(f"  - {worker_name}: {len(tasks)} active tasks")
        
        # Get active batches
        active_batches = batch_service.get_active_batches()
        print(f"\nActive Batches: {len(active_batches)}")
        
        for batch in active_batches:
            print(f"  - {batch.get('batch_id', 'N/A')}: {batch.get('status', 'unknown')}")
        
    except Exception as e:
        print(f"Error getting worker status: {e}")


def demonstrate_custom_thresholds():
    """Demonstrate batch processing with custom quality thresholds."""
    print("\n=== Custom Thresholds Example ===")
    
    # Define custom quality thresholds
    custom_thresholds = {
        'snr': {
            'warning_threshold': 10.0,
            'fail_threshold': 8.0,
            'direction': 'higher_better'
        },
        'cnr': {
            'warning_threshold': 3.0,
            'fail_threshold': 2.5,
            'direction': 'higher_better'
        },
        'efc': {
            'warning_threshold': 0.5,
            'fail_threshold': 0.6,
            'direction': 'lower_better'
        }
    }
    
    print("Custom thresholds defined:")
    for metric, thresholds in custom_thresholds.items():
        print(f"  {metric}: warning={thresholds['warning_threshold']}, "
              f"fail={thresholds['fail_threshold']} ({thresholds['direction']})")
    
    # Create sample file with varying quality
    sample_dir = Path('data/custom_threshold_test')
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Create data with some subjects that will fail/warn with custom thresholds
        test_data = pd.DataFrame({
            'bids_name': ['sub-good_T1w', 'sub-warn_T1w', 'sub-fail_T1w'],
            'subject_id': ['sub-good', 'sub-warn', 'sub-fail'],
            'session_id': ['ses-01', 'ses-01', 'ses-01'],
            'snr': [15.0, 9.5, 7.0],  # good, warning, fail
            'cnr': [4.0, 2.8, 2.0],   # good, warning, fail
            'fber': [1600.0, 1400.0, 1200.0],
            'efc': [0.4, 0.52, 0.65],  # good, warning, fail
            'fwhm_avg': [2.5, 2.8, 3.2]
        })
        
        test_file = sample_dir / 'custom_threshold_test.csv'
        test_data.to_csv(test_file, index=False)
        
        print(f"\nCreated test file with varying quality: {test_file}")
        print("Expected results with custom thresholds:")
        print("  - sub-good: PASS")
        print("  - sub-warn: WARNING")
        print("  - sub-fail: FAIL")
        
        # Note: Actual processing would require running Celery worker
        print("\nTo see results, start Celery worker and submit this file for processing")
        
    except Exception as e:
        print(f"Error creating custom threshold test: {e}")
    
    finally:
        # Cleanup
        if sample_dir.exists():
            for file in sample_dir.glob('*.csv'):
                file.unlink()
            sample_dir.rmdir()


async def main():
    """Main example function."""
    print("MRIQC Dashboard Batch Processing Examples")
    print("=" * 50)
    
    # Note: These examples assume Redis and Celery worker are running
    print("Prerequisites:")
    print("1. Redis server running on localhost:6379")
    print("2. Celery worker started with: python start_celery.py")
    print("3. Optional: Celery beat for periodic tasks: python start_celery_beat.py")
    print()
    
    try:
        # Demonstrate different features
        await demonstrate_batch_processing()
        demonstrate_file_monitoring()
        demonstrate_worker_management()
        demonstrate_custom_thresholds()
        
    except KeyboardInterrupt:
        print("\nExample interrupted by user")
    except Exception as e:
        print(f"Example failed with error: {e}")
    finally:
        # Stop all monitoring
        file_monitor.stop_all_monitoring()
        print("\nStopped all file monitoring")


if __name__ == '__main__':
    asyncio.run(main())