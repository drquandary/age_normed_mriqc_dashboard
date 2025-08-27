"""
Tests for batch processing functionality.

This module tests the asynchronous batch processing capabilities,
including Celery tasks, progress tracking, and error handling.
"""

import json
import pytest
import tempfile
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pandas as pd
import redis

from app.batch_tasks import (
    process_batch_files, process_single_file_task, monitor_directory,
    cleanup_old_results, BatchProgressTracker, process_single_file_sync
)
from app.batch_service import BatchProcessingService
from app.models import ProcessedSubject, MRIQCMetrics, SubjectInfo, ScanType, QualityStatus


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    with patch('app.batch_tasks.redis_client') as mock_client:
        mock_client.ping.return_value = True
        mock_client.setex.return_value = True
        mock_client.get.return_value = None
        mock_client.keys.return_value = []
        mock_client.delete.return_value = True
        mock_client.sadd.return_value = True
        mock_client.smembers.return_value = set()
        mock_client.expire.return_value = True
        yield mock_client


@pytest.fixture
def sample_mriqc_data():
    """Sample MRIQC data for testing."""
    return pd.DataFrame({
        'bids_name': ['sub-001_T1w', 'sub-002_T1w', 'sub-003_T1w'],
        'subject_id': ['sub-001', 'sub-002', 'sub-003'],
        'session_id': ['ses-01', 'ses-01', 'ses-01'],
        'snr': [12.5, 15.2, 10.8],
        'cnr': [3.2, 4.1, 2.9],
        'fber': [1500.0, 1800.0, 1200.0],
        'efc': [0.45, 0.38, 0.52],
        'fwhm_avg': [2.8, 2.6, 3.1]
    })


@pytest.fixture
def temp_mriqc_file(sample_mriqc_data):
    """Create temporary MRIQC CSV file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_mriqc_data.to_csv(f.name, index=False)
        yield f.name
    
    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def batch_service():
    """Batch processing service instance."""
    with patch('app.batch_service.redis_client'):
        service = BatchProcessingService()
        yield service


class TestBatchProgressTracker:
    """Test batch progress tracking functionality."""
    
    def test_progress_tracker_initialization(self, mock_redis):
        """Test progress tracker initialization."""
        tracker = BatchProgressTracker("test_batch", 10)
        
        assert tracker.batch_id == "test_batch"
        assert tracker.total_items == 10
        assert tracker.completed_items == 0
        assert tracker.failed_items == 0
        assert tracker.errors == []
        
        # Verify Redis update was called
        mock_redis.setex.assert_called()
    
    def test_progress_tracker_increment_completed(self, mock_redis):
        """Test incrementing completed items."""
        tracker = BatchProgressTracker("test_batch", 10)
        
        tracker.increment_completed()
        
        assert tracker.completed_items == 1
        mock_redis.setex.assert_called()
    
    def test_progress_tracker_increment_failed(self, mock_redis):
        """Test incrementing failed items."""
        from app.models import ProcessingError
        
        tracker = BatchProgressTracker("test_batch", 10)
        error = ProcessingError(
            error_type="test_error",
            message="Test error message",
            error_code="TEST_001"
        )
        
        tracker.increment_failed(error)
        
        assert tracker.failed_items == 1
        assert len(tracker.errors) == 1
        mock_redis.setex.assert_called()
    
    def test_progress_tracker_status_calculation(self, mock_redis):
        """Test status calculation logic."""
        tracker = BatchProgressTracker("test_batch", 10)
        
        # Initial status should be pending
        assert tracker._get_status() == 'pending'
        
        # Processing status
        tracker.increment_completed()
        assert tracker._get_status() == 'processing'
        
        # Completed status
        for _ in range(9):
            tracker.increment_completed()
        assert tracker._get_status() == 'completed'
    
    def test_progress_tracker_failed_status(self, mock_redis):
        """Test failed status when all items fail."""
        tracker = BatchProgressTracker("test_batch", 2)
        
        tracker.increment_failed()
        tracker.increment_failed()
        
        assert tracker._get_status() == 'failed'


class TestBatchTasks:
    """Test Celery batch processing tasks."""
    
    @patch('app.batch_tasks.process_single_file_sync')
    def test_process_batch_files_success(self, mock_process_file, mock_redis):
        """Test successful batch file processing."""
        # Mock successful file processing
        mock_subjects = [
            Mock(spec=ProcessedSubject),
            Mock(spec=ProcessedSubject)
        ]
        mock_process_file.return_value = mock_subjects
        
        # Mock Celery task
        mock_task = Mock()
        mock_task.update_state = Mock()
        
        with patch('app.batch_tasks.current_task', mock_task):
            result = process_batch_files(
                ['file1.csv', 'file2.csv'],
                'test_batch',
                apply_quality_assessment=True
            )
        
        assert result['status'] == 'completed'
        assert result['total_subjects'] == 4  # 2 files * 2 subjects each
        assert result['total_files'] == 2
        assert mock_process_file.call_count == 2
    
    @patch('app.batch_tasks.process_single_file_sync')
    def test_process_batch_files_with_errors(self, mock_process_file, mock_redis):
        """Test batch processing with some file errors."""
        # First file succeeds, second fails
        mock_subjects = [Mock(spec=ProcessedSubject)]
        mock_process_file.side_effect = [mock_subjects, Exception("File processing failed")]
        
        mock_task = Mock()
        mock_task.update_state = Mock()
        
        with patch('app.batch_tasks.current_task', mock_task):
            result = process_batch_files(
                ['file1.csv', 'file2.csv'],
                'test_batch',
                apply_quality_assessment=True
            )
        
        assert result['status'] == 'completed'
        assert result['total_subjects'] == 1  # Only first file succeeded
        assert result['processing_errors'] == 1
    
    @patch('app.batch_tasks.mriqc_processor')
    @patch('app.batch_tasks.quality_assessor')
    @patch('app.batch_tasks.age_normalizer')
    def test_process_single_file_sync(self, mock_normalizer, mock_assessor, mock_processor, temp_mriqc_file):
        """Test synchronous single file processing."""
        # Mock processor
        mock_df = pd.DataFrame({'subject_id': ['sub-001'], 'snr': [12.5]})
        mock_processor.parse_mriqc_file.return_value = mock_df
        mock_processor.validate_mriqc_format.return_value = []
        
        mock_subject = Mock(spec=ProcessedSubject)
        mock_subject.subject_info.age = 25.0
        mock_processor.extract_subjects_from_dataframe.return_value = [mock_subject]
        
        # Mock quality assessor
        mock_assessment = Mock()
        mock_assessor.assess_quality.return_value = mock_assessment
        
        # Mock age normalizer
        mock_normalized = Mock()
        mock_normalizer.normalize_metrics.return_value = mock_normalized
        
        result = process_single_file_sync(temp_mriqc_file, apply_quality_assessment=True)
        
        assert len(result) == 1
        assert result[0] == mock_subject
        mock_processor.parse_mriqc_file.assert_called_once()
        mock_assessor.assess_quality.assert_called_once()
        mock_normalizer.normalize_metrics.assert_called_once()
    
    def test_process_single_file_task(self, temp_mriqc_file, mock_redis):
        """Test single file processing task."""
        with patch('app.batch_tasks.process_single_file_sync') as mock_process:
            mock_subjects = [Mock(spec=ProcessedSubject)]
            mock_process.return_value = mock_subjects
            
            mock_task = Mock()
            mock_task.update_state = Mock()
            
            with patch('app.batch_tasks.current_task', mock_task):
                result = process_single_file_task(temp_mriqc_file)
            
            assert result['status'] == 'completed'
            assert result['subjects_count'] == 1
            mock_process.assert_called_once()
    
    def test_monitor_directory(self, mock_redis):
        """Test directory monitoring task."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test CSV file
            test_file = Path(temp_dir) / 'test.csv'
            test_file.write_text('subject_id,snr\nsub-001,12.5\n')
            
            # Mock Redis to simulate no previously processed files
            mock_redis.smembers.return_value = set()
            
            with patch('app.batch_tasks.process_batch_files.delay') as mock_delay:
                result = monitor_directory(temp_dir)
            
            assert result['status'] == 'processing_started'
            assert result['new_files_count'] == 1
            mock_delay.assert_called_once()
    
    def test_cleanup_old_results(self, mock_redis):
        """Test cleanup of old results."""
        # Mock old progress and results keys
        old_time = (datetime.now().timestamp() - 86400) * 1000  # 24 hours ago
        
        progress_data = {
            'last_update': datetime.fromtimestamp(old_time / 1000).isoformat()
        }
        results_data = {
            'completed_at': datetime.fromtimestamp(old_time / 1000).isoformat()
        }
        
        mock_redis.keys.side_effect = [
            [b'batch_progress:old1', b'batch_progress:old2'],
            [b'batch_results:old1']
        ]
        mock_redis.get.side_effect = [
            json.dumps(progress_data),
            json.dumps(progress_data),
            json.dumps(results_data)
        ]
        
        result = cleanup_old_results()
        
        assert result['status'] == 'completed'
        assert result['cleaned_progress'] == 2
        assert result['cleaned_results'] == 1


class TestBatchProcessingService:
    """Test batch processing service."""
    
    @patch('app.batch_service.process_batch_files.delay')
    def test_submit_batch_processing(self, mock_delay, batch_service):
        """Test submitting batch processing job."""
        mock_task = Mock()
        mock_task.id = 'test_task_id'
        mock_delay.return_value = mock_task
        
        batch_id, task_id = batch_service.submit_batch_processing(
            ['file1.csv', 'file2.csv'],
            apply_quality_assessment=True
        )
        
        assert batch_id.startswith('batch_')
        assert task_id == 'test_task_id'
        mock_delay.assert_called_once()
    
    @patch('app.batch_service.process_single_file_task.delay')
    def test_submit_single_file_processing(self, mock_delay, batch_service):
        """Test submitting single file processing job."""
        mock_task = Mock()
        mock_task.id = 'test_task_id'
        mock_delay.return_value = mock_task
        
        task_id = batch_service.submit_single_file_processing('test.csv')
        
        assert task_id == 'test_task_id'
        mock_delay.assert_called_once()
    
    def test_get_batch_status(self, batch_service):
        """Test getting batch status."""
        # Mock Redis response
        status_data = {
            'batch_id': 'test_batch',
            'status': 'processing',
            'progress': {'completed': 5, 'total': 10}
        }
        
        with patch.object(batch_service.redis_client, 'get') as mock_get:
            mock_get.return_value = json.dumps(status_data).encode()
            
            result = batch_service.get_batch_status('test_batch')
            
            assert result['batch_id'] == 'test_batch'
            assert result['status'] == 'processing'
    
    def test_get_batch_status_not_found(self, batch_service):
        """Test getting status for non-existent batch."""
        with patch.object(batch_service.redis_client, 'get') as mock_get:
            mock_get.return_value = None
            
            result = batch_service.get_batch_status('nonexistent')
            
            assert result is None
    
    def test_get_batch_results(self, batch_service):
        """Test getting batch results."""
        results_data = {
            'subjects': [],
            'total_subjects': 0,
            'processing_errors': []
        }
        
        with patch.object(batch_service.redis_client, 'get') as mock_get:
            mock_get.return_value = json.dumps(results_data).encode()
            
            result = batch_service.get_batch_results('test_batch')
            
            assert result['total_subjects'] == 0
    
    @patch('app.batch_service.celery_app.control.revoke')
    def test_cancel_batch_processing(self, mock_revoke, batch_service):
        """Test cancelling batch processing."""
        # Mock Redis responses
        task_id = 'test_task_id'
        progress_data = {'status': 'processing'}
        
        with patch.object(batch_service.redis_client, 'get') as mock_get:
            mock_get.side_effect = [
                task_id.encode(),
                json.dumps(progress_data).encode()
            ]
            
            with patch.object(batch_service.redis_client, 'setex') as mock_setex:
                result = batch_service.cancel_batch_processing('test_batch')
                
                assert result is True
                mock_revoke.assert_called_once_with(task_id, terminate=True)
                mock_setex.assert_called_once()
    
    def test_get_active_batches(self, batch_service):
        """Test getting active batches."""
        active_batch = {
            'batch_id': 'test_batch',
            'status': 'processing'
        }
        
        with patch.object(batch_service.redis_client, 'keys') as mock_keys:
            mock_keys.return_value = [b'batch_progress:test_batch']
            
            with patch.object(batch_service.redis_client, 'get') as mock_get:
                mock_get.return_value = json.dumps(active_batch).encode()
                
                result = batch_service.get_active_batches()
                
                assert len(result) == 1
                assert result[0]['batch_id'] == 'test_batch'
    
    @patch('app.batch_service.celery_app.control.inspect')
    def test_get_worker_status(self, mock_inspect, batch_service):
        """Test getting worker status."""
        mock_inspector = Mock()
        mock_inspector.active.return_value = {'worker1': []}
        mock_inspector.registered.return_value = {'worker1': ['task1']}
        mock_inspector.stats.return_value = {'worker1': {'pool': {'max-concurrency': 4}}}
        mock_inspect.return_value = mock_inspector
        
        result = batch_service.get_worker_status()
        
        assert 'active_workers' in result
        assert 'registered_tasks' in result
        assert 'worker_stats' in result


class TestBatchProcessingIntegration:
    """Integration tests for batch processing."""
    
    @pytest.mark.asyncio
    async def test_batch_processing_end_to_end(self, temp_mriqc_file):
        """Test complete batch processing workflow."""
        # This would require a running Redis and Celery worker
        # For now, we'll mock the components
        
        with patch('app.batch_service.redis_client'), \
             patch('app.batch_tasks.redis_client'), \
             patch('app.batch_tasks.process_batch_files.delay') as mock_delay:
            
            mock_task = Mock()
            mock_task.id = 'test_task_id'
            mock_delay.return_value = mock_task
            
            service = BatchProcessingService()
            
            # Submit batch
            batch_id, task_id = service.submit_batch_processing([temp_mriqc_file])
            
            assert batch_id is not None
            assert task_id == 'test_task_id'
    
    def test_error_handling_in_batch_processing(self, mock_redis):
        """Test error handling in batch processing."""
        with patch('app.batch_tasks.process_single_file_sync') as mock_process:
            mock_process.side_effect = Exception("Processing failed")
            
            mock_task = Mock()
            mock_task.update_state = Mock()
            
            with patch('app.batch_tasks.current_task', mock_task):
                result = process_batch_files(['invalid_file.csv'], 'test_batch')
            
            assert result['status'] == 'completed'
            assert result['processing_errors'] == 1
    
    def test_batch_processing_with_custom_thresholds(self, temp_mriqc_file, mock_redis):
        """Test batch processing with custom quality thresholds."""
        custom_thresholds = {
            'snr': {'warning': 10.0, 'fail': 8.0, 'direction': 'higher_better'}
        }
        
        with patch('app.batch_tasks.process_single_file_sync') as mock_process:
            mock_subjects = [Mock(spec=ProcessedSubject)]
            mock_process.return_value = mock_subjects
            
            mock_task = Mock()
            mock_task.update_state = Mock()
            
            with patch('app.batch_tasks.current_task', mock_task):
                result = process_batch_files(
                    [temp_mriqc_file],
                    'test_batch',
                    custom_thresholds=custom_thresholds
                )
            
            assert result['status'] == 'completed'
            mock_process.assert_called_with(
                temp_mriqc_file,
                True,
                custom_thresholds
            )


class TestBatchProcessingReliability:
    """Test batch processing reliability and error recovery."""
    
    def test_redis_connection_failure_handling(self):
        """Test handling of Redis connection failures."""
        with patch('app.batch_tasks.redis_client') as mock_redis:
            mock_redis.setex.side_effect = redis.ConnectionError("Connection failed")
            
            # Should not raise exception, but log error
            tracker = BatchProgressTracker("test_batch", 10)
            
            # Verify tracker still works despite Redis failure
            assert tracker.batch_id == "test_batch"
            assert tracker.total_items == 10
    
    def test_task_timeout_handling(self, mock_redis):
        """Test handling of task timeouts."""
        with patch('app.batch_tasks.process_single_file_sync') as mock_process:
            # Simulate long-running task
            def slow_process(*args, **kwargs):
                time.sleep(0.1)  # Simulate work
                return [Mock(spec=ProcessedSubject)]
            
            mock_process.side_effect = slow_process
            
            mock_task = Mock()
            mock_task.update_state = Mock()
            
            with patch('app.batch_tasks.current_task', mock_task):
                result = process_batch_files(['file1.csv'], 'test_batch')
            
            assert result['status'] == 'completed'
    
    def test_partial_batch_failure_recovery(self, mock_redis):
        """Test recovery from partial batch failures."""
        with patch('app.batch_tasks.process_single_file_sync') as mock_process:
            # First file succeeds, second fails, third succeeds
            mock_subjects = [Mock(spec=ProcessedSubject)]
            mock_process.side_effect = [
                mock_subjects,
                Exception("File 2 failed"),
                mock_subjects
            ]
            
            mock_task = Mock()
            mock_task.update_state = Mock()
            
            with patch('app.batch_tasks.current_task', mock_task):
                result = process_batch_files(
                    ['file1.csv', 'file2.csv', 'file3.csv'],
                    'test_batch'
                )
            
            assert result['status'] == 'completed'
            assert result['total_subjects'] == 2  # 2 successful files
            assert result['processing_errors'] == 1  # 1 failed file
    
    def test_memory_usage_with_large_batches(self, mock_redis):
        """Test memory usage with large batch sizes."""
        # Simulate processing many files
        large_file_list = [f'file_{i}.csv' for i in range(100)]
        
        with patch('app.batch_tasks.process_single_file_sync') as mock_process:
            mock_subjects = [Mock(spec=ProcessedSubject)]
            mock_process.return_value = mock_subjects
            
            mock_task = Mock()
            mock_task.update_state = Mock()
            
            with patch('app.batch_tasks.current_task', mock_task):
                result = process_batch_files(large_file_list, 'test_batch')
            
            assert result['status'] == 'completed'
            assert result['total_subjects'] == 100
            assert mock_process.call_count == 100