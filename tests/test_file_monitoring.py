"""
Tests for file monitoring functionality.

This module tests the automatic file monitoring and processing capabilities,
including watchdog integration and file system event handling.
"""

import pytest
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from threading import Event

from watchdog.events import FileCreatedEvent, FileModifiedEvent

from app.file_monitor import (
    MRIQCFileHandler, FileMonitorService, setup_default_monitoring
)


@pytest.fixture
def temp_directory():
    """Create temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_batch_service():
    """Mock batch service."""
    with patch('app.file_monitor.batch_service') as mock_service:
        mock_service.submit_single_file_processing.return_value = 'test_task_id'
        yield mock_service


class TestMRIQCFileHandler:
    """Test MRIQC file event handler."""
    
    def test_handler_initialization(self):
        """Test file handler initialization."""
        handler = MRIQCFileHandler(
            auto_process=True,
            file_extensions=['.csv', '.tsv'],
            min_file_size=2048,
            stabilization_time=1.0
        )
        
        assert handler.auto_process is True
        assert handler.file_extensions == ['.csv', '.tsv']
        assert handler.min_file_size == 2048
        assert handler.stabilization_time == 1.0
        assert len(handler.pending_files) == 0
        assert len(handler.processed_files) == 0
    
    def test_file_creation_event_handling(self, temp_directory, mock_batch_service):
        """Test handling of file creation events."""
        handler = MRIQCFileHandler(auto_process=False, stabilization_time=0.1)
        
        # Create test file
        test_file = temp_directory / 'test.csv'
        test_file.write_text('subject_id,snr\nsub-001,12.5\n')
        
        # Simulate file creation event
        event = FileCreatedEvent(str(test_file))
        handler.on_created(event)
        
        # File should be added to pending files
        assert str(test_file) in handler.pending_files
    
    def test_file_modification_event_handling(self, temp_directory):
        """Test handling of file modification events."""
        handler = MRIQCFileHandler(auto_process=False, stabilization_time=0.1)
        
        # Create test file
        test_file = temp_directory / 'test.csv'
        test_file.write_text('subject_id,snr\nsub-001,12.5\n')
        
        # Simulate file modification event
        event = FileModifiedEvent(str(test_file))
        handler.on_modified(event)
        
        # File should be added to pending files
        assert str(test_file) in handler.pending_files
    
    def test_file_extension_filtering(self, temp_directory):
        """Test filtering by file extensions."""
        handler = MRIQCFileHandler(
            auto_process=False,
            file_extensions=['.csv'],
            stabilization_time=0.1
        )
        
        # Create CSV file (should be processed)
        csv_file = temp_directory / 'test.csv'
        csv_file.write_text('subject_id,snr\nsub-001,12.5\n')
        
        # Create TXT file (should be ignored)
        txt_file = temp_directory / 'test.txt'
        txt_file.write_text('some text')
        
        # Simulate events
        csv_event = FileCreatedEvent(str(csv_file))
        txt_event = FileCreatedEvent(str(txt_file))
        
        handler.on_created(csv_event)
        handler.on_created(txt_event)
        
        # Only CSV file should be pending
        assert str(csv_file) in handler.pending_files
        assert str(txt_file) not in handler.pending_files
    
    def test_minimum_file_size_filtering(self, temp_directory):
        """Test filtering by minimum file size."""
        handler = MRIQCFileHandler(
            auto_process=False,
            min_file_size=100,  # 100 bytes minimum
            stabilization_time=0.1
        )
        
        # Create small file (should be ignored)
        small_file = temp_directory / 'small.csv'
        small_file.write_text('small')  # Less than 100 bytes
        
        # Create large file (should be processed)
        large_file = temp_directory / 'large.csv'
        large_file.write_text('x' * 200)  # More than 100 bytes
        
        # Simulate events
        small_event = FileCreatedEvent(str(small_file))
        large_event = FileCreatedEvent(str(large_file))
        
        handler.on_created(small_event)
        handler.on_created(large_event)
        
        # Only large file should be pending
        assert str(small_file) not in handler.pending_files
        assert str(large_file) in handler.pending_files
    
    def test_file_stabilization_processing(self, temp_directory, mock_batch_service):
        """Test file processing after stabilization period."""
        handler = MRIQCFileHandler(
            auto_process=True,
            stabilization_time=0.1  # Short stabilization time for testing
        )
        
        # Create test file with MRIQC-like content
        test_file = temp_directory / 'test.csv'
        test_file.write_text('subject_id,snr,cnr\nsub-001,12.5,3.2\n')
        
        # Simulate file creation event
        event = FileCreatedEvent(str(test_file))
        handler.on_created(event)
        
        # Wait for stabilization and processing
        time.sleep(0.2)
        
        # File should be processed
        mock_batch_service.submit_single_file_processing.assert_called_once()
        assert str(test_file) in handler.processed_files
    
    def test_mriqc_file_detection(self, temp_directory):
        """Test detection of MRIQC files."""
        handler = MRIQCFileHandler(auto_process=False)
        
        # Create MRIQC-like file
        mriqc_file = temp_directory / 'mriqc.csv'
        mriqc_file.write_text('bids_name,subject_id,snr,cnr\nsub-001_T1w,sub-001,12.5,3.2\n')
        
        # Create non-MRIQC file
        other_file = temp_directory / 'other.csv'
        other_file.write_text('name,value\ntest,123\n')
        
        # Test MRIQC file detection
        assert handler._is_likely_mriqc_file(mriqc_file) is True
        assert handler._is_likely_mriqc_file(other_file) is False
    
    def test_handler_stop(self):
        """Test stopping the file handler."""
        handler = MRIQCFileHandler(auto_process=False, stabilization_time=0.1)
        
        # Handler should be running
        assert handler._processing_thread.is_alive()
        
        # Stop handler
        handler.stop()
        
        # Processing thread should stop
        time.sleep(0.2)
        assert not handler._processing_thread.is_alive()
    
    def test_duplicate_file_processing_prevention(self, temp_directory, mock_batch_service):
        """Test prevention of duplicate file processing."""
        handler = MRIQCFileHandler(
            auto_process=True,
            stabilization_time=0.1
        )
        
        # Create test file
        test_file = temp_directory / 'test.csv'
        test_file.write_text('subject_id,snr\nsub-001,12.5\n')
        
        # Simulate multiple events for same file
        event = FileCreatedEvent(str(test_file))
        handler.on_created(event)
        handler.on_created(event)  # Duplicate event
        
        # Wait for processing
        time.sleep(0.2)
        
        # File should only be processed once
        assert mock_batch_service.submit_single_file_processing.call_count == 1
        assert str(test_file) in handler.processed_files


class TestFileMonitorService:
    """Test file monitoring service."""
    
    def test_service_initialization(self):
        """Test service initialization."""
        service = FileMonitorService()
        
        assert len(service.observers) == 0
        assert len(service.handlers) == 0
    
    def test_start_monitoring_new_directory(self, temp_directory):
        """Test starting monitoring for new directory."""
        service = FileMonitorService()
        
        success = service.start_monitoring(
            str(temp_directory),
            auto_process=True,
            recursive=False
        )
        
        assert success is True
        assert str(temp_directory) in service.observers
        assert str(temp_directory) in service.handlers
        
        # Cleanup
        service.stop_monitoring(str(temp_directory))
    
    def test_start_monitoring_nonexistent_directory(self):
        """Test starting monitoring for non-existent directory."""
        service = FileMonitorService()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            nonexistent_dir = Path(temp_dir) / 'nonexistent'
            
            success = service.start_monitoring(
                str(nonexistent_dir),
                auto_process=True
            )
            
            assert success is True
            assert nonexistent_dir.exists()  # Should be created
            
            # Cleanup
            service.stop_monitoring(str(nonexistent_dir))
    
    def test_start_monitoring_already_monitored(self, temp_directory):
        """Test starting monitoring for already monitored directory."""
        service = FileMonitorService()
        
        # Start monitoring
        success1 = service.start_monitoring(str(temp_directory))
        success2 = service.start_monitoring(str(temp_directory))  # Duplicate
        
        assert success1 is True
        assert success2 is True  # Should still return True but log warning
        
        # Should only have one observer
        assert len(service.observers) == 1
        
        # Cleanup
        service.stop_monitoring(str(temp_directory))
    
    def test_stop_monitoring(self, temp_directory):
        """Test stopping directory monitoring."""
        service = FileMonitorService()
        
        # Start monitoring
        service.start_monitoring(str(temp_directory))
        assert str(temp_directory) in service.observers
        
        # Stop monitoring
        success = service.stop_monitoring(str(temp_directory))
        
        assert success is True
        assert str(temp_directory) not in service.observers
        assert str(temp_directory) not in service.handlers
    
    def test_stop_monitoring_not_monitored(self):
        """Test stopping monitoring for non-monitored directory."""
        service = FileMonitorService()
        
        success = service.stop_monitoring('/nonexistent/path')
        
        assert success is True  # Should return True but log warning
    
    def test_stop_all_monitoring(self, temp_directory):
        """Test stopping all monitoring."""
        service = FileMonitorService()
        
        # Start monitoring multiple directories
        with tempfile.TemporaryDirectory() as temp_dir2:
            service.start_monitoring(str(temp_directory))
            service.start_monitoring(temp_dir2)
            
            assert len(service.observers) == 2
            
            # Stop all monitoring
            service.stop_all_monitoring()
            
            assert len(service.observers) == 0
            assert len(service.handlers) == 0
    
    def test_get_monitored_directories(self, temp_directory):
        """Test getting list of monitored directories."""
        service = FileMonitorService()
        
        # Initially no directories
        monitored = service.get_monitored_directories()
        assert len(monitored) == 0
        
        # Start monitoring
        service.start_monitoring(
            str(temp_directory),
            auto_process=True,
            file_extensions=['.csv']
        )
        
        # Should have one monitored directory
        monitored = service.get_monitored_directories()
        assert len(monitored) == 1
        assert monitored[0]['directory'] == str(temp_directory)
        assert monitored[0]['auto_process'] is True
        assert monitored[0]['file_extensions'] == ['.csv']
        
        # Cleanup
        service.stop_monitoring(str(temp_directory))
    
    def test_get_monitoring_status(self, temp_directory):
        """Test getting monitoring status for specific directory."""
        service = FileMonitorService()
        
        # Not monitored initially
        status = service.get_monitoring_status(str(temp_directory))
        assert status is None
        
        # Start monitoring
        service.start_monitoring(
            str(temp_directory),
            auto_process=False,
            file_extensions=['.csv', '.tsv']
        )
        
        # Should have status
        status = service.get_monitoring_status(str(temp_directory))
        assert status is not None
        assert status['directory'] == str(temp_directory)
        assert status['auto_process'] is False
        assert status['file_extensions'] == ['.csv', '.tsv']
        
        # Cleanup
        service.stop_monitoring(str(temp_directory))


class TestFileMonitoringIntegration:
    """Integration tests for file monitoring."""
    
    def test_end_to_end_file_monitoring(self, temp_directory, mock_batch_service):
        """Test complete file monitoring workflow."""
        service = FileMonitorService()
        
        # Start monitoring
        success = service.start_monitoring(
            str(temp_directory),
            auto_process=True,
            recursive=False
        )
        assert success is True
        
        # Create MRIQC file
        test_file = temp_directory / 'new_data.csv'
        test_file.write_text('subject_id,snr,cnr\nsub-001,12.5,3.2\n')
        
        # Wait for file to be detected and processed
        time.sleep(0.5)
        
        # Should have submitted processing job
        mock_batch_service.submit_single_file_processing.assert_called()
        
        # Cleanup
        service.stop_monitoring(str(temp_directory))
    
    def test_recursive_directory_monitoring(self, temp_directory, mock_batch_service):
        """Test recursive directory monitoring."""
        service = FileMonitorService()
        
        # Create subdirectory
        subdir = temp_directory / 'subdir'
        subdir.mkdir()
        
        # Start recursive monitoring
        service.start_monitoring(
            str(temp_directory),
            auto_process=True,
            recursive=True
        )
        
        # Create file in subdirectory
        test_file = subdir / 'nested_data.csv'
        test_file.write_text('subject_id,snr\nsub-002,15.0\n')
        
        # Wait for processing
        time.sleep(0.5)
        
        # Should have processed nested file
        mock_batch_service.submit_single_file_processing.assert_called()
        
        # Cleanup
        service.stop_monitoring(str(temp_directory))
    
    def test_multiple_file_extensions(self, temp_directory, mock_batch_service):
        """Test monitoring multiple file extensions."""
        service = FileMonitorService()
        
        # Start monitoring for CSV and TSV files
        service.start_monitoring(
            str(temp_directory),
            auto_process=True,
            file_extensions=['.csv', '.tsv']
        )
        
        # Create CSV file
        csv_file = temp_directory / 'data.csv'
        csv_file.write_text('subject_id,snr\nsub-001,12.5\n')
        
        # Create TSV file
        tsv_file = temp_directory / 'data.tsv'
        tsv_file.write_text('subject_id\tsnr\nsub-002\t15.0\n')
        
        # Create TXT file (should be ignored)
        txt_file = temp_directory / 'data.txt'
        txt_file.write_text('some text')
        
        # Wait for processing
        time.sleep(0.5)
        
        # Should have processed CSV and TSV files, but not TXT
        assert mock_batch_service.submit_single_file_processing.call_count == 2
        
        # Cleanup
        service.stop_monitoring(str(temp_directory))
    
    def test_monitoring_with_file_errors(self, temp_directory, mock_batch_service):
        """Test monitoring behavior with file processing errors."""
        # Mock batch service to raise error
        mock_batch_service.submit_single_file_processing.side_effect = Exception("Processing failed")
        
        service = FileMonitorService()
        
        # Start monitoring
        service.start_monitoring(
            str(temp_directory),
            auto_process=True
        )
        
        # Create file
        test_file = temp_directory / 'error_data.csv'
        test_file.write_text('subject_id,snr\nsub-001,12.5\n')
        
        # Wait for processing attempt
        time.sleep(0.5)
        
        # Should have attempted processing despite error
        mock_batch_service.submit_single_file_processing.assert_called()
        
        # Cleanup
        service.stop_monitoring(str(temp_directory))


class TestFileMonitoringReliability:
    """Test file monitoring reliability and error handling."""
    
    def test_observer_failure_recovery(self, temp_directory):
        """Test recovery from observer failures."""
        service = FileMonitorService()
        
        with patch('app.file_monitor.Observer') as mock_observer_class:
            mock_observer = Mock()
            mock_observer.start.side_effect = Exception("Observer failed to start")
            mock_observer_class.return_value = mock_observer
            
            # Should handle observer failure gracefully
            success = service.start_monitoring(str(temp_directory))
            
            assert success is False
    
    def test_file_system_permission_errors(self, temp_directory):
        """Test handling of file system permission errors."""
        handler = MRIQCFileHandler(auto_process=False)
        
        # Create file and make it unreadable
        test_file = temp_directory / 'unreadable.csv'
        test_file.write_text('subject_id,snr\nsub-001,12.5\n')
        test_file.chmod(0o000)  # Remove all permissions
        
        try:
            # Should handle permission error gracefully
            is_mriqc = handler._is_likely_mriqc_file(test_file)
            assert is_mriqc is True  # Should assume it's valid if can't check
        finally:
            # Restore permissions for cleanup
            test_file.chmod(0o644)
    
    def test_large_file_handling(self, temp_directory):
        """Test handling of large files."""
        handler = MRIQCFileHandler(
            auto_process=False,
            min_file_size=1024 * 1024  # 1MB minimum
        )
        
        # Create large file
        large_file = temp_directory / 'large.csv'
        with open(large_file, 'w') as f:
            f.write('subject_id,snr\n')
            # Write enough data to exceed minimum size
            for i in range(100000):
                f.write(f'sub-{i:06d},{i % 100}\n')
        
        # Simulate file creation event
        event = FileCreatedEvent(str(large_file))
        handler.on_created(event)
        
        # Large file should be added to pending files
        assert str(large_file) in handler.pending_files
    
    def test_concurrent_file_events(self, temp_directory):
        """Test handling of concurrent file events."""
        handler = MRIQCFileHandler(auto_process=False, stabilization_time=0.1)
        
        # Create multiple files simultaneously
        files = []
        for i in range(10):
            test_file = temp_directory / f'concurrent_{i}.csv'
            test_file.write_text(f'subject_id,snr\nsub-{i:03d},{i}\n')
            files.append(test_file)
        
        # Simulate concurrent events
        for test_file in files:
            event = FileCreatedEvent(str(test_file))
            handler.on_created(event)
        
        # All files should be pending
        assert len(handler.pending_files) == 10
        
        # Wait for stabilization
        time.sleep(0.2)
        
        # All files should be processed
        assert len(handler.processed_files) == 10


@patch('app.file_monitor.PROJECT_ROOT')
def test_setup_default_monitoring(mock_project_root, temp_directory):
    """Test setup of default monitoring directories."""
    mock_project_root.return_value = temp_directory
    
    with patch('app.file_monitor.file_monitor') as mock_monitor:
        setup_default_monitoring()
        
        # Should have called start_monitoring for default directories
        assert mock_monitor.start_monitoring.call_count >= 1