"""
File monitoring service for automatic MRIQC file processing.

This module provides real-time file system monitoring capabilities
to automatically process new MRIQC files as they are added to watched directories.
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
from threading import Thread, Event

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent

from .batch_service import batch_service
from .config import PROJECT_ROOT

logger = logging.getLogger(__name__)


class MRIQCFileHandler(FileSystemEventHandler):
    """File system event handler for MRIQC files."""
    
    def __init__(self, 
                 auto_process: bool = True,
                 file_extensions: Optional[List[str]] = None,
                 min_file_size: int = 1024,  # Minimum file size in bytes
                 stabilization_time: float = 2.0):  # Time to wait for file to stabilize
        """
        Initialize MRIQC file handler.
        
        Args:
            auto_process: Whether to automatically process detected files
            file_extensions: List of file extensions to monitor (default: ['.csv'])
            min_file_size: Minimum file size to consider for processing
            stabilization_time: Time to wait for file to stabilize before processing
        """
        super().__init__()
        self.auto_process = auto_process
        self.file_extensions = file_extensions or ['.csv']
        self.min_file_size = min_file_size
        self.stabilization_time = stabilization_time
        self.pending_files: Dict[str, float] = {}  # file_path -> timestamp
        self.processed_files: Set[str] = set()
        
        # Start background thread for processing pending files
        self._stop_event = Event()
        self._processing_thread = Thread(target=self._process_pending_files, daemon=True)
        self._processing_thread.start()
    
    def on_created(self, event):
        """Handle file creation events."""
        if not event.is_directory:
            self._handle_file_event(event.src_path, 'created')
    
    def on_modified(self, event):
        """Handle file modification events."""
        if not event.is_directory:
            self._handle_file_event(event.src_path, 'modified')
    
    def _handle_file_event(self, file_path: str, event_type: str):
        """Handle file system events for potential MRIQC files."""
        try:
            path = Path(file_path)
            
            # Check if file has monitored extension
            if path.suffix.lower() not in self.file_extensions:
                return
            
            # Check if file meets minimum size requirement
            if path.exists() and path.stat().st_size < self.min_file_size:
                return
            
            # Add to pending files for stabilization
            self.pending_files[file_path] = time.time()
            
            logger.debug(f"Detected {event_type} event for potential MRIQC file: {file_path}")
            
        except Exception as e:
            logger.error(f"Error handling file event for {file_path}: {str(e)}")
    
    def _process_pending_files(self):
        """Background thread to process pending files after stabilization."""
        while not self._stop_event.is_set():
            try:
                current_time = time.time()
                files_to_process = []
                
                # Check for stabilized files
                for file_path, timestamp in list(self.pending_files.items()):
                    if current_time - timestamp >= self.stabilization_time:
                        files_to_process.append(file_path)
                        del self.pending_files[file_path]
                
                # Process stabilized files
                for file_path in files_to_process:
                    if file_path not in self.processed_files:
                        self._process_file(file_path)
                        self.processed_files.add(file_path)
                
                # Sleep before next check
                time.sleep(1.0)
                
            except Exception as e:
                logger.error(f"Error in pending files processing thread: {str(e)}")
                time.sleep(5.0)  # Wait longer on error
    
    def _process_file(self, file_path: str):
        """Process a detected MRIQC file."""
        try:
            path = Path(file_path)
            
            # Verify file still exists and is readable
            if not path.exists() or not path.is_file():
                logger.warning(f"File no longer exists or is not a file: {file_path}")
                return
            
            # Check if file appears to be a valid MRIQC file (basic check)
            if not self._is_likely_mriqc_file(path):
                logger.debug(f"File does not appear to be an MRIQC file: {file_path}")
                return
            
            if self.auto_process:
                # Submit for automatic processing
                task_id = batch_service.submit_single_file_processing(
                    file_path,
                    apply_quality_assessment=True
                )
                
                logger.info(f"Submitted automatic processing for {file_path} (task: {task_id})")
            else:
                logger.info(f"Detected MRIQC file (auto-processing disabled): {file_path}")
                
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
    
    def _is_likely_mriqc_file(self, file_path: Path) -> bool:
        """Check if file is likely an MRIQC output file."""
        try:
            # Read first few lines to check for MRIQC-like headers
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip().lower()
                
                # Check for common MRIQC column headers
                mriqc_indicators = [
                    'bids_name', 'subject_id', 'session_id',
                    'snr', 'cnr', 'fber', 'efc', 'fwhm',
                    'dvars', 'fd_mean', 'gcor'
                ]
                
                return any(indicator in first_line for indicator in mriqc_indicators)
                
        except Exception as e:
            logger.debug(f"Could not check MRIQC file format for {file_path}: {str(e)}")
            return True  # Assume it's valid if we can't check
    
    def stop(self):
        """Stop the file handler and background processing."""
        self._stop_event.set()
        if self._processing_thread.is_alive():
            self._processing_thread.join(timeout=5.0)


class FileMonitorService:
    """Service for monitoring directories for new MRIQC files."""
    
    def __init__(self):
        self.observers: Dict[str, Observer] = {}
        self.handlers: Dict[str, MRIQCFileHandler] = {}
    
    def start_monitoring(self, 
                        directory_path: str,
                        auto_process: bool = True,
                        recursive: bool = False,
                        file_extensions: Optional[List[str]] = None) -> bool:
        """
        Start monitoring a directory for MRIQC files.
        
        Args:
            directory_path: Directory to monitor
            auto_process: Whether to automatically process detected files
            recursive: Whether to monitor subdirectories
            file_extensions: List of file extensions to monitor
            
        Returns:
            True if monitoring started successfully
        """
        try:
            directory = Path(directory_path)
            
            # Create directory if it doesn't exist
            if not directory.exists():
                directory.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created monitoring directory: {directory_path}")
            
            # Check if already monitoring this directory
            if directory_path in self.observers:
                logger.warning(f"Already monitoring directory: {directory_path}")
                return True
            
            # Create file handler
            handler = MRIQCFileHandler(
                auto_process=auto_process,
                file_extensions=file_extensions
            )
            
            # Create observer
            observer = Observer()
            observer.schedule(handler, directory_path, recursive=recursive)
            
            # Start monitoring
            observer.start()
            
            # Store references
            self.observers[directory_path] = observer
            self.handlers[directory_path] = handler
            
            logger.info(f"Started monitoring directory: {directory_path} (auto_process={auto_process}, recursive={recursive})")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start monitoring {directory_path}: {str(e)}")
            return False
    
    def stop_monitoring(self, directory_path: str) -> bool:
        """
        Stop monitoring a directory.
        
        Args:
            directory_path: Directory to stop monitoring
            
        Returns:
            True if monitoring stopped successfully
        """
        try:
            if directory_path not in self.observers:
                logger.warning(f"Not monitoring directory: {directory_path}")
                return True
            
            # Stop observer
            observer = self.observers[directory_path]
            observer.stop()
            observer.join(timeout=5.0)
            
            # Stop handler
            handler = self.handlers[directory_path]
            handler.stop()
            
            # Remove references
            del self.observers[directory_path]
            del self.handlers[directory_path]
            
            logger.info(f"Stopped monitoring directory: {directory_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop monitoring {directory_path}: {str(e)}")
            return False
    
    def stop_all_monitoring(self):
        """Stop monitoring all directories."""
        directories = list(self.observers.keys())
        for directory in directories:
            self.stop_monitoring(directory)
    
    def get_monitored_directories(self) -> List[Dict]:
        """
        Get list of currently monitored directories.
        
        Returns:
            List of monitoring information
        """
        monitored = []
        
        for directory_path, observer in self.observers.items():
            handler = self.handlers.get(directory_path)
            
            monitored.append({
                'directory': directory_path,
                'is_alive': observer.is_alive(),
                'auto_process': handler.auto_process if handler else False,
                'file_extensions': handler.file_extensions if handler else [],
                'pending_files_count': len(handler.pending_files) if handler else 0,
                'processed_files_count': len(handler.processed_files) if handler else 0
            })
        
        return monitored
    
    def get_monitoring_status(self, directory_path: str) -> Optional[Dict]:
        """
        Get monitoring status for a specific directory.
        
        Args:
            directory_path: Directory to check
            
        Returns:
            Monitoring status information or None if not monitored
        """
        if directory_path not in self.observers:
            return None
        
        observer = self.observers[directory_path]
        handler = self.handlers[directory_path]
        
        return {
            'directory': directory_path,
            'is_alive': observer.is_alive(),
            'auto_process': handler.auto_process,
            'file_extensions': handler.file_extensions,
            'pending_files': list(handler.pending_files.keys()),
            'processed_files_count': len(handler.processed_files),
            'min_file_size': handler.min_file_size,
            'stabilization_time': handler.stabilization_time
        }


# Global file monitor service instance
file_monitor = FileMonitorService()


def setup_default_monitoring():
    """Set up default directory monitoring."""
    try:
        # Monitor default upload directory
        upload_dir = PROJECT_ROOT / 'data' / 'uploads'
        file_monitor.start_monitoring(
            str(upload_dir),
            auto_process=True,
            recursive=False
        )
        
        # Monitor watch directory if it exists
        watch_dir = PROJECT_ROOT / 'data' / 'watch'
        if watch_dir.exists():
            file_monitor.start_monitoring(
                str(watch_dir),
                auto_process=True,
                recursive=True
            )
        
        logger.info("Default directory monitoring set up successfully")
        
    except Exception as e:
        logger.error(f"Failed to set up default monitoring: {str(e)}")


# Set up default monitoring on module import
# setup_default_monitoring()