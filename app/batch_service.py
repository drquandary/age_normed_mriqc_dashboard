"""
Batch processing service for managing asynchronous operations.

This module provides a high-level interface for batch processing operations,
integrating Celery tasks with the FastAPI application.
"""

import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import redis
from celery.result import AsyncResult

from .batch_tasks import process_batch_files, process_single_file_task, monitor_directory
from .celery_app import celery_app
from .models import ProcessedSubject, ProcessingError, QualityStatus

logger = logging.getLogger(__name__)

# Redis client for batch status management
redis_client = redis.Redis.from_url('redis://localhost:6379/0')


class BatchProcessingService:
    """Service for managing batch processing operations."""
    
    def __init__(self):
        self.redis_client = redis_client
    
    def submit_batch_processing(
        self,
        file_paths: List[str],
        apply_quality_assessment: bool = True,
        custom_thresholds: Optional[Dict] = None
    ) -> Tuple[str, str]:
        """
        Submit batch processing job.
        
        Args:
            file_paths: List of file paths to process
            apply_quality_assessment: Whether to apply quality assessment
            custom_thresholds: Custom quality thresholds
            
        Returns:
            Tuple of (batch_id, task_id)
        """
        batch_id = f"batch_{uuid.uuid4().hex[:8]}_{int(datetime.now().timestamp())}"
        
        # Submit Celery task
        task = process_batch_files.delay(
            file_paths,
            batch_id,
            apply_quality_assessment,
            custom_thresholds
        )
        
        # Store task mapping
        task_key = f"batch_task:{batch_id}"
        self.redis_client.setex(
            task_key,
            3600,  # Expire after 1 hour
            task.id
        )
        
        logger.info(f"Submitted batch processing job {batch_id} with task {task.id}")
        
        return batch_id, task.id
    
    def submit_single_file_processing(
        self,
        file_path: str,
        apply_quality_assessment: bool = True,
        custom_thresholds: Optional[Dict] = None
    ) -> str:
        """
        Submit single file processing job.
        
        Args:
            file_path: Path to file to process
            apply_quality_assessment: Whether to apply quality assessment
            custom_thresholds: Custom quality thresholds
            
        Returns:
            Task ID
        """
        task = process_single_file_task.delay(
            file_path,
            apply_quality_assessment,
            custom_thresholds
        )
        
        logger.info(f"Submitted single file processing job {task.id} for {file_path}")
        
        return task.id
    
    def get_batch_status(self, batch_id: str) -> Optional[Dict]:
        """
        Get batch processing status.
        
        Args:
            batch_id: Batch identifier
            
        Returns:
            Batch status information or None if not found
        """
        # Get progress information
        progress_key = f"batch_progress:{batch_id}"
        progress_data = self.redis_client.get(progress_key)
        
        if not progress_data:
            return None
        
        try:
            progress_info = json.loads(progress_data)
            
            # Get task information if available
            task_key = f"batch_task:{batch_id}"
            task_id = self.redis_client.get(task_key)
            
            if task_id:
                task_id = task_id.decode()
                task_result = AsyncResult(task_id, app=celery_app)
                
                progress_info.update({
                    'task_id': task_id,
                    'task_state': task_result.state,
                    'task_info': task_result.info if task_result.info else {}
                })
            
            return progress_info
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode progress data for batch {batch_id}: {str(e)}")
            return None
    
    def get_batch_results(self, batch_id: str) -> Optional[Dict]:
        """
        Get batch processing results.
        
        Args:
            batch_id: Batch identifier
            
        Returns:
            Batch results or None if not found
        """
        results_key = f"batch_results:{batch_id}"
        results_data = self.redis_client.get(results_key)
        
        if not results_data:
            return None
        
        try:
            return json.loads(results_data)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode results data for batch {batch_id}: {str(e)}")
            return None
    
    def get_processed_subjects(self, batch_id: str) -> List[ProcessedSubject]:
        """
        Get processed subjects from batch results.
        
        Args:
            batch_id: Batch identifier
            
        Returns:
            List of processed subjects
        """
        results = self.get_batch_results(batch_id)
        if not results or 'subjects' not in results:
            return []
        
        try:
            subjects = []
            for subject_data in results['subjects']:
                subject = ProcessedSubject.model_validate(subject_data)
                subjects.append(subject)
            return subjects
        except Exception as e:
            logger.error(f"Failed to parse subjects for batch {batch_id}: {str(e)}")
            return []
    
    def cancel_batch_processing(self, batch_id: str) -> bool:
        """
        Cancel batch processing job.
        
        Args:
            batch_id: Batch identifier
            
        Returns:
            True if cancellation was successful
        """
        try:
            # Get task ID
            task_key = f"batch_task:{batch_id}"
            task_id = self.redis_client.get(task_key)
            
            if not task_id:
                return False
            
            task_id = task_id.decode()
            
            # Revoke task
            celery_app.control.revoke(task_id, terminate=True)
            
            # Update status
            progress_key = f"batch_progress:{batch_id}"
            progress_data = self.redis_client.get(progress_key)
            
            if progress_data:
                progress_info = json.loads(progress_data)
                progress_info.update({
                    'status': 'cancelled',
                    'cancelled_at': datetime.now().isoformat()
                })
                
                self.redis_client.setex(
                    progress_key,
                    3600,
                    json.dumps(progress_info)
                )
            
            logger.info(f"Cancelled batch processing job {batch_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cancel batch {batch_id}: {str(e)}")
            return False
    
    def get_task_status(self, task_id: str) -> Dict:
        """
        Get Celery task status.
        
        Args:
            task_id: Celery task ID
            
        Returns:
            Task status information
        """
        try:
            task_result = AsyncResult(task_id, app=celery_app)
            
            return {
                'task_id': task_id,
                'state': task_result.state,
                'info': task_result.info if task_result.info else {},
                'successful': task_result.successful(),
                'failed': task_result.failed(),
                'ready': task_result.ready()
            }
            
        except Exception as e:
            logger.error(f"Failed to get task status for {task_id}: {str(e)}")
            return {
                'task_id': task_id,
                'state': 'UNKNOWN',
                'error': str(e)
            }
    
    def start_directory_monitoring(self, directory_path: str) -> str:
        """
        Start monitoring directory for new files.
        
        Args:
            directory_path: Directory to monitor
            
        Returns:
            Task ID for monitoring task
        """
        task = monitor_directory.delay(directory_path)
        
        # Store monitoring task info
        monitor_key = f"monitor_task:{directory_path}"
        self.redis_client.setex(
            monitor_key,
            86400,  # Expire after 24 hours
            json.dumps({
                'task_id': task.id,
                'directory': directory_path,
                'started_at': datetime.now().isoformat()
            })
        )
        
        logger.info(f"Started directory monitoring for {directory_path} with task {task.id}")
        
        return task.id
    
    def stop_directory_monitoring(self, directory_path: str) -> bool:
        """
        Stop monitoring directory.
        
        Args:
            directory_path: Directory being monitored
            
        Returns:
            True if monitoring was stopped successfully
        """
        try:
            monitor_key = f"monitor_task:{directory_path}"
            monitor_data = self.redis_client.get(monitor_key)
            
            if not monitor_data:
                return False
            
            monitor_info = json.loads(monitor_data)
            task_id = monitor_info.get('task_id')
            
            if task_id:
                celery_app.control.revoke(task_id, terminate=True)
            
            # Remove monitoring info
            self.redis_client.delete(monitor_key)
            
            logger.info(f"Stopped directory monitoring for {directory_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop monitoring for {directory_path}: {str(e)}")
            return False
    
    def get_active_batches(self) -> List[Dict]:
        """
        Get list of active batch processing jobs.
        
        Returns:
            List of active batch information
        """
        try:
            progress_keys = self.redis_client.keys('batch_progress:*')
            active_batches = []
            
            for key in progress_keys:
                try:
                    data = self.redis_client.get(key)
                    if data:
                        batch_info = json.loads(data)
                        if batch_info.get('status') in ['pending', 'processing']:
                            active_batches.append(batch_info)
                except Exception as e:
                    logger.warning(f"Failed to process batch key {key}: {str(e)}")
            
            return active_batches
            
        except Exception as e:
            logger.error(f"Failed to get active batches: {str(e)}")
            return []
    
    def get_worker_status(self) -> Dict:
        """
        Get Celery worker status information.
        
        Returns:
            Worker status information
        """
        try:
            # Get active workers
            inspect = celery_app.control.inspect()
            
            active_workers = inspect.active()
            registered_tasks = inspect.registered()
            stats = inspect.stats()
            
            return {
                'active_workers': active_workers or {},
                'registered_tasks': registered_tasks or {},
                'worker_stats': stats or {},
                'broker_url': celery_app.conf.broker_url,
                'result_backend': celery_app.conf.result_backend
            }
            
        except Exception as e:
            logger.error(f"Failed to get worker status: {str(e)}")
            return {
                'error': str(e),
                'broker_url': celery_app.conf.broker_url,
                'result_backend': celery_app.conf.result_backend
            }


# Global service instance
batch_service = BatchProcessingService()