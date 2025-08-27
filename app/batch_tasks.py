"""
Celery tasks for batch processing and automation.

This module contains asynchronous tasks for processing MRIQC files,
monitoring directories, and managing batch operations.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import redis
from celery import current_task
from celery.exceptions import Retry

from .celery_app import celery_app
from .models import ProcessedSubject, ProcessingError, QualityStatus
from .mriqc_processor import MRIQCProcessor, MRIQCProcessingError, MRIQCValidationError
from .quality_assessor import QualityAssessor
from .age_normalizer import AgeNormalizer
from .config import PROJECT_ROOT

logger = logging.getLogger(__name__)

# Redis client for storing batch status and results
redis_client = redis.Redis.from_url(os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0'))

# Global instances
mriqc_processor = MRIQCProcessor()
quality_assessor = QualityAssessor()
age_normalizer = AgeNormalizer()


class BatchProgressTracker:
    """Tracks progress of batch processing operations."""
    
    def __init__(self, batch_id: str, total_items: int):
        self.batch_id = batch_id
        self.total_items = total_items
        self.completed_items = 0
        self.failed_items = 0
        self.errors = []
        self.start_time = datetime.now()
        self.redis_key = f"batch_progress:{batch_id}"
        
        # Initialize progress in Redis
        self._update_progress()
    
    def _update_progress(self):
        """Update progress information in Redis."""
        progress_data = {
            'batch_id': self.batch_id,
            'total_items': self.total_items,
            'completed_items': self.completed_items,
            'failed_items': self.failed_items,
            'progress_percent': (self.completed_items / self.total_items * 100) if self.total_items > 0 else 0,
            'errors': [error.model_dump() if hasattr(error, 'model_dump') else error for error in self.errors],
            'start_time': self.start_time.isoformat(),
            'last_update': datetime.now().isoformat(),
            'status': self._get_status()
        }
        
        redis_client.setex(
            self.redis_key,
            3600,  # Expire after 1 hour
            json.dumps(progress_data)
        )
    
    def _get_status(self) -> str:
        """Get current batch status."""
        if self.completed_items + self.failed_items == 0:
            return 'pending'
        elif self.completed_items + self.failed_items < self.total_items:
            return 'processing'
        elif self.failed_items == self.total_items:
            return 'failed'
        else:
            return 'completed'
    
    def increment_completed(self):
        """Increment completed items counter."""
        self.completed_items += 1
        self._update_progress()
    
    def increment_failed(self, error: Optional[ProcessingError] = None):
        """Increment failed items counter and add error."""
        self.failed_items += 1
        if error:
            self.errors.append(error)
        self._update_progress()
    
    def get_progress(self) -> Dict:
        """Get current progress information."""
        data = redis_client.get(self.redis_key)
        if data:
            return json.loads(data)
        return {}


@celery_app.task(bind=True, name='process_batch_files')
def process_batch_files(
    self,
    file_paths: List[str],
    batch_id: str,
    apply_quality_assessment: bool = True,
    custom_thresholds: Optional[Dict] = None
) -> Dict:
    """
    Process multiple MRIQC files in batch mode.
    
    Args:
        file_paths: List of file paths to process
        batch_id: Unique batch identifier
        apply_quality_assessment: Whether to apply quality assessment
        custom_thresholds: Custom quality thresholds
        
    Returns:
        Dict with batch processing results
    """
    logger.info(f"Starting batch processing for batch {batch_id} with {len(file_paths)} files")
    
    # Initialize progress tracker
    tracker = BatchProgressTracker(batch_id, len(file_paths))
    
    all_subjects = []
    processing_errors = []
    
    try:
        for i, file_path in enumerate(file_paths):
            try:
                # Update task progress
                current_task.update_state(
                    state='PROGRESS',
                    meta={
                        'current': i + 1,
                        'total': len(file_paths),
                        'status': f'Processing file {i + 1}/{len(file_paths)}: {Path(file_path).name}'
                    }
                )
                
                # Process single file
                subjects = process_single_file_sync(
                    file_path,
                    apply_quality_assessment,
                    custom_thresholds
                )
                
                all_subjects.extend(subjects)
                tracker.increment_completed()
                
                logger.info(f"Processed file {i + 1}/{len(file_paths)}: {len(subjects)} subjects")
                
            except Exception as e:
                error = ProcessingError(
                    error_type="file_processing_error",
                    message=f"Failed to process file {file_path}: {str(e)}",
                    error_code="BATCH_001",
                    details={"file_path": file_path}
                )
                processing_errors.append(error)
                tracker.increment_failed(error)
                
                logger.error(f"Failed to process file {file_path}: {str(e)}")
        
        # Store results in Redis
        results_key = f"batch_results:{batch_id}"
        results_data = {
            'subjects': [subject.model_dump() for subject in all_subjects],
            'processing_errors': [error.model_dump() for error in processing_errors],
            'total_subjects': len(all_subjects),
            'total_files': len(file_paths),
            'completed_at': datetime.now().isoformat()
        }
        
        redis_client.setex(
            results_key,
            7200,  # Expire after 2 hours
            json.dumps(results_data)
        )
        
        logger.info(f"Batch {batch_id} completed: {len(all_subjects)} subjects processed")
        
        return {
            'batch_id': batch_id,
            'status': 'completed',
            'total_subjects': len(all_subjects),
            'total_files': len(file_paths),
            'processing_errors': len(processing_errors),
            'completed_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Batch processing failed for {batch_id}: {str(e)}")
        
        error = ProcessingError(
            error_type="batch_processing_error",
            message=f"Batch processing failed: {str(e)}",
            error_code="BATCH_002"
        )
        
        return {
            'batch_id': batch_id,
            'status': 'failed',
            'error': error.model_dump(),
            'failed_at': datetime.now().isoformat()
        }


@celery_app.task(bind=True, name='process_single_file_task')
def process_single_file_task(
    self,
    file_path: str,
    apply_quality_assessment: bool = True,
    custom_thresholds: Optional[Dict] = None
) -> Dict:
    """
    Process a single MRIQC file asynchronously.
    
    Args:
        file_path: Path to MRIQC file
        apply_quality_assessment: Whether to apply quality assessment
        custom_thresholds: Custom quality thresholds
        
    Returns:
        Dict with processing results
    """
    try:
        current_task.update_state(
            state='PROGRESS',
            meta={'status': f'Processing file: {Path(file_path).name}'}
        )
        
        subjects = process_single_file_sync(
            file_path,
            apply_quality_assessment,
            custom_thresholds
        )
        
        return {
            'status': 'completed',
            'subjects_count': len(subjects),
            'subjects': [subject.model_dump() for subject in subjects],
            'completed_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to process file {file_path}: {str(e)}")
        
        return {
            'status': 'failed',
            'error': str(e),
            'failed_at': datetime.now().isoformat()
        }


def process_single_file_sync(
    file_path: str,
    apply_quality_assessment: bool = True,
    custom_thresholds: Optional[Dict] = None
) -> List[ProcessedSubject]:
    """
    Synchronously process a single MRIQC file.
    
    Args:
        file_path: Path to MRIQC file
        apply_quality_assessment: Whether to apply quality assessment
        custom_thresholds: Custom quality thresholds
        
    Returns:
        List of processed subjects
    """
    # Parse and validate file
    df = mriqc_processor.parse_mriqc_file(file_path)
    validation_errors = mriqc_processor.validate_mriqc_format(df, file_path)
    
    if validation_errors:
        error_messages = [error.message for error in validation_errors]
        raise MRIQCValidationError(f"Invalid MRIQC file: {'; '.join(error_messages)}")
    
    # Extract subjects
    subjects = mriqc_processor.extract_subjects_from_dataframe(df)
    
    # Apply quality assessment if requested
    if apply_quality_assessment:
        for subject in subjects:
            try:
                # Apply quality assessment
                quality_assessment = quality_assessor.assess_quality(
                    subject.raw_metrics,
                    subject.subject_info,
                    custom_thresholds=custom_thresholds
                )
                subject.quality_assessment = quality_assessment
                
                # Add normalized metrics if age is available
                if subject.subject_info.age is not None:
                    normalized_metrics = age_normalizer.normalize_metrics(
                        subject.raw_metrics,
                        subject.subject_info.age
                    )
                    subject.normalized_metrics = normalized_metrics
                    
            except Exception as e:
                logger.warning(f"Failed to assess quality for {subject.subject_info.subject_id}: {str(e)}")
                # Set default quality assessment
                from .models import QualityAssessment
                subject.quality_assessment = QualityAssessment(
                    overall_status=QualityStatus.UNCERTAIN,
                    metric_assessments={},
                    composite_score=0.0,
                    confidence=0.0,
                    recommendations=["Quality assessment failed - manual review required"]
                )
    
    return subjects


@celery_app.task(name='monitor_directory')
def monitor_directory(directory_path: str) -> Dict:
    """
    Monitor directory for new MRIQC files and process them automatically.
    
    Args:
        directory_path: Directory to monitor
        
    Returns:
        Dict with monitoring results
    """
    try:
        directory = Path(directory_path)
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            return {'status': 'created_directory', 'path': str(directory)}
        
        # Look for new CSV files
        csv_files = list(directory.glob('*.csv'))
        processed_files = []
        
        # Track processed files to avoid reprocessing
        processed_key = f"processed_files:{directory_path}"
        processed_set = redis_client.smembers(processed_key)
        processed_files_set = {f.decode() for f in processed_set}
        
        new_files = []
        for csv_file in csv_files:
            file_key = f"{csv_file.name}:{csv_file.stat().st_mtime}"
            if file_key not in processed_files_set:
                new_files.append(str(csv_file))
                # Mark as processed
                redis_client.sadd(processed_key, file_key)
                redis_client.expire(processed_key, 86400)  # Expire after 24 hours
        
        if new_files:
            # Start batch processing for new files
            batch_id = f"auto_{int(time.time())}"
            
            # Submit batch processing task
            process_batch_files.delay(
                new_files,
                batch_id,
                apply_quality_assessment=True
            )
            
            logger.info(f"Started automatic processing of {len(new_files)} new files in {directory_path}")
            
            return {
                'status': 'processing_started',
                'new_files_count': len(new_files),
                'batch_id': batch_id,
                'files': new_files
            }
        
        return {
            'status': 'no_new_files',
            'directory': str(directory),
            'total_files': len(csv_files)
        }
        
    except Exception as e:
        logger.error(f"Directory monitoring failed for {directory_path}: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'directory': directory_path
        }


@celery_app.task(name='cleanup_old_results')
def cleanup_old_results() -> Dict:
    """
    Clean up old batch results and progress data from Redis.
    
    Returns:
        Dict with cleanup results
    """
    try:
        # Clean up old batch progress data (older than 24 hours)
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        progress_keys = redis_client.keys('batch_progress:*')
        results_keys = redis_client.keys('batch_results:*')
        
        cleaned_progress = 0
        cleaned_results = 0
        
        for key in progress_keys:
            try:
                data = redis_client.get(key)
                if data:
                    progress_data = json.loads(data)
                    last_update = datetime.fromisoformat(progress_data.get('last_update', ''))
                    if last_update < cutoff_time:
                        redis_client.delete(key)
                        cleaned_progress += 1
            except Exception as e:
                logger.warning(f"Failed to process progress key {key}: {str(e)}")
        
        for key in results_keys:
            try:
                data = redis_client.get(key)
                if data:
                    results_data = json.loads(data)
                    completed_at = datetime.fromisoformat(results_data.get('completed_at', ''))
                    if completed_at < cutoff_time:
                        redis_client.delete(key)
                        cleaned_results += 1
            except Exception as e:
                logger.warning(f"Failed to process results key {key}: {str(e)}")
        
        logger.info(f"Cleanup completed: {cleaned_progress} progress entries, {cleaned_results} result entries")
        
        return {
            'status': 'completed',
            'cleaned_progress': cleaned_progress,
            'cleaned_results': cleaned_results,
            'cleanup_time': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        return {
            'status': 'error',
            'error': str(e)
        }


@celery_app.task(bind=True, name='health_check')
def health_check(self) -> Dict:
    """
    Health check task for monitoring Celery worker status.
    
    Returns:
        Dict with health status
    """
    return {
        'status': 'healthy',
        'worker_id': self.request.id,
        'timestamp': datetime.now().isoformat(),
        'redis_connection': redis_client.ping()
    }