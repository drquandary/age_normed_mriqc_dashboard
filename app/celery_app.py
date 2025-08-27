"""
Celery application configuration for asynchronous batch processing.

This module sets up Celery for handling long-running batch processing tasks
with Redis as the message broker and result backend.
"""

import os
from celery import Celery
from .config import PROJECT_ROOT

# Celery configuration
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')

# Create Celery app
celery_app = Celery(
    'age_normed_mriqc_dashboard',
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=['age_normed_mriqc_dashboard.app.batch_tasks']
)

# Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    result_expires=3600,  # 1 hour
    task_routes={
        'age_normed_mriqc_dashboard.app.batch_tasks.process_batch_files': {'queue': 'batch_processing'},
        'age_normed_mriqc_dashboard.app.batch_tasks.process_single_file_task': {'queue': 'file_processing'},
        'age_normed_mriqc_dashboard.app.batch_tasks.monitor_directory': {'queue': 'file_monitoring'},
    },
    task_annotations={
        'age_normed_mriqc_dashboard.app.batch_tasks.process_batch_files': {
            'rate_limit': '10/m'
        },
        'age_normed_mriqc_dashboard.app.batch_tasks.process_single_file_task': {
            'rate_limit': '50/m'
        },
    }
)

# Optional: Configure beat schedule for periodic tasks
celery_app.conf.beat_schedule = {
    'monitor-upload-directory': {
        'task': 'age_normed_mriqc_dashboard.app.batch_tasks.monitor_directory',
        'schedule': 30.0,  # Run every 30 seconds
        'args': (str(PROJECT_ROOT / 'data' / 'uploads'),)
    },
    'cleanup-old-results': {
        'task': 'age_normed_mriqc_dashboard.app.batch_tasks.cleanup_old_results',
        'schedule': 3600.0,  # Run every hour
    },
}

if __name__ == '__main__':
    celery_app.start()