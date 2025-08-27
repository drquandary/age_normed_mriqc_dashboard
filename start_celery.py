#!/usr/bin/env python3
"""
Celery worker startup script for batch processing.

This script starts the Celery worker with appropriate configuration
for handling MRIQC batch processing tasks.
"""

import os
import sys
from pathlib import Path

# Add the app directory to Python path
app_dir = Path(__file__).parent / 'app'
sys.path.insert(0, str(app_dir))

from app.celery_app import celery_app

if __name__ == '__main__':
    # Set default log level if not specified
    if 'CELERY_LOG_LEVEL' not in os.environ:
        os.environ['CELERY_LOG_LEVEL'] = 'INFO'
    
    # Start Celery worker
    celery_app.worker_main([
        'worker',
        '--loglevel=INFO',
        '--concurrency=4',
        '--queues=batch_processing,file_processing,file_monitoring',
        '--hostname=mriqc-worker@%h',
        '--max-tasks-per-child=1000',
        '--time-limit=1800',  # 30 minutes
        '--soft-time-limit=1500',  # 25 minutes
    ])