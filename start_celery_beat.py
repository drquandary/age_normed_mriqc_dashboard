#!/usr/bin/env python3
"""
Celery beat scheduler startup script.

This script starts the Celery beat scheduler for periodic tasks
like directory monitoring and cleanup.
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
    
    # Start Celery beat scheduler
    celery_app.worker_main([
        'beat',
        '--loglevel=INFO',
        '--schedule=/tmp/celerybeat-schedule',
        '--pidfile=/tmp/celerybeat.pid',
    ])