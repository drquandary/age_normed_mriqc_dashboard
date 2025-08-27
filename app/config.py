"""Configuration for age_normed_mriqc_dashboard."""

import os
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
APP_DIR = PROJECT_ROOT / "app"

# Application settings
APP_NAME = "Age Normed Mriqc Dashboard"
DEBUG = os.getenv("DEBUG", "false").lower() == "true"
HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", "8000"))

# Data settings
SAMPLE_DATA_FILE = DATA_DIR / "sample_data.csv"

# Batch processing settings
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# File monitoring settings
UPLOAD_DIR = DATA_DIR / "uploads"
WATCH_DIR = DATA_DIR / "watch"
TEMP_DIR = DATA_DIR / "temp"
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", "52428800"))  # 50MB default
SUPPORTED_EXTENSIONS = ['.csv']
FILE_STABILIZATION_TIME = float(os.getenv("FILE_STABILIZATION_TIME", "2.0"))  # seconds

# Batch processing limits
MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "1000"))  # Maximum files per batch
MAX_CONCURRENT_BATCHES = int(os.getenv("MAX_CONCURRENT_BATCHES", "5"))
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", "1800"))  # 30 minutes
TASK_SOFT_TIME_LIMIT = int(os.getenv("TASK_SOFT_TIME_LIMIT", "1500"))  # 25 minutes

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(exist_ok=True)
WATCH_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)
