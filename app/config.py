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

# Security settings
SECURITY_ENABLED = os.getenv("SECURITY_ENABLED", "true").lower() == "true"
VIRUS_SCAN_ENABLED = os.getenv("VIRUS_SCAN_ENABLED", "true").lower() == "true"
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "30"))
CLEANUP_INTERVAL_HOURS = int(os.getenv("CLEANUP_INTERVAL_HOURS", "24"))
ENABLE_AUDIT_LOGGING = os.getenv("ENABLE_AUDIT_LOGGING", "true").lower() == "true"
MAX_FILENAME_LENGTH = int(os.getenv("MAX_FILENAME_LENGTH", "255"))
ALLOWED_MIME_TYPES = set(os.getenv("ALLOWED_MIME_TYPES", "text/csv,application/csv").split(","))
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "3600"))  # 1 hour

# Batch processing limits
MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "1000"))  # Maximum files per batch
MAX_CONCURRENT_BATCHES = int(os.getenv("MAX_CONCURRENT_BATCHES", "5"))
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", "1800"))  # 30 minutes
TASK_SOFT_TIME_LIMIT = int(os.getenv("TASK_SOFT_TIME_LIMIT", "1500"))  # 25 minutes

# Performance optimization settings
ENABLE_CACHING = os.getenv("ENABLE_CACHING", "true").lower() == "true"
ENABLE_CONNECTION_POOLING = os.getenv("ENABLE_CONNECTION_POOLING", "true").lower() == "true"
CONNECTION_POOL_SIZE = int(os.getenv("CONNECTION_POOL_SIZE", "10"))
CONNECTION_POOL_MAX_IDLE_TIME = int(os.getenv("CONNECTION_POOL_MAX_IDLE_TIME", "300"))  # 5 minutes
BATCH_CHUNK_SIZE = int(os.getenv("BATCH_CHUNK_SIZE", "100"))
BATCH_MAX_WORKERS = int(os.getenv("BATCH_MAX_WORKERS", "4"))
BATCH_USE_MULTIPROCESSING = os.getenv("BATCH_USE_MULTIPROCESSING", "true").lower() == "true"
BATCH_MEMORY_LIMIT_MB = int(os.getenv("BATCH_MEMORY_LIMIT_MB", "1024"))

# Cache TTL settings (in seconds)
CACHE_TTL_NORMATIVE_DATA = int(os.getenv("CACHE_TTL_NORMATIVE_DATA", "86400"))  # 24 hours
CACHE_TTL_AGE_GROUPS = int(os.getenv("CACHE_TTL_AGE_GROUPS", "86400"))  # 24 hours
CACHE_TTL_QUALITY_THRESHOLDS = int(os.getenv("CACHE_TTL_QUALITY_THRESHOLDS", "86400"))  # 24 hours
CACHE_TTL_NORMALIZED_METRICS = int(os.getenv("CACHE_TTL_NORMALIZED_METRICS", "3600"))  # 1 hour
CACHE_TTL_QUALITY_ASSESSMENT = int(os.getenv("CACHE_TTL_QUALITY_ASSESSMENT", "3600"))  # 1 hour
CACHE_TTL_BATCH_STATUS = int(os.getenv("CACHE_TTL_BATCH_STATUS", "7200"))  # 2 hours

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(exist_ok=True)
WATCH_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)
