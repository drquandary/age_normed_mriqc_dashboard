# Batch Processing and Automation Features

This document describes the batch processing and automation capabilities of the Age-Normed MRIQC Dashboard, including asynchronous processing, file monitoring, and progress tracking.

## Overview

The batch processing system provides:

- **Asynchronous Processing**: Handle multiple MRIQC files concurrently using Celery
- **Progress Tracking**: Real-time status updates and progress monitoring
- **File Monitoring**: Automatic detection and processing of new files
- **Error Handling**: Robust error recovery and detailed error reporting
- **Scalability**: Support for large datasets and concurrent operations

## Architecture

### Components

1. **Celery Tasks** (`batch_tasks.py`): Asynchronous task definitions
2. **Batch Service** (`batch_service.py`): High-level batch processing interface
3. **File Monitor** (`file_monitor.py`): Automatic file detection and processing
4. **Progress Tracker**: Real-time progress monitoring with Redis backend

### Technology Stack

- **Celery**: Distributed task queue for asynchronous processing
- **Redis**: Message broker and result backend
- **Watchdog**: File system monitoring
- **FastAPI**: REST API endpoints for batch operations

## Setup and Configuration

### Prerequisites

1. **Redis Server**: Install and start Redis
   ```bash
   # Ubuntu/Debian
   sudo apt-get install redis-server
   sudo systemctl start redis-server
   
   # macOS with Homebrew
   brew install redis
   brew services start redis
   ```

2. **Python Dependencies**: Install required packages
   ```bash
   pip install -r requirements.txt
   ```

### Environment Variables

Configure the following environment variables:

```bash
# Redis Configuration
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0
REDIS_URL=redis://localhost:6379/0

# File Processing Settings
MAX_FILE_SIZE=52428800  # 50MB
FILE_STABILIZATION_TIME=2.0  # seconds
MAX_BATCH_SIZE=1000
MAX_CONCURRENT_BATCHES=5

# Task Timeouts
BATCH_TIMEOUT=1800  # 30 minutes
TASK_SOFT_TIME_LIMIT=1500  # 25 minutes
```

### Starting Services

1. **Start Celery Worker**:
   ```bash
   python start_celery.py
   ```

2. **Start Celery Beat Scheduler** (optional, for periodic tasks):
   ```bash
   python start_celery_beat.py
   ```

3. **Start FastAPI Application**:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```

## Usage

### Batch Processing API

#### Submit Batch Processing Job

```http
POST /api/batch/submit
Content-Type: application/json

{
  "file_paths": [
    "/path/to/mriqc_file1.csv",
    "/path/to/mriqc_file2.csv"
  ],
  "apply_quality_assessment": true,
  "custom_thresholds": {
    "snr": {
      "warning_threshold": 10.0,
      "fail_threshold": 8.0,
      "direction": "higher_better"
    }
  }
}
```

Response:
```json
{
  "batch_id": "batch_abc123_1640995200",
  "task_id": "celery-task-uuid",
  "message": "Batch processing submitted for 2 files",
  "total_files": 2
}
```

#### Get Batch Status

```http
GET /api/batch/{batch_id}/status
```

Response:
```json
{
  "batch_id": "batch_abc123_1640995200",
  "status": "processing",
  "progress": {
    "completed": 5,
    "total": 10,
    "progress_percent": 50.0
  },
  "total_items": 10,
  "completed_items": 5,
  "failed_items": 0,
  "errors": [],
  "start_time": "2024-01-15T10:30:00",
  "last_update": "2024-01-15T10:35:00"
}
```

#### Get Batch Results

```http
GET /api/batch/{batch_id}/results
```

#### Cancel Batch Processing

```http
DELETE /api/batch/{batch_id}
```

### File Monitoring API

#### Start Directory Monitoring

```http
POST /api/monitoring/start
Content-Type: application/json

{
  "directory_path": "/path/to/watch/directory",
  "auto_process": true,
  "recursive": false,
  "file_extensions": [".csv"]
}
```

#### Stop Directory Monitoring

```http
DELETE /api/monitoring/{directory_path}
```

#### Get Monitoring Status

```http
GET /api/monitoring/status
```

### Python API

#### Batch Processing Service

```python
from app.batch_service import batch_service

# Submit batch processing
batch_id, task_id = batch_service.submit_batch_processing(
    file_paths=['file1.csv', 'file2.csv'],
    apply_quality_assessment=True
)

# Monitor progress
status = batch_service.get_batch_status(batch_id)
print(f"Status: {status['status']}")
print(f"Progress: {status['completed_items']}/{status['total_items']}")

# Get results
results = batch_service.get_batch_results(batch_id)
subjects = batch_service.get_processed_subjects(batch_id)
```

#### File Monitoring Service

```python
from app.file_monitor import file_monitor

# Start monitoring
file_monitor.start_monitoring(
    '/path/to/directory',
    auto_process=True,
    recursive=True,
    file_extensions=['.csv', '.tsv']
)

# Check status
status = file_monitor.get_monitoring_status('/path/to/directory')
print(f"Monitoring: {status['is_alive']}")
print(f"Processed files: {status['processed_files_count']}")

# Stop monitoring
file_monitor.stop_monitoring('/path/to/directory')
```

## Features

### Asynchronous Batch Processing

- **Concurrent Processing**: Multiple files processed simultaneously
- **Progress Tracking**: Real-time progress updates via WebSocket
- **Error Recovery**: Continue processing remaining files if some fail
- **Custom Thresholds**: Apply study-specific quality thresholds

### File Monitoring

- **Automatic Detection**: Monitor directories for new MRIQC files
- **File Stabilization**: Wait for files to finish writing before processing
- **Extension Filtering**: Monitor specific file types (CSV, TSV, etc.)
- **Recursive Monitoring**: Watch subdirectories for new files
- **Duplicate Prevention**: Avoid reprocessing the same files

### Progress Tracking

- **Real-time Updates**: Live progress information via Redis
- **Detailed Status**: Track completed, failed, and pending items
- **Error Reporting**: Detailed error messages and suggestions
- **Time Tracking**: Start time, completion time, and duration

### Quality Assessment Integration

- **Age-Appropriate Thresholds**: Apply age-specific quality criteria
- **Custom Thresholds**: Override default thresholds per batch
- **Comprehensive Assessment**: Full quality evaluation pipeline
- **Normalization**: Age-normalized percentiles and z-scores

## Error Handling

### Common Error Types

1. **File Processing Errors**
   - Invalid MRIQC file format
   - Missing required columns
   - Corrupted data

2. **System Errors**
   - Redis connection failures
   - Celery worker unavailable
   - File system permissions

3. **Resource Errors**
   - Memory limitations
   - Disk space issues
   - Network timeouts

### Error Recovery

- **Graceful Degradation**: Continue processing other files when one fails
- **Retry Logic**: Automatic retry for transient failures
- **Error Logging**: Detailed error logs with stack traces
- **User Notification**: Clear error messages with suggested solutions

## Performance Optimization

### Batch Size Optimization

- **Small Batches**: Better progress tracking, faster feedback
- **Large Batches**: Better throughput, reduced overhead
- **Recommended**: 50-100 files per batch for optimal balance

### Memory Management

- **Streaming Processing**: Process files one at a time to limit memory usage
- **Result Caching**: Cache results in Redis with expiration
- **Cleanup**: Automatic cleanup of old results and temporary files

### Concurrency Settings

```python
# Celery worker configuration
CELERY_WORKER_CONCURRENCY = 4  # Number of concurrent processes
CELERY_WORKER_PREFETCH_MULTIPLIER = 1  # Tasks per worker
CELERY_TASK_TIME_LIMIT = 1800  # 30 minutes
CELERY_TASK_SOFT_TIME_LIMIT = 1500  # 25 minutes
```

## Monitoring and Debugging

### Worker Status

```http
GET /api/batch/worker-status
```

Returns information about active Celery workers, registered tasks, and worker statistics.

### Active Batches

```http
GET /api/batch/active
```

Lists all currently running batch processing jobs.

### Task Status

```http
GET /api/tasks/{task_id}/status
```

Get detailed status for a specific Celery task.

### Logs

- **Application Logs**: FastAPI application logs
- **Celery Logs**: Worker and task execution logs
- **Redis Logs**: Message broker logs

### Health Checks

```python
from app.batch_tasks import health_check

# Test worker connectivity
result = health_check.delay()
status = result.get(timeout=10)
print(f"Worker health: {status}")
```

## Best Practices

### File Organization

- **Separate Directories**: Use different directories for input, processing, and output
- **Naming Conventions**: Use consistent file naming patterns
- **Backup Strategy**: Keep backups of original MRIQC files

### Batch Processing

- **Validate Files**: Check file format before submitting batches
- **Monitor Progress**: Use WebSocket connections for real-time updates
- **Handle Errors**: Implement proper error handling in client applications
- **Resource Planning**: Consider system resources when sizing batches

### File Monitoring

- **Stable Writes**: Ensure files are completely written before processing
- **Permission Management**: Set appropriate file system permissions
- **Network Shares**: Be cautious with network-mounted directories
- **Cleanup**: Regularly clean up processed files to save disk space

## Troubleshooting

### Common Issues

1. **Redis Connection Failed**
   ```bash
   # Check Redis status
   redis-cli ping
   
   # Restart Redis
   sudo systemctl restart redis-server
   ```

2. **Celery Worker Not Starting**
   ```bash
   # Check for port conflicts
   netstat -tulpn | grep 6379
   
   # Start worker with debug logging
   celery -A app.celery_app worker --loglevel=DEBUG
   ```

3. **File Monitoring Not Working**
   - Check directory permissions
   - Verify file extensions configuration
   - Monitor system resources (inotify limits on Linux)

4. **Batch Processing Stuck**
   - Check worker status
   - Review task logs
   - Verify Redis connectivity
   - Check file accessibility

### Performance Issues

1. **Slow Processing**
   - Increase worker concurrency
   - Optimize file I/O operations
   - Check system resources (CPU, memory, disk)

2. **Memory Usage**
   - Reduce batch sizes
   - Enable result compression
   - Implement streaming processing

3. **Network Issues**
   - Use local Redis instance
   - Optimize Redis configuration
   - Monitor network latency

## Examples

See `examples/batch_processing_example.py` for comprehensive usage examples including:

- Batch processing workflow
- File monitoring setup
- Custom threshold configuration
- Error handling patterns
- Performance optimization techniques

## API Reference

For complete API documentation, see the FastAPI interactive docs at `/docs` when the application is running.