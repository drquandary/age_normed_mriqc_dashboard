# Error Handling and Logging Implementation

This document describes the comprehensive error handling and logging system implemented for the Age-Normed MRIQC Dashboard.

## Overview

The error handling system provides:

- **Structured Error Responses**: Consistent error format with unique IDs, error codes, and recovery suggestions
- **Comprehensive Logging**: Multi-level logging with JSON formatting for structured log analysis
- **Audit Trail**: Complete audit logging for quality control decisions and user actions
- **Custom Exceptions**: Specific exception types for different error scenarios
- **Error Recovery**: Built-in suggestions and recovery options for common errors

## Components

### 1. Error Handler (`app/error_handling.py`)

The `ErrorHandler` class provides centralized error handling with:

- **Error Response Creation**: Structured error responses with unique IDs and error codes
- **Error Classification**: Categorization by type (validation, processing, system, etc.)
- **Severity Levels**: LOW, MEDIUM, HIGH, CRITICAL severity classification
- **Automatic Suggestions**: Context-aware suggestions and recovery options
- **Comprehensive Logging**: Automatic logging of all errors with structured data

#### Usage Example:

```python
from app.error_handling import error_handler, ErrorCategory, ErrorSeverity

# Create structured error response
error_response = error_handler.create_error_response(
    error_type="VALIDATION_FAILED",
    message="Invalid age value",
    category=ErrorCategory.VALIDATION,
    severity=ErrorSeverity.MEDIUM,
    details={"field": "age", "invalid_value": -5},
    request_id="req-123"
)

# Handle specific error types
validation_error = error_handler.handle_validation_error(
    field="snr",
    message="SNR value out of range",
    invalid_value=1000.5,
    expected_type="float (0-100)"
)
```

### 2. Audit Logger (`app/error_handling.py`)

The `AuditLogger` class provides comprehensive audit logging:

- **Quality Control Decisions**: Log all automated and manual QC decisions
- **User Actions**: Track all user interactions with the system
- **Data Access**: Log data access for compliance and security
- **Configuration Changes**: Track system configuration modifications

#### Usage Example:

```python
from app.error_handling import audit_logger

# Log quality control decision
audit_logger.log_quality_decision(
    subject_id="sub-001",
    decision="pass",
    reason="All metrics within age-appropriate thresholds",
    automated=True,
    confidence=0.92,
    metrics={"snr": 12.5, "cnr": 3.2},
    thresholds={"snr": {"warning": 10.0, "fail": 8.0}}
)

# Log user action
audit_logger.log_user_action(
    action_type="file_upload",
    resource_type="mriqc_file",
    resource_id="file_456",
    user_id="researcher_123",
    new_values={"filename": "batch_001.csv", "size": 2048576},
    request=request
)
```

### 3. Custom Exceptions (`app/exceptions.py`)

Specialized exception classes for different error scenarios:

- **ValidationException**: Data validation errors
- **FileProcessingException**: File processing errors
- **MRIQCProcessingException**: MRIQC-specific processing errors
- **NormalizationException**: Age normalization errors
- **QualityAssessmentException**: Quality assessment errors
- **BatchProcessingException**: Batch processing errors
- **DatabaseException**: Database-related errors
- **ConfigurationException**: Configuration errors
- **SecurityException**: Security-related errors

#### Usage Example:

```python
from app.exceptions import ValidationException, MRIQCProcessingException

# Raise validation exception
raise ValidationException(
    message="Invalid age value",
    field="age",
    invalid_value=-5,
    expected_type="positive float"
)

# Raise MRIQC processing exception
raise MRIQCProcessingException(
    message="Missing required columns",
    file_path="/data/file.csv",
    missing_columns=["snr", "cnr"]
)
```

### 4. Logging Configuration (`app/error_handling.py`)

The `LoggingConfig` class sets up comprehensive logging:

- **Multiple Log Files**: Separate files for application, errors, audit, and QC logs
- **JSON Formatting**: Structured JSON logs for easy parsing and analysis
- **Multiple Handlers**: Console, file, and specialized handlers
- **Configurable Levels**: Different log levels for different components

#### Log Files Created:

- `logs/application.log`: General application logs
- `logs/errors.log`: Error-specific logs in JSON format
- `logs/audit.log`: Audit trail logs in JSON format
- `logs/quality_control.log`: Quality control decision logs

### 5. Error Handler Middleware (`app/error_handling.py`)

FastAPI middleware for automatic error handling:

- **Request Tracking**: Unique request IDs for tracing
- **Automatic Error Handling**: Catches and formats all unhandled exceptions
- **Structured Responses**: Converts exceptions to structured error responses
- **Request/Response Logging**: Logs all requests and responses

## Error Codes

The system uses a structured error code system:

- **1000-1999**: Validation errors
- **2000-2999**: Processing errors
- **3000-3999**: System errors
- **4000-4999**: Security errors
- **5000-5999**: Business logic errors
- **6000-6999**: External service errors

### Common Error Codes:

- `1001`: VALIDATION_FAILED
- `1002`: INVALID_FILE_FORMAT
- `2001`: PROCESSING_FAILED
- `2002`: FILE_PROCESSING_ERROR
- `3001`: DATABASE_ERROR
- `4001`: AUTHENTICATION_FAILED
- `5001`: THRESHOLD_VIOLATION

## Integration with Application

### 1. Main Application Integration

The error handling system is integrated into the main FastAPI application:

```python
# app/main.py
from .error_handling import setup_logging, error_handler_middleware

# Setup logging
setup_logging()

app = FastAPI(title="Age-Normed MRIQC Dashboard")

# Add error handling middleware
app.middleware("http")(error_handler_middleware)
```

### 2. API Endpoint Integration

API endpoints use the error handling system:

```python
# app/routes.py
from .error_handling import error_handler, audit_logger, get_request_id

@router.post('/upload')
async def upload_file(request: Request, file: UploadFile = File(...)):
    request_id = get_request_id(request)
    
    # Log upload attempt
    audit_logger.log_user_action(
        action_type="file_upload_attempt",
        resource_type="mriqc_file",
        resource_id=file.filename,
        request=request
    )
    
    try:
        # Process file...
        pass
    except Exception as e:
        error_response = error_handler.handle_processing_error(
            operation="file_upload",
            message="File upload failed",
            exception=e,
            request_id=request_id
        )
        raise HTTPException(status_code=500, detail=error_response.message)
```

### 3. Background Task Integration

Background tasks include error handling and audit logging:

```python
async def process_subjects_background(subjects, batch_id):
    # Log batch start
    audit_logger.log_user_action(
        action_type="batch_processing_start",
        resource_type="batch",
        resource_id=batch_id
    )
    
    for subject in subjects:
        try:
            # Process subject...
            
            # Log quality decision
            audit_logger.log_quality_decision(
                subject_id=subject.subject_info.subject_id,
                decision=quality_assessment.overall_status.value,
                reason="Automated assessment",
                automated=True,
                confidence=quality_assessment.confidence
            )
        except Exception as e:
            error_response = error_handler.handle_processing_error(
                operation="subject_processing",
                message=f"Failed to process subject {subject.subject_info.subject_id}",
                exception=e
            )
            # Handle error appropriately...
```

## Error Recovery Strategies

The system implements several error recovery strategies:

### 1. Fallback to Default Values

When age normalization fails, the system falls back to adult normative data:

```python
try:
    normalized_metrics = age_normalizer.normalize_metrics(metrics, age)
except NormalizationException:
    # Fallback to adult norms
    normalized_metrics = age_normalizer.normalize_metrics(metrics, 30.0)
    normalized_metrics.fallback = True
```

### 2. Graceful Degradation

When optional services fail, the system continues with reduced functionality:

```python
try:
    advanced_assessment = advanced_quality_assessor.assess(metrics)
except Exception:
    # Continue with basic assessment
    basic_assessment = basic_quality_assessor.assess(metrics)
    basic_assessment.advanced_features_unavailable = True
```

### 3. Retry with Exponential Backoff

For transient errors, the system implements retry logic:

```python
import time
import random

def retry_with_backoff(func, max_retries=3):
    for attempt in range(max_retries):
        try:
            return func()
        except TransientException as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait_time)
            else:
                raise
```

## Monitoring and Alerting

The error handling system supports monitoring and alerting:

### 1. Error Rate Tracking

Track error rates by category and severity:

```python
# Error metrics can be extracted from logs
error_rate = errors_count / total_requests
critical_errors = [e for e in errors if e.severity == ErrorSeverity.CRITICAL]
```

### 2. Log Analysis

Structured JSON logs enable easy analysis:

```bash
# Count errors by type
jq '.error_type' logs/errors.log | sort | uniq -c

# Find high-severity errors
jq 'select(.severity == "high")' logs/errors.log

# Analyze quality control decisions
jq 'select(.action_type == "quality_decision")' logs/quality_control.log
```

### 3. Alerting Integration

The system can integrate with alerting systems:

```python
# Example: Send alert for critical errors
if error_response.severity == ErrorSeverity.CRITICAL:
    alert_service.send_alert(
        title="Critical Error in MRIQC Dashboard",
        message=error_response.message,
        error_id=error_response.error_id
    )
```

## Testing

The error handling system includes comprehensive tests:

- **Unit Tests**: Test individual components (`tests/test_error_handling.py`)
- **Integration Tests**: Test system integration (`tests/test_error_handling_integration.py`)
- **Error Scenario Tests**: Test real-world error scenarios
- **Recovery Tests**: Test error recovery mechanisms

### Running Tests:

```bash
# Run error handling tests
python -m pytest tests/test_error_handling.py -v

# Run integration tests
python -m pytest tests/test_error_handling_integration.py -v
```

## Example Usage

See `examples/error_handling_example.py` for a comprehensive example demonstrating:

- Error handler usage
- Audit logging
- Custom exceptions
- Real-world error scenarios
- Error recovery strategies

### Running the Example:

```bash
python examples/error_handling_example.py
```

## Best Practices

1. **Always Use Structured Errors**: Use the error handler for consistent error responses
2. **Log Important Actions**: Use audit logging for all significant user actions
3. **Provide Recovery Options**: Include helpful suggestions and recovery options
4. **Use Appropriate Severity**: Classify errors with appropriate severity levels
5. **Include Context**: Provide relevant context information in error details
6. **Test Error Scenarios**: Include error scenarios in your tests
7. **Monitor Error Rates**: Track and monitor error rates and patterns
8. **Document Error Codes**: Maintain documentation for all error codes

## Configuration

The error handling system can be configured through environment variables:

```bash
# Logging configuration
LOG_LEVEL=INFO
LOG_DIR=/var/log/mriqc_dashboard

# Error handling configuration
ERROR_TRACKING_ENABLED=true
AUDIT_LOGGING_ENABLED=true
```

## Security Considerations

The error handling system includes security features:

- **No Sensitive Data in Logs**: Ensures no sensitive data is logged
- **Request Tracking**: Tracks requests for security analysis
- **Access Logging**: Logs all data access for compliance
- **Error Rate Limiting**: Can be integrated with rate limiting systems

## Performance Considerations

The system is designed for performance:

- **Asynchronous Logging**: Non-blocking log operations
- **Efficient JSON Serialization**: Fast JSON formatting
- **Minimal Overhead**: Low-impact error handling
- **Configurable Log Levels**: Adjustable verbosity

## Compliance

The audit logging system supports compliance requirements:

- **Complete Audit Trail**: All actions are logged
- **Immutable Logs**: Log entries cannot be modified
- **Data Access Tracking**: All data access is logged
- **Quality Control Documentation**: All QC decisions are documented

This comprehensive error handling and logging system ensures robust, maintainable, and compliant operation of the Age-Normed MRIQC Dashboard.