"""
Example demonstrating the comprehensive error handling and logging system.

This script shows how to use the error handling system, audit logging,
and structured error responses in the Age-Normed MRIQC Dashboard.
"""

import asyncio
import tempfile
from pathlib import Path
from datetime import datetime
from fastapi import Request
from unittest.mock import Mock

# Import error handling components
from app.error_handling import (
    ErrorHandler, AuditLogger, LoggingConfig, setup_logging,
    ErrorSeverity, ErrorCategory
)
from app.exceptions import (
    ValidationException, FileProcessingException, MRIQCProcessingException,
    NormalizationException, QualityAssessmentException, BatchProcessingException
)
from app.models import ProcessedSubject, SubjectInfo, MRIQCMetrics, QualityAssessment, QualityStatus


def setup_example_logging():
    """Setup logging for the example."""
    print("Setting up logging configuration...")
    
    # Create temporary log directory
    temp_dir = Path(tempfile.mkdtemp())
    log_dir = temp_dir / "example_logs"
    
    # Setup logging
    logging_config = LoggingConfig(log_dir=log_dir)
    
    print(f"Logs will be written to: {log_dir}")
    print("Log files created:")
    for log_file in log_dir.glob("*.log"):
        print(f"  - {log_file.name}")
    
    return log_dir


def demonstrate_error_handler():
    """Demonstrate the ErrorHandler functionality."""
    print("\n" + "="*60)
    print("DEMONSTRATING ERROR HANDLER")
    print("="*60)
    
    error_handler = ErrorHandler()
    
    # 1. Basic error response creation
    print("\n1. Creating basic error response:")
    error_response = error_handler.create_error_response(
        error_type="VALIDATION_FAILED",
        message="Age value is invalid",
        category=ErrorCategory.VALIDATION,
        severity=ErrorSeverity.MEDIUM,
        details={"field": "age", "invalid_value": -5},
        request_id="req-123"
    )
    
    print(f"   Error ID: {error_response.error_id}")
    print(f"   Error Code: {error_response.error_code}")
    print(f"   Message: {error_response.message}")
    print(f"   Severity: {error_response.severity}")
    print(f"   Suggestions: {error_response.suggestions[:2]}...")  # Show first 2
    print(f"   Recovery Options: {error_response.recovery_options[:2]}...")
    
    # 2. Validation error handling
    print("\n2. Handling validation error:")
    validation_error = error_handler.handle_validation_error(
        field="snr",
        message="SNR value out of valid range",
        invalid_value=1000.5,
        expected_type="float (0-100)"
    )
    
    print(f"   Error Type: {validation_error.error_type}")
    print(f"   Field: {validation_error.details['field']}")
    print(f"   Invalid Value: {validation_error.details['invalid_value']}")
    
    # 3. Processing error handling
    print("\n3. Handling processing error with exception:")
    try:
        # Simulate a processing error
        raise ValueError("Failed to parse MRIQC file")
    except Exception as e:
        processing_error = error_handler.handle_processing_error(
            operation="mriqc_file_parsing",
            message="MRIQC file parsing failed",
            exception=e,
            context={"file_name": "subject_001.csv", "line_number": 42}
        )
        
        print(f"   Error Type: {processing_error.error_type}")
        print(f"   Operation: {processing_error.details['operation']}")
        print(f"   Exception Type: {processing_error.details['exception_type']}")
        print(f"   Context: {processing_error.context}")
    
    # 4. System error handling
    print("\n4. Handling system error:")
    try:
        raise ConnectionError("Database connection failed")
    except Exception as e:
        system_error = error_handler.handle_system_error(
            component="database",
            message="Failed to connect to normative database",
            exception=e
        )
        
        print(f"   Error Type: {system_error.error_type}")
        print(f"   Component: {system_error.details['component']}")
        print(f"   Severity: {system_error.severity}")


def demonstrate_audit_logger():
    """Demonstrate the AuditLogger functionality."""
    print("\n" + "="*60)
    print("DEMONSTRATING AUDIT LOGGER")
    print("="*60)
    
    audit_logger = AuditLogger()
    
    # 1. Log quality control decision
    print("\n1. Logging quality control decision:")
    audit_logger.log_quality_decision(
        subject_id="sub-001",
        decision="pass",
        reason="All metrics within age-appropriate thresholds",
        user_id="researcher_123",
        automated=True,
        confidence=0.92,
        metrics={
            "snr": 12.5,
            "cnr": 3.2,
            "fber": 1500.0,
            "efc": 0.45
        },
        thresholds={
            "snr": {"warning": 10.0, "fail": 8.0},
            "cnr": {"warning": 2.5, "fail": 2.0}
        }
    )
    print("   ✓ Quality control decision logged")
    
    # 2. Log user action
    print("\n2. Logging user action:")
    mock_request = Mock()
    mock_request.client.host = "192.168.1.100"
    mock_request.headers = {
        "user-agent": "Mozilla/5.0 (Research Browser)",
        "x-session-id": "session_abc123"
    }
    
    audit_logger.log_user_action(
        action_type="file_upload",
        resource_type="mriqc_file",
        resource_id="file_456",
        user_id="researcher_123",
        old_values=None,
        new_values={
            "filename": "batch_001.csv",
            "size": 2048576,
            "subjects_count": 150
        },
        reason="Uploading new batch of MRIQC data",
        request=mock_request
    )
    print("   ✓ User action logged")
    
    # 3. Log data access
    print("\n3. Logging data access:")
    audit_logger.log_data_access(
        resource_type="subject_data",
        resource_id="sub-001",
        access_type="read",
        user_id="researcher_123",
        request=mock_request
    )
    print("   ✓ Data access logged")
    
    # 4. Log configuration change
    print("\n4. Logging configuration change:")
    old_config = {
        "age_groups": {
            "young_adult": {"min_age": 18, "max_age": 35}
        },
        "quality_thresholds": {
            "snr": {"warning": 10.0, "fail": 8.0}
        }
    }
    
    new_config = {
        "age_groups": {
            "young_adult": {"min_age": 18, "max_age": 40}  # Extended range
        },
        "quality_thresholds": {
            "snr": {"warning": 12.0, "fail": 10.0}  # Stricter thresholds
        }
    }
    
    audit_logger.log_configuration_change(
        config_type="study_configuration",
        old_config=old_config,
        new_config=new_config,
        user_id="admin_456",
        reason="Updated thresholds based on new normative data"
    )
    print("   ✓ Configuration change logged")


def demonstrate_custom_exceptions():
    """Demonstrate custom exception classes."""
    print("\n" + "="*60)
    print("DEMONSTRATING CUSTOM EXCEPTIONS")
    print("="*60)
    
    # 1. ValidationException
    print("\n1. ValidationException:")
    try:
        raise ValidationException(
            message="Invalid age value provided",
            field="age",
            invalid_value=-5.5,
            expected_type="positive float"
        )
    except ValidationException as e:
        print(f"   Message: {e.message}")
        print(f"   Category: {e.category}")
        print(f"   Error Code: {e.error_code}")
        print(f"   Field: {e.details['field']}")
        print(f"   Suggestions: {e.suggestions[0]}")
    
    # 2. MRIQCProcessingException
    print("\n2. MRIQCProcessingException:")
    try:
        raise MRIQCProcessingException(
            message="MRIQC file missing required columns",
            file_path="/data/uploads/batch_001.csv",
            missing_columns=["snr", "cnr", "fber"],
            invalid_metrics=["qi1"]
        )
    except MRIQCProcessingException as e:
        print(f"   Message: {e.message}")
        print(f"   Missing Columns: {e.details['missing_columns']}")
        print(f"   Invalid Metrics: {e.details['invalid_metrics']}")
        print(f"   Recovery Options: {e.recovery_options[0]}")
    
    # 3. NormalizationException
    print("\n3. NormalizationException:")
    try:
        raise NormalizationException(
            message="Failed to normalize metrics for age group",
            age=25.5,
            age_group="young_adult",
            metric_name="snr"
        )
    except NormalizationException as e:
        print(f"   Message: {e.message}")
        print(f"   Age: {e.details['age']}")
        print(f"   Age Group: {e.details['age_group']}")
        print(f"   Metric: {e.details['metric_name']}")
    
    # 4. BatchProcessingException
    print("\n4. BatchProcessingException:")
    try:
        raise BatchProcessingException(
            message="Batch processing failed for multiple files",
            batch_id="batch_789",
            failed_files=["file1.csv", "file2.csv", "file3.csv"]
        )
    except BatchProcessingException as e:
        print(f"   Message: {e.message}")
        print(f"   Batch ID: {e.details['batch_id']}")
        print(f"   Failed Files: {e.details['failed_files']}")
        print(f"   Failed Count: {e.details['failed_count']}")


def demonstrate_error_scenarios():
    """Demonstrate real-world error scenarios."""
    print("\n" + "="*60)
    print("DEMONSTRATING REAL-WORLD ERROR SCENARIOS")
    print("="*60)
    
    error_handler = ErrorHandler()
    audit_logger = AuditLogger()
    
    # Scenario 1: File upload with validation errors
    print("\n1. File Upload Validation Error Scenario:")
    print("   Simulating upload of invalid MRIQC file...")
    
    try:
        # Simulate file validation
        missing_columns = ["snr", "cnr", "fber"]
        if missing_columns:
            raise MRIQCProcessingException(
                message="MRIQC file validation failed",
                file_path="uploads/invalid_file.csv",
                missing_columns=missing_columns
            )
    except MRIQCProcessingException as e:
        error_response = error_handler.create_error_response(
            error_type="INVALID_FILE_FORMAT",
            message=e.message,
            details=e.details,
            suggestions=e.suggestions
        )
        
        print(f"   ✗ Error: {error_response.message}")
        print(f"   ✓ Error ID: {error_response.error_id}")
        print(f"   ✓ Suggestions provided: {len(error_response.suggestions)}")
        print(f"   ✓ Recovery options: {len(error_response.recovery_options)}")
    
    # Scenario 2: Quality assessment with normalization failure
    print("\n2. Quality Assessment with Normalization Failure:")
    print("   Simulating quality assessment with age normalization issues...")
    
    subject_id = "sub-002"
    try:
        # Simulate normalization failure
        raise NormalizationException(
            message="No normative data available for age group",
            age=95.5,
            age_group="elderly_extreme",
            metric_name="snr"
        )
    except NormalizationException as e:
        # Log the issue but continue with assessment
        audit_logger.log_quality_decision(
            subject_id=subject_id,
            decision="uncertain",
            reason=f"Normalization failed: {e.message}",
            automated=True,
            confidence=0.5,
            metrics={"snr": 8.5, "cnr": 2.1}
        )
        
        print(f"   ⚠ Warning: {e.message}")
        print(f"   ✓ Logged as 'uncertain' quality decision")
        print(f"   ✓ Graceful degradation applied")
    
    # Scenario 3: Batch processing with partial failures
    print("\n3. Batch Processing with Partial Failures:")
    print("   Simulating batch processing with some file failures...")
    
    batch_id = "batch_456"
    total_files = 10
    failed_files = ["corrupt_file1.csv", "invalid_file2.csv"]
    successful_files = total_files - len(failed_files)
    
    try:
        if failed_files:
            raise BatchProcessingException(
                message=f"Batch processing completed with {len(failed_files)} failures",
                batch_id=batch_id,
                failed_files=failed_files
            )
    except BatchProcessingException as e:
        # Log batch completion with partial success
        audit_logger.log_user_action(
            action_type="batch_processing_partial_success",
            resource_type="batch",
            resource_id=batch_id,
            new_values={
                "total_files": total_files,
                "successful_files": successful_files,
                "failed_files": len(failed_files),
                "success_rate": successful_files / total_files
            }
        )
        
        print(f"   ⚠ Partial Success: {successful_files}/{total_files} files processed")
        print(f"   ✓ Failed files logged for review")
        print(f"   ✓ Success rate: {successful_files/total_files:.1%}")


def demonstrate_error_recovery():
    """Demonstrate error recovery strategies."""
    print("\n" + "="*60)
    print("DEMONSTRATING ERROR RECOVERY STRATEGIES")
    print("="*60)
    
    error_handler = ErrorHandler()
    
    # Recovery Strategy 1: Fallback to default values
    print("\n1. Fallback to Default Values:")
    print("   Simulating age normalization failure with fallback...")
    
    def normalize_with_fallback(age, metric_value):
        try:
            if age > 100:  # Simulate edge case
                raise NormalizationException(
                    message="Age outside normative data range",
                    age=age,
                    metric_name="snr"
                )
            return {"percentile": 75.0, "z_score": 0.67}
        except NormalizationException as e:
            print(f"   ⚠ Normalization failed: {e.message}")
            print("   ✓ Falling back to adult normative data")
            # Fallback to adult norms
            return {"percentile": 50.0, "z_score": 0.0, "fallback": True}
    
    result = normalize_with_fallback(105, 12.5)
    print(f"   Result: {result}")
    
    # Recovery Strategy 2: Retry with exponential backoff
    print("\n2. Retry with Exponential Backoff:")
    print("   Simulating database connection with retry logic...")
    
    import time
    import random
    
    def connect_with_retry(max_retries=3):
        for attempt in range(max_retries):
            try:
                # Simulate connection failure
                if random.random() < 0.7:  # 70% failure rate
                    raise ConnectionError("Database connection failed")
                print("   ✓ Database connection successful")
                return True
            except ConnectionError as e:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"   ⚠ Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    print(f"   ⏳ Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print("   ✗ All retry attempts exhausted")
                    return False
    
    connect_with_retry()
    
    # Recovery Strategy 3: Graceful degradation
    print("\n3. Graceful Degradation:")
    print("   Simulating service degradation with reduced functionality...")
    
    def process_with_degradation(enable_advanced_features=True):
        try:
            if not enable_advanced_features:
                raise Exception("Advanced processing service unavailable")
            
            print("   ✓ Full processing with advanced features")
            return {"quality_score": 85.0, "advanced_metrics": True}
        except Exception as e:
            print(f"   ⚠ Advanced features unavailable: {e}")
            print("   ✓ Continuing with basic processing")
            return {"quality_score": 75.0, "advanced_metrics": False}
    
    result = process_with_degradation(enable_advanced_features=False)
    print(f"   Result: {result}")


def main():
    """Main example function."""
    print("Age-Normed MRIQC Dashboard - Error Handling System Example")
    print("=" * 80)
    
    # Setup logging
    log_dir = setup_example_logging()
    
    try:
        # Demonstrate different components
        demonstrate_error_handler()
        demonstrate_audit_logger()
        demonstrate_custom_exceptions()
        demonstrate_error_scenarios()
        demonstrate_error_recovery()
        
        print("\n" + "="*80)
        print("EXAMPLE COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"\nCheck the log files in: {log_dir}")
        print("Log files contain structured JSON entries for:")
        print("  - Application logs (application.log)")
        print("  - Error logs (errors.log)")
        print("  - Audit logs (audit.log)")
        print("  - Quality control logs (quality_control.log)")
        
    except Exception as e:
        print(f"\n✗ Example failed with error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup logging handlers
        import logging
        for logger_name in ['', 'audit', 'quality_control']:
            logger = logging.getLogger(logger_name)
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)


if __name__ == "__main__":
    main()