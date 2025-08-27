"""
Tests for error handling and logging system.

This module tests the comprehensive error handling, structured error responses,
and audit logging functionality.
"""

import pytest
import json
import tempfile
import logging
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from fastapi import Request, HTTPException
from fastapi.testclient import TestClient

from app.error_handling import (
    ErrorHandler, AuditLogger, LoggingConfig, JsonFormatter,
    ErrorResponse, AuditLogEntry, ErrorSeverity, ErrorCategory,
    error_handler_middleware, setup_logging
)
from app.exceptions import (
    ValidationException, FileProcessingException, MRIQCProcessingException,
    NormalizationException, QualityAssessmentException, DatabaseException,
    ConfigurationException, BatchProcessingException, ExportException,
    SecurityException, ExternalServiceException
)


class TestErrorHandler:
    """Test the ErrorHandler class."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.error_handler = ErrorHandler()
    
    def test_create_error_response_basic(self):
        """Test basic error response creation."""
        error_response = self.error_handler.create_error_response(
            error_type="TEST_ERROR",
            message="Test error message",
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.MEDIUM
        )
        
        assert error_response.error_type == "TEST_ERROR"
        assert error_response.message == "Test error message"
        assert error_response.error_category == ErrorCategory.VALIDATION
        assert error_response.severity == ErrorSeverity.MEDIUM
        assert error_response.error_code == "9999"  # Default for unknown error type
        assert isinstance(error_response.error_id, str)
        assert isinstance(error_response.timestamp, datetime)
    
    def test_create_error_response_with_known_error_code(self):
        """Test error response creation with known error code."""
        error_response = self.error_handler.create_error_response(
            error_type="VALIDATION_FAILED",
            message="Validation failed",
            category=ErrorCategory.VALIDATION
        )
        
        assert error_response.error_code == "1001"
        assert len(error_response.suggestions) > 0
        assert len(error_response.recovery_options) > 0
    
    def test_create_error_response_with_details(self):
        """Test error response creation with additional details."""
        details = {"field": "age", "invalid_value": -5}
        context = {"operation": "data_validation"}
        
        error_response = self.error_handler.create_error_response(
            error_type="VALIDATION_FAILED",
            message="Invalid age value",
            details=details,
            context=context,
            request_id="test-request-123"
        )
        
        assert error_response.details == details
        assert error_response.context == context
        assert error_response.request_id == "test-request-123"
    
    def test_handle_validation_error(self):
        """Test validation error handling."""
        error_response = self.error_handler.handle_validation_error(
            field="age",
            message="Age must be positive",
            invalid_value=-5,
            expected_type="positive float"
        )
        
        assert error_response.error_category == ErrorCategory.VALIDATION
        assert error_response.details["field"] == "age"
        assert error_response.details["invalid_value"] == -5
        assert error_response.details["expected_type"] == "positive float"
    
    def test_handle_processing_error(self):
        """Test processing error handling."""
        test_exception = ValueError("Test processing error")
        
        error_response = self.error_handler.handle_processing_error(
            operation="file_processing",
            message="Failed to process file",
            exception=test_exception,
            context={"file_name": "test.csv"}
        )
        
        assert error_response.error_category == ErrorCategory.PROCESSING
        assert error_response.severity == ErrorSeverity.HIGH
        assert error_response.details["operation"] == "file_processing"
        assert error_response.details["exception_type"] == "ValueError"
        assert error_response.context["file_name"] == "test.csv"
    
    def test_handle_system_error(self):
        """Test system error handling."""
        test_exception = ConnectionError("Database connection failed")
        
        error_response = self.error_handler.handle_system_error(
            component="database",
            message="Database connection error",
            exception=test_exception
        )
        
        assert error_response.error_category == ErrorCategory.SYSTEM
        assert error_response.severity == ErrorSeverity.CRITICAL
        assert error_response.details["component"] == "database"
        assert error_response.details["exception_type"] == "ConnectionError"


class TestAuditLogger:
    """Test the AuditLogger class."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.audit_logger = AuditLogger()
    
    @patch('app.error_handling.logging.getLogger')
    def test_log_quality_decision(self, mock_get_logger):
        """Test quality control decision logging."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        self.audit_logger.log_quality_decision(
            subject_id="sub-001",
            decision="pass",
            reason="All metrics within normal range",
            user_id="user123",
            automated=True,
            confidence=0.95,
            metrics={"snr": 12.5, "cnr": 3.2},
            thresholds={"snr": {"warning": 10.0, "fail": 8.0}}
        )
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert "Quality control decision made" in call_args[0][0]
        
        extra_data = call_args[1]['extra']
        assert extra_data['subject_id'] == "sub-001"
        assert extra_data['decision'] == "pass"
        assert extra_data['automated'] is True
        assert extra_data['confidence'] == 0.95
    
    @patch('app.error_handling.logging.getLogger')
    def test_log_user_action(self, mock_get_logger):
        """Test user action logging."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        # Mock request object
        mock_request = Mock()
        mock_request.client.host = "192.168.1.1"
        mock_request.headers = {"user-agent": "test-browser", "x-session-id": "session123"}
        
        self.audit_logger.log_user_action(
            action_type="file_upload",
            resource_type="mriqc_file",
            resource_id="file123",
            user_id="user456",
            old_values=None,
            new_values={"filename": "test.csv", "size": 1024},
            reason="User uploaded new file",
            request=mock_request
        )
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert "User action logged" in call_args[0][0]
        
        extra_data = call_args[1]['extra']
        assert extra_data['action_type'] == "file_upload"
        assert extra_data['resource_type'] == "mriqc_file"
        assert extra_data['ip_address'] == "192.168.1.1"
        assert extra_data['session_id'] == "session123"
    
    @patch('app.error_handling.logging.getLogger')
    def test_log_data_access(self, mock_get_logger):
        """Test data access logging."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        mock_request = Mock()
        mock_request.client.host = "10.0.0.1"
        mock_request.headers = {"user-agent": "api-client"}
        
        self.audit_logger.log_data_access(
            resource_type="subject_data",
            resource_id="sub-001",
            access_type="read",
            user_id="researcher123",
            request=mock_request
        )
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert "Data access logged" in call_args[0][0]
        
        extra_data = call_args[1]['extra']
        assert extra_data['resource_type'] == "subject_data"
        assert extra_data['access_type'] == "read"
        assert extra_data['ip_address'] == "10.0.0.1"
    
    @patch('app.error_handling.logging.getLogger')
    def test_log_configuration_change(self, mock_get_logger):
        """Test configuration change logging."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        old_config = {"threshold": 10.0, "enabled": True}
        new_config = {"threshold": 12.0, "enabled": False}
        
        self.audit_logger.log_configuration_change(
            config_type="quality_thresholds",
            old_config=old_config,
            new_config=new_config,
            user_id="admin123",
            reason="Updated thresholds based on new research"
        )
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert "Configuration changed" in call_args[0][0]
        
        extra_data = call_args[1]['extra']
        assert extra_data['resource_type'] == "configuration"
        assert extra_data['old_values'] == old_config
        assert extra_data['new_values'] == new_config


class TestJsonFormatter:
    """Test the JsonFormatter class."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.formatter = JsonFormatter()
    
    def test_format_basic_record(self):
        """Test basic log record formatting."""
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/test/path.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None
        )
        record.module = "test_module"
        record.funcName = "test_function"
        
        formatted = self.formatter.format(record)
        log_data = json.loads(formatted)
        
        assert log_data['level'] == 'INFO'
        assert log_data['logger'] == 'test_logger'
        assert log_data['module'] == 'test_module'
        assert log_data['function'] == 'test_function'
        assert log_data['line'] == 42
        assert log_data['message'] == 'Test message'
        assert 'timestamp' in log_data
    
    def test_format_record_with_exception(self):
        """Test log record formatting with exception."""
        try:
            raise ValueError("Test exception")
        except ValueError:
            import sys
            exc_info = sys.exc_info()
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="/test/path.py",
            lineno=42,
            msg="Error occurred",
            args=(),
            exc_info=exc_info
        )
        record.module = "test_module"
        record.funcName = "test_function"
        
        formatted = self.formatter.format(record)
        log_data = json.loads(formatted)
        
        assert 'exception' in log_data
        assert log_data['exception']['type'] == 'ValueError'
        assert log_data['exception']['message'] == 'Test exception'
        assert isinstance(log_data['exception']['traceback'], list)
    
    def test_format_record_with_extra_fields(self):
        """Test log record formatting with extra fields."""
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/test/path.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None
        )
        record.module = "test_module"
        record.funcName = "test_function"
        record.request_id = "req-123"
        record.user_id = "user-456"
        
        formatted = self.formatter.format(record)
        log_data = json.loads(formatted)
        
        assert log_data['request_id'] == 'req-123'
        assert log_data['user_id'] == 'user-456'


class TestLoggingConfig:
    """Test the LoggingConfig class."""
    
    def test_logging_config_initialization(self):
        """Test logging configuration initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir) / "logs"
            config = LoggingConfig(log_dir=log_dir)
            
            # Check that log directory was created
            assert log_dir.exists()
            
            # Check that loggers are configured
            root_logger = logging.getLogger()
            assert len(root_logger.handlers) > 0
            
            audit_logger = logging.getLogger("audit")
            assert len(audit_logger.handlers) > 0
            
            qc_logger = logging.getLogger("quality_control")
            assert len(qc_logger.handlers) > 0


class TestCustomExceptions:
    """Test custom exception classes."""
    
    def test_validation_exception(self):
        """Test ValidationException."""
        exc = ValidationException(
            message="Invalid age value",
            field="age",
            invalid_value=-5,
            expected_type="positive float"
        )
        
        assert exc.message == "Invalid age value"
        assert exc.category == ErrorCategory.VALIDATION
        assert exc.details["field"] == "age"
        assert exc.details["invalid_value"] == -5
        assert len(exc.suggestions) > 0
        assert len(exc.recovery_options) > 0
    
    def test_file_processing_exception(self):
        """Test FileProcessingException."""
        exc = FileProcessingException(
            message="Failed to process file",
            file_path="/path/to/file.csv",
            file_type="CSV"
        )
        
        assert exc.message == "Failed to process file"
        assert exc.category == ErrorCategory.PROCESSING
        assert exc.details["file_path"] == "/path/to/file.csv"
        assert exc.details["file_type"] == "CSV"
    
    def test_mriqc_processing_exception(self):
        """Test MRIQCProcessingException."""
        exc = MRIQCProcessingException(
            message="Invalid MRIQC format",
            missing_columns=["snr", "cnr"],
            invalid_metrics=["fber"]
        )
        
        assert exc.message == "Invalid MRIQC format"
        assert exc.details["missing_columns"] == ["snr", "cnr"]
        assert exc.details["invalid_metrics"] == ["fber"]
    
    def test_normalization_exception(self):
        """Test NormalizationException."""
        exc = NormalizationException(
            message="Failed to normalize metrics",
            age=25.5,
            age_group="young_adult",
            metric_name="snr"
        )
        
        assert exc.message == "Failed to normalize metrics"
        assert exc.details["age"] == 25.5
        assert exc.details["age_group"] == "young_adult"
        assert exc.details["metric_name"] == "snr"
    
    def test_quality_assessment_exception(self):
        """Test QualityAssessmentException."""
        exc = QualityAssessmentException(
            message="Quality assessment failed",
            subject_id="sub-001",
            assessment_type="automated"
        )
        
        assert exc.message == "Quality assessment failed"
        assert exc.category == ErrorCategory.BUSINESS_LOGIC
        assert exc.details["subject_id"] == "sub-001"
        assert exc.details["assessment_type"] == "automated"
    
    def test_batch_processing_exception(self):
        """Test BatchProcessingException."""
        failed_files = ["file1.csv", "file2.csv"]
        exc = BatchProcessingException(
            message="Batch processing failed",
            batch_id="batch-123",
            failed_files=failed_files
        )
        
        assert exc.message == "Batch processing failed"
        assert exc.details["batch_id"] == "batch-123"
        assert exc.details["failed_files"] == failed_files
        assert exc.details["failed_count"] == 2


class TestErrorHandlerMiddleware:
    """Test error handler middleware."""
    
    @pytest.mark.asyncio
    async def test_middleware_success(self):
        """Test middleware with successful request."""
        # Mock request and response
        mock_request = Mock()
        mock_request.method = "GET"
        mock_request.url = "http://test.com/api/test"
        mock_request.client.host = "127.0.0.1"
        mock_request.headers = {"user-agent": "test-client"}
        mock_request.state = Mock()
        
        mock_response = Mock()
        mock_response.status_code = 200
        
        async def mock_call_next(request):
            return mock_response
        
        with patch('app.error_handling.logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            response = await error_handler_middleware(mock_request, mock_call_next)
            
            assert response == mock_response
            assert hasattr(mock_request.state, 'request_id')
            
            # Check that request was logged
            assert mock_logger.info.call_count >= 2  # Start and completion
    
    @pytest.mark.asyncio
    async def test_middleware_http_exception(self):
        """Test middleware with HTTP exception."""
        mock_request = Mock()
        mock_request.method = "POST"
        mock_request.url = "http://test.com/api/upload"
        mock_request.client.host = "127.0.0.1"
        mock_request.headers = {"user-agent": "test-client"}
        mock_request.state = Mock()
        
        async def mock_call_next(request):
            raise HTTPException(status_code=400, detail="Bad request")
        
        with patch('app.error_handling.logging.getLogger'):
            response = await error_handler_middleware(mock_request, mock_call_next)
            
            assert response.status_code == 400
            # Response should be JSONResponse with structured error
            response_data = json.loads(response.body)
            assert 'error_id' in response_data
            assert 'error_type' in response_data
            assert response_data['message'] == "Bad request"
    
    @pytest.mark.asyncio
    async def test_middleware_unexpected_exception(self):
        """Test middleware with unexpected exception."""
        mock_request = Mock()
        mock_request.method = "GET"
        mock_request.url = "http://test.com/api/test"
        mock_request.client.host = "127.0.0.1"
        mock_request.headers = {"user-agent": "test-client"}
        mock_request.state = Mock()
        
        async def mock_call_next(request):
            raise ValueError("Unexpected error")
        
        with patch('app.error_handling.logging.getLogger'):
            response = await error_handler_middleware(mock_request, mock_call_next)
            
            assert response.status_code == 500
            # Response should be JSONResponse with structured error
            response_data = json.loads(response.body)
            assert 'error_id' in response_data
            assert response_data['error_type'] == "SYSTEM_ERROR"


class TestErrorScenarios:
    """Test various error scenarios."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.error_handler = ErrorHandler()
    
    def test_file_validation_error_scenario(self):
        """Test file validation error scenario."""
        # Simulate file validation error
        error_response = self.error_handler.create_error_response(
            error_type="INVALID_FILE_FORMAT",
            message="Missing required columns in MRIQC file",
            details={
                'missing_columns': ['snr', 'cnr'],
                'file_name': 'test.csv'
            }
        )
        
        assert error_response.error_code == "1002"
        assert "Check file format" in error_response.suggestions[0]
        assert "Convert file to CSV format" in error_response.recovery_options[0]
    
    def test_processing_timeout_scenario(self):
        """Test processing timeout scenario."""
        error_response = self.error_handler.create_error_response(
            error_type="TIMEOUT_ERROR",
            message="Processing timed out after 30 minutes",
            details={
                'timeout_duration': 1800,
                'subjects_processed': 150,
                'total_subjects': 200
            }
        )
        
        assert error_response.error_code == "3005"
        assert error_response.severity == ErrorSeverity.MEDIUM
    
    def test_database_connection_error_scenario(self):
        """Test database connection error scenario."""
        error_response = self.error_handler.create_error_response(
            error_type="DATABASE_ERROR",
            message="Failed to connect to normative database",
            details={
                'database_type': 'sqlite',
                'connection_string': 'sqlite:///data/normative_data.db'
            }
        )
        
        assert error_response.error_code == "3001"
        assert "Check database connection" in error_response.suggestions[0]
        assert "Retry operation after brief delay" in error_response.recovery_options[0]


@pytest.fixture
def setup_logging_for_tests():
    """Setup logging for tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir) / "test_logs"
        config = LoggingConfig(log_dir=log_dir)
        yield config
        
        # Cleanup loggers
        logging.getLogger().handlers.clear()
        logging.getLogger("audit").handlers.clear()
        logging.getLogger("quality_control").handlers.clear()


class TestIntegrationScenarios:
    """Test integration scenarios with error handling."""
    
    def test_complete_error_handling_flow(self, setup_logging_for_tests):
        """Test complete error handling flow."""
        error_handler = ErrorHandler()
        audit_logger = AuditLogger()
        
        # Simulate a processing error
        try:
            raise ValueError("Test processing error")
        except Exception as e:
            error_response = error_handler.handle_processing_error(
                operation="test_operation",
                message="Test processing failed",
                exception=e,
                context={'test_context': 'value'}
            )
        
        # Log the error decision
        audit_logger.log_user_action(
            action_type="error_handling",
            resource_type="processing_error",
            resource_id=error_response.error_id,
            new_values={
                'error_type': error_response.error_type,
                'severity': error_response.severity.value
            }
        )
        
        assert error_response.error_type == "PROCESSING_FAILED"
        assert error_response.severity == ErrorSeverity.HIGH
        assert error_response.details['exception_type'] == "ValueError"
        assert len(error_response.suggestions) > 0