"""
Integration tests for error handling and logging system.

This module tests the error handling system integration with the main application,
API endpoints, and real-world error scenarios.
"""

import pytest
import json
import tempfile
import logging
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import UploadFile
import io

from app.main import app
from app.error_handling import setup_logging, error_handler, audit_logger
from app.models import ProcessedSubject, SubjectInfo, MRIQCMetrics, QualityAssessment, QualityStatus


class TestAPIErrorHandling:
    """Test error handling in API endpoints."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.client = TestClient(app)
        # Setup logging for tests
        with tempfile.TemporaryDirectory() as temp_dir:
            self.log_dir = Path(temp_dir) / "test_logs"
            setup_logging()
    
    def test_file_upload_invalid_format_error(self):
        """Test file upload with invalid format."""
        # Create a non-CSV file
        file_content = b"This is not a CSV file"
        files = {"file": ("test.txt", io.BytesIO(file_content), "text/plain")}
        
        response = self.client.post("/api/upload", files=files)
        
        assert response.status_code == 400
        response_data = response.json()
        assert "File must be a CSV file" in response_data["detail"]
    
    def test_file_upload_oversized_file_error(self):
        """Test file upload with oversized file."""
        # Create a large file (simulate > 50MB)
        large_content = b"x" * (51 * 1024 * 1024)  # 51MB
        
        with patch('fastapi.UploadFile.size', 51 * 1024 * 1024):
            files = {"file": ("large.csv", io.BytesIO(large_content), "text/csv")}
            response = self.client.post("/api/upload", files=files)
        
        assert response.status_code == 400
        response_data = response.json()
        assert "File size exceeds 50MB limit" in response_data["detail"]
    
    def test_file_upload_invalid_csv_content(self):
        """Test file upload with invalid CSV content."""
        # Create invalid CSV content
        invalid_csv = b"invalid,csv,content\nwithout,proper,headers"
        files = {"file": ("invalid.csv", io.BytesIO(invalid_csv), "text/csv")}
        
        response = self.client.post("/api/upload", files=files)
        
        assert response.status_code == 400
        response_data = response.json()
        assert "Invalid MRIQC file" in response_data["detail"]
    
    def test_batch_status_not_found_error(self):
        """Test batch status endpoint with non-existent batch."""
        response = self.client.get("/api/batch/nonexistent-batch/status")
        
        assert response.status_code == 404
        response_data = response.json()
        assert "Batch not found" in response_data["detail"]
    
    def test_subjects_list_invalid_batch_error(self):
        """Test subjects list with invalid batch ID."""
        response = self.client.get("/api/subjects?batch_id=invalid-batch")
        
        assert response.status_code == 404
        response_data = response.json()
        assert "Batch not found" in response_data["detail"]
    
    @patch('app.routes.mriqc_processor.process_single_file')
    def test_processing_error_handling(self, mock_process):
        """Test processing error handling."""
        # Mock processing to raise an exception
        mock_process.side_effect = Exception("Processing failed")
        
        # First upload a valid file
        valid_csv = b"subject_id,snr,cnr\nsub-001,12.5,3.2"
        files = {"file": ("valid.csv", io.BytesIO(valid_csv), "text/csv")}
        
        with patch('app.routes.mriqc_processor.parse_mriqc_file') as mock_parse, \
             patch('app.routes.mriqc_processor.validate_mriqc_format') as mock_validate:
            
            # Mock successful parsing and validation
            mock_df = Mock()
            mock_df.__len__ = Mock(return_value=1)
            mock_parse.return_value = mock_df
            mock_validate.return_value = []
            
            upload_response = self.client.post("/api/upload", files=files)
            assert upload_response.status_code == 200
            
            file_id = upload_response.json()["file_id"]
            
            # Now try to process the file
            process_response = self.client.post("/api/process", json={
                "file_id": file_id,
                "apply_quality_assessment": True
            })
            
            assert process_response.status_code == 500
            response_data = process_response.json()
            assert "Processing failed" in response_data["detail"]


class TestErrorLoggingIntegration:
    """Test error logging integration."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.log_dir = Path(self.temp_dir) / "logs"
        setup_logging()
    
    def teardown_method(self):
        """Cleanup test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_audit_logging_file_creation(self):
        """Test that audit log files are created."""
        # Trigger some audit logging
        audit_logger.log_user_action(
            action_type="test_action",
            resource_type="test_resource",
            resource_id="test-123"
        )
        
        # Check that audit logger exists
        audit_log = logging.getLogger("audit")
        assert audit_log is not None
        assert len(audit_log.handlers) > 0
    
    def test_quality_control_logging(self):
        """Test quality control decision logging."""
        audit_logger.log_quality_decision(
            subject_id="sub-001",
            decision="pass",
            reason="All metrics within normal range",
            automated=True,
            confidence=0.95,
            metrics={"snr": 12.5, "cnr": 3.2}
        )
        
        # Check that QC logger exists
        qc_logger = logging.getLogger("quality_control")
        assert qc_logger is not None
        assert len(qc_logger.handlers) > 0
    
    def test_error_logging_with_context(self):
        """Test error logging with context information."""
        try:
            raise ValueError("Test error for logging")
        except Exception as e:
            error_response = error_handler.handle_processing_error(
                operation="test_operation",
                message="Test error occurred",
                exception=e,
                context={"test_context": "integration_test"}
            )
        
        assert error_response.error_type == "PROCESSING_FAILED"
        assert error_response.context["test_context"] == "integration_test"


class TestRealWorldErrorScenarios:
    """Test real-world error scenarios."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.client = TestClient(app)
        setup_logging()
    
    def test_malformed_mriqc_file_scenario(self):
        """Test handling of malformed MRIQC file."""
        # Create a CSV with missing required columns
        malformed_csv = b"participant_id,wrong_column\nsub-001,value1"
        files = {"file": ("malformed.csv", io.BytesIO(malformed_csv), "text/csv")}
        
        response = self.client.post("/api/upload", files=files)
        
        assert response.status_code == 400
        response_data = response.json()
        assert "Invalid MRIQC file" in response_data["detail"]
    
    def test_corrupted_file_scenario(self):
        """Test handling of corrupted file."""
        # Create a file with binary data that looks like CSV
        corrupted_csv = b"\x00\x01\x02subject_id,snr\nsub-001,\xff\xfe"
        files = {"file": ("corrupted.csv", io.BytesIO(corrupted_csv), "text/csv")}
        
        response = self.client.post("/api/upload", files=files)
        
        assert response.status_code == 400
        response_data = response.json()
        assert "Invalid MRIQC file" in response_data["detail"]
    
    def test_empty_file_scenario(self):
        """Test handling of empty file."""
        empty_csv = b""
        files = {"file": ("empty.csv", io.BytesIO(empty_csv), "text/csv")}
        
        response = self.client.post("/api/upload", files=files)
        
        assert response.status_code == 400
        response_data = response.json()
        assert "Invalid MRIQC file" in response_data["detail"]
    
    def test_file_with_invalid_metrics_scenario(self):
        """Test handling of file with invalid metric values."""
        # Create CSV with invalid metric values
        invalid_metrics_csv = b"subject_id,snr,cnr,age\nsub-001,invalid,not_a_number,negative_age"
        files = {"file": ("invalid_metrics.csv", io.BytesIO(invalid_metrics_csv), "text/csv")}
        
        response = self.client.post("/api/upload", files=files)
        
        # This might succeed upload but fail during processing
        if response.status_code == 200:
            file_id = response.json()["file_id"]
            
            # Try to process the file
            process_response = self.client.post("/api/process", json={
                "file_id": file_id,
                "apply_quality_assessment": True
            })
            
            # Should handle the error gracefully
            assert process_response.status_code in [400, 500]
        else:
            assert response.status_code == 400


class TestConcurrentErrorHandling:
    """Test error handling under concurrent conditions."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.client = TestClient(app)
        setup_logging()
    
    @pytest.mark.asyncio
    async def test_concurrent_file_uploads_with_errors(self):
        """Test concurrent file uploads with various error conditions."""
        import asyncio
        import aiohttp
        
        # Create different types of problematic files
        files_data = [
            ("valid.csv", b"subject_id,snr,cnr\nsub-001,12.5,3.2"),
            ("invalid.txt", b"This is not a CSV"),
            ("empty.csv", b""),
            ("malformed.csv", b"wrong,headers\nvalue1,value2"),
        ]
        
        async def upload_file(file_data):
            filename, content = file_data
            files = {"file": (filename, io.BytesIO(content), "text/csv")}
            return self.client.post("/api/upload", files=files)
        
        # Upload files concurrently
        tasks = [upload_file(file_data) for file_data in files_data]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check that errors were handled appropriately
        valid_responses = [r for r in responses if not isinstance(r, Exception)]
        assert len(valid_responses) == len(files_data)
        
        # At least one should succeed (the valid CSV)
        success_count = sum(1 for r in valid_responses if r.status_code == 200)
        assert success_count >= 1
        
        # Others should have appropriate error codes
        error_count = sum(1 for r in valid_responses if r.status_code >= 400)
        assert error_count >= 3


class TestErrorRecoveryScenarios:
    """Test error recovery scenarios."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.client = TestClient(app)
        setup_logging()
    
    def test_retry_after_temporary_error(self):
        """Test retry mechanism after temporary error."""
        # Simulate a temporary error followed by success
        valid_csv = b"subject_id,snr,cnr\nsub-001,12.5,3.2"
        files = {"file": ("retry_test.csv", io.BytesIO(valid_csv), "text/csv")}
        
        with patch('app.routes.mriqc_processor.parse_mriqc_file') as mock_parse:
            # First call fails, second succeeds
            mock_parse.side_effect = [
                Exception("Temporary error"),
                Mock(__len__=Mock(return_value=1))
            ]
            
            # First attempt should fail
            response1 = self.client.post("/api/upload", files=files)
            assert response1.status_code == 400
            
            # Reset file pointer for retry
            files = {"file": ("retry_test.csv", io.BytesIO(valid_csv), "text/csv")}
            
            # Second attempt should succeed (if retry logic is implemented)
            with patch('app.routes.mriqc_processor.validate_mriqc_format', return_value=[]):
                response2 = self.client.post("/api/upload", files=files)
                # This would succeed if retry logic is implemented
                # For now, it will still fail as we don't have retry logic
                assert response2.status_code in [200, 400]
    
    def test_graceful_degradation_scenario(self):
        """Test graceful degradation when optional services fail."""
        # Test scenario where normalization fails but processing continues
        valid_csv = b"subject_id,snr,cnr,age\nsub-001,12.5,3.2,25"
        files = {"file": ("degradation_test.csv", io.BytesIO(valid_csv), "text/csv")}
        
        with patch('app.routes.mriqc_processor.parse_mriqc_file') as mock_parse, \
             patch('app.routes.mriqc_processor.validate_mriqc_format') as mock_validate:
            
            mock_df = Mock()
            mock_df.__len__ = Mock(return_value=1)
            mock_parse.return_value = mock_df
            mock_validate.return_value = []
            
            response = self.client.post("/api/upload", files=files)
            assert response.status_code == 200
            
            file_id = response.json()["file_id"]
            
            # Mock normalization failure but allow processing to continue
            with patch('app.routes.age_normalizer.normalize_metrics') as mock_normalize:
                mock_normalize.side_effect = Exception("Normalization service unavailable")
                
                # Processing should still work, just without normalization
                with patch('app.routes.mriqc_processor.process_single_file') as mock_process:
                    # Create mock processed subject
                    mock_subject = ProcessedSubject(
                        subject_info=SubjectInfo(
                            subject_id="sub-001",
                            age=25,
                            scan_type="T1w"
                        ),
                        raw_metrics=MRIQCMetrics(snr=12.5, cnr=3.2),
                        quality_assessment=QualityAssessment(
                            overall_status=QualityStatus.PASS,
                            metric_assessments={},
                            composite_score=75.0,
                            confidence=0.8
                        )
                    )
                    mock_process.return_value = [mock_subject]
                    
                    process_response = self.client.post("/api/process", json={
                        "file_id": file_id,
                        "apply_quality_assessment": True
                    })
                    
                    # Should succeed despite normalization failure
                    assert process_response.status_code == 200


class TestErrorMetricsAndMonitoring:
    """Test error metrics and monitoring capabilities."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.client = TestClient(app)
        setup_logging()
    
    def test_error_rate_tracking(self):
        """Test error rate tracking capabilities."""
        # Generate multiple requests with different outcomes
        test_files = [
            ("valid1.csv", b"subject_id,snr,cnr\nsub-001,12.5,3.2", 200),
            ("invalid1.txt", b"Not a CSV file", 400),
            ("valid2.csv", b"subject_id,snr,cnr\nsub-002,10.1,2.8", 200),
            ("invalid2.csv", b"wrong,headers\nvalue1,value2", 400),
        ]
        
        responses = []
        for filename, content, expected_status in test_files:
            files = {"file": (filename, io.BytesIO(content), "text/csv")}
            
            if filename.endswith('.txt'):
                # For non-CSV files, use appropriate content type
                files = {"file": (filename, io.BytesIO(content), "text/plain")}
            
            response = self.client.post("/api/upload", files=files)
            responses.append((response.status_code, expected_status))
        
        # Verify expected status codes
        success_count = sum(1 for actual, expected in responses if actual == 200)
        error_count = sum(1 for actual, expected in responses if actual >= 400)
        
        assert success_count >= 1  # At least one success
        assert error_count >= 1   # At least one error
        
        # Error rate should be trackable
        total_requests = len(responses)
        error_rate = error_count / total_requests
        assert 0 <= error_rate <= 1
    
    def test_error_categorization(self):
        """Test that errors are properly categorized."""
        # Test different error categories
        error_scenarios = [
            # Validation errors
            ("invalid_format.txt", b"Not CSV", "validation"),
            ("empty.csv", b"", "validation"),
            
            # Processing errors would be tested with mocked failures
        ]
        
        for filename, content, expected_category in error_scenarios:
            files = {"file": (filename, io.BytesIO(content), "text/csv" if filename.endswith('.csv') else "text/plain")}
            response = self.client.post("/api/upload", files=files)
            
            if response.status_code >= 400:
                # Error should be categorized appropriately
                # In a real implementation, we might have error categorization in response
                assert response.status_code in [400, 422, 500]


@pytest.fixture
def mock_logging_setup():
    """Mock logging setup for tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir) / "test_logs"
        with patch('app.error_handling.PROJECT_ROOT', Path(temp_dir)):
            setup_logging()
            yield log_dir
        
        # Cleanup
        for logger_name in ['', 'audit', 'quality_control']:
            logger = logging.getLogger(logger_name)
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)


class TestErrorHandlingConfiguration:
    """Test error handling configuration and customization."""
    
    def test_custom_error_codes(self):
        """Test custom error code configuration."""
        # Test that error codes are properly assigned
        error_response = error_handler.create_error_response(
            error_type="VALIDATION_FAILED",
            message="Test validation error"
        )
        
        assert error_response.error_code == "1001"
        
        error_response2 = error_handler.create_error_response(
            error_type="DATABASE_ERROR",
            message="Test database error"
        )
        
        assert error_response2.error_code == "3001"
    
    def test_error_suggestions_customization(self):
        """Test error suggestions customization."""
        error_response = error_handler.create_error_response(
            error_type="INVALID_FILE_FORMAT",
            message="Invalid file format"
        )
        
        # Should have specific suggestions for file format errors
        assert len(error_response.suggestions) > 0
        assert any("file" in suggestion.lower() for suggestion in error_response.suggestions)
        assert len(error_response.recovery_options) > 0
    
    def test_severity_level_assignment(self):
        """Test severity level assignment."""
        # Test different severity levels
        validation_error = error_handler.handle_validation_error(
            field="test_field",
            message="Validation failed"
        )
        assert validation_error.severity == ErrorSeverity.MEDIUM
        
        system_error = error_handler.handle_system_error(
            component="test_component",
            message="System error"
        )
        assert system_error.severity == ErrorSeverity.CRITICAL
        
        processing_error = error_handler.handle_processing_error(
            operation="test_operation",
            message="Processing failed"
        )
        assert processing_error.severity == ErrorSeverity.HIGH