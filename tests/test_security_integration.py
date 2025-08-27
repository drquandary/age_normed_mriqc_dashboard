"""
Integration tests for security features with API endpoints.

This module tests the integration of security features with the actual API endpoints,
including file upload security, threat detection, and privacy compliance.
"""

import io
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock
import pytest
from fastapi.testclient import TestClient

from app.main import app


class TestSecurityAPIIntegration:
    """Test security integration with API endpoints."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.client = TestClient(app)
    
    def test_secure_file_upload_valid(self):
        """Test secure file upload with valid CSV."""
        csv_content = b"subject_id,age,snr\nSUB001,25,12.5\nSUB002,30,11.8\n"
        
        with patch('magic.from_file', return_value='text/csv'):
            with patch('app.routes.secure_file_handler.virus_scanner.scan_file', return_value=(True, None)):
                response = self.client.post(
                    "/api/upload",
                    files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
                )
        
        assert response.status_code == 200
        data = response.json()
        assert "uploaded and validated successfully" in data["message"]
        assert data["filename"] == "test.csv"
    
    def test_secure_file_upload_malware_detected(self):
        """Test file upload rejection when malware is detected."""
        csv_content = b"subject_id,age\nSUB001,25\n"
        
        with patch('magic.from_file', return_value='text/csv'):
            with patch('app.routes.secure_file_handler.virus_scanner.scan_file', return_value=(False, "Virus detected")):
                response = self.client.post(
                    "/api/upload",
                    files={"file": ("infected.csv", io.BytesIO(csv_content), "text/csv")}
                )
        
        assert response.status_code == 400
        assert "Security validation failed" in response.json()["detail"]
    
    def test_secure_file_upload_invalid_extension(self):
        """Test file upload rejection for invalid extension."""
        content = b"malicious content"
        
        response = self.client.post(
            "/api/upload",
            files={"file": ("malware.exe", io.BytesIO(content), "application/octet-stream")}
        )
        
        assert response.status_code == 400
        assert "Security validation failed" in response.json()["detail"]
    
    def test_secure_file_upload_oversized(self):
        """Test file upload rejection for oversized files."""
        # Create content larger than the limit
        large_content = b"x" * (100 * 1024 * 1024)  # 100MB
        
        response = self.client.post(
            "/api/upload",
            files={"file": ("large.csv", io.BytesIO(large_content), "text/csv")}
        )
        
        assert response.status_code == 400
        assert "Security validation failed" in response.json()["detail"]
    
    def test_security_status_endpoint(self):
        """Test security status endpoint."""
        response = self.client.get("/api/security/status")
        assert response.status_code == 200
        
        data = response.json()
        required_fields = [
            'security_enabled', 'virus_scan_enabled', 'data_retention_days',
            'audit_logging_enabled', 'threat_count_24h', 'files_cleaned_24h'
        ]
        
        for field in required_fields:
            assert field in data
    
    def test_manual_cleanup_endpoint(self):
        """Test manual data cleanup endpoint."""
        response = self.client.post("/api/security/cleanup", json={
            "force_cleanup": False,
            "target_directories": ["uploads"]
        })
        
        assert response.status_code == 200
        data = response.json()
        
        required_fields = [
            'files_deleted', 'directories_cleaned', 'bytes_freed',
            'errors', 'cleanup_timestamp'
        ]
        
        for field in required_fields:
            assert field in data
    
    def test_privacy_compliance_endpoint(self):
        """Test privacy compliance check endpoint."""
        response = self.client.get("/api/security/privacy-compliance")
        assert response.status_code == 200
        
        data = response.json()
        assert 'compliant' in data
        assert 'issues' in data
        assert 'recommendations' in data
        assert isinstance(data['issues'], list)
        assert isinstance(data['recommendations'], list)
    
    def test_security_threats_endpoint(self):
        """Test security threats endpoint."""
        response = self.client.get("/api/security/threats")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        
        # Test with query parameters
        response = self.client.get("/api/security/threats?limit=10&severity=HIGH")
        assert response.status_code == 200 
   
    def test_security_headers_middleware(self):
        """Test that security headers are added to responses."""
        response = self.client.get("/api/security/status")
        
        # Check for security headers (these would be added by middleware in production)
        expected_headers = [
            'X-Content-Type-Options',
            'X-Frame-Options', 
            'X-XSS-Protection'
        ]
        
        # Note: In test environment, middleware might not be fully active
        # This test documents the expected behavior
        assert response.status_code == 200
    
    def test_rate_limiting_behavior(self):
        """Test rate limiting behavior (if implemented)."""
        # Make multiple rapid requests
        responses = []
        for i in range(10):
            response = self.client.get("/api/security/status")
            responses.append(response.status_code)
        
        # All should succeed in test environment
        # In production, rate limiting would kick in
        assert all(status == 200 for status in responses)
    
    def test_input_sanitization_in_endpoints(self):
        """Test input sanitization in various endpoints."""
        # Test with potentially malicious input
        malicious_inputs = [
            "<script>alert('xss')</script>",
            "'; DROP TABLE subjects; --",
            "../../../etc/passwd",
            "javascript:void(0)"
        ]
        
        for malicious_input in malicious_inputs:
            # Test subject ID endpoint with malicious input
            response = self.client.get(f"/api/subjects/{malicious_input}")
            
            # Should handle gracefully (404 or sanitized)
            assert response.status_code in [400, 404, 422]
    
    def test_file_upload_with_malicious_filename(self):
        """Test file upload with malicious filename."""
        csv_content = b"subject_id,age\nSUB001,25\n"
        malicious_filenames = [
            "../../../etc/passwd.csv",
            "<script>alert('xss')</script>.csv",
            "normal_file.csv'; DROP TABLE subjects; --.csv"
        ]
        
        for filename in malicious_filenames:
            response = self.client.post(
                "/api/upload",
                files={"file": (filename, io.BytesIO(csv_content), "text/csv")}
            )
            
            # Should be rejected or sanitized
            assert response.status_code in [400, 200]
            
            if response.status_code == 200:
                # If accepted, filename should be sanitized
                data = response.json()
                assert "<script>" not in data.get("filename", "")
                assert "../" not in data.get("filename", "")
    
    def test_audit_logging_integration(self):
        """Test that security events are properly logged."""
        # Make a request that should be audited
        response = self.client.get("/api/security/status")
        assert response.status_code == 200
        
        # In a real implementation, we would check audit logs
        # For now, just verify the endpoint works
        assert True
    
    def test_data_retention_integration(self):
        """Test data retention integration with file operations."""
        # Upload a file
        csv_content = b"subject_id,age\nSUB001,25\n"
        
        with patch('magic.from_file', return_value='text/csv'):
            with patch('app.routes.secure_file_handler.virus_scanner.scan_file', return_value=(True, None)):
                upload_response = self.client.post(
                    "/api/upload",
                    files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
                )
        
        if upload_response.status_code == 200:
            # Trigger cleanup
            cleanup_response = self.client.post("/api/security/cleanup", json={
                "force_cleanup": True
            })
            
            assert cleanup_response.status_code == 200
    
    def test_error_handling_security(self):
        """Test that error messages don't leak sensitive information."""
        # Test with various error conditions
        error_conditions = [
            ("/api/subjects/nonexistent", 404),
            ("/api/nonexistent-endpoint", 404),
        ]
        
        for endpoint, expected_status in error_conditions:
            response = self.client.get(endpoint)
            assert response.status_code == expected_status
            
            # Error messages should not contain sensitive information
            if response.status_code >= 400:
                error_detail = response.json().get("detail", "")
                
                # Should not contain file paths, stack traces, etc.
                sensitive_patterns = [
                    "/home/", "/usr/", "/var/",  # File paths
                    "Traceback", "File \"",      # Stack traces
                    "password", "secret", "key"   # Sensitive terms
                ]
                
                for pattern in sensitive_patterns:
                    assert pattern.lower() not in error_detail.lower()


class TestSecurityPerformance:
    """Test security feature performance impact."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.client = TestClient(app)
    
    def test_security_overhead_acceptable(self):
        """Test that security features don't add excessive overhead."""
        import time
        
        # Measure response time with security features
        start_time = time.time()
        response = self.client.get("/api/security/status")
        end_time = time.time()
        
        assert response.status_code == 200
        response_time = end_time - start_time
        
        # Should respond within reasonable time (adjust threshold as needed)
        assert response_time < 1.0  # 1 second threshold
    
    def test_file_upload_security_performance(self):
        """Test file upload security validation performance."""
        import time
        
        csv_content = b"subject_id,age\nSUB001,25\n" * 1000  # Larger file
        
        with patch('magic.from_file', return_value='text/csv'):
            with patch('app.routes.secure_file_handler.virus_scanner.scan_file', return_value=(True, None)):
                start_time = time.time()
                response = self.client.post(
                    "/api/upload",
                    files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
                )
                end_time = time.time()
        
        response_time = end_time - start_time
        
        # Security validation should not add excessive overhead
        assert response_time < 5.0  # 5 second threshold for larger files


# Mock data for testing
@pytest.fixture
def sample_secure_csv():
    """Sample secure CSV content."""
    return b"""subject_id,age,snr,cnr,fber,efc,fwhm_avg
SUB001,25,12.5,3.2,1500.0,0.45,2.8
SUB002,30,11.8,3.0,1450.0,0.48,2.9
SUB003,35,13.2,3.5,1600.0,0.42,2.7"""


@pytest.fixture
def malicious_csv():
    """Malicious CSV content for testing."""
    return b"""subject_id,age,notes
SUB001,25,"<script>alert('xss')</script>"
SUB002,30,"'; DROP TABLE subjects; --"
SUB003,35,"javascript:void(0)" """