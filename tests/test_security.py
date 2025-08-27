"""
Tests for security and privacy features.

This module tests comprehensive security measures including:
- Input validation and sanitization
- Secure file upload handling
- Data retention policies
- Privacy compliance validation
"""

import hashlib
import os
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest
import pandas as pd
from fastapi import HTTPException, UploadFile
from fastapi.testclient import TestClient

from app.security import (
    SecurityConfig, InputSanitizer, VirusScanner, SecureFileHandler,
    DataRetentionManager, SecurityAuditor, SecurityThreat, ThreatType,
    SecurityLevel
)


class TestSecurityConfig:
    """Test security configuration."""
    
    def test_default_config(self):
        """Test default security configuration."""
        config = SecurityConfig()
        assert config.max_file_size == 52428800  # 50MB
        assert '.csv' in config.allowed_extensions
        assert 'text/csv' in config.allowed_mime_types
        assert config.virus_scan_enabled is True
        assert config.data_retention_days == 30
    
    def test_custom_config(self):
        """Test custom security configuration."""
        config = SecurityConfig(
            max_file_size=10485760,  # 10MB
            data_retention_days=7,
            virus_scan_enabled=False
        )
        assert config.max_file_size == 10485760
        assert config.data_retention_days == 7
        assert config.virus_scan_enabled is False


class TestInputSanitizer:
    """Test input sanitization and validation."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.config = SecurityConfig()
        self.sanitizer = InputSanitizer(self.config)
    
    def test_sanitize_filename_valid(self):
        """Test sanitizing valid filenames."""
        assert self.sanitizer.sanitize_filename("test.csv") == "test.csv"
        assert self.sanitizer.sanitize_filename("subject_001.csv") == "subject_001.csv"
    
    def test_sanitize_filename_invalid_chars(self):
        """Test sanitizing filenames with invalid characters."""
        result = self.sanitizer.sanitize_filename("test<script>.csv")
        assert "<" not in result
        assert "script" in result
    
    def test_sanitize_filename_path_traversal(self):
        """Test preventing path traversal attacks."""
        result = self.sanitizer.sanitize_filename("../../../etc/passwd")
        assert ".." not in result
        assert result == "passwd"
    
    def test_sanitize_filename_blocked_patterns(self):
        """Test blocking dangerous patterns in filenames."""
        with pytest.raises(ValueError, match="blocked pattern"):
            self.sanitizer.sanitize_filename("test<script>alert.csv")
    
    def test_sanitize_filename_too_long(self):
        """Test filename length validation."""
        long_name = "a" * 300 + ".csv"
        with pytest.raises(ValueError, match="too long"):
            self.sanitizer.sanitize_filename(long_name)
    
    def test_validate_file_extension_valid(self):
        """Test valid file extension validation."""
        assert self.sanitizer.validate_file_extension("test.csv") is True
        assert self.sanitizer.validate_file_extension("TEST.CSV") is True
    
    def test_validate_file_extension_invalid(self):
        """Test invalid file extension validation."""
        assert self.sanitizer.validate_file_extension("test.exe") is False
        assert self.sanitizer.validate_file_extension("test.txt") is False
    
    def test_sanitize_text_input_clean(self):
        """Test sanitizing clean text input."""
        text = "This is clean text"
        result = self.sanitizer.sanitize_text_input(text)
        assert result == text
    
    def test_sanitize_text_input_with_html(self):
        """Test sanitizing text with HTML tags."""
        text = "Hello <script>alert('xss')</script> world"
        result = self.sanitizer.sanitize_text_input(text)
        assert "<script>" not in result
        assert "Hello" in result
        assert "world" in result
    
    def test_validate_subject_id_valid(self):
        """Test valid subject ID validation."""
        assert self.sanitizer.validate_subject_id("SUB001") == "SUB001"
        assert self.sanitizer.validate_subject_id("sub_001") == "sub_001"
        assert self.sanitizer.validate_subject_id("sub-001") == "sub-001"
    
    def test_validate_subject_id_invalid_chars(self):
        """Test subject ID with invalid characters."""
        with pytest.raises(ValueError, match="invalid characters"):
            self.sanitizer.validate_subject_id("sub@001")
    
    def test_validate_subject_id_potential_identifier(self):
        """Test subject ID that might contain identifying information."""
        with pytest.raises(ValueError, match="identifying information"):
            self.sanitizer.validate_subject_id("123-45-6789")  # SSN pattern


class TestVirusScanner:
    """Test virus scanning functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.config = SecurityConfig()
        self.scanner = VirusScanner(self.config)
    
    @patch('subprocess.run')
    def test_scan_file_clean_clamav(self, mock_run):
        """Test scanning clean file with ClamAV."""
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        self.scanner.scanner_type = 'clamav'
        self.scanner.enabled = True
        
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmp:
            tmp.write(b"test,data\n1,2\n")
            tmp.flush()
            
            is_clean, threat = self.scanner.scan_file(Path(tmp.name))
            assert is_clean is True
            assert threat is None
    
    @patch('subprocess.run')
    def test_scan_file_infected_clamav(self, mock_run):
        """Test scanning infected file with ClamAV."""
        mock_run.return_value = Mock(
            returncode=1, 
            stdout="FOUND: Eicar-Test-Signature",
            stderr=""
        )
        self.scanner.scanner_type = 'clamav'
        self.scanner.enabled = True
        
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmp:
            is_clean, threat = self.scanner.scan_file(Path(tmp.name))
            assert is_clean is False
            assert "Malware detected" in threat
    
    def test_scan_file_disabled(self):
        """Test scanning when virus scanning is disabled."""
        self.scanner.enabled = False
        
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmp:
            is_clean, threat = self.scanner.scan_file(Path(tmp.name))
            assert is_clean is True
            assert threat is None
    
    @patch('magic.from_file')
    def test_basic_malware_check_valid_csv(self, mock_magic):
        """Test basic malware check for valid CSV."""
        mock_magic.return_value = 'text/csv'
        self.scanner.enabled = True
        self.scanner.scanner_type = None
        
        with tempfile.NamedTemporaryFile(suffix='.csv', mode='w') as tmp:
            tmp.write("subject_id,age,snr\nSUB001,25,12.5\n")
            tmp.flush()
            
            is_clean, threat = self.scanner.scan_file(Path(tmp.name))
            assert is_clean is True
            assert threat is None
    
    @patch('magic.from_file')
    def test_basic_malware_check_suspicious_content(self, mock_magic):
        """Test basic malware check for suspicious content."""
        mock_magic.return_value = 'text/csv'
        self.scanner.enabled = True
        self.scanner.scanner_type = None
        
        with tempfile.NamedTemporaryFile(suffix='.csv', mode='w') as tmp:
            tmp.write("subject_id,age\n<script>alert('xss')</script>,25\n")
            tmp.flush()
            
            is_clean, threat = self.scanner.scan_file(Path(tmp.name))
            assert is_clean is False
            assert "script content" in threat


class TestSecureFileHandler:
    """Test secure file upload handling."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.config = SecurityConfig(max_file_size=1024*1024)  # 1MB for testing
        self.temp_dir = Path(tempfile.mkdtemp())
        self.upload_dir = Path(tempfile.mkdtemp())
        self.handler = SecureFileHandler(self.config, self.upload_dir, self.temp_dir)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        shutil.rmtree(self.upload_dir, ignore_errors=True)
    
    @pytest.mark.asyncio
    async def test_validate_and_save_file_valid(self):
        """Test validating and saving a valid file."""
        # Create mock UploadFile
        content = b"subject_id,age,snr\nSUB001,25,12.5\n"
        mock_file = Mock(spec=UploadFile)
        mock_file.filename = "test.csv"
        mock_file.read = Mock(return_value=content)
        
        with patch('magic.from_file', return_value='text/csv'):
            with patch.object(self.handler.virus_scanner, 'scan_file', return_value=(True, None)):
                file_path, metadata = await self.handler.validate_and_save_file(mock_file)
                
                assert file_path.exists()
                assert metadata['original_filename'] == "test.csv"
                assert metadata['file_size'] == len(content)
                assert metadata['mime_type'] == 'text/csv'
                assert 'sha256_hash' in metadata
    
    @pytest.mark.asyncio
    async def test_validate_and_save_file_too_large(self):
        """Test rejecting oversized files."""
        # Create large content
        content = b"x" * (self.config.max_file_size + 1)
        mock_file = Mock(spec=UploadFile)
        mock_file.filename = "large.csv"
        mock_file.read = Mock(return_value=content)
        
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.validate_and_save_file(mock_file)
        
        assert "File too large" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_validate_and_save_file_invalid_extension(self):
        """Test rejecting files with invalid extensions."""
        content = b"test content"
        mock_file = Mock(spec=UploadFile)
        mock_file.filename = "test.exe"
        mock_file.read = Mock(return_value=content)
        
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.validate_and_save_file(mock_file)
        
        assert "not allowed" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_validate_and_save_file_virus_detected(self):
        """Test rejecting files with detected malware."""
        content = b"subject_id,age\nSUB001,25\n"
        mock_file = Mock(spec=UploadFile)
        mock_file.filename = "infected.csv"
        mock_file.read = Mock(return_value=content)
        
        with patch('magic.from_file', return_value='text/csv'):
            with patch.object(self.handler.virus_scanner, 'scan_file', return_value=(False, "Virus detected")):
                with pytest.raises(HTTPException) as exc_info:
                    await self.handler.validate_and_save_file(mock_file)
                
                assert "Malware detected" in str(exc_info.value.detail)

class 
TestDataRetentionManager:
    """Test data retention and cleanup functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.config = SecurityConfig(data_retention_days=1)  # 1 day for testing
        self.manager = DataRetentionManager(self.config)
        self.test_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)
        self.manager.stop_cleanup_service()
    
    def test_cleanup_expired_data(self):
        """Test cleaning up expired data."""
        # Create old file
        old_file = self.test_dir / "old_file.csv"
        old_file.write_text("old,data\n")
        
        # Set modification time to 2 days ago
        old_time = time.time() - (2 * 24 * 3600)
        os.utime(old_file, (old_time, old_time))
        
        # Create recent file
        recent_file = self.test_dir / "recent_file.csv"
        recent_file.write_text("recent,data\n")
        
        # Mock the directories to clean
        with patch.object(self.manager, '_cleanup_directory') as mock_cleanup:
            mock_cleanup.return_value = {'files_deleted': 1, 'bytes_freed': 100}
            
            stats = self.manager.cleanup_expired_data()
            assert stats['files_deleted'] >= 0
            assert stats['bytes_freed'] >= 0
    
    def test_force_cleanup_file(self):
        """Test force cleanup of specific file."""
        test_file = self.test_dir / "test_file.csv"
        test_file.write_text("test,data\n")
        
        assert test_file.exists()
        result = self.manager.force_cleanup_file(test_file)
        assert result is True
        assert not test_file.exists()
    
    def test_force_cleanup_nonexistent_file(self):
        """Test force cleanup of non-existent file."""
        nonexistent_file = self.test_dir / "nonexistent.csv"
        result = self.manager.force_cleanup_file(nonexistent_file)
        assert result is False


class TestSecurityAuditor:
    """Test security audit logging."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.config = SecurityConfig(enable_audit_logging=True)
        self.auditor = SecurityAuditor(self.config)
        self.test_log_path = Path(tempfile.mktemp(suffix='.log'))
        self.auditor.audit_log_path = self.test_log_path
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if self.test_log_path.exists():
            self.test_log_path.unlink()
    
    def test_log_security_event(self):
        """Test logging security events."""
        self.auditor.log_security_event(
            'test_event',
            {'key': 'value'},
            SecurityLevel.HIGH
        )
        
        assert self.test_log_path.exists()
        content = self.test_log_path.read_text()
        assert 'test_event' in content
        assert 'HIGH' in content
    
    def test_log_file_upload(self):
        """Test logging file upload events."""
        self.auditor.log_file_upload(
            filename="test.csv",
            file_size=1024,
            client_ip="127.0.0.1",
            success=True
        )
        
        assert self.test_log_path.exists()
        content = self.test_log_path.read_text()
        assert 'file_upload' in content
        assert 'test.csv' in content
    
    def test_log_threat_detected(self):
        """Test logging detected threats."""
        threat = SecurityThreat(
            ThreatType.MALWARE,
            SecurityLevel.CRITICAL,
            "Test threat"
        )
        
        self.auditor.log_threat_detected(threat, "127.0.0.1")
        
        assert self.test_log_path.exists()
        content = self.test_log_path.read_text()
        assert 'threat_detected' in content
        assert 'CRITICAL' in content
    
    def test_disabled_audit_logging(self):
        """Test that logging is disabled when configured."""
        config = SecurityConfig(enable_audit_logging=False)
        auditor = SecurityAuditor(config)
        
        auditor.log_security_event('test', {})
        # Should not create log file when disabled
        assert not auditor.audit_log_path.exists()


class TestSecurityIntegration:
    """Integration tests for security features."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.config = SecurityConfig()
        self.temp_dir = Path(tempfile.mkdtemp())
        self.upload_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        shutil.rmtree(self.upload_dir, ignore_errors=True)
    
    @pytest.mark.asyncio
    async def test_complete_security_workflow(self):
        """Test complete security workflow from upload to cleanup."""
        handler = SecureFileHandler(self.config, self.upload_dir, self.temp_dir)
        retention_manager = DataRetentionManager(self.config)
        auditor = SecurityAuditor(self.config)
        
        # Create valid CSV content
        content = b"subject_id,age,snr\nSUB001,25,12.5\nSUB002,30,11.8\n"
        mock_file = Mock(spec=UploadFile)
        mock_file.filename = "mriqc_data.csv"
        mock_file.read = Mock(return_value=content)
        
        with patch('magic.from_file', return_value='text/csv'):
            with patch.object(handler.virus_scanner, 'scan_file', return_value=(True, None)):
                # Upload and validate file
                file_path, metadata = await handler.validate_and_save_file(mock_file)
                
                assert file_path.exists()
                assert metadata['file_size'] == len(content)
                
                # Log the upload
                auditor.log_file_upload(
                    filename=mock_file.filename,
                    file_size=metadata['file_size'],
                    client_ip="127.0.0.1",
                    success=True
                )
                
                # Verify file can be cleaned up
                result = retention_manager.force_cleanup_file(file_path)
                assert result is True
                assert not file_path.exists()


# Fixtures for testing
@pytest.fixture
def sample_csv_content():
    """Sample CSV content for testing."""
    return b"""subject_id,age,snr,cnr,fber,efc,fwhm_avg
SUB001,25,12.5,3.2,1500.0,0.45,2.8
SUB002,30,11.8,3.0,1450.0,0.48,2.9
SUB003,35,13.2,3.5,1600.0,0.42,2.7"""


@pytest.fixture
def malicious_csv_content():
    """Malicious CSV content for testing."""
    return b"""subject_id,age,script
SUB001,25,"<script>alert('xss')</script>"
SUB002,30,"javascript:void(0)"
SUB003,35,"normal_value" """


@pytest.fixture
def oversized_content():
    """Oversized content for testing."""
    return b"x" * (100 * 1024 * 1024)  # 100MB


# Parametrized tests for different threat scenarios
@pytest.mark.parametrize("filename,expected_threat", [
    ("../../../etc/passwd", "path_traversal"),
    ("test<script>.csv", "blocked_pattern"),
    ("normal_file.exe", "invalid_extension"),
    ("a" * 300 + ".csv", "filename_too_long"),
])
def test_filename_threats(filename, expected_threat):
    """Test various filename-based threats."""
    config = SecurityConfig()
    sanitizer = InputSanitizer(config)
    
    if expected_threat == "invalid_extension":
        assert not sanitizer.validate_file_extension(filename)
    else:
        with pytest.raises(ValueError):
            sanitizer.sanitize_filename(filename)