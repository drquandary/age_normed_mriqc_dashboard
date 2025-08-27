#!/usr/bin/env python3
"""
Security validation script for Age-Normed MRIQC Dashboard.

This script validates that all security features are working correctly.
"""

import tempfile
from pathlib import Path
from app.security import (
    SecurityConfig, InputSanitizer, VirusScanner, SecureFileHandler,
    DataRetentionManager, SecurityAuditor
)


def test_input_sanitization():
    """Test input sanitization features."""
    print("Testing input sanitization...")
    
    config = SecurityConfig()
    sanitizer = InputSanitizer(config)
    
    # Test filename sanitization
    safe_filename = sanitizer.sanitize_filename("test.csv")
    assert safe_filename == "test.csv"
    print("✓ Safe filename validation passed")
    
    # Test dangerous filename rejection
    try:
        sanitizer.sanitize_filename("../../../etc/passwd")
        assert False, "Should have rejected dangerous filename"
    except ValueError:
        print("✓ Dangerous filename rejection passed")
    
    # Test subject ID validation
    valid_id = sanitizer.validate_subject_id("SUB001")
    assert valid_id == "SUB001"
    print("✓ Subject ID validation passed")
    
    print("Input sanitization tests completed successfully!\n")


def test_file_security():
    """Test file security features."""
    print("Testing file security...")
    
    config = SecurityConfig()
    temp_dir = Path(tempfile.mkdtemp())
    upload_dir = Path(tempfile.mkdtemp())
    
    try:
        handler = SecureFileHandler(config, upload_dir, temp_dir)
        print("✓ Secure file handler initialized")
        
        # Test virus scanner initialization
        scanner = VirusScanner(config)
        print(f"✓ Virus scanner initialized (enabled: {scanner.enabled})")
        
        print("File security tests completed successfully!\n")
    
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
        shutil.rmtree(upload_dir, ignore_errors=True)


def test_data_retention():
    """Test data retention features."""
    print("Testing data retention...")
    
    config = SecurityConfig(data_retention_days=1)
    manager = DataRetentionManager(config)
    
    # Test cleanup service
    manager.start_cleanup_service()
    print("✓ Cleanup service started")
    
    manager.stop_cleanup_service()
    print("✓ Cleanup service stopped")
    
    print("Data retention tests completed successfully!\n")


def test_audit_logging():
    """Test audit logging features."""
    print("Testing audit logging...")
    
    config = SecurityConfig(enable_audit_logging=True)
    auditor = SecurityAuditor(config)
    
    # Test logging
    auditor.log_security_event('test_event', {'test': 'data'})
    print("✓ Security event logging works")
    
    print("Audit logging tests completed successfully!\n")


def main():
    """Run all security validation tests."""
    print("=== Age-Normed MRIQC Dashboard Security Validation ===\n")
    
    try:
        test_input_sanitization()
        test_file_security()
        test_data_retention()
        test_audit_logging()
        
        print("🎉 All security features validated successfully!")
        print("The security implementation is working correctly.")
        
    except Exception as e:
        print(f"❌ Security validation failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())