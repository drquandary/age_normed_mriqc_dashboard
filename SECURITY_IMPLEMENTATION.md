# Security and Privacy Implementation

This document describes the comprehensive security and privacy features implemented in the Age-Normed MRIQC Dashboard.

## Overview

The security implementation addresses the requirements specified in task 12:
- Input validation and sanitization to prevent security vulnerabilities
- Secure file upload handling with virus scanning and size limits
- Data retention policies and automatic cleanup mechanisms
- Privacy compliance validation and audit logging

## Security Features

### 1. Input Validation and Sanitization

#### Filename Sanitization
- **Path Traversal Protection**: Detects and blocks `../` patterns and absolute paths
- **Dangerous Character Removal**: Sanitizes characters like `<>:"/\|?*`
- **Length Validation**: Enforces maximum filename length (255 characters)
- **Pattern Blocking**: Blocks dangerous patterns like `<script>`, `javascript:`, etc.

#### Text Input Sanitization
- **HTML/Script Tag Removal**: Strips potentially dangerous HTML and script tags
- **Null Byte Removal**: Removes null bytes that could cause security issues
- **Pattern Detection**: Identifies and blocks common injection patterns

#### Subject ID Validation
- **Format Validation**: Allows only alphanumeric characters, hyphens, and underscores
- **Identifier Detection**: Basic detection of potential personally identifiable information

### 2. Secure File Upload Handling

#### File Type Validation
- **Extension Checking**: Only allows `.csv` files
- **MIME Type Validation**: Verifies actual file content matches expected MIME types
- **Magic Number Detection**: Uses `python-magic` to detect actual file types

#### File Size Limits
- **Configurable Limits**: Default 50MB maximum file size
- **Early Rejection**: Rejects oversized files before processing

#### Virus Scanning
- **ClamAV Integration**: Supports ClamAV antivirus scanning when available
- **Fallback Scanning**: Basic malware detection when external scanner unavailable
- **Content Analysis**: Scans CSV content for suspicious patterns

#### Secure Storage
- **Hash-based Naming**: Files stored with SHA256 hash prefixes
- **Secure Permissions**: Files created with restrictive permissions (640)
- **Temporary File Cleanup**: Automatic cleanup of temporary files

### 3. Data Retention and Cleanup

#### Automatic Cleanup Service
- **Background Service**: Runs cleanup tasks on configurable intervals
- **Age-based Deletion**: Removes files older than retention period
- **Statistics Tracking**: Logs cleanup statistics and errors

#### Manual Cleanup
- **API Endpoint**: `/api/security/cleanup` for manual cleanup triggers
- **Force Cleanup**: Option to force cleanup regardless of retention policy
- **Selective Cleanup**: Target specific directories for cleanup

#### Retention Policies
- **Configurable Periods**: Default 30-day retention, configurable
- **Multiple Directories**: Cleans uploads, temp, and watch directories
- **Graceful Handling**: Continues cleanup even if individual files fail

### 4. Security Audit and Logging

#### Comprehensive Audit Trail
- **Security Events**: Logs all security-related events with timestamps
- **File Operations**: Tracks file uploads, processing, and deletions
- **Access Logging**: Records data access attempts with client information
- **Threat Detection**: Logs detected security threats and responses

#### Audit Log Format
```json
{
  "timestamp": "2024-01-01T12:00:00Z",
  "event_type": "file_upload",
  "severity": "MEDIUM",
  "details": {
    "filename": "test.csv",
    "file_size": 1024,
    "client_ip": "127.0.0.1",
    "success": true
  },
  "process_id": 12345
}
```

## Privacy Compliance Features

### 1. Data Minimization
- **Required Fields Only**: Collects only necessary MRIQC metrics
- **No Personal Information**: Validates against collection of names, addresses, etc.
- **Subject ID Validation**: Ensures subject IDs don't contain identifying information

### 2. Data Anonymization
- **Identifier Detection**: Scans for potential SSNs, phone numbers, names
- **Content Validation**: Checks CSV content for personally identifiable information
- **Warning System**: Flags potentially identifying data for review

### 3. Right to Erasure (GDPR Article 17)
- **Force Cleanup**: Ability to immediately delete specific files
- **Batch Cleanup**: Remove multiple files or entire datasets
- **Audit Trail**: Logs all deletion activities for compliance

### 4. Data Portability (GDPR Article 20)
- **Export Functionality**: CSV and PDF export capabilities
- **Machine-readable Format**: Structured data export for portability
- **Complete Data Sets**: Export includes all processed metrics and assessments

### 5. Consent Tracking
- **Audit Logging**: Framework for logging consent events
- **Event Types**: Support for consent given/withdrawn events
- **Data Types**: Track consent for specific data processing activities

## API Security Endpoints

### Security Status
```
GET /api/security/status
```
Returns current security configuration and statistics.

### Manual Cleanup
```
POST /api/security/cleanup
```
Triggers manual data cleanup with optional parameters.

### Privacy Compliance Check
```
GET /api/security/privacy-compliance
```
Returns privacy compliance status and recommendations.

### Security Threats
```
GET /api/security/threats
```
Returns list of detected security threats and incidents.

## Configuration

### Environment Variables
```bash
# Security settings
SECURITY_ENABLED=true
VIRUS_SCAN_ENABLED=true
DATA_RETENTION_DAYS=30
CLEANUP_INTERVAL_HOURS=24
ENABLE_AUDIT_LOGGING=true
MAX_FILENAME_LENGTH=255
ALLOWED_MIME_TYPES=text/csv,application/csv
RATE_LIMIT_REQUESTS=100
RATE_LIMIT_WINDOW=3600
```

### Security Configuration Class
```python
class SecurityConfig(BaseModel):
    max_file_size: int = 52428800  # 50MB
    max_files_per_batch: int = 100
    allowed_extensions: Set[str] = {'.csv'}
    allowed_mime_types: Set[str] = {'text/csv', 'application/csv'}
    virus_scan_enabled: bool = True
    data_retention_days: int = 30
    cleanup_interval_hours: int = 24
    enable_audit_logging: bool = True
```

## Security Headers

The application automatically adds security headers to all responses:
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `X-XSS-Protection: 1; mode=block`
- `Strict-Transport-Security: max-age=31536000; includeSubDomains`
- `Content-Security-Policy: default-src 'self'; script-src 'self' 'unsafe-inline'`

## Testing

### Security Test Coverage
- **Input Sanitization Tests**: Validate all sanitization functions
- **File Upload Security Tests**: Test malware detection, size limits, type validation
- **Data Retention Tests**: Verify cleanup functionality and policies
- **Privacy Compliance Tests**: Check GDPR/HIPAA compliance features
- **Integration Tests**: End-to-end security workflow testing

### Running Security Tests
```bash
# Run all security tests
python -m pytest tests/test_security.py -v

# Run privacy compliance tests
python -m pytest tests/test_privacy_compliance.py -v

# Run security integration tests
python -m pytest tests/test_security_integration.py -v

# Validate security implementation
python validate_security.py
```

## Deployment Considerations

### Production Security
1. **HTTPS Only**: Ensure all traffic uses HTTPS in production
2. **Virus Scanner**: Install and configure ClamAV for production use
3. **File Permissions**: Verify secure file permissions on deployment
4. **Log Rotation**: Configure log rotation for audit logs
5. **Monitoring**: Set up monitoring for security events and threats

### Compliance Checklist
- [ ] Data retention policies configured
- [ ] Audit logging enabled
- [ ] Virus scanning operational
- [ ] Input validation active
- [ ] Security headers configured
- [ ] Privacy compliance validated
- [ ] Backup and recovery procedures
- [ ] Incident response plan

## Threat Model

### Identified Threats
1. **Malicious File Upload**: Mitigated by virus scanning and content validation
2. **Path Traversal**: Prevented by filename sanitization
3. **Code Injection**: Blocked by input sanitization and pattern detection
4. **Data Leakage**: Prevented by identifier detection and validation
5. **Unauthorized Access**: Mitigated by audit logging and access controls

### Security Boundaries
- File upload validation boundary
- Data processing boundary
- Export/output boundary
- Audit logging boundary

## Maintenance

### Regular Security Tasks
1. **Update Dependencies**: Keep security libraries up to date
2. **Review Audit Logs**: Regular review of security events
3. **Test Backup/Recovery**: Verify data recovery procedures
4. **Security Assessments**: Periodic security reviews and penetration testing
5. **Compliance Audits**: Regular privacy compliance assessments

### Monitoring and Alerting
- Monitor failed upload attempts
- Alert on detected security threats
- Track data retention compliance
- Monitor audit log integrity

## Support and Documentation

For additional security questions or to report security issues:
1. Review this documentation
2. Check the test suite for examples
3. Run the security validation script
4. Consult the API documentation for endpoint details

This implementation provides a robust security foundation while maintaining usability and performance for the Age-Normed MRIQC Dashboard.