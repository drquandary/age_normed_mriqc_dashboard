"""
Security and privacy features for the Age-Normed MRIQC Dashboard.

This module implements comprehensive security measures including:
- Input validation and sanitization
- Secure file upload handling with virus scanning
- Data retention policies and automatic cleanup
- Privacy compliance validation
"""

import hashlib
import logging
import os
import re
import shutil
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union
import mimetypes
import magic
import subprocess
import threading
from dataclasses import dataclass
from enum import Enum

from fastapi import HTTPException, UploadFile
from pydantic import BaseModel, Field, validator
import pandas as pd

logger = logging.getLogger(__name__)


class SecurityLevel(str, Enum):
    """Security levels for different operations."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ThreatType(str, Enum):
    """Types of security threats."""
    MALWARE = "malware"
    INJECTION = "injection"
    PATH_TRAVERSAL = "path_traversal"
    DATA_LEAK = "data_leak"
    OVERSIZED_FILE = "oversized_file"
    INVALID_FORMAT = "invalid_format"


@dataclass
class SecurityThreat:
    """Represents a detected security threat."""
    threat_type: ThreatType
    severity: SecurityLevel
    description: str
    file_path: Optional[str] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


class SecurityConfig(BaseModel):
    """Security configuration settings."""
    max_file_size: int = Field(default=52428800, description="Maximum file size in bytes (50MB)")
    max_files_per_batch: int = Field(default=100, description="Maximum files per batch upload")
    allowed_extensions: Set[str] = Field(default={'.csv'}, description="Allowed file extensions")
    allowed_mime_types: Set[str] = Field(default={'text/csv', 'application/csv'}, description="Allowed MIME types")
    virus_scan_enabled: bool = Field(default=True, description="Enable virus scanning")
    data_retention_days: int = Field(default=30, description="Data retention period in days")
    cleanup_interval_hours: int = Field(default=24, description="Cleanup interval in hours")
    enable_audit_logging: bool = Field(default=True, description="Enable security audit logging")
    max_filename_length: int = Field(default=255, description="Maximum filename length")
    blocked_patterns: List[str] = Field(
        default=[
            r'\.\./',  # Path traversal
            r'<script',  # XSS
            r'javascript:',  # JavaScript injection
            r'vbscript:',  # VBScript injection
            r'onload=',  # Event handlers
            r'onerror=',  # Event handlers
        ],
        description="Blocked patterns in filenames and content"
    )


class InputSanitizer:
    """Handles input validation and sanitization."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self.blocked_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in config.blocked_patterns]
    
    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to prevent security issues."""
        if not filename:
            raise ValueError("Filename cannot be empty")
        
        # Check length
        if len(filename) > self.config.max_filename_length:
            raise ValueError(f"Filename too long (max {self.config.max_filename_length} characters)")
        
        # Check for blocked patterns BEFORE removing path components
        for pattern in self.blocked_patterns:
            if pattern.search(filename):
                raise ValueError(f"Filename contains blocked pattern: {filename}")
        
        # Check for path traversal attempts
        if '..' in filename or filename.startswith('/'):
            raise ValueError(f"Path traversal attempt detected: {filename}")
        
        # Remove path traversal attempts (keep only basename)
        filename = os.path.basename(filename)
        
        # Remove or replace dangerous characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        # Ensure it doesn't start with dot or dash
        if sanitized.startswith(('.', '-')):
            sanitized = 'file_' + sanitized
        
        return sanitized
    
    def validate_file_extension(self, filename: str) -> bool:
        """Validate file extension against allowed list."""
        ext = Path(filename).suffix.lower()
        return ext in self.config.allowed_extensions
    
    def sanitize_text_input(self, text: str) -> str:
        """Sanitize text input to prevent injection attacks."""
        if not text:
            return text
        
        # Check for blocked patterns
        for pattern in self.blocked_patterns:
            if pattern.search(text):
                raise ValueError(f"Text contains blocked pattern")
        
        # Basic HTML/script tag removal
        text = re.sub(r'<[^>]*>', '', text)
        
        # Remove null bytes
        text = text.replace('\x00', '')
        
        return text.strip()
    
    def validate_subject_id(self, subject_id: str) -> str:
        """Validate and sanitize subject ID."""
        if not subject_id:
            raise ValueError("Subject ID cannot be empty")
        
        # Allow only alphanumeric, hyphens, and underscores
        if not re.match(r'^[a-zA-Z0-9_-]+$', subject_id):
            raise ValueError("Subject ID contains invalid characters")
        
        # Check for potential identifiers (basic patterns)
        if self._contains_potential_identifier(subject_id):
            raise ValueError("Subject ID may contain identifying information")
        
        return subject_id
    
    def _contains_potential_identifier(self, text: str) -> bool:
        """Check if text contains potential identifying information."""
        # Basic patterns for common identifiers
        patterns = [
            r'\b\d{3}-\d{2}-\d{4}\b',  # SSN pattern
            r'\b\d{10,}\b',  # Long numbers (potential phone/ID)
            r'\b[A-Za-z]+\s+[A-Za-z]+\b',  # Potential names (basic)
        ]
        
        for pattern in patterns:
            if re.search(pattern, text):
                return True
        
        return False


class VirusScanner:
    """Handles virus scanning of uploaded files."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self.enabled = config.virus_scan_enabled
        self._check_scanner_availability()
    
    def _check_scanner_availability(self):
        """Check if virus scanner is available."""
        if not self.enabled:
            return
        
        try:
            # Try to find ClamAV
            result = subprocess.run(['clamscan', '--version'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                self.scanner_type = 'clamav'
                logger.info("ClamAV virus scanner detected")
                return
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        
        # If no scanner found, disable scanning but log warning
        logger.warning("No virus scanner found. Virus scanning disabled.")
        self.enabled = False
        self.scanner_type = None
    
    def scan_file(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """
        Scan file for viruses.
        
        Returns:
            Tuple of (is_clean, threat_description)
        """
        if not self.enabled:
            return True, None
        
        try:
            if self.scanner_type == 'clamav':
                return self._scan_with_clamav(file_path)
            else:
                # Fallback to basic checks
                return self._basic_malware_check(file_path)
        except Exception as e:
            logger.error(f"Virus scan failed for {file_path}: {e}")
            # Fail secure - reject file if scan fails
            return False, f"Virus scan failed: {str(e)}"
    
    def _scan_with_clamav(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """Scan file using ClamAV."""
        try:
            result = subprocess.run(
                ['clamscan', '--no-summary', str(file_path)],
                capture_output=True, text=True, timeout=30
            )
            
            if result.returncode == 0:
                return True, None
            elif result.returncode == 1:
                # Virus found
                threat_info = result.stdout.strip()
                return False, f"Malware detected: {threat_info}"
            else:
                # Error occurred
                return False, f"Scanner error: {result.stderr.strip()}"
        
        except subprocess.TimeoutExpired:
            return False, "Virus scan timeout"
        except Exception as e:
            return False, f"Scanner error: {str(e)}"
    
    def _basic_malware_check(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """Basic malware checks without external scanner."""
        try:
            # Check file size (extremely large files might be suspicious)
            if file_path.stat().st_size > 100 * 1024 * 1024:  # 100MB
                return False, "File too large for security scan"
            
            # Check MIME type
            mime_type = magic.from_file(str(file_path), mime=True)
            if mime_type not in {'text/csv', 'text/plain', 'application/csv'}:
                return False, f"Unexpected MIME type: {mime_type}"
            
            # Basic content checks for CSV files
            if file_path.suffix.lower() == '.csv':
                return self._check_csv_content(file_path)
            
            return True, None
        
        except Exception as e:
            return False, f"Security check failed: {str(e)}"
    
    def _check_csv_content(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """Check CSV file content for suspicious patterns."""
        try:
            # Read first few KB to check for suspicious content
            with open(file_path, 'rb') as f:
                content = f.read(8192)  # Read first 8KB
            
            # Check for executable signatures
            executable_signatures = [
                b'MZ',  # PE executable
                b'\x7fELF',  # ELF executable
                b'\xca\xfe\xba\xbe',  # Mach-O
                b'PK',  # ZIP/JAR (could contain executables)
            ]
            
            for sig in executable_signatures:
                if content.startswith(sig):
                    return False, "File contains executable signature"
            
            # Check for script content in CSV
            text_content = content.decode('utf-8', errors='ignore').lower()
            script_patterns = ['<script', 'javascript:', 'vbscript:', 'powershell']
            
            for pattern in script_patterns:
                if pattern in text_content:
                    return False, f"Suspicious script content detected: {pattern}"
            
            return True, None
        
        except Exception as e:
            return False, f"Content check failed: {str(e)}"


class SecureFileHandler:
    """Handles secure file upload and processing."""
    
    def __init__(self, config: SecurityConfig, upload_dir: Path, temp_dir: Path):
        self.config = config
        self.upload_dir = upload_dir
        self.temp_dir = temp_dir
        self.sanitizer = InputSanitizer(config)
        self.virus_scanner = VirusScanner(config)
        
        # Ensure directories exist with proper permissions
        self._setup_directories()
    
    def _setup_directories(self):
        """Setup directories with secure permissions."""
        for directory in [self.upload_dir, self.temp_dir]:
            directory.mkdir(exist_ok=True, mode=0o750)  # rwxr-x---
    
    async def validate_and_save_file(self, file: UploadFile) -> Tuple[Path, Dict]:
        """
        Validate and securely save uploaded file.
        
        Returns:
            Tuple of (file_path, metadata)
        """
        threats = []
        metadata = {
            'original_filename': file.filename,
            'upload_timestamp': datetime.utcnow(),
            'file_size': 0,
            'mime_type': None,
            'sha256_hash': None,
        }
        
        try:
            # Validate filename
            if not file.filename:
                raise SecurityThreat(
                    ThreatType.INVALID_FORMAT,
                    SecurityLevel.HIGH,
                    "No filename provided"
                )
            
            sanitized_filename = self.sanitizer.sanitize_filename(file.filename)
            
            # Validate file extension
            if not self.sanitizer.validate_file_extension(sanitized_filename):
                raise SecurityThreat(
                    ThreatType.INVALID_FORMAT,
                    SecurityLevel.HIGH,
                    f"File extension not allowed: {Path(sanitized_filename).suffix}"
                )
            
            # Create temporary file for processing
            temp_file = self.temp_dir / f"upload_{int(time.time())}_{sanitized_filename}"
            
            # Read and validate file content
            content = await file.read()
            file_size = len(content)
            
            # Check file size
            if file_size > self.config.max_file_size:
                raise SecurityThreat(
                    ThreatType.OVERSIZED_FILE,
                    SecurityLevel.MEDIUM,
                    f"File too large: {file_size} bytes (max: {self.config.max_file_size})"
                )
            
            # Calculate hash
            sha256_hash = hashlib.sha256(content).hexdigest()
            metadata['file_size'] = file_size
            metadata['sha256_hash'] = sha256_hash
            
            # Write to temporary file
            with open(temp_file, 'wb') as f:
                f.write(content)
            
            # Validate MIME type
            mime_type = magic.from_file(str(temp_file), mime=True)
            metadata['mime_type'] = mime_type
            
            if mime_type not in self.config.allowed_mime_types:
                raise SecurityThreat(
                    ThreatType.INVALID_FORMAT,
                    SecurityLevel.HIGH,
                    f"MIME type not allowed: {mime_type}"
                )
            
            # Virus scan
            is_clean, threat_desc = self.virus_scanner.scan_file(temp_file)
            if not is_clean:
                raise SecurityThreat(
                    ThreatType.MALWARE,
                    SecurityLevel.CRITICAL,
                    threat_desc or "Malware detected"
                )
            
            # Additional content validation for CSV files
            if temp_file.suffix.lower() == '.csv':
                self._validate_csv_content(temp_file)
            
            # Move to final location
            final_path = self.upload_dir / f"{sha256_hash}_{sanitized_filename}"
            shutil.move(str(temp_file), str(final_path))
            
            # Set secure permissions
            os.chmod(final_path, 0o640)  # rw-r-----
            
            logger.info(f"File uploaded successfully: {final_path}")
            return final_path, metadata
        
        except SecurityThreat as threat:
            # Clean up temporary file
            if 'temp_file' in locals() and temp_file.exists():
                temp_file.unlink()
            
            logger.warning(f"Security threat detected: {threat.description}")
            raise HTTPException(
                status_code=400,
                detail=f"Security validation failed: {threat.description}"
            )
        
        except Exception as e:
            # Clean up temporary file
            if 'temp_file' in locals() and temp_file.exists():
                temp_file.unlink()
            
            logger.error(f"File upload failed: {e}")
            raise HTTPException(
                status_code=500,
                detail="File upload failed due to security validation error"
            )
    
    def _validate_csv_content(self, file_path: Path):
        """Validate CSV file content for security issues."""
        try:
            # Try to read as CSV to validate format
            df = pd.read_csv(file_path, nrows=10)  # Read first 10 rows for validation
            
            # Check for suspicious column names
            suspicious_columns = ['password', 'ssn', 'social_security', 'credit_card']
            for col in df.columns:
                if any(sus in col.lower() for sus in suspicious_columns):
                    raise SecurityThreat(
                        ThreatType.DATA_LEAK,
                        SecurityLevel.HIGH,
                        f"Potentially sensitive column detected: {col}"
                    )
            
            # Check for potential identifiers in data
            for col in df.columns:
                if df[col].dtype == 'object':  # String columns
                    sample_values = df[col].dropna().head(5).astype(str)
                    for value in sample_values:
                        if self.sanitizer._contains_potential_identifier(value):
                            logger.warning(f"Potential identifier in column {col}: {value[:10]}...")
        
        except pd.errors.EmptyDataError:
            raise SecurityThreat(
                ThreatType.INVALID_FORMAT,
                SecurityLevel.MEDIUM,
                "CSV file is empty"
            )
        except pd.errors.ParserError as e:
            raise SecurityThreat(
                ThreatType.INVALID_FORMAT,
                SecurityLevel.MEDIUM,
                f"Invalid CSV format: {str(e)}"
            )


class DataRetentionManager:
    """Manages data retention policies and automatic cleanup."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self.cleanup_thread = None
        self.running = False
    
    def start_cleanup_service(self):
        """Start the automatic cleanup service."""
        if self.cleanup_thread and self.cleanup_thread.is_alive():
            return
        
        self.running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        logger.info("Data retention cleanup service started")
    
    def stop_cleanup_service(self):
        """Stop the automatic cleanup service."""
        self.running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        logger.info("Data retention cleanup service stopped")
    
    def _cleanup_loop(self):
        """Main cleanup loop."""
        while self.running:
            try:
                self.cleanup_expired_data()
                # Sleep for the configured interval
                time.sleep(self.config.cleanup_interval_hours * 3600)
            except Exception as e:
                logger.error(f"Cleanup service error: {e}")
                time.sleep(300)  # Wait 5 minutes before retrying
    
    def cleanup_expired_data(self) -> Dict[str, int]:
        """Clean up expired data based on retention policy."""
        cleanup_stats = {
            'files_deleted': 0,
            'directories_cleaned': 0,
            'bytes_freed': 0,
            'errors': 0
        }
        
        cutoff_time = datetime.utcnow() - timedelta(days=self.config.data_retention_days)
        
        # Directories to clean
        directories_to_clean = [
            Path('age_normed_mriqc_dashboard/data/uploads'),
            Path('age_normed_mriqc_dashboard/data/temp'),
            Path('age_normed_mriqc_dashboard/data/watch'),
        ]
        
        for directory in directories_to_clean:
            if not directory.exists():
                continue
            
            try:
                stats = self._cleanup_directory(directory, cutoff_time)
                cleanup_stats['files_deleted'] += stats['files_deleted']
                cleanup_stats['bytes_freed'] += stats['bytes_freed']
                cleanup_stats['directories_cleaned'] += 1
            except Exception as e:
                logger.error(f"Failed to clean directory {directory}: {e}")
                cleanup_stats['errors'] += 1
        
        if cleanup_stats['files_deleted'] > 0:
            logger.info(f"Cleanup completed: {cleanup_stats}")
        
        return cleanup_stats
    
    def _cleanup_directory(self, directory: Path, cutoff_time: datetime) -> Dict[str, int]:
        """Clean up files in a specific directory."""
        stats = {'files_deleted': 0, 'bytes_freed': 0}
        
        for file_path in directory.iterdir():
            if not file_path.is_file():
                continue
            
            try:
                # Check file modification time
                mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if mtime < cutoff_time:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    stats['files_deleted'] += 1
                    stats['bytes_freed'] += file_size
                    logger.debug(f"Deleted expired file: {file_path}")
            
            except Exception as e:
                logger.error(f"Failed to delete file {file_path}: {e}")
        
        return stats
    
    def force_cleanup_file(self, file_path: Path) -> bool:
        """Force cleanup of a specific file."""
        try:
            if file_path.exists():
                file_path.unlink()
                logger.info(f"Force deleted file: {file_path}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to force delete file {file_path}: {e}")
            return False


class SecurityAuditor:
    """Handles security audit logging and monitoring."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self.enabled = config.enable_audit_logging
        self.audit_log_path = Path('age_normed_mriqc_dashboard/security_audit.log')
        self._setup_audit_logging()
    
    def _setup_audit_logging(self):
        """Setup security audit logging."""
        if not self.enabled:
            return
        
        # Create audit log file if it doesn't exist
        self.audit_log_path.parent.mkdir(exist_ok=True)
        self.audit_log_path.touch(exist_ok=True)
        
        # Set secure permissions
        os.chmod(self.audit_log_path, 0o640)
    
    def log_security_event(self, event_type: str, details: Dict, severity: SecurityLevel = SecurityLevel.MEDIUM):
        """Log a security event."""
        if not self.enabled:
            return
        
        event = {
            'timestamp': datetime.utcnow().isoformat(),
            'event_type': event_type,
            'severity': severity.value,
            'details': details,
            'process_id': os.getpid(),
        }
        
        try:
            with open(self.audit_log_path, 'a') as f:
                f.write(f"{event}\n")
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")
    
    def log_file_upload(self, filename: str, file_size: int, client_ip: str, success: bool):
        """Log file upload event."""
        self.log_security_event(
            'file_upload',
            {
                'filename': filename,
                'file_size': file_size,
                'client_ip': client_ip,
                'success': success,
            },
            SecurityLevel.MEDIUM if success else SecurityLevel.HIGH
        )
    
    def log_threat_detected(self, threat: SecurityThreat, client_ip: str):
        """Log detected security threat."""
        self.log_security_event(
            'threat_detected',
            {
                'threat_type': threat.threat_type.value,
                'description': threat.description,
                'file_path': threat.file_path,
                'client_ip': client_ip,
            },
            threat.severity
        )
    
    def log_data_access(self, resource: str, client_ip: str, user_agent: str):
        """Log data access event."""
        self.log_security_event(
            'data_access',
            {
                'resource': resource,
                'client_ip': client_ip,
                'user_agent': user_agent,
            },
            SecurityLevel.LOW
        )


# Global security instances
security_config = SecurityConfig()
data_retention_manager = DataRetentionManager(security_config)
security_auditor = SecurityAuditor(security_config)

# Start cleanup service
data_retention_manager.start_cleanup_service()