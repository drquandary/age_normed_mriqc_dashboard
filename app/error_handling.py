"""
Comprehensive error handling and logging system for Age-Normed MRIQC Dashboard.

This module provides structured error responses, comprehensive logging,
and audit trail functionality for quality control decisions.
"""

import logging
import traceback
import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from enum import Enum

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .models import ProcessingError, ValidationError
from .config import PROJECT_ROOT


class ErrorSeverity(str, Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(str, Enum):
    """Error categories for classification."""
    VALIDATION = "validation"
    PROCESSING = "processing"
    SYSTEM = "system"
    SECURITY = "security"
    BUSINESS_LOGIC = "business_logic"
    EXTERNAL_SERVICE = "external_service"


class LogLevel(str, Enum):
    """Logging levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ErrorResponse(BaseModel):
    """Structured error response model."""
    
    error_id: str = Field(..., description="Unique error identifier")
    error_type: str = Field(..., description="Type of error")
    error_category: ErrorCategory = Field(..., description="Error category")
    severity: ErrorSeverity = Field(..., description="Error severity")
    message: str = Field(..., description="Human-readable error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    suggestions: List[str] = Field(default_factory=list, description="Suggested solutions")
    error_code: str = Field(..., description="Unique error code")
    timestamp: datetime = Field(default_factory=datetime.now, description="When error occurred")
    request_id: Optional[str] = Field(None, description="Request identifier")
    user_id: Optional[str] = Field(None, description="User identifier")
    context: Optional[Dict[str, Any]] = Field(None, description="Error context information")
    recovery_options: List[str] = Field(default_factory=list, description="Recovery options")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class AuditLogEntry(BaseModel):
    """Audit log entry for quality control decisions."""
    
    audit_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique audit ID")
    timestamp: datetime = Field(default_factory=datetime.now, description="When action occurred")
    user_id: Optional[str] = Field(None, description="User who performed action")
    action_type: str = Field(..., description="Type of action performed")
    subject_id: Optional[str] = Field(None, description="Subject affected by action")
    resource_type: str = Field(..., description="Type of resource affected")
    resource_id: Optional[str] = Field(None, description="ID of affected resource")
    old_values: Optional[Dict[str, Any]] = Field(None, description="Previous values")
    new_values: Optional[Dict[str, Any]] = Field(None, description="New values")
    reason: Optional[str] = Field(None, description="Reason for action")
    ip_address: Optional[str] = Field(None, description="Client IP address")
    user_agent: Optional[str] = Field(None, description="Client user agent")
    session_id: Optional[str] = Field(None, description="Session identifier")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class LoggingConfig:
    """Centralized logging configuration."""
    
    def __init__(self, log_dir: Optional[Path] = None):
        self.log_dir = log_dir or PROJECT_ROOT / "logs"
        self.log_dir.mkdir(exist_ok=True)
        
        # Configure logging
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup comprehensive logging configuration."""
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        
        json_formatter = JsonFormatter()
        
        # Setup root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(console_handler)
        
        # Application log file
        app_handler = logging.FileHandler(self.log_dir / "application.log")
        app_handler.setLevel(logging.INFO)
        app_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(app_handler)
        
        # Error log file
        error_handler = logging.FileHandler(self.log_dir / "errors.log")
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(json_formatter)
        root_logger.addHandler(error_handler)
        
        # Audit log file
        audit_handler = logging.FileHandler(self.log_dir / "audit.log")
        audit_handler.setLevel(logging.INFO)
        audit_handler.setFormatter(json_formatter)
        
        # Create audit logger
        audit_logger = logging.getLogger("audit")
        audit_logger.setLevel(logging.INFO)
        audit_logger.addHandler(audit_handler)
        audit_logger.propagate = False
        
        # Quality control decisions log
        qc_handler = logging.FileHandler(self.log_dir / "quality_control.log")
        qc_handler.setLevel(logging.INFO)
        qc_handler.setFormatter(json_formatter)
        
        qc_logger = logging.getLogger("quality_control")
        qc_logger.setLevel(logging.INFO)
        qc_logger.addHandler(qc_handler)
        qc_logger.propagate = False


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record):
        """Format log record as JSON."""
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'message': record.getMessage(),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
                          'filename', 'module', 'lineno', 'funcName', 'created',
                          'msecs', 'relativeCreated', 'thread', 'threadName',
                          'processName', 'process', 'getMessage', 'exc_info',
                          'exc_text', 'stack_info']:
                log_entry[key] = value
        
        return json.dumps(log_entry, default=str)


class ErrorHandler:
    """Centralized error handling system."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.audit_logger = logging.getLogger("audit")
        self.qc_logger = logging.getLogger("quality_control")
        
        # Error code mappings
        self.error_codes = {
            # Validation errors (1000-1999)
            "VALIDATION_FAILED": "1001",
            "INVALID_FILE_FORMAT": "1002",
            "MISSING_REQUIRED_FIELD": "1003",
            "INVALID_DATA_TYPE": "1004",
            "VALUE_OUT_OF_RANGE": "1005",
            "INCONSISTENT_DATA": "1006",
            
            # Processing errors (2000-2999)
            "PROCESSING_FAILED": "2001",
            "FILE_PROCESSING_ERROR": "2002",
            "NORMALIZATION_ERROR": "2003",
            "QUALITY_ASSESSMENT_ERROR": "2004",
            "BATCH_PROCESSING_ERROR": "2005",
            "EXPORT_ERROR": "2006",
            
            # System errors (3000-3999)
            "DATABASE_ERROR": "3001",
            "REDIS_CONNECTION_ERROR": "3002",
            "FILE_SYSTEM_ERROR": "3003",
            "MEMORY_ERROR": "3004",
            "TIMEOUT_ERROR": "3005",
            
            # Security errors (4000-4999)
            "AUTHENTICATION_FAILED": "4001",
            "AUTHORIZATION_FAILED": "4002",
            "INVALID_TOKEN": "4003",
            "SUSPICIOUS_ACTIVITY": "4004",
            "DATA_BREACH_ATTEMPT": "4005",
            
            # Business logic errors (5000-5999)
            "THRESHOLD_VIOLATION": "5001",
            "AGE_GROUP_ASSIGNMENT_ERROR": "5002",
            "NORMATIVE_DATA_MISSING": "5003",
            "CONFIGURATION_ERROR": "5004",
            
            # External service errors (6000-6999)
            "EXTERNAL_API_ERROR": "6001",
            "NETWORK_ERROR": "6002",
            "SERVICE_UNAVAILABLE": "6003",
        }
        
        # Error suggestions
        self.error_suggestions = {
            "1001": [
                "Check input data format and types",
                "Verify all required fields are present",
                "Review validation rules documentation"
            ],
            "1002": [
                "Ensure file is in CSV format",
                "Check file encoding (UTF-8 recommended)",
                "Verify MRIQC output format compatibility"
            ],
            "2001": [
                "Check input data quality",
                "Verify system resources availability",
                "Review processing logs for details"
            ],
            "3001": [
                "Check database connection",
                "Verify database schema",
                "Contact system administrator"
            ],
            "4001": [
                "Verify credentials",
                "Check authentication configuration",
                "Contact administrator if issue persists"
            ],
            "5001": [
                "Review quality thresholds",
                "Consider manual review",
                "Check age-specific normative data"
            ]
        }
        
        # Recovery options
        self.recovery_options = {
            "1002": [
                "Convert file to CSV format",
                "Re-export from MRIQC with correct settings",
                "Use file format conversion tool"
            ],
            "2001": [
                "Retry processing with different parameters",
                "Process file individually instead of batch",
                "Contact support with error details"
            ],
            "3001": [
                "Retry operation after brief delay",
                "Check database service status",
                "Use cached data if available"
            ]
        }
    
    def create_error_response(
        self,
        error_type: str,
        message: str,
        category: ErrorCategory = ErrorCategory.SYSTEM,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        details: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None,
        user_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> ErrorResponse:
        """Create structured error response."""
        
        error_id = str(uuid.uuid4())
        error_code = self.error_codes.get(error_type, "9999")
        
        suggestions = self.error_suggestions.get(error_code, [
            "Check system logs for more details",
            "Contact support if issue persists"
        ])
        
        recovery_options = self.recovery_options.get(error_code, [
            "Retry the operation",
            "Check input data and try again"
        ])
        
        error_response = ErrorResponse(
            error_id=error_id,
            error_type=error_type,
            error_category=category,
            severity=severity,
            message=message,
            details=details,
            suggestions=suggestions,
            error_code=error_code,
            request_id=request_id,
            user_id=user_id,
            context=context,
            recovery_options=recovery_options
        )
        
        # Log the error
        self._log_error(error_response)
        
        return error_response
    
    def _log_error(self, error_response: ErrorResponse):
        """Log error with appropriate level."""
        
        log_data = {
            'error_id': error_response.error_id,
            'error_type': error_response.error_type,
            'error_code': error_response.error_code,
            'category': error_response.error_category,
            'severity': error_response.severity,
            'error_message': error_response.message,  # Changed from 'message' to avoid conflict
            'details': error_response.details,
            'request_id': error_response.request_id,
            'user_id': error_response.user_id,
            'context': error_response.context
        }
        
        if error_response.severity == ErrorSeverity.CRITICAL:
            self.logger.critical("Critical error occurred", extra=log_data)
        elif error_response.severity == ErrorSeverity.HIGH:
            self.logger.error("High severity error occurred", extra=log_data)
        elif error_response.severity == ErrorSeverity.MEDIUM:
            self.logger.warning("Medium severity error occurred", extra=log_data)
        else:
            self.logger.info("Low severity error occurred", extra=log_data)
    
    def handle_validation_error(
        self,
        field: str,
        message: str,
        invalid_value: Any = None,
        expected_type: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> ErrorResponse:
        """Handle validation errors."""
        
        details = {
            'field': field,
            'invalid_value': invalid_value,
            'expected_type': expected_type
        }
        
        return self.create_error_response(
            error_type="VALIDATION_FAILED",
            message=f"Validation failed for field '{field}': {message}",
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.MEDIUM,
            details=details,
            request_id=request_id
        )
    
    def handle_processing_error(
        self,
        operation: str,
        message: str,
        exception: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None
    ) -> ErrorResponse:
        """Handle processing errors."""
        
        details = {
            'operation': operation,
            'exception_type': type(exception).__name__ if exception else None,
            'exception_message': str(exception) if exception else None
        }
        
        if exception:
            details['traceback'] = traceback.format_exc()
        
        return self.create_error_response(
            error_type="PROCESSING_FAILED",
            message=f"Processing failed for operation '{operation}': {message}",
            category=ErrorCategory.PROCESSING,
            severity=ErrorSeverity.HIGH,
            details=details,
            context=context,
            request_id=request_id
        )
    
    def handle_system_error(
        self,
        component: str,
        message: str,
        exception: Optional[Exception] = None,
        request_id: Optional[str] = None
    ) -> ErrorResponse:
        """Handle system errors."""
        
        details = {
            'component': component,
            'exception_type': type(exception).__name__ if exception else None,
            'exception_message': str(exception) if exception else None
        }
        
        if exception:
            details['traceback'] = traceback.format_exc()
        
        return self.create_error_response(
            error_type="SYSTEM_ERROR",
            message=f"System error in component '{component}': {message}",
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.CRITICAL,
            details=details,
            request_id=request_id
        )


class AuditLogger:
    """Audit logging for quality control decisions and user actions."""
    
    def __init__(self):
        self.logger = logging.getLogger("audit")
        self.qc_logger = logging.getLogger("quality_control")
    
    def log_quality_decision(
        self,
        subject_id: str,
        decision: str,
        reason: str,
        user_id: Optional[str] = None,
        automated: bool = True,
        confidence: Optional[float] = None,
        metrics: Optional[Dict[str, Any]] = None,
        thresholds: Optional[Dict[str, Any]] = None
    ):
        """Log quality control decisions."""
        
        entry = {
            'audit_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'action_type': 'quality_decision',
            'subject_id': subject_id,
            'decision': decision,
            'reason': reason,
            'user_id': user_id,
            'automated': automated,
            'confidence': confidence,
            'metrics': metrics,
            'thresholds': thresholds
        }
        
        self.qc_logger.info("Quality control decision made", extra=entry)
    
    def log_user_action(
        self,
        action_type: str,
        resource_type: str,
        resource_id: Optional[str] = None,
        user_id: Optional[str] = None,
        old_values: Optional[Dict[str, Any]] = None,
        new_values: Optional[Dict[str, Any]] = None,
        reason: Optional[str] = None,
        request: Optional[Request] = None
    ):
        """Log user actions for audit trail."""
        
        entry = AuditLogEntry(
            action_type=action_type,
            resource_type=resource_type,
            resource_id=resource_id,
            user_id=user_id,
            old_values=old_values,
            new_values=new_values,
            reason=reason
        )
        
        if request:
            entry.ip_address = request.client.host if request.client else None
            entry.user_agent = request.headers.get("user-agent")
            # Add session ID if available
            entry.session_id = request.headers.get("x-session-id")
        
        self.logger.info("User action logged", extra=entry.model_dump())
    
    def log_data_access(
        self,
        resource_type: str,
        resource_id: str,
        access_type: str,
        user_id: Optional[str] = None,
        request: Optional[Request] = None
    ):
        """Log data access for compliance."""
        
        entry = {
            'audit_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'action_type': 'data_access',
            'resource_type': resource_type,
            'resource_id': resource_id,
            'access_type': access_type,
            'user_id': user_id,
            'ip_address': request.client.host if request and request.client else None,
            'user_agent': request.headers.get("user-agent") if request else None
        }
        
        self.logger.info("Data access logged", extra=entry)
    
    def log_configuration_change(
        self,
        config_type: str,
        old_config: Dict[str, Any],
        new_config: Dict[str, Any],
        user_id: Optional[str] = None,
        reason: Optional[str] = None
    ):
        """Log configuration changes."""
        
        entry = {
            'audit_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'action_type': 'configuration_change',
            'resource_type': 'configuration',
            'resource_id': config_type,
            'old_values': old_config,
            'new_values': new_config,
            'user_id': user_id,
            'reason': reason
        }
        
        self.logger.info("Configuration changed", extra=entry)


# Global instances
error_handler = ErrorHandler()
audit_logger = AuditLogger()


def setup_logging():
    """Setup logging configuration."""
    logging_config = LoggingConfig()
    return logging_config


async def error_handler_middleware(request: Request, call_next):
    """Middleware for handling errors and logging requests."""
    
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    # Log request
    logger = logging.getLogger(__name__)
    logger.info(
        f"Request started: {request.method} {request.url}",
        extra={
            'request_id': request_id,
            'method': request.method,
            'url': str(request.url),
            'client_ip': request.client.host if request.client else None,
            'user_agent': request.headers.get("user-agent")
        }
    )
    
    try:
        response = await call_next(request)
        
        # Log successful response
        logger.info(
            f"Request completed: {response.status_code}",
            extra={
                'request_id': request_id,
                'status_code': response.status_code
            }
        )
        
        return response
        
    except HTTPException as e:
        # Handle HTTP exceptions
        error_response = error_handler.create_error_response(
            error_type="HTTP_ERROR",
            message=e.detail,
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.MEDIUM,
            request_id=request_id,
            details={'status_code': e.status_code}
        )
        
        return JSONResponse(
            status_code=e.status_code,
            content=error_response.dict()
        )
        
    except Exception as e:
        # Handle unexpected exceptions
        error_response = error_handler.handle_system_error(
            component="request_handler",
            message="Unexpected error occurred",
            exception=e,
            request_id=request_id
        )
        
        return JSONResponse(
            status_code=500,
            content=error_response.dict()
        )


def get_request_id(request: Request) -> Optional[str]:
    """Get request ID from request state."""
    return getattr(request.state, 'request_id', None)