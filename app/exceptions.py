"""
Custom exception classes for Age-Normed MRIQC Dashboard.

This module defines specific exception types for different error scenarios
with structured error information and recovery suggestions.
"""

from typing import Dict, List, Optional, Any
from .error_handling import ErrorCategory, ErrorSeverity


class MRIQCDashboardException(Exception):
    """Base exception class for MRIQC Dashboard errors."""
    
    def __init__(
        self,
        message: str,
        error_code: str,
        category: ErrorCategory = ErrorCategory.SYSTEM,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        details: Optional[Dict[str, Any]] = None,
        suggestions: Optional[List[str]] = None,
        recovery_options: Optional[List[str]] = None
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.category = category
        self.severity = severity
        self.details = details or {}
        self.suggestions = suggestions or []
        self.recovery_options = recovery_options or []


class ValidationException(MRIQCDashboardException):
    """Exception for data validation errors."""
    
    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        invalid_value: Any = None,
        expected_type: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'field': field,
            'invalid_value': invalid_value,
            'expected_type': expected_type
        })
        
        suggestions = kwargs.get('suggestions', [
            "Check input data format and types",
            "Verify all required fields are present",
            "Review validation rules documentation"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Correct the invalid data",
            "Check data format requirements",
            "Contact support for validation rules"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '1001'),
            category=ErrorCategory.VALIDATION,
            severity=kwargs.get('severity', ErrorSeverity.MEDIUM),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )


class FileProcessingException(MRIQCDashboardException):
    """Exception for file processing errors."""
    
    def __init__(
        self,
        message: str,
        file_path: Optional[str] = None,
        file_type: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'file_path': file_path,
            'file_type': file_type
        })
        
        suggestions = kwargs.get('suggestions', [
            "Check file format and encoding",
            "Verify file is not corrupted",
            "Ensure file permissions are correct"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Re-export file from source",
            "Check file format compatibility",
            "Try processing file individually"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '2002'),
            category=ErrorCategory.PROCESSING,
            severity=kwargs.get('severity', ErrorSeverity.HIGH),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )


class MRIQCProcessingException(FileProcessingException):
    """Exception for MRIQC-specific processing errors."""
    
    def __init__(
        self,
        message: str,
        missing_columns: Optional[List[str]] = None,
        invalid_metrics: Optional[List[str]] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'missing_columns': missing_columns,
            'invalid_metrics': invalid_metrics
        })
        
        suggestions = kwargs.get('suggestions', [
            "Verify MRIQC output format",
            "Check MRIQC version compatibility",
            "Ensure all required metrics are present"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Re-run MRIQC with correct parameters",
            "Check MRIQC configuration",
            "Use compatible MRIQC version"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '1002'),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options,
            **{k: v for k, v in kwargs.items() if k not in ['details', 'suggestions', 'recovery_options']}
        )


class MRIQCValidationException(ValidationException):
    """Exception for MRIQC data validation errors."""
    
    def __init__(
        self,
        message: str,
        subject_id: Optional[str] = None,
        metric_name: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'subject_id': subject_id,
            'metric_name': metric_name
        })
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '1006'),
            **kwargs
        )


class NormalizationException(MRIQCDashboardException):
    """Exception for age normalization errors."""
    
    def __init__(
        self,
        message: str,
        age: Optional[float] = None,
        age_group: Optional[str] = None,
        metric_name: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'age': age,
            'age_group': age_group,
            'metric_name': metric_name
        })
        
        suggestions = kwargs.get('suggestions', [
            "Check age value is valid",
            "Verify normative data is available",
            "Consider using default adult norms"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Use adult normative data as fallback",
            "Check age group definitions",
            "Contact support for normative data"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '2003'),
            category=ErrorCategory.PROCESSING,
            severity=kwargs.get('severity', ErrorSeverity.MEDIUM),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )


class QualityAssessmentException(MRIQCDashboardException):
    """Exception for quality assessment errors."""
    
    def __init__(
        self,
        message: str,
        subject_id: Optional[str] = None,
        assessment_type: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'subject_id': subject_id,
            'assessment_type': assessment_type
        })
        
        suggestions = kwargs.get('suggestions', [
            "Check quality thresholds configuration",
            "Verify metric values are valid",
            "Review assessment algorithm"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Use manual quality assessment",
            "Adjust quality thresholds",
            "Skip automated assessment"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '2004'),
            category=ErrorCategory.BUSINESS_LOGIC,
            severity=kwargs.get('severity', ErrorSeverity.MEDIUM),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )


class DatabaseException(MRIQCDashboardException):
    """Exception for database-related errors."""
    
    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        table: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'operation': operation,
            'table': table
        })
        
        suggestions = kwargs.get('suggestions', [
            "Check database connection",
            "Verify database schema",
            "Check database permissions"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Retry operation after delay",
            "Check database service status",
            "Use cached data if available"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '3001'),
            category=ErrorCategory.SYSTEM,
            severity=kwargs.get('severity', ErrorSeverity.HIGH),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )


class ConfigurationException(MRIQCDashboardException):
    """Exception for configuration-related errors."""
    
    def __init__(
        self,
        message: str,
        config_type: Optional[str] = None,
        config_key: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'config_type': config_type,
            'config_key': config_key
        })
        
        suggestions = kwargs.get('suggestions', [
            "Check configuration file format",
            "Verify all required settings",
            "Review configuration documentation"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Use default configuration",
            "Restore from backup",
            "Recreate configuration file"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '5004'),
            category=ErrorCategory.BUSINESS_LOGIC,
            severity=kwargs.get('severity', ErrorSeverity.MEDIUM),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )


class BatchProcessingException(MRIQCDashboardException):
    """Exception for batch processing errors."""
    
    def __init__(
        self,
        message: str,
        batch_id: Optional[str] = None,
        failed_files: Optional[List[str]] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'batch_id': batch_id,
            'failed_files': failed_files,
            'failed_count': len(failed_files) if failed_files else 0
        })
        
        suggestions = kwargs.get('suggestions', [
            "Check individual file errors",
            "Verify batch size limits",
            "Review system resources"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Process files individually",
            "Reduce batch size",
            "Retry failed files only"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '2005'),
            category=ErrorCategory.PROCESSING,
            severity=kwargs.get('severity', ErrorSeverity.HIGH),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )


class ExportException(MRIQCDashboardException):
    """Exception for data export errors."""
    
    def __init__(
        self,
        message: str,
        export_format: Optional[str] = None,
        export_type: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'export_format': export_format,
            'export_type': export_type
        })
        
        suggestions = kwargs.get('suggestions', [
            "Check export format requirements",
            "Verify data is available for export",
            "Check file system permissions"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Try different export format",
            "Export subset of data",
            "Check available disk space"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '2006'),
            category=ErrorCategory.PROCESSING,
            severity=kwargs.get('severity', ErrorSeverity.MEDIUM),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )


class SecurityException(MRIQCDashboardException):
    """Exception for security-related errors."""
    
    def __init__(
        self,
        message: str,
        security_issue: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'security_issue': security_issue
        })
        
        suggestions = kwargs.get('suggestions', [
            "Review security policies",
            "Check authentication status",
            "Contact security administrator"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Re-authenticate",
            "Check permissions",
            "Contact administrator"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '4001'),
            category=ErrorCategory.SECURITY,
            severity=kwargs.get('severity', ErrorSeverity.HIGH),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )


class ExternalServiceException(MRIQCDashboardException):
    """Exception for external service errors."""
    
    def __init__(
        self,
        message: str,
        service_name: Optional[str] = None,
        status_code: Optional[int] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'service_name': service_name,
            'status_code': status_code
        })
        
        suggestions = kwargs.get('suggestions', [
            "Check service availability",
            "Verify network connectivity",
            "Review service configuration"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Retry after delay",
            "Use cached data",
            "Contact service provider"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '6001'),
            category=ErrorCategory.EXTERNAL_SERVICE,
            severity=kwargs.get('severity', ErrorSeverity.MEDIUM),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )

class WorkflowException(MRIQCDashboardException):
    """Exception for workflow execution errors."""
    
    def __init__(
        self,
        message: str,
        workflow_id: Optional[str] = None,
        workflow_step: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'workflow_id': workflow_id,
            'workflow_step': workflow_step
        })
        
        suggestions = kwargs.get('suggestions', [
            "Check workflow configuration",
            "Verify input data quality",
            "Review workflow step requirements"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Restart workflow from failed step",
            "Adjust workflow configuration",
            "Process data manually"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '7001'),
            category=ErrorCategory.PROCESSING,
            severity=kwargs.get('severity', ErrorSeverity.HIGH),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )


class IntegrationException(MRIQCDashboardException):
    """Exception for integration service errors."""
    
    def __init__(
        self,
        message: str,
        integration_id: Optional[str] = None,
        components_involved: Optional[List[str]] = None,
        **kwargs
    ):
        details = kwargs.get('details', {})
        details.update({
            'integration_id': integration_id,
            'components_involved': components_involved
        })
        
        suggestions = kwargs.get('suggestions', [
            "Check component compatibility",
            "Verify service dependencies",
            "Review integration configuration"
        ])
        
        recovery_options = kwargs.get('recovery_options', [
            "Restart integration services",
            "Check component status",
            "Use fallback processing"
        ])
        
        super().__init__(
            message=message,
            error_code=kwargs.get('error_code', '7002'),
            category=ErrorCategory.SYSTEM,
            severity=kwargs.get('severity', ErrorSeverity.HIGH),
            details=details,
            suggestions=suggestions,
            recovery_options=recovery_options
        )


class MRIQCProcessingError(MRIQCProcessingException):
    """Legacy alias for MRIQCProcessingException."""
    pass


class MRIQCValidationError(MRIQCValidationException):
    """Legacy alias for MRIQCValidationException."""
    pass