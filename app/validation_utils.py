"""
Validation utilities for the Age-Normed MRIQC Dashboard.

This module provides helper functions for common validation tasks
and error message generation.
"""

from typing import Dict, List, Any, Optional, Tuple
import re
from datetime import datetime

from .models import (
    MRIQCMetrics, SubjectInfo, QualityStatus, AgeGroup,
    ValidationError as CustomValidationError
)


class ValidationUtils:
    """Utility class for common validation operations."""
    
    # Common MRIQC metric ranges for validation
    METRIC_RANGES = {
        'snr': (0, 1000),
        'cnr': (0, 100),
        'fber': (0, 100000),
        'efc': (0, 1),
        'fwhm_avg': (0, 20),
        'fwhm_x': (0, 20),
        'fwhm_y': (0, 20),
        'fwhm_z': (0, 20),
        'qi1': (0, 1),
        'qi2': (0, 1),
        'cjv': (0, 10),
        'wm2max': (0, 1),
        'dvars': (0, 1000),
        'fd_mean': (0, 10),
        'fd_perc': (0, 100),
        'gcor': (-1, 1),
        'outlier_fraction': (0, 1)
    }
    
    # PII patterns to detect in subject IDs
    PII_PATTERNS = {
        'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
        'date': r'\b\d{2}[/-]\d{2}[/-]\d{4}\b',
        'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
        'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    }
    
    @classmethod
    def validate_metric_range(cls, metric_name: str, value: float) -> bool:
        """
        Validate that a metric value is within expected range.
        
        Args:
            metric_name: Name of the MRIQC metric
            value: Value to validate
            
        Returns:
            True if value is within range, False otherwise
        """
        if metric_name not in cls.METRIC_RANGES:
            return True  # Unknown metric, assume valid
        
        min_val, max_val = cls.METRIC_RANGES[metric_name]
        return min_val <= value <= max_val
    
    @classmethod
    def check_for_pii(cls, text: str) -> List[str]:
        """
        Check text for potential personally identifiable information.
        
        Args:
            text: Text to check for PII
            
        Returns:
            List of PII types found
        """
        found_pii = []
        for pii_type, pattern in cls.PII_PATTERNS.items():
            if re.search(pattern, text, re.IGNORECASE):
                found_pii.append(pii_type)
        return found_pii
    
    @classmethod
    def validate_subject_id_format(cls, subject_id: str) -> Tuple[bool, List[str]]:
        """
        Validate subject ID format and check for PII.
        
        Args:
            subject_id: Subject identifier to validate
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        # Check basic format
        if not re.match(r'^[a-zA-Z0-9_-]+$', subject_id):
            issues.append("Subject ID contains invalid characters")
        
        # Check length
        if len(subject_id) < 1:
            issues.append("Subject ID is too short")
        elif len(subject_id) > 50:
            issues.append("Subject ID is too long")
        
        # Check for PII
        pii_found = cls.check_for_pii(subject_id)
        if pii_found:
            issues.extend([f"Subject ID appears to contain {pii}" for pii in pii_found])
        
        return len(issues) == 0, issues
    
    @classmethod
    def validate_age_reasonableness(cls, age: float) -> Tuple[bool, Optional[str]]:
        """
        Validate that age is reasonable for neuroimaging studies.
        
        Args:
            age: Age in years
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if age < 0:
            return False, "Age cannot be negative"
        elif age > 120:
            return False, "Age is unreasonably high for neuroimaging"
        elif age < 0.1:
            return False, "Age is too low for typical neuroimaging studies"
        elif age > 110:
            return False, "Age is very high, please verify"
        
        return True, None
    
    @classmethod
    def determine_age_group(cls, age: float) -> AgeGroup:
        """
        Determine age group based on age in years.
        
        Args:
            age: Age in years
            
        Returns:
            Appropriate AgeGroup enum value
        """
        if age < 13:
            return AgeGroup.PEDIATRIC
        elif age < 18:
            return AgeGroup.ADOLESCENT
        elif age < 36:
            return AgeGroup.YOUNG_ADULT
        elif age < 66:
            return AgeGroup.MIDDLE_AGE
        else:
            return AgeGroup.ELDERLY
    
    @classmethod
    def validate_mriqc_metrics_consistency(cls, metrics: MRIQCMetrics) -> List[str]:
        """
        Validate consistency between related MRIQC metrics.
        
        Args:
            metrics: MRIQCMetrics instance to validate
            
        Returns:
            List of consistency issues found
        """
        issues = []
        
        # Check FWHM consistency
        fwhm_components = [metrics.fwhm_x, metrics.fwhm_y, metrics.fwhm_z]
        if all(x is not None for x in fwhm_components) and metrics.fwhm_avg is not None:
            calculated_avg = sum(fwhm_components) / 3
            if abs(calculated_avg - metrics.fwhm_avg) > 0.5:
                issues.append(
                    f"FWHM average ({metrics.fwhm_avg:.2f}) inconsistent with "
                    f"component average ({calculated_avg:.2f})"
                )
        
        # Check framewise displacement consistency
        if (metrics.fd_num is not None and metrics.fd_perc is not None and 
            metrics.fd_num == 0 and metrics.fd_perc > 0):
            issues.append(
                "Inconsistent framewise displacement: 0 high motion timepoints "
                f"but {metrics.fd_perc}% high motion percentage"
            )
        
        # Check SNR and CNR relationship (SNR should generally be higher)
        if (metrics.snr is not None and metrics.cnr is not None and 
            metrics.cnr > metrics.snr):
            issues.append(
                f"CNR ({metrics.cnr:.2f}) is higher than SNR ({metrics.snr:.2f}), "
                "which is unusual"
            )
        
        return issues
    
    @classmethod
    def generate_validation_report(cls, 
                                 subject_info: SubjectInfo, 
                                 metrics: MRIQCMetrics) -> Dict[str, Any]:
        """
        Generate a comprehensive validation report for subject and metrics.
        
        Args:
            subject_info: Subject information to validate
            metrics: MRIQC metrics to validate
            
        Returns:
            Dictionary containing validation results
        """
        report = {
            'is_valid': True,
            'warnings': [],
            'errors': [],
            'subject_validation': {},
            'metrics_validation': {}
        }
        
        # Validate subject info
        id_valid, id_issues = cls.validate_subject_id_format(subject_info.subject_id)
        report['subject_validation']['subject_id'] = {
            'valid': id_valid,
            'issues': id_issues
        }
        
        if not id_valid:
            report['is_valid'] = False
            report['errors'].extend(id_issues)
        
        # Validate age if provided
        if subject_info.age is not None:
            age_valid, age_error = cls.validate_age_reasonableness(subject_info.age)
            report['subject_validation']['age'] = {
                'valid': age_valid,
                'error': age_error,
                'age_group': cls.determine_age_group(subject_info.age).value
            }
            
            if not age_valid:
                report['is_valid'] = False
                report['errors'].append(age_error)
        
        # Validate metrics
        metric_issues = cls.validate_mriqc_metrics_consistency(metrics)
        report['metrics_validation']['consistency'] = {
            'issues': metric_issues
        }
        
        if metric_issues:
            report['warnings'].extend(metric_issues)
        
        # Check individual metric ranges
        range_issues = []
        for field_name, value in metrics.model_dump().items():
            if value is not None and not cls.validate_metric_range(field_name, value):
                min_val, max_val = cls.METRIC_RANGES.get(field_name, (None, None))
                range_issues.append(
                    f"{field_name} value {value} outside expected range "
                    f"({min_val}-{max_val})"
                )
        
        report['metrics_validation']['ranges'] = {
            'issues': range_issues
        }
        
        if range_issues:
            report['warnings'].extend(range_issues)
        
        # Set overall validation timestamp
        report['validation_timestamp'] = datetime.now().isoformat()
        
        return report
    
    @classmethod
    def create_validation_error(cls, 
                              field: str, 
                              message: str, 
                              invalid_value: Any = None,
                              expected_type: str = None) -> CustomValidationError:
        """
        Create a standardized validation error.
        
        Args:
            field: Field name that failed validation
            message: Error message
            invalid_value: The invalid value (optional)
            expected_type: Expected data type (optional)
            
        Returns:
            CustomValidationError instance
        """
        return CustomValidationError(
            field=field,
            message=message,
            invalid_value=invalid_value,
            expected_type=expected_type
        )


# Convenience functions for common validations

def validate_subject_data(subject_info: SubjectInfo, 
                         metrics: MRIQCMetrics) -> Dict[str, Any]:
    """
    Convenience function to validate complete subject data.
    
    Args:
        subject_info: Subject information
        metrics: MRIQC metrics
        
    Returns:
        Validation report dictionary
    """
    return ValidationUtils.generate_validation_report(subject_info, metrics)


def is_valid_subject_id(subject_id: str) -> bool:
    """
    Quick check if subject ID is valid.
    
    Args:
        subject_id: Subject identifier
        
    Returns:
        True if valid, False otherwise
    """
    valid, _ = ValidationUtils.validate_subject_id_format(subject_id)
    return valid


def get_age_group(age: float) -> AgeGroup:
    """
    Get age group for given age.
    
    Args:
        age: Age in years
        
    Returns:
        AgeGroup enum value
    """
    return ValidationUtils.determine_age_group(age)