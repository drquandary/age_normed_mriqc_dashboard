"""
Unit tests for validation utilities.
"""

import pytest
from datetime import datetime

from app.validation_utils import (
    ValidationUtils, validate_subject_data, is_valid_subject_id, get_age_group
)
from app.models import SubjectInfo, MRIQCMetrics, ScanType, AgeGroup


class TestValidationUtils:
    """Test ValidationUtils class methods."""
    
    def test_validate_metric_range(self):
        """Test metric range validation."""
        # Valid ranges
        assert ValidationUtils.validate_metric_range('snr', 12.5) == True
        assert ValidationUtils.validate_metric_range('efc', 0.5) == True
        assert ValidationUtils.validate_metric_range('gcor', -0.5) == True
        
        # Invalid ranges
        assert ValidationUtils.validate_metric_range('snr', -1) == False
        assert ValidationUtils.validate_metric_range('efc', 1.5) == False
        assert ValidationUtils.validate_metric_range('gcor', 2.0) == False
        
        # Unknown metric (should pass)
        assert ValidationUtils.validate_metric_range('unknown_metric', 999) == True
    
    def test_check_for_pii(self):
        """Test PII detection."""
        # No PII
        assert ValidationUtils.check_for_pii('sub-001') == []
        
        # SSN pattern
        pii = ValidationUtils.check_for_pii('123-45-6789')
        assert 'ssn' in pii
        
        # Date pattern
        pii = ValidationUtils.check_for_pii('01-15-1990')
        assert 'date' in pii
        
        # Phone pattern
        pii = ValidationUtils.check_for_pii('555-123-4567')
        assert 'phone' in pii
        
        # Email pattern
        pii = ValidationUtils.check_for_pii('test@example.com')
        assert 'email' in pii
    
    def test_validate_subject_id_format(self):
        """Test subject ID format validation."""
        # Valid IDs
        valid, issues = ValidationUtils.validate_subject_id_format('sub-001')
        assert valid == True
        assert len(issues) == 0
        
        # Invalid characters
        valid, issues = ValidationUtils.validate_subject_id_format('sub@001')
        assert valid == False
        assert any('invalid characters' in issue for issue in issues)
        
        # Too short
        valid, issues = ValidationUtils.validate_subject_id_format('')
        assert valid == False
        assert any('too short' in issue for issue in issues)
        
        # Too long
        valid, issues = ValidationUtils.validate_subject_id_format('x' * 51)
        assert valid == False
        assert any('too long' in issue for issue in issues)
        
        # Contains PII
        valid, issues = ValidationUtils.validate_subject_id_format('123-45-6789')
        assert valid == False
        assert any('ssn' in issue.lower() for issue in issues)
    
    def test_validate_age_reasonableness(self):
        """Test age validation."""
        # Valid ages
        valid, error = ValidationUtils.validate_age_reasonableness(25.0)
        assert valid == True
        assert error is None
        
        # Negative age
        valid, error = ValidationUtils.validate_age_reasonableness(-1.0)
        assert valid == False
        assert 'negative' in error.lower()
        
        # Too high
        valid, error = ValidationUtils.validate_age_reasonableness(150.0)
        assert valid == False
        assert 'unreasonably high' in error.lower()
        
        # Very low
        valid, error = ValidationUtils.validate_age_reasonableness(0.05)
        assert valid == False
        assert 'too low' in error.lower()
    
    def test_determine_age_group(self):
        """Test age group determination."""
        assert ValidationUtils.determine_age_group(8.0) == AgeGroup.PEDIATRIC
        assert ValidationUtils.determine_age_group(15.0) == AgeGroup.ADOLESCENT
        assert ValidationUtils.determine_age_group(25.0) == AgeGroup.YOUNG_ADULT
        assert ValidationUtils.determine_age_group(45.0) == AgeGroup.MIDDLE_AGE
        assert ValidationUtils.determine_age_group(75.0) == AgeGroup.ELDERLY
    
    def test_validate_mriqc_metrics_consistency(self):
        """Test MRIQC metrics consistency validation."""
        # Consistent metrics
        metrics = MRIQCMetrics(
            fwhm_x=2.5,
            fwhm_y=2.7,
            fwhm_z=2.9,
            fwhm_avg=2.7,
            fd_num=5,
            fd_perc=10.0,
            snr=12.0,
            cnr=3.0
        )
        issues = ValidationUtils.validate_mriqc_metrics_consistency(metrics)
        assert len(issues) == 0
        
        # Test with manually created metrics to bypass Pydantic validation
        # Create a metrics object and manually set inconsistent values
        metrics = MRIQCMetrics()
        metrics.fwhm_x = 2.0
        metrics.fwhm_y = 2.0
        metrics.fwhm_z = 2.0
        metrics.fwhm_avg = 5.0  # Very different from components
        
        issues = ValidationUtils.validate_mriqc_metrics_consistency(metrics)
        assert any('FWHM' in issue for issue in issues)
        
        # Test FD inconsistency
        metrics = MRIQCMetrics()
        metrics.fd_num = 0
        metrics.fd_perc = 10.0  # Should be 0 if fd_num is 0
        
        issues = ValidationUtils.validate_mriqc_metrics_consistency(metrics)
        assert any('framewise displacement' in issue for issue in issues)
        
        # CNR higher than SNR (unusual but not invalid)
        metrics = MRIQCMetrics()
        metrics.snr = 2.0
        metrics.cnr = 5.0  # Higher than SNR
        
        issues = ValidationUtils.validate_mriqc_metrics_consistency(metrics)
        assert any('CNR' in issue and 'SNR' in issue for issue in issues)
    
    def test_generate_validation_report(self):
        """Test comprehensive validation report generation."""
        subject_info = SubjectInfo(
            subject_id='sub-001',
            age=25.0,
            scan_type=ScanType.T1W
        )
        
        metrics = MRIQCMetrics(
            snr=12.5,
            cnr=3.2,
            fber=1500.0
        )
        
        report = ValidationUtils.generate_validation_report(subject_info, metrics)
        
        assert 'is_valid' in report
        assert 'warnings' in report
        assert 'errors' in report
        assert 'subject_validation' in report
        assert 'metrics_validation' in report
        assert 'validation_timestamp' in report
        
        # Should be valid
        assert report['is_valid'] == True
        
        # Should have age group
        assert report['subject_validation']['age']['age_group'] == 'young_adult'
    
    def test_generate_validation_report_with_issues(self):
        """Test validation report with issues."""
        # Create valid objects first, then test validation logic separately
        subject_info = SubjectInfo(
            subject_id='sub-001',
            age=25.0,
            scan_type=ScanType.T1W
        )
        
        metrics = MRIQCMetrics(
            snr=12.5,
            cnr=3.2
        )
        
        # Test the validation functions directly with problematic values
        id_valid, id_issues = ValidationUtils.validate_subject_id_format('123-45-6789')
        assert id_valid == False
        assert any('ssn' in issue.lower() for issue in id_issues)
        
        age_valid, age_error = ValidationUtils.validate_age_reasonableness(-5.0)
        assert age_valid == False
        assert age_error is not None
        
        range_valid = ValidationUtils.validate_metric_range('snr', 2000.0)
        assert range_valid == False
        
        # Test with valid data to ensure report structure
        report = ValidationUtils.generate_validation_report(subject_info, metrics)
        assert 'is_valid' in report
        assert 'warnings' in report
        assert 'errors' in report
    
    def test_create_validation_error(self):
        """Test validation error creation."""
        error = ValidationUtils.create_validation_error(
            field='age',
            message='Age must be positive',
            invalid_value=-5,
            expected_type='float'
        )
        
        assert error.field == 'age'
        assert error.message == 'Age must be positive'
        assert error.invalid_value == -5
        assert error.expected_type == 'float'


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    def test_validate_subject_data(self):
        """Test validate_subject_data convenience function."""
        subject_info = SubjectInfo(
            subject_id='sub-001',
            age=25.0,
            scan_type=ScanType.T1W
        )
        
        metrics = MRIQCMetrics(snr=12.5, cnr=3.2)
        
        report = validate_subject_data(subject_info, metrics)
        assert isinstance(report, dict)
        assert 'is_valid' in report
    
    def test_is_valid_subject_id(self):
        """Test is_valid_subject_id convenience function."""
        assert is_valid_subject_id('sub-001') == True
        assert is_valid_subject_id('sub@001') == False
        assert is_valid_subject_id('123-45-6789') == False
    
    def test_get_age_group(self):
        """Test get_age_group convenience function."""
        assert get_age_group(8.0) == AgeGroup.PEDIATRIC
        assert get_age_group(25.0) == AgeGroup.YOUNG_ADULT
        assert get_age_group(75.0) == AgeGroup.ELDERLY


if __name__ == "__main__":
    pytest.main([__file__])