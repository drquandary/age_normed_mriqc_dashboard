"""
Unit tests for data models in the Age-Normed MRIQC Dashboard.

Tests cover validation, error handling, and edge cases for all Pydantic models.
"""

import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import Dict, Any

from app.models import (
    AgeGroup, QualityStatus, ScanType, Sex,
    MRIQCMetrics, SubjectInfo, NormalizedMetrics,
    QualityAssessment, ProcessedSubject, StudySummary,
    QualityThresholds, StudyConfiguration,
    ValidationError as CustomValidationError,
    ProcessingError
)


class TestEnums:
    """Test enum definitions."""
    
    def test_age_group_values(self):
        """Test AgeGroup enum values."""
        assert AgeGroup.PEDIATRIC == "pediatric"
        assert AgeGroup.ADOLESCENT == "adolescent"
        assert AgeGroup.YOUNG_ADULT == "young_adult"
        assert AgeGroup.MIDDLE_AGE == "middle_age"
        assert AgeGroup.ELDERLY == "elderly"
    
    def test_quality_status_values(self):
        """Test QualityStatus enum values."""
        assert QualityStatus.PASS == "pass"
        assert QualityStatus.WARNING == "warning"
        assert QualityStatus.FAIL == "fail"
        assert QualityStatus.UNCERTAIN == "uncertain"
    
    def test_scan_type_values(self):
        """Test ScanType enum values."""
        assert ScanType.T1W == "T1w"
        assert ScanType.T2W == "T2w"
        assert ScanType.BOLD == "BOLD"
        assert ScanType.DWI == "DWI"
        assert ScanType.FLAIR == "FLAIR"


class TestMRIQCMetrics:
    """Test MRIQCMetrics model validation."""
    
    def test_valid_metrics(self):
        """Test creation with valid metrics."""
        metrics = MRIQCMetrics(
            snr=12.5,
            cnr=3.2,
            fber=1500.0,
            efc=0.45,
            fwhm_avg=2.8,
            qi1=0.85,
            cjv=0.42
        )
        assert metrics.snr == 12.5
        assert metrics.cnr == 3.2
        assert metrics.fber == 1500.0
    
    def test_optional_metrics(self):
        """Test that all metrics are optional."""
        metrics = MRIQCMetrics()
        assert metrics.snr is None
        assert metrics.cnr is None
        assert metrics.fber is None
    
    def test_metric_range_validation(self):
        """Test metric range validation."""
        # Test SNR range
        with pytest.raises(ValidationError):
            MRIQCMetrics(snr=-1)  # Below minimum
        
        with pytest.raises(ValidationError):
            MRIQCMetrics(snr=1001)  # Above maximum
        
        # Test EFC range (0-1)
        with pytest.raises(ValidationError):
            MRIQCMetrics(efc=1.5)  # Above maximum
        
        # Test GCOR range (-1 to 1)
        with pytest.raises(ValidationError):
            MRIQCMetrics(gcor=1.5)  # Above maximum
        
        with pytest.raises(ValidationError):
            MRIQCMetrics(gcor=-1.5)  # Below minimum
    
    def test_string_conversion(self):
        """Test conversion of string values to numeric."""
        metrics = MRIQCMetrics(
            snr="12.5",
            fd_num="10",
            efc="0.45"
        )
        assert metrics.snr == 12.5
        assert metrics.fd_num == 10
        assert metrics.efc == 0.45
    
    def test_invalid_string_conversion(self):
        """Test handling of invalid string values."""
        metrics = MRIQCMetrics(snr="invalid")
        assert metrics.snr is None
    
    def test_fwhm_consistency_validation(self):
        """Test FWHM consistency validation."""
        # Valid consistent FWHM values
        metrics = MRIQCMetrics(
            fwhm_x=2.5,
            fwhm_y=2.7,
            fwhm_z=2.9,
            fwhm_avg=2.7  # Close to average of components
        )
        assert metrics.fwhm_avg == 2.7
        
        # Inconsistent FWHM values should raise error
        with pytest.raises(ValidationError):
            MRIQCMetrics(
                fwhm_x=2.0,
                fwhm_y=2.0,
                fwhm_z=2.0,
                fwhm_avg=5.0  # Very different from components
            )
    
    def test_framewise_displacement_consistency(self):
        """Test framewise displacement consistency validation."""
        # Inconsistent FD values should raise error
        with pytest.raises(ValidationError):
            MRIQCMetrics(
                fd_num=0,  # No high motion timepoints
                fd_perc=10.0  # But 10% high motion percentage
            )


class TestSubjectInfo:
    """Test SubjectInfo model validation."""
    
    def test_valid_subject_info(self):
        """Test creation with valid subject information."""
        subject = SubjectInfo(
            subject_id="sub-001",
            age=25.5,
            sex=Sex.FEMALE,
            session="ses-01",
            scan_type=ScanType.T1W,
            site="site-A"
        )
        assert subject.subject_id == "sub-001"
        assert subject.age == 25.5
        assert subject.sex == Sex.FEMALE
    
    def test_subject_id_validation(self):
        """Test subject ID validation."""
        # Valid IDs
        SubjectInfo(subject_id="sub-001", scan_type=ScanType.T1W)
        SubjectInfo(subject_id="participant_123", scan_type=ScanType.T1W)
        SubjectInfo(subject_id="P001-T1", scan_type=ScanType.T1W)
        
        # Invalid IDs
        with pytest.raises(ValidationError):
            SubjectInfo(subject_id="", scan_type=ScanType.T1W)  # Empty
        
        with pytest.raises(ValidationError):
            SubjectInfo(subject_id="sub 001", scan_type=ScanType.T1W)  # Space
        
        with pytest.raises(ValidationError):
            SubjectInfo(subject_id="sub@001", scan_type=ScanType.T1W)  # Special char
    
    def test_pii_detection(self):
        """Test detection of potentially identifying information."""
        # SSN pattern should be rejected
        with pytest.raises(ValidationError, match="SSN"):
            SubjectInfo(subject_id="123-45-6789", scan_type=ScanType.T1W)
        
        # Date pattern should be rejected - use valid characters but date pattern
        with pytest.raises(ValidationError, match="date"):
            SubjectInfo(subject_id="01-15-1990", scan_type=ScanType.T1W)
    
    def test_age_validation(self):
        """Test age validation."""
        # Valid ages
        SubjectInfo(subject_id="sub-001", age=25.0, scan_type=ScanType.T1W)
        SubjectInfo(subject_id="sub-002", age=0.5, scan_type=ScanType.T1W)  # Infant
        
        # Invalid ages
        with pytest.raises(ValidationError):
            SubjectInfo(subject_id="sub-001", age=-1, scan_type=ScanType.T1W)
        
        with pytest.raises(ValidationError):
            SubjectInfo(subject_id="sub-001", age=150, scan_type=ScanType.T1W)
    
    def test_optional_fields(self):
        """Test that optional fields work correctly."""
        subject = SubjectInfo(
            subject_id="sub-001",
            scan_type=ScanType.T1W
        )
        assert subject.age is None
        assert subject.sex is None
        assert subject.session is None


class TestNormalizedMetrics:
    """Test NormalizedMetrics model validation."""
    
    def test_valid_normalized_metrics(self):
        """Test creation with valid normalized metrics."""
        raw_metrics = MRIQCMetrics(snr=12.5, cnr=3.2)
        normalized = NormalizedMetrics(
            raw_metrics=raw_metrics,
            percentiles={"snr": 75.0, "cnr": 60.0},
            z_scores={"snr": 0.67, "cnr": 0.25},
            age_group=AgeGroup.YOUNG_ADULT,
            normative_dataset="HCP-YA"
        )
        assert normalized.age_group == AgeGroup.YOUNG_ADULT
        assert normalized.percentiles["snr"] == 75.0
    
    def test_percentile_validation(self):
        """Test percentile range validation."""
        raw_metrics = MRIQCMetrics(snr=12.5)
        
        # Valid percentiles
        NormalizedMetrics(
            raw_metrics=raw_metrics,
            percentiles={"snr": 50.0},
            z_scores={"snr": 0.0},
            age_group=AgeGroup.YOUNG_ADULT,
            normative_dataset="test"
        )
        
        # Invalid percentiles
        with pytest.raises(ValidationError):
            NormalizedMetrics(
                raw_metrics=raw_metrics,
                percentiles={"snr": 150.0},  # Above 100
                z_scores={"snr": 0.0},
                age_group=AgeGroup.YOUNG_ADULT,
                normative_dataset="test"
            )
        
        with pytest.raises(ValidationError):
            NormalizedMetrics(
                raw_metrics=raw_metrics,
                percentiles={"snr": -10.0},  # Below 0
                z_scores={"snr": 0.0},
                age_group=AgeGroup.YOUNG_ADULT,
                normative_dataset="test"
            )
    
    def test_extreme_z_score_validation(self):
        """Test validation of extreme z-scores."""
        raw_metrics = MRIQCMetrics(snr=12.5)
        
        # Extreme z-scores should raise warning
        with pytest.raises(ValidationError):
            NormalizedMetrics(
                raw_metrics=raw_metrics,
                percentiles={"snr": 50.0},
                z_scores={"snr": 15.0},  # Extremely high
                age_group=AgeGroup.YOUNG_ADULT,
                normative_dataset="test"
            )


class TestQualityAssessment:
    """Test QualityAssessment model validation."""
    
    def test_valid_quality_assessment(self):
        """Test creation with valid quality assessment."""
        assessment = QualityAssessment(
            overall_status=QualityStatus.PASS,
            metric_assessments={"snr": QualityStatus.PASS, "cnr": QualityStatus.WARNING},
            composite_score=78.5,
            recommendations=["Consider manual review", "Overall acceptable"],
            flags=["cnr_borderline"],
            confidence=0.85
        )
        assert assessment.overall_status == QualityStatus.PASS
        assert assessment.composite_score == 78.5
        assert len(assessment.recommendations) == 2
    
    def test_composite_score_validation(self):
        """Test composite score range validation."""
        # Valid score
        QualityAssessment(
            overall_status=QualityStatus.PASS,
            metric_assessments={},
            composite_score=50.0,
            confidence=0.8
        )
        
        # Invalid scores
        with pytest.raises(ValidationError):
            QualityAssessment(
                overall_status=QualityStatus.PASS,
                metric_assessments={},
                composite_score=-10.0,  # Below 0
                confidence=0.8
            )
        
        with pytest.raises(ValidationError):
            QualityAssessment(
                overall_status=QualityStatus.PASS,
                metric_assessments={},
                composite_score=150.0,  # Above 100
                confidence=0.8
            )
    
    def test_confidence_validation(self):
        """Test confidence range validation."""
        # Valid confidence
        QualityAssessment(
            overall_status=QualityStatus.PASS,
            metric_assessments={},
            composite_score=50.0,
            confidence=0.5
        )
        
        # Invalid confidence
        with pytest.raises(ValidationError):
            QualityAssessment(
                overall_status=QualityStatus.PASS,
                metric_assessments={},
                composite_score=50.0,
                confidence=1.5  # Above 1
            )
    
    def test_text_list_cleaning(self):
        """Test cleaning of recommendation and flag lists."""
        assessment = QualityAssessment(
            overall_status=QualityStatus.PASS,
            metric_assessments={},
            composite_score=50.0,
            confidence=0.8,
            recommendations=["  Valid recommendation  ", "", "Another one", "   "],
            flags=["flag1", "", "  flag2  "]
        )
        assert assessment.recommendations == ["Valid recommendation", "Another one"]
        assert assessment.flags == ["flag1", "flag2"]


class TestProcessedSubject:
    """Test ProcessedSubject model."""
    
    def test_valid_processed_subject(self):
        """Test creation with valid processed subject data."""
        subject_info = SubjectInfo(
            subject_id="sub-001",
            age=25.0,
            scan_type=ScanType.T1W
        )
        raw_metrics = MRIQCMetrics(snr=12.5, cnr=3.2)
        quality_assessment = QualityAssessment(
            overall_status=QualityStatus.PASS,
            metric_assessments={"snr": QualityStatus.PASS},
            composite_score=78.5,
            confidence=0.85
        )
        
        processed = ProcessedSubject(
            subject_info=subject_info,
            raw_metrics=raw_metrics,
            quality_assessment=quality_assessment
        )
        
        assert processed.subject_info.subject_id == "sub-001"
        assert processed.raw_metrics.snr == 12.5
        assert processed.quality_assessment.overall_status == QualityStatus.PASS
        assert processed.processing_version == "1.0.0"  # Default value
    
    def test_optional_normalized_metrics(self):
        """Test that normalized metrics are optional."""
        subject_info = SubjectInfo(subject_id="sub-001", scan_type=ScanType.T1W)
        raw_metrics = MRIQCMetrics(snr=12.5)
        quality_assessment = QualityAssessment(
            overall_status=QualityStatus.PASS,
            metric_assessments={},
            composite_score=50.0,
            confidence=0.8
        )
        
        processed = ProcessedSubject(
            subject_info=subject_info,
            raw_metrics=raw_metrics,
            quality_assessment=quality_assessment
        )
        
        assert processed.normalized_metrics is None


class TestStudySummary:
    """Test StudySummary model validation."""
    
    def test_valid_study_summary(self):
        """Test creation with valid study summary."""
        summary = StudySummary(
            total_subjects=100,
            quality_distribution={
                QualityStatus.PASS: 75,
                QualityStatus.WARNING: 15,
                QualityStatus.FAIL: 8,
                QualityStatus.UNCERTAIN: 2
            },
            exclusion_rate=0.08
        )
        assert summary.total_subjects == 100
        assert summary.exclusion_rate == 0.08
    
    def test_quality_distribution_validation(self):
        """Test quality distribution consistency validation."""
        # Valid distribution
        StudySummary(
            total_subjects=10,
            quality_distribution={
                QualityStatus.PASS: 7,
                QualityStatus.WARNING: 2,
                QualityStatus.FAIL: 1,
                QualityStatus.UNCERTAIN: 0
            },
            exclusion_rate=0.1
        )
        
        # Invalid distribution (counts don't match total)
        with pytest.raises(ValidationError):
            StudySummary(
                total_subjects=10,
                quality_distribution={
                    QualityStatus.PASS: 5,
                    QualityStatus.WARNING: 2,
                    QualityStatus.FAIL: 1,
                    QualityStatus.UNCERTAIN: 0
                    # Total = 8, but total_subjects = 10
                },
                exclusion_rate=0.1
            )


class TestQualityThresholds:
    """Test QualityThresholds model validation."""
    
    def test_valid_thresholds(self):
        """Test creation with valid thresholds."""
        thresholds = QualityThresholds(
            metric_name="snr",
            age_group=AgeGroup.YOUNG_ADULT,
            warning_threshold=10.0,
            fail_threshold=8.0,
            direction="higher_better"
        )
        assert thresholds.metric_name == "snr"
        assert thresholds.direction == "higher_better"
    
    def test_threshold_order_validation_higher_better(self):
        """Test threshold order validation for higher_better metrics."""
        # Valid order for higher_better
        QualityThresholds(
            metric_name="snr",
            age_group=AgeGroup.YOUNG_ADULT,
            warning_threshold=10.0,
            fail_threshold=8.0,  # Lower than warning
            direction="higher_better"
        )
        
        # Invalid order for higher_better
        with pytest.raises(ValidationError):
            QualityThresholds(
                metric_name="snr",
                age_group=AgeGroup.YOUNG_ADULT,
                warning_threshold=8.0,
                fail_threshold=10.0,  # Higher than warning
                direction="higher_better"
            )
    
    def test_threshold_order_validation_lower_better(self):
        """Test threshold order validation for lower_better metrics."""
        # Valid order for lower_better
        QualityThresholds(
            metric_name="fd_mean",
            age_group=AgeGroup.YOUNG_ADULT,
            warning_threshold=0.3,
            fail_threshold=0.5,  # Higher than warning
            direction="lower_better"
        )
        
        # Invalid order for lower_better
        with pytest.raises(ValidationError):
            QualityThresholds(
                metric_name="fd_mean",
                age_group=AgeGroup.YOUNG_ADULT,
                warning_threshold=0.5,
                fail_threshold=0.3,  # Lower than warning
                direction="lower_better"
            )
    
    def test_direction_validation(self):
        """Test direction field validation."""
        # Valid directions
        QualityThresholds(
            metric_name="snr",
            age_group=AgeGroup.YOUNG_ADULT,
            warning_threshold=10.0,
            fail_threshold=8.0,
            direction="higher_better"
        )
        
        QualityThresholds(
            metric_name="fd_mean",
            age_group=AgeGroup.YOUNG_ADULT,
            warning_threshold=0.3,
            fail_threshold=0.5,
            direction="lower_better"
        )
        
        # Invalid direction
        with pytest.raises(ValidationError):
            QualityThresholds(
                metric_name="snr",
                age_group=AgeGroup.YOUNG_ADULT,
                warning_threshold=10.0,
                fail_threshold=8.0,
                direction="invalid_direction"
            )


class TestStudyConfiguration:
    """Test StudyConfiguration model validation."""
    
    def test_valid_configuration(self):
        """Test creation with valid configuration."""
        config = StudyConfiguration(
            study_name="Test Study",
            normative_dataset="HCP-YA",
            exclusion_criteria=["excessive_motion"],
            created_by="researcher_001"
        )
        assert config.study_name == "Test Study"
        assert config.created_by == "researcher_001"
    
    def test_required_fields(self):
        """Test that required fields are enforced."""
        # Missing study_name
        with pytest.raises(ValidationError):
            StudyConfiguration(created_by="researcher_001")
        
        # Missing created_by
        with pytest.raises(ValidationError):
            StudyConfiguration(study_name="Test Study")
    
    def test_study_name_validation(self):
        """Test study name validation."""
        # Valid name
        StudyConfiguration(
            study_name="Valid Study Name",
            created_by="researcher_001"
        )
        
        # Empty name
        with pytest.raises(ValidationError):
            StudyConfiguration(
                study_name="",
                created_by="researcher_001"
            )
        
        # Too long name
        with pytest.raises(ValidationError):
            StudyConfiguration(
                study_name="x" * 101,  # 101 characters
                created_by="researcher_001"
            )


class TestErrorModels:
    """Test error model validation."""
    
    def test_validation_error(self):
        """Test ValidationError model."""
        error = CustomValidationError(
            field="age",
            message="Age must be positive",
            invalid_value=-5,
            expected_type="float"
        )
        assert error.field == "age"
        assert error.invalid_value == -5
    
    def test_processing_error(self):
        """Test ProcessingError model."""
        error = ProcessingError(
            error_type="file_format_error",
            message="Invalid MRIQC file",
            details={"missing_columns": ["snr", "cnr"]},
            suggestions=["Check file format"],
            error_code="MRIQC_001"
        )
        assert error.error_type == "file_format_error"
        assert error.error_code == "MRIQC_001"
        assert "snr" in error.details["missing_columns"]


class TestModelIntegration:
    """Test integration between different models."""
    
    def test_complete_workflow_models(self):
        """Test that models work together in a complete workflow."""
        # Create subject info
        subject_info = SubjectInfo(
            subject_id="sub-001",
            age=25.0,
            sex=Sex.FEMALE,
            scan_type=ScanType.T1W
        )
        
        # Create raw metrics
        raw_metrics = MRIQCMetrics(
            snr=12.5,
            cnr=3.2,
            fber=1500.0,
            efc=0.45
        )
        
        # Create normalized metrics
        normalized_metrics = NormalizedMetrics(
            raw_metrics=raw_metrics,
            percentiles={"snr": 75.0, "cnr": 60.0, "fber": 80.0, "efc": 55.0},
            z_scores={"snr": 0.67, "cnr": 0.25, "fber": 0.84, "efc": 0.13},
            age_group=AgeGroup.YOUNG_ADULT,
            normative_dataset="HCP-YA"
        )
        
        # Create quality assessment
        quality_assessment = QualityAssessment(
            overall_status=QualityStatus.PASS,
            metric_assessments={
                "snr": QualityStatus.PASS,
                "cnr": QualityStatus.WARNING,
                "fber": QualityStatus.PASS,
                "efc": QualityStatus.PASS
            },
            composite_score=78.5,
            recommendations=["Consider manual review of CNR"],
            flags=["cnr_borderline"],
            confidence=0.85
        )
        
        # Create processed subject
        processed_subject = ProcessedSubject(
            subject_info=subject_info,
            raw_metrics=raw_metrics,
            normalized_metrics=normalized_metrics,
            quality_assessment=quality_assessment
        )
        
        # Verify all components are properly integrated
        assert processed_subject.subject_info.subject_id == "sub-001"
        assert processed_subject.raw_metrics.snr == 12.5
        assert processed_subject.normalized_metrics.age_group == AgeGroup.YOUNG_ADULT
        assert processed_subject.quality_assessment.overall_status == QualityStatus.PASS
        assert processed_subject.processing_timestamp is not None
    
    def test_study_summary_creation(self):
        """Test study summary creation with realistic data."""
        summary = StudySummary(
            total_subjects=100,
            quality_distribution={
                QualityStatus.PASS: 70,
                QualityStatus.WARNING: 20,
                QualityStatus.FAIL: 8,
                QualityStatus.UNCERTAIN: 2
            },
            age_group_distribution={
                AgeGroup.YOUNG_ADULT: 60,
                AgeGroup.MIDDLE_AGE: 30,
                AgeGroup.ELDERLY: 10
            },
            metric_statistics={
                "snr": {"mean": 12.5, "std": 2.1, "min": 8.0, "max": 18.0},
                "cnr": {"mean": 3.2, "std": 0.8, "min": 1.5, "max": 5.0}
            },
            exclusion_rate=0.08,
            study_name="Multi-Age Cohort Study"
        )
        
        assert summary.total_subjects == 100
        assert summary.exclusion_rate == 0.08
        assert summary.study_name == "Multi-Age Cohort Study"
        assert sum(summary.quality_distribution.values()) == 100
        assert sum(summary.age_group_distribution.values()) == 100


if __name__ == "__main__":
    pytest.main([__file__])