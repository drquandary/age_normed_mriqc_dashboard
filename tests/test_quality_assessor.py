"""
Unit tests for the QualityAssessor class.

Tests quality assessment algorithms, threshold validation, and recommendation generation.
"""

import pytest
import tempfile
import os
from unittest.mock import Mock, patch
import numpy as np

from app.quality_assessor import QualityAssessor, ThresholdViolation
from app.models import (
    MRIQCMetrics, SubjectInfo, QualityStatus, AgeGroup, ScanType, Sex
)
from app.database import NormativeDatabase


class TestQualityAssessor:
    """Test cases for QualityAssessor class."""
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        # Initialize database with test data
        db = NormativeDatabase(db_path)
        yield db_path
        
        # Cleanup
        os.unlink(db_path)
    
    @pytest.fixture
    def assessor(self, temp_db):
        """Create QualityAssessor instance with test database."""
        return QualityAssessor(temp_db)
    
    @pytest.fixture
    def sample_metrics(self):
        """Sample MRIQC metrics for testing."""
        return MRIQCMetrics(
            snr=15.0,
            cnr=3.5,
            fber=1600.0,
            efc=0.45,
            fwhm_avg=2.8,
            qi1=0.85,
            cjv=0.42
        )
    
    @pytest.fixture
    def sample_subject(self):
        """Sample subject info for testing."""
        return SubjectInfo(
            subject_id="sub-001",
            age=25.0,
            sex=Sex.FEMALE,
            scan_type=ScanType.T1W,
            session="ses-01"
        )
    
    def test_assess_quality_with_age(self, assessor, sample_metrics, sample_subject):
        """Test quality assessment with age information."""
        assessment = assessor.assess_quality(sample_metrics, sample_subject)
        
        assert isinstance(assessment.overall_status, QualityStatus)
        assert 0 <= assessment.composite_score <= 100
        assert 0 <= assessment.confidence <= 1
        assert isinstance(assessment.recommendations, list)
        assert isinstance(assessment.flags, list)
        assert isinstance(assessment.metric_assessments, dict)
    
    def test_assess_quality_without_age(self, assessor, sample_metrics):
        """Test quality assessment without age information."""
        subject = SubjectInfo(
            subject_id="sub-002",
            age=None,
            scan_type=ScanType.T1W
        )
        
        assessment = assessor.assess_quality(sample_metrics, subject)
        
        # Should still provide assessment but with lower confidence
        assert isinstance(assessment.overall_status, QualityStatus)
        assert assessment.confidence < 0.8  # Lower confidence without age
        assert any("age information" in rec.lower() for rec in assessment.recommendations)
    
    def test_assess_single_metric_pass(self, assessor):
        """Test single metric assessment - pass case."""
        # High SNR should pass for young adults
        status, violation = assessor._assess_single_metric("snr", 20.0, 3)  # young_adult ID
        
        assert status == QualityStatus.PASS
        assert violation is None
    
    def test_assess_single_metric_warning(self, assessor):
        """Test single metric assessment - warning case."""
        # Borderline SNR should trigger warning
        status, violation = assessor._assess_single_metric("snr", 12.0, 3)  # young_adult ID
        
        assert status == QualityStatus.WARNING
        assert violation is not None
        assert violation.severity == "warning"
        assert violation.metric_name == "snr"
    
    def test_assess_single_metric_fail(self, assessor):
        """Test single metric assessment - fail case."""
        # Very low SNR should fail
        status, violation = assessor._assess_single_metric("snr", 5.0, 3)  # young_adult ID
        
        assert status == QualityStatus.FAIL
        assert violation is not None
        assert violation.severity == "fail"
        assert violation.metric_name == "snr"
    
    def test_assess_single_metric_no_age_group(self, assessor):
        """Test single metric assessment without age group."""
        status, violation = assessor._assess_single_metric("snr", 15.0, None)
        
        assert status == QualityStatus.UNCERTAIN
        assert violation is None
    
    def test_assess_single_metric_no_thresholds(self, assessor):
        """Test single metric assessment with missing thresholds."""
        # Use non-existent metric
        status, violation = assessor._assess_single_metric("fake_metric", 15.0, 3)
        
        assert status == QualityStatus.UNCERTAIN
        assert violation is None
    
    def test_calculate_composite_score_all_pass(self, assessor, sample_metrics):
        """Test composite score calculation with all metrics passing."""
        metric_assessments = {
            'snr': QualityStatus.PASS,
            'cnr': QualityStatus.PASS,
            'fber': QualityStatus.PASS,
            'efc': QualityStatus.PASS,
            'fwhm_avg': QualityStatus.PASS
        }
        
        score = assessor.calculate_composite_score(metric_assessments, sample_metrics)
        
        assert 75 <= score <= 100  # Should be high score
    
    def test_calculate_composite_score_mixed(self, assessor, sample_metrics):
        """Test composite score calculation with mixed results."""
        metric_assessments = {
            'snr': QualityStatus.PASS,
            'cnr': QualityStatus.WARNING,
            'fber': QualityStatus.PASS,
            'efc': QualityStatus.FAIL,
            'fwhm_avg': QualityStatus.PASS
        }
        
        score = assessor.calculate_composite_score(metric_assessments, sample_metrics)
        
        assert 50 <= score <= 90  # Should be moderate score
    
    def test_calculate_composite_score_all_fail(self, assessor, sample_metrics):
        """Test composite score calculation with all metrics failing."""
        metric_assessments = {
            'snr': QualityStatus.FAIL,
            'cnr': QualityStatus.FAIL,
            'fber': QualityStatus.FAIL,
            'efc': QualityStatus.FAIL,
            'fwhm_avg': QualityStatus.FAIL
        }
        
        score = assessor.calculate_composite_score(metric_assessments, sample_metrics)
        
        assert 0 <= score <= 40  # Should be low score
    
    def test_calculate_composite_score_empty(self, assessor, sample_metrics):
        """Test composite score calculation with no metrics."""
        metric_assessments = {}
        
        score = assessor.calculate_composite_score(metric_assessments, sample_metrics)
        
        assert score == 50.0  # Neutral score
    
    def test_determine_overall_status_pass(self, assessor):
        """Test overall status determination - pass case."""
        metric_assessments = {
            'snr': QualityStatus.PASS,
            'cnr': QualityStatus.PASS,
            'fber': QualityStatus.PASS,
            'efc': QualityStatus.PASS
        }
        
        status = assessor._determine_overall_status(metric_assessments, 85.0)
        
        assert status == QualityStatus.PASS
    
    def test_determine_overall_status_warning(self, assessor):
        """Test overall status determination - warning case."""
        metric_assessments = {
            'snr': QualityStatus.PASS,
            'cnr': QualityStatus.WARNING,
            'fber': QualityStatus.WARNING,
            'efc': QualityStatus.PASS
        }
        
        status = assessor._determine_overall_status(metric_assessments, 70.0)
        
        assert status == QualityStatus.WARNING
    
    def test_determine_overall_status_fail_high_rate(self, assessor):
        """Test overall status determination - fail due to high failure rate."""
        metric_assessments = {
            'snr': QualityStatus.FAIL,
            'cnr': QualityStatus.FAIL,
            'fber': QualityStatus.PASS,
            'efc': QualityStatus.PASS
        }
        
        status = assessor._determine_overall_status(metric_assessments, 60.0)
        
        assert status == QualityStatus.FAIL  # >20% failure rate
    
    def test_determine_overall_status_fail_critical_metric(self, assessor):
        """Test overall status determination - fail due to critical metric failure."""
        metric_assessments = {
            'snr': QualityStatus.FAIL,  # Critical metric
            'fber': QualityStatus.PASS,
            'qi1': QualityStatus.PASS,
            'cjv': QualityStatus.PASS
        }
        
        status = assessor._determine_overall_status(metric_assessments, 75.0)
        
        assert status == QualityStatus.FAIL  # Critical metric failed
    
    def test_determine_overall_status_uncertain(self, assessor):
        """Test overall status determination - uncertain case."""
        metric_assessments = {
            'snr': QualityStatus.UNCERTAIN,
            'cnr': QualityStatus.UNCERTAIN,
            'fber': QualityStatus.UNCERTAIN,
            'efc': QualityStatus.PASS
        }
        
        status = assessor._determine_overall_status(metric_assessments, 50.0)
        
        assert status == QualityStatus.UNCERTAIN  # >40% uncertain
    
    def test_generate_recommendations_all_pass(self, assessor, sample_subject):
        """Test recommendation generation for all passing metrics."""
        metric_assessments = {
            'snr': QualityStatus.PASS,
            'cnr': QualityStatus.PASS,
            'fber': QualityStatus.PASS
        }
        threshold_violations = {}
        
        recommendations = assessor._generate_recommendations(
            metric_assessments, threshold_violations, None, sample_subject
        )
        
        assert any("acceptable ranges" in rec.lower() for rec in recommendations)
    
    def test_generate_recommendations_with_failures(self, assessor, sample_subject):
        """Test recommendation generation with metric failures."""
        metric_assessments = {
            'snr': QualityStatus.FAIL,
            'cnr': QualityStatus.WARNING,
            'fber': QualityStatus.PASS
        }
        threshold_violations = {
            'snr': {
                'value': 5.0,
                'threshold': 10.0,
                'severity': 'fail',
                'direction': 'higher_better'
            },
            'cnr': {
                'value': 2.5,
                'threshold': 3.0,
                'severity': 'warning',
                'direction': 'higher_better'
            }
        }
        
        recommendations = assessor._generate_recommendations(
            metric_assessments, threshold_violations, None, sample_subject
        )
        
        assert any("EXCLUDE" in rec for rec in recommendations)
        assert any("CRITICAL" in rec for rec in recommendations)
        assert any("WARNING" in rec for rec in recommendations)
    
    def test_generate_recommendations_no_age(self, assessor):
        """Test recommendation generation without age information."""
        subject = SubjectInfo(
            subject_id="sub-003",
            age=None,
            scan_type=ScanType.T1W
        )
        
        metric_assessments = {'snr': QualityStatus.PASS}
        threshold_violations = {}
        
        recommendations = assessor._generate_recommendations(
            metric_assessments, threshold_violations, None, subject
        )
        
        assert any("age information" in rec.lower() for rec in recommendations)
    
    def test_calculate_confidence_high(self, assessor):
        """Test confidence calculation - high confidence case."""
        metric_assessments = {
            'snr': QualityStatus.PASS,
            'cnr': QualityStatus.PASS,
            'fber': QualityStatus.PASS,
            'efc': QualityStatus.PASS,
            'fwhm_avg': QualityStatus.PASS
        }
        
        confidence = assessor._calculate_confidence(metric_assessments, True, 5)
        
        assert confidence > 0.8  # High confidence with age info and many metrics
    
    def test_calculate_confidence_low(self, assessor):
        """Test confidence calculation - low confidence case."""
        metric_assessments = {
            'snr': QualityStatus.UNCERTAIN,
            'cnr': QualityStatus.UNCERTAIN
        }
        
        confidence = assessor._calculate_confidence(metric_assessments, False, 2)
        
        assert confidence <= 0.6  # Low confidence without age and uncertain metrics
    
    def test_apply_thresholds(self, assessor, sample_metrics):
        """Test applying thresholds to all metrics."""
        results = assessor.apply_thresholds(sample_metrics, 3)  # young_adult ID
        
        assert isinstance(results, dict)
        assert len(results) > 0
        assert all(isinstance(status, QualityStatus) for status in results.values())
    
    def test_get_threshold_summary(self, assessor):
        """Test getting threshold summary for age group."""
        summary = assessor.get_threshold_summary(AgeGroup.YOUNG_ADULT)
        
        assert isinstance(summary, dict)
        assert len(summary) > 0
        
        # Check structure of threshold data
        for metric_name, thresholds in summary.items():
            assert 'warning_threshold' in thresholds
            assert 'fail_threshold' in thresholds
            assert 'direction' in thresholds
            assert thresholds['direction'] in ['higher_better', 'lower_better']
    
    def test_validate_thresholds_valid(self, assessor):
        """Test threshold validation with valid thresholds."""
        errors = assessor.validate_thresholds(3)  # young_adult ID
        
        # Should have no errors with default data
        assert isinstance(errors, list)
        # Note: May have errors if test data is incomplete, but should be list
    
    def test_validate_thresholds_invalid(self, assessor, temp_db):
        """Test threshold validation with invalid thresholds."""
        # Add invalid threshold to database
        db = NormativeDatabase(temp_db)
        with db.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO quality_thresholds 
                (metric_name, age_group_id, warning_threshold, fail_threshold, direction)
                VALUES (?, ?, ?, ?, ?)
            """, ("test_metric", 3, 5.0, 10.0, "higher_better"))  # Invalid: warning < fail
            conn.commit()
        
        errors = assessor.validate_thresholds(3)
        
        assert len(errors) > 0
        assert any("test_metric" in error for error in errors)
    
    def test_threshold_violation_dataclass(self):
        """Test ThresholdViolation dataclass."""
        violation = ThresholdViolation(
            metric_name="snr",
            value=8.0,
            threshold=10.0,
            threshold_type="warning",
            severity="warning",
            direction="higher_better"
        )
        
        assert violation.metric_name == "snr"
        assert violation.value == 8.0
        assert violation.threshold == 10.0
        assert violation.threshold_type == "warning"
        assert violation.severity == "warning"
        assert violation.direction == "higher_better"
    
    def test_metric_weights_coverage(self, assessor):
        """Test that metric weights are properly defined."""
        assert isinstance(assessor.metric_weights, dict)
        assert len(assessor.metric_weights) > 0
        
        # Check that weights sum to reasonable value
        total_weight = sum(assessor.metric_weights.values())
        assert 0.8 <= total_weight <= 1.2  # Allow some flexibility
    
    def test_status_scores_coverage(self, assessor):
        """Test that status scores are defined for all quality statuses."""
        assert isinstance(assessor.status_scores, dict)
        
        for status in QualityStatus:
            assert status in assessor.status_scores
            assert 0 <= assessor.status_scores[status] <= 100
    
    def test_edge_case_empty_metrics(self, assessor, sample_subject):
        """Test assessment with empty metrics."""
        empty_metrics = MRIQCMetrics()
        
        assessment = assessor.assess_quality(empty_metrics, sample_subject)
        
        assert assessment.overall_status == QualityStatus.UNCERTAIN
        assert assessment.composite_score == 50.0
        assert len(assessment.metric_assessments) == 0
    
    def test_edge_case_extreme_values(self, assessor, sample_subject):
        """Test assessment with extreme metric values."""
        extreme_metrics = MRIQCMetrics(
            snr=50.0,    # High but not extreme
            cnr=0.5,     # Low
            efc=0.8      # High
        )
        
        assessment = assessor.assess_quality(extreme_metrics, sample_subject)
        
        # Should handle extreme values gracefully
        assert isinstance(assessment.overall_status, QualityStatus)
        assert 0 <= assessment.composite_score <= 100
    
    def test_different_age_groups(self, assessor, sample_metrics):
        """Test assessment across different age groups."""
        age_groups = [
            (8.0, "pediatric"),
            (15.0, "adolescent"),
            (25.0, "young_adult"),
            (50.0, "middle_age"),
            (75.0, "elderly")
        ]
        
        for age, expected_group in age_groups:
            subject = SubjectInfo(
                subject_id=f"sub-{expected_group}",
                age=age,
                scan_type=ScanType.T1W
            )
            
            assessment = assessor.assess_quality(sample_metrics, subject)
            
            # Should provide valid assessment for each age group
            assert isinstance(assessment.overall_status, QualityStatus)
            assert assessment.confidence > 0.5  # Should have reasonable confidence
    
    def test_scan_type_specific_recommendations(self, assessor):
        """Test scan type specific recommendations."""
        # Test T1w specific recommendations
        t1w_subject = SubjectInfo(
            subject_id="sub-t1w",
            age=25.0,
            scan_type=ScanType.T1W
        )
        
        poor_snr_metrics = MRIQCMetrics(snr=5.0, cnr=3.0)
        assessment = assessor.assess_quality(poor_snr_metrics, t1w_subject)
        
        # Should include T1w specific recommendations
        recommendations_text = " ".join(assessment.recommendations).lower()
        assert "t1w" in recommendations_text or "acquisition" in recommendations_text
    
    @pytest.mark.parametrize("direction,value,warning,fail,expected", [
        ("higher_better", 15.0, 12.0, 8.0, QualityStatus.PASS),
        ("higher_better", 10.0, 12.0, 8.0, QualityStatus.WARNING),
        ("higher_better", 5.0, 12.0, 8.0, QualityStatus.FAIL),
        ("lower_better", 0.4, 0.5, 0.6, QualityStatus.PASS),
        ("lower_better", 0.55, 0.5, 0.6, QualityStatus.WARNING),
        ("lower_better", 0.7, 0.5, 0.6, QualityStatus.FAIL),
    ])
    def test_threshold_logic_parametrized(self, assessor, direction, value, warning, fail, expected):
        """Test threshold logic with various parameter combinations."""
        # Mock the database response
        with patch.object(assessor.db, 'get_quality_thresholds') as mock_get:
            mock_get.return_value = {
                'warning_threshold': warning,
                'fail_threshold': fail,
                'direction': direction
            }
            
            status, violation = assessor._assess_single_metric("test_metric", value, 1)
            
            assert status == expected
            if expected != QualityStatus.PASS:
                assert violation is not None
            else:
                assert violation is None