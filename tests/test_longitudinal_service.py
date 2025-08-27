"""
Tests for longitudinal data service.
"""

import pytest
import tempfile
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from app.longitudinal_service import LongitudinalService
from app.database import NormativeDatabase
from app.age_normalizer import AgeNormalizer
from app.models import (
    ProcessedSubject, SubjectInfo, MRIQCMetrics, QualityAssessment,
    LongitudinalSubject, TimePoint, LongitudinalTrend, LongitudinalSummary,
    ScanType, Sex, QualityStatus, AgeGroup
)


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    db = NormativeDatabase(db_path)
    yield db
    
    # Cleanup
    os.unlink(db_path)


@pytest.fixture
def longitudinal_service(temp_db):
    """Create a longitudinal service with test database."""
    # AgeNormalizer expects a db_path string, not a database object
    age_normalizer = AgeNormalizer(str(temp_db.db_path))
    return LongitudinalService(temp_db, age_normalizer)


@pytest.fixture
def sample_processed_subject():
    """Create a sample processed subject for testing."""
    return ProcessedSubject(
        subject_info=SubjectInfo(
            subject_id="sub-001",
            age=25.5,
            sex=Sex.FEMALE,
            session="baseline",
            scan_type=ScanType.T1W,
            acquisition_date=datetime(2024, 1, 15, 10, 30)
        ),
        raw_metrics=MRIQCMetrics(
            snr=15.2,
            cnr=3.8,
            fber=1420.0,
            efc=0.45,
            fwhm_avg=2.8
        ),
        quality_assessment=QualityAssessment(
            overall_status=QualityStatus.PASS,
            metric_assessments={
                "snr": QualityStatus.PASS,
                "cnr": QualityStatus.PASS,
                "fber": QualityStatus.PASS,
                "efc": QualityStatus.PASS,
                "fwhm_avg": QualityStatus.PASS
            },
            composite_score=85.0,
            confidence=0.9
        )
    )


class TestLongitudinalService:
    """Test cases for LongitudinalService."""
    
    def test_add_subject_timepoint(self, longitudinal_service, sample_processed_subject):
        """Test adding a timepoint for a longitudinal subject."""
        timepoint_id = longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=sample_processed_subject,
            session="baseline",
            days_from_baseline=0,
            study_name="Test Study"
        )
        
        assert timepoint_id == "sub-001_baseline"
        
        # Verify subject was created
        subject = longitudinal_service.db.get_longitudinal_subject("sub-001")
        assert subject is not None
        assert subject['subject_id'] == "sub-001"
        assert subject['baseline_age'] == 25.5
        assert subject['sex'] == 'F'
        assert subject['study_name'] == "Test Study"
        
        # Verify timepoint was added
        timepoints = longitudinal_service.db.get_subject_timepoints("sub-001")
        assert len(timepoints) == 1
        assert timepoints[0]['timepoint_id'] == "sub-001_baseline"
        assert timepoints[0]['session'] == "baseline"
        assert timepoints[0]['age_at_scan'] == 25.5
        assert timepoints[0]['days_from_baseline'] == 0
    
    def test_add_multiple_timepoints(self, longitudinal_service, sample_processed_subject):
        """Test adding multiple timepoints for a subject."""
        # Add baseline
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=sample_processed_subject,
            session="baseline",
            days_from_baseline=0
        )
        
        # Create follow-up subject
        followup_subject = sample_processed_subject.model_copy()
        followup_subject.subject_info.age = 26.0
        followup_subject.subject_info.session = "followup1"
        followup_subject.subject_info.acquisition_date = datetime(2024, 7, 15, 10, 30)
        followup_subject.raw_metrics.snr = 15.8  # Slightly improved
        
        # Add follow-up
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=followup_subject,
            session="followup1",
            days_from_baseline=180
        )
        
        # Verify both timepoints exist
        timepoints = longitudinal_service.db.get_subject_timepoints("sub-001")
        assert len(timepoints) == 2
        
        # Verify they're sorted by days from baseline
        assert timepoints[0]['days_from_baseline'] == 0
        assert timepoints[1]['days_from_baseline'] == 180
    
    def test_get_longitudinal_subject(self, longitudinal_service, sample_processed_subject):
        """Test retrieving longitudinal subject data."""
        # Add timepoints
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=sample_processed_subject,
            session="baseline",
            days_from_baseline=0
        )
        
        followup_subject = sample_processed_subject.model_copy()
        followup_subject.subject_info.age = 26.0
        followup_subject.subject_info.session = "followup1"
        followup_subject.raw_metrics.snr = 15.8
        
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=followup_subject,
            session="followup1",
            days_from_baseline=180
        )
        
        # Retrieve longitudinal subject
        longitudinal_subject = longitudinal_service.get_longitudinal_subject("sub-001")
        
        assert longitudinal_subject is not None
        assert longitudinal_subject.subject_id == "sub-001"
        assert longitudinal_subject.baseline_age == 25.5
        assert longitudinal_subject.num_timepoints == 2
        assert longitudinal_subject.age_range == {"min": 25.5, "max": 26.0}
        assert longitudinal_subject.follow_up_duration_days == 180
        
        # Verify timepoints are sorted
        assert longitudinal_subject.timepoints[0].session == "baseline"
        assert longitudinal_subject.timepoints[1].session == "followup1"
    
    def test_calculate_metric_trend_improving(self, longitudinal_service, sample_processed_subject):
        """Test calculating an improving trend."""
        # Add baseline with lower SNR
        baseline_subject = sample_processed_subject.model_copy()
        baseline_subject.raw_metrics.snr = 12.0
        
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=baseline_subject,
            session="baseline",
            days_from_baseline=0
        )
        
        # Add follow-up with higher SNR
        followup_subject = sample_processed_subject.model_copy()
        followup_subject.subject_info.age = 26.0
        followup_subject.subject_info.session = "followup1"
        followup_subject.raw_metrics.snr = 18.0  # Significant improvement
        
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=followup_subject,
            session="followup1",
            days_from_baseline=180
        )
        
        # Calculate trend
        trend = longitudinal_service.calculate_metric_trend("sub-001", "snr")
        
        assert trend is not None
        assert trend.subject_id == "sub-001"
        assert trend.metric_name == "snr"
        assert trend.trend_direction == "improving"  # SNR higher is better
        assert trend.trend_slope > 0  # Positive slope
        assert len(trend.values_over_time) == 2
        assert trend.values_over_time[0]['value'] == 12.0
        assert trend.values_over_time[1]['value'] == 18.0
    
    def test_calculate_metric_trend_declining(self, longitudinal_service, sample_processed_subject):
        """Test calculating a declining trend."""
        # Add baseline with lower EFC (lower is better for EFC)
        baseline_subject = sample_processed_subject.model_copy()
        baseline_subject.raw_metrics.efc = 0.40  # Good value
        
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=baseline_subject,
            session="baseline",
            days_from_baseline=0
        )
        
        # Add follow-up with higher EFC (worse)
        followup_subject = sample_processed_subject.model_copy()
        followup_subject.subject_info.age = 26.0
        followup_subject.subject_info.session = "followup1"
        followup_subject.raw_metrics.efc = 0.60  # Worse value
        
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=followup_subject,
            session="followup1",
            days_from_baseline=180
        )
        
        # Calculate trend
        trend = longitudinal_service.calculate_metric_trend("sub-001", "efc")
        
        assert trend is not None
        assert trend.metric_name == "efc"
        assert trend.trend_direction == "declining"  # EFC increasing is declining quality
        assert trend.trend_slope > 0  # Positive slope but declining quality
    
    def test_calculate_metric_trend_stable(self, longitudinal_service, sample_processed_subject):
        """Test calculating a stable trend."""
        # Add multiple timepoints with similar values
        for i, days in enumerate([0, 90, 180, 270]):
            subject = sample_processed_subject.model_copy()
            subject.subject_info.session = f"tp{i+1}"
            subject.raw_metrics.snr = 15.0 + (i * 0.1)  # Very small changes
            
            longitudinal_service.add_subject_timepoint(
                subject_id="sub-001",
                processed_subject=subject,
                session=f"tp{i+1}",
                days_from_baseline=days
            )
        
        # Calculate trend
        trend = longitudinal_service.calculate_metric_trend("sub-001", "snr")
        
        assert trend is not None
        assert trend.trend_direction in ["stable", "variable"]  # Small changes should be stable
        assert abs(trend.trend_slope) < 0.01  # Very small slope
    
    def test_calculate_all_trends_for_subject(self, longitudinal_service, sample_processed_subject):
        """Test calculating trends for all metrics."""
        # Add two timepoints
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=sample_processed_subject,
            session="baseline",
            days_from_baseline=0
        )
        
        followup_subject = sample_processed_subject.model_copy()
        followup_subject.subject_info.age = 26.0
        followup_subject.subject_info.session = "followup1"
        followup_subject.raw_metrics.snr = 16.0
        followup_subject.raw_metrics.cnr = 4.0
        
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=followup_subject,
            session="followup1",
            days_from_baseline=180
        )
        
        # Calculate all trends
        trends = longitudinal_service.calculate_all_trends_for_subject("sub-001")
        
        assert len(trends) > 0
        metric_names = [trend.metric_name for trend in trends]
        assert "snr" in metric_names
        assert "cnr" in metric_names
        assert "fber" in metric_names
        assert "efc" in metric_names
        assert "fwhm_avg" in metric_names
    
    def test_detect_age_group_transitions(self, longitudinal_service, sample_processed_subject):
        """Test detecting age group transitions."""
        # Add baseline in young adult group
        baseline_subject = sample_processed_subject.model_copy()
        baseline_subject.subject_info.age = 34.0  # Young adult
        
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=baseline_subject,
            session="baseline",
            days_from_baseline=0
        )
        
        # Add follow-up in middle age group
        followup_subject = sample_processed_subject.model_copy()
        followup_subject.subject_info.age = 37.0  # Middle age
        followup_subject.subject_info.session = "followup1"
        
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=followup_subject,
            session="followup1",
            days_from_baseline=1095  # 3 years
        )
        
        # Detect transitions
        transitions = longitudinal_service.detect_age_group_transitions("sub-001")
        
        assert len(transitions) == 1
        assert transitions[0]['from_age_group'] == 'young_adult'
        assert transitions[0]['to_age_group'] == 'middle_age'
        assert transitions[0]['age_at_transition'] == 37.0
    
    def test_get_study_longitudinal_summary(self, longitudinal_service, sample_processed_subject):
        """Test generating study longitudinal summary."""
        study_name = "Test Study"
        
        # Add subjects with multiple timepoints
        for subject_num in range(3):
            subject_id = f"sub-{subject_num:03d}"
            
            for tp_num, days in enumerate([0, 180, 360]):
                subject = sample_processed_subject.model_copy()
                subject.subject_info.subject_id = subject_id
                subject.subject_info.age = 25.0 + (tp_num * 0.5)
                subject.subject_info.session = f"tp{tp_num+1}"
                
                longitudinal_service.add_subject_timepoint(
                    subject_id=subject_id,
                    processed_subject=subject,
                    session=f"tp{tp_num+1}",
                    days_from_baseline=days,
                    study_name=study_name
                )
        
        # Generate summary
        summary = longitudinal_service.get_study_longitudinal_summary(study_name)
        
        assert summary is not None
        assert summary.study_name == study_name
        assert summary.total_subjects == 3
        assert summary.total_timepoints == 9
        assert summary.timepoints_per_subject['mean'] == 3.0
        assert summary.follow_up_duration['max'] == 360
    
    def test_export_longitudinal_csv(self, longitudinal_service, sample_processed_subject):
        """Test exporting longitudinal data as CSV."""
        # Add test data
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=sample_processed_subject,
            session="baseline",
            days_from_baseline=0,
            study_name="Test Study"
        )
        
        # Export data
        filepath = longitudinal_service.export_longitudinal_data("Test Study", "csv")
        
        assert filepath.endswith('.csv')
        assert os.path.exists(filepath)
        
        # Verify file content
        with open(filepath, 'r') as f:
            content = f.read()
            assert 'subject_id' in content
            assert 'sub-001' in content
            assert 'baseline' in content
        
        # Cleanup
        os.unlink(filepath)
    
    def test_export_longitudinal_json(self, longitudinal_service, sample_processed_subject):
        """Test exporting longitudinal data as JSON."""
        # Add test data
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=sample_processed_subject,
            session="baseline",
            days_from_baseline=0,
            study_name="Test Study"
        )
        
        # Export data
        filepath = longitudinal_service.export_longitudinal_data("Test Study", "json")
        
        assert filepath.endswith('.json')
        assert os.path.exists(filepath)
        
        # Verify file content
        import json
        with open(filepath, 'r') as f:
            data = json.load(f)
            assert data['study_name'] == "Test Study"
            assert len(data['subjects']) == 1
            assert data['subjects'][0]['subject_id'] == "sub-001"
        
        # Cleanup
        os.unlink(filepath)
    
    def test_insufficient_data_for_trend(self, longitudinal_service, sample_processed_subject):
        """Test trend calculation with insufficient data."""
        # Add only one timepoint
        longitudinal_service.add_subject_timepoint(
            subject_id="sub-001",
            processed_subject=sample_processed_subject,
            session="baseline",
            days_from_baseline=0
        )
        
        # Try to calculate trend
        trend = longitudinal_service.calculate_metric_trend("sub-001", "snr")
        
        assert trend is None  # Should return None for insufficient data
    
    def test_nonexistent_subject(self, longitudinal_service):
        """Test operations on nonexistent subject."""
        # Try to get nonexistent subject
        subject = longitudinal_service.get_longitudinal_subject("nonexistent")
        assert subject is None
        
        # Try to calculate trend for nonexistent subject
        trend = longitudinal_service.calculate_metric_trend("nonexistent", "snr")
        assert trend is None
    
    def test_metric_direction_classification(self, longitudinal_service):
        """Test metric direction classification."""
        # Test higher-is-better metrics
        assert longitudinal_service._is_higher_better("snr") == True
        assert longitudinal_service._is_higher_better("cnr") == True
        assert longitudinal_service._is_higher_better("fber") == True
        
        # Test lower-is-better metrics
        assert longitudinal_service._is_higher_better("efc") == False
        assert longitudinal_service._is_higher_better("fwhm_avg") == False
        assert longitudinal_service._is_higher_better("cjv") == False
        
        # Test unknown metric (should default to higher is better)
        assert longitudinal_service._is_higher_better("unknown_metric") == True


class TestLongitudinalDatabase:
    """Test cases for longitudinal database operations."""
    
    def test_create_longitudinal_subject(self, temp_db):
        """Test creating a longitudinal subject."""
        subject_id = temp_db.create_longitudinal_subject(
            subject_id="sub-001",
            baseline_age=25.5,
            sex="F",
            study_name="Test Study"
        )
        
        assert subject_id > 0
        
        # Verify subject was created
        subject = temp_db.get_longitudinal_subject("sub-001")
        assert subject['subject_id'] == "sub-001"
        assert subject['baseline_age'] == 25.5
        assert subject['sex'] == "F"
        assert subject['study_name'] == "Test Study"
    
    def test_add_timepoint(self, temp_db):
        """Test adding a timepoint."""
        # Create subject first
        temp_db.create_longitudinal_subject("sub-001")
        
        # Add timepoint
        timepoint_id = temp_db.add_timepoint(
            timepoint_id="sub-001_baseline",
            subject_id="sub-001",
            session="baseline",
            age_at_scan=25.5,
            days_from_baseline=0,
            scan_date="2024-01-15T10:30:00",
            processed_data={"test": "data"}
        )
        
        assert timepoint_id > 0
        
        # Verify timepoint was added
        timepoints = temp_db.get_subject_timepoints("sub-001")
        assert len(timepoints) == 1
        assert timepoints[0]['timepoint_id'] == "sub-001_baseline"
        assert timepoints[0]['session'] == "baseline"
        assert timepoints[0]['processed_data'] == {"test": "data"}
    
    def test_calculate_and_store_trend(self, temp_db):
        """Test storing trend data."""
        # Create subject first
        temp_db.create_longitudinal_subject("sub-001")
        
        # Store trend
        trend_id = temp_db.calculate_and_store_trend(
            subject_id="sub-001",
            metric_name="snr",
            trend_direction="improving",
            trend_slope=0.01,
            trend_r_squared=0.85,
            trend_p_value=0.02,
            values_over_time=[{"timepoint": "tp1", "value": 15.0}],
            age_group_changes=["young_adult to middle_age"],
            quality_status_changes=[{"timepoint": "tp1", "status": "pass"}]
        )
        
        assert trend_id > 0
        
        # Verify trend was stored
        trends = temp_db.get_subject_trends("sub-001")
        assert len(trends) == 1
        assert trends[0]['metric_name'] == "snr"
        assert trends[0]['trend_direction'] == "improving"
        assert trends[0]['trend_slope'] == 0.01
    
    def test_delete_operations(self, temp_db):
        """Test deletion operations."""
        # Create test data
        temp_db.create_longitudinal_subject("sub-001")
        temp_db.add_timepoint("sub-001_baseline", "sub-001", "baseline")
        
        # Test timepoint deletion
        success = temp_db.delete_timepoint("sub-001_baseline")
        assert success == True
        
        timepoints = temp_db.get_subject_timepoints("sub-001")
        assert len(timepoints) == 0
        
        # Test subject deletion
        success = temp_db.delete_longitudinal_subject("sub-001")
        assert success == True
        
        subject = temp_db.get_longitudinal_subject("sub-001")
        assert subject is None