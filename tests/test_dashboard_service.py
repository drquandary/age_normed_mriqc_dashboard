"""
Tests for dashboard data service functionality.

This module tests the dashboard endpoints including summary data,
filtering, sorting, and WebSocket real-time updates.
"""

import pytest
import json
from datetime import datetime, timedelta
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch

from app.main import app
from app.models import (
    ProcessedSubject, SubjectInfo, MRIQCMetrics, QualityAssessment,
    QualityStatus, AgeGroup, ScanType, Sex, NormalizedMetrics
)


@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def sample_subjects():
    """Create sample processed subjects for testing."""
    subjects = []
    
    # Create diverse sample data
    test_data = [
        {
            "subject_id": "sub-001", "age": 25.0, "sex": Sex.FEMALE, "scan_type": ScanType.T1W,
            "snr": 15.2, "cnr": 3.5, "fber": 1800.0, "quality_status": QualityStatus.PASS,
            "composite_score": 85.0, "age_group": AgeGroup.YOUNG_ADULT
        },
        {
            "subject_id": "sub-002", "age": 45.0, "sex": Sex.MALE, "scan_type": ScanType.T1W,
            "snr": 12.8, "cnr": 2.9, "fber": 1600.0, "quality_status": QualityStatus.WARNING,
            "composite_score": 72.0, "age_group": AgeGroup.MIDDLE_AGE
        },
        {
            "subject_id": "sub-003", "age": 8.0, "sex": Sex.FEMALE, "scan_type": ScanType.BOLD,
            "snr": 8.5, "cnr": 2.1, "fber": 1200.0, "quality_status": QualityStatus.FAIL,
            "composite_score": 45.0, "age_group": AgeGroup.PEDIATRIC
        },
        {
            "subject_id": "sub-004", "age": 70.0, "sex": Sex.MALE, "scan_type": ScanType.T2W,
            "snr": 11.2, "cnr": 2.7, "fber": 1500.0, "quality_status": QualityStatus.PASS,
            "composite_score": 78.0, "age_group": AgeGroup.ELDERLY
        },
        {
            "subject_id": "sub-005", "age": 16.0, "sex": Sex.FEMALE, "scan_type": ScanType.BOLD,
            "snr": 13.5, "cnr": 3.2, "fber": 1700.0, "quality_status": QualityStatus.UNCERTAIN,
            "composite_score": 68.0, "age_group": AgeGroup.ADOLESCENT
        }
    ]
    
    for data in test_data:
        subject_info = SubjectInfo(
            subject_id=data["subject_id"],
            age=data["age"],
            sex=data["sex"],
            scan_type=data["scan_type"],
            acquisition_date=datetime.now() - timedelta(days=1)
        )
        
        raw_metrics = MRIQCMetrics(
            snr=data["snr"],
            cnr=data["cnr"],
            fber=data["fber"]
        )
        
        normalized_metrics = NormalizedMetrics(
            raw_metrics=raw_metrics,
            percentiles={"snr": 75.0, "cnr": 60.0, "fber": 80.0},
            z_scores={"snr": 0.67, "cnr": 0.25, "fber": 0.84},
            age_group=data["age_group"],
            normative_dataset="test_dataset"
        )
        
        quality_assessment = QualityAssessment(
            overall_status=data["quality_status"],
            metric_assessments={"snr": data["quality_status"], "cnr": data["quality_status"]},
            composite_score=data["composite_score"],
            recommendations=["Test recommendation"],
            flags=["test_flag"] if data["quality_status"] != QualityStatus.PASS else [],
            confidence=0.85
        )
        
        subject = ProcessedSubject(
            subject_info=subject_info,
            raw_metrics=raw_metrics,
            normalized_metrics=normalized_metrics,
            quality_assessment=quality_assessment,
            processing_timestamp=datetime.now()
        )
        
        subjects.append(subject)
    
    return subjects


class TestDashboardSummary:
    """Test dashboard summary endpoint."""
    
    def test_dashboard_summary_empty(self, client):
        """Test dashboard summary with no data."""
        response = client.get("/api/dashboard/summary")
        assert response.status_code == 200
        
        data = response.json()
        assert data["total_subjects"] == 0
        assert data["quality_distribution"] == {}
        assert data["exclusion_rate"] == 0.0
        assert "recent_activity" in data
        assert "alerts" in data    

    def test_dashboard_summary_with_data(self, client, sample_subjects):
        """Test dashboard summary with sample data."""
        # Mock the processed subjects store
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            response = client.get("/api/dashboard/summary")
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_subjects"] == 5
            assert data["quality_distribution"]["pass"] == 2
            assert data["quality_distribution"]["warning"] == 1
            assert data["quality_distribution"]["fail"] == 1
            assert data["quality_distribution"]["uncertain"] == 1
            assert data["exclusion_rate"] == 0.2  # 1 failed out of 5
            
            # Check age group distribution
            assert data["age_group_distribution"]["young_adult"] == 1
            assert data["age_group_distribution"]["middle_age"] == 1
            assert data["age_group_distribution"]["pediatric"] == 1
            assert data["age_group_distribution"]["elderly"] == 1
            assert data["age_group_distribution"]["adolescent"] == 1
            
            # Check scan type distribution
            assert data["scan_type_distribution"]["T1w"] == 2
            assert data["scan_type_distribution"]["BOLD"] == 2
            assert data["scan_type_distribution"]["T2w"] == 1
            
            # Check metric statistics
            assert "snr" in data["metric_statistics"]
            assert "cnr" in data["metric_statistics"]
            assert "fber" in data["metric_statistics"]
    
    def test_dashboard_summary_batch_filter(self, client, sample_subjects):
        """Test dashboard summary with batch filter."""
        with patch('app.routes.processed_subjects_store', {"batch1": sample_subjects[:3], "batch2": sample_subjects[3:]}):
            response = client.get("/api/dashboard/summary?batch_id=batch1")
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_subjects"] == 3
            assert data["batch_id"] == "batch1"
    
    def test_dashboard_summary_alerts(self, client, sample_subjects):
        """Test dashboard summary alert generation."""
        # Create data with high exclusion rate
        high_fail_subjects = []
        for i, subject in enumerate(sample_subjects):
            if i < 3:  # Make 3 out of 5 fail (60% exclusion rate)
                subject.quality_assessment.overall_status = QualityStatus.FAIL
            high_fail_subjects.append(subject)
        
        with patch('app.routes.processed_subjects_store', {"test_batch": high_fail_subjects}):
            response = client.get("/api/dashboard/summary")
            assert response.status_code == 200
            
            data = response.json()
            alerts = data["alerts"]
            
            # Should have high exclusion rate alert
            exclusion_alerts = [a for a in alerts if "exclusion rate" in a["message"]]
            assert len(exclusion_alerts) > 0
            assert exclusion_alerts[0]["type"] == "warning"


class TestSubjectFiltering:
    """Test subject filtering and sorting endpoints."""
    
    def test_get_subjects_basic(self, client, sample_subjects):
        """Test basic subject retrieval."""
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            response = client.get("/api/subjects")
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_count"] == 5
            assert len(data["subjects"]) == 5
            assert data["page"] == 1
            assert data["page_size"] == 50
    
    def test_get_subjects_quality_filter(self, client, sample_subjects):
        """Test filtering by quality status."""
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            response = client.get("/api/subjects?quality_status=pass")
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_count"] == 2  # 2 subjects with pass status
            assert all(s["quality_assessment"]["overall_status"] == "pass" for s in data["subjects"])
            assert data["filters_applied"]["quality_status"] == "pass"
    
    def test_get_subjects_age_group_filter(self, client, sample_subjects):
        """Test filtering by age group."""
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            response = client.get("/api/subjects?age_group=pediatric")
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_count"] == 1  # 1 pediatric subject
            assert data["subjects"][0]["normalized_metrics"]["age_group"] == "pediatric"
    
    def test_get_subjects_scan_type_filter(self, client, sample_subjects):
        """Test filtering by scan type."""
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            response = client.get("/api/subjects?scan_type=BOLD")
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_count"] == 2  # 2 BOLD subjects
            assert all(s["subject_info"]["scan_type"] == "BOLD" for s in data["subjects"])
    
    def test_get_subjects_sorting(self, client, sample_subjects):
        """Test subject sorting."""
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            # Sort by age ascending
            response = client.get("/api/subjects?sort_by=age&sort_order=asc")
            assert response.status_code == 200
            
            data = response.json()
            ages = [s["subject_info"]["age"] for s in data["subjects"]]
            assert ages == sorted(ages)  # Should be sorted ascending
            assert data["sort_applied"]["sort_by"] == "age"
            assert data["sort_applied"]["sort_order"] == "asc"
            
            # Sort by composite score descending
            response = client.get("/api/subjects?sort_by=composite_score&sort_order=desc")
            assert response.status_code == 200
            
            data = response.json()
            scores = [s["quality_assessment"]["composite_score"] for s in data["subjects"]]
            assert scores == sorted(scores, reverse=True)  # Should be sorted descending
    
    def test_get_subjects_pagination(self, client, sample_subjects):
        """Test subject pagination."""
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            # Get first page with page size 2
            response = client.get("/api/subjects?page=1&page_size=2")
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_count"] == 5
            assert len(data["subjects"]) == 2
            assert data["page"] == 1
            assert data["page_size"] == 2
            
            # Get second page
            response = client.get("/api/subjects?page=2&page_size=2")
            assert response.status_code == 200
            
            data = response.json()
            assert len(data["subjects"]) == 2
            assert data["page"] == 2
    
    def test_advanced_filtering(self, client, sample_subjects):
        """Test advanced filtering endpoint."""
        request_body = {
            "filter_criteria": {
                "quality_status": ["pass", "warning"],
                "age_range": {"min": 20, "max": 50},
                "scan_type": ["T1w"],
                "metric_filters": {
                    "snr": {"min": 12.0, "max": 20.0}
                }
            }
        }
        
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            response = client.post("/api/subjects/filter", json=request_body)
            if response.status_code != 200:
                print(f"Error response: {response.json()}")
            assert response.status_code == 200
            
            data = response.json()
            # Should match subjects that meet all criteria
            for subject in data["subjects"]:
                assert subject["quality_assessment"]["overall_status"] in ["pass", "warning"]
                assert 20 <= subject["subject_info"]["age"] <= 50
                assert subject["subject_info"]["scan_type"] == "T1w"
                assert 12.0 <= subject["raw_metrics"]["snr"] <= 20.0
    
    def test_text_search_filtering(self, client, sample_subjects):
        """Test text search functionality."""
        request_body = {
            "filter_criteria": {
                "search_text": "sub-001"
            }
        }
        
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            response = client.post("/api/subjects/filter", json=request_body)
            if response.status_code != 200:
                print(f"Error response: {response.json()}")
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_count"] == 1
            assert data["subjects"][0]["subject_info"]["subject_id"] == "sub-001"


class TestMetricsSummary:
    """Test metrics summary endpoint."""
    
    def test_metrics_summary_basic(self, client, sample_subjects):
        """Test basic metrics summary."""
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            response = client.get("/api/dashboard/metrics/summary")
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_subjects"] == 5
            assert "metrics" in data
            
            # Check that metrics are included
            metrics = data["metrics"]
            assert "snr" in metrics
            assert "cnr" in metrics
            assert "fber" in metrics
            
            # Check metric structure
            snr_data = metrics["snr"]
            assert "basic_stats" in snr_data
            assert "percentiles" in snr_data
            assert "outliers" in snr_data
            assert "quality_breakdown" in snr_data
            
            # Check basic stats
            basic_stats = snr_data["basic_stats"]
            assert "mean" in basic_stats
            assert "median" in basic_stats
            assert "std" in basic_stats
            assert "count" in basic_stats
    
    def test_metrics_summary_specific_metrics(self, client, sample_subjects):
        """Test metrics summary with specific metrics filter."""
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            response = client.get("/api/dashboard/metrics/summary?metric_names=snr&metric_names=cnr")
            assert response.status_code == 200
            
            data = response.json()
            metrics = data["metrics"]
            
            # Should only include requested metrics
            assert "snr" in metrics
            assert "cnr" in metrics
            assert "fber" not in metrics
    
    def test_metrics_summary_quality_breakdown(self, client, sample_subjects):
        """Test quality breakdown in metrics summary."""
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            response = client.get("/api/dashboard/metrics/summary")
            assert response.status_code == 200
            
            data = response.json()
            snr_data = data["metrics"]["snr"]
            quality_breakdown = snr_data["quality_breakdown"]
            
            # Check that all quality statuses are represented
            assert "pass" in quality_breakdown
            assert "warning" in quality_breakdown
            assert "fail" in quality_breakdown
            assert "uncertain" in quality_breakdown
            
            # Check structure of quality breakdown
            pass_data = quality_breakdown["pass"]
            assert "count" in pass_data
            assert "mean" in pass_data
            assert "std" in pass_data


class TestErrorHandling:
    """Test error handling in dashboard endpoints."""
    
    def test_dashboard_summary_nonexistent_batch(self, client):
        """Test dashboard summary with nonexistent batch."""
        response = client.get("/api/dashboard/summary?batch_id=nonexistent")
        assert response.status_code == 404
        assert "Batch not found" in response.json()["detail"]
    
    def test_subjects_nonexistent_batch(self, client):
        """Test subjects endpoint with nonexistent batch."""
        response = client.get("/api/subjects?batch_id=nonexistent")
        assert response.status_code == 404
        assert "Batch not found" in response.json()["detail"]
    
    def test_invalid_sort_parameters(self, client, sample_subjects):
        """Test invalid sort parameters."""
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            # Invalid sort order
            response = client.get("/api/subjects?sort_by=age&sort_order=invalid")
            assert response.status_code == 422  # Validation error
    
    def test_invalid_pagination_parameters(self, client, sample_subjects):
        """Test invalid pagination parameters."""
        with patch('app.routes.processed_subjects_store', {"test_batch": sample_subjects}):
            # Invalid page number
            response = client.get("/api/subjects?page=0")
            assert response.status_code == 422  # Validation error
            
            # Invalid page size
            response = client.get("/api/subjects?page_size=0")
            assert response.status_code == 422  # Validation error


class TestPerformance:
    """Test performance aspects of dashboard endpoints."""
    
    def test_large_dataset_pagination(self, client):
        """Test pagination with large dataset."""
        # Create a large number of subjects
        large_subjects = []
        for i in range(1000):
            subject_info = SubjectInfo(
                subject_id=f"sub-{i:04d}",
                age=25.0 + (i % 50),
                scan_type=ScanType.T1W
            )
            
            raw_metrics = MRIQCMetrics(snr=10.0 + (i % 10))
            
            quality_assessment = QualityAssessment(
                overall_status=QualityStatus.PASS,
                metric_assessments={"snr": QualityStatus.PASS},
                composite_score=75.0,
                confidence=0.85
            )
            
            subject = ProcessedSubject(
                subject_info=subject_info,
                raw_metrics=raw_metrics,
                quality_assessment=quality_assessment
            )
            
            large_subjects.append(subject)
        
        with patch('app.routes.processed_subjects_store', {"large_batch": large_subjects}):
            # Test that pagination works efficiently
            response = client.get("/api/subjects?page=1&page_size=100")
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_count"] == 1000
            assert len(data["subjects"]) == 100
            assert data["page"] == 1
    
    def test_filtering_performance(self, client):
        """Test filtering performance with complex criteria."""
        # This test would be expanded in a real scenario to measure response times
        # and ensure they meet performance requirements
        pass


if __name__ == "__main__":
    pytest.main([__file__])