"""
Tests for longitudinal API endpoints.
"""

import pytest
import json
import tempfile
import os
from datetime import datetime
from fastapi.testclient import TestClient

from app.main import app
from app.models import (
    ProcessedSubject, SubjectInfo, MRIQCMetrics, QualityAssessment,
    ScanType, Sex, QualityStatus
)


@pytest.fixture
def client():
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def sample_processed_subject_data():
    """Create sample processed subject data for API testing."""
    return {
        "subject_info": {
            "subject_id": "sub-001",
            "age": 25.5,
            "sex": "F",
            "session": "baseline",
            "scan_type": "T1w",
            "acquisition_date": "2024-01-15T10:30:00"
        },
        "raw_metrics": {
            "snr": 15.2,
            "cnr": 3.8,
            "fber": 1420.0,
            "efc": 0.45,
            "fwhm_avg": 2.8
        },
        "quality_assessment": {
            "overall_status": "pass",
            "metric_assessments": {
                "snr": "pass",
                "cnr": "pass",
                "fber": "pass",
                "efc": "pass",
                "fwhm_avg": "pass"
            },
            "composite_score": 85.0,
            "confidence": 0.9,
            "recommendations": [],
            "flags": [],
            "threshold_violations": {}
        },
        "processing_timestamp": "2024-01-15T14:30:00"
    }


class TestLongitudinalAPI:
    """Test cases for longitudinal API endpoints."""
    
    def test_add_subject_timepoint(self, client, sample_processed_subject_data):
        """Test adding a timepoint via API."""
        response = client.post(
            "/api/longitudinal/subjects/sub-001/timepoints?session=baseline&days_from_baseline=0&study_name=Test Study",
            json=sample_processed_subject_data
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["timepoint_id"] == "sub-001_baseline"
        assert data["subject_id"] == "sub-001"
        assert data["session"] == "baseline"
        assert "message" in data
    
    def test_add_multiple_timepoints(self, client, sample_processed_subject_data):
        """Test adding multiple timepoints for the same subject."""
        # Add baseline
        response = client.post(
            "/api/longitudinal/subjects/sub-001/timepoints?session=baseline&days_from_baseline=0",
            json=sample_processed_subject_data
        )
        assert response.status_code == 200
        
        # Add follow-up
        followup_data = sample_processed_subject_data.copy()
        followup_data["subject_info"]["age"] = 26.0
        followup_data["subject_info"]["session"] = "followup1"
        followup_data["raw_metrics"]["snr"] = 16.0  # Improved
        
        response = client.post(
            "/api/longitudinal/subjects/sub-001/timepoints?session=followup1&days_from_baseline=180",
            json=followup_data
        )
        assert response.status_code == 200
        
        data = response.json()
        assert data["timepoint_id"] == "sub-001_followup1"
    
    def test_get_longitudinal_subject(self, client, sample_processed_subject_data):
        """Test retrieving longitudinal subject data."""
        # First add some timepoints
        client.post(
            "/api/longitudinal/subjects/sub-002/timepoints?session=baseline&days_from_baseline=0",
            json=sample_processed_subject_data
        )
        
        followup_data = sample_processed_subject_data.copy()
        followup_data["subject_info"]["age"] = 26.0
        followup_data["subject_info"]["session"] = "followup1"
        
        client.post(
            "/api/longitudinal/subjects/sub-002/timepoints?session=followup1&days_from_baseline=180",
            json=followup_data
        )
        
        # Retrieve subject
        response = client.get("/api/longitudinal/subjects/sub-002")
        
        assert response.status_code == 200
        data = response.json()
        assert data["subject_id"] == "sub-002"
        assert data["num_timepoints"] == 2
        assert len(data["timepoints"]) == 2
        assert data["timepoints"][0]["session"] == "baseline"
        assert data["timepoints"][1]["session"] == "followup1"
    
    def test_get_nonexistent_subject(self, client):
        """Test retrieving a nonexistent subject."""
        response = client.get("/api/longitudinal/subjects/nonexistent")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()
    
    def test_get_longitudinal_subjects_list(self, client, sample_processed_subject_data):
        """Test getting list of longitudinal subjects."""
        # Add some test subjects
        for i in range(3):
            subject_data = sample_processed_subject_data.copy()
            subject_data["subject_info"]["subject_id"] = f"sub-{i:03d}"
            
            client.post(
                f"/api/longitudinal/subjects/sub-{i:03d}/timepoints?session=baseline&study_name=Test Study",
                json=subject_data
            )
        
        # Get subjects list
        response = client.get("/api/longitudinal/subjects?study_name=Test Study")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 3
        assert len(data["subjects"]) == 3
        assert data["study_name"] == "Test Study"
    
    def test_get_longitudinal_subjects_pagination(self, client, sample_processed_subject_data):
        """Test pagination of longitudinal subjects list."""
        # Add test subjects
        for i in range(5):
            subject_data = sample_processed_subject_data.copy()
            subject_data["subject_info"]["subject_id"] = f"sub-{i:03d}"
            
            client.post(
                f"/api/longitudinal/subjects/sub-{i:03d}/timepoints?session=baseline",
                json=subject_data
            )
        
        # Test pagination
        response = client.get("/api/longitudinal/subjects?page=1&page_size=2")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 5
        assert len(data["subjects"]) == 2
        assert data["page"] == 1
        assert data["page_size"] == 2
        
        # Test second page
        response = client.get("/api/longitudinal/subjects?page=2&page_size=2")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["subjects"]) == 2
        assert data["page"] == 2
    
    def test_get_subject_trends(self, client, sample_processed_subject_data):
        """Test getting trends for a subject."""
        # Add timepoints with different metric values
        baseline_data = sample_processed_subject_data.copy()
        baseline_data["raw_metrics"]["snr"] = 12.0
        
        client.post(
            "/api/longitudinal/subjects/sub-003/timepoints?session=baseline&days_from_baseline=0",
            json=baseline_data
        )
        
        followup_data = sample_processed_subject_data.copy()
        followup_data["subject_info"]["age"] = 26.0
        followup_data["subject_info"]["session"] = "followup1"
        followup_data["raw_metrics"]["snr"] = 18.0  # Significant improvement
        
        client.post(
            "/api/longitudinal/subjects/sub-003/timepoints?session=followup1&days_from_baseline=180",
            json=followup_data
        )
        
        # Get trends
        response = client.get("/api/longitudinal/subjects/sub-003/trends")
        
        assert response.status_code == 200
        trends = response.json()
        assert len(trends) > 0
        
        # Find SNR trend
        snr_trend = next((t for t in trends if t["metric_name"] == "snr"), None)
        assert snr_trend is not None
        assert snr_trend["trend_direction"] == "improving"
        assert snr_trend["trend_slope"] > 0
        assert len(snr_trend["values_over_time"]) == 2
    
    def test_get_subject_metric_trend(self, client, sample_processed_subject_data):
        """Test getting trend for a specific metric."""
        # Add timepoints
        baseline_data = sample_processed_subject_data.copy()
        baseline_data["raw_metrics"]["cnr"] = 3.0
        
        client.post(
            "/api/longitudinal/subjects/sub-004/timepoints?session=baseline&days_from_baseline=0",
            json=baseline_data
        )
        
        followup_data = sample_processed_subject_data.copy()
        followup_data["subject_info"]["session"] = "followup1"
        followup_data["raw_metrics"]["cnr"] = 4.5  # Improvement
        
        client.post(
            "/api/longitudinal/subjects/sub-004/timepoints?session=followup1&days_from_baseline=180",
            json=followup_data
        )
        
        # Get specific metric trend
        response = client.get("/api/longitudinal/subjects/sub-004/trends/cnr")
        
        assert response.status_code == 200
        trend = response.json()
        assert trend["metric_name"] == "cnr"
        assert trend["trend_direction"] == "improving"
        assert len(trend["values_over_time"]) == 2
    
    def test_get_nonexistent_metric_trend(self, client, sample_processed_subject_data):
        """Test getting trend for nonexistent metric/subject."""
        # Add one timepoint (insufficient for trend)
        client.post(
            "/api/longitudinal/subjects/sub-005/timepoints?session=baseline",
            json=sample_processed_subject_data
        )
        
        # Try to get trend (should fail due to insufficient data)
        response = client.get("/api/longitudinal/subjects/sub-005/trends/snr")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()
    
    def test_calculate_subject_trends(self, client, sample_processed_subject_data):
        """Test calculating all trends for a subject."""
        # Add timepoints
        client.post(
            "/api/longitudinal/subjects/sub-006/timepoints?session=baseline&days_from_baseline=0",
            json=sample_processed_subject_data
        )
        
        followup_data = sample_processed_subject_data.copy()
        followup_data["subject_info"]["session"] = "followup1"
        followup_data["raw_metrics"]["snr"] = 16.0
        
        client.post(
            "/api/longitudinal/subjects/sub-006/timepoints?session=followup1&days_from_baseline=180",
            json=followup_data
        )
        
        # Calculate trends
        response = client.post("/api/longitudinal/subjects/sub-006/calculate-trends")
        
        assert response.status_code == 200
        data = response.json()
        assert data["subject_id"] == "sub-006"
        assert data["trends_calculated"] > 0
        assert "snr" in data["metrics"]
        assert "trend_directions" in data
    
    def test_get_study_longitudinal_summary(self, client, sample_processed_subject_data):
        """Test getting study longitudinal summary."""
        study_name = "Summary Test Study"
        
        # Add multiple subjects with multiple timepoints
        for subject_num in range(2):
            for tp_num, days in enumerate([0, 180]):
                subject_data = sample_processed_subject_data.copy()
                subject_data["subject_info"]["subject_id"] = f"sub-{subject_num:03d}"
                subject_data["subject_info"]["age"] = 25.0 + (tp_num * 0.5)
                subject_data["subject_info"]["session"] = f"tp{tp_num+1}"
                
                client.post(
                    f"/api/longitudinal/subjects/sub-{subject_num:03d}/timepoints"
                    f"?session=tp{tp_num+1}&days_from_baseline={days}&study_name={study_name}",
                    json=subject_data
                )
        
        # Get summary
        response = client.get(f"/api/longitudinal/studies/{study_name}/summary")
        
        assert response.status_code == 200
        summary = response.json()
        assert summary["study_name"] == study_name
        assert summary["total_subjects"] == 2
        assert summary["total_timepoints"] == 4
    
    def test_get_subject_age_transitions(self, client, sample_processed_subject_data):
        """Test getting age group transitions for a subject."""
        # Add baseline in young adult group
        baseline_data = sample_processed_subject_data.copy()
        baseline_data["subject_info"]["age"] = 34.0  # Young adult
        
        client.post(
            "/api/longitudinal/subjects/sub-007/timepoints?session=baseline&days_from_baseline=0",
            json=baseline_data
        )
        
        # Add follow-up in middle age group
        followup_data = sample_processed_subject_data.copy()
        followup_data["subject_info"]["age"] = 37.0  # Middle age
        followup_data["subject_info"]["session"] = "followup1"
        
        client.post(
            "/api/longitudinal/subjects/sub-007/timepoints?session=followup1&days_from_baseline=1095",
            json=followup_data
        )
        
        # Get age transitions
        response = client.get("/api/longitudinal/subjects/sub-007/age-transitions")
        
        assert response.status_code == 200
        data = response.json()
        assert data["subject_id"] == "sub-007"
        assert data["transition_count"] == 1
        assert len(data["transitions"]) == 1
        assert data["transitions"][0]["from_age_group"] == "young_adult"
        assert data["transitions"][0]["to_age_group"] == "middle_age"
    
    def test_export_longitudinal_data_csv(self, client, sample_processed_subject_data):
        """Test exporting longitudinal data as CSV."""
        study_name = "Export Test Study"
        
        # Add test data
        client.post(
            f"/api/longitudinal/subjects/sub-008/timepoints?session=baseline&study_name={study_name}",
            json=sample_processed_subject_data
        )
        
        # Export data
        response = client.post(f"/api/longitudinal/export?study_name={study_name}&format=csv")
        
        assert response.status_code == 200
        assert response.headers["content-type"] == "text/csv; charset=utf-8"
        assert "attachment" in response.headers["content-disposition"]
        
        # Verify CSV content
        content = response.content.decode()
        assert "subject_id" in content
        assert "sub-008" in content
    
    def test_export_longitudinal_data_json(self, client, sample_processed_subject_data):
        """Test exporting longitudinal data as JSON."""
        study_name = "JSON Export Test Study"
        
        # Add test data
        client.post(
            f"/api/longitudinal/subjects/sub-009/timepoints?session=baseline&study_name={study_name}",
            json=sample_processed_subject_data
        )
        
        # Export data
        response = client.post(f"/api/longitudinal/export?study_name={study_name}&format=json")
        
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json; charset=utf-8"
        assert "attachment" in response.headers["content-disposition"]
        
        # Verify JSON content
        content = response.content.decode()
        data = json.loads(content)
        assert data["study_name"] == study_name
        assert len(data["subjects"]) == 1
        assert data["subjects"][0]["subject_id"] == "sub-009"
    
    def test_export_invalid_format(self, client):
        """Test exporting with invalid format."""
        response = client.post("/api/longitudinal/export?format=invalid")
        
        assert response.status_code == 400
        assert "Format must be" in response.json()["detail"]
    
    def test_delete_longitudinal_subject(self, client, sample_processed_subject_data):
        """Test deleting a longitudinal subject."""
        # Add subject first
        client.post(
            "/api/longitudinal/subjects/sub-010/timepoints?session=baseline",
            json=sample_processed_subject_data
        )
        
        # Verify subject exists
        response = client.get("/api/longitudinal/subjects/sub-010")
        assert response.status_code == 200
        
        # Delete subject
        response = client.delete("/api/longitudinal/subjects/sub-010")
        
        assert response.status_code == 200
        data = response.json()
        assert data["subject_id"] == "sub-010"
        assert "deleted successfully" in data["message"]
        
        # Verify subject is gone
        response = client.get("/api/longitudinal/subjects/sub-010")
        assert response.status_code == 404
    
    def test_delete_nonexistent_subject(self, client):
        """Test deleting a nonexistent subject."""
        response = client.delete("/api/longitudinal/subjects/nonexistent")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()
    
    def test_delete_timepoint(self, client, sample_processed_subject_data):
        """Test deleting a specific timepoint."""
        # Add timepoint first
        client.post(
            "/api/longitudinal/subjects/sub-011/timepoints?session=baseline",
            json=sample_processed_subject_data
        )
        
        # Delete timepoint
        response = client.delete("/api/longitudinal/timepoints/sub-011_baseline")
        
        assert response.status_code == 200
        data = response.json()
        assert data["timepoint_id"] == "sub-011_baseline"
        assert "deleted successfully" in data["message"]
    
    def test_delete_nonexistent_timepoint(self, client):
        """Test deleting a nonexistent timepoint."""
        response = client.delete("/api/longitudinal/timepoints/nonexistent")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()
    
    def test_api_error_handling(self, client):
        """Test API error handling for various scenarios."""
        # Test invalid subject ID format
        invalid_data = {
            "subject_info": {
                "subject_id": "",  # Empty subject ID
                "scan_type": "T1w"
            },
            "raw_metrics": {},
            "quality_assessment": {
                "overall_status": "pass",
                "metric_assessments": {},
                "composite_score": 85.0,
                "confidence": 0.9
            }
        }
        
        response = client.post(
            "/api/longitudinal/subjects/test/timepoints",
            json=invalid_data
        )
        
        assert response.status_code == 422  # Validation error
    
    def test_concurrent_timepoint_addition(self, client, sample_processed_subject_data):
        """Test adding timepoints concurrently (simulated)."""
        subject_id = "sub-concurrent"
        
        # Add multiple timepoints rapidly
        responses = []
        for i in range(3):
            data = sample_processed_subject_data.copy()
            data["subject_info"]["session"] = f"tp{i+1}"
            
            response = client.post(
                f"/api/longitudinal/subjects/{subject_id}/timepoints"
                f"?session=tp{i+1}&days_from_baseline={i*90}",
                json=data
            )
            responses.append(response)
        
        # All should succeed
        for response in responses:
            assert response.status_code == 200
        
        # Verify all timepoints were added
        response = client.get(f"/api/longitudinal/subjects/{subject_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["num_timepoints"] == 3