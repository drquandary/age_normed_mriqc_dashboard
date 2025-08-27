"""
Tests for export API endpoints.

This module tests the FastAPI endpoints for CSV export, PDF report generation,
and study summary export functionality.
"""

import pytest
import json
import io
import csv
from datetime import datetime
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient

from app.main import app
from app.models import (
    ProcessedSubject, SubjectInfo, MRIQCMetrics, QualityAssessment,
    NormalizedMetrics, QualityStatus, AgeGroup, ScanType, Sex
)
from app.routes import processed_subjects_store, batch_status_store


# Global fixtures for all test classes
@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)

@pytest.fixture
def sample_subjects_data():
    """Create sample subjects data for testing."""
    subjects = []
    
    # Subject 1: Pass
    subject1 = ProcessedSubject(
        subject_info=SubjectInfo(
            subject_id="sub-001",
            age=25.5,
            sex=Sex.FEMALE,
            scan_type=ScanType.T1W
        ),
        raw_metrics=MRIQCMetrics(
            snr=12.5,
            cnr=3.2,
            fber=1500.0
        ),
        normalized_metrics=NormalizedMetrics(
            raw_metrics=MRIQCMetrics(snr=12.5, cnr=3.2),
            percentiles={"snr": 75.0, "cnr": 60.0},
            z_scores={"snr": 0.67, "cnr": 0.25},
            age_group=AgeGroup.YOUNG_ADULT,
            normative_dataset="HCP-YA"
        ),
        quality_assessment=QualityAssessment(
            overall_status=QualityStatus.PASS,
            metric_assessments={"snr": QualityStatus.PASS},
            composite_score=78.5,
            confidence=0.85
        )
    )
    subjects.append(subject1)
    
    # Subject 2: Fail
    subject2 = ProcessedSubject(
        subject_info=SubjectInfo(
            subject_id="sub-002",
            age=65.0,
            sex=Sex.MALE,
            scan_type=ScanType.T1W
        ),
        raw_metrics=MRIQCMetrics(
            snr=8.0,
            cnr=2.0,
            fber=800.0
        ),
        normalized_metrics=NormalizedMetrics(
            raw_metrics=MRIQCMetrics(snr=8.0, cnr=2.0),
            percentiles={"snr": 15.0, "cnr": 20.0},
            z_scores={"snr": -1.5, "cnr": -1.8},
            age_group=AgeGroup.ELDERLY,
            normative_dataset="elderly_norms"
        ),
        quality_assessment=QualityAssessment(
            overall_status=QualityStatus.FAIL,
            metric_assessments={"snr": QualityStatus.FAIL},
            composite_score=35.2,
            confidence=0.95
        )
    )
    subjects.append(subject2)
    
    return subjects

@pytest.fixture
def setup_test_data(sample_subjects_data):
    """Set up test data in the global stores."""
    batch_id = "test-batch-001"
    
    # Clear existing data
    processed_subjects_store.clear()
    batch_status_store.clear()
    
    # Add test data
    processed_subjects_store[batch_id] = sample_subjects_data
    batch_status_store[batch_id] = {
        'batch_id': batch_id,
        'status': 'completed',
        'total_subjects': len(sample_subjects_data),
        'created_at': datetime.now()
    }
    
    yield batch_id
    
    # Cleanup
    processed_subjects_store.clear()
    batch_status_store.clear()


class TestExportEndpoints:
    """Test export API endpoints."""
    pass


class TestCSVExportEndpoint:
    """Test CSV export endpoint."""
    
    def test_export_csv_basic(self, client, setup_test_data):
        """Test basic CSV export."""
        batch_id = setup_test_data
        
        response = client.post("/api/export/csv", json={
            "batch_ids": [batch_id],
            "include_raw_metrics": True,
            "include_normalized_metrics": True,
            "include_quality_assessment": True
        })
        
        assert response.status_code == 200
        assert response.headers["content-type"] == "text/csv; charset=utf-8"
        assert "attachment" in response.headers["content-disposition"]
        
        # Verify CSV content
        csv_content = response.text
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)
        
        assert len(rows) == 2  # Two subjects
        assert rows[0]['subject_id'] == 'sub-001'
        assert rows[1]['subject_id'] == 'sub-002'
    
    def test_export_csv_with_filters(self, client, setup_test_data):
        """Test CSV export with quality status filter."""
        batch_id = setup_test_data
        
        response = client.post("/api/export/csv", json={
            "batch_ids": [batch_id],
            "quality_status_filter": ["pass"]
        })
        
        assert response.status_code == 200
        
        csv_content = response.text
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)
        
        # Should only have the passing subject
        assert len(rows) == 1
        assert rows[0]['subject_id'] == 'sub-001'
        assert rows[0]['overall_quality_status'] == 'pass'
    
    def test_export_csv_age_group_filter(self, client, setup_test_data):
        """Test CSV export with age group filter."""
        batch_id = setup_test_data
        
        response = client.post("/api/export/csv", json={
            "batch_ids": [batch_id],
            "age_group_filter": ["elderly"]
        })
        
        assert response.status_code == 200
        
        csv_content = response.text
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)
        
        # Should only have the elderly subject
        assert len(rows) == 1
        assert rows[0]['subject_id'] == 'sub-002'
    
    def test_export_csv_all_batches(self, client, setup_test_data):
        """Test CSV export for all batches."""
        response = client.post("/api/export/csv", json={
            "include_raw_metrics": True
        })
        
        assert response.status_code == 200
        
        csv_content = response.text
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)
        
        assert len(rows) == 2  # All subjects from all batches
    
    def test_export_csv_custom_options(self, client, setup_test_data):
        """Test CSV export with custom options."""
        batch_id = setup_test_data
        
        response = client.post("/api/export/csv", json={
            "batch_ids": [batch_id],
            "include_raw_metrics": False,
            "include_normalized_metrics": True,
            "include_quality_assessment": False,
            "study_name": "Test Study"
        })
        
        assert response.status_code == 200
        assert "Test Study" in response.headers["content-disposition"]
        
        csv_content = response.text
        reader = csv.DictReader(io.StringIO(csv_content))
        fieldnames = reader.fieldnames
        
        # Should not have raw metrics
        assert not any('raw_' in field for field in fieldnames)
        # Should have normalized metrics
        assert any('percentile_' in field for field in fieldnames)
        # Should not have quality assessment
        assert 'overall_quality_status' not in fieldnames
    
    def test_export_csv_no_subjects_found(self, client):
        """Test CSV export when no subjects found."""
        response = client.post("/api/export/csv", json={
            "batch_ids": ["nonexistent-batch"]
        })
        
        assert response.status_code == 404
        assert "No subjects found for export" in response.json()["detail"]
    
    def test_export_csv_no_subjects_match_filter(self, client, setup_test_data):
        """Test CSV export when no subjects match filter."""
        batch_id = setup_test_data
        
        response = client.post("/api/export/csv", json={
            "batch_ids": [batch_id],
            "quality_status_filter": ["warning"]  # No subjects have warning status
        })
        
        assert response.status_code == 404
        assert "No subjects match the specified filters" in response.json()["detail"]


class TestPDFExportEndpoint:
    """Test PDF export endpoint."""
    
    def test_export_pdf_basic(self, client, setup_test_data):
        """Test basic PDF export."""
        batch_id = setup_test_data
        
        response = client.post("/api/export/pdf", json={
            "batch_ids": [batch_id],
            "study_name": "Test Study"
        })
        
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/pdf"
        assert "attachment" in response.headers["content-disposition"]
        assert "Test Study" in response.headers["content-disposition"]
        
        # Verify PDF content
        pdf_content = response.content
        assert pdf_content.startswith(b'%PDF')
        assert len(pdf_content) > 1000  # Should be substantial content
    
    def test_export_pdf_with_filters(self, client, setup_test_data):
        """Test PDF export with filters."""
        batch_id = setup_test_data
        
        response = client.post("/api/export/pdf", json={
            "batch_ids": [batch_id],
            "quality_status_filter": ["pass", "fail"]
        })
        
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/pdf"
    
    def test_export_pdf_no_subjects(self, client):
        """Test PDF export when no subjects found."""
        response = client.post("/api/export/pdf", json={
            "batch_ids": ["nonexistent-batch"]
        })
        
        assert response.status_code == 404
        assert "No subjects found for export" in response.json()["detail"]
    
    @patch('app.export_engine.ExportEngine.generate_pdf_report')
    def test_export_pdf_handles_errors(self, mock_pdf, client, setup_test_data):
        """Test PDF export handles generation errors."""
        batch_id = setup_test_data
        mock_pdf.side_effect = Exception("PDF generation failed")
        
        response = client.post("/api/export/pdf", json={
            "batch_ids": [batch_id]
        })
        
        assert response.status_code == 500
        assert "PDF export failed" in response.json()["detail"]


class TestStudySummaryEndpoint:
    """Test study summary export endpoint."""
    
    def test_export_study_summary_json(self, client, setup_test_data):
        """Test study summary export in JSON format."""
        batch_id = setup_test_data
        
        response = client.get(f"/api/export/study-summary/{batch_id}?format=json")
        
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"
        
        summary_data = response.json()
        assert summary_data["total_subjects"] == 2
        assert "quality_distribution" in summary_data
        assert "age_group_distribution" in summary_data
        assert "metric_statistics" in summary_data
        
        # Verify quality distribution
        quality_dist = summary_data["quality_distribution"]
        assert quality_dist["pass"] == 1
        assert quality_dist["fail"] == 1
    
    def test_export_study_summary_csv(self, client, setup_test_data):
        """Test study summary export in CSV format."""
        batch_id = setup_test_data
        
        response = client.get(f"/api/export/study-summary/{batch_id}?format=csv")
        
        assert response.status_code == 200
        assert response.headers["content-type"] == "text/csv; charset=utf-8"
        assert "attachment" in response.headers["content-disposition"]
        
        csv_content = response.text
        assert "Study Summary Report" in csv_content
        assert "Quality Distribution" in csv_content
        assert "Metric Statistics" in csv_content
    
    def test_export_study_summary_invalid_format(self, client, setup_test_data):
        """Test study summary export with invalid format."""
        batch_id = setup_test_data
        
        response = client.get(f"/api/export/study-summary/{batch_id}?format=xml")
        
        assert response.status_code == 422  # Validation error
    
    def test_export_study_summary_nonexistent_batch(self, client):
        """Test study summary export for nonexistent batch."""
        response = client.get("/api/export/study-summary/nonexistent-batch")
        
        assert response.status_code == 404
        assert "Batch not found" in response.json()["detail"]
    
    def test_export_study_summary_empty_batch(self, client):
        """Test study summary export for empty batch."""
        batch_id = "empty-batch"
        processed_subjects_store[batch_id] = []
        
        try:
            response = client.get(f"/api/export/study-summary/{batch_id}")
            
            assert response.status_code == 404
            assert "No subjects found in batch" in response.json()["detail"]
        finally:
            # Cleanup
            if batch_id in processed_subjects_store:
                del processed_subjects_store[batch_id]


class TestBatchListEndpoint:
    """Test batch list endpoint."""
    
    def test_get_available_batches(self, client, setup_test_data):
        """Test getting list of available batches."""
        batch_id = setup_test_data
        
        response = client.get("/api/export/batch-list")
        
        assert response.status_code == 200
        
        data = response.json()
        assert "batches" in data
        assert "total_batches" in data
        assert "total_subjects" in data
        
        assert data["total_batches"] == 1
        assert data["total_subjects"] == 2
        
        batch_info = data["batches"][0]
        assert batch_info["batch_id"] == batch_id
        assert batch_info["subject_count"] == 2
        assert "quality_distribution" in batch_info
        assert "scan_types" in batch_info
        assert "age_range" in batch_info
    
    def test_get_available_batches_empty(self, client):
        """Test getting batch list when no batches exist."""
        # Ensure stores are empty
        processed_subjects_store.clear()
        batch_status_store.clear()
        
        response = client.get("/api/export/batch-list")
        
        assert response.status_code == 200
        
        data = response.json()
        assert data["total_batches"] == 0
        assert data["total_subjects"] == 0
        assert len(data["batches"]) == 0
    
    def test_get_available_batches_multiple(self, client, sample_subjects_data):
        """Test getting batch list with multiple batches."""
        # Clear existing data
        processed_subjects_store.clear()
        batch_status_store.clear()
        
        # Add multiple batches
        for i in range(3):
            batch_id = f"batch-{i}"
            processed_subjects_store[batch_id] = sample_subjects_data
            batch_status_store[batch_id] = {
                'batch_id': batch_id,
                'status': 'completed',
                'total_subjects': len(sample_subjects_data),
                'created_at': datetime.now()
            }
        
        try:
            response = client.get("/api/export/batch-list")
            
            assert response.status_code == 200
            
            data = response.json()
            assert data["total_batches"] == 3
            assert data["total_subjects"] == 6  # 2 subjects * 3 batches
            assert len(data["batches"]) == 3
            
            # Should be sorted by created_at (most recent first)
            batch_ids = [batch["batch_id"] for batch in data["batches"]]
            assert "batch-" in batch_ids[0]  # Should have batch prefix
            
        finally:
            # Cleanup
            processed_subjects_store.clear()
            batch_status_store.clear()


class TestExportEndpointsIntegration:
    """Integration tests for export endpoints."""
    
    def test_full_export_workflow(self, client, setup_test_data):
        """Test complete export workflow."""
        batch_id = setup_test_data
        
        # 1. Get batch list
        batch_list_response = client.get("/api/export/batch-list")
        assert batch_list_response.status_code == 200
        
        # 2. Export CSV
        csv_response = client.post("/api/export/csv", json={
            "batch_ids": [batch_id],
            "study_name": "Integration Test"
        })
        assert csv_response.status_code == 200
        
        # 3. Export PDF
        pdf_response = client.post("/api/export/pdf", json={
            "batch_ids": [batch_id],
            "study_name": "Integration Test"
        })
        assert pdf_response.status_code == 200
        
        # 4. Export study summary
        summary_response = client.get(f"/api/export/study-summary/{batch_id}")
        assert summary_response.status_code == 200
        
        # Verify all exports have consistent data
        csv_content = csv_response.text
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        csv_rows = list(csv_reader)
        
        summary_data = summary_response.json()
        
        assert len(csv_rows) == summary_data["total_subjects"]
    
    def test_export_with_complex_filters(self, client, setup_test_data):
        """Test export with complex filtering scenarios."""
        batch_id = setup_test_data
        
        # Test multiple quality status filters
        response = client.post("/api/export/csv", json={
            "batch_ids": [batch_id],
            "quality_status_filter": ["pass", "fail"],
            "age_group_filter": ["young_adult", "elderly"]
        })
        
        assert response.status_code == 200
        
        csv_content = response.text
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)
        
        # Should have both subjects (one pass, one fail)
        assert len(rows) == 2
        
        # Test restrictive filter
        response = client.post("/api/export/csv", json={
            "batch_ids": [batch_id],
            "quality_status_filter": ["pass"],
            "age_group_filter": ["elderly"]  # No elderly subjects with pass status
        })
        
        assert response.status_code == 404  # No subjects match filters
    
    def test_export_error_handling(self, client):
        """Test export endpoints handle various error conditions."""
        # Test with invalid batch ID
        response = client.post("/api/export/csv", json={
            "batch_ids": ["invalid-batch-id"]
        })
        assert response.status_code == 404
        
        # Test with empty request
        response = client.post("/api/export/csv", json={})
        assert response.status_code == 404  # No subjects found
        
        # Test study summary with invalid batch
        response = client.get("/api/export/study-summary/invalid-batch")
        assert response.status_code == 404


if __name__ == "__main__":
    pytest.main([__file__])