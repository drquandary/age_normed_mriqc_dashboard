"""
Integration tests for Age-Normed MRIQC Dashboard API endpoints.

This module contains comprehensive tests for all API endpoints including
file upload, processing, subject retrieval, and batch status tracking.
"""

import pytest
import asyncio
import tempfile
import csv
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import UploadFile
import io

from app.main import app
from app.models import (
    QualityStatus, AgeGroup, ScanType, Sex, MRIQCMetrics, 
    SubjectInfo, ProcessedSubject, QualityAssessment
)
from app.routes import batch_status_store, processed_subjects_store


@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def sample_mriqc_csv():
    """Create sample MRIQC CSV file."""
    csv_content = """bids_name,snr,cnr,fber,efc,fwhm_avg,qi_1,cjv,age,sex
sub-001_T1w.nii.gz,12.5,3.2,1500.0,0.45,2.8,0.85,0.42,25.5,F
sub-002_T1w.nii.gz,10.8,2.9,1200.0,0.52,3.1,0.78,0.48,30.2,M
sub-003_T1w.nii.gz,8.2,2.1,900.0,0.68,3.8,0.65,0.62,45.8,F
"""
    return csv_content


@pytest.fixture
def sample_mriqc_file(sample_mriqc_csv):
    """Create temporary MRIQC CSV file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(sample_mriqc_csv)
        temp_path = Path(f.name)
    
    yield temp_path
    
    # Cleanup
    temp_path.unlink(missing_ok=True)


@pytest.fixture
def sample_processed_subject():
    """Create sample processed subject."""
    subject_info = SubjectInfo(
        subject_id="sub-001",
        age=25.5,
        sex=Sex.FEMALE,
        scan_type=ScanType.T1W
    )
    
    raw_metrics = MRIQCMetrics(
        snr=12.5,
        cnr=3.2,
        fber=1500.0,
        efc=0.45,
        fwhm_avg=2.8
    )
    
    quality_assessment = QualityAssessment(
        overall_status=QualityStatus.PASS,
        metric_assessments={"snr": QualityStatus.PASS},
        composite_score=85.0,
        confidence=0.9
    )
    
    return ProcessedSubject(
        subject_info=subject_info,
        raw_metrics=raw_metrics,
        quality_assessment=quality_assessment
    )


@pytest.fixture(autouse=True)
def cleanup_stores():
    """Clean up global stores before and after each test."""
    batch_status_store.clear()
    processed_subjects_store.clear()
    yield
    batch_status_store.clear()
    processed_subjects_store.clear()


class TestHealthEndpoint:
    """Test health check endpoint."""
    
    def test_health_check(self, client):
        """Test health endpoint returns OK status."""
        response = client.get("/api/health")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "ok"
        assert "timestamp" in data


class TestFileUploadEndpoint:
    """Test file upload endpoint."""
    
    def test_upload_valid_csv(self, client, sample_mriqc_csv):
        """Test uploading valid MRIQC CSV file."""
        csv_bytes = sample_mriqc_csv.encode('utf-8')
        files = {"file": ("test_mriqc.csv", io.BytesIO(csv_bytes), "text/csv")}
        
        response = client.post("/api/upload", files=files)
        assert response.status_code == 200
        
        data = response.json()
        assert data["message"] == "File uploaded successfully"
        assert "file_id" in data
        assert data["filename"] == "test_mriqc.csv"
        assert data["size"] > 0
        assert data["subjects_count"] == 3
    
    def test_upload_non_csv_file(self, client):
        """Test uploading non-CSV file returns error."""
        files = {"file": ("test.txt", io.BytesIO(b"not a csv"), "text/plain")}
        
        response = client.post("/api/upload", files=files)
        assert response.status_code == 400
        assert "must be a CSV file" in response.json()["detail"]
    
    def test_upload_large_file(self, client):
        """Test uploading file exceeding size limit."""
        # Create large content (>50MB)
        large_content = "a" * (51 * 1024 * 1024)
        files = {"file": ("large.csv", io.BytesIO(large_content.encode()), "text/csv")}
        
        response = client.post("/api/upload", files=files)
        assert response.status_code == 400
        assert "exceeds 50MB limit" in response.json()["detail"]
    
    def test_upload_invalid_csv_content(self, client):
        """Test uploading CSV with invalid content."""
        invalid_csv = "invalid,csv,content\nno,mriqc,columns"
        files = {"file": ("invalid.csv", io.BytesIO(invalid_csv.encode()), "text/csv")}
        
        response = client.post("/api/upload", files=files)
        assert response.status_code == 400
        assert "Invalid MRIQC file" in response.json()["detail"]


class TestProcessEndpoint:
    """Test file processing endpoint."""
    
    def test_process_valid_file(self, client, sample_mriqc_csv):
        """Test processing valid MRIQC file."""
        # First upload file
        csv_bytes = sample_mriqc_csv.encode('utf-8')
        files = {"file": ("test_mriqc.csv", io.BytesIO(csv_bytes), "text/csv")}
        upload_response = client.post("/api/upload", files=files)
        file_id = upload_response.json()["file_id"]
        
        # Then process file
        process_request = {
            "file_id": file_id,
            "apply_quality_assessment": True
        }
        
        response = client.post("/api/process", json=process_request)
        assert response.status_code == 200
        
        data = response.json()
        assert "batch_id" in data
        assert "Processing started" in data["message"]
        assert data["subjects_processed"] == 0  # Background processing
    
    def test_process_nonexistent_file(self, client):
        """Test processing non-existent file."""
        process_request = {
            "file_id": "nonexistent-file-id",
            "apply_quality_assessment": True
        }
        
        response = client.post("/api/process", json=process_request)
        assert response.status_code == 404
        assert "File not found" in response.json()["detail"]
    
    def test_process_without_quality_assessment(self, client, sample_mriqc_csv):
        """Test processing file without quality assessment."""
        # Upload file
        csv_bytes = sample_mriqc_csv.encode('utf-8')
        files = {"file": ("test_mriqc.csv", io.BytesIO(csv_bytes), "text/csv")}
        upload_response = client.post("/api/upload", files=files)
        file_id = upload_response.json()["file_id"]
        
        # Process without quality assessment
        process_request = {
            "file_id": file_id,
            "apply_quality_assessment": False
        }
        
        response = client.post("/api/process", json=process_request)
        assert response.status_code == 200


class TestBatchStatusEndpoint:
    """Test batch status tracking endpoint."""
    
    def test_get_batch_status_existing(self, client):
        """Test getting status for existing batch."""
        # Create mock batch status
        batch_id = "test-batch-123"
        batch_status_store[batch_id] = {
            'batch_id': batch_id,
            'status': 'processing',
            'progress': {'completed': 2, 'total': 5, 'progress_percent': 40.0},
            'subjects_processed': 2,
            'total_subjects': 5,
            'errors': [],
            'created_at': datetime.now(),
            'started_at': datetime.now()
        }
        
        response = client.get(f"/api/batch/{batch_id}/status")
        assert response.status_code == 200
        
        data = response.json()
        assert data["batch_id"] == batch_id
        assert data["status"] == "processing"
        assert data["subjects_processed"] == 2
        assert data["total_subjects"] == 5
    
    def test_get_batch_status_nonexistent(self, client):
        """Test getting status for non-existent batch."""
        response = client.get("/api/batch/nonexistent-batch/status")
        assert response.status_code == 404
        assert "Batch not found" in response.json()["detail"]
    
    def test_get_completed_batch_status(self, client):
        """Test getting status for completed batch."""
        batch_id = "completed-batch-456"
        batch_status_store[batch_id] = {
            'batch_id': batch_id,
            'status': 'completed',
            'progress': {'completed': 3, 'total': 3, 'progress_percent': 100.0},
            'subjects_processed': 3,
            'total_subjects': 3,
            'errors': [],
            'created_at': datetime.now(),
            'started_at': datetime.now(),
            'completed_at': datetime.now()
        }
        
        response = client.get(f"/api/batch/{batch_id}/status")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "completed"
        assert data["completed_at"] is not None


class TestSubjectsEndpoint:
    """Test subjects listing endpoint."""
    
    def test_get_subjects_empty(self, client):
        """Test getting subjects when none exist."""
        response = client.get("/api/subjects")
        assert response.status_code == 200
        
        data = response.json()
        assert data["subjects"] == []
        assert data["total_count"] == 0
        assert data["page"] == 1
        assert data["page_size"] == 50
    
    def test_get_subjects_with_data(self, client, sample_processed_subject):
        """Test getting subjects with existing data."""
        # Add sample data
        batch_id = "test-batch"
        processed_subjects_store[batch_id] = [sample_processed_subject]
        
        response = client.get("/api/subjects")
        assert response.status_code == 200
        
        data = response.json()
        assert len(data["subjects"]) == 1
        assert data["total_count"] == 1
        assert data["subjects"][0]["subject_info"]["subject_id"] == "sub-001"
    
    def test_get_subjects_with_batch_filter(self, client, sample_processed_subject):
        """Test getting subjects filtered by batch ID."""
        # Add sample data to specific batch
        batch_id = "specific-batch"
        processed_subjects_store[batch_id] = [sample_processed_subject]
        processed_subjects_store["other-batch"] = []
        
        response = client.get(f"/api/subjects?batch_id={batch_id}")
        assert response.status_code == 200
        
        data = response.json()
        assert len(data["subjects"]) == 1
        assert data["filters_applied"]["batch_id"] == batch_id
    
    def test_get_subjects_with_quality_filter(self, client, sample_processed_subject):
        """Test getting subjects filtered by quality status."""
        batch_id = "test-batch"
        processed_subjects_store[batch_id] = [sample_processed_subject]
        
        response = client.get("/api/subjects?quality_status=pass")
        assert response.status_code == 200
        
        data = response.json()
        assert len(data["subjects"]) == 1
        assert data["filters_applied"]["quality_status"] == "pass"
    
    def test_get_subjects_pagination(self, client):
        """Test subjects pagination."""
        # Create multiple subjects
        subjects = []
        for i in range(10):
            subject_info = SubjectInfo(
                subject_id=f"sub-{i:03d}",
                age=25.0 + i,
                scan_type=ScanType.T1W
            )
            raw_metrics = MRIQCMetrics(snr=10.0 + i)
            quality_assessment = QualityAssessment(
                overall_status=QualityStatus.PASS,
                metric_assessments={},
                composite_score=80.0,
                confidence=0.8
            )
            subjects.append(ProcessedSubject(
                subject_info=subject_info,
                raw_metrics=raw_metrics,
                quality_assessment=quality_assessment
            ))
        
        processed_subjects_store["test-batch"] = subjects
        
        # Test first page
        response = client.get("/api/subjects?page=1&page_size=5")
        assert response.status_code == 200
        
        data = response.json()
        assert len(data["subjects"]) == 5
        assert data["total_count"] == 10
        assert data["page"] == 1
        
        # Test second page
        response = client.get("/api/subjects?page=2&page_size=5")
        assert response.status_code == 200
        
        data = response.json()
        assert len(data["subjects"]) == 5
        assert data["page"] == 2
    
    def test_get_subjects_nonexistent_batch(self, client):
        """Test getting subjects from non-existent batch."""
        response = client.get("/api/subjects?batch_id=nonexistent")
        assert response.status_code == 404
        assert "Batch not found" in response.json()["detail"]


class TestSubjectDetailEndpoint:
    """Test individual subject detail endpoint."""
    
    def test_get_subject_detail_existing(self, client, sample_processed_subject):
        """Test getting details for existing subject."""
        batch_id = "test-batch"
        processed_subjects_store[batch_id] = [sample_processed_subject]
        
        response = client.get("/api/subjects/sub-001")
        assert response.status_code == 200
        
        data = response.json()
        assert data["subject"]["subject_info"]["subject_id"] == "sub-001"
        assert "recommendations" in data
    
    def test_get_subject_detail_nonexistent(self, client):
        """Test getting details for non-existent subject."""
        response = client.get("/api/subjects/nonexistent-subject")
        assert response.status_code == 404
        assert "Subject not found" in response.json()["detail"]
    
    def test_get_subject_detail_with_batch_filter(self, client, sample_processed_subject):
        """Test getting subject details with batch filter."""
        batch_id = "specific-batch"
        processed_subjects_store[batch_id] = [sample_processed_subject]
        
        response = client.get(f"/api/subjects/sub-001?batch_id={batch_id}")
        assert response.status_code == 200
        
        data = response.json()
        assert data["subject"]["subject_info"]["subject_id"] == "sub-001"
    
    def test_get_subject_detail_wrong_batch(self, client, sample_processed_subject):
        """Test getting subject details from wrong batch."""
        processed_subjects_store["correct-batch"] = [sample_processed_subject]
        
        response = client.get("/api/subjects/sub-001?batch_id=wrong-batch")
        assert response.status_code == 404
        assert "Batch not found" in response.json()["detail"]


class TestDashboardSummaryEndpoint:
    """Test dashboard summary endpoint."""
    
    def test_get_dashboard_summary_empty(self, client):
        """Test getting dashboard summary with no data."""
        response = client.get("/api/dashboard/summary")
        assert response.status_code == 200
        
        data = response.json()
        assert data["total_subjects"] == 0
        assert data["quality_distribution"] == {}
        assert data["exclusion_rate"] == 0.0
    
    def test_get_dashboard_summary_with_data(self, client):
        """Test getting dashboard summary with subject data."""
        # Create subjects with different quality statuses
        subjects = []
        
        # Pass subject
        pass_subject = ProcessedSubject(
            subject_info=SubjectInfo(subject_id="sub-pass", scan_type=ScanType.T1W),
            raw_metrics=MRIQCMetrics(snr=15.0),
            quality_assessment=QualityAssessment(
                overall_status=QualityStatus.PASS,
                metric_assessments={},
                composite_score=90.0,
                confidence=0.9
            )
        )
        subjects.append(pass_subject)
        
        # Fail subject
        fail_subject = ProcessedSubject(
            subject_info=SubjectInfo(subject_id="sub-fail", scan_type=ScanType.T1W),
            raw_metrics=MRIQCMetrics(snr=5.0),
            quality_assessment=QualityAssessment(
                overall_status=QualityStatus.FAIL,
                metric_assessments={},
                composite_score=30.0,
                confidence=0.8
            )
        )
        subjects.append(fail_subject)
        
        processed_subjects_store["test-batch"] = subjects
        
        response = client.get("/api/dashboard/summary")
        assert response.status_code == 200
        
        data = response.json()
        assert data["total_subjects"] == 2
        assert data["quality_distribution"]["pass"] == 1
        assert data["quality_distribution"]["fail"] == 1
        assert data["exclusion_rate"] == 0.5
        assert "metric_statistics" in data
        assert "snr" in data["metric_statistics"]
    
    def test_get_dashboard_summary_with_batch_filter(self, client, sample_processed_subject):
        """Test getting dashboard summary filtered by batch."""
        processed_subjects_store["batch-1"] = [sample_processed_subject]
        processed_subjects_store["batch-2"] = []
        
        response = client.get("/api/dashboard/summary?batch_id=batch-1")
        assert response.status_code == 200
        
        data = response.json()
        assert data["total_subjects"] == 1
        assert data["batch_id"] == "batch-1"
    
    def test_get_dashboard_summary_nonexistent_batch(self, client):
        """Test getting dashboard summary for non-existent batch."""
        response = client.get("/api/dashboard/summary?batch_id=nonexistent")
        assert response.status_code == 404
        assert "Batch not found" in response.json()["detail"]


class TestBatchDeletionEndpoint:
    """Test batch deletion endpoint."""
    
    def test_delete_existing_batch(self, client, sample_processed_subject):
        """Test deleting existing batch."""
        batch_id = "test-batch"
        batch_status_store[batch_id] = {"status": "completed"}
        processed_subjects_store[batch_id] = [sample_processed_subject]
        
        response = client.delete(f"/api/batch/{batch_id}")
        assert response.status_code == 200
        assert "deleted successfully" in response.json()["message"]
        
        # Verify deletion
        assert batch_id not in batch_status_store
        assert batch_id not in processed_subjects_store
    
    def test_delete_nonexistent_batch(self, client):
        """Test deleting non-existent batch."""
        response = client.delete("/api/batch/nonexistent")
        assert response.status_code == 404
        assert "Batch not found" in response.json()["detail"]


class TestEndToEndWorkflow:
    """Test complete end-to-end workflow."""
    
    @pytest.mark.asyncio
    async def test_complete_workflow(self, client, sample_mriqc_csv):
        """Test complete workflow from upload to retrieval."""
        # Step 1: Upload file
        csv_bytes = sample_mriqc_csv.encode('utf-8')
        files = {"file": ("test_mriqc.csv", io.BytesIO(csv_bytes), "text/csv")}
        upload_response = client.post("/api/upload", files=files)
        assert upload_response.status_code == 200
        file_id = upload_response.json()["file_id"]
        
        # Step 2: Process file
        process_request = {"file_id": file_id, "apply_quality_assessment": True}
        process_response = client.post("/api/process", json=process_request)
        assert process_response.status_code == 200
        batch_id = process_response.json()["batch_id"]
        
        # Step 3: Wait for processing (simulate)
        await asyncio.sleep(0.1)  # Brief wait for background task
        
        # Step 4: Check batch status
        status_response = client.get(f"/api/batch/{batch_id}/status")
        assert status_response.status_code == 200
        
        # Step 5: Get subjects (may be empty if processing not complete)
        subjects_response = client.get("/api/subjects")
        assert subjects_response.status_code == 200
        
        # Step 6: Get dashboard summary
        summary_response = client.get("/api/dashboard/summary")
        assert summary_response.status_code == 200


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_invalid_json_request(self, client):
        """Test handling of invalid JSON in request."""
        response = client.post(
            "/api/process",
            data="invalid json",
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 422  # Unprocessable Entity
    
    def test_missing_required_fields(self, client):
        """Test handling of missing required fields."""
        response = client.post("/api/process", json={})
        assert response.status_code == 422
    
    def test_invalid_query_parameters(self, client):
        """Test handling of invalid query parameters."""
        response = client.get("/api/subjects?page=0")  # Invalid page number
        assert response.status_code == 422
        
        response = client.get("/api/subjects?page_size=0")  # Invalid page size
        assert response.status_code == 422


if __name__ == "__main__":
    pytest.main([__file__, "-v"])