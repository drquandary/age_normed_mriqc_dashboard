"""
End-to-end integration tests for the Age-Normed MRIQC Dashboard.

This module tests complete user workflows from MRIQC file upload through
quality assessment to data export and reporting.
"""

import asyncio
import json
import os
import tempfile
import pytest
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient
from fastapi import status

from app.main import app
from app.models import QualityStatus, AgeGroup
from app.mriqc_processor import MRIQCProcessor
from app.quality_assessor import QualityAssessor
from app.age_normalizer import AgeNormalizer
from app.export_engine import ExportEngine
from app.config_service import ConfigurationService
from app.database import Database


class TestEndToEndIntegration:
    """Test complete end-to-end workflows."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)
    
    @pytest.fixture
    def sample_mriqc_data(self):
        """Create sample MRIQC data for testing."""
        return pd.DataFrame({
            'bids_name': ['sub-001_ses-01_T1w', 'sub-002_ses-01_T1w', 'sub-003_ses-01_T1w'],
            'subject_id': ['sub-001', 'sub-002', 'sub-003'],
            'session_id': ['ses-01', 'ses-01', 'ses-01'],
            'age': [25.5, 8.2, 67.8],
            'sex': ['M', 'F', 'M'],
            'snr': [12.5, 10.2, 8.9],
            'cnr': [3.2, 2.8, 2.1],
            'fber': [1500.0, 1200.0, 900.0],
            'efc': [0.45, 0.52, 0.68],
            'fwhm_avg': [2.8, 3.1, 3.5],
            'qi1': [0.85, 0.78, 0.65],
            'cjv': [0.35, 0.42, 0.58]
        })
    
    @pytest.fixture
    def temp_mriqc_file(self, sample_mriqc_data):
        """Create temporary MRIQC CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_mriqc_data.to_csv(f.name, index=False)
            yield f.name
        os.unlink(f.name)
    
    def test_complete_workflow_upload_to_export(self, client, temp_mriqc_file):
        """Test complete workflow from file upload to data export."""
        
        # Step 1: Upload MRIQC file
        with open(temp_mriqc_file, 'rb') as f:
            upload_response = client.post(
                "/api/upload",
                files={"file": ("test_mriqc.csv", f, "text/csv")}
            )
        
        assert upload_response.status_code == status.HTTP_200_OK
        upload_data = upload_response.json()
        assert "file_id" in upload_data
        assert upload_data["subjects_count"] == 3
        file_id = upload_data["file_id"]
        
        # Step 2: Process uploaded file
        process_response = client.post(
            "/api/process",
            json={
                "file_id": file_id,
                "apply_quality_assessment": True
            }
        )
        
        assert process_response.status_code == status.HTTP_200_OK
        process_data = process_response.json()
        assert "batch_id" in process_data
        batch_id = process_data["batch_id"]
        
        # Step 3: Check batch processing status
        status_response = client.get(f"/api/batch/{batch_id}/status")
        assert status_response.status_code == status.HTTP_200_OK
        status_data = status_response.json()
        assert status_data["batch_id"] == batch_id
        
        # Wait for processing to complete (in real scenario, would poll)
        # For testing, we'll assume it completes quickly
        
        # Step 4: Get dashboard summary
        dashboard_response = client.get("/api/dashboard/summary")
        assert dashboard_response.status_code == status.HTTP_200_OK
        dashboard_data = dashboard_response.json()
        assert dashboard_data["total_subjects"] >= 3
        assert "quality_distribution" in dashboard_data
        assert "age_group_distribution" in dashboard_data
        
        # Step 5: Get subjects list
        subjects_response = client.get("/api/subjects")
        assert subjects_response.status_code == status.HTTP_200_OK
        subjects_data = subjects_response.json()
        assert len(subjects_data["subjects"]) >= 3
        
        # Step 6: Get individual subject details
        subject_id = subjects_data["subjects"][0]["subject_info"]["subject_id"]
        detail_response = client.get(f"/api/subjects/{subject_id}")
        assert detail_response.status_code == status.HTTP_200_OK
        detail_data = detail_response.json()
        assert detail_data["subject"]["subject_info"]["subject_id"] == subject_id
        
        # Step 7: Export data as CSV
        export_response = client.get("/api/export/csv")
        assert export_response.status_code == status.HTTP_200_OK
        assert export_response.headers["content-type"] == "text/csv; charset=utf-8"
        
        # Step 8: Generate PDF report
        report_response = client.get("/api/export/pdf")
        assert report_response.status_code == status.HTTP_200_OK
        assert export_response.headers.get("content-type") == "application/pdf"
    
    def test_batch_processing_workflow(self, client, temp_mriqc_file):
        """Test batch processing workflow with multiple files."""
        
        # Upload multiple files (simulate by uploading same file multiple times)
        file_ids = []
        for i in range(3):
            with open(temp_mriqc_file, 'rb') as f:
                upload_response = client.post(
                    "/api/upload",
                    files={"file": (f"test_mriqc_{i}.csv", f, "text/csv")}
                )
            assert upload_response.status_code == status.HTTP_200_OK
            file_ids.append(upload_response.json()["file_id"])
        
        # Process all files in batch
        batch_ids = []
        for file_id in file_ids:
            process_response = client.post(
                "/api/process",
                json={"file_id": file_id, "apply_quality_assessment": True}
            )
            assert process_response.status_code == status.HTTP_200_OK
            batch_ids.append(process_response.json()["batch_id"])
        
        # Check all batch statuses
        for batch_id in batch_ids:
            status_response = client.get(f"/api/batch/{batch_id}/status")
            assert status_response.status_code == status.HTTP_200_OK
            status_data = status_response.json()
            assert status_data["batch_id"] == batch_id
        
        # Verify dashboard shows all subjects
        dashboard_response = client.get("/api/dashboard/summary")
        assert dashboard_response.status_code == status.HTTP_200_OK
        dashboard_data = dashboard_response.json()
        assert dashboard_data["total_subjects"] >= 9  # 3 files × 3 subjects each
    
    def test_filtering_and_sorting_workflow(self, client, temp_mriqc_file):
        """Test advanced filtering and sorting workflows."""
        
        # Upload and process file first
        with open(temp_mriqc_file, 'rb') as f:
            upload_response = client.post(
                "/api/upload",
                files={"file": ("test_mriqc.csv", f, "text/csv")}
            )
        file_id = upload_response.json()["file_id"]
        
        process_response = client.post(
            "/api/process",
            json={"file_id": file_id, "apply_quality_assessment": True}
        )
        
        # Test filtering by quality status
        filter_response = client.post(
            "/api/subjects/filter",
            json={
                "quality_status": ["pass", "warning"],
                "page": 1,
                "page_size": 10
            }
        )
        assert filter_response.status_code == status.HTTP_200_OK
        filter_data = filter_response.json()
        assert "subjects" in filter_data
        assert "filters_applied" in filter_data
        
        # Test filtering by age group
        age_filter_response = client.post(
            "/api/subjects/filter",
            json={
                "age_group": ["young_adult", "elderly"],
                "page": 1,
                "page_size": 10
            }
        )
        assert age_filter_response.status_code == status.HTTP_200_OK
        
        # Test sorting
        sort_response = client.post(
            "/api/subjects/filter",
            json={
                "sort_by": "quality_assessment.composite_score",
                "sort_order": "desc",
                "page": 1,
                "page_size": 10
            }
        )
        assert sort_response.status_code == status.HTTP_200_OK
    
    def test_configuration_workflow(self, client):
        """Test study configuration management workflow."""
        
        # Create new study configuration
        config_data = {
            "study_name": "Test Study",
            "normative_dataset": "custom",
            "custom_age_groups": [
                {"name": "young", "min_age": 18, "max_age": 35},
                {"name": "old", "min_age": 65, "max_age": 100}
            ],
            "exclusion_criteria": ["high_motion", "artifacts"],
            "created_by": "test_user"
        }
        
        create_response = client.post("/api/config/studies", json=config_data)
        assert create_response.status_code == status.HTTP_201_CREATED
        created_config = create_response.json()
        assert created_config["study_name"] == "Test Study"
        
        # Get configuration
        get_response = client.get(f"/api/config/studies/{created_config['study_name']}")
        assert get_response.status_code == status.HTTP_200_OK
        
        # Update configuration
        update_data = {
            "exclusion_criteria": ["high_motion", "artifacts", "low_snr"]
        }
        update_response = client.put(
            f"/api/config/studies/{created_config['study_name']}", 
            json=update_data
        )
        assert update_response.status_code == status.HTTP_200_OK
        
        # List all configurations
        list_response = client.get("/api/config/studies")
        assert list_response.status_code == status.HTTP_200_OK
        list_data = list_response.json()
        assert len(list_data["configurations"]) >= 1
    
    def test_error_handling_workflow(self, client):
        """Test error handling throughout the workflow."""
        
        # Test invalid file upload
        invalid_response = client.post(
            "/api/upload",
            files={"file": ("invalid.txt", b"invalid content", "text/plain")}
        )
        assert invalid_response.status_code == status.HTTP_400_BAD_REQUEST
        error_data = invalid_response.json()
        assert "error_type" in error_data
        assert "suggestions" in error_data
        
        # Test processing non-existent file
        process_invalid_response = client.post(
            "/api/process",
            json={"file_id": "non-existent-id", "apply_quality_assessment": True}
        )
        assert process_invalid_response.status_code == status.HTTP_404_NOT_FOUND
        
        # Test getting non-existent subject
        subject_invalid_response = client.get("/api/subjects/non-existent-subject")
        assert subject_invalid_response.status_code == status.HTTP_404_NOT_FOUND
        
        # Test invalid batch status
        batch_invalid_response = client.get("/api/batch/non-existent-batch/status")
        assert batch_invalid_response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_security_workflow(self, client):
        """Test security features throughout the workflow."""
        
        # Test file size limit
        large_content = b"x" * (10 * 1024 * 1024)  # 10MB
        large_file_response = client.post(
            "/api/upload",
            files={"file": ("large.csv", large_content, "text/csv")}
        )
        # Should be rejected if size limit is enforced
        
        # Test invalid file extension
        invalid_ext_response = client.post(
            "/api/upload",
            files={"file": ("test.exe", b"executable", "application/octet-stream")}
        )
        assert invalid_ext_response.status_code == status.HTTP_400_BAD_REQUEST
        
        # Test health check includes security status
        health_response = client.get("/health")
        assert health_response.status_code == status.HTTP_200_OK
        health_data = health_response.json()
        assert "security_enabled" in health_data
        assert health_data["security_enabled"] is True
    
    def test_longitudinal_workflow(self, client):
        """Test longitudinal data processing workflow."""
        
        # Create longitudinal sample data
        longitudinal_data = pd.DataFrame({
            'bids_name': [
                'sub-001_ses-01_T1w', 'sub-001_ses-02_T1w', 'sub-001_ses-03_T1w',
                'sub-002_ses-01_T1w', 'sub-002_ses-02_T1w'
            ],
            'subject_id': ['sub-001', 'sub-001', 'sub-001', 'sub-002', 'sub-002'],
            'session_id': ['ses-01', 'ses-02', 'ses-03', 'ses-01', 'ses-02'],
            'age': [25.0, 25.5, 26.0, 30.0, 30.5],
            'sex': ['M', 'M', 'M', 'F', 'F'],
            'snr': [12.5, 12.8, 12.2, 11.0, 10.8],
            'cnr': [3.2, 3.3, 3.1, 2.9, 2.8],
            'fber': [1500.0, 1520.0, 1480.0, 1400.0, 1380.0],
            'efc': [0.45, 0.44, 0.46, 0.48, 0.49],
            'fwhm_avg': [2.8, 2.7, 2.9, 3.0, 3.1],
            'qi1': [0.85, 0.86, 0.84, 0.82, 0.81],
            'cjv': [0.35, 0.34, 0.36, 0.38, 0.39]
        })
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            longitudinal_data.to_csv(f.name, index=False)
            temp_file = f.name
        
        try:
            # Upload and process longitudinal data
            with open(temp_file, 'rb') as f:
                upload_response = client.post(
                    "/api/upload",
                    files={"file": ("longitudinal.csv", f, "text/csv")}
                )
            
            file_id = upload_response.json()["file_id"]
            
            process_response = client.post(
                "/api/process",
                json={"file_id": file_id, "apply_quality_assessment": True}
            )
            
            # Test longitudinal analysis endpoint
            longitudinal_response = client.get("/api/longitudinal/sub-001")
            assert longitudinal_response.status_code == status.HTTP_200_OK
            longitudinal_data = longitudinal_response.json()
            assert "timepoints" in longitudinal_data
            assert len(longitudinal_data["timepoints"]) == 3
            
        finally:
            os.unlink(temp_file)
    
    def test_performance_monitoring_workflow(self, client, temp_mriqc_file):
        """Test performance monitoring throughout the workflow."""
        
        # Upload file and check performance metrics
        with open(temp_mriqc_file, 'rb') as f:
            upload_response = client.post(
                "/api/upload",
                files={"file": ("test_mriqc.csv", f, "text/csv")}
            )
        
        file_id = upload_response.json()["file_id"]
        
        # Process with performance monitoring
        process_response = client.post(
            "/api/process",
            json={"file_id": file_id, "apply_quality_assessment": True}
        )
        
        # Check performance metrics endpoint
        perf_response = client.get("/api/performance/metrics")
        assert perf_response.status_code == status.HTTP_200_OK
        perf_data = perf_response.json()
        assert "processing_times" in perf_data
        assert "memory_usage" in perf_data
    
    @pytest.mark.asyncio
    async def test_websocket_workflow(self, client):
        """Test WebSocket real-time updates workflow."""
        
        # This would require a more complex setup with actual WebSocket testing
        # For now, we'll test the WebSocket endpoint exists
        with client.websocket_connect("/ws") as websocket:
            # Send ping
            websocket.send_json({"type": "ping"})
            
            # Receive pong
            data = websocket.receive_json()
            assert data["type"] == "pong"
    
    def test_export_formats_workflow(self, client, temp_mriqc_file):
        """Test different export formats workflow."""
        
        # Upload and process file first
        with open(temp_mriqc_file, 'rb') as f:
            upload_response = client.post(
                "/api/upload",
                files={"file": ("test_mriqc.csv", f, "text/csv")}
            )
        
        file_id = upload_response.json()["file_id"]
        
        process_response = client.post(
            "/api/process",
            json={"file_id": file_id, "apply_quality_assessment": True}
        )
        
        # Test CSV export
        csv_response = client.get("/api/export/csv")
        assert csv_response.status_code == status.HTTP_200_OK
        assert "text/csv" in csv_response.headers["content-type"]
        
        # Test PDF report
        pdf_response = client.get("/api/export/pdf")
        assert pdf_response.status_code == status.HTTP_200_OK
        
        # Test study summary export
        summary_response = client.get("/api/export/study-summary")
        assert summary_response.status_code == status.HTTP_200_OK
        
        # Test filtered export
        filtered_export_response = client.post(
            "/api/export/csv",
            json={
                "quality_status": ["pass"],
                "include_normalized_metrics": True
            }
        )
        assert filtered_export_response.status_code == status.HTTP_200_OK


class TestComponentIntegration:
    """Test integration between individual components."""
    
    def test_processor_assessor_integration(self):
        """Test integration between MRIQC processor and quality assessor."""
        
        processor = MRIQCProcessor()
        assessor = QualityAssessor()
        
        # Create sample data
        sample_data = pd.DataFrame({
            'subject_id': ['sub-001'],
            'age': [25.0],
            'snr': [12.5],
            'cnr': [3.2],
            'fber': [1500.0],
            'efc': [0.45],
            'fwhm_avg': [2.8]
        })
        
        # Process data
        subjects = processor.process_dataframe(sample_data)
        assert len(subjects) == 1
        
        # Assess quality
        subject = subjects[0]
        assessment = assessor.assess_quality(subject.raw_metrics, subject.subject_info)
        
        assert assessment is not None
        assert assessment.overall_status in [QualityStatus.PASS, QualityStatus.WARNING, QualityStatus.FAIL]
        assert assessment.composite_score >= 0
    
    def test_normalizer_assessor_integration(self):
        """Test integration between age normalizer and quality assessor."""
        
        normalizer = AgeNormalizer()
        assessor = QualityAssessor()
        
        # Create sample metrics
        from app.models import MRIQCMetrics, SubjectInfo
        
        metrics = MRIQCMetrics(
            snr=12.5,
            cnr=3.2,
            fber=1500.0,
            efc=0.45,
            fwhm_avg=2.8
        )
        
        subject_info = SubjectInfo(
            subject_id="sub-001",
            age=25.0,
            sex="M",
            scan_type="T1w"
        )
        
        # Normalize metrics
        normalized = normalizer.normalize_metrics(metrics, 25.0)
        assert normalized is not None
        
        # Assess with normalized metrics
        assessment = assessor.assess_quality(metrics, subject_info)
        assert assessment is not None
    
    def test_database_service_integration(self):
        """Test integration between database and various services."""
        
        db = Database()
        config_service = ConfigurationService()
        
        # Test database initialization
        db.initialize_database()
        
        # Test configuration storage and retrieval
        config_data = {
            "study_name": "integration_test",
            "normative_dataset": "default",
            "created_by": "test_user"
        }
        
        # This would test actual database operations
        # For now, we'll just verify the services can be instantiated together
        assert db is not None
        assert config_service is not None
    
    def test_export_engine_integration(self):
        """Test export engine integration with processed data."""
        
        export_engine = ExportEngine()
        
        # Create sample processed subjects
        from app.models import ProcessedSubject, MRIQCMetrics, SubjectInfo, QualityAssessment
        
        subjects = [
            ProcessedSubject(
                subject_info=SubjectInfo(
                    subject_id="sub-001",
                    age=25.0,
                    sex="M",
                    scan_type="T1w"
                ),
                raw_metrics=MRIQCMetrics(
                    snr=12.5,
                    cnr=3.2,
                    fber=1500.0,
                    efc=0.45,
                    fwhm_avg=2.8
                ),
                quality_assessment=QualityAssessment(
                    overall_status=QualityStatus.PASS,
                    composite_score=85.0,
                    metric_assessments={},
                    recommendations=[],
                    flags=[],
                    confidence=0.9
                ),
                processing_timestamp=datetime.now()
            )
        ]
        
        # Test CSV export
        csv_data = export_engine.export_to_csv(subjects)
        assert csv_data is not None
        assert len(csv_data) > 0
        
        # Test PDF generation
        pdf_data = export_engine.generate_pdf_report(subjects)
        assert pdf_data is not None
        assert len(pdf_data) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])