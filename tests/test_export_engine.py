"""
Tests for the export engine functionality.

This module tests CSV export, PDF report generation, and study summary creation
with various data scenarios and edge cases.
"""

import pytest
import tempfile
import csv
import io
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

from app.export_engine import ExportEngine, ExportError
from app.models import (
    ProcessedSubject, SubjectInfo, MRIQCMetrics, QualityAssessment,
    NormalizedMetrics, StudySummary, QualityStatus, AgeGroup, ScanType, Sex
)


# Global fixtures for all test classes
@pytest.fixture
def export_engine():
    """Create ExportEngine instance for testing."""
    return ExportEngine()

@pytest.fixture
def sample_subject():
    """Create a sample processed subject for testing."""
    subject_info = SubjectInfo(
        subject_id="sub-001",
        age=25.5,
        sex=Sex.FEMALE,
        session="ses-01",
        scan_type=ScanType.T1W,
        acquisition_date=datetime(2024, 1, 15, 10, 30),
        site="site-A",
        scanner="Siemens Prisma 3T"
    )
    
    raw_metrics = MRIQCMetrics(
        snr=12.5,
        cnr=3.2,
        fber=1500.0,
        efc=0.45,
        fwhm_avg=2.8,
        qi1=0.85,
        cjv=0.42
    )
    
    normalized_metrics = NormalizedMetrics(
        raw_metrics=raw_metrics,
        percentiles={
            "snr": 75.0,
            "cnr": 60.0,
            "fber": 80.0
        },
        z_scores={
            "snr": 0.67,
            "cnr": 0.25,
            "fber": 0.84
        },
        age_group=AgeGroup.YOUNG_ADULT,
        normative_dataset="HCP-YA"
    )
    
    quality_assessment = QualityAssessment(
        overall_status=QualityStatus.PASS,
        metric_assessments={
            "snr": QualityStatus.PASS,
            "cnr": QualityStatus.WARNING,
            "fber": QualityStatus.PASS
        },
        composite_score=78.5,
        recommendations=["Consider manual review of CNR values"],
        flags=["cnr_borderline"],
        confidence=0.85,
        threshold_violations={
            "cnr": {
                "value": 2.8,
                "threshold": 3.0,
                "severity": "warning"
            }
        }
    )
    
    return ProcessedSubject(
        subject_info=subject_info,
        raw_metrics=raw_metrics,
        normalized_metrics=normalized_metrics,
        quality_assessment=quality_assessment,
        processing_timestamp=datetime(2024, 1, 15, 14, 30),
        processing_version="1.0.0"
    )
    


@pytest.fixture
def sample_subjects_list(sample_subject):
    """Create a list of sample subjects with variations."""
    subjects = [sample_subject]
    
    # Add a failed subject
    failed_subject = ProcessedSubject(
        subject_info=SubjectInfo(
            subject_id="sub-002",
            age=65.0,
            sex=Sex.MALE,
            scan_type=ScanType.T1W
        ),
        raw_metrics=MRIQCMetrics(
            snr=8.0,
            cnr=2.0,
            fber=800.0,
            efc=0.65,
            fwhm_avg=4.2
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
            metric_assessments={"snr": QualityStatus.FAIL, "cnr": QualityStatus.FAIL},
            composite_score=35.2,
            recommendations=["Exclude from analysis"],
            flags=["low_snr", "low_cnr"],
            confidence=0.95
        ),
        processing_timestamp=datetime(2024, 1, 15, 14, 35)
    )
    subjects.append(failed_subject)
    
    # Add a subject without age (no normalization)
    no_age_subject = ProcessedSubject(
        subject_info=SubjectInfo(
            subject_id="sub-003",
            age=None,
            scan_type=ScanType.BOLD
        ),
        raw_metrics=MRIQCMetrics(
            dvars=1.2,
            fd_mean=0.15,
            gcor=0.05
        ),
        normalized_metrics=None,
        quality_assessment=QualityAssessment(
            overall_status=QualityStatus.UNCERTAIN,
            metric_assessments={"dvars": QualityStatus.PASS},
            composite_score=60.0,
            recommendations=["Age unknown, manual review recommended"],
            flags=["no_age_normalization"],
            confidence=0.60
        ),
        processing_timestamp=datetime(2024, 1, 15, 14, 40)
    )
    subjects.append(no_age_subject)
    
    return subjects


class TestExportEngine:
    """Test cases for ExportEngine class."""
    pass


class TestCSVExport:
    """Test CSV export functionality."""
    
    def test_export_subjects_csv_basic(self, export_engine, sample_subjects_list):
        """Test basic CSV export functionality."""
        csv_content = export_engine.export_subjects_csv(sample_subjects_list)
        
        assert isinstance(csv_content, str)
        assert len(csv_content) > 0
        
        # Parse CSV to verify structure
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)
        
        assert len(rows) == len(sample_subjects_list)
        
        # Check required columns
        expected_columns = [
            'subject_id', 'age', 'sex', 'scan_type', 'processing_timestamp',
            'overall_quality_status', 'composite_score', 'confidence'
        ]
        
        for col in expected_columns:
            assert col in reader.fieldnames
        
        # Verify first row data
        first_row = rows[0]
        assert first_row['subject_id'] == 'sub-001'
        assert first_row['age'] == '25.5'
        assert first_row['overall_quality_status'] == 'pass'
    
    def test_export_csv_with_filters(self, export_engine, sample_subjects_list):
        """Test CSV export with different inclusion filters."""
        # Test with only raw metrics
        csv_content = export_engine.export_subjects_csv(
            sample_subjects_list,
            include_raw_metrics=True,
            include_normalized_metrics=False,
            include_quality_assessment=False
        )
        
        reader = csv.DictReader(io.StringIO(csv_content))
        fieldnames = reader.fieldnames
        
        # Should have raw metrics
        assert any('raw_' in field for field in fieldnames)
        # Should not have normalized metrics
        assert not any('percentile_' in field for field in fieldnames)
        assert not any('zscore_' in field for field in fieldnames)
        # Should not have quality assessment
        assert 'overall_quality_status' not in fieldnames
    
    def test_export_csv_normalized_metrics_only(self, export_engine, sample_subjects_list):
        """Test CSV export with only normalized metrics."""
        csv_content = export_engine.export_subjects_csv(
            sample_subjects_list,
            include_raw_metrics=False,
            include_normalized_metrics=True,
            include_quality_assessment=False
        )
        
        reader = csv.DictReader(io.StringIO(csv_content))
        fieldnames = reader.fieldnames
        
        # Should not have raw metrics
        assert not any('raw_' in field for field in fieldnames)
        # Should have normalized metrics
        assert any('percentile_' in field for field in fieldnames)
        assert any('zscore_' in field for field in fieldnames)
        assert 'age_group' in fieldnames
    
    def test_export_csv_custom_fields(self, export_engine, sample_subjects_list):
        """Test CSV export with custom fields."""
        csv_content = export_engine.export_subjects_csv(
            sample_subjects_list,
            custom_fields=['processing_version', 'notes']
        )
        
        reader = csv.DictReader(io.StringIO(csv_content))
        fieldnames = reader.fieldnames
        
        assert 'processing_version' in fieldnames
        # notes field might not exist in all subjects, but should be in fieldnames
        assert 'notes' in fieldnames
    
    def test_export_csv_empty_subjects(self, export_engine):
        """Test CSV export with empty subjects list."""
        with pytest.raises(ExportError, match="No subjects provided for export"):
            export_engine.export_subjects_csv([])
    
    def test_export_csv_handles_none_values(self, export_engine):
        """Test CSV export handles None values gracefully."""
        # Create subject with many None values
        subject = ProcessedSubject(
            subject_info=SubjectInfo(
                subject_id="sub-test",
                age=None,
                sex=None,
                session=None,
                scan_type=ScanType.T1W
            ),
            raw_metrics=MRIQCMetrics(
                snr=None,
                cnr=12.0,
                fber=None
            ),
            normalized_metrics=None,
            quality_assessment=QualityAssessment(
                overall_status=QualityStatus.UNCERTAIN,
                metric_assessments={},
                composite_score=50.0,
                confidence=0.5
            )
        )
        
        csv_content = export_engine.export_subjects_csv([subject])
        
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)
        
        assert len(rows) == 1
        row = rows[0]
        
        # None values should be empty strings in CSV
        assert row['age'] == ''
        assert row['sex'] == ''
        assert row['raw_snr'] == ''
        assert row['raw_cnr'] == '12.0'


class TestStudySummary:
    """Test study summary generation."""
    
    def test_generate_study_summary_basic(self, export_engine, sample_subjects_list):
        """Test basic study summary generation."""
        summary = export_engine.generate_study_summary(sample_subjects_list, "Test Study")
        
        assert isinstance(summary, StudySummary)
        assert summary.total_subjects == len(sample_subjects_list)
        assert summary.study_name == "Test Study"
        assert isinstance(summary.processing_date, datetime)
        
        # Check quality distribution
        assert QualityStatus.PASS in summary.quality_distribution
        assert QualityStatus.FAIL in summary.quality_distribution
        assert QualityStatus.UNCERTAIN in summary.quality_distribution
        
        # Verify counts
        assert summary.quality_distribution[QualityStatus.PASS] == 1
        assert summary.quality_distribution[QualityStatus.FAIL] == 1
        assert summary.quality_distribution[QualityStatus.UNCERTAIN] == 1
        
        # Check exclusion rate
        expected_exclusion_rate = 1 / 3  # 1 failed out of 3 subjects
        assert abs(summary.exclusion_rate - expected_exclusion_rate) < 0.01
    
    def test_generate_study_summary_age_groups(self, export_engine, sample_subjects_list):
        """Test study summary age group distribution."""
        summary = export_engine.generate_study_summary(sample_subjects_list)
        
        # Should have young adult and elderly groups
        assert summary.age_group_distribution[AgeGroup.YOUNG_ADULT] == 1
        assert summary.age_group_distribution[AgeGroup.ELDERLY] == 1
        # Subject without age shouldn't be counted in age groups
        assert sum(summary.age_group_distribution.values()) == 2
    
    def test_generate_study_summary_metric_statistics(self, export_engine, sample_subjects_list):
        """Test study summary metric statistics."""
        summary = export_engine.generate_study_summary(sample_subjects_list)
        
        # Should have statistics for metrics present in subjects
        assert 'snr' in summary.metric_statistics
        assert 'cnr' in summary.metric_statistics
        
        snr_stats = summary.metric_statistics['snr']
        assert 'mean' in snr_stats
        assert 'std' in snr_stats
        assert 'min' in snr_stats
        assert 'max' in snr_stats
        assert 'count' in snr_stats
        
        # SNR values are 12.5 and 8.0, so mean should be 10.25
        assert abs(snr_stats['mean'] - 10.25) < 0.01
        assert snr_stats['count'] == 2
    
    def test_generate_study_summary_empty_subjects(self, export_engine):
        """Test study summary with empty subjects list."""
        with pytest.raises(ExportError, match="No subjects provided for summary"):
            export_engine.generate_study_summary([])
    
    def test_export_study_summary_csv(self, export_engine, sample_subjects_list):
        """Test study summary CSV export."""
        summary = export_engine.generate_study_summary(sample_subjects_list, "Test Study")
        csv_content = export_engine.export_study_summary_csv(summary)
        
        assert isinstance(csv_content, str)
        assert len(csv_content) > 0
        
        # Check that key information is present
        assert "Study Summary Report" in csv_content
        assert "Test Study" in csv_content
        assert "Quality Distribution" in csv_content
        assert "Age Group Distribution" in csv_content
        assert "Metric Statistics" in csv_content


class TestPDFReportGeneration:
    """Test PDF report generation."""
    
    def test_generate_pdf_report_basic(self, export_engine, sample_subjects_list):
        """Test basic PDF report generation."""
        pdf_content = export_engine.generate_pdf_report(
            sample_subjects_list,
            study_name="Test Study"
        )
        
        assert isinstance(pdf_content, bytes)
        assert len(pdf_content) > 0
        
        # Check PDF header
        assert pdf_content.startswith(b'%PDF')
    
    def test_generate_pdf_report_options(self, export_engine, sample_subjects_list):
        """Test PDF report generation with different options."""
        # Test without individual subjects
        pdf_content = export_engine.generate_pdf_report(
            sample_subjects_list,
            include_individual_subjects=False,
            include_summary_charts=True
        )
        
        assert isinstance(pdf_content, bytes)
        assert len(pdf_content) > 0
        
        # Test without summary charts
        pdf_content = export_engine.generate_pdf_report(
            sample_subjects_list,
            include_individual_subjects=True,
            include_summary_charts=False
        )
        
        assert isinstance(pdf_content, bytes)
        assert len(pdf_content) > 0
    
    def test_generate_pdf_report_limited_subjects(self, export_engine, sample_subjects_list):
        """Test PDF report with subject limit."""
        # Create many subjects
        many_subjects = sample_subjects_list * 20  # 60 subjects
        
        pdf_content = export_engine.generate_pdf_report(
            many_subjects,
            max_subjects_detailed=10
        )
        
        assert isinstance(pdf_content, bytes)
        assert len(pdf_content) > 0
    
    def test_generate_pdf_report_empty_subjects(self, export_engine):
        """Test PDF report with empty subjects list."""
        with pytest.raises(ExportError, match="No subjects provided for PDF report"):
            export_engine.generate_pdf_report([])
    
    @patch('app.export_engine.SimpleDocTemplate')
    def test_generate_pdf_report_handles_errors(self, mock_doc, export_engine, sample_subjects_list):
        """Test PDF report generation handles errors gracefully."""
        # Mock document build to raise an exception
        mock_doc.return_value.build.side_effect = Exception("PDF generation failed")
        
        with pytest.raises(ExportError, match="Failed to generate PDF report"):
            export_engine.generate_pdf_report(sample_subjects_list)


class TestExportEngineEdgeCases:
    """Test edge cases and error handling."""
    
    def test_export_engine_initialization(self):
        """Test export engine initializes correctly."""
        engine = ExportEngine()
        
        assert engine is not None
        assert hasattr(engine, 'styles')
        assert 'CustomTitle' in engine.styles
        assert 'SectionHeader' in engine.styles
    
    def test_export_with_malformed_data(self, export_engine):
        """Test export handles malformed data gracefully."""
        # Create subject with minimal data
        minimal_subject = ProcessedSubject(
            subject_info=SubjectInfo(
                subject_id="minimal",
                scan_type=ScanType.T1W
            ),
            raw_metrics=MRIQCMetrics(),  # All None values
            quality_assessment=QualityAssessment(
                overall_status=QualityStatus.UNCERTAIN,
                metric_assessments={},
                composite_score=0.0,
                confidence=0.0
            )
        )
        
        # Should not raise an exception
        csv_content = export_engine.export_subjects_csv([minimal_subject])
        assert isinstance(csv_content, str)
        
        # Should be able to generate summary
        summary = export_engine.generate_study_summary([minimal_subject])
        assert summary.total_subjects == 1
    
    def test_export_with_unicode_characters(self, export_engine, sample_subject):
        """Test export handles unicode characters in data."""
        # Add unicode characters to subject data
        sample_subject.subject_info.site = "Hôpital Universitaire"
        sample_subject.quality_assessment.recommendations = ["Révision manuelle recommandée"]
        
        csv_content = export_engine.export_subjects_csv([sample_subject])
        assert isinstance(csv_content, str)
        assert "Hôpital Universitaire" in csv_content
        
        # PDF should also handle unicode
        pdf_content = export_engine.generate_pdf_report([sample_subject])
        assert isinstance(pdf_content, bytes)
    
    def test_large_dataset_performance(self, export_engine, sample_subject):
        """Test export performance with larger datasets."""
        # Create a larger dataset (100 subjects)
        large_dataset = []
        for i in range(100):
            subject_copy = ProcessedSubject(
                subject_info=SubjectInfo(
                    subject_id=f"sub-{i:03d}",
                    age=20 + (i % 50),  # Ages 20-69
                    scan_type=ScanType.T1W
                ),
                raw_metrics=sample_subject.raw_metrics,
                normalized_metrics=sample_subject.normalized_metrics,
                quality_assessment=sample_subject.quality_assessment,
                processing_timestamp=datetime.now()
            )
            large_dataset.append(subject_copy)
        
        # CSV export should complete without issues
        csv_content = export_engine.export_subjects_csv(large_dataset)
        assert isinstance(csv_content, str)
        assert len(csv_content) > 0
        
        # Study summary should handle large dataset
        summary = export_engine.generate_study_summary(large_dataset)
        assert summary.total_subjects == 100
        
        # PDF generation should work (but limit individual subjects)
        pdf_content = export_engine.generate_pdf_report(
            large_dataset,
            max_subjects_detailed=20
        )
        assert isinstance(pdf_content, bytes)


class TestExportEngineIntegration:
    """Integration tests for export engine."""
    
    def test_full_export_workflow(self, export_engine, sample_subjects_list):
        """Test complete export workflow."""
        # Generate study summary
        summary = export_engine.generate_study_summary(sample_subjects_list, "Integration Test")
        
        # Export CSV
        csv_content = export_engine.export_subjects_csv(sample_subjects_list)
        
        # Export study summary CSV
        summary_csv = export_engine.export_study_summary_csv(summary)
        
        # Generate PDF report
        pdf_content = export_engine.generate_pdf_report(sample_subjects_list, "Integration Test")
        
        # Verify all exports completed successfully
        assert isinstance(csv_content, str) and len(csv_content) > 0
        assert isinstance(summary_csv, str) and len(summary_csv) > 0
        assert isinstance(pdf_content, bytes) and len(pdf_content) > 0
        
        # Verify data consistency
        reader = csv.DictReader(io.StringIO(csv_content))
        csv_rows = list(reader)
        assert len(csv_rows) == len(sample_subjects_list)
        assert summary.total_subjects == len(sample_subjects_list)
    
    def test_export_data_integrity(self, export_engine, sample_subject):
        """Test that exported data maintains integrity."""
        csv_content = export_engine.export_subjects_csv([sample_subject])
        
        reader = csv.DictReader(io.StringIO(csv_content))
        row = next(reader)
        
        # Verify key data points
        assert row['subject_id'] == sample_subject.subject_info.subject_id
        assert float(row['age']) == sample_subject.subject_info.age
        assert row['overall_quality_status'] == sample_subject.quality_assessment.overall_status.value
        assert float(row['composite_score']) == sample_subject.quality_assessment.composite_score
        
        # Verify raw metrics
        assert float(row['raw_snr']) == sample_subject.raw_metrics.snr
        assert float(row['raw_cnr']) == sample_subject.raw_metrics.cnr
        
        # Verify normalized metrics
        assert float(row['percentile_snr']) == sample_subject.normalized_metrics.percentiles['snr']
        assert float(row['zscore_snr']) == sample_subject.normalized_metrics.z_scores['snr']


if __name__ == "__main__":
    pytest.main([__file__])