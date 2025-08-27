"""
Unit tests for MRIQC data processor.

Tests file parsing, validation, metric extraction, and batch processing
functionality with various input scenarios and edge cases.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import asyncio
from datetime import datetime
from unittest.mock import Mock, patch

from app.mriqc_processor import (
    MRIQCProcessor, MRIQCValidationError, MRIQCProcessingError,
    ProgressTracker
)
from app.models import (
    MRIQCMetrics, SubjectInfo, ProcessedSubject, QualityStatus,
    ScanType, Sex, ValidationError, ProcessingError
)


class TestProgressTracker:
    """Test progress tracking functionality."""
    
    def test_progress_tracker_initialization(self):
        """Test progress tracker initialization."""
        tracker = ProgressTracker(total=10)
        
        assert tracker.total == 10
        assert tracker.completed == 0
        assert tracker.failed == 0
        assert isinstance(tracker.start_time, datetime)
        assert tracker.callbacks == []
    
    def test_progress_tracker_update_success(self):
        """Test successful progress updates."""
        tracker = ProgressTracker(total=5)
        
        tracker.update(success=True)
        assert tracker.completed == 1
        assert tracker.failed == 0
        
        tracker.update(success=True)
        assert tracker.completed == 2
        assert tracker.failed == 0
    
    def test_progress_tracker_update_failure(self):
        """Test failed progress updates."""
        tracker = ProgressTracker(total=5)
        
        tracker.update(success=False)
        assert tracker.completed == 0
        assert tracker.failed == 1
        
        tracker.update(success=False)
        assert tracker.completed == 0
        assert tracker.failed == 2
    
    def test_progress_tracker_status(self):
        """Test progress status calculation."""
        tracker = ProgressTracker(total=10)
        
        # Initial status
        status = tracker.get_status()
        assert status['total'] == 10
        assert status['completed'] == 0
        assert status['failed'] == 0
        assert status['processed'] == 0
        assert status['remaining'] == 10
        assert status['progress_percent'] == 0
        
        # After some progress
        tracker.update(success=True)
        tracker.update(success=False)
        
        status = tracker.get_status()
        assert status['completed'] == 1
        assert status['failed'] == 1
        assert status['processed'] == 2
        assert status['remaining'] == 8
        assert status['progress_percent'] == 20.0
    
    def test_progress_tracker_callbacks(self):
        """Test progress callbacks."""
        tracker = ProgressTracker(total=5)
        callback_calls = []
        
        def test_callback(status):
            callback_calls.append(status)
        
        tracker.add_callback(test_callback)
        tracker.update(success=True)
        
        assert len(callback_calls) == 1
        assert callback_calls[0]['completed'] == 1


class TestMRIQCProcessor:
    """Test MRIQC processor functionality."""
    
    @pytest.fixture
    def processor(self):
        """Create MRIQC processor instance."""
        return MRIQCProcessor(max_workers=2)
    
    @pytest.fixture
    def sample_mriqc_data(self):
        """Create sample MRIQC data."""
        return pd.DataFrame({
            'bids_name': [
                'sub-001_ses-01_T1w.nii.gz',
                'sub-002_T2w.nii.gz',
                'sub-003_ses-02_task-rest_bold.nii.gz'
            ],
            'snr': [12.5, 15.2, np.nan],
            'cnr': [3.2, 4.1, 2.8],
            'fber': [1500.0, 1800.0, np.nan],
            'efc': [0.45, 0.38, 0.52],
            'fwhm_avg': [2.8, 2.6, 3.1],
            'fwhm_x': [2.9, 2.7, 3.2],
            'fwhm_y': [2.8, 2.6, 3.1],
            'fwhm_z': [2.7, 2.5, 3.0],
            'qi_1': [0.85, 0.92, 0.78],
            'cjv': [0.42, 0.38, 0.48],
            'dvars_std': [np.nan, np.nan, 1.2],
            'fd_mean': [np.nan, np.nan, 0.15],
            'gcor': [np.nan, np.nan, 0.05],
            'age': [25.5, 32.0, 28.3],
            'sex': ['F', 'M', 'F']
        })
    
    @pytest.fixture
    def sample_csv_file(self, sample_mriqc_data):
        """Create temporary CSV file with sample data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_mriqc_data.to_csv(f.name, index=False)
            return Path(f.name)
    
    def test_processor_initialization(self):
        """Test processor initialization."""
        processor = MRIQCProcessor(max_workers=4)
        assert processor.max_workers == 4
        assert hasattr(processor, 'executor')
    
    def test_parse_mriqc_file_success(self, processor, sample_csv_file):
        """Test successful MRIQC file parsing."""
        df = processor.parse_mriqc_file(sample_csv_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert 'bids_name' in df.columns
        assert 'snr' in df.columns
    
    def test_parse_mriqc_file_not_found(self, processor):
        """Test parsing non-existent file."""
        with pytest.raises(MRIQCProcessingError, match="File not found"):
            processor.parse_mriqc_file("nonexistent.csv")
    
    def test_parse_mriqc_file_wrong_extension(self, processor):
        """Test parsing file with wrong extension."""
        with tempfile.NamedTemporaryFile(suffix='.txt') as f:
            with pytest.raises(MRIQCProcessingError, match="File must be CSV format"):
                processor.parse_mriqc_file(f.name)
    
    def test_parse_mriqc_file_empty(self, processor):
        """Test parsing empty CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv') as f:
            f.write("")  # Empty file
            f.flush()
            
            with pytest.raises(MRIQCProcessingError, match="empty"):
                processor.parse_mriqc_file(f.name)
    
    def test_validate_mriqc_format_valid(self, processor, sample_mriqc_data):
        """Test validation of valid MRIQC format."""
        errors = processor.validate_mriqc_format(sample_mriqc_data)
        assert len(errors) == 0
    
    def test_validate_mriqc_format_missing_required(self, processor):
        """Test validation with missing required columns."""
        df = pd.DataFrame({'snr': [1, 2, 3]})  # Missing bids_name
        errors = processor.validate_mriqc_format(df)
        
        assert len(errors) > 0
        assert any("required columns" in error.message for error in errors)
    
    def test_validate_mriqc_format_no_metrics(self, processor):
        """Test validation with no quality metrics."""
        df = pd.DataFrame({'bids_name': ['sub-001_T1w.nii.gz']})
        errors = processor.validate_mriqc_format(df)
        
        assert len(errors) > 0
        assert any("No recognized quality metrics" in error.message for error in errors)
    
    def test_validate_mriqc_format_non_numeric(self, processor):
        """Test validation with non-numeric metric values."""
        df = pd.DataFrame({
            'bids_name': ['sub-001_T1w.nii.gz'],
            'snr': ['invalid_value']
        })
        errors = processor.validate_mriqc_format(df)
        
        assert len(errors) > 0
        assert any("Non-numeric values" in error.message for error in errors)
    
    def test_extract_subject_info_basic(self, processor):
        """Test basic subject info extraction."""
        row = pd.Series({
            'bids_name': 'sub-001_ses-01_T1w.nii.gz',
            'age': 25.5,
            'sex': 'F'
        })
        
        subject_info = processor.extract_subject_info(row)
        
        assert subject_info.subject_id == '001'
        assert subject_info.session == '01'
        assert subject_info.scan_type == ScanType.T1W
        assert subject_info.age == 25.5
        assert subject_info.sex == Sex.FEMALE
    
    def test_extract_subject_info_missing_bids_name(self, processor):
        """Test subject info extraction with missing bids_name."""
        row = pd.Series({'age': 25.5})
        
        with pytest.raises(MRIQCValidationError, match="Missing bids_name"):
            processor.extract_subject_info(row)
    
    def test_parse_bids_name_variations(self, processor):
        """Test parsing various BIDS filename formats."""
        test_cases = [
            ('sub-001_T1w.nii.gz', '001', None, ScanType.T1W),
            ('sub-ABC123_ses-baseline_T2w.nii.gz', 'ABC123', 'baseline', ScanType.T2W),
            ('sub-P001_task-rest_bold.nii.gz', 'P001', None, ScanType.BOLD),
            ('sub-S01_ses-01_dwi.nii.gz', 'S01', '01', ScanType.DWI),
            ('sub-X_FLAIR.nii.gz', 'X', None, ScanType.FLAIR)
        ]
        
        for bids_name, expected_sub, expected_ses, expected_scan in test_cases:
            subject_id, session, scan_type = processor._parse_bids_name(bids_name)
            assert subject_id == expected_sub
            assert session == expected_ses
            assert scan_type == expected_scan
    
    def test_parse_sex_variations(self, processor):
        """Test parsing various sex value formats."""
        test_cases = [
            ('M', Sex.MALE),
            ('male', Sex.MALE),
            ('1', Sex.MALE),
            ('F', Sex.FEMALE),
            ('female', Sex.FEMALE),
            ('2', Sex.FEMALE),
            ('O', Sex.OTHER),
            ('U', Sex.UNKNOWN),
            ('unknown', Sex.UNKNOWN),
            (np.nan, None),
            ('invalid', Sex.UNKNOWN)
        ]
        
        for input_val, expected in test_cases:
            result = processor._parse_sex(input_val)
            assert result == expected
    
    def test_extract_quality_metrics_anatomical(self, processor):
        """Test extraction of anatomical quality metrics."""
        row = pd.Series({
            'snr': 12.5,
            'cnr': 3.2,
            'fber': 1500.0,
            'efc': 0.45,
            'fwhm_avg': 2.8,
            'qi_1': 0.85,
            'cjv': 0.42
        })
        
        metrics = processor.extract_quality_metrics(row)
        
        assert metrics.snr == 12.5
        assert metrics.cnr == 3.2
        assert metrics.fber == 1500.0
        assert metrics.efc == 0.45
        assert metrics.fwhm_avg == 2.8
        assert metrics.qi1 == 0.85
        assert metrics.cjv == 0.42
    
    def test_extract_quality_metrics_functional(self, processor):
        """Test extraction of functional quality metrics."""
        row = pd.Series({
            'dvars_std': 1.2,
            'fd_mean': 0.15,
            'fd_num': 5,
            'fd_perc': 2.5,
            'gcor': 0.05
        })
        
        metrics = processor.extract_quality_metrics(row)
        
        assert metrics.dvars == 1.2
        assert metrics.fd_mean == 0.15
        assert metrics.fd_num == 5
        assert metrics.fd_perc == 2.5
        assert metrics.gcor == 0.05
    
    def test_extract_quality_metrics_missing_values(self, processor):
        """Test extraction with missing metric values."""
        row = pd.Series({
            'snr': 12.5,
            'cnr': np.nan,  # Missing value
            'fber': None    # Missing value
        })
        
        metrics = processor.extract_quality_metrics(row)
        
        assert metrics.snr == 12.5
        assert metrics.cnr is None
        assert metrics.fber is None
    
    def test_extract_quality_metrics_alternative_columns(self, processor):
        """Test extraction using alternative column names."""
        row = pd.Series({
            'snr_total': 12.5,  # Alternative to 'snr'
            'dvars_vstd': 1.2   # Alternative to 'dvars_std'
        })
        
        metrics = processor.extract_quality_metrics(row)
        
        assert metrics.snr == 12.5
        assert metrics.dvars == 1.2
    
    def test_handle_special_metrics_fwhm_calculation(self, processor):
        """Test FWHM average calculation from components."""
        row = pd.Series({
            'fwhm_x': 2.9,
            'fwhm_y': 2.8,
            'fwhm_z': 2.7
            # No fwhm_avg provided
        })
        
        metrics = processor.extract_quality_metrics(row)
        
        # Should calculate average
        expected_avg = (2.9 + 2.8 + 2.7) / 3
        assert abs(metrics.fwhm_avg - expected_avg) < 0.001
    
    def test_handle_special_metrics_outlier_fraction(self, processor):
        """Test outlier fraction handling."""
        test_cases = [
            ({'outlier_frac': 0.05}, 0.05),
            ({'outliers_percent': 5.0}, 0.05),  # Convert percentage
            ({'outliers_percent': 0.05}, 0.05)  # Already fraction
        ]
        
        for row_data, expected in test_cases:
            row = pd.Series(row_data)
            metrics_data = {}
            processor._handle_special_metrics(row, metrics_data)
            
            assert abs(metrics_data.get('outlier_fraction', 0) - expected) < 0.001
    
    def test_process_single_file_success(self, processor, sample_csv_file):
        """Test successful single file processing."""
        subjects = processor.process_single_file(sample_csv_file)
        
        assert len(subjects) == 3
        assert all(isinstance(s, ProcessedSubject) for s in subjects)
        assert all(s.subject_info.subject_id for s in subjects)
        assert all(isinstance(s.raw_metrics, MRIQCMetrics) for s in subjects)
    
    def test_process_single_file_validation_error(self, processor):
        """Test single file processing with validation errors."""
        # Create invalid CSV
        invalid_data = pd.DataFrame({'invalid_column': [1, 2, 3]})
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            invalid_data.to_csv(f.name, index=False)
            
            with pytest.raises(MRIQCValidationError):
                processor.process_single_file(f.name)
    
    @pytest.mark.asyncio
    async def test_batch_process_files_success(self, processor, sample_csv_file):
        """Test successful batch file processing."""
        # Create multiple test files
        file_paths = [sample_csv_file]
        
        subjects, errors = await processor.batch_process_files(file_paths)
        
        assert len(subjects) == 3  # 3 subjects in sample file
        assert len(errors) == 0
        assert all(isinstance(s, ProcessedSubject) for s in subjects)
    
    @pytest.mark.asyncio
    async def test_batch_process_files_with_errors(self, processor, sample_csv_file):
        """Test batch processing with some file errors."""
        # Mix valid and invalid files
        file_paths = [sample_csv_file, "nonexistent.csv"]
        
        subjects, errors = await processor.batch_process_files(file_paths)
        
        assert len(subjects) == 3  # From valid file
        assert len(errors) == 1    # From invalid file
        assert isinstance(errors[0], ProcessingError)
    
    @pytest.mark.asyncio
    async def test_batch_process_files_empty_list(self, processor):
        """Test batch processing with empty file list."""
        subjects, errors = await processor.batch_process_files([])
        
        assert len(subjects) == 0
        assert len(errors) == 0
    
    @pytest.mark.asyncio
    async def test_batch_process_files_with_callback(self, processor, sample_csv_file):
        """Test batch processing with progress callback."""
        callback_calls = []
        
        def progress_callback(status):
            callback_calls.append(status)
        
        subjects, errors = await processor.batch_process_files(
            [sample_csv_file], 
            progress_callback=progress_callback
        )
        
        assert len(subjects) == 3
        assert len(callback_calls) > 0
        assert all('progress_percent' in call for call in callback_calls)
    
    def test_get_supported_metrics(self, processor):
        """Test getting supported metrics list."""
        metrics = processor.get_supported_metrics()
        
        assert 'anatomical' in metrics
        assert 'functional' in metrics
        assert 'snr' in metrics['anatomical']
        assert 'dvars' in metrics['functional']
    
    def test_safe_numeric_convert(self, processor):
        """Test safe numeric conversion."""
        test_cases = [
            (12.5, 12.5),
            ('12.5', 12.5),
            ('invalid', None),
            (np.nan, None),
            (None, None),
            (0, 0.0)
        ]
        
        for input_val, expected in test_cases:
            result = processor._safe_numeric_convert(input_val)
            if expected is None:
                assert result is None
            else:
                assert abs(result - expected) < 0.001
    
    def test_is_numeric_or_null(self, processor):
        """Test numeric/null value checking."""
        test_cases = [
            (12.5, True),
            ('12.5', True),
            ('invalid', False),
            (np.nan, True),
            (None, True),
            (pd.NA, True)
        ]
        
        for input_val, expected in test_cases:
            result = processor._is_numeric_or_null(input_val)
            assert result == expected
    
    def test_parse_date_formats(self, processor):
        """Test date parsing with various formats."""
        test_cases = [
            ('2024-01-15', datetime(2024, 1, 15)),
            ('2024-01-15 10:30:00', datetime(2024, 1, 15, 10, 30, 0)),
            ('2024-01-15T10:30:00', datetime(2024, 1, 15, 10, 30, 0)),
            ('01/15/2024', datetime(2024, 1, 15)),
            ('invalid_date', None),
            (np.nan, None)
        ]
        
        for input_val, expected in test_cases:
            result = processor._parse_date(input_val)
            assert result == expected


class TestMRIQCProcessorIntegration:
    """Integration tests for MRIQC processor."""
    
    @pytest.fixture
    def processor(self):
        """Create processor for integration tests."""
        return MRIQCProcessor(max_workers=2)
    
    def test_full_processing_pipeline(self, processor):
        """Test complete processing pipeline from CSV to ProcessedSubject."""
        # Create comprehensive test data
        test_data = pd.DataFrame({
            'bids_name': ['sub-001_ses-baseline_T1w.nii.gz'],
            'snr': [12.5],
            'cnr': [3.2],
            'fber': [1500.0],
            'efc': [0.45],
            'fwhm_avg': [2.8],
            'fwhm_x': [2.9],
            'fwhm_y': [2.8],
            'fwhm_z': [2.7],
            'qi_1': [0.85],
            'qi_2': [0.78],
            'cjv': [0.42],
            'wm2max': [0.65],
            'age': [25.5],
            'sex': ['F'],
            'site': ['Site_A'],
            'scanner': ['Siemens Prisma 3T']
        })
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f.name, index=False)
            
            # Process the file
            subjects = processor.process_single_file(f.name)
            
            assert len(subjects) == 1
            subject = subjects[0]
            
            # Verify subject info
            assert subject.subject_info.subject_id == '001'
            assert subject.subject_info.session == 'baseline'
            assert subject.subject_info.scan_type == ScanType.T1W
            assert subject.subject_info.age == 25.5
            assert subject.subject_info.sex == Sex.FEMALE
            assert subject.subject_info.site == 'Site_A'
            assert subject.subject_info.scanner == 'Siemens Prisma 3T'
            
            # Verify metrics
            metrics = subject.raw_metrics
            assert metrics.snr == 12.5
            assert metrics.cnr == 3.2
            assert metrics.fber == 1500.0
            assert metrics.efc == 0.45
            assert metrics.fwhm_avg == 2.8
            assert metrics.qi1 == 0.85
            assert metrics.qi2 == 0.78
            assert metrics.cjv == 0.42
            assert metrics.wm2max == 0.65
            
            # Verify processing metadata
            assert isinstance(subject.processing_timestamp, datetime)
            assert subject.quality_assessment.overall_status == QualityStatus.UNCERTAIN
    
    def test_mixed_scan_types_processing(self, processor):
        """Test processing file with mixed scan types."""
        test_data = pd.DataFrame({
            'bids_name': [
                'sub-001_T1w.nii.gz',
                'sub-001_T2w.nii.gz',
                'sub-001_task-rest_bold.nii.gz',
                'sub-001_dwi.nii.gz'
            ],
            'snr': [12.5, 15.2, np.nan, 8.3],
            'cnr': [3.2, 4.1, np.nan, 2.8],
            'dvars_std': [np.nan, np.nan, 1.2, np.nan],
            'fd_mean': [np.nan, np.nan, 0.15, np.nan],
            'age': [25.5, 25.5, 25.5, 25.5],
            'sex': ['F', 'F', 'F', 'F']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f.name, index=False)
            
            subjects = processor.process_single_file(f.name)
            
            assert len(subjects) == 4
            
            # Check scan types
            scan_types = [s.subject_info.scan_type for s in subjects]
            assert ScanType.T1W in scan_types
            assert ScanType.T2W in scan_types
            assert ScanType.BOLD in scan_types
            assert ScanType.DWI in scan_types
            
            # Check that functional metrics are only present for BOLD
            bold_subject = next(s for s in subjects if s.subject_info.scan_type == ScanType.BOLD)
            assert bold_subject.raw_metrics.dvars == 1.2
            assert bold_subject.raw_metrics.fd_mean == 0.15
            
            # Check that anatomical metrics are present for structural scans
            t1_subject = next(s for s in subjects if s.subject_info.scan_type == ScanType.T1W)
            assert t1_subject.raw_metrics.snr == 12.5
            assert t1_subject.raw_metrics.cnr == 3.2


if __name__ == "__main__":
    pytest.main([__file__])