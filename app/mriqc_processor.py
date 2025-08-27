"""
MRIQC Data Processor

This module provides functionality to parse, validate, and extract quality metrics
from MRIQC output files. It supports both individual file processing and batch
processing with progress tracking.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Optional, Union, Tuple, Callable, Any
from datetime import datetime
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
import re

from .models import (
    MRIQCMetrics, SubjectInfo, ProcessedSubject, QualityAssessment,
    QualityStatus, ScanType, Sex, ProcessingError, ValidationError
)

logger = logging.getLogger(__name__)


class MRIQCValidationError(Exception):
    """Custom exception for MRIQC validation errors."""
    pass


class MRIQCProcessingError(Exception):
    """Custom exception for MRIQC processing errors."""
    pass


class ProgressTracker:
    """Simple progress tracking for batch operations."""
    
    def __init__(self, total: int):
        self.total = total
        self.completed = 0
        self.failed = 0
        self.start_time = datetime.now()
        self.callbacks: List[Callable] = []
    
    def add_callback(self, callback: Callable):
        """Add a progress callback function."""
        self.callbacks.append(callback)
    
    def update(self, success: bool = True):
        """Update progress counters."""
        if success:
            self.completed += 1
        else:
            self.failed += 1
        
        # Call all registered callbacks
        for callback in self.callbacks:
            try:
                callback(self.get_status())
            except Exception as e:
                logger.warning(f"Progress callback failed: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current progress status."""
        processed = self.completed + self.failed
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        return {
            'total': self.total,
            'completed': self.completed,
            'failed': self.failed,
            'processed': processed,
            'remaining': self.total - processed,
            'progress_percent': (processed / self.total * 100) if self.total > 0 else 0,
            'elapsed_seconds': elapsed,
            'estimated_remaining_seconds': (elapsed / processed * (self.total - processed)) if processed > 0 else None
        }


class MRIQCProcessor:
    """
    Main processor for MRIQC data files.
    
    Handles parsing, validation, and extraction of quality metrics from
    MRIQC CSV output files with support for batch processing.
    """
    
    # Standard MRIQC column mappings
    ANATOMICAL_COLUMNS = {
        'snr': ['snr', 'snr_total', 'snr_wm'],
        'cnr': ['cnr'],
        'fber': ['fber'],
        'efc': ['efc'],
        'fwhm_avg': ['fwhm_avg'],
        'fwhm_x': ['fwhm_x'],
        'fwhm_y': ['fwhm_y'],
        'fwhm_z': ['fwhm_z'],
        'qi1': ['qi_1'],
        'qi2': ['qi_2'],
        'cjv': ['cjv'],
        'wm2max': ['wm2max']
    }
    
    FUNCTIONAL_COLUMNS = {
        'dvars': ['dvars_std', 'dvars_vstd'],
        'fd_mean': ['fd_mean'],
        'fd_num': ['fd_num'],
        'fd_perc': ['fd_perc'],
        'gcor': ['gcor'],
        'gsr_x': ['gsr_x'],
        'gsr_y': ['gsr_y']
    }
    
    # Required columns for basic processing
    REQUIRED_COLUMNS = ['bids_name']
    
    def __init__(self, max_workers: int = 4):
        """
        Initialize the MRIQC processor.
        
        Args:
            max_workers: Maximum number of worker threads for batch processing
        """
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    def parse_mriqc_file(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Parse an MRIQC CSV file.
        
        Args:
            file_path: Path to the MRIQC CSV file
            
        Returns:
            Parsed DataFrame
            
        Raises:
            MRIQCProcessingError: If file cannot be parsed
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise MRIQCProcessingError(f"File not found: {file_path}")
        
        if not file_path.suffix.lower() == '.csv':
            raise MRIQCProcessingError(f"File must be CSV format: {file_path}")
        
        try:
            # Try different encodings
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info(f"Successfully parsed {file_path} with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise MRIQCProcessingError(f"Could not decode file with any supported encoding: {file_path}")
            
            if df.empty:
                raise MRIQCProcessingError(f"File is empty: {file_path}")
            
            logger.info(f"Parsed MRIQC file: {file_path} ({len(df)} rows, {len(df.columns)} columns)")
            return df
            
        except pd.errors.EmptyDataError:
            raise MRIQCProcessingError(f"File is empty or contains no data: {file_path}")
        except pd.errors.ParserError as e:
            raise MRIQCProcessingError(f"Failed to parse CSV file {file_path}: {str(e)}")
        except Exception as e:
            raise MRIQCProcessingError(f"Unexpected error parsing {file_path}: {str(e)}")
    
    def validate_mriqc_format(self, df: pd.DataFrame, file_path: Optional[str] = None) -> List[ValidationError]:
        """
        Validate MRIQC DataFrame format and content.
        
        Args:
            df: DataFrame to validate
            file_path: Optional file path for error reporting
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        file_ref = f" in {file_path}" if file_path else ""
        
        # Check for required columns
        missing_required = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
        if missing_required:
            errors.append(ValidationError(
                field="required_columns",
                message=f"Missing required columns{file_ref}: {missing_required}",
                invalid_value=missing_required,
                expected_type="list[str]"
            ))
        
        # Check for at least some quality metrics
        all_metric_columns = []
        for metric_list in {**self.ANATOMICAL_COLUMNS, **self.FUNCTIONAL_COLUMNS}.values():
            all_metric_columns.extend(metric_list)
        
        available_metrics = [col for col in all_metric_columns if col in df.columns]
        if not available_metrics:
            errors.append(ValidationError(
                field="quality_metrics",
                message=f"No recognized quality metrics found{file_ref}",
                invalid_value=list(df.columns),
                expected_type="quality_metric_columns"
            ))
        
        # Validate data types for numeric columns
        for col in available_metrics:
            if col in df.columns:
                non_numeric = df[col].apply(lambda x: not self._is_numeric_or_null(x))
                if non_numeric.any():
                    invalid_values = df.loc[non_numeric, col].unique()[:5]  # Show first 5
                    errors.append(ValidationError(
                        field=col,
                        message=f"Non-numeric values in metric column {col}{file_ref}",
                        invalid_value=invalid_values.tolist(),
                        expected_type="numeric"
                    ))
        
        # Check for completely empty rows
        if df.isnull().all(axis=1).any():
            empty_rows = df.index[df.isnull().all(axis=1)].tolist()
            errors.append(ValidationError(
                field="empty_rows",
                message=f"Found completely empty rows{file_ref}",
                invalid_value=empty_rows,
                expected_type="non_empty_rows"
            ))
        
        return errors
    
    def _is_numeric_or_null(self, value) -> bool:
        """Check if a value is numeric or null."""
        if pd.isna(value):
            return True
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, str):
            try:
                float(value)
                return True
            except ValueError:
                return False
        return False
    
    def extract_subject_info(self, row: pd.Series) -> SubjectInfo:
        """
        Extract subject information from a DataFrame row.
        
        Args:
            row: DataFrame row containing subject data
            
        Returns:
            SubjectInfo object
            
        Raises:
            MRIQCValidationError: If required information is missing or invalid
        """
        # Extract subject ID from bids_name
        bids_name = row.get('bids_name', '')
        if not bids_name:
            raise MRIQCValidationError("Missing bids_name field")
        
        # Parse BIDS filename to extract components
        subject_id, session, scan_type = self._parse_bids_name(bids_name)
        
        # Extract other fields with fallbacks
        age = self._safe_numeric_convert(row.get('age'))
        sex = self._parse_sex(row.get('sex'))
        
        # Try to extract acquisition date
        acq_date = None
        for date_col in ['acquisition_date', 'acq_date', 'date']:
            if date_col in row and pd.notna(row[date_col]):
                acq_date = self._parse_date(row[date_col])
                break
        
        # Extract site and scanner info
        site = row.get('site', row.get('scanner_site'))
        scanner = row.get('scanner', row.get('scanner_model'))
        
        return SubjectInfo(
            subject_id=subject_id,
            age=age,
            sex=sex,
            session=session,
            scan_type=scan_type,
            acquisition_date=acq_date,
            site=site,
            scanner=scanner
        )
    
    def _parse_bids_name(self, bids_name: str) -> Tuple[str, Optional[str], ScanType]:
        """
        Parse BIDS filename to extract subject, session, and scan type.
        
        Args:
            bids_name: BIDS filename
            
        Returns:
            Tuple of (subject_id, session, scan_type)
        """
        # Remove file extension
        name = Path(bids_name).stem
        
        # Extract subject ID
        sub_match = re.search(r'sub-([^_]+)', name)
        subject_id = sub_match.group(1) if sub_match else name
        
        # Extract session
        ses_match = re.search(r'ses-([^_]+)', name)
        session = ses_match.group(1) if ses_match else None
        
        # Extract scan type
        scan_type = ScanType.T1W  # Default
        if '_T1w' in name:
            scan_type = ScanType.T1W
        elif '_T2w' in name:
            scan_type = ScanType.T2W
        elif '_bold' in name or '_BOLD' in name:
            scan_type = ScanType.BOLD
        elif '_dwi' in name or '_DWI' in name:
            scan_type = ScanType.DWI
        elif '_FLAIR' in name or '_flair' in name:
            scan_type = ScanType.FLAIR
        
        return subject_id, session, scan_type
    
    def _parse_sex(self, sex_value) -> Optional[Sex]:
        """Parse sex value from various formats."""
        if pd.isna(sex_value):
            return None
        
        sex_str = str(sex_value).upper().strip()
        sex_mapping = {
            'M': Sex.MALE, 'MALE': Sex.MALE, '1': Sex.MALE,
            'F': Sex.FEMALE, 'FEMALE': Sex.FEMALE, '2': Sex.FEMALE,
            'O': Sex.OTHER, 'OTHER': Sex.OTHER,
            'U': Sex.UNKNOWN, 'UNKNOWN': Sex.UNKNOWN, 'UNK': Sex.UNKNOWN
        }
        
        return sex_mapping.get(sex_str, Sex.UNKNOWN)
    
    def _parse_date(self, date_value) -> Optional[datetime]:
        """Parse date from various formats."""
        if pd.isna(date_value):
            return None
        
        date_str = str(date_value).strip()
        
        # Try common date formats
        date_formats = [
            '%Y-%m-%d',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%m/%d/%Y',
            '%d/%m/%Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {date_value}")
        return None
    
    def _safe_numeric_convert(self, value) -> Optional[float]:
        """Safely convert value to float."""
        if pd.isna(value):
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def extract_quality_metrics(self, row: pd.Series) -> MRIQCMetrics:
        """
        Extract quality metrics from a DataFrame row.
        
        Args:
            row: DataFrame row containing MRIQC metrics
            
        Returns:
            MRIQCMetrics object with extracted values
        """
        metrics_data = {}
        
        # Extract anatomical metrics
        for metric_name, possible_columns in self.ANATOMICAL_COLUMNS.items():
            value = self._extract_metric_value(row, possible_columns)
            if value is not None:
                metrics_data[metric_name] = value
        
        # Extract functional metrics
        for metric_name, possible_columns in self.FUNCTIONAL_COLUMNS.items():
            value = self._extract_metric_value(row, possible_columns)
            if value is not None:
                metrics_data[metric_name] = value
        
        # Handle special cases and derived metrics
        self._handle_special_metrics(row, metrics_data)
        
        return MRIQCMetrics(**metrics_data)
    
    def _extract_metric_value(self, row: pd.Series, possible_columns: List[str]) -> Optional[float]:
        """
        Extract metric value from row using possible column names.
        
        Args:
            row: DataFrame row
            possible_columns: List of possible column names for the metric
            
        Returns:
            Extracted numeric value or None
        """
        for col in possible_columns:
            if col in row and pd.notna(row[col]):
                return self._safe_numeric_convert(row[col])
        return None
    
    def _handle_special_metrics(self, row: pd.Series, metrics_data: Dict[str, float]):
        """Handle special metric calculations and derivations."""
        
        # Calculate outlier fraction if not present
        if 'outlier_fraction' not in metrics_data:
            # Try to calculate from other outlier metrics
            outlier_cols = ['outlier_frac', 'outlier_ratio', 'outliers_percent']
            for col in outlier_cols:
                if col in row and pd.notna(row[col]):
                    value = self._safe_numeric_convert(row[col])
                    if value is not None:
                        # Convert percentage to fraction if needed
                        if col == 'outliers_percent' and value > 1:
                            value = value / 100
                        metrics_data['outlier_fraction'] = value
                        break
        
        # Ensure FWHM consistency
        fwhm_components = ['fwhm_x', 'fwhm_y', 'fwhm_z']
        if all(comp in metrics_data for comp in fwhm_components) and 'fwhm_avg' not in metrics_data:
            # Calculate average if components are available
            fwhm_avg = sum(metrics_data[comp] for comp in fwhm_components) / 3
            metrics_data['fwhm_avg'] = fwhm_avg
    
    def process_single_file(self, file_path: Union[str, Path]) -> List[ProcessedSubject]:
        """
        Process a single MRIQC file.
        
        Args:
            file_path: Path to MRIQC CSV file
            
        Returns:
            List of ProcessedSubject objects
            
        Raises:
            MRIQCProcessingError: If processing fails
        """
        try:
            # Parse the file
            df = self.parse_mriqc_file(file_path)
            
            # Validate format
            validation_errors = self.validate_mriqc_format(df, str(file_path))
            if validation_errors:
                error_messages = [error.message for error in validation_errors]
                raise MRIQCValidationError(f"Validation failed: {'; '.join(error_messages)}")
            
            # Process each row
            processed_subjects = []
            for idx, row in df.iterrows():
                try:
                    subject_info = self.extract_subject_info(row)
                    raw_metrics = self.extract_quality_metrics(row)
                    
                    # Create basic quality assessment (will be enhanced by QualityAssessor)
                    quality_assessment = QualityAssessment(
                        overall_status=QualityStatus.UNCERTAIN,
                        metric_assessments={},
                        composite_score=0.0,
                        confidence=0.0
                    )
                    
                    processed_subject = ProcessedSubject(
                        subject_info=subject_info,
                        raw_metrics=raw_metrics,
                        quality_assessment=quality_assessment,
                        processing_timestamp=datetime.now()
                    )
                    
                    processed_subjects.append(processed_subject)
                    
                except Exception as e:
                    logger.error(f"Failed to process row {idx} in {file_path}: {str(e)}")
                    # Continue processing other rows
                    continue
            
            if not processed_subjects:
                raise MRIQCProcessingError(f"No subjects could be processed from {file_path}")
            
            logger.info(f"Successfully processed {len(processed_subjects)} subjects from {file_path}")
            return processed_subjects
            
        except (MRIQCProcessingError, MRIQCValidationError):
            raise
        except Exception as e:
            raise MRIQCProcessingError(f"Unexpected error processing {file_path}: {str(e)}")
    
    async def batch_process_files(
        self,
        file_paths: List[Union[str, Path]],
        progress_callback: Optional[Callable] = None
    ) -> Tuple[List[ProcessedSubject], List[ProcessingError]]:
        """
        Process multiple MRIQC files in batch with progress tracking.
        
        Args:
            file_paths: List of file paths to process
            progress_callback: Optional callback function for progress updates
            
        Returns:
            Tuple of (successful_subjects, processing_errors)
        """
        if not file_paths:
            return [], []
        
        # Initialize progress tracker
        progress = ProgressTracker(len(file_paths))
        if progress_callback:
            progress.add_callback(progress_callback)
        
        all_subjects = []
        processing_errors = []
        
        # Process files concurrently
        loop = asyncio.get_event_loop()
        
        async def process_file_async(file_path):
            """Async wrapper for file processing."""
            try:
                subjects = await loop.run_in_executor(
                    self.executor, 
                    self.process_single_file, 
                    file_path
                )
                progress.update(success=True)
                return subjects, None
            except Exception as e:
                error = ProcessingError(
                    error_type="file_processing_error",
                    message=f"Failed to process {file_path}: {str(e)}",
                    details={"file_path": str(file_path)},
                    suggestions=[
                        "Check file format and encoding",
                        "Verify MRIQC output structure",
                        "Check file permissions"
                    ],
                    error_code="MRIQC_PROC_001"
                )
                progress.update(success=False)
                return None, error
        
        # Process all files
        tasks = [process_file_async(file_path) for file_path in file_paths]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect results
        for result in results:
            if isinstance(result, Exception):
                processing_errors.append(ProcessingError(
                    error_type="async_processing_error",
                    message=f"Async processing failed: {str(result)}",
                    error_code="MRIQC_ASYNC_001"
                ))
            else:
                subjects, error = result
                if subjects:
                    all_subjects.extend(subjects)
                if error:
                    processing_errors.append(error)
        
        logger.info(f"Batch processing complete: {len(all_subjects)} subjects, {len(processing_errors)} errors")
        return all_subjects, processing_errors
    
    def get_supported_metrics(self) -> Dict[str, List[str]]:
        """
        Get list of supported MRIQC metrics.
        
        Returns:
            Dictionary mapping metric categories to metric names
        """
        return {
            'anatomical': list(self.ANATOMICAL_COLUMNS.keys()),
            'functional': list(self.FUNCTIONAL_COLUMNS.keys())
        }
    
    def __del__(self):
        """Cleanup executor on deletion."""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)