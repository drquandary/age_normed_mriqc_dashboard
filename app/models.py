"""
Core data models for the Age-Normed MRIQC Dashboard.

This module contains Pydantic models for MRIQC metrics, subject information,
quality assessments, and related data structures with comprehensive validation.
"""

from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from typing import List, Optional, Dict, Union, Any
from datetime import datetime
from enum import Enum
import re


class AgeGroup(str, Enum):
    """Age group classifications for normative data."""
    PEDIATRIC = "pediatric"
    ADOLESCENT = "adolescent"
    YOUNG_ADULT = "young_adult"
    MIDDLE_AGE = "middle_age"
    ELDERLY = "elderly"


class QualityStatus(str, Enum):
    """Quality assessment status categories."""
    PASS = "pass"
    WARNING = "warning"
    FAIL = "fail"
    UNCERTAIN = "uncertain"


class ScanType(str, Enum):
    """Supported MRI scan types."""
    T1W = "T1w"
    T2W = "T2w"
    BOLD = "BOLD"
    DWI = "DWI"
    FLAIR = "FLAIR"


class Sex(str, Enum):
    """Subject sex categories."""
    MALE = "M"
    FEMALE = "F"
    OTHER = "O"
    UNKNOWN = "U"


class MRIQCMetrics(BaseModel):
    """
    MRIQC quality metrics with validation.
    
    Contains both anatomical and functional MRI quality metrics
    with appropriate range validation.
    """
    
    # Anatomical metrics
    snr: Optional[float] = Field(
        None, 
        ge=0, 
        le=1000,
        description="Signal-to-noise ratio"
    )
    cnr: Optional[float] = Field(
        None, 
        ge=0, 
        le=100,
        description="Contrast-to-noise ratio"
    )
    fber: Optional[float] = Field(
        None, 
        ge=0, 
        le=100000,
        description="Foreground-background energy ratio"
    )
    efc: Optional[float] = Field(
        None, 
        ge=0, 
        le=1,
        description="Entropy focus criterion"
    )
    fwhm_avg: Optional[float] = Field(
        None, 
        ge=0, 
        le=20,
        description="Average full-width half-maximum"
    )
    fwhm_x: Optional[float] = Field(
        None, 
        ge=0, 
        le=20,
        description="FWHM in x direction"
    )
    fwhm_y: Optional[float] = Field(
        None, 
        ge=0, 
        le=20,
        description="FWHM in y direction"
    )
    fwhm_z: Optional[float] = Field(
        None, 
        ge=0, 
        le=20,
        description="FWHM in z direction"
    )
    qi1: Optional[float] = Field(
        None, 
        ge=0, 
        le=1,
        description="Mortamet quality index 1"
    )
    qi2: Optional[float] = Field(
        None, 
        ge=0, 
        le=1,
        description="Mortamet quality index 2"
    )
    cjv: Optional[float] = Field(
        None, 
        ge=0, 
        le=10,
        description="Coefficient of joint variation"
    )
    wm2max: Optional[float] = Field(
        None, 
        ge=0, 
        le=1,
        description="White matter to maximum intensity ratio"
    )
    
    # Functional metrics
    dvars: Optional[float] = Field(
        None, 
        ge=0, 
        le=1000,
        description="DVARS (temporal derivative of RMS variance over voxels)"
    )
    fd_mean: Optional[float] = Field(
        None, 
        ge=0, 
        le=10,
        description="Mean framewise displacement"
    )
    fd_num: Optional[int] = Field(
        None, 
        ge=0,
        description="Number of high motion timepoints"
    )
    fd_perc: Optional[float] = Field(
        None, 
        ge=0, 
        le=100,
        description="Percentage of high motion timepoints"
    )
    gcor: Optional[float] = Field(
        None, 
        ge=-1, 
        le=1,
        description="Global correlation"
    )
    gsr_x: Optional[float] = Field(
        None,
        description="Global signal regression x-component"
    )
    gsr_y: Optional[float] = Field(
        None,
        description="Global signal regression y-component"
    )
    
    # Additional derived metrics
    outlier_fraction: Optional[float] = Field(
        None, 
        ge=0, 
        le=1,
        description="Fraction of outlier voxels"
    )
    
    @field_validator('*', mode='before')
    @classmethod
    def convert_numeric_strings(cls, v):
        """Convert string representations of numbers to float/int."""
        if isinstance(v, str) and v.strip():
            try:
                # Try integer first for fd_num
                if '.' not in v:
                    return int(v)
                return float(v)
            except ValueError:
                return None
        return v
    
    @model_validator(mode='after')
    def validate_metric_consistency(self):
        """Validate consistency between related metrics."""
        # Check FWHM consistency
        fwhm_components = [self.fwhm_x, self.fwhm_y, self.fwhm_z]
        fwhm_avg = self.fwhm_avg
        
        if all(x is not None for x in fwhm_components) and fwhm_avg is not None:
            calculated_avg = sum(fwhm_components) / 3
            if abs(calculated_avg - fwhm_avg) > 0.5:  # Allow some tolerance
                raise ValueError("FWHM average inconsistent with component values")
        
        # Check framewise displacement consistency
        fd_num = self.fd_num
        fd_perc = self.fd_perc
        
        if fd_num is not None and fd_perc is not None:
            if fd_num == 0 and fd_perc > 0:
                raise ValueError("Inconsistent framewise displacement metrics")
        
        return self

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "snr": 12.5,
                "cnr": 3.2,
                "fber": 1500.0,
                "efc": 0.45,
                "fwhm_avg": 2.8,
                "qi1": 0.85,
                "cjv": 0.42,
                "dvars": 1.2,
                "fd_mean": 0.15,
                "gcor": 0.05
            }
        }
    )


class SubjectInfo(BaseModel):
    """
    Subject information with validation.
    
    Contains demographic and scan session information.
    """
    
    subject_id: str = Field(
        ..., 
        min_length=1,
        max_length=50,
        pattern=r'^[a-zA-Z0-9_-]+$',
        description="Subject identifier (alphanumeric, underscore, hyphen only)"
    )
    age: Optional[float] = Field(
        None, 
        ge=0, 
        le=120,
        description="Subject age in years"
    )
    sex: Optional[Sex] = Field(
        None,
        description="Subject sex"
    )
    session: Optional[str] = Field(
        None,
        max_length=20,
        pattern=r'^[a-zA-Z0-9_-]*$',
        description="Session identifier"
    )
    scan_type: ScanType = Field(
        ...,
        description="Type of MRI scan"
    )
    acquisition_date: Optional[datetime] = Field(
        None,
        description="Date and time of scan acquisition"
    )
    site: Optional[str] = Field(
        None,
        max_length=50,
        description="Acquisition site identifier"
    )
    scanner: Optional[str] = Field(
        None,
        max_length=100,
        description="Scanner model and manufacturer"
    )
    
    @field_validator('subject_id')
    @classmethod
    def validate_subject_id(cls, v):
        """Ensure subject ID doesn't contain potentially identifying information."""
        # Check for common PII patterns
        if re.search(r'\b\d{3}-\d{2}-\d{4}\b', v):  # SSN pattern
            raise ValueError("Subject ID appears to contain SSN")
        if re.search(r'\b\d{2}[/-]\d{2}[/-]\d{4}\b', v):  # Date pattern
            raise ValueError("Subject ID appears to contain date")
        return v
    
    @field_validator('age')
    @classmethod
    def validate_age_reasonableness(cls, v):
        """Validate age is reasonable for neuroimaging studies."""
        if v is not None and (v < 0.1 or v > 110):
            raise ValueError("Age outside reasonable range for neuroimaging")
        return v

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subject_id": "sub-001",
                "age": 25.5,
                "sex": "F",
                "session": "ses-01",
                "scan_type": "T1w",
                "acquisition_date": "2024-01-15T10:30:00",
                "site": "site-A",
                "scanner": "Siemens Prisma 3T"
            }
        }
    )


class NormalizedMetrics(BaseModel):
    """
    Age-normalized quality metrics.
    
    Contains raw metrics along with age-appropriate percentiles and z-scores.
    """
    
    raw_metrics: MRIQCMetrics
    percentiles: Dict[str, float] = Field(
        ...,
        description="Percentile scores for each metric relative to age group"
    )
    z_scores: Dict[str, float] = Field(
        ...,
        description="Z-scores for each metric relative to age group"
    )
    age_group: AgeGroup = Field(
        ...,
        description="Age group used for normalization"
    )
    normative_dataset: str = Field(
        ...,
        min_length=1,
        description="Name of normative dataset used"
    )
    
    @field_validator('percentiles', 'z_scores')
    @classmethod
    def validate_score_ranges(cls, v, info):
        """Validate score ranges are reasonable."""
        for metric, score in v.items():
            if info.field_name == 'percentiles':
                if not 0 <= score <= 100:
                    raise ValueError(f"Percentile for {metric} outside valid range (0-100)")
            elif info.field_name == 'z_scores':
                if abs(score) > 50:  # Very extreme z-scores are suspicious (increased threshold)
                    raise ValueError(f"Z-score for {metric} is extremely high: {score}")
        return v

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "raw_metrics": {
                    "snr": 12.5,
                    "cnr": 3.2,
                    "fber": 1500.0
                },
                "percentiles": {
                    "snr": 75.0,
                    "cnr": 60.0,
                    "fber": 80.0
                },
                "z_scores": {
                    "snr": 0.67,
                    "cnr": 0.25,
                    "fber": 0.84
                },
                "age_group": "young_adult",
                "normative_dataset": "HCP-YA"
            }
        }
    )


class QualityAssessment(BaseModel):
    """
    Quality assessment results.
    
    Contains overall and per-metric quality assessments with recommendations.
    """
    
    overall_status: QualityStatus = Field(
        ...,
        description="Overall quality assessment status"
    )
    metric_assessments: Dict[str, QualityStatus] = Field(
        ...,
        description="Quality status for each individual metric"
    )
    composite_score: float = Field(
        ...,
        ge=0,
        le=100,
        description="Composite quality score (0-100)"
    )
    recommendations: List[str] = Field(
        default_factory=list,
        description="Specific recommendations based on assessment"
    )
    flags: List[str] = Field(
        default_factory=list,
        description="Quality control flags raised"
    )
    confidence: float = Field(
        ...,
        ge=0,
        le=1,
        description="Confidence in the assessment (0-1)"
    )
    threshold_violations: Dict[str, Dict[str, Union[float, str]]] = Field(
        default_factory=dict,
        description="Details of threshold violations"
    )
    
    @field_validator('recommendations', 'flags')
    @classmethod
    def validate_text_lists(cls, v):
        """Validate text lists contain meaningful content."""
        return [item.strip() for item in v if item and item.strip()]

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "overall_status": "pass",
                "metric_assessments": {
                    "snr": "pass",
                    "cnr": "warning",
                    "fber": "pass"
                },
                "composite_score": 78.5,
                "recommendations": [
                    "Consider manual review of CNR values",
                    "Overall quality acceptable for analysis"
                ],
                "flags": ["cnr_borderline"],
                "confidence": 0.85,
                "threshold_violations": {
                    "cnr": {
                        "value": 2.8,
                        "threshold": 3.0,
                        "severity": "warning"
                    }
                }
            }
        }
    )


class ProcessedSubject(BaseModel):
    """
    Complete processed subject data.
    
    Combines all subject information, metrics, and assessments.
    """
    
    subject_info: SubjectInfo
    raw_metrics: MRIQCMetrics
    normalized_metrics: Optional[NormalizedMetrics] = Field(
        None,
        description="Age-normalized metrics (if age available)"
    )
    quality_assessment: QualityAssessment
    processing_timestamp: datetime = Field(
        default_factory=datetime.now,
        description="When the processing was completed"
    )
    processing_version: str = Field(
        default="1.0.0",
        description="Version of processing pipeline used"
    )
    notes: Optional[str] = Field(
        None,
        max_length=1000,
        description="Additional processing notes"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subject_info": {
                    "subject_id": "sub-001",
                    "age": 25.5,
                    "scan_type": "T1w"
                },
                "raw_metrics": {
                    "snr": 12.5,
                    "cnr": 3.2
                },
                "quality_assessment": {
                    "overall_status": "pass",
                    "composite_score": 78.5,
                    "confidence": 0.85
                },
                "processing_timestamp": "2024-01-15T14:30:00"
            }
        }
    )


class StudySummary(BaseModel):
    """
    Study-level summary statistics.
    
    Aggregated quality metrics and statistics across all subjects.
    """
    
    total_subjects: int = Field(
        ...,
        ge=0,
        description="Total number of subjects processed"
    )
    quality_distribution: Dict[QualityStatus, int] = Field(
        ...,
        description="Count of subjects by quality status"
    )
    age_group_distribution: Dict[AgeGroup, int] = Field(
        default_factory=dict,
        description="Count of subjects by age group"
    )
    metric_statistics: Dict[str, Dict[str, float]] = Field(
        default_factory=dict,
        description="Statistical summaries for each metric"
    )
    exclusion_rate: float = Field(
        ...,
        ge=0,
        le=1,
        description="Proportion of subjects failing quality control"
    )
    processing_date: datetime = Field(
        default_factory=datetime.now,
        description="When the summary was generated"
    )
    study_name: Optional[str] = Field(
        None,
        max_length=100,
        description="Name of the study"
    )
    
    @model_validator(mode='after')
    def validate_quality_counts(self):
        """Ensure quality distribution counts are consistent."""
        total = self.total_subjects
        if sum(self.quality_distribution.values()) != total:
            raise ValueError("Quality distribution counts don't match total subjects")
        return self

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "total_subjects": 100,
                "quality_distribution": {
                    "pass": 75,
                    "warning": 15,
                    "fail": 8,
                    "uncertain": 2
                },
                "age_group_distribution": {
                    "young_adult": 60,
                    "middle_age": 30,
                    "elderly": 10
                },
                "exclusion_rate": 0.08,
                "processing_date": "2024-01-15T16:00:00",
                "study_name": "Aging Brain Study"
            }
        }
    )


# Configuration and threshold models

class QualityThresholds(BaseModel):
    """
    Quality thresholds for a specific metric and age group.
    """
    
    metric_name: str = Field(
        ...,
        min_length=1,
        description="Name of the quality metric"
    )
    age_group: Union[AgeGroup, str] = Field(
        ...,
        description="Age group these thresholds apply to"
    )
    warning_threshold: float = Field(
        ...,
        description="Threshold for warning status"
    )
    fail_threshold: float = Field(
        ...,
        description="Threshold for fail status"
    )
    direction: str = Field(
        ...,
        pattern=r'^(higher_better|lower_better)$',
        description="Whether higher or lower values are better"
    )
    
    @model_validator(mode='after')
    def validate_threshold_order(self):
        """Ensure thresholds are in correct order based on direction."""
        warning = self.warning_threshold
        fail = self.fail_threshold
        direction = self.direction
        
        if warning is not None and fail is not None and direction:
            if direction == 'higher_better' and warning <= fail:
                raise ValueError("For higher_better metrics, warning threshold must be > fail threshold")
            elif direction == 'lower_better' and warning >= fail:
                raise ValueError("For lower_better metrics, warning threshold must be < fail threshold")
        
        return self
    
    @field_validator('age_group')
    @classmethod
    def validate_age_group(cls, v):
        """Convert AgeGroup enum to string if needed."""
        if isinstance(v, AgeGroup):
            return v.value
        return v

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "metric_name": "snr",
                "age_group": "young_adult",
                "warning_threshold": 10.0,
                "fail_threshold": 8.0,
                "direction": "higher_better"
            }
        }
    )


class StudyConfiguration(BaseModel):
    """
    Study-specific configuration settings.
    """
    
    study_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Name of the study"
    )
    custom_age_groups: Optional[List[Dict[str, Union[str, float]]]] = Field(
        None,
        description="Custom age group definitions"
    )
    custom_thresholds: Optional[List[QualityThresholds]] = Field(
        None,
        description="Custom quality thresholds"
    )
    normative_dataset: str = Field(
        default="default",
        description="Normative dataset to use"
    )
    exclusion_criteria: List[str] = Field(
        default_factory=list,
        description="Additional exclusion criteria"
    )
    created_by: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="User who created the configuration"
    )
    created_at: datetime = Field(
        default_factory=datetime.now,
        description="When the configuration was created"
    )
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "study_name": "Pediatric Development Study",
                "custom_age_groups": [
                    {"name": "early_childhood", "min_age": 3.0, "max_age": 6.0},
                    {"name": "school_age", "min_age": 6.0, "max_age": 12.0}
                ],
                "normative_dataset": "pediatric_norms_v2",
                "exclusion_criteria": ["excessive_motion", "artifacts"],
                "created_by": "researcher_001"
            }
        }
    )


# Error and validation models

class ValidationError(BaseModel):
    """
    Detailed validation error information.
    """
    
    field: str = Field(..., description="Field that failed validation")
    message: str = Field(..., description="Error message")
    invalid_value: Optional[Any] = Field(None, description="The invalid value")
    expected_type: Optional[str] = Field(None, description="Expected data type")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "field": "age",
                "message": "Age must be between 0 and 120",
                "invalid_value": -5,
                "expected_type": "float"
            }
        }
    )


class ProcessingError(BaseModel):
    """
    Processing error information.
    """
    
    error_type: str = Field(..., description="Type of error")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    suggestions: List[str] = Field(default_factory=list, description="Suggested solutions")
    error_code: str = Field(..., description="Unique error code")
    timestamp: datetime = Field(default_factory=datetime.now, description="When error occurred")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "error_type": "validation_error",
                "message": "Invalid MRIQC file format",
                "details": {"missing_columns": ["snr", "cnr"]},
                "suggestions": ["Check file format", "Verify MRIQC version"],
                "error_code": "MRIQC_001",
                "timestamp": "2024-01-15T10:30:00"
            }
        }
    )