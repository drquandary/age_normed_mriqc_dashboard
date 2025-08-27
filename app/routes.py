"""
API routes for the Age-Normed MRIQC Dashboard.

This module implements the core API endpoints for file upload, processing,
subject data retrieval, and batch processing status tracking.
"""

import asyncio
import io
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union
import tempfile
import os
import json

from fastapi import APIRouter, HTTPException, UploadFile, File, BackgroundTasks, Query, Depends, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .models import (
    ProcessedSubject, MRIQCMetrics, SubjectInfo, QualityAssessment,
    QualityStatus, AgeGroup, ProcessingError, StudySummary, StudyConfiguration,
    QualityThresholds
)
from .mriqc_processor import MRIQCProcessor, MRIQCProcessingError, MRIQCValidationError
from .quality_assessor import QualityAssessor
from .age_normalizer import AgeNormalizer
from .config_service import ConfigurationService, ConfigurationValidationError
from .batch_service import batch_service
from .file_monitor import file_monitor
from .error_handling import error_handler, audit_logger, get_request_id
from .exceptions import (
    FileProcessingException, MRIQCProcessingException, ValidationException,
    BatchProcessingException, ConfigurationException
)
from .security import (
    SecureFileHandler, SecurityConfig, security_auditor, data_retention_manager,
    SecurityThreat, ThreatType, SecurityLevel
)
from .performance_monitor import performance_monitor, monitor_performance
from .cache_service import cache_service
from .connection_pool import get_connection_pool
from . import config

logger = logging.getLogger(__name__)

router = APIRouter()

# Global instances
mriqc_processor = MRIQCProcessor()
quality_assessor = QualityAssessor()
age_normalizer = AgeNormalizer()
config_service = ConfigurationService()

# Security instances
security_config = SecurityConfig(
    max_file_size=config.MAX_FILE_SIZE,
    allowed_extensions=set(config.SUPPORTED_EXTENSIONS),
    allowed_mime_types=config.ALLOWED_MIME_TYPES,
    virus_scan_enabled=config.VIRUS_SCAN_ENABLED,
    data_retention_days=config.DATA_RETENTION_DAYS,
    cleanup_interval_hours=config.CLEANUP_INTERVAL_HOURS,
    enable_audit_logging=config.ENABLE_AUDIT_LOGGING,
    max_filename_length=config.MAX_FILENAME_LENGTH
)
secure_file_handler = SecureFileHandler(security_config, config.UPLOAD_DIR, config.TEMP_DIR)

# In-memory storage for batch processing status (in production, use Redis or database)
batch_status_store: Dict[str, Dict] = {}
processed_subjects_store: Dict[str, List[ProcessedSubject]] = {}

# WebSocket connection manager for real-time updates
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.batch_subscribers: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, batch_id: Optional[str] = None):
        await websocket.accept()
        self.active_connections.append(websocket)
        if batch_id:
            if batch_id not in self.batch_subscribers:
                self.batch_subscribers[batch_id] = []
            self.batch_subscribers[batch_id].append(websocket)

    def disconnect(self, websocket: WebSocket, batch_id: Optional[str] = None):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if batch_id and batch_id in self.batch_subscribers:
            if websocket in self.batch_subscribers[batch_id]:
                self.batch_subscribers[batch_id].remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        try:
            await websocket.send_text(message)
        except:
            # Connection might be closed
            pass

    async def broadcast_to_batch(self, message: str, batch_id: str):
        if batch_id in self.batch_subscribers:
            disconnected = []
            for connection in self.batch_subscribers[batch_id]:
                try:
                    await connection.send_text(message)
                except:
                    disconnected.append(connection)
            
            # Clean up disconnected connections
            for conn in disconnected:
                self.disconnect(conn, batch_id)

    async def broadcast_dashboard_update(self, message: str):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                disconnected.append(connection)
        
        # Clean up disconnected connections
        for conn in disconnected:
            self.disconnect(conn)

manager = ConnectionManager()


# Security middleware functions
def get_client_ip(request: Request) -> str:
    """Extract client IP address from request."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def get_user_agent(request: Request) -> str:
    """Extract user agent from request."""
    return request.headers.get("User-Agent", "unknown")


async def security_check_middleware(request: Request):
    """Perform security checks on incoming requests."""
    client_ip = get_client_ip(request)
    user_agent = get_user_agent(request)
    
    # Log data access
    security_auditor.log_data_access(
        resource=str(request.url.path),
        client_ip=client_ip,
        user_agent=user_agent
    )
    
    # Basic rate limiting could be implemented here
    # For now, just log the access
    return {"client_ip": client_ip, "user_agent": user_agent}


# Request/Response Models
class FileUploadResponse(BaseModel):
    """Response model for file upload."""
    message: str
    file_id: str
    filename: str
    size: int
    subjects_count: Optional[int] = None


class ProcessFileRequest(BaseModel):
    """Request model for processing uploaded files."""
    file_id: str
    apply_quality_assessment: bool = Field(default=True, description="Apply quality assessment")
    custom_thresholds: Optional[Dict[str, Dict]] = Field(default=None, description="Custom quality thresholds")


class ProcessFileResponse(BaseModel):
    """Response model for file processing."""
    batch_id: str
    message: str
    subjects_processed: int
    processing_errors: List[ProcessingError]
    estimated_completion_time: Optional[str] = None


class BatchStatusResponse(BaseModel):
    """Response model for batch processing status."""
    batch_id: str
    status: str  # 'pending', 'processing', 'completed', 'failed'
    progress: Dict
    subjects_processed: int
    total_subjects: int
    errors: List[ProcessingError]
    started_at: datetime
    completed_at: Optional[datetime] = None


class SubjectListResponse(BaseModel):
    """Response model for subject list."""
    subjects: List[ProcessedSubject]
    total_count: int
    page: int
    page_size: int
    filters_applied: Dict
    sort_applied: Optional[Dict] = None


class DashboardSummaryResponse(BaseModel):
    """Response model for dashboard summary."""
    total_subjects: int
    quality_distribution: Dict[str, int]
    age_group_distribution: Dict[str, int]
    scan_type_distribution: Dict[str, int]
    metric_statistics: Dict[str, Dict[str, float]]
    exclusion_rate: float
    processing_date: str
    batch_id: Optional[str] = None
    recent_activity: List[Dict]
    alerts: List[Dict]


class SubjectFilterRequest(BaseModel):
    """Request model for advanced subject filtering."""
    quality_status: Optional[List[QualityStatus]] = None
    age_group: Optional[List[AgeGroup]] = None
    scan_type: Optional[List[str]] = None
    age_range: Optional[Dict[str, float]] = None  # {"min": 18, "max": 65}
    metric_filters: Optional[Dict[str, Dict[str, float]]] = None  # {"snr": {"min": 10, "max": 50}}
    date_range: Optional[Dict[str, str]] = None  # {"start": "2024-01-01", "end": "2024-12-31"}
    batch_ids: Optional[List[str]] = None
    search_text: Optional[str] = None


class SubjectSortRequest(BaseModel):
    """Request model for subject sorting."""
    sort_by: str  # field name to sort by
    sort_order: str = Field(default="asc", pattern="^(asc|desc)$")  # "asc" or "desc"


class SubjectDetailResponse(BaseModel):
    """Response model for individual subject details."""
    subject: ProcessedSubject
    age_group_statistics: Optional[Dict] = None
    recommendations: List[str]


# Configuration Management Models

class CreateConfigurationRequest(BaseModel):
    """Request model for creating study configuration."""
    study_name: str = Field(..., min_length=1, max_length=100)
    normative_dataset: str = Field(default="default")
    custom_age_groups: Optional[List[Dict[str, Union[str, float]]]] = None
    custom_thresholds: Optional[List[QualityThresholds]] = None
    exclusion_criteria: List[str] = Field(default_factory=list)
    created_by: str = Field(..., min_length=1, max_length=50)


class UpdateConfigurationRequest(BaseModel):
    """Request model for updating study configuration."""
    normative_dataset: Optional[str] = None
    exclusion_criteria: Optional[List[str]] = None


class ConfigurationResponse(BaseModel):
    """Response model for study configuration."""
    study_name: str
    normative_dataset: str
    custom_age_groups: List[Dict]
    custom_thresholds: List[Dict]
    exclusion_criteria: List[str]
    created_by: str
    created_at: str
    updated_at: str


class ConfigurationSummaryResponse(BaseModel):
    """Response model for configuration summary."""
    study_name: str
    normative_dataset: str
    created_by: str
    created_at: str
    updated_at: str
    custom_age_groups_count: int
    custom_thresholds_count: int
    exclusion_criteria_count: int
    has_customizations: bool


class ConfigurationListResponse(BaseModel):
    """Response model for configuration list."""
    configurations: List[ConfigurationSummaryResponse]
    total_count: int


# Utility functions
def generate_batch_id() -> str:
    """Generate unique batch ID."""
    return str(uuid.uuid4())


def generate_file_id() -> str:
    """Generate unique file ID."""
    return str(uuid.uuid4())


async def process_subjects_background(
    subjects: List[ProcessedSubject],
    batch_id: str,
    apply_quality_assessment: bool = True
):
    """Background task for processing subjects with quality assessment."""
    
    # Log batch processing start
    audit_logger.log_user_action(
        action_type="batch_processing_start",
        resource_type="batch",
        resource_id=batch_id,
        new_values={
            'subjects_count': len(subjects),
            'apply_quality_assessment': apply_quality_assessment
        }
    )
    
    try:
        batch_status_store[batch_id]['status'] = 'processing'
        batch_status_store[batch_id]['started_at'] = datetime.now()
        
        # Send initial processing update
        await manager.broadcast_to_batch(
            json.dumps({
                "type": "batch_status_update",
                "batch_id": batch_id,
                "status": "processing",
                "progress": {"completed": 0, "total": len(subjects), "progress_percent": 0}
            }),
            batch_id
        )
        
        processed_subjects = []
        errors = []
        
        for i, subject in enumerate(subjects):
            try:
                if apply_quality_assessment:
                    # Apply quality assessment
                    quality_assessment = quality_assessor.assess_quality(
                        subject.raw_metrics,
                        subject.subject_info
                    )
                    
                    # Log quality control decision
                    audit_logger.log_quality_decision(
                        subject_id=subject.subject_info.subject_id,
                        decision=quality_assessment.overall_status.value,
                        reason=f"Automated assessment: {quality_assessment.composite_score:.1f}% score",
                        automated=True,
                        confidence=quality_assessment.confidence,
                        metrics=subject.raw_metrics.dict(exclude_none=True),
                        thresholds=quality_assessment.threshold_violations
                    )
                    
                    # Update subject with quality assessment
                    subject.quality_assessment = quality_assessment
                    
                    # Add normalized metrics if age is available
                    if subject.subject_info.age is not None:
                        try:
                            normalized_metrics = age_normalizer.normalize_metrics(
                                subject.raw_metrics,
                                subject.subject_info.age
                            )
                            subject.normalized_metrics = normalized_metrics
                        except Exception as norm_e:
                            logger.warning(f"Failed to normalize metrics for {subject.subject_info.subject_id}: {str(norm_e)}")
                            # Continue processing without normalized metrics
                
                processed_subjects.append(subject)
                
                # Update progress
                progress = {
                    'completed': i + 1,
                    'total': len(subjects),
                    'progress_percent': ((i + 1) / len(subjects)) * 100
                }
                batch_status_store[batch_id]['progress'] = progress
                
                # Send progress update every 10 subjects or at completion
                if (i + 1) % 10 == 0 or i + 1 == len(subjects):
                    await manager.broadcast_to_batch(
                        json.dumps({
                            "type": "batch_progress_update",
                            "batch_id": batch_id,
                            "progress": progress,
                            "current_subject": subject.subject_info.subject_id
                        }),
                        batch_id
                    )
                
            except Exception as e:
                # Create structured error response
                error_response = error_handler.handle_processing_error(
                    operation="subject_quality_assessment",
                    message=f"Failed to assess quality for subject {subject.subject_info.subject_id}",
                    exception=e,
                    context={
                        'subject_id': subject.subject_info.subject_id,
                        'batch_id': batch_id
                    }
                )
                
                error = ProcessingError(
                    error_type=error_response.error_type,
                    message=error_response.message,
                    details=error_response.details,
                    error_code=error_response.error_code
                )
                errors.append(error)
                
                # Send error update
                await manager.broadcast_to_batch(
                    json.dumps({
                        "type": "processing_error",
                        "batch_id": batch_id,
                        "error": error.model_dump(),
                        "subject_id": subject.subject_info.subject_id
                    }),
                    batch_id
                )
        
        # Store results
        processed_subjects_store[batch_id] = processed_subjects
        batch_status_store[batch_id].update({
            'status': 'completed',
            'completed_at': datetime.now(),
            'subjects_processed': len(processed_subjects),
            'errors': errors
        })
        
        # Log batch completion
        audit_logger.log_user_action(
            action_type="batch_processing_complete",
            resource_type="batch",
            resource_id=batch_id,
            new_values={
                'subjects_processed': len(processed_subjects),
                'errors_count': len(errors),
                'success_rate': len(processed_subjects) / len(subjects) if subjects else 0
            }
        )
        
        # Send completion update
        await manager.broadcast_to_batch(
            json.dumps({
                "type": "batch_completed",
                "batch_id": batch_id,
                "subjects_processed": len(processed_subjects),
                "errors_count": len(errors),
                "completion_time": datetime.now().isoformat()
            }),
            batch_id
        )
        
        # Send dashboard update
        await manager.broadcast_dashboard_update(
            json.dumps({
                "type": "dashboard_update",
                "message": f"Batch {batch_id} completed with {len(processed_subjects)} subjects"
            })
        )
        
        logger.info(f"Batch {batch_id} completed: {len(processed_subjects)} subjects processed")
        
    except Exception as e:
        # Handle batch-level errors
        error_response = error_handler.handle_processing_error(
            operation="batch_processing",
            message=f"Batch processing failed for batch {batch_id}",
            exception=e,
            context={'batch_id': batch_id, 'subjects_count': len(subjects)}
        )
        
        batch_status_store[batch_id].update({
            'status': 'failed',
            'completed_at': datetime.now(),
            'error_message': error_response.message,
            'error_id': error_response.error_id
        })
        
        # Log batch failure
        audit_logger.log_user_action(
            action_type="batch_processing_failed",
            resource_type="batch",
            resource_id=batch_id,
            new_values={
                'error_message': error_response.message,
                'error_id': error_response.error_id
            }
        )
        
        # Send failure update
        await manager.broadcast_to_batch(
            json.dumps({
                "type": "batch_failed",
                "batch_id": batch_id,
                "error_message": error_response.message,
                "error_id": error_response.error_id
            }),
            batch_id
        )
        
        # Send failure update
        await manager.broadcast_to_batch(
            json.dumps({
                "type": "batch_failed",
                "batch_id": batch_id,
                "error_message": str(e),
                "failure_time": datetime.now().isoformat()
            }),
            batch_id
        )


# API Endpoints

@router.get('/health')
async def health():
    """Health check endpoint."""
    return {'status': 'ok', 'timestamp': datetime.now().isoformat()}


@router.post('/upload', response_model=FileUploadResponse)
async def upload_mriqc_file(request: Request, file: UploadFile = File(...)):
    """
    Upload MRIQC CSV file for processing with comprehensive security validation.
    
    Args:
        request: FastAPI request object
        file: MRIQC CSV file
        
    Returns:
        FileUploadResponse with file information
    """
    request_id = get_request_id(request)
    client_ip = get_client_ip(request)
    user_agent = get_user_agent(request)
    
    # Perform security checks
    await security_check_middleware(request)
    
    # Log file upload attempt
    audit_logger.log_user_action(
        action_type="file_upload_attempt",
        resource_type="mriqc_file",
        resource_id=file.filename,
        request=request
    )
    
    security_auditor.log_file_upload(
        filename=file.filename or "unknown",
        file_size=file.size or 0,
        client_ip=client_ip,
        success=False  # Will update to True if successful
    )
    
    try:
        # Use secure file handler for validation and storage
        file_path, metadata = await secure_file_handler.validate_and_save_file(file)
        
        # Generate unique file ID from hash
        file_id = metadata['sha256_hash'][:16]  # Use first 16 chars of hash as ID
        
        # Quick validation to get subject count
        try:
            df = mriqc_processor.parse_mriqc_file(file_path)
            validation_errors = mriqc_processor.validate_mriqc_format(df, str(file_path))
            if validation_errors:
                # Clean up uploaded file
                data_retention_manager.force_cleanup_file(file_path)
                error_messages = [error.message for error in validation_errors]
                
                # Create structured error response
                error_response = error_handler.create_error_response(
                    error_type="INVALID_FILE_FORMAT",
                    message=f"Invalid MRIQC file format: {'; '.join(error_messages)}",
                    details={
                        'validation_errors': [error.dict() for error in validation_errors],
                        'file_name': file.filename
                    },
                    request_id=request_id
                )
                raise HTTPException(status_code=400, detail=error_response.message)
                
            subjects_count = len(df)
            
        except HTTPException:
            raise
        except Exception as e:
            # Clean up uploaded file
            data_retention_manager.force_cleanup_file(file_path)
            
            error_response = error_handler.handle_processing_error(
                operation="file_validation",
                message="Failed to validate MRIQC file",
                exception=e,
                context={'file_name': file.filename},
                request_id=request_id
            )
            raise HTTPException(status_code=400, detail=error_response.message)
        
        logger.info(f"File uploaded successfully: {file.filename} ({metadata['file_size']} bytes, {subjects_count} subjects)")
        
        # Log successful upload
        audit_logger.log_user_action(
            action_type="file_upload_success",
            resource_type="mriqc_file",
            resource_id=file_id,
            new_values={
                'filename': file.filename,
                'size': metadata['file_size'],
                'subjects_count': subjects_count,
                'sha256_hash': metadata['sha256_hash'],
                'mime_type': metadata['mime_type']
            },
            request=request
        )
        
        security_auditor.log_file_upload(
            filename=file.filename,
            file_size=metadata['file_size'],
            client_ip=client_ip,
            success=True
        )
        
        return FileUploadResponse(
            message="File uploaded and validated successfully",
            file_id=file_id,
            filename=file.filename,
            size=metadata['file_size'],
            subjects_count=subjects_count
        )
        
    except HTTPException:
        # Log security threat if it's a security-related error
        if "Security validation failed" in str(HTTPException):
            security_auditor.log_threat_detected(
                SecurityThreat(
                    ThreatType.INVALID_FORMAT,
                    SecurityLevel.HIGH,
                    f"File upload security validation failed: {file.filename}"
                ),
                client_ip
            )
        raise
    except Exception as e:
        error_response = error_handler.handle_system_error(
            component="file_upload",
            message="Unexpected error during secure file upload",
            exception=e,
            request_id=request_id
        )
        raise HTTPException(status_code=500, detail=error_response.message)


@router.post('/process', response_model=ProcessFileResponse)
async def process_mriqc_file(
    request: ProcessFileRequest,
    background_tasks: BackgroundTasks
):
    """
    Process uploaded MRIQC file with quality assessment.
    
    Args:
        request: Processing request parameters
        background_tasks: FastAPI background tasks
        
    Returns:
        ProcessFileResponse with batch processing information
    """
    try:
        # Find uploaded file
        temp_dir = Path(tempfile.gettempdir()) / "mriqc_uploads"
        file_pattern = f"{request.file_id}_*"
        
        matching_files = list(temp_dir.glob(file_pattern))
        if not matching_files:
            raise HTTPException(status_code=404, detail="File not found or expired")
        
        temp_file_path = matching_files[0]
        
        # Process the file to extract subjects
        try:
            subjects = mriqc_processor.process_single_file(temp_file_path)
        except (MRIQCProcessingError, MRIQCValidationError) as e:
            raise HTTPException(status_code=400, detail=str(e))
        
        # Generate batch ID for tracking
        batch_id = generate_batch_id()
        
        # Initialize batch status
        batch_status_store[batch_id] = {
            'batch_id': batch_id,
            'status': 'pending',
            'progress': {'completed': 0, 'total': len(subjects), 'progress_percent': 0},
            'subjects_processed': 0,
            'total_subjects': len(subjects),
            'errors': [],
            'created_at': datetime.now()
        }
        
        # Start background processing
        background_tasks.add_task(
            process_subjects_background,
            subjects,
            batch_id,
            request.apply_quality_assessment
        )
        
        # Clean up temp file after a delay
        background_tasks.add_task(
            lambda: asyncio.sleep(3600) or temp_file_path.unlink(missing_ok=True)
        )
        
        logger.info(f"Started processing batch {batch_id} with {len(subjects)} subjects")
        
        return ProcessFileResponse(
            batch_id=batch_id,
            message=f"Processing started for {len(subjects)} subjects",
            subjects_processed=0,
            processing_errors=[],
            estimated_completion_time=None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@router.get('/batch/{batch_id}/status', response_model=BatchStatusResponse)
async def get_batch_status(batch_id: str):
    """
    Get batch processing status.
    
    Args:
        batch_id: Batch processing ID
        
    Returns:
        BatchStatusResponse with current status
    """
    if batch_id not in batch_status_store:
        raise HTTPException(status_code=404, detail="Batch not found")
    
    status_data = batch_status_store[batch_id]
    
    return BatchStatusResponse(
        batch_id=batch_id,
        status=status_data['status'],
        progress=status_data['progress'],
        subjects_processed=status_data.get('subjects_processed', 0),
        total_subjects=status_data['total_subjects'],
        errors=status_data.get('errors', []),
        started_at=status_data.get('started_at', status_data['created_at']),
        completed_at=status_data.get('completed_at')
    )


@router.get('/subjects', response_model=SubjectListResponse)
async def get_subjects(
    batch_id: Optional[str] = Query(None, description="Filter by batch ID"),
    quality_status: Optional[QualityStatus] = Query(None, description="Filter by quality status"),
    age_group: Optional[AgeGroup] = Query(None, description="Filter by age group"),
    scan_type: Optional[str] = Query(None, description="Filter by scan type"),
    sort_by: Optional[str] = Query(None, description="Field to sort by"),
    sort_order: Optional[str] = Query("asc", pattern="^(asc|desc)$", description="Sort order"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Page size")
):
    """
    Get list of processed subjects with filtering, sorting, and pagination.
    
    Args:
        batch_id: Optional batch ID filter
        quality_status: Optional quality status filter
        age_group: Optional age group filter
        scan_type: Optional scan type filter
        sort_by: Optional field to sort by
        sort_order: Sort order (asc/desc)
        page: Page number (1-based)
        page_size: Number of subjects per page
        
    Returns:
        SubjectListResponse with filtered and sorted subjects
    """
    try:
        # Get all subjects from specified batch or all batches
        all_subjects = []
        
        if batch_id:
            if batch_id not in processed_subjects_store:
                raise HTTPException(status_code=404, detail="Batch not found")
            all_subjects = processed_subjects_store[batch_id]
        else:
            # Combine subjects from all batches
            for subjects in processed_subjects_store.values():
                all_subjects.extend(subjects)
        
        # Apply filters
        filtered_subjects = all_subjects
        filters_applied = {}
        
        if quality_status:
            filtered_subjects = [s for s in filtered_subjects 
                               if s.quality_assessment.overall_status == quality_status]
            filters_applied['quality_status'] = quality_status.value
        
        if age_group:
            filtered_subjects = [s for s in filtered_subjects 
                               if (s.normalized_metrics and 
                                   s.normalized_metrics.age_group == age_group)]
            filters_applied['age_group'] = age_group.value
        
        if scan_type:
            filtered_subjects = [s for s in filtered_subjects 
                               if s.subject_info.scan_type.value == scan_type]
            filters_applied['scan_type'] = scan_type
        
        if batch_id:
            filters_applied['batch_id'] = batch_id
        
        # Apply sorting
        sort_applied = None
        if sort_by:
            try:
                reverse_order = sort_order == "desc"
                
                def get_sort_value(subject):
                    # Handle different sort fields
                    if sort_by == "subject_id":
                        return subject.subject_info.subject_id
                    elif sort_by == "age":
                        return subject.subject_info.age or 0
                    elif sort_by == "quality_status":
                        return subject.quality_assessment.overall_status.value
                    elif sort_by == "composite_score":
                        return subject.quality_assessment.composite_score
                    elif sort_by == "processing_timestamp":
                        return subject.processing_timestamp
                    elif sort_by == "scan_type":
                        return subject.subject_info.scan_type.value
                    elif hasattr(subject.raw_metrics, sort_by):
                        # Sort by metric value
                        return getattr(subject.raw_metrics, sort_by) or 0
                    else:
                        return 0
                
                filtered_subjects.sort(key=get_sort_value, reverse=reverse_order)
                sort_applied = {"sort_by": sort_by, "sort_order": sort_order}
                
            except Exception as e:
                logger.warning(f"Failed to sort by {sort_by}: {str(e)}")
        
        # Apply pagination
        total_count = len(filtered_subjects)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_subjects = filtered_subjects[start_idx:end_idx]
        
        return SubjectListResponse(
            subjects=paginated_subjects,
            total_count=total_count,
            page=page,
            page_size=page_size,
            filters_applied=filters_applied,
            sort_applied=sort_applied
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get subjects: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get subjects: {str(e)}")


class AdvancedFilterRequest(BaseModel):
    """Combined request model for advanced filtering."""
    filter_criteria: SubjectFilterRequest
    sort_criteria: Optional[SubjectSortRequest] = None


@router.post('/subjects/filter', response_model=SubjectListResponse)
async def filter_subjects_advanced(
    request: AdvancedFilterRequest,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Page size")
):
    """
    Advanced subject filtering with complex criteria.
    
    Args:
        request: Combined filtering and sorting criteria
        page: Page number (1-based)
        page_size: Number of subjects per page
        
    Returns:
        SubjectListResponse with filtered subjects
    """
    try:
        filter_request = request.filter_criteria
        sort_request = request.sort_criteria
        
        # Get all subjects
        all_subjects = []
        if filter_request.batch_ids:
            for batch_id in filter_request.batch_ids:
                if batch_id in processed_subjects_store:
                    all_subjects.extend(processed_subjects_store[batch_id])
        else:
            for subjects in processed_subjects_store.values():
                all_subjects.extend(subjects)
        
        filtered_subjects = all_subjects
        filters_applied = {}
        
        # Apply quality status filter
        if filter_request.quality_status:
            filtered_subjects = [s for s in filtered_subjects 
                               if s.quality_assessment.overall_status in filter_request.quality_status]
            filters_applied['quality_status'] = [status.value for status in filter_request.quality_status]
        
        # Apply age group filter
        if filter_request.age_group:
            filtered_subjects = [s for s in filtered_subjects 
                               if (s.normalized_metrics and 
                                   s.normalized_metrics.age_group in filter_request.age_group)]
            filters_applied['age_group'] = [group.value for group in filter_request.age_group]
        
        # Apply scan type filter
        if filter_request.scan_type:
            filtered_subjects = [s for s in filtered_subjects 
                               if s.subject_info.scan_type.value in filter_request.scan_type]
            filters_applied['scan_type'] = filter_request.scan_type
        
        # Apply age range filter
        if filter_request.age_range:
            min_age = filter_request.age_range.get('min', 0)
            max_age = filter_request.age_range.get('max', 120)
            filtered_subjects = [s for s in filtered_subjects 
                               if s.subject_info.age is not None and 
                               min_age <= s.subject_info.age <= max_age]
            filters_applied['age_range'] = filter_request.age_range
        
        # Apply metric filters
        if filter_request.metric_filters:
            for metric_name, metric_range in filter_request.metric_filters.items():
                min_val = metric_range.get('min')
                max_val = metric_range.get('max')
                
                def metric_filter(subject):
                    value = getattr(subject.raw_metrics, metric_name, None)
                    if value is None:
                        return False
                    if min_val is not None and value < min_val:
                        return False
                    if max_val is not None and value > max_val:
                        return False
                    return True
                
                filtered_subjects = [s for s in filtered_subjects if metric_filter(s)]
            
            filters_applied['metric_filters'] = filter_request.metric_filters
        
        # Apply date range filter
        if filter_request.date_range:
            from datetime import datetime as dt
            start_date = dt.fromisoformat(filter_request.date_range['start']) if filter_request.date_range.get('start') else None
            end_date = dt.fromisoformat(filter_request.date_range['end']) if filter_request.date_range.get('end') else None
            
            def date_filter(subject):
                proc_date = subject.processing_timestamp
                if start_date and proc_date < start_date:
                    return False
                if end_date and proc_date > end_date:
                    return False
                return True
            
            filtered_subjects = [s for s in filtered_subjects if date_filter(s)]
            filters_applied['date_range'] = filter_request.date_range
        
        # Apply text search
        if filter_request.search_text:
            search_text = filter_request.search_text.lower()
            
            def text_search(subject):
                # Search in subject ID, session, site, scanner
                searchable_fields = [
                    subject.subject_info.subject_id,
                    subject.subject_info.session or "",
                    subject.subject_info.site or "",
                    subject.subject_info.scanner or "",
                    subject.subject_info.scan_type.value
                ]
                return any(search_text in field.lower() for field in searchable_fields if field)
            
            filtered_subjects = [s for s in filtered_subjects if text_search(s)]
            filters_applied['search_text'] = filter_request.search_texter_request.search_text
        
        # Apply sorting
        sort_applied = None
        if sort_request:
            try:
                reverse_order = sort_request.sort_order == "desc"
                
                def get_sort_value(subject):
                    if sort_request.sort_by == "subject_id":
                        return subject.subject_info.subject_id
                    elif sort_request.sort_by == "age":
                        return subject.subject_info.age or 0
                    elif sort_request.sort_by == "quality_status":
                        return subject.quality_assessment.overall_status.value
                    elif sort_request.sort_by == "composite_score":
                        return subject.quality_assessment.composite_score
                    elif sort_request.sort_by == "processing_timestamp":
                        return subject.processing_timestamp
                    elif hasattr(subject.raw_metrics, sort_request.sort_by):
                        return getattr(subject.raw_metrics, sort_request.sort_by) or 0
                    else:
                        return 0
                
                filtered_subjects.sort(key=get_sort_value, reverse=reverse_order)
                sort_applied = {"sort_by": sort_request.sort_by, "sort_order": sort_request.sort_order}
                
            except Exception as e:
                logger.warning(f"Failed to sort by {sort_request.sort_by}: {str(e)}")
        
        # Apply pagination
        total_count = len(filtered_subjects)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_subjects = filtered_subjects[start_idx:end_idx]
        
        return SubjectListResponse(
            subjects=paginated_subjects,
            total_count=total_count,
            page=page,
            page_size=page_size,
            filters_applied=filters_applied,
            sort_applied=sort_applied
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to filter subjects: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to filter subjects: {str(e)}")


@router.get('/subjects/{subject_id}', response_model=SubjectDetailResponse)
async def get_subject_detail(
    subject_id: str,
    batch_id: Optional[str] = Query(None, description="Batch ID to search in")
):
    """
    Get detailed information for a specific subject.
    
    Args:
        subject_id: Subject identifier
        batch_id: Optional batch ID to limit search
        
    Returns:
        SubjectDetailResponse with detailed subject information
    """
    try:
        # Search for subject
        found_subject = None
        
        if batch_id:
            if batch_id not in processed_subjects_store:
                raise HTTPException(status_code=404, detail="Batch not found")
            subjects = processed_subjects_store[batch_id]
        else:
            # Search all batches
            subjects = []
            for batch_subjects in processed_subjects_store.values():
                subjects.extend(batch_subjects)
        
        for subject in subjects:
            if subject.subject_info.subject_id == subject_id:
                found_subject = subject
                break
        
        if not found_subject:
            raise HTTPException(status_code=404, detail="Subject not found")
        
        # Get age group statistics if available
        age_group_statistics = None
        if found_subject.normalized_metrics:
            age_group_statistics = age_normalizer.get_age_group_statistics(
                found_subject.normalized_metrics.age_group
            )
        
        # Generate additional recommendations
        recommendations = found_subject.quality_assessment.recommendations.copy()
        if found_subject.normalized_metrics:
            age_recommendations = age_normalizer.get_metric_recommendations(
                found_subject.normalized_metrics
            )
            recommendations.extend(age_recommendations)
        
        return SubjectDetailResponse(
            subject=found_subject,
            age_group_statistics=age_group_statistics,
            recommendations=recommendations
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get subject detail: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get subject detail: {str(e)}")


@router.get('/dashboard/summary', response_model=DashboardSummaryResponse)
async def get_dashboard_summary(
    batch_id: Optional[str] = Query(None, description="Filter by batch ID")
):
    """
    Get enhanced dashboard summary statistics.
    
    Args:
        batch_id: Optional batch ID filter
        
    Returns:
        Enhanced dashboard summary with quality statistics, alerts, and recent activity
    """
    try:
        # Get subjects
        if batch_id:
            if batch_id not in processed_subjects_store:
                raise HTTPException(status_code=404, detail="Batch not found")
            subjects = processed_subjects_store[batch_id]
        else:
            subjects = []
            for batch_subjects in processed_subjects_store.values():
                subjects.extend(batch_subjects)
        
        if not subjects:
            return DashboardSummaryResponse(
                total_subjects=0,
                quality_distribution={},
                age_group_distribution={},
                scan_type_distribution={},
                metric_statistics={},
                exclusion_rate=0.0,
                processing_date=datetime.now().isoformat(),
                batch_id=batch_id,
                recent_activity=[],
                alerts=[]
            )
        
        # Calculate quality distribution
        quality_counts = {}
        for status in QualityStatus:
            quality_counts[status.value] = sum(
                1 for s in subjects if s.quality_assessment.overall_status == status
            )
        
        # Calculate age group distribution
        age_group_counts = {}
        for group in AgeGroup:
            age_group_counts[group.value] = sum(
                1 for s in subjects 
                if s.normalized_metrics and s.normalized_metrics.age_group == group
            )
        
        # Calculate scan type distribution
        scan_type_counts = {}
        for subject in subjects:
            scan_type = subject.subject_info.scan_type.value
            scan_type_counts[scan_type] = scan_type_counts.get(scan_type, 0) + 1
        
        # Calculate exclusion rate
        failed_count = quality_counts.get(QualityStatus.FAIL.value, 0)
        exclusion_rate = failed_count / len(subjects) if subjects else 0.0
        
        # Calculate metric statistics
        metric_stats = {}
        if subjects:
            # Get all available metrics
            all_metrics = set()
            for subject in subjects:
                for metric_name, value in subject.raw_metrics.model_dump().items():
                    if value is not None:
                        all_metrics.add(metric_name)
            
            # Calculate statistics for each metric
            for metric_name in all_metrics:
                values = []
                for subject in subjects:
                    value = getattr(subject.raw_metrics, metric_name, None)
                    if value is not None:
                        values.append(value)
                
                if values:
                    import statistics
                    metric_stats[metric_name] = {
                        'mean': statistics.mean(values),
                        'median': statistics.median(values),
                        'std': statistics.stdev(values) if len(values) > 1 else 0.0,
                        'min': min(values),
                        'max': max(values),
                        'count': len(values)
                    }
        
        # Generate recent activity (last 10 processing events)
        recent_activity = []
        for batch_id_key, batch_info in sorted(
            batch_status_store.items(), 
            key=lambda x: x[1].get('completed_at', x[1].get('created_at', datetime.min)),
            reverse=True
        )[:10]:
            activity = {
                'batch_id': batch_id_key,
                'status': batch_info.get('status', 'unknown'),
                'subjects_count': batch_info.get('subjects_processed', batch_info.get('total_subjects', 0)),
                'timestamp': (batch_info.get('completed_at') or batch_info.get('created_at', datetime.now())).isoformat(),
                'errors_count': len(batch_info.get('errors', []))
            }
            recent_activity.append(activity)
        
        # Generate alerts based on quality issues
        alerts = []
        
        # High exclusion rate alert
        if exclusion_rate > 0.2:  # More than 20% exclusion
            alerts.append({
                'type': 'warning',
                'message': f'High exclusion rate: {exclusion_rate:.1%} of subjects failed quality control',
                'severity': 'high' if exclusion_rate > 0.3 else 'medium'
            })
        
        # Low sample size alert
        if len(subjects) < 10:
            alerts.append({
                'type': 'info',
                'message': f'Small sample size: Only {len(subjects)} subjects processed',
                'severity': 'low'
            })
        
        # Age group imbalance alert
        if age_group_counts:
            max_group_count = max(age_group_counts.values())
            min_group_count = min(v for v in age_group_counts.values() if v > 0)
            if max_group_count > 0 and min_group_count > 0 and max_group_count / min_group_count > 5:
                alerts.append({
                    'type': 'warning',
                    'message': 'Significant age group imbalance detected',
                    'severity': 'medium'
                })
        
        # Recent processing errors
        recent_errors = sum(len(batch_info.get('errors', [])) for batch_info in batch_status_store.values())
        if recent_errors > 0:
            alerts.append({
                'type': 'error',
                'message': f'{recent_errors} processing errors detected across recent batches',
                'severity': 'high' if recent_errors > 10 else 'medium'
            })
        
        return DashboardSummaryResponse(
            total_subjects=len(subjects),
            quality_distribution=quality_counts,
            age_group_distribution=age_group_counts,
            scan_type_distribution=scan_type_counts,
            metric_statistics=metric_stats,
            exclusion_rate=exclusion_rate,
            processing_date=datetime.now().isoformat(),
            batch_id=batch_id,
            recent_activity=recent_activity,
            alerts=alerts
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get dashboard summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get dashboard summary: {str(e)}")


# WebSocket endpoints for real-time updates

@router.websocket("/ws/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    """
    WebSocket endpoint for real-time dashboard updates.
    
    Provides real-time updates for:
    - Processing status changes
    - New batch completions
    - Quality assessment alerts
    """
    await manager.connect(websocket)
    try:
        # Send initial connection confirmation
        await websocket.send_text(json.dumps({
            "type": "connection_established",
            "message": "Connected to dashboard updates",
            "timestamp": datetime.now().isoformat()
        }))
        
        # Keep connection alive and handle incoming messages
        while True:
            try:
                # Wait for messages from client (ping/pong, subscriptions, etc.)
                data = await websocket.receive_text()
                message = json.loads(data)
                
                if message.get("type") == "ping":
                    await websocket.send_text(json.dumps({
                        "type": "pong",
                        "timestamp": datetime.now().isoformat()
                    }))
                elif message.get("type") == "subscribe_batch":
                    batch_id = message.get("batch_id")
                    if batch_id:
                        if batch_id not in manager.batch_subscribers:
                            manager.batch_subscribers[batch_id] = []
                        if websocket not in manager.batch_subscribers[batch_id]:
                            manager.batch_subscribers[batch_id].append(websocket)
                        
                        await websocket.send_text(json.dumps({
                            "type": "subscription_confirmed",
                            "batch_id": batch_id,
                            "message": f"Subscribed to batch {batch_id} updates"
                        }))
                
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"WebSocket error: {str(e)}")
                break
                
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)


@router.websocket("/ws/batch/{batch_id}")
async def websocket_batch_status(websocket: WebSocket, batch_id: str):
    """
    WebSocket endpoint for real-time batch processing updates.
    
    Args:
        batch_id: Batch ID to monitor
        
    Provides real-time updates for:
    - Processing progress
    - Individual subject completion
    - Error notifications
    - Batch completion
    """
    await manager.connect(websocket, batch_id)
    try:
        # Send initial batch status if available
        if batch_id in batch_status_store:
            status_data = batch_status_store[batch_id]
            await websocket.send_text(json.dumps({
                "type": "initial_status",
                "batch_id": batch_id,
                "status": status_data.get('status', 'unknown'),
                "progress": status_data.get('progress', {}),
                "subjects_processed": status_data.get('subjects_processed', 0),
                "total_subjects": status_data.get('total_subjects', 0),
                "errors": status_data.get('errors', [])
            }))
        else:
            await websocket.send_text(json.dumps({
                "type": "batch_not_found",
                "batch_id": batch_id,
                "message": "Batch not found or not yet started"
            }))
        
        # Keep connection alive
        while True:
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                
                if message.get("type") == "ping":
                    await websocket.send_text(json.dumps({
                        "type": "pong",
                        "batch_id": batch_id,
                        "timestamp": datetime.now().isoformat()
                    }))
                    
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"WebSocket batch error: {str(e)}")
                break
                
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket, batch_id)


# Additional dashboard endpoints

@router.get('/dashboard/metrics/summary')
async def get_metrics_summary(
    batch_id: Optional[str] = Query(None, description="Filter by batch ID"),
    metric_names: Optional[List[str]] = Query(None, description="Specific metrics to include")
):
    """
    Get detailed metrics summary for dashboard visualizations.
    
    Args:
        batch_id: Optional batch ID filter
        metric_names: Optional list of specific metrics to include
        
    Returns:
        Detailed metrics summary with distributions and outliers
    """
    try:
        # Get subjects
        if batch_id:
            if batch_id not in processed_subjects_store:
                raise HTTPException(status_code=404, detail="Batch not found")
            subjects = processed_subjects_store[batch_id]
        else:
            subjects = []
            for batch_subjects in processed_subjects_store.values():
                subjects.extend(batch_subjects)
        
        if not subjects:
            return {"metrics": {}, "total_subjects": 0}
        
        # Get available metrics
        all_metrics = set()
        for subject in subjects:
            for metric_name, value in subject.raw_metrics.model_dump().items():
                if value is not None:
                    all_metrics.add(metric_name)
        
        # Filter metrics if specified
        if metric_names:
            all_metrics = all_metrics.intersection(set(metric_names))
        
        metrics_summary = {}
        
        for metric_name in all_metrics:
            values = []
            quality_breakdown = {"pass": [], "warning": [], "fail": [], "uncertain": []}
            
            for subject in subjects:
                value = getattr(subject.raw_metrics, metric_name, None)
                if value is not None:
                    values.append(value)
                    status = subject.quality_assessment.overall_status.value
                    if status in quality_breakdown:
                        quality_breakdown[status].append(value)
            
            if values:
                import statistics
                import numpy as np
                
                # Calculate basic statistics
                mean_val = statistics.mean(values)
                median_val = statistics.median(values)
                std_val = statistics.stdev(values) if len(values) > 1 else 0.0
                
                # Calculate percentiles
                percentiles = {
                    '5th': np.percentile(values, 5),
                    '25th': np.percentile(values, 25),
                    '75th': np.percentile(values, 75),
                    '95th': np.percentile(values, 95)
                }
                
                # Identify outliers (values beyond 1.5 * IQR)
                q1, q3 = percentiles['25th'], percentiles['75th']
                iqr = q3 - q1
                outlier_threshold_low = q1 - 1.5 * iqr
                outlier_threshold_high = q3 + 1.5 * iqr
                outliers = [v for v in values if v < outlier_threshold_low or v > outlier_threshold_high]
                
                metrics_summary[metric_name] = {
                    'basic_stats': {
                        'mean': mean_val,
                        'median': median_val,
                        'std': std_val,
                        'min': min(values),
                        'max': max(values),
                        'count': len(values)
                    },
                    'percentiles': percentiles,
                    'outliers': {
                        'count': len(outliers),
                        'values': outliers[:10],  # Limit to first 10 outliers
                        'threshold_low': outlier_threshold_low,
                        'threshold_high': outlier_threshold_high
                    },
                    'quality_breakdown': {
                        status: {
                            'count': len(vals),
                            'mean': statistics.mean(vals) if vals else 0,
                            'std': statistics.stdev(vals) if len(vals) > 1 else 0
                        }
                        for status, vals in quality_breakdown.items()
                    }
                }
        
        return {
            'metrics': metrics_summary,
            'total_subjects': len(subjects),
            'batch_id': batch_id,
            'generated_at': datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get metrics summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get metrics summary: {str(e)}")


# Export endpoints
from fastapi.responses import StreamingResponse
from .export_engine import ExportEngine, ExportError

# Initialize export engine
export_engine = ExportEngine()


class ExportRequest(BaseModel):
    """Request model for data export."""
    batch_ids: Optional[List[str]] = Field(None, description="Specific batch IDs to export")
    quality_status_filter: Optional[List[QualityStatus]] = Field(None, description="Filter by quality status")
    age_group_filter: Optional[List[AgeGroup]] = Field(None, description="Filter by age group")
    include_raw_metrics: bool = Field(True, description="Include raw MRIQC metrics")
    include_normalized_metrics: bool = Field(True, description="Include normalized metrics")
    include_quality_assessment: bool = Field(True, description="Include quality assessment")
    study_name: Optional[str] = Field(None, description="Study name for report")


@router.post('/export/csv')
async def export_subjects_csv(request: ExportRequest):
    """
    Export subjects data to CSV format.
    
    Args:
        request: Export configuration
        
    Returns:
        CSV file as streaming response
    """
    try:
        # Get subjects based on filters
        subjects = []
        
        if request.batch_ids:
            for batch_id in request.batch_ids:
                if batch_id in processed_subjects_store:
                    subjects.extend(processed_subjects_store[batch_id])
        else:
            # Get all subjects
            for batch_subjects in processed_subjects_store.values():
                subjects.extend(batch_subjects)
        
        if not subjects:
            raise HTTPException(status_code=404, detail="No subjects found for export")
        
        # Apply filters
        filtered_subjects = subjects
        
        if request.quality_status_filter:
            filtered_subjects = [
                s for s in filtered_subjects 
                if s.quality_assessment.overall_status in request.quality_status_filter
            ]
        
        if request.age_group_filter:
            filtered_subjects = [
                s for s in filtered_subjects 
                if (s.normalized_metrics and 
                    s.normalized_metrics.age_group in request.age_group_filter)
            ]
        
        if not filtered_subjects:
            raise HTTPException(status_code=404, detail="No subjects match the specified filters")
        
        # Generate CSV
        csv_content = export_engine.export_subjects_csv(
            filtered_subjects,
            include_raw_metrics=request.include_raw_metrics,
            include_normalized_metrics=request.include_normalized_metrics,
            include_quality_assessment=request.include_quality_assessment
        )
        
        # Create filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        study_prefix = f"{request.study_name}_" if request.study_name else ""
        filename = f"{study_prefix}mriqc_export_{timestamp}.csv"
        
        # Return as streaming response
        return StreamingResponse(
            io.StringIO(csv_content),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except ExportError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"CSV export failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"CSV export failed: {str(e)}")


@router.post('/export/pdf')
async def export_subjects_pdf(request: ExportRequest):
    """
    Export subjects data to PDF report.
    
    Args:
        request: Export configuration
        
    Returns:
        PDF file as streaming response
    """
    try:
        # Get subjects based on filters
        subjects = []
        
        if request.batch_ids:
            for batch_id in request.batch_ids:
                if batch_id in processed_subjects_store:
                    subjects.extend(processed_subjects_store[batch_id])
        else:
            # Get all subjects
            for batch_subjects in processed_subjects_store.values():
                subjects.extend(batch_subjects)
        
        if not subjects:
            raise HTTPException(status_code=404, detail="No subjects found for export")
        
        # Apply filters
        filtered_subjects = subjects
        
        if request.quality_status_filter:
            filtered_subjects = [
                s for s in filtered_subjects 
                if s.quality_assessment.overall_status in request.quality_status_filter
            ]
        
        if request.age_group_filter:
            filtered_subjects = [
                s for s in filtered_subjects 
                if (s.normalized_metrics and 
                    s.normalized_metrics.age_group in request.age_group_filter)
            ]
        
        if not filtered_subjects:
            raise HTTPException(status_code=404, detail="No subjects match the specified filters")
        
        # Generate PDF
        pdf_content = export_engine.generate_pdf_report(
            filtered_subjects,
            study_name=request.study_name,
            include_individual_subjects=True,
            include_summary_charts=True
        )
        
        # Create filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        study_prefix = f"{request.study_name}_" if request.study_name else ""
        filename = f"{study_prefix}mriqc_report_{timestamp}.pdf"
        
        # Return as streaming response
        return StreamingResponse(
            io.BytesIO(pdf_content),
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except ExportError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"PDF export failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"PDF export failed: {str(e)}")


@router.get('/export/study-summary/{batch_id}')
async def export_study_summary(
    batch_id: str,
    format: str = Query("json", pattern="^(json|csv)$", description="Export format")
):
    """
    Export study-level summary for a specific batch.
    
    Args:
        batch_id: Batch ID to generate summary for
        format: Export format (json or csv)
        
    Returns:
        Study summary in requested format
    """
    try:
        if batch_id not in processed_subjects_store:
            raise HTTPException(status_code=404, detail="Batch not found")
        
        subjects = processed_subjects_store[batch_id]
        if not subjects:
            raise HTTPException(status_code=404, detail="No subjects found in batch")
        
        # Generate study summary
        study_summary = export_engine.generate_study_summary(subjects)
        
        if format == "json":
            return study_summary
        elif format == "csv":
            csv_content = export_engine.export_study_summary_csv(study_summary)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"study_summary_{batch_id}_{timestamp}.csv"
            
            return StreamingResponse(
                io.StringIO(csv_content),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
    except ExportError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Study summary export failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Study summary export failed: {str(e)}")


@router.get('/export/batch-list')
async def get_available_batches():
    """
    Get list of available batches for export.
    
    Returns:
        List of batch information
    """
    try:
        batch_info = []
        
        for batch_id, subjects in processed_subjects_store.items():
            if subjects:
                status_info = batch_status_store.get(batch_id, {})
                
                # Calculate quality distribution
                quality_counts = {}
                for subject in subjects:
                    status = subject.quality_assessment.overall_status.value
                    quality_counts[status] = quality_counts.get(status, 0) + 1
                
                batch_info.append({
                    'batch_id': batch_id,
                    'subject_count': len(subjects),
                    'status': status_info.get('status', 'unknown'),
                    'created_at': status_info.get('created_at', datetime.now()).isoformat(),
                    'completed_at': status_info.get('completed_at', {}).isoformat() if status_info.get('completed_at') else None,
                    'quality_distribution': quality_counts,
                    'scan_types': list(set(s.subject_info.scan_type.value for s in subjects)),
                    'age_range': {
                        'min': min((s.subject_info.age for s in subjects if s.subject_info.age), default=None),
                        'max': max((s.subject_info.age for s in subjects if s.subject_info.age), default=None)
                    }
                })
        
        return {
            'batches': sorted(batch_info, key=lambda x: x['created_at'], reverse=True),
            'total_batches': len(batch_info),
            'total_subjects': sum(info['subject_count'] for info in batch_info)
        }
        
    except Exception as e:
        logger.error(f"Failed to get batch list: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get batch list: {str(e)}")


# Cleanup endpoint for testing
@router.delete('/batch/{batch_id}')
async def delete_batch(batch_id: str):
    """
    Delete batch data (for testing purposes).
    
    Args:
        batch_id: Batch ID to delete
        
    Returns:
        Confirmation message
    """
    if batch_id not in batch_status_store:
        raise HTTPException(status_code=404, detail="Batch not found")
    
    # Clean up batch data
    del batch_status_store[batch_id]
    if batch_id in processed_subjects_store:
        del processed_subjects_store[batch_id]
    
    return {"message": f"Batch {batch_id} deleted successfully"}


# Configuration Management Endpoints

@router.post('/configurations', response_model=ConfigurationResponse)
async def create_study_configuration(request: CreateConfigurationRequest):
    """
    Create a new study configuration.
    
    Args:
        request: Configuration creation request
        
    Returns:
        Created configuration details
    """
    try:
        # Convert request to StudyConfiguration model
        config = StudyConfiguration(
            study_name=request.study_name,
            normative_dataset=request.normative_dataset,
            custom_age_groups=request.custom_age_groups,
            custom_thresholds=request.custom_thresholds,
            exclusion_criteria=request.exclusion_criteria,
            created_by=request.created_by
        )
        
        # Create configuration
        success, errors = config_service.create_study_configuration(config)
        
        if not success:
            raise HTTPException(status_code=400, detail={"errors": errors})
        
        # Return created configuration
        created_config = config_service.get_study_configuration(request.study_name)
        if not created_config:
            raise HTTPException(status_code=500, detail="Failed to retrieve created configuration")
        
        return ConfigurationResponse(
            study_name=created_config['study_name'],
            normative_dataset=created_config['normative_dataset'],
            custom_age_groups=created_config['custom_age_groups'],
            custom_thresholds=created_config['custom_thresholds'],
            exclusion_criteria=created_config['exclusion_criteria'],
            created_by=created_config['created_by'],
            created_at=created_config['created_at'],
            updated_at=created_config['updated_at']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create configuration: {str(e)}")


@router.get('/configurations', response_model=ConfigurationListResponse)
async def get_study_configurations():
    """
    Get all study configurations.
    
    Returns:
        List of configuration summaries
    """
    try:
        configs = config_service.get_all_study_configurations()
        
        summaries = []
        for config in configs:
            summary = config_service.get_configuration_summary(config['study_name'])
            if summary:
                summaries.append(ConfigurationSummaryResponse(**summary))
        
        return ConfigurationListResponse(
            configurations=summaries,
            total_count=len(summaries)
        )
        
    except Exception as e:
        logger.error(f"Failed to get configurations: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get configurations: {str(e)}")


@router.get('/configurations/{study_name}', response_model=ConfigurationResponse)
async def get_study_configuration(study_name: str):
    """
    Get a specific study configuration.
    
    Args:
        study_name: Name of the study configuration
        
    Returns:
        Configuration details
    """
    try:
        config = config_service.get_study_configuration(study_name)
        if not config:
            raise HTTPException(status_code=404, detail=f"Configuration '{study_name}' not found")
        
        return ConfigurationResponse(
            study_name=config['study_name'],
            normative_dataset=config['normative_dataset'],
            custom_age_groups=config['custom_age_groups'],
            custom_thresholds=config['custom_thresholds'],
            exclusion_criteria=config['exclusion_criteria'],
            created_by=config['created_by'],
            created_at=config['created_at'],
            updated_at=config['updated_at']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get configuration {study_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get configuration: {str(e)}")


@router.put('/configurations/{study_name}', response_model=ConfigurationResponse)
async def update_study_configuration(study_name: str, request: UpdateConfigurationRequest):
    """
    Update an existing study configuration.
    
    Args:
        study_name: Name of the study configuration
        request: Configuration update request
        
    Returns:
        Updated configuration details
    """
    try:
        # Validate update request
        updates = {}
        if request.normative_dataset is not None:
            updates['normative_dataset'] = request.normative_dataset
        if request.exclusion_criteria is not None:
            updates['exclusion_criteria'] = request.exclusion_criteria
        
        validation_errors = config_service.validate_configuration_update(study_name, updates)
        if validation_errors:
            raise HTTPException(status_code=400, detail={"errors": validation_errors})
        
        # Update configuration
        success = config_service.update_study_configuration(
            study_name=study_name,
            normative_dataset=request.normative_dataset,
            exclusion_criteria=request.exclusion_criteria
        )
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Configuration '{study_name}' not found")
        
        # Return updated configuration
        updated_config = config_service.get_study_configuration(study_name)
        if not updated_config:
            raise HTTPException(status_code=500, detail="Failed to retrieve updated configuration")
        
        return ConfigurationResponse(
            study_name=updated_config['study_name'],
            normative_dataset=updated_config['normative_dataset'],
            custom_age_groups=updated_config['custom_age_groups'],
            custom_thresholds=updated_config['custom_thresholds'],
            exclusion_criteria=updated_config['exclusion_criteria'],
            created_by=updated_config['created_by'],
            created_at=updated_config['created_at'],
            updated_at=updated_config['updated_at']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update configuration {study_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update configuration: {str(e)}")


@router.delete('/configurations/{study_name}')
async def delete_study_configuration(study_name: str):
    """
    Delete a study configuration.
    
    Args:
        study_name: Name of the study configuration
        
    Returns:
        Confirmation message
    """
    try:
        success = config_service.delete_study_configuration(study_name)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Configuration '{study_name}' not found")
        
        return {"message": f"Configuration '{study_name}' deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete configuration {study_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete configuration: {str(e)}")


@router.get('/configurations/{study_name}/age-groups')
async def get_study_age_groups(study_name: str):
    """
    Get effective age groups for a study (custom or default).
    
    Args:
        study_name: Name of the study configuration
        
    Returns:
        List of age groups
    """
    try:
        config = config_service.get_study_configuration(study_name)
        if not config:
            raise HTTPException(status_code=404, detail=f"Configuration '{study_name}' not found")
        
        age_groups = config_service.db.get_effective_age_groups_for_study(study_name)
        
        return {
            'study_name': study_name,
            'age_groups': age_groups,
            'is_custom': len(config['custom_age_groups']) > 0
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get age groups for {study_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get age groups: {str(e)}")


@router.get('/configurations/{study_name}/thresholds/{metric_name}')
async def get_study_metric_thresholds(study_name: str, metric_name: str):
    """
    Get quality thresholds for a specific metric in a study.
    
    Args:
        study_name: Name of the study configuration
        metric_name: Name of the quality metric
        
    Returns:
        Thresholds for each age group
    """
    try:
        config = config_service.get_study_configuration(study_name)
        if not config:
            raise HTTPException(status_code=404, detail=f"Configuration '{study_name}' not found")
        
        # Get age groups for the study
        age_groups = config_service.db.get_effective_age_groups_for_study(study_name)
        
        # Get thresholds for each age group
        thresholds = {}
        for age_group in age_groups:
            threshold = config_service.get_quality_thresholds_for_study(
                study_name, metric_name, age_group['name']
            )
            if threshold:
                thresholds[age_group['name']] = threshold
        
        return {
            'study_name': study_name,
            'metric_name': metric_name,
            'thresholds': thresholds,
            'has_custom_thresholds': any(
                t['metric_name'] == metric_name for t in config['custom_thresholds']
            )
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get thresholds for {metric_name} in {study_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get thresholds: {str(e)}")


@router.post('/configurations/{study_name}/validate')
async def validate_study_configuration(study_name: str, config_data: CreateConfigurationRequest):
    """
    Validate a study configuration without creating it.
    
    Args:
        study_name: Name for the study configuration
        config_data: Configuration data to validate
        
    Returns:
        Validation results
    """
    try:
        # Create temporary configuration for validation
        temp_config = StudyConfiguration(
            study_name=study_name,
            normative_dataset=config_data.normative_dataset,
            custom_age_groups=config_data.custom_age_groups,
            custom_thresholds=config_data.custom_thresholds,
            exclusion_criteria=config_data.exclusion_criteria,
            created_by=config_data.created_by
        )
        
        # Validate configuration
        validation_errors = config_service.validate_study_configuration(temp_config)
        
        return {
            'is_valid': len(validation_errors) == 0,
            'errors': validation_errors,
            'warnings': [],  # Could add warnings for non-blocking issues
            'validation_timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to validate configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to validate configuration: {str(e)}")

# Batch Processing Endpoints

class BatchProcessingRequest(BaseModel):
    """Request model for batch processing."""
    file_paths: List[str] = Field(..., min_items=1, max_items=1000)
    apply_quality_assessment: bool = Field(default=True)
    custom_thresholds: Optional[Dict] = Field(default=None)


class BatchProcessingResponse(BaseModel):
    """Response model for batch processing submission."""
    batch_id: str
    task_id: str
    message: str
    total_files: int
    estimated_completion_time: Optional[str] = None


class BatchStatusDetailResponse(BaseModel):
    """Detailed response model for batch status."""
    batch_id: str
    task_id: Optional[str] = None
    status: str
    progress: Dict
    total_items: int
    completed_items: int
    failed_items: int
    errors: List[Dict]
    start_time: str
    last_update: str
    task_state: Optional[str] = None
    task_info: Optional[Dict] = None


class FileMonitoringRequest(BaseModel):
    """Request model for file monitoring setup."""
    directory_path: str
    auto_process: bool = Field(default=True)
    recursive: bool = Field(default=False)
    file_extensions: Optional[List[str]] = Field(default=None)


class FileMonitoringResponse(BaseModel):
    """Response model for file monitoring operations."""
    directory: str
    status: str
    message: str
    is_monitoring: bool


class MonitoringStatusResponse(BaseModel):
    """Response model for monitoring status."""
    monitored_directories: List[Dict]
    total_directories: int


@router.post('/batch/submit', response_model=BatchProcessingResponse)
async def submit_batch_processing(request: BatchProcessingRequest):
    """
    Submit batch processing job for multiple MRIQC files.
    
    Args:
        request: Batch processing request
        
    Returns:
        BatchProcessingResponse with job information
    """
    try:
        # Validate file paths exist
        missing_files = []
        for file_path in request.file_paths:
            if not Path(file_path).exists():
                missing_files.append(file_path)
        
        if missing_files:
            raise HTTPException(
                status_code=400,
                detail=f"Files not found: {', '.join(missing_files[:5])}"
            )
        
        # Submit batch processing
        batch_id, task_id = batch_service.submit_batch_processing(
            request.file_paths,
            request.apply_quality_assessment,
            request.custom_thresholds
        )
        
        logger.info(f"Submitted batch processing: {batch_id} with {len(request.file_paths)} files")
        
        return BatchProcessingResponse(
            batch_id=batch_id,
            task_id=task_id,
            message=f"Batch processing submitted for {len(request.file_paths)} files",
            total_files=len(request.file_paths)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to submit batch processing: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to submit batch processing: {str(e)}")


@router.get('/batch/{batch_id}/status', response_model=BatchStatusDetailResponse)
async def get_batch_processing_status(batch_id: str):
    """
    Get detailed batch processing status.
    
    Args:
        batch_id: Batch identifier
        
    Returns:
        BatchStatusDetailResponse with detailed status
    """
    try:
        status_info = batch_service.get_batch_status(batch_id)
        
        if not status_info:
            raise HTTPException(status_code=404, detail="Batch not found")
        
        return BatchStatusDetailResponse(
            batch_id=batch_id,
            task_id=status_info.get('task_id'),
            status=status_info.get('status', 'unknown'),
            progress=status_info.get('progress', {}),
            total_items=status_info.get('total_items', 0),
            completed_items=status_info.get('completed_items', 0),
            failed_items=status_info.get('failed_items', 0),
            errors=status_info.get('errors', []),
            start_time=status_info.get('start_time', ''),
            last_update=status_info.get('last_update', ''),
            task_state=status_info.get('task_state'),
            task_info=status_info.get('task_info')
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get batch status for {batch_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get batch status: {str(e)}")


@router.get('/batch/{batch_id}/results')
async def get_batch_results(batch_id: str):
    """
    Get batch processing results.
    
    Args:
        batch_id: Batch identifier
        
    Returns:
        Batch processing results
    """
    try:
        results = batch_service.get_batch_results(batch_id)
        
        if not results:
            raise HTTPException(status_code=404, detail="Batch results not found")
        
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get batch results for {batch_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get batch results: {str(e)}")


@router.get('/batch/{batch_id}/subjects', response_model=SubjectListResponse)
async def get_batch_subjects(
    batch_id: str,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Page size")
):
    """
    Get processed subjects from batch results.
    
    Args:
        batch_id: Batch identifier
        page: Page number
        page_size: Page size
        
    Returns:
        SubjectListResponse with batch subjects
    """
    try:
        subjects = batch_service.get_processed_subjects(batch_id)
        
        if not subjects:
            raise HTTPException(status_code=404, detail="Batch subjects not found")
        
        # Apply pagination
        total_count = len(subjects)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_subjects = subjects[start_idx:end_idx]
        
        return SubjectListResponse(
            subjects=paginated_subjects,
            total_count=total_count,
            page=page,
            page_size=page_size,
            filters_applied={'batch_id': batch_id}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get batch subjects for {batch_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get batch subjects: {str(e)}")


@router.delete('/batch/{batch_id}')
async def cancel_batch_processing(batch_id: str):
    """
    Cancel batch processing job.
    
    Args:
        batch_id: Batch identifier
        
    Returns:
        Cancellation status
    """
    try:
        success = batch_service.cancel_batch_processing(batch_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Batch not found or cannot be cancelled")
        
        return {
            'message': f'Batch {batch_id} cancelled successfully',
            'batch_id': batch_id,
            'cancelled_at': datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel batch {batch_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to cancel batch: {str(e)}")


@router.get('/batch/active')
async def get_active_batches():
    """
    Get list of active batch processing jobs.
    
    Returns:
        List of active batches
    """
    try:
        active_batches = batch_service.get_active_batches()
        
        return {
            'active_batches': active_batches,
            'total_active': len(active_batches)
        }
        
    except Exception as e:
        logger.error(f"Failed to get active batches: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get active batches: {str(e)}")


@router.get('/batch/worker-status')
async def get_worker_status():
    """
    Get Celery worker status information.
    
    Returns:
        Worker status information
    """
    try:
        status = batch_service.get_worker_status()
        return status
        
    except Exception as e:
        logger.error(f"Failed to get worker status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get worker status: {str(e)}")


# File Monitoring Endpoints

@router.post('/monitoring/start', response_model=FileMonitoringResponse)
async def start_file_monitoring(request: FileMonitoringRequest):
    """
    Start monitoring directory for new MRIQC files.
    
    Args:
        request: File monitoring request
        
    Returns:
        FileMonitoringResponse with monitoring status
    """
    try:
        success = file_monitor.start_monitoring(
            request.directory_path,
            request.auto_process,
            request.recursive,
            request.file_extensions
        )
        
        if not success:
            raise HTTPException(status_code=400, detail="Failed to start monitoring")
        
        return FileMonitoringResponse(
            directory=request.directory_path,
            status="started",
            message=f"Started monitoring {request.directory_path}",
            is_monitoring=True
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start monitoring {request.directory_path}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start monitoring: {str(e)}")


@router.delete('/monitoring/{directory_path:path}', response_model=FileMonitoringResponse)
async def stop_file_monitoring(directory_path: str):
    """
    Stop monitoring directory.
    
    Args:
        directory_path: Directory to stop monitoring
        
    Returns:
        FileMonitoringResponse with monitoring status
    """
    try:
        success = file_monitor.stop_monitoring(directory_path)
        
        if not success:
            raise HTTPException(status_code=404, detail="Directory not being monitored")
        
        return FileMonitoringResponse(
            directory=directory_path,
            status="stopped",
            message=f"Stopped monitoring {directory_path}",
            is_monitoring=False
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to stop monitoring {directory_path}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to stop monitoring: {str(e)}")


@router.get('/monitoring/status', response_model=MonitoringStatusResponse)
async def get_monitoring_status():
    """
    Get file monitoring status for all directories.
    
    Returns:
        MonitoringStatusResponse with monitoring information
    """
    try:
        monitored_directories = file_monitor.get_monitored_directories()
        
        return MonitoringStatusResponse(
            monitored_directories=monitored_directories,
            total_directories=len(monitored_directories)
        )
        
    except Exception as e:
        logger.error(f"Failed to get monitoring status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get monitoring status: {str(e)}")


@router.get('/monitoring/{directory_path:path}')
async def get_directory_monitoring_status(directory_path: str):
    """
    Get monitoring status for specific directory.
    
    Args:
        directory_path: Directory to check
        
    Returns:
        Directory monitoring status
    """
    try:
        status = file_monitor.get_monitoring_status(directory_path)
        
        if not status:
            raise HTTPException(status_code=404, detail="Directory not being monitored")
        
        return status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get monitoring status for {directory_path}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get monitoring status: {str(e)}")


# Task Management Endpoints

@router.get('/tasks/{task_id}/status')
async def get_task_status(task_id: str):
    """
    Get Celery task status.
    
    Args:
        task_id: Celery task ID
        
    Returns:
        Task status information
    """
    try:
        status = batch_service.get_task_status(task_id)
        return status
        
    except Exception as e:
        logger.error(f"Failed to get task status for {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {str(e)}")

# Security and Privacy Endpoints

class SecurityStatusResponse(BaseModel):
    """Response model for security status."""
    security_enabled: bool
    virus_scan_enabled: bool
    data_retention_days: int
    cleanup_last_run: Optional[datetime]
    audit_logging_enabled: bool
    threat_count_24h: int
    files_cleaned_24h: int


@router.get('/security/status', response_model=SecurityStatusResponse)
async def get_security_status(request: Request):
    """
    Get current security status and statistics.
    
    Returns:
        SecurityStatusResponse with security information
    """
    await security_check_middleware(request)
    
    # This would typically come from a database or cache
    # For now, return basic status
    return SecurityStatusResponse(
        security_enabled=config.SECURITY_ENABLED,
        virus_scan_enabled=config.VIRUS_SCAN_ENABLED,
        data_retention_days=config.DATA_RETENTION_DAYS,
        cleanup_last_run=None,  # Would track actual cleanup runs
        audit_logging_enabled=config.ENABLE_AUDIT_LOGGING,
        threat_count_24h=0,  # Would count from audit logs
        files_cleaned_24h=0   # Would count from cleanup logs
    )


class DataCleanupRequest(BaseModel):
    """Request model for manual data cleanup."""
    force_cleanup: bool = Field(default=False, description="Force cleanup regardless of retention policy")
    target_directories: Optional[List[str]] = Field(default=None, description="Specific directories to clean")


class DataCleanupResponse(BaseModel):
    """Response model for data cleanup."""
    files_deleted: int
    directories_cleaned: int
    bytes_freed: int
    errors: int
    cleanup_timestamp: datetime


@router.post('/security/cleanup', response_model=DataCleanupResponse)
async def manual_data_cleanup(request: Request, cleanup_request: DataCleanupRequest):
    """
    Manually trigger data cleanup based on retention policies.
    
    Args:
        request: FastAPI request object
        cleanup_request: Cleanup configuration
        
    Returns:
        DataCleanupResponse with cleanup statistics
    """
    await security_check_middleware(request)
    client_ip = get_client_ip(request)
    
    # Log cleanup request
    security_auditor.log_security_event(
        'manual_cleanup_requested',
        {
            'force_cleanup': cleanup_request.force_cleanup,
            'target_directories': cleanup_request.target_directories,
            'client_ip': client_ip
        },
        SecurityLevel.MEDIUM
    )
    
    try:
        # Perform cleanup
        cleanup_stats = data_retention_manager.cleanup_expired_data()
        
        # Log successful cleanup
        security_auditor.log_security_event(
            'manual_cleanup_completed',
            cleanup_stats,
            SecurityLevel.LOW
        )
        
        return DataCleanupResponse(
            files_deleted=cleanup_stats['files_deleted'],
            directories_cleaned=cleanup_stats['directories_cleaned'],
            bytes_freed=cleanup_stats['bytes_freed'],
            errors=cleanup_stats['errors'],
            cleanup_timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Manual cleanup failed: {e}")
        security_auditor.log_security_event(
            'manual_cleanup_failed',
            {'error': str(e), 'client_ip': client_ip},
            SecurityLevel.HIGH
        )
        raise HTTPException(
            status_code=500,
            detail="Data cleanup failed"
        )


class PrivacyComplianceResponse(BaseModel):
    """Response model for privacy compliance check."""
    compliant: bool
    issues: List[str]
    recommendations: List[str]
    last_check: datetime


@router.get('/security/privacy-compliance', response_model=PrivacyComplianceResponse)
async def check_privacy_compliance(request: Request):
    """
    Check privacy compliance status.
    
    Returns:
        PrivacyComplianceResponse with compliance information
    """
    await security_check_middleware(request)
    
    issues = []
    recommendations = []
    
    # Check data retention policy
    if config.DATA_RETENTION_DAYS > 90:
        issues.append("Data retention period exceeds recommended 90 days")
        recommendations.append("Consider reducing data retention period")
    
    # Check audit logging
    if not config.ENABLE_AUDIT_LOGGING:
        issues.append("Audit logging is disabled")
        recommendations.append("Enable audit logging for compliance")
    
    # Check virus scanning
    if not config.VIRUS_SCAN_ENABLED:
        issues.append("Virus scanning is disabled")
        recommendations.append("Enable virus scanning for security")
    
    # Check file size limits
    if config.MAX_FILE_SIZE > 100 * 1024 * 1024:  # 100MB
        issues.append("File size limit may be too high")
        recommendations.append("Consider reducing maximum file size")
    
    compliant = len(issues) == 0
    
    return PrivacyComplianceResponse(
        compliant=compliant,
        issues=issues,
        recommendations=recommendations,
        last_check=datetime.utcnow()
    )


class SecurityThreatResponse(BaseModel):
    """Response model for security threats."""
    threat_id: str
    threat_type: str
    severity: str
    description: str
    timestamp: datetime
    resolved: bool


@router.get('/security/threats', response_model=List[SecurityThreatResponse])
async def get_security_threats(
    request: Request,
    limit: int = Query(default=50, le=100),
    severity: Optional[str] = Query(default=None)
):
    """
    Get recent security threats and incidents.
    
    Args:
        request: FastAPI request object
        limit: Maximum number of threats to return
        severity: Filter by severity level
        
    Returns:
        List of SecurityThreatResponse objects
    """
    await security_check_middleware(request)
    
    # This would typically read from audit logs or database
    # For now, return empty list as placeholder
    return []


# Add security headers middleware function
def add_security_headers(response):
    """Add security headers to response."""
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'"
    return response


@router.post('/subjects/filter', response_model=SubjectListResponse)
async def filter_subjects(
    filter_request: SubjectFilterRequest,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=1000)
):
    """
    Advanced subject filtering with multiple criteria.
    
    Args:
        filter_request: Filter criteria including sort options
        page: Page number
        page_size: Page size
        
    Returns:
        SubjectListResponse with filtered subjects
    """
    try:
        # Get all subjects from all batches
        all_subjects = []
        for subjects in processed_subjects_store.values():
            all_subjects.extend(subjects)
        
        # Apply filters
        filtered_subjects = all_subjects
        filters_applied = {}
        
        # Quality status filter
        if filter_request.quality_status:
            filtered_subjects = [s for s in filtered_subjects 
                               if s.quality_assessment.overall_status in filter_request.quality_status]
            filters_applied['quality_status'] = [status.value for status in filter_request.quality_status]
        
        # Age group filter
        if filter_request.age_group:
            filtered_subjects = [s for s in filtered_subjects 
                               if (s.normalized_metrics and 
                                   s.normalized_metrics.age_group in filter_request.age_group)]
            filters_applied['age_group'] = [group.value for group in filter_request.age_group]
        
        # Scan type filter
        if filter_request.scan_type:
            filtered_subjects = [s for s in filtered_subjects 
                               if s.subject_info.scan_type in filter_request.scan_type]
            filters_applied['scan_type'] = filter_request.scan_type
        
        # Age range filter
        if filter_request.age_range:
            min_age = filter_request.age_range.get('min')
            max_age = filter_request.age_range.get('max')
            
            def age_in_range(subject):
                age = subject.subject_info.age
                if age is None:
                    return False
                if min_age is not None and age < min_age:
                    return False
                if max_age is not None and age > max_age:
                    return False
                return True
            
            filtered_subjects = [s for s in filtered_subjects if age_in_range(s)]
            filters_applied['age_range'] = filter_request.age_range
        
        # Metric filters
        if filter_request.metric_filters:
            def metric_in_range(subject, metric_name, criteria):
                metric_value = getattr(subject.raw_metrics, metric_name, None)
                if metric_value is None:
                    return False
                
                min_val = criteria.get('min')
                max_val = criteria.get('max')
                
                if min_val is not None and metric_value < min_val:
                    return False
                if max_val is not None and metric_value > max_val:
                    return False
                return True
            
            for metric_name, criteria in filter_request.metric_filters.items():
                filtered_subjects = [s for s in filtered_subjects 
                                   if metric_in_range(s, metric_name, criteria)]
            
            filters_applied['metric_filters'] = filter_request.metric_filters
        
        # Date range filter
        if filter_request.date_range:
            start_date = filter_request.date_range.get('start')
            end_date = filter_request.date_range.get('end')
            
            def date_in_range(subject):
                proc_date = subject.processing_timestamp.date()
                
                if start_date:
                    start = datetime.strptime(start_date, '%Y-%m-%d').date()
                    if proc_date < start:
                        return False
                
                if end_date:
                    end = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if proc_date > end:
                        return False
                
                return True
            
            filtered_subjects = [s for s in filtered_subjects if date_in_range(s)]
            filters_applied['date_range'] = filter_request.date_range
        
        # Batch IDs filter
        if filter_request.batch_ids:
            # This would require tracking which batch each subject belongs to
            # For now, we'll skip this filter
            filters_applied['batch_ids'] = filter_request.batch_ids
        
        # Search text filter
        if filter_request.search_text:
            search_lower = filter_request.search_text.lower()
            
            def matches_search(subject):
                # Search in subject ID
                if search_lower in subject.subject_info.subject_id.lower():
                    return True
                
                # Search in session if available
                if (subject.subject_info.session and 
                    search_lower in subject.subject_info.session.lower()):
                    return True
                
                return False
            
            filtered_subjects = [s for s in filtered_subjects if matches_search(s)]
            filters_applied['search_text'] = filter_request.search_text
        
        # Apply sorting (default to processing timestamp desc)
        sort_by = 'processing_timestamp'
        sort_order = 'desc'
        reverse = sort_order == 'desc'
        
        if sort_by == 'subject_id':
            filtered_subjects.sort(key=lambda s: s.subject_info.subject_id, reverse=reverse)
        elif sort_by == 'age':
            filtered_subjects.sort(key=lambda s: s.subject_info.age or 0, reverse=reverse)
        elif sort_by == 'quality_status':
            status_order = {'pass': 0, 'warning': 1, 'uncertain': 2, 'fail': 3}
            filtered_subjects.sort(
                key=lambda s: status_order.get(s.quality_assessment.overall_status.value, 4),
                reverse=reverse
            )
        elif sort_by == 'composite_score':
            filtered_subjects.sort(key=lambda s: s.quality_assessment.composite_score, reverse=reverse)
        elif sort_by == 'processing_timestamp':
            filtered_subjects.sort(key=lambda s: s.processing_timestamp, reverse=reverse)
        
        # Apply pagination
        total_count = len(filtered_subjects)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_subjects = filtered_subjects[start_idx:end_idx]
        
        # Add sort information to response
        sort_applied = {'sort_by': sort_by, 'sort_order': sort_order}
        
        return SubjectListResponse(
            subjects=paginated_subjects,
            total_count=total_count,
            page=page,
            page_size=page_size,
            filters_applied=filters_applied,
            sort_applied=sort_applied
        )
        
    except Exception as e:
        logger.error(f"Error filtering subjects: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to filter subjects: {str(e)}")


class BulkUpdateRequest(BaseModel):
    """Request model for bulk quality updates."""
    subject_ids: List[str]
    quality_status: QualityStatus
    reason: str = ""


@router.post('/subjects/bulk-update')
async def bulk_update_subjects_quality(
    request: Request,
    bulk_request: BulkUpdateRequest
):
    """
    Bulk update quality status for multiple subjects.
    
    Args:
        request: FastAPI request object
        bulk_request: Bulk update request data
        
    Returns:
        Success message with update count
    """
    request_id = get_request_id(request)
    
    try:
        updated_count = 0
        errors = []
        
        # Find and update subjects across all batches
        for batch_id, subjects in processed_subjects_store.items():
            for subject in subjects:
                if subject.subject_info.subject_id in bulk_request.subject_ids:
                    try:
                        # Update quality status
                        old_status = subject.quality_assessment.overall_status
                        subject.quality_assessment.overall_status = bulk_request.quality_status
                        
                        # Log the quality decision change
                        audit_logger.log_quality_decision(
                            subject_id=subject.subject_info.subject_id,
                            decision=bulk_request.quality_status.value,
                            reason=bulk_request.reason or f"Bulk update from {old_status.value} to {bulk_request.quality_status.value}",
                            automated=False,
                            confidence=subject.quality_assessment.confidence,
                            metrics=subject.raw_metrics.dict(exclude_none=True),
                            previous_decision=old_status.value
                        )
                        
                        updated_count += 1
                        
                    except Exception as e:
                        error_msg = f"Failed to update {subject.subject_info.subject_id}: {str(e)}"
                        errors.append(error_msg)
                        logger.error(error_msg)
        
        # Log bulk update action
        audit_logger.log_user_action(
            action_type="bulk_quality_update",
            resource_type="subjects",
            resource_id=f"bulk_update_{len(bulk_request.subject_ids)}_subjects",
            new_values={
                'subject_ids': bulk_request.subject_ids,
                'new_quality_status': bulk_request.quality_status.value,
                'reason': bulk_request.reason,
                'updated_count': updated_count,
                'errors_count': len(errors)
            },
            request=request
        )
        
        if errors:
            logger.warning(f"Bulk update completed with {len(errors)} errors: {errors}")
        
        return {
            "message": f"Successfully updated {updated_count} subjects",
            "updated_count": updated_count,
            "requested_count": len(bulk_request.subject_ids),
            "errors": errors
        }
        
    except Exception as e:
        error_response = error_handler.handle_system_error(
            component="bulk_update",
            message="Bulk update operation failed",
            exception=e,
            request_id=request_id
        )
        raise HTTPException(status_code=500, detail=error_response.message)
# Lon
gitudinal data endpoints
from .longitudinal_service import LongitudinalService
from .models import LongitudinalSubject, LongitudinalTrend, LongitudinalSummary

# Initialize longitudinal service
longitudinal_service = LongitudinalService(db=normative_db, age_normalizer=age_normalizer)


@router.post('/longitudinal/subjects/{subject_id}/timepoints')
async def add_subject_timepoint(
    subject_id: str,
    processed_subject: ProcessedSubject,
    session: Optional[str] = Query(None, description="Session identifier"),
    days_from_baseline: Optional[int] = Query(None, description="Days from baseline scan"),
    study_name: Optional[str] = Query(None, description="Study name")
):
    """
    Add a timepoint for a longitudinal subject.
    
    Args:
        subject_id: Subject identifier
        processed_subject: Complete processed subject data
        session: Session identifier (e.g., 'baseline', 'followup1')
        days_from_baseline: Days elapsed from baseline scan
        study_name: Name of longitudinal study
        
    Returns:
        Timepoint ID and confirmation
    """
    try:
        timepoint_id = longitudinal_service.add_subject_timepoint(
            subject_id=subject_id,
            processed_subject=processed_subject,
            session=session,
            days_from_baseline=days_from_baseline,
            study_name=study_name
        )
        
        return {
            "timepoint_id": timepoint_id,
            "subject_id": subject_id,
            "session": session,
            "message": "Timepoint added successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to add timepoint for subject {subject_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to add timepoint: {str(e)}")


@router.get('/longitudinal/subjects/{subject_id}', response_model=LongitudinalSubject)
async def get_longitudinal_subject(subject_id: str):
    """
    Get complete longitudinal subject data.
    
    Args:
        subject_id: Subject identifier
        
    Returns:
        LongitudinalSubject with all timepoints
    """
    try:
        longitudinal_subject = longitudinal_service.get_longitudinal_subject(subject_id)
        
        if not longitudinal_subject:
            raise HTTPException(status_code=404, detail="Longitudinal subject not found")
        
        return longitudinal_subject
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get longitudinal subject {subject_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get longitudinal subject: {str(e)}")


@router.get('/longitudinal/subjects')
async def get_longitudinal_subjects(
    study_name: Optional[str] = Query(None, description="Filter by study name"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Page size")
):
    """
    Get all subjects with longitudinal data.
    
    Args:
        study_name: Optional study name filter
        page: Page number (1-based)
        page_size: Number of subjects per page
        
    Returns:
        List of longitudinal subjects with summary information
    """
    try:
        subjects = longitudinal_service.get_subjects_with_longitudinal_data(study_name)
        
        # Apply pagination
        total_count = len(subjects)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_subjects = subjects[start_idx:end_idx]
        
        return {
            "subjects": paginated_subjects,
            "total_count": total_count,
            "page": page,
            "page_size": page_size,
            "study_name": study_name
        }
        
    except Exception as e:
        logger.error(f"Failed to get longitudinal subjects: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get longitudinal subjects: {str(e)}")


@router.get('/longitudinal/subjects/{subject_id}/trends', response_model=List[LongitudinalTrend])
async def get_subject_trends(subject_id: str):
    """
    Get all quality metric trends for a subject.
    
    Args:
        subject_id: Subject identifier
        
    Returns:
        List of LongitudinalTrend objects for all metrics
    """
    try:
        trends = longitudinal_service.calculate_all_trends_for_subject(subject_id)
        return trends
        
    except Exception as e:
        logger.error(f"Failed to get trends for subject {subject_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get subject trends: {str(e)}")


@router.get('/longitudinal/subjects/{subject_id}/trends/{metric_name}', response_model=LongitudinalTrend)
async def get_subject_metric_trend(subject_id: str, metric_name: str):
    """
    Get trend for a specific metric for a subject.
    
    Args:
        subject_id: Subject identifier
        metric_name: Name of the quality metric
        
    Returns:
        LongitudinalTrend for the specified metric
    """
    try:
        trend = longitudinal_service.calculate_metric_trend(subject_id, metric_name)
        
        if not trend:
            raise HTTPException(
                status_code=404, 
                detail=f"Trend not found for subject {subject_id}, metric {metric_name}"
            )
        
        return trend
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get trend for {subject_id}, metric {metric_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get metric trend: {str(e)}")


@router.post('/longitudinal/subjects/{subject_id}/calculate-trends')
async def calculate_subject_trends(subject_id: str):
    """
    Calculate and store trends for all metrics for a subject.
    
    Args:
        subject_id: Subject identifier
        
    Returns:
        Summary of calculated trends
    """
    try:
        trends = longitudinal_service.calculate_all_trends_for_subject(subject_id)
        
        trend_summary = {
            "subject_id": subject_id,
            "trends_calculated": len(trends),
            "metrics": [trend.metric_name for trend in trends],
            "trend_directions": {
                trend.metric_name: trend.trend_direction for trend in trends
            }
        }
        
        return trend_summary
        
    except Exception as e:
        logger.error(f"Failed to calculate trends for subject {subject_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to calculate trends: {str(e)}")


@router.get('/longitudinal/studies/{study_name}/summary', response_model=LongitudinalSummary)
async def get_study_longitudinal_summary(study_name: str):
    """
    Get longitudinal summary for a study.
    
    Args:
        study_name: Name of the study
        
    Returns:
        LongitudinalSummary with study-level statistics
    """
    try:
        summary = longitudinal_service.get_study_longitudinal_summary(study_name)
        
        if not summary:
            raise HTTPException(status_code=404, detail=f"Study {study_name} not found")
        
        return summary
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get longitudinal summary for study {study_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get study summary: {str(e)}")


@router.get('/longitudinal/subjects/{subject_id}/age-transitions')
async def get_subject_age_transitions(subject_id: str):
    """
    Get age group transitions for a subject.
    
    Args:
        subject_id: Subject identifier
        
    Returns:
        List of age group transition events
    """
    try:
        transitions = longitudinal_service.detect_age_group_transitions(subject_id)
        
        return {
            "subject_id": subject_id,
            "transitions": transitions,
            "transition_count": len(transitions)
        }
        
    except Exception as e:
        logger.error(f"Failed to get age transitions for subject {subject_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get age transitions: {str(e)}")


@router.post('/longitudinal/export')
async def export_longitudinal_data(
    study_name: Optional[str] = Query(None, description="Filter by study name"),
    format: str = Query("csv", description="Export format (csv or json)")
):
    """
    Export longitudinal data for analysis.
    
    Args:
        study_name: Optional study name filter
        format: Export format ('csv' or 'json')
        
    Returns:
        File download response
    """
    try:
        if format not in ['csv', 'json']:
            raise HTTPException(status_code=400, detail="Format must be 'csv' or 'json'")
        
        filepath = longitudinal_service.export_longitudinal_data(study_name, format)
        
        # Determine content type
        content_type = "text/csv" if format == "csv" else "application/json"
        
        # Create filename for download
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"longitudinal_data_{study_name or 'all'}_{timestamp}.{format}"
        
        def iterfile(file_path: str):
            with open(file_path, 'rb') as file:
                yield from file
        
        return StreamingResponse(
            iterfile(filepath),
            media_type=content_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to export longitudinal data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to export data: {str(e)}")


@router.delete('/longitudinal/subjects/{subject_id}')
async def delete_longitudinal_subject(subject_id: str):
    """
    Delete a longitudinal subject and all associated data.
    
    Args:
        subject_id: Subject identifier
        
    Returns:
        Deletion confirmation
    """
    try:
        success = longitudinal_service.db.delete_longitudinal_subject(subject_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Longitudinal subject not found")
        
        return {
            "subject_id": subject_id,
            "message": "Longitudinal subject deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete longitudinal subject {subject_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete subject: {str(e)}")


@router.delete('/longitudinal/timepoints/{timepoint_id}')
async def delete_timepoint(timepoint_id: str):
    """
    Delete a specific timepoint.
    
    Args:
        timepoint_id: Timepoint identifier
        
    Returns:
        Deletion confirmation
    """
    try:
        success = longitudinal_service.db.delete_timepoint(timepoint_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Timepoint not found")
        
        return {
            "timepoint_id": timepoint_id,
            "message": "Timepoint deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete timepoint {timepoint_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete timepoint: {str(e)}")
# Perfo
rmance Monitoring Endpoints

@router.get("/api/performance/stats")
@monitor_performance("get_performance_stats")
async def get_performance_stats():
    """Get comprehensive performance statistics."""
    try:
        stats = performance_monitor.get_performance_summary()
        return JSONResponse(content=stats)
    except Exception as e:
        logger.error(f"Error getting performance stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve performance statistics")


@router.get("/api/performance/operations")
@monitor_performance("get_operation_stats")
async def get_operation_stats(operation_name: Optional[str] = Query(None)):
    """Get statistics for specific operations."""
    try:
        stats = performance_monitor.get_operation_stats(operation_name)
        return JSONResponse(content=stats)
    except Exception as e:
        logger.error(f"Error getting operation stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve operation statistics")


@router.get("/api/performance/cache")
@monitor_performance("get_cache_stats")
async def get_cache_stats():
    """Get cache performance statistics."""
    try:
        stats = cache_service.get_cache_stats()
        return JSONResponse(content=stats)
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve cache statistics")


@router.get("/api/performance/database")
@monitor_performance("get_database_stats")
async def get_database_stats():
    """Get database connection pool statistics."""
    try:
        pool = get_connection_pool()
        stats = pool.get_stats()
        return JSONResponse(content=stats)
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve database statistics")


@router.post("/api/performance/reset")
@monitor_performance("reset_performance_stats")
async def reset_performance_stats():
    """Reset performance statistics."""
    try:
        performance_monitor.reset_stats()
        return JSONResponse(content={"message": "Performance statistics reset successfully"})
    except Exception as e:
        logger.error(f"Error resetting performance stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to reset performance statistics")


@router.get("/api/performance/export")
@monitor_performance("export_performance_metrics")
async def export_performance_metrics(format: str = Query("json")):
    """Export performance metrics."""
    try:
        if format.lower() not in ["json"]:
            raise HTTPException(status_code=400, detail="Unsupported export format")
        
        exported_data = performance_monitor.export_metrics(format)
        
        return JSONResponse(
            content={"data": exported_data, "format": format},
            headers={"Content-Disposition": f"attachment; filename=performance_metrics.{format}"}
        )
    except Exception as e:
        logger.error(f"Error exporting performance metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to export performance metrics")


@router.post("/api/performance/cache/clear")
@monitor_performance("clear_cache")
async def clear_cache(pattern: Optional[str] = Query(None)):
    """Clear cache entries."""
    try:
        if pattern:
            cleared_count = cache_service.clear_pattern(pattern)
            message = f"Cleared {cleared_count} cache entries matching pattern '{pattern}'"
        else:
            # Clear all cache entries (use with caution)
            cleared_count = cache_service.clear_pattern("*")
            message = f"Cleared {cleared_count} cache entries"
        
        return JSONResponse(content={"message": message, "cleared_count": cleared_count})
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        raise HTTPException(status_code=500, detail="Failed to clear cache")


# Cache warming endpoints

@router.post("/api/performance/cache/warm")
@monitor_performance("warm_cache")
async def warm_cache():
    """Warm up the cache with frequently accessed data."""
    try:
        from .database import NormativeDatabase
        
        db = NormativeDatabase()
        
        # Warm up age groups cache
        age_groups = db.get_age_groups()
        logger.info(f"Warmed cache with {len(age_groups)} age groups")
        
        # Warm up normative data cache for common metrics
        common_metrics = ['snr', 'cnr', 'fber', 'efc', 'fwhm_avg']
        warmed_count = 0
        
        for age_group in age_groups:
            for metric in common_metrics:
                normative_data = db.get_normative_data(metric, age_group['id'])
                if normative_data:
                    warmed_count += 1
                
                thresholds = db.get_quality_thresholds(metric, age_group['id'])
                if thresholds:
                    warmed_count += 1
        
        return JSONResponse(content={
            "message": "Cache warmed successfully",
            "age_groups": len(age_groups),
            "normative_entries": warmed_count
        })
    except Exception as e:
        logger.error(f"Error warming cache: {e}")
        raise HTTPException(status_code=500, detail="Failed to warm cache")
# Impo
rt integration services
from .workflow_orchestrator import workflow_orchestrator
from .integration_service import integration_service
from .models import WorkflowConfiguration, BatchWorkflowRequest, EndToEndTestResult


# End-to-end workflow endpoints

@router.post("/workflow/execute", response_model=Dict)
async def execute_complete_workflow(
    request: Dict,
    background_tasks: BackgroundTasks,
    security_info: Dict = Depends(security_check_middleware)
):
    """
    Execute complete end-to-end workflow from file upload to export.
    
    This endpoint orchestrates the entire user workflow including:
    - File upload and validation
    - Data processing and quality assessment
    - Age normalization and longitudinal analysis
    - Export generation and reporting
    """
    request_id = get_request_id()
    
    try:
        # Validate request
        file_path = request.get('file_path')
        if not file_path:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="file_path is required"
            )
        
        # Parse workflow configuration
        config_data = request.get('config', {})
        workflow_config = WorkflowConfiguration(**config_data)
        
        # Create progress callback for WebSocket updates
        workflow_id = str(uuid.uuid4())
        
        async def progress_callback(progress_data):
            await manager.broadcast_dashboard_update(
                json.dumps({
                    "type": "workflow_progress",
                    "workflow_id": workflow_id,
                    **progress_data
                })
            )
        
        # Execute workflow in background
        async def execute_workflow():
            try:
                result = await integration_service.execute_complete_user_workflow(
                    file_path=file_path,
                    user_id=security_info.get('client_ip'),
                    config=workflow_config,
                    progress_callback=progress_callback
                )
                
                # Store result for retrieval
                processed_subjects_store[workflow_id] = result.subjects
                batch_status_store[workflow_id] = {
                    'workflow_id': workflow_id,
                    'status': result.status.value,
                    'result': result,
                    'completed_at': datetime.now()
                }
                
                # Notify completion
                await manager.broadcast_dashboard_update(
                    json.dumps({
                        "type": "workflow_completed",
                        "workflow_id": workflow_id,
                        "status": result.status.value,
                        "subjects_processed": len(result.subjects)
                    })
                )
                
            except Exception as e:
                logger.error(f"Workflow {workflow_id} failed: {str(e)}")
                
                # Store error result
                batch_status_store[workflow_id] = {
                    'workflow_id': workflow_id,
                    'status': 'failed',
                    'error': str(e),
                    'completed_at': datetime.now()
                }
                
                # Notify error
                await manager.broadcast_dashboard_update(
                    json.dumps({
                        "type": "workflow_error",
                        "workflow_id": workflow_id,
                        "error": str(e)
                    })
                )
        
        background_tasks.add_task(execute_workflow)
        
        return {
            "message": "Workflow execution started",
            "workflow_id": workflow_id,
            "status": "initializing"
        }
        
    except Exception as e:
        error_response = error_handler.handle_api_error(
            operation="execute_complete_workflow",
            message="Failed to start workflow execution",
            exception=e,
            request_id=request_id
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_response.model_dump()
        )


@router.post("/workflow/batch", response_model=Dict)
async def execute_batch_workflow(
    request: BatchWorkflowRequest,
    background_tasks: BackgroundTasks,
    security_info: Dict = Depends(security_check_middleware)
):
    """
    Execute batch workflow for multiple MRIQC files.
    
    This endpoint processes multiple files in batch mode with optional
    parallel processing and comprehensive error handling.
    """
    request_id = get_request_id()
    batch_id = str(uuid.uuid4())
    
    try:
        # Parse workflow configuration
        workflow_config = WorkflowConfiguration(**(request.workflow_config or {}))
        
        # Create progress callback
        async def batch_progress_callback(progress_data):
            await manager.broadcast_dashboard_update(
                json.dumps({
                    "type": "batch_workflow_progress",
                    "batch_id": batch_id,
                    **progress_data
                })
            )
        
        # Execute batch workflow in background
        async def execute_batch():
            try:
                results = await integration_service.execute_batch_integration_workflow(
                    file_paths=request.file_paths,
                    user_id=security_info.get('client_ip'),
                    config=workflow_config,
                    progress_callback=batch_progress_callback
                )
                
                # Store results
                batch_status_store[batch_id] = {
                    'batch_id': batch_id,
                    'status': 'completed',
                    'results': results,
                    'completed_at': datetime.now(),
                    'total_files': len(request.file_paths),
                    'successful_files': len([r for r in results if r.status.value == 'completed']),
                    'failed_files': len([r for r in results if r.status.value == 'failed'])
                }
                
                # Aggregate all subjects
                all_subjects = []
                for result in results:
                    all_subjects.extend(result.subjects)
                processed_subjects_store[batch_id] = all_subjects
                
                # Notify completion
                await manager.broadcast_dashboard_update(
                    json.dumps({
                        "type": "batch_workflow_completed",
                        "batch_id": batch_id,
                        "total_files": len(request.file_paths),
                        "successful_files": batch_status_store[batch_id]['successful_files'],
                        "failed_files": batch_status_store[batch_id]['failed_files'],
                        "total_subjects": len(all_subjects)
                    })
                )
                
            except Exception as e:
                logger.error(f"Batch workflow {batch_id} failed: {str(e)}")
                
                batch_status_store[batch_id] = {
                    'batch_id': batch_id,
                    'status': 'failed',
                    'error': str(e),
                    'completed_at': datetime.now()
                }
                
                await manager.broadcast_dashboard_update(
                    json.dumps({
                        "type": "batch_workflow_error",
                        "batch_id": batch_id,
                        "error": str(e)
                    })
                )
        
        background_tasks.add_task(execute_batch)
        
        return {
            "message": "Batch workflow execution started",
            "batch_id": batch_id,
            "status": "initializing",
            "total_files": len(request.file_paths)
        }
        
    except Exception as e:
        error_response = error_handler.handle_api_error(
            operation="execute_batch_workflow",
            message="Failed to start batch workflow execution",
            exception=e,
            request_id=request_id
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_response.model_dump()
        )


@router.get("/workflow/{workflow_id}/status", response_model=Dict)
async def get_workflow_status(
    workflow_id: str,
    security_info: Dict = Depends(security_check_middleware)
):
    """Get status of a running or completed workflow."""
    request_id = get_request_id()
    
    try:
        # Check batch status store first
        if workflow_id in batch_status_store:
            status_data = batch_status_store[workflow_id]
            return {
                "workflow_id": workflow_id,
                "status": status_data.get('status', 'unknown'),
                "completed_at": status_data.get('completed_at'),
                "subjects_processed": len(processed_subjects_store.get(workflow_id, [])),
                "result_available": workflow_id in processed_subjects_store
            }
        
        # Check active workflows in orchestrator
        orchestrator_status = workflow_orchestrator.get_workflow_status(workflow_id)
        if orchestrator_status:
            return {
                "workflow_id": workflow_id,
                "status": orchestrator_status.get('status', 'unknown'),
                "progress": orchestrator_status.get('progress', 0),
                "current_step": orchestrator_status.get('current_step'),
                "subjects_processed": orchestrator_status.get('subjects_processed', 0),
                "total_subjects": orchestrator_status.get('total_subjects', 0)
            }
        
        # Check integration service
        integration_status = integration_service.get_integration_status(workflow_id)
        if integration_status:
            return {
                "workflow_id": workflow_id,
                "status": integration_status.get('status', 'unknown'),
                "components_involved": integration_status.get('components_involved', []),
                "errors": integration_status.get('errors', []),
                "warnings": integration_status.get('warnings', [])
            }
        
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workflow {workflow_id} not found"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        error_response = error_handler.handle_api_error(
            operation="get_workflow_status",
            message=f"Failed to get workflow status for {workflow_id}",
            exception=e,
            request_id=request_id
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_response.model_dump()
        )


@router.get("/workflow/{workflow_id}/result", response_model=Dict)
async def get_workflow_result(
    workflow_id: str,
    security_info: Dict = Depends(security_check_middleware)
):
    """Get complete result of a completed workflow."""
    request_id = get_request_id()
    
    try:
        if workflow_id not in batch_status_store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow {workflow_id} not found"
            )
        
        status_data = batch_status_store[workflow_id]
        
        if status_data.get('status') != 'completed':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Workflow {workflow_id} is not completed (status: {status_data.get('status')})"
            )
        
        # Get subjects
        subjects = processed_subjects_store.get(workflow_id, [])
        
        # Get workflow result if available
        workflow_result = status_data.get('result')
        
        return {
            "workflow_id": workflow_id,
            "status": status_data['status'],
            "subjects": [subject.model_dump() for subject in subjects],
            "subjects_count": len(subjects),
            "completed_at": status_data.get('completed_at'),
            "export_files": workflow_result.export_files if workflow_result else {},
            "summary": workflow_result.summary.model_dump() if workflow_result and workflow_result.summary else None,
            "processing_time": workflow_result.processing_time if workflow_result else 0,
            "metadata": workflow_result.metadata if workflow_result else {}
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error_response = error_handler.handle_api_error(
            operation="get_workflow_result",
            message=f"Failed to get workflow result for {workflow_id}",
            exception=e,
            request_id=request_id
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_response.model_dump()
        )


@router.post("/test/end-to-end", response_model=EndToEndTestResult)
async def run_end_to_end_tests(
    request: Dict,
    security_info: Dict = Depends(security_check_middleware)
):
    """
    Run comprehensive end-to-end integration tests.
    
    This endpoint executes complete integration tests covering all
    components and workflows to validate system functionality.
    """
    request_id = get_request_id()
    
    try:
        test_data_paths = request.get('test_data_paths', [])
        test_config = request.get('test_config', {})
        
        if not test_data_paths:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="test_data_paths is required"
            )
        
        # Execute end-to-end tests
        test_result = await integration_service.run_end_to_end_tests(
            test_data_paths=test_data_paths,
            test_config=test_config
        )
        
        return test_result
        
    except HTTPException:
        raise
    except Exception as e:
        error_response = error_handler.handle_api_error(
            operation="run_end_to_end_tests",
            message="Failed to run end-to-end tests",
            exception=e,
            request_id=request_id
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_response.model_dump()
        )


@router.get("/integration/status", response_model=Dict)
async def get_integration_status(
    security_info: Dict = Depends(security_check_middleware)
):
    """Get overall integration system status and health."""
    request_id = get_request_id()
    
    try:
        # Get workflow orchestrator metrics
        orchestrator_metrics = workflow_orchestrator.get_performance_metrics()
        
        # Get integration service status
        integration_history = integration_service.get_integration_history()
        active_integrations = len(integration_service.active_integrations)
        
        # Get performance metrics
        performance_metrics = performance_monitor.get_stats()
        
        # Get cache status
        cache_status = await cache_service.get_status()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "orchestrator_metrics": orchestrator_metrics,
            "active_integrations": active_integrations,
            "completed_integrations": len(integration_history),
            "performance_metrics": performance_metrics,
            "cache_status": cache_status,
            "components_status": {
                "workflow_orchestrator": "active",
                "integration_service": "active",
                "performance_monitor": "active",
                "cache_service": "active",
                "security_auditor": "active"
            }
        }
        
    except Exception as e:
        error_response = error_handler.handle_api_error(
            operation="get_integration_status",
            message="Failed to get integration status",
            exception=e,
            request_id=request_id
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_response.model_dump()
        )


# Security headers function
def add_security_headers(response):
    """Add security headers to response."""
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'"
    return response