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

from fastapi import APIRouter, HTTPException, UploadFile, File, BackgroundTasks, Query, Depends, WebSocket, WebSocketDisconnect
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

logger = logging.getLogger(__name__)

router = APIRouter()

# Global instances
mriqc_processor = MRIQCProcessor()
quality_assessor = QualityAssessor()
age_normalizer = AgeNormalizer()
config_service = ConfigurationService()

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
                    
                    # Update subject with quality assessment
                    subject.quality_assessment = quality_assessment
                    
                    # Add normalized metrics if age is available
                    if subject.subject_info.age is not None:
                        normalized_metrics = age_normalizer.normalize_metrics(
                            subject.raw_metrics,
                            subject.subject_info.age
                        )
                        subject.normalized_metrics = normalized_metrics
                
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
                logger.error(f"Error processing subject {subject.subject_info.subject_id}: {str(e)}")
                error = ProcessingError(
                    error_type="quality_assessment_error",
                    message=f"Failed to assess quality for {subject.subject_info.subject_id}: {str(e)}",
                    error_code="QA_001"
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
        logger.error(f"Batch processing failed for {batch_id}: {str(e)}")
        batch_status_store[batch_id].update({
            'status': 'failed',
            'completed_at': datetime.now(),
            'error_message': str(e)
        })
        
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
async def upload_mriqc_file(file: UploadFile = File(...)):
    """
    Upload MRIQC CSV file for processing.
    
    Args:
        file: MRIQC CSV file
        
    Returns:
        FileUploadResponse with file information
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV file")
    
    if file.size and file.size > 50 * 1024 * 1024:  # 50MB limit
        raise HTTPException(status_code=400, detail="File size exceeds 50MB limit")
    
    try:
        # Generate unique file ID
        file_id = generate_file_id()
        
        # Create temporary file
        temp_dir = Path(tempfile.gettempdir()) / "mriqc_uploads"
        temp_dir.mkdir(exist_ok=True)
        
        temp_file_path = temp_dir / f"{file_id}_{file.filename}"
        
        # Save uploaded file
        with open(temp_file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Quick validation to get subject count
        try:
            df = mriqc_processor.parse_mriqc_file(temp_file_path)
            validation_errors = mriqc_processor.validate_mriqc_format(df, str(temp_file_path))
            if validation_errors:
                # Clean up temp file
                temp_file_path.unlink(missing_ok=True)
                error_messages = [error.message for error in validation_errors]
                raise HTTPException(status_code=400, detail=f"Invalid MRIQC file: {'; '.join(error_messages)}")
            subjects_count = len(df)
        except HTTPException:
            raise
        except Exception as e:
            # Clean up temp file
            temp_file_path.unlink(missing_ok=True)
            raise HTTPException(status_code=400, detail=f"Invalid MRIQC file: {str(e)}")
        
        logger.info(f"File uploaded: {file.filename} ({file.size} bytes, {subjects_count} subjects)")
        
        return FileUploadResponse(
            message="File uploaded successfully",
            file_id=file_id,
            filename=file.filename,
            size=file.size or len(content),
            subjects_count=subjects_count
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"File upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"File upload failed: {str(e)}")


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