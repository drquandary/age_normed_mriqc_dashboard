"""
Workflow Orchestrator for Age-Normed MRIQC Dashboard.

This module orchestrates complete end-to-end workflows, integrating all
components from file upload through quality assessment to export and reporting.
"""

import asyncio
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import tempfile
import json

from .models import (
    ProcessedSubject, MRIQCMetrics, SubjectInfo, QualityAssessment,
    QualityStatus, AgeGroup, ProcessingError, StudySummary, StudyConfiguration,
    WorkflowStatus, WorkflowStep, WorkflowResult
)
from .mriqc_processor import MRIQCProcessor, MRIQCProcessingError
from .quality_assessor import QualityAssessor
from .age_normalizer import AgeNormalizer
from .export_engine import ExportEngine
from .config_service import ConfigurationService
from .batch_service import batch_service
from .file_monitor import file_monitor
from .error_handling import error_handler, audit_logger
from .security import SecureFileHandler, security_auditor, data_retention_manager
from .performance_monitor import performance_monitor, monitor_performance
from .cache_service import cache_service
from .longitudinal_service import LongitudinalService
from .exceptions import (
    WorkflowException, FileProcessingException, QualityAssessmentException,
    ExportException, ConfigurationException
)

logger = logging.getLogger(__name__)


class WorkflowOrchestrator:
    """
    Orchestrates complete end-to-end workflows for MRIQC data processing.
    
    This class coordinates all components to provide seamless user workflows
    from file upload through quality assessment to data export and reporting.
    """
    
    def __init__(self):
        """Initialize workflow orchestrator with all required services."""
        self.mriqc_processor = MRIQCProcessor()
        self.quality_assessor = QualityAssessor()
        self.age_normalizer = AgeNormalizer()
        self.export_engine = ExportEngine()
        self.config_service = ConfigurationService()
        self.longitudinal_service = LongitudinalService()
        
        # Workflow state tracking
        self.active_workflows: Dict[str, Dict] = {}
        self.workflow_history: List[Dict] = []
        
        # Performance tracking
        self.workflow_metrics: Dict[str, List[float]] = {
            'upload_time': [],
            'processing_time': [],
            'assessment_time': [],
            'export_time': [],
            'total_time': []
        }
    
    async def execute_complete_workflow(
        self,
        file_path: str,
        workflow_config: Optional[Dict] = None,
        progress_callback: Optional[callable] = None
    ) -> WorkflowResult:
        """
        Execute complete workflow from file upload to final export.
        
        Args:
            file_path: Path to MRIQC CSV file
            workflow_config: Optional workflow configuration
            progress_callback: Optional callback for progress updates
            
        Returns:
            WorkflowResult with complete processing results
        """
        workflow_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        try:
            # Initialize workflow tracking
            workflow_state = {
                'workflow_id': workflow_id,
                'status': WorkflowStatus.INITIALIZING,
                'steps_completed': [],
                'current_step': None,
                'start_time': start_time,
                'progress': 0,
                'errors': [],
                'subjects_processed': 0,
                'total_subjects': 0
            }
            self.active_workflows[workflow_id] = workflow_state
            
            # Log workflow start
            audit_logger.log_user_action(
                action_type="workflow_start",
                resource_type="workflow",
                resource_id=workflow_id,
                new_values={
                    'file_path': file_path,
                    'config': workflow_config or {}
                }
            )
            
            # Step 1: File Upload and Validation
            await self._update_workflow_progress(
                workflow_id, WorkflowStep.FILE_UPLOAD, 10, progress_callback
            )
            
            upload_result = await self._execute_file_upload_step(
                file_path, workflow_config
            )
            workflow_state['file_id'] = upload_result['file_id']
            workflow_state['file_path'] = upload_result['file_path']
            workflow_state['total_subjects'] = upload_result['subjects_count']
            
            # Step 2: Data Processing
            await self._update_workflow_progress(
                workflow_id, WorkflowStep.DATA_PROCESSING, 30, progress_callback
            )
            
            processing_result = await self._execute_processing_step(
                upload_result['file_id'], {**workflow_config, 'file_path': upload_result['file_path']}
            )
            workflow_state['subjects'] = processing_result['subjects']
            workflow_state['subjects_processed'] = len(processing_result['subjects'])
            
            # Step 3: Quality Assessment
            await self._update_workflow_progress(
                workflow_id, WorkflowStep.QUALITY_ASSESSMENT, 60, progress_callback
            )
            
            assessment_result = await self._execute_quality_assessment_step(
                processing_result['subjects'], workflow_config
            )
            workflow_state['assessed_subjects'] = assessment_result['subjects']
            
            # Step 4: Age Normalization (if applicable)
            await self._update_workflow_progress(
                workflow_id, WorkflowStep.AGE_NORMALIZATION, 75, progress_callback
            )
            
            normalization_result = await self._execute_normalization_step(
                assessment_result['subjects'], workflow_config
            )
            workflow_state['normalized_subjects'] = normalization_result['subjects']
            
            # Step 5: Longitudinal Analysis (if applicable)
            await self._update_workflow_progress(
                workflow_id, WorkflowStep.LONGITUDINAL_ANALYSIS, 85, progress_callback
            )
            
            longitudinal_result = await self._execute_longitudinal_step(
                normalization_result['subjects'], workflow_config
            )
            
            # Step 6: Export and Reporting
            await self._update_workflow_progress(
                workflow_id, WorkflowStep.EXPORT_GENERATION, 95, progress_callback
            )
            
            export_result = await self._execute_export_step(
                longitudinal_result['subjects'], workflow_config
            )
            
            # Step 7: Finalization
            await self._update_workflow_progress(
                workflow_id, WorkflowStep.FINALIZATION, 100, progress_callback
            )
            
            # Create final result
            end_time = datetime.now()
            total_time = (end_time - start_time).total_seconds()
            
            workflow_result = WorkflowResult(
                workflow_id=workflow_id,
                status=WorkflowStatus.COMPLETED,
                subjects=longitudinal_result['subjects'],
                summary=self._generate_workflow_summary(longitudinal_result['subjects']),
                export_files=export_result['files'],
                processing_time=total_time,
                steps_completed=workflow_state['steps_completed'],
                errors=workflow_state['errors'],
                metadata={
                    'file_path': file_path,
                    'config': workflow_config or {},
                    'subjects_processed': workflow_state['subjects_processed'],
                    'total_subjects': workflow_state['total_subjects']
                }
            )
            
            # Update workflow state
            workflow_state['status'] = WorkflowStatus.COMPLETED
            workflow_state['end_time'] = end_time
            workflow_state['result'] = workflow_result
            
            # Log workflow completion
            audit_logger.log_user_action(
                action_type="workflow_complete",
                resource_type="workflow",
                resource_id=workflow_id,
                new_values={
                    'processing_time': total_time,
                    'subjects_processed': workflow_state['subjects_processed'],
                    'status': 'completed'
                }
            )
            
            # Archive workflow
            self._archive_workflow(workflow_id)
            
            return workflow_result
            
        except Exception as e:
            # Handle workflow failure
            error_result = await self._handle_workflow_error(
                workflow_id, e, progress_callback
            )
            return error_result
    
    async def execute_batch_workflow(
        self,
        file_paths: List[str],
        workflow_config: Optional[Dict] = None,
        progress_callback: Optional[callable] = None
    ) -> List[WorkflowResult]:
        """
        Execute batch workflow for multiple files.
        
        Args:
            file_paths: List of MRIQC CSV file paths
            workflow_config: Optional workflow configuration
            progress_callback: Optional callback for progress updates
            
        Returns:
            List of WorkflowResult objects
        """
        batch_id = str(uuid.uuid4())
        start_time = datetime.now()
        results = []
        
        try:
            # Log batch start
            audit_logger.log_user_action(
                action_type="batch_workflow_start",
                resource_type="batch_workflow",
                resource_id=batch_id,
                new_values={
                    'file_count': len(file_paths),
                    'config': workflow_config or {}
                }
            )
            
            # Process each file
            for i, file_path in enumerate(file_paths):
                try:
                    # Update batch progress
                    batch_progress = (i / len(file_paths)) * 100
                    if progress_callback:
                        await progress_callback({
                            'type': 'batch_progress',
                            'batch_id': batch_id,
                            'file_index': i,
                            'total_files': len(file_paths),
                            'progress': batch_progress,
                            'current_file': file_path
                        })
                    
                    # Execute individual workflow
                    result = await self.execute_complete_workflow(
                        file_path, workflow_config, progress_callback
                    )
                    results.append(result)
                    
                except Exception as e:
                    logger.error(f"Failed to process file {file_path}: {str(e)}")
                    # Continue with other files
                    error_result = WorkflowResult(
                        workflow_id=str(uuid.uuid4()),
                        status=WorkflowStatus.FAILED,
                        subjects=[],
                        summary=None,
                        export_files={},
                        processing_time=0,
                        steps_completed=[],
                        errors=[str(e)],
                        metadata={'file_path': file_path, 'batch_id': batch_id}
                    )
                    results.append(error_result)
            
            # Log batch completion
            end_time = datetime.now()
            total_time = (end_time - start_time).total_seconds()
            
            audit_logger.log_user_action(
                action_type="batch_workflow_complete",
                resource_type="batch_workflow",
                resource_id=batch_id,
                new_values={
                    'processing_time': total_time,
                    'files_processed': len(file_paths),
                    'successful_workflows': len([r for r in results if r.status == WorkflowStatus.COMPLETED]),
                    'failed_workflows': len([r for r in results if r.status == WorkflowStatus.FAILED])
                }
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Batch workflow failed: {str(e)}")
            raise WorkflowException(f"Batch workflow failed: {str(e)}")
    
    async def _execute_file_upload_step(
        self, file_path: str, config: Optional[Dict]
    ) -> Dict[str, Any]:
        """Execute file upload and validation step."""
        try:
            with performance_monitor.measure_operation("file_upload"):
                # Validate file exists and is readable
                if not Path(file_path).exists():
                    raise FileProcessingException(f"File not found: {file_path}")
                
                # Generate file ID
                file_id = str(uuid.uuid4())
                
                # Parse and validate MRIQC format
                df = self.mriqc_processor.parse_mriqc_file(file_path)
                validation_errors = self.mriqc_processor.validate_mriqc_format(df, file_path)
                validation_result = {
                    'is_valid': len(validation_errors) == 0,
                    'errors': [str(error) for error in validation_errors],
                    'subjects_count': len(df)
                }
                if not validation_result['is_valid']:
                    raise FileProcessingException(
                        f"Invalid MRIQC file format: {validation_result['errors']}"
                    )
                
                # Count subjects
                subjects_count = validation_result['subjects_count']
                
                # Log file upload
                security_auditor.log_data_access(
                    resource=file_path,
                    client_ip="system",
                    user_agent="workflow_orchestrator"
                )
                
                return {
                    'file_id': file_id,
                    'file_path': file_path,
                    'subjects_count': subjects_count,
                    'validation_result': validation_result
                }
                
        except Exception as e:
            logger.error(f"File upload step failed: {str(e)}")
            raise FileProcessingException(f"File upload failed: {str(e)}")
    
    async def _execute_processing_step(
        self, file_id: str, config: Optional[Dict]
    ) -> Dict[str, Any]:
        """Execute data processing step."""
        try:
            with performance_monitor.measure_operation("data_processing"):
                # Get file path from file_id (in real implementation, would be stored)
                # For now, we'll assume it's passed in config
                file_path = config.get('file_path') if config else None
                if not file_path:
                    raise FileProcessingException("File path not found for processing")
                
                # Process MRIQC data
                subjects = self.mriqc_processor.process_single_file(file_path)
                
                # Cache processed data
                cache_key = f"processed_subjects_{file_id}"
                cache_service.set(cache_key, subjects, ttl=3600)
                
                return {
                    'subjects': subjects,
                    'file_id': file_id
                }
                
        except Exception as e:
            logger.error(f"Data processing step failed: {str(e)}")
            raise FileProcessingException(f"Data processing failed: {str(e)}")
    
    async def _execute_quality_assessment_step(
        self, subjects: List[ProcessedSubject], config: Optional[Dict]
    ) -> Dict[str, Any]:
        """Execute quality assessment step."""
        try:
            with performance_monitor.measure_operation("quality_assessment"):
                assessed_subjects = []
                
                for subject in subjects:
                    # Apply quality assessment
                    assessment = self.quality_assessor.assess_quality(
                        subject.raw_metrics,
                        subject.subject_info
                    )
                    
                    # Update subject with assessment
                    subject.quality_assessment = assessment
                    assessed_subjects.append(subject)
                    
                    # Log quality decision
                    audit_logger.log_quality_decision(
                        subject_id=subject.subject_info.subject_id,
                        decision=assessment.overall_status.value,
                        reason=f"Automated assessment: {assessment.composite_score:.1f}% score",
                        automated=True,
                        confidence=assessment.confidence,
                        metrics=subject.raw_metrics.dict(exclude_none=True),
                        thresholds=getattr(assessment, 'threshold_violations', {})
                    )
                
                return {
                    'subjects': assessed_subjects
                }
                
        except Exception as e:
            logger.error(f"Quality assessment step failed: {str(e)}")
            raise QualityAssessmentException(f"Quality assessment failed: {str(e)}")
    
    async def _execute_normalization_step(
        self, subjects: List[ProcessedSubject], config: Optional[Dict]
    ) -> Dict[str, Any]:
        """Execute age normalization step."""
        try:
            with performance_monitor.measure_operation("age_normalization"):
                normalized_subjects = []
                
                for subject in subjects:
                    if subject.subject_info.age is not None:
                        # Apply age normalization
                        normalized_metrics = self.age_normalizer.normalize_metrics(
                            subject.raw_metrics,
                            subject.subject_info.age
                        )
                        subject.normalized_metrics = normalized_metrics
                    
                    normalized_subjects.append(subject)
                
                return {
                    'subjects': normalized_subjects
                }
                
        except Exception as e:
            logger.error(f"Age normalization step failed: {str(e)}")
            # Continue without normalization rather than failing
            return {
                'subjects': subjects
            }
    
    async def _execute_longitudinal_step(
        self, subjects: List[ProcessedSubject], config: Optional[Dict]
    ) -> Dict[str, Any]:
        """Execute longitudinal analysis step."""
        try:
            with performance_monitor.measure_operation("longitudinal_analysis"):
                # Group subjects by subject_id for longitudinal analysis
                subject_groups = {}
                for subject in subjects:
                    subject_id = subject.subject_info.subject_id
                    if subject_id not in subject_groups:
                        subject_groups[subject_id] = []
                    subject_groups[subject_id].append(subject)
                
                # Perform longitudinal analysis for subjects with multiple timepoints
                for subject_id, timepoints in subject_groups.items():
                    if len(timepoints) > 1:
                        longitudinal_analysis = self.longitudinal_service.analyze_timepoints(
                            timepoints
                        )
                        
                        # Add longitudinal analysis to each timepoint
                        for timepoint in timepoints:
                            timepoint.longitudinal_analysis = longitudinal_analysis
                
                return {
                    'subjects': subjects
                }
                
        except Exception as e:
            logger.error(f"Longitudinal analysis step failed: {str(e)}")
            # Continue without longitudinal analysis rather than failing
            return {
                'subjects': subjects
            }
    
    async def _execute_export_step(
        self, subjects: List[ProcessedSubject], config: Optional[Dict]
    ) -> Dict[str, Any]:
        """Execute export and reporting step."""
        try:
            with performance_monitor.measure_operation("export_generation"):
                export_files = {}
                
                # Generate CSV export
                csv_data = self.export_engine.export_subjects_csv(subjects)
                export_files['csv'] = csv_data
                
                # Generate PDF report
                pdf_data = self.export_engine.generate_pdf_report(subjects)
                export_files['pdf'] = pdf_data
                
                # Generate study summary
                summary_data = self.export_engine.generate_study_summary(subjects)
                export_files['summary'] = summary_data
                
                return {
                    'files': export_files
                }
                
        except Exception as e:
            logger.error(f"Export step failed: {str(e)}")
            raise ExportException(f"Export generation failed: {str(e)}")
    
    async def _update_workflow_progress(
        self,
        workflow_id: str,
        step: WorkflowStep,
        progress: int,
        callback: Optional[callable]
    ):
        """Update workflow progress and notify callback."""
        if workflow_id in self.active_workflows:
            workflow_state = self.active_workflows[workflow_id]
            workflow_state['current_step'] = step
            workflow_state['progress'] = progress
            workflow_state['steps_completed'].append(step)
            
            if callback:
                await callback({
                    'type': 'workflow_progress',
                    'workflow_id': workflow_id,
                    'step': step.value,
                    'progress': progress,
                    'timestamp': datetime.now().isoformat()
                })
    
    async def _handle_workflow_error(
        self,
        workflow_id: str,
        error: Exception,
        callback: Optional[callable]
    ) -> WorkflowResult:
        """Handle workflow errors and create error result."""
        logger.error(f"Workflow {workflow_id} failed: {str(error)}")
        
        # Update workflow state
        if workflow_id in self.active_workflows:
            workflow_state = self.active_workflows[workflow_id]
            workflow_state['status'] = WorkflowStatus.FAILED
            workflow_state['errors'].append(str(error))
            workflow_state['end_time'] = datetime.now()
        
        # Log error
        audit_logger.log_user_action(
            action_type="workflow_error",
            resource_type="workflow",
            resource_id=workflow_id,
            new_values={
                'error': str(error),
                'error_type': type(error).__name__
            }
        )
        
        # Notify callback
        if callback:
            await callback({
                'type': 'workflow_error',
                'workflow_id': workflow_id,
                'error': str(error),
                'timestamp': datetime.now().isoformat()
            })
        
        # Create error result
        error_result = WorkflowResult(
            workflow_id=workflow_id,
            status=WorkflowStatus.FAILED,
            subjects=[],
            summary=None,
            export_files={},
            processing_time=0,
            steps_completed=[],
            errors=[str(error)],
            metadata={'error_type': type(error).__name__}
        )
        
        # Archive failed workflow
        self._archive_workflow(workflow_id)
        
        return error_result
    
    def _generate_workflow_summary(self, subjects: List[ProcessedSubject]) -> StudySummary:
        """Generate summary statistics for workflow results."""
        if not subjects:
            return StudySummary(
                total_subjects=0,
                quality_distribution={},
                age_group_distribution={},
                metric_statistics={},
                exclusion_rate=0.0,
                processing_date=datetime.now()
            )
        
        # Calculate quality distribution
        quality_dist = {}
        for status in QualityStatus:
            quality_dist[status.value] = len([
                s for s in subjects 
                if s.quality_assessment and s.quality_assessment.overall_status == status
            ])
        
        # Calculate age group distribution
        age_dist = {}
        for group in AgeGroup:
            age_dist[group.value] = len([
                s for s in subjects 
                if s.normalized_metrics and s.normalized_metrics.age_group == group
            ])
        
        # Calculate exclusion rate
        failed_count = quality_dist.get(QualityStatus.FAIL.value, 0)
        exclusion_rate = (failed_count / len(subjects)) * 100 if subjects else 0
        
        return StudySummary(
            total_subjects=len(subjects),
            quality_distribution=quality_dist,
            age_group_distribution=age_dist,
            metric_statistics={},  # Would calculate detailed stats
            exclusion_rate=exclusion_rate,
            processing_date=datetime.now()
        )
    
    def _archive_workflow(self, workflow_id: str):
        """Archive completed workflow."""
        if workflow_id in self.active_workflows:
            workflow_state = self.active_workflows[workflow_id]
            self.workflow_history.append(workflow_state)
            del self.active_workflows[workflow_id]
    
    def get_workflow_status(self, workflow_id: str) -> Optional[Dict]:
        """Get current status of a workflow."""
        return self.active_workflows.get(workflow_id)
    
    def get_workflow_history(self) -> List[Dict]:
        """Get history of completed workflows."""
        return self.workflow_history.copy()
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get workflow performance metrics."""
        return {
            'active_workflows': len(self.active_workflows),
            'completed_workflows': len(self.workflow_history),
            'average_processing_times': {
                metric: sum(times) / len(times) if times else 0
                for metric, times in self.workflow_metrics.items()
            },
            'performance_monitor_stats': performance_monitor.get_performance_summary()
        }


# Global workflow orchestrator instance
workflow_orchestrator = WorkflowOrchestrator()