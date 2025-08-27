"""
Integration Service for Age-Normed MRIQC Dashboard.

This module provides comprehensive integration between all components,
ensuring proper error propagation, user feedback, and end-to-end workflow
coordination.
"""

import asyncio
import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path

from .workflow_orchestrator import workflow_orchestrator
from .models import (
    WorkflowResult, WorkflowProgress, WorkflowConfiguration, 
    EndToEndTestResult, ProcessedSubject, WorkflowStatus
)
from .error_handling import error_handler, audit_logger
from .security import security_auditor
from .performance_monitor import performance_monitor
from .cache_service import cache_service
from .exceptions import IntegrationException, WorkflowException

logger = logging.getLogger(__name__)


class IntegrationService:
    """
    Service for managing end-to-end integration and workflow coordination.
    
    This service ensures all components work together seamlessly and provides
    comprehensive error handling, user feedback, and workflow management.
    """
    
    def __init__(self):
        """Initialize integration service."""
        self.active_integrations: Dict[str, Dict] = {}
        self.integration_history: List[Dict] = []
        self.error_handlers: Dict[str, Callable] = {}
        self.progress_callbacks: Dict[str, List[Callable]] = {}
        
        # Register default error handlers
        self._register_default_error_handlers()
    
    async def execute_complete_user_workflow(
        self,
        file_path: str,
        user_id: Optional[str] = None,
        config: Optional[WorkflowConfiguration] = None,
        progress_callback: Optional[Callable] = None
    ) -> WorkflowResult:
        """
        Execute complete user workflow with full integration.
        
        This method orchestrates the entire user journey from file upload
        through quality assessment to final export and reporting.
        
        Args:
            file_path: Path to MRIQC CSV file
            user_id: Optional user identifier for audit logging
            config: Workflow configuration options
            progress_callback: Optional callback for progress updates
            
        Returns:
            WorkflowResult with complete processing results
        """
        integration_id = f"integration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Initialize integration tracking
            integration_state = {
                'integration_id': integration_id,
                'user_id': user_id,
                'file_path': file_path,
                'config': config.dict() if config else {},
                'start_time': datetime.now(),
                'status': 'initializing',
                'components_involved': [],
                'errors': [],
                'warnings': []
            }
            self.active_integrations[integration_id] = integration_state
            
            # Log integration start
            audit_logger.log_user_action(
                action_type="integration_workflow_start",
                resource_type="integration",
                resource_id=integration_id,
                user_id=user_id,
                new_values={
                    'file_path': file_path,
                    'config': integration_state['config']
                }
            )
            
            # Create enhanced progress callback
            enhanced_callback = self._create_enhanced_progress_callback(
                integration_id, progress_callback
            )
            
            # Execute workflow with full integration
            workflow_config = self._prepare_workflow_config(config)
            
            integration_state['status'] = 'executing_workflow'
            integration_state['components_involved'].append('workflow_orchestrator')
            
            workflow_result = await workflow_orchestrator.execute_complete_workflow(
                file_path=file_path,
                workflow_config=workflow_config,
                progress_callback=enhanced_callback
            )
            
            # Post-process workflow result
            enhanced_result = await self._post_process_workflow_result(
                workflow_result, integration_state
            )
            
            # Validate integration completeness
            validation_result = await self._validate_integration_completeness(
                enhanced_result, integration_state
            )
            
            if not validation_result['is_complete']:
                logger.warning(f"Integration {integration_id} incomplete: {validation_result['issues']}")
                integration_state['warnings'].extend(validation_result['issues'])
            
            # Finalize integration
            integration_state['status'] = 'completed'
            integration_state['end_time'] = datetime.now()
            integration_state['result'] = enhanced_result
            
            # Log integration completion
            audit_logger.log_user_action(
                action_type="integration_workflow_complete",
                resource_type="integration",
                resource_id=integration_id,
                user_id=user_id,
                new_values={
                    'status': 'completed',
                    'subjects_processed': len(enhanced_result.subjects),
                    'processing_time': enhanced_result.processing_time,
                    'components_involved': integration_state['components_involved']
                }
            )
            
            # Archive integration
            self._archive_integration(integration_id)
            
            return enhanced_result
            
        except Exception as e:
            # Handle integration failure with comprehensive error handling
            return await self._handle_integration_error(
                integration_id, e, integration_state
            )
    
    async def execute_batch_integration_workflow(
        self,
        file_paths: List[str],
        user_id: Optional[str] = None,
        config: Optional[WorkflowConfiguration] = None,
        progress_callback: Optional[Callable] = None
    ) -> List[WorkflowResult]:
        """
        Execute batch integration workflow for multiple files.
        
        Args:
            file_paths: List of MRIQC CSV file paths
            user_id: Optional user identifier
            config: Workflow configuration options
            progress_callback: Optional callback for progress updates
            
        Returns:
            List of WorkflowResult objects
        """
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Log batch start
            audit_logger.log_user_action(
                action_type="batch_integration_start",
                resource_type="batch_integration",
                resource_id=batch_id,
                user_id=user_id,
                new_values={
                    'file_count': len(file_paths),
                    'config': config.dict() if config else {}
                }
            )
            
            results = []
            
            # Process each file with full integration
            for i, file_path in enumerate(file_paths):
                try:
                    # Create file-specific progress callback
                    file_callback = self._create_batch_progress_callback(
                        batch_id, i, len(file_paths), progress_callback
                    )
                    
                    # Execute individual integration
                    result = await self.execute_complete_user_workflow(
                        file_path=file_path,
                        user_id=user_id,
                        config=config,
                        progress_callback=file_callback
                    )
                    
                    results.append(result)
                    
                except Exception as e:
                    logger.error(f"Failed to process file {file_path} in batch {batch_id}: {str(e)}")
                    
                    # Create error result
                    error_result = WorkflowResult(
                        workflow_id=f"error_{i}",
                        status=WorkflowStatus.FAILED,
                        subjects=[],
                        summary=None,
                        export_files={},
                        processing_time=0,
                        steps_completed=[],
                        errors=[str(e)],
                        metadata={
                            'file_path': file_path,
                            'batch_id': batch_id,
                            'error_type': type(e).__name__
                        }
                    )
                    results.append(error_result)
            
            # Log batch completion
            successful_count = len([r for r in results if r.status == WorkflowStatus.COMPLETED])
            failed_count = len([r for r in results if r.status == WorkflowStatus.FAILED])
            
            audit_logger.log_user_action(
                action_type="batch_integration_complete",
                resource_type="batch_integration",
                resource_id=batch_id,
                user_id=user_id,
                new_values={
                    'total_files': len(file_paths),
                    'successful_files': successful_count,
                    'failed_files': failed_count
                }
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Batch integration {batch_id} failed: {str(e)}")
            raise IntegrationException(f"Batch integration failed: {str(e)}")
    
    async def run_end_to_end_tests(
        self,
        test_data_paths: List[str],
        test_config: Optional[Dict] = None
    ) -> EndToEndTestResult:
        """
        Run comprehensive end-to-end integration tests.
        
        Args:
            test_data_paths: List of test MRIQC files
            test_config: Test configuration options
            
        Returns:
            EndToEndTestResult with test results and metrics
        """
        test_id = f"e2e_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.now()
        
        try:
            test_result = EndToEndTestResult(
                test_id=test_id,
                test_name="Complete End-to-End Integration Test",
                status="running"
            )
            
            # Test individual workflows
            workflow_results = []
            assertions_passed = 0
            assertions_failed = 0
            
            for i, test_file in enumerate(test_data_paths):
                try:
                    # Execute workflow
                    workflow_result = await self.execute_complete_user_workflow(
                        file_path=test_file,
                        user_id="test_user",
                        config=WorkflowConfiguration(**(test_config or {}))
                    )
                    
                    workflow_results.append(workflow_result)
                    
                    # Validate workflow result
                    validation_results = self._validate_workflow_result(workflow_result)
                    assertions_passed += validation_results['passed']
                    assertions_failed += validation_results['failed']
                    
                    if validation_results['failed'] > 0:
                        test_result.error_messages.extend(validation_results['errors'])
                    
                except Exception as e:
                    assertions_failed += 1
                    test_result.error_messages.append(f"Test file {test_file} failed: {str(e)}")
            
            # Test batch processing
            try:
                batch_results = await self.execute_batch_integration_workflow(
                    file_paths=test_data_paths,
                    user_id="test_user",
                    config=WorkflowConfiguration(**(test_config or {}))
                )
                
                # Validate batch results
                batch_validation = self._validate_batch_results(batch_results)
                assertions_passed += batch_validation['passed']
                assertions_failed += batch_validation['failed']
                
                if batch_validation['failed'] > 0:
                    test_result.error_messages.extend(batch_validation['errors'])
                
            except Exception as e:
                assertions_failed += 1
                test_result.error_messages.append(f"Batch test failed: {str(e)}")
            
            # Finalize test result
            end_time = datetime.now()
            test_result.status = "passed" if assertions_failed == 0 else "failed"
            test_result.workflow_results = workflow_results
            test_result.assertions_passed = assertions_passed
            test_result.assertions_failed = assertions_failed
            test_result.execution_time = (end_time - start_time).total_seconds()
            test_result.test_data = {
                'test_files': test_data_paths,
                'config': test_config or {}
            }
            test_result.performance_metrics = performance_monitor.get_performance_summary()
            
            # Log test completion
            audit_logger.log_user_action(
                action_type="end_to_end_test_complete",
                resource_type="integration_test",
                resource_id=test_id,
                new_values={
                    'status': test_result.status,
                    'assertions_passed': assertions_passed,
                    'assertions_failed': assertions_failed,
                    'execution_time': test_result.execution_time
                }
            )
            
            return test_result
            
        except Exception as e:
            logger.error(f"End-to-end test {test_id} failed: {str(e)}")
            
            # Return failed test result
            end_time = datetime.now()
            return EndToEndTestResult(
                test_id=test_id,
                test_name="Complete End-to-End Integration Test",
                status="error",
                execution_time=(end_time - start_time).total_seconds(),
                error_messages=[str(e)],
                test_data={'test_files': test_data_paths, 'config': test_config or {}}
            )
    
    def _register_default_error_handlers(self):
        """Register default error handlers for different component types."""
        
        self.error_handlers['file_processing'] = self._handle_file_processing_error
        self.error_handlers['quality_assessment'] = self._handle_quality_assessment_error
        self.error_handlers['age_normalization'] = self._handle_age_normalization_error
        self.error_handlers['export_generation'] = self._handle_export_generation_error
        self.error_handlers['database'] = self._handle_database_error
        self.error_handlers['security'] = self._handle_security_error
        self.error_handlers['performance'] = self._handle_performance_error
    
    def _create_enhanced_progress_callback(
        self,
        integration_id: str,
        user_callback: Optional[Callable]
    ) -> Callable:
        """Create enhanced progress callback with integration tracking."""
        
        async def enhanced_callback(progress_data: Dict):
            # Update integration state
            if integration_id in self.active_integrations:
                integration_state = self.active_integrations[integration_id]
                integration_state['last_progress'] = progress_data
                integration_state['last_update'] = datetime.now()
            
            # Log progress for audit
            audit_logger.log_user_action(
                action_type="workflow_progress",
                resource_type="integration",
                resource_id=integration_id,
                new_values=progress_data
            )
            
            # Call user callback if provided
            if user_callback:
                try:
                    await user_callback(progress_data)
                except Exception as e:
                    logger.warning(f"User progress callback failed: {str(e)}")
        
        return enhanced_callback
    
    def _create_batch_progress_callback(
        self,
        batch_id: str,
        file_index: int,
        total_files: int,
        user_callback: Optional[Callable]
    ) -> Callable:
        """Create batch-specific progress callback."""
        
        async def batch_callback(progress_data: Dict):
            # Enhance progress data with batch information
            enhanced_data = {
                **progress_data,
                'batch_id': batch_id,
                'file_index': file_index,
                'total_files': total_files,
                'batch_progress': (file_index / total_files) * 100
            }
            
            # Call user callback if provided
            if user_callback:
                try:
                    await user_callback(enhanced_data)
                except Exception as e:
                    logger.warning(f"Batch progress callback failed: {str(e)}")
        
        return batch_callback
    
    def _prepare_workflow_config(
        self, config: Optional[WorkflowConfiguration]
    ) -> Dict[str, Any]:
        """Prepare workflow configuration for orchestrator."""
        if not config:
            config = WorkflowConfiguration()
        
        return {
            'apply_quality_assessment': config.apply_quality_assessment,
            'apply_age_normalization': config.apply_age_normalization,
            'apply_longitudinal_analysis': config.apply_longitudinal_analysis,
            'custom_thresholds': config.custom_thresholds,
            'normative_dataset': config.normative_dataset,
            'export_formats': config.export_formats,
            'cache_results': config.cache_results,
            'cache_ttl': config.cache_ttl,
            'performance_monitoring': config.performance_monitoring,
            'audit_logging': config.audit_logging
        }
    
    async def _post_process_workflow_result(
        self,
        workflow_result: WorkflowResult,
        integration_state: Dict
    ) -> WorkflowResult:
        """Post-process workflow result with integration enhancements."""
        
        # Add integration metadata
        workflow_result.metadata.update({
            'integration_id': integration_state['integration_id'],
            'components_involved': integration_state['components_involved'],
            'integration_warnings': integration_state.get('warnings', [])
        })
        
        # Validate data integrity
        integrity_check = await self._validate_data_integrity(workflow_result)
        if not integrity_check['valid']:
            workflow_result.errors.extend(integrity_check['errors'])
            integration_state['warnings'].extend(integrity_check['warnings'])
        
        # Add performance metrics
        workflow_result.metadata['performance_metrics'] = performance_monitor.get_performance_summary()
        
        return workflow_result
    
    async def _validate_integration_completeness(
        self,
        workflow_result: WorkflowResult,
        integration_state: Dict
    ) -> Dict[str, Any]:
        """Validate that integration completed all expected steps."""
        
        issues = []
        expected_components = [
            'workflow_orchestrator', 'mriqc_processor', 'quality_assessor'
        ]
        
        # Check if all expected components were involved
        involved_components = integration_state.get('components_involved', [])
        for component in expected_components:
            if component not in involved_components:
                issues.append(f"Component {component} was not involved in integration")
        
        # Check workflow result completeness
        if not workflow_result.subjects:
            issues.append("No subjects were processed")
        
        if not workflow_result.export_files:
            issues.append("No export files were generated")
        
        # Check for required metadata
        required_metadata = ['integration_id', 'performance_metrics']
        for key in required_metadata:
            if key not in workflow_result.metadata:
                issues.append(f"Missing required metadata: {key}")
        
        return {
            'is_complete': len(issues) == 0,
            'issues': issues
        }
    
    async def _validate_data_integrity(self, workflow_result: WorkflowResult) -> Dict[str, Any]:
        """Validate data integrity across all processed subjects."""
        
        errors = []
        warnings = []
        
        for subject in workflow_result.subjects:
            # Check required fields
            if not subject.subject_info.subject_id:
                errors.append(f"Subject missing ID")
            
            if not subject.raw_metrics:
                errors.append(f"Subject {subject.subject_info.subject_id} missing raw metrics")
            
            # Check quality assessment
            if not subject.quality_assessment:
                warnings.append(f"Subject {subject.subject_info.subject_id} missing quality assessment")
            
            # Check age normalization if age is available
            if subject.subject_info.age and not subject.normalized_metrics:
                warnings.append(f"Subject {subject.subject_info.subject_id} missing age normalization")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
    
    def _validate_workflow_result(self, workflow_result: WorkflowResult) -> Dict[str, Any]:
        """Validate individual workflow result for testing."""
        
        passed = 0
        failed = 0
        errors = []
        
        # Test 1: Workflow completed successfully
        if workflow_result.status == WorkflowStatus.COMPLETED:
            passed += 1
        else:
            failed += 1
            errors.append(f"Workflow status is {workflow_result.status}, expected COMPLETED")
        
        # Test 2: Subjects were processed
        if len(workflow_result.subjects) > 0:
            passed += 1
        else:
            failed += 1
            errors.append("No subjects were processed")
        
        # Test 3: Export files were generated
        if len(workflow_result.export_files) > 0:
            passed += 1
        else:
            failed += 1
            errors.append("No export files were generated")
        
        # Test 4: Processing time is reasonable
        if 0 < workflow_result.processing_time < 300:  # Less than 5 minutes
            passed += 1
        else:
            failed += 1
            errors.append(f"Processing time {workflow_result.processing_time}s is unreasonable")
        
        return {
            'passed': passed,
            'failed': failed,
            'errors': errors
        }
    
    def _validate_batch_results(self, batch_results: List[WorkflowResult]) -> Dict[str, Any]:
        """Validate batch processing results for testing."""
        
        passed = 0
        failed = 0
        errors = []
        
        # Test 1: All files were processed
        if len(batch_results) > 0:
            passed += 1
        else:
            failed += 1
            errors.append("No batch results returned")
        
        # Test 2: At least some workflows succeeded
        successful_count = len([r for r in batch_results if r.status == WorkflowStatus.COMPLETED])
        if successful_count > 0:
            passed += 1
        else:
            failed += 1
            errors.append("No workflows in batch succeeded")
        
        return {
            'passed': passed,
            'failed': failed,
            'errors': errors
        }
    
    async def _handle_integration_error(
        self,
        integration_id: str,
        error: Exception,
        integration_state: Dict
    ) -> WorkflowResult:
        """Handle integration errors with comprehensive error handling."""
        
        logger.error(f"Integration {integration_id} failed: {str(error)}")
        
        # Update integration state
        integration_state['status'] = 'failed'
        integration_state['end_time'] = datetime.now()
        integration_state['errors'].append(str(error))
        
        # Use appropriate error handler
        error_type = type(error).__name__.lower()
        handler_key = None
        
        for key in self.error_handlers.keys():
            if key in error_type:
                handler_key = key
                break
        
        if handler_key and handler_key in self.error_handlers:
            try:
                await self.error_handlers[handler_key](error, integration_state)
            except Exception as handler_error:
                logger.error(f"Error handler {handler_key} failed: {str(handler_error)}")
        
        # Log integration failure
        audit_logger.log_user_action(
            action_type="integration_workflow_failed",
            resource_type="integration",
            resource_id=integration_id,
            new_values={
                'error': str(error),
                'error_type': type(error).__name__,
                'components_involved': integration_state.get('components_involved', [])
            }
        )
        
        # Create error result
        error_result = WorkflowResult(
            workflow_id=integration_id,
            status=WorkflowStatus.FAILED,
            subjects=[],
            summary=None,
            export_files={},
            processing_time=0,
            steps_completed=[],
            errors=[str(error)],
            metadata={
                'integration_id': integration_id,
                'error_type': type(error).__name__,
                'components_involved': integration_state.get('components_involved', [])
            }
        )
        
        # Archive failed integration
        self._archive_integration(integration_id)
        
        return error_result
    
    # Error handler methods
    async def _handle_file_processing_error(self, error: Exception, state: Dict):
        """Handle file processing errors."""
        logger.error(f"File processing error in integration {state['integration_id']}: {str(error)}")
        # Could implement file recovery, alternative processing, etc.
    
    async def _handle_quality_assessment_error(self, error: Exception, state: Dict):
        """Handle quality assessment errors."""
        logger.error(f"Quality assessment error in integration {state['integration_id']}: {str(error)}")
        # Could implement fallback assessment methods
    
    async def _handle_age_normalization_error(self, error: Exception, state: Dict):
        """Handle age normalization errors."""
        logger.error(f"Age normalization error in integration {state['integration_id']}: {str(error)}")
        # Could implement alternative normalization approaches
    
    async def _handle_export_generation_error(self, error: Exception, state: Dict):
        """Handle export generation errors."""
        logger.error(f"Export generation error in integration {state['integration_id']}: {str(error)}")
        # Could implement alternative export formats
    
    async def _handle_database_error(self, error: Exception, state: Dict):
        """Handle database errors."""
        logger.error(f"Database error in integration {state['integration_id']}: {str(error)}")
        # Could implement database recovery, caching fallbacks
    
    async def _handle_security_error(self, error: Exception, state: Dict):
        """Handle security errors."""
        logger.error(f"Security error in integration {state['integration_id']}: {str(error)}")
        security_auditor.log_security_event(
            'integration_security_error',
            {'integration_id': state['integration_id'], 'error': str(error)},
            'HIGH'
        )
    
    async def _handle_performance_error(self, error: Exception, state: Dict):
        """Handle performance-related errors."""
        logger.error(f"Performance error in integration {state['integration_id']}: {str(error)}")
        # Could implement performance optimization, resource management
    
    def _archive_integration(self, integration_id: str):
        """Archive completed integration."""
        if integration_id in self.active_integrations:
            integration_state = self.active_integrations[integration_id]
            self.integration_history.append(integration_state)
            del self.active_integrations[integration_id]
    
    def get_integration_status(self, integration_id: str) -> Optional[Dict]:
        """Get current status of an integration."""
        return self.active_integrations.get(integration_id)
    
    def get_integration_history(self) -> List[Dict]:
        """Get history of completed integrations."""
        return self.integration_history.copy()


# Global integration service instance
integration_service = IntegrationService()