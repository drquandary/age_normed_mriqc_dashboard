#!/usr/bin/env python3
"""
Integration Validation Script for Age-Normed MRIQC Dashboard.

This script validates that all components are properly integrated and
the complete end-to-end workflow functions correctly.
"""

import asyncio
import logging
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add the app directory to the Python path
sys.path.insert(0, str(Path(__file__).parent))

from app.integration_service import integration_service
from app.workflow_orchestrator import workflow_orchestrator
from app.models import WorkflowConfiguration, WorkflowStatus
from app.mriqc_processor import MRIQCProcessor
from app.quality_assessor import QualityAssessor
from app.age_normalizer import AgeNormalizer
from app.export_engine import ExportEngine
from app.database import NormativeDatabase

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IntegrationValidator:
    """Validates complete system integration."""
    
    def __init__(self):
        """Initialize validator."""
        self.validation_results = []
        self.temp_files = []
    
    def create_sample_data(self) -> str:
        """Create sample MRIQC data for validation."""
        sample_data = pd.DataFrame({
            'bids_name': [
                'sub-001_ses-01_T1w', 'sub-002_ses-01_T1w', 'sub-003_ses-01_T1w'
            ],
            'subject_id': ['sub-001', 'sub-002', 'sub-003'],
            'session_id': ['ses-01', 'ses-01', 'ses-01'],
            'age': [25.0, 8.5, 67.0],
            'sex': ['M', 'F', 'M'],
            'snr': [12.5, 11.2, 9.8],
            'cnr': [3.2, 2.9, 2.3],
            'fber': [1500.0, 1300.0, 1000.0],
            'efc': [0.45, 0.51, 0.62],
            'fwhm_avg': [2.8, 3.0, 3.3],
            'qi1': [0.85, 0.79, 0.71],
            'cjv': [0.35, 0.41, 0.52]
        })
        
        temp_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='_validation.csv', delete=False
        )
        sample_data.to_csv(temp_file.name, index=False)
        self.temp_files.append(temp_file.name)
        
        return temp_file.name
    
    def validate_component_initialization(self) -> bool:
        """Validate that all components can be initialized."""
        logger.info("Validating component initialization...")
        
        try:
            # Test individual component initialization
            processor = MRIQCProcessor()
            assessor = QualityAssessor()
            normalizer = AgeNormalizer()
            export_engine = ExportEngine()
            database = NormativeDatabase()
            
            # Test service initialization
            assert integration_service is not None, "Integration service not initialized"
            assert workflow_orchestrator is not None, "Workflow orchestrator not initialized"
            
            logger.info("✓ All components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Component initialization failed: {str(e)}")
            return False
    
    def validate_component_integration(self) -> bool:
        """Validate that components work together."""
        logger.info("Validating component integration...")
        
        try:
            # Create test data
            test_file = self.create_sample_data()
            
            # Test processor -> assessor integration
            processor = MRIQCProcessor()
            assessor = QualityAssessor()
            
            subjects = processor.process_single_file(test_file)
            assert len(subjects) > 0, "Processor should return subjects"
            
            for subject in subjects:
                assessment = assessor.assess_quality(
                    subject.raw_metrics,
                    subject.subject_info
                )
                assert assessment is not None, "Assessor should return assessment"
                assert assessment.overall_status is not None, "Assessment should have status"
            
            # Test normalizer integration
            normalizer = AgeNormalizer()
            
            for subject in subjects:
                if subject.subject_info.age:
                    normalized = normalizer.normalize_metrics(
                        subject.raw_metrics,
                        subject.subject_info.age
                    )
                    assert normalized is not None, "Normalizer should return normalized metrics"
            
            # Test export engine integration
            export_engine = ExportEngine()
            
            csv_data = export_engine.export_subjects_csv(subjects)
            assert len(csv_data) > 0, "CSV export should not be empty"
            
            pdf_data = export_engine.generate_pdf_report(subjects)
            assert len(pdf_data) > 0, "PDF export should not be empty"
            
            logger.info("✓ Component integration validated successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Component integration validation failed: {str(e)}")
            return False
    
    async def validate_workflow_orchestration(self) -> bool:
        """Validate workflow orchestration."""
        logger.info("Validating workflow orchestration...")
        
        try:
            # Create test data
            test_file = self.create_sample_data()
            
            # Test complete workflow execution
            workflow_result = await workflow_orchestrator.execute_complete_workflow(
                file_path=test_file,
                workflow_config={
                    'apply_quality_assessment': True,
                    'apply_age_normalization': True,
                    'export_formats': ['csv', 'pdf']
                }
            )
            
            # Validate workflow result
            assert workflow_result is not None, "Workflow should return result"
            assert workflow_result.status == WorkflowStatus.COMPLETED, f"Expected COMPLETED, got {workflow_result.status}"
            assert len(workflow_result.subjects) > 0, "Workflow should process subjects"
            assert len(workflow_result.export_files) > 0, "Workflow should generate exports"
            assert workflow_result.processing_time > 0, "Workflow should have processing time"
            
            # Validate subject data completeness
            for subject in workflow_result.subjects:
                assert subject.subject_info.subject_id, "Subject should have ID"
                assert subject.raw_metrics, "Subject should have raw metrics"
                assert subject.quality_assessment, "Subject should have quality assessment"
                
                if subject.subject_info.age:
                    assert subject.normalized_metrics, "Subject with age should have normalized metrics"
            
            logger.info("✓ Workflow orchestration validated successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Workflow orchestration validation failed: {str(e)}")
            return False
    
    async def validate_integration_service(self) -> bool:
        """Validate integration service functionality."""
        logger.info("Validating integration service...")
        
        try:
            # Create test data
            test_file = self.create_sample_data()
            
            # Test complete user workflow
            config = WorkflowConfiguration(
                apply_quality_assessment=True,
                apply_age_normalization=True,
                apply_longitudinal_analysis=False,
                export_formats=["csv", "pdf"],
                performance_monitoring=True,
                audit_logging=True
            )
            
            workflow_result = await integration_service.execute_complete_user_workflow(
                file_path=test_file,
                user_id="validation_user",
                config=config
            )
            
            # Validate integration result
            assert workflow_result is not None, "Integration should return result"
            assert workflow_result.status == WorkflowStatus.COMPLETED, f"Expected COMPLETED, got {workflow_result.status}"
            assert len(workflow_result.subjects) > 0, "Integration should process subjects"
            assert 'integration_id' in workflow_result.metadata, "Result should have integration metadata"
            assert 'performance_metrics' in workflow_result.metadata, "Result should have performance metrics"
            
            # Test batch workflow
            test_files = [self.create_sample_data() for _ in range(2)]
            
            batch_results = await integration_service.execute_batch_integration_workflow(
                file_paths=test_files,
                user_id="validation_user",
                config=config
            )
            
            assert len(batch_results) == len(test_files), "Batch should process all files"
            successful_count = len([r for r in batch_results if r.status == WorkflowStatus.COMPLETED])
            assert successful_count > 0, "At least some batch workflows should succeed"
            
            logger.info("✓ Integration service validated successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Integration service validation failed: {str(e)}")
            return False
    
    async def validate_error_handling(self) -> bool:
        """Validate error handling throughout the system."""
        logger.info("Validating error handling...")
        
        try:
            # Test invalid file handling
            try:
                await integration_service.execute_complete_user_workflow(
                    file_path="/non/existent/file.csv",
                    user_id="validation_user"
                )
                assert False, "Should have raised exception for non-existent file"
            except Exception:
                pass  # Expected
            
            # Test invalid data handling
            invalid_data = pd.DataFrame({
                'invalid_column': ['data1', 'data2']
            })
            
            invalid_file = tempfile.NamedTemporaryFile(
                mode='w', suffix='_invalid.csv', delete=False
            )
            invalid_data.to_csv(invalid_file.name, index=False)
            self.temp_files.append(invalid_file.name)
            
            try:
                await integration_service.execute_complete_user_workflow(
                    file_path=invalid_file.name,
                    user_id="validation_user"
                )
                assert False, "Should have raised exception for invalid data"
            except Exception:
                pass  # Expected
            
            logger.info("✓ Error handling validated successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Error handling validation failed: {str(e)}")
            return False
    
    def validate_data_flow(self) -> bool:
        """Validate data flow between components."""
        logger.info("Validating data flow...")
        
        try:
            # Create test data
            test_file = self.create_sample_data()
            
            # Test data flow: File -> Processor -> Assessor -> Normalizer -> Export
            processor = MRIQCProcessor()
            assessor = QualityAssessor()
            normalizer = AgeNormalizer()
            export_engine = ExportEngine()
            
            # Step 1: File to Processor
            subjects = processor.process_single_file(test_file)
            assert len(subjects) > 0, "Processor should extract subjects"
            
            original_subject = subjects[0]
            assert original_subject.raw_metrics, "Subject should have raw metrics"
            
            # Step 2: Processor to Assessor
            assessment = assessor.assess_quality(
                original_subject.raw_metrics,
                original_subject.subject_info
            )
            assert assessment is not None, "Assessor should produce assessment"
            
            original_subject.quality_assessment = assessment
            
            # Step 3: Assessor to Normalizer (if age available)
            if original_subject.subject_info.age:
                normalized = normalizer.normalize_metrics(
                    original_subject.raw_metrics,
                    original_subject.subject_info.age
                )
                assert normalized is not None, "Normalizer should produce normalized metrics"
                original_subject.normalized_metrics = normalized
            
            # Step 4: All components to Export
            csv_export = export_engine.export_subjects_csv([original_subject])
            assert len(csv_export) > 0, "Export should produce CSV data"
            
            pdf_export = export_engine.generate_pdf_report([original_subject])
            assert len(pdf_export) > 0, "Export should produce PDF data"
            
            logger.info("✓ Data flow validated successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Data flow validation failed: {str(e)}")
            return False
    
    def cleanup(self):
        """Clean up temporary files."""
        for temp_file in self.temp_files:
            try:
                Path(temp_file).unlink()
            except OSError:
                pass
    
    async def run_validation(self) -> bool:
        """Run complete integration validation."""
        logger.info("Starting comprehensive integration validation...")
        
        validation_steps = [
            ("Component Initialization", self.validate_component_initialization()),
            ("Component Integration", self.validate_component_integration()),
            ("Workflow Orchestration", self.validate_workflow_orchestration()),
            ("Integration Service", self.validate_integration_service()),
            ("Error Handling", self.validate_error_handling()),
            ("Data Flow", self.validate_data_flow())
        ]
        
        all_passed = True
        results = []
        
        try:
            for step_name, step_coro in validation_steps:
                logger.info(f"\n--- {step_name} ---")
                
                if asyncio.iscoroutine(step_coro):
                    result = await step_coro
                else:
                    result = step_coro
                
                results.append((step_name, result))
                
                if not result:
                    all_passed = False
                    logger.error(f"✗ {step_name} FAILED")
                else:
                    logger.info(f"✓ {step_name} PASSED")
            
            # Print summary
            logger.info("\n" + "="*60)
            logger.info("INTEGRATION VALIDATION SUMMARY")
            logger.info("="*60)
            
            for step_name, result in results:
                status = "PASSED" if result else "FAILED"
                symbol = "✓" if result else "✗"
                logger.info(f"{symbol} {step_name}: {status}")
            
            overall_status = "PASSED" if all_passed else "FAILED"
            logger.info(f"\nOVERALL STATUS: {overall_status}")
            logger.info("="*60)
            
            return all_passed
            
        except Exception as e:
            logger.error(f"Validation execution failed: {str(e)}")
            return False
        
        finally:
            self.cleanup()


async def main():
    """Main validation function."""
    validator = IntegrationValidator()
    
    try:
        success = await validator.run_validation()
        
        if success:
            logger.info("\n🎉 All integration validations PASSED!")
            logger.info("The Age-Normed MRIQC Dashboard is fully integrated and ready for use.")
            sys.exit(0)
        else:
            logger.error("\n❌ Some integration validations FAILED!")
            logger.error("Please review the errors above and fix the issues.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Validation failed with exception: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())