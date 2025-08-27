#!/usr/bin/env python3
"""
Comprehensive integration test runner for Age-Normed MRIQC Dashboard.

This script runs complete end-to-end integration tests covering all
workflows from file upload through quality assessment to export and reporting.
"""

import asyncio
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
import pytest
from fastapi.testclient import TestClient

# Add the app directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.main import app
from app.integration_service import integration_service
from app.workflow_orchestrator import workflow_orchestrator
from app.models import WorkflowConfiguration, WorkflowStatus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IntegrationTestRunner:
    """Comprehensive integration test runner."""
    
    def __init__(self):
        """Initialize test runner."""
        self.client = TestClient(app)
        self.test_results = []
        self.temp_files = []
        
    def create_test_data(self) -> List[str]:
        """Create test MRIQC data files."""
        test_files = []
        
        # Test data 1: Mixed age groups with good quality
        data1 = pd.DataFrame({
            'bids_name': [
                'sub-001_ses-01_T1w', 'sub-002_ses-01_T1w', 'sub-003_ses-01_T1w',
                'sub-004_ses-01_T1w', 'sub-005_ses-01_T1w'
            ],
            'subject_id': ['sub-001', 'sub-002', 'sub-003', 'sub-004', 'sub-005'],
            'session_id': ['ses-01', 'ses-01', 'ses-01', 'ses-01', 'ses-01'],
            'age': [8.5, 25.0, 45.0, 67.0, 15.5],
            'sex': ['F', 'M', 'F', 'M', 'F'],
            'snr': [11.2, 12.5, 11.8, 9.2, 13.1],
            'cnr': [2.8, 3.2, 3.0, 2.1, 3.5],
            'fber': [1200.0, 1500.0, 1400.0, 900.0, 1600.0],
            'efc': [0.52, 0.45, 0.48, 0.68, 0.42],
            'fwhm_avg': [3.1, 2.8, 2.9, 3.5, 2.7],
            'qi1': [0.78, 0.85, 0.82, 0.65, 0.88],
            'cjv': [0.42, 0.35, 0.38, 0.58, 0.32]
        })
        
        # Test data 2: Longitudinal subjects
        data2 = pd.DataFrame({
            'bids_name': [
                'sub-101_ses-01_T1w', 'sub-101_ses-02_T1w', 'sub-101_ses-03_T1w',
                'sub-102_ses-01_T1w', 'sub-102_ses-02_T1w'
            ],
            'subject_id': ['sub-101', 'sub-101', 'sub-101', 'sub-102', 'sub-102'],
            'session_id': ['ses-01', 'ses-02', 'ses-03', 'ses-01', 'ses-02'],
            'age': [30.0, 30.5, 31.0, 25.0, 25.5],
            'sex': ['M', 'M', 'M', 'F', 'F'],
            'snr': [12.0, 12.2, 11.8, 13.5, 13.2],
            'cnr': [3.1, 3.2, 3.0, 3.6, 3.4],
            'fber': [1450.0, 1480.0, 1420.0, 1650.0, 1620.0],
            'efc': [0.46, 0.45, 0.47, 0.40, 0.41],
            'fwhm_avg': [2.9, 2.8, 3.0, 2.6, 2.7],
            'qi1': [0.83, 0.84, 0.82, 0.89, 0.87],
            'cjv': [0.37, 0.36, 0.38, 0.30, 0.31]
        })
        
        # Test data 3: Poor quality data
        data3 = pd.DataFrame({
            'bids_name': ['sub-201_ses-01_T1w', 'sub-202_ses-01_T1w'],
            'subject_id': ['sub-201', 'sub-202'],
            'session_id': ['ses-01', 'ses-01'],
            'age': [35.0, 28.0],
            'sex': ['M', 'F'],
            'snr': [6.5, 7.2],  # Low SNR
            'cnr': [1.5, 1.8],  # Low CNR
            'fber': [600.0, 700.0],  # Low FBER
            'efc': [0.85, 0.82],  # High EFC (bad)
            'fwhm_avg': [4.5, 4.2],  # High FWHM (bad)
            'qi1': [0.45, 0.52],  # Low QI1
            'cjv': [0.75, 0.68]  # High CJV (bad)
        })
        
        # Save test data to temporary files
        for i, data in enumerate([data1, data2, data3], 1):
            temp_file = tempfile.NamedTemporaryFile(
                mode='w', suffix=f'_test_{i}.csv', delete=False
            )
            data.to_csv(temp_file.name, index=False)
            test_files.append(temp_file.name)
            self.temp_files.append(temp_file.name)
        
        return test_files
    
    def cleanup_test_data(self):
        """Clean up temporary test files."""
        for temp_file in self.temp_files:
            try:
                os.unlink(temp_file)
            except OSError:
                pass
    
    async def test_complete_workflow_integration(self) -> Dict[str, Any]:
        """Test complete workflow integration."""
        logger.info("Testing complete workflow integration...")
        
        test_result = {
            'test_name': 'complete_workflow_integration',
            'status': 'running',
            'start_time': datetime.now(),
            'assertions': [],
            'errors': []
        }
        
        try:
            # Create test data
            test_files = self.create_test_data()
            test_file = test_files[0]  # Use first test file
            
            # Test 1: Execute complete workflow via integration service
            logger.info("Executing complete workflow via integration service...")
            
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
                user_id="test_user",
                config=config
            )
            
            # Validate workflow result
            assert workflow_result is not None, "Workflow result should not be None"
            test_result['assertions'].append("Workflow result is not None")
            
            assert workflow_result.status == WorkflowStatus.COMPLETED, f"Expected COMPLETED, got {workflow_result.status}"
            test_result['assertions'].append(f"Workflow status is {workflow_result.status}")
            
            assert len(workflow_result.subjects) > 0, "Should have processed subjects"
            test_result['assertions'].append(f"Processed {len(workflow_result.subjects)} subjects")
            
            assert len(workflow_result.export_files) > 0, "Should have export files"
            test_result['assertions'].append(f"Generated {len(workflow_result.export_files)} export files")
            
            assert workflow_result.processing_time > 0, "Should have positive processing time"
            test_result['assertions'].append(f"Processing time: {workflow_result.processing_time:.2f}s")
            
            # Test 2: Validate subject data integrity
            logger.info("Validating subject data integrity...")
            
            for i, subject in enumerate(workflow_result.subjects):
                assert subject.subject_info.subject_id, f"Subject {i} missing ID"
                assert subject.raw_metrics, f"Subject {i} missing raw metrics"
                assert subject.quality_assessment, f"Subject {i} missing quality assessment"
                
                if subject.subject_info.age:
                    assert subject.normalized_metrics, f"Subject {i} with age missing normalized metrics"
            
            test_result['assertions'].append("All subjects have required data")
            
            # Test 3: Validate export files
            logger.info("Validating export files...")
            
            assert 'csv' in workflow_result.export_files, "CSV export missing"
            assert 'pdf' in workflow_result.export_files, "PDF export missing"
            
            csv_data = workflow_result.export_files['csv']
            assert len(csv_data) > 0, "CSV export should not be empty"
            
            pdf_data = workflow_result.export_files['pdf']
            assert len(pdf_data) > 0, "PDF export should not be empty"
            
            test_result['assertions'].append("Export files validated")
            
            # Test 4: Validate summary statistics
            logger.info("Validating summary statistics...")
            
            if workflow_result.summary:
                assert workflow_result.summary.total_subjects == len(workflow_result.subjects), "Summary subject count mismatch"
                assert 'quality_distribution' in workflow_result.summary.dict(), "Missing quality distribution"
                assert 'age_group_distribution' in workflow_result.summary.dict(), "Missing age group distribution"
            
            test_result['assertions'].append("Summary statistics validated")
            
            test_result['status'] = 'passed'
            
        except Exception as e:
            logger.error(f"Complete workflow integration test failed: {str(e)}")
            test_result['status'] = 'failed'
            test_result['errors'].append(str(e))
        
        test_result['end_time'] = datetime.now()
        test_result['duration'] = (test_result['end_time'] - test_result['start_time']).total_seconds()
        
        return test_result
    
    async def test_batch_workflow_integration(self) -> Dict[str, Any]:
        """Test batch workflow integration."""
        logger.info("Testing batch workflow integration...")
        
        test_result = {
            'test_name': 'batch_workflow_integration',
            'status': 'running',
            'start_time': datetime.now(),
            'assertions': [],
            'errors': []
        }
        
        try:
            # Create test data
            test_files = self.create_test_data()
            
            # Execute batch workflow
            logger.info(f"Executing batch workflow with {len(test_files)} files...")
            
            config = WorkflowConfiguration(
                apply_quality_assessment=True,
                apply_age_normalization=True,
                export_formats=["csv"]
            )
            
            batch_results = await integration_service.execute_batch_integration_workflow(
                file_paths=test_files,
                user_id="test_user",
                config=config
            )
            
            # Validate batch results
            assert len(batch_results) == len(test_files), f"Expected {len(test_files)} results, got {len(batch_results)}"
            test_result['assertions'].append(f"Processed {len(batch_results)} files in batch")
            
            successful_count = len([r for r in batch_results if r.status == WorkflowStatus.COMPLETED])
            assert successful_count > 0, "At least some workflows should succeed"
            test_result['assertions'].append(f"{successful_count} workflows succeeded")
            
            # Validate individual results
            total_subjects = 0
            for result in batch_results:
                if result.status == WorkflowStatus.COMPLETED:
                    assert len(result.subjects) > 0, "Successful workflow should have subjects"
                    total_subjects += len(result.subjects)
            
            test_result['assertions'].append(f"Total subjects processed: {total_subjects}")
            
            test_result['status'] = 'passed'
            
        except Exception as e:
            logger.error(f"Batch workflow integration test failed: {str(e)}")
            test_result['status'] = 'failed'
            test_result['errors'].append(str(e))
        
        test_result['end_time'] = datetime.now()
        test_result['duration'] = (test_result['end_time'] - test_result['start_time']).total_seconds()
        
        return test_result
    
    def test_api_endpoints_integration(self) -> Dict[str, Any]:
        """Test API endpoints integration."""
        logger.info("Testing API endpoints integration...")
        
        test_result = {
            'test_name': 'api_endpoints_integration',
            'status': 'running',
            'start_time': datetime.now(),
            'assertions': [],
            'errors': []
        }
        
        try:
            # Create test data
            test_files = self.create_test_data()
            test_file = test_files[0]
            
            # Test 1: Upload file
            logger.info("Testing file upload endpoint...")
            
            with open(test_file, 'rb') as f:
                upload_response = self.client.post(
                    "/api/upload",
                    files={"file": ("test.csv", f, "text/csv")}
                )
            
            assert upload_response.status_code == 200, f"Upload failed: {upload_response.status_code}"
            upload_data = upload_response.json()
            assert "file_id" in upload_data, "Upload response missing file_id"
            
            test_result['assertions'].append("File upload successful")
            
            # Test 2: Process file
            logger.info("Testing file processing endpoint...")
            
            process_response = self.client.post(
                "/api/process",
                json={
                    "file_id": upload_data["file_id"],
                    "apply_quality_assessment": True
                }
            )
            
            assert process_response.status_code == 200, f"Processing failed: {process_response.status_code}"
            process_data = process_response.json()
            assert "batch_id" in process_data, "Process response missing batch_id"
            
            test_result['assertions'].append("File processing initiated")
            
            # Test 3: Check batch status
            logger.info("Testing batch status endpoint...")
            
            batch_id = process_data["batch_id"]
            
            # Wait a bit for processing
            time.sleep(2)
            
            status_response = self.client.get(f"/api/batch/{batch_id}/status")
            assert status_response.status_code == 200, f"Status check failed: {status_response.status_code}"
            
            test_result['assertions'].append("Batch status check successful")
            
            # Test 4: Get dashboard summary
            logger.info("Testing dashboard summary endpoint...")
            
            dashboard_response = self.client.get("/api/dashboard/summary")
            assert dashboard_response.status_code == 200, f"Dashboard failed: {dashboard_response.status_code}"
            
            dashboard_data = dashboard_response.json()
            assert "total_subjects" in dashboard_data, "Dashboard missing total_subjects"
            
            test_result['assertions'].append("Dashboard summary successful")
            
            # Test 5: Get subjects list
            logger.info("Testing subjects list endpoint...")
            
            subjects_response = self.client.get("/api/subjects")
            assert subjects_response.status_code == 200, f"Subjects list failed: {subjects_response.status_code}"
            
            test_result['assertions'].append("Subjects list successful")
            
            # Test 6: Export data
            logger.info("Testing export endpoints...")
            
            export_response = self.client.get("/api/export/csv")
            assert export_response.status_code == 200, f"CSV export failed: {export_response.status_code}"
            
            test_result['assertions'].append("CSV export successful")
            
            # Test 7: Integration status
            logger.info("Testing integration status endpoint...")
            
            integration_response = self.client.get("/api/integration/status")
            assert integration_response.status_code == 200, f"Integration status failed: {integration_response.status_code}"
            
            integration_data = integration_response.json()
            assert integration_data["status"] == "healthy", "Integration status not healthy"
            
            test_result['assertions'].append("Integration status healthy")
            
            test_result['status'] = 'passed'
            
        except Exception as e:
            logger.error(f"API endpoints integration test failed: {str(e)}")
            test_result['status'] = 'failed'
            test_result['errors'].append(str(e))
        
        test_result['end_time'] = datetime.now()
        test_result['duration'] = (test_result['end_time'] - test_result['start_time']).total_seconds()
        
        return test_result
    
    def test_error_handling_integration(self) -> Dict[str, Any]:
        """Test error handling integration."""
        logger.info("Testing error handling integration...")
        
        test_result = {
            'test_name': 'error_handling_integration',
            'status': 'running',
            'start_time': datetime.now(),
            'assertions': [],
            'errors': []
        }
        
        try:
            # Test 1: Invalid file upload
            logger.info("Testing invalid file upload...")
            
            invalid_response = self.client.post(
                "/api/upload",
                files={"file": ("invalid.txt", b"invalid content", "text/plain")}
            )
            
            assert invalid_response.status_code == 400, "Should reject invalid file"
            test_result['assertions'].append("Invalid file upload properly rejected")
            
            # Test 2: Non-existent file processing
            logger.info("Testing non-existent file processing...")
            
            process_invalid_response = self.client.post(
                "/api/process",
                json={"file_id": "non-existent", "apply_quality_assessment": True}
            )
            
            assert process_invalid_response.status_code == 404, "Should return 404 for non-existent file"
            test_result['assertions'].append("Non-existent file processing properly handled")
            
            # Test 3: Invalid workflow request
            logger.info("Testing invalid workflow request...")
            
            workflow_response = self.client.post(
                "/api/workflow/execute",
                json={}  # Missing required file_path
            )
            
            assert workflow_response.status_code == 400, "Should reject invalid workflow request"
            test_result['assertions'].append("Invalid workflow request properly rejected")
            
            test_result['status'] = 'passed'
            
        except Exception as e:
            logger.error(f"Error handling integration test failed: {str(e)}")
            test_result['status'] = 'failed'
            test_result['errors'].append(str(e))
        
        test_result['end_time'] = datetime.now()
        test_result['duration'] = (test_result['end_time'] - test_result['start_time']).total_seconds()
        
        return test_result
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all integration tests."""
        logger.info("Starting comprehensive integration tests...")
        
        overall_result = {
            'test_suite': 'comprehensive_integration_tests',
            'start_time': datetime.now(),
            'tests': [],
            'summary': {
                'total_tests': 0,
                'passed_tests': 0,
                'failed_tests': 0,
                'total_assertions': 0,
                'total_errors': 0
            }
        }
        
        try:
            # Run individual tests
            tests = [
                self.test_complete_workflow_integration(),
                self.test_batch_workflow_integration(),
                self.test_api_endpoints_integration(),
                self.test_error_handling_integration()
            ]
            
            # Execute async tests
            for test_coro in tests:
                if asyncio.iscoroutine(test_coro):
                    test_result = await test_coro
                else:
                    test_result = test_coro
                
                overall_result['tests'].append(test_result)
                overall_result['summary']['total_tests'] += 1
                
                if test_result['status'] == 'passed':
                    overall_result['summary']['passed_tests'] += 1
                else:
                    overall_result['summary']['failed_tests'] += 1
                
                overall_result['summary']['total_assertions'] += len(test_result['assertions'])
                overall_result['summary']['total_errors'] += len(test_result['errors'])
            
            # Determine overall status
            overall_result['status'] = 'passed' if overall_result['summary']['failed_tests'] == 0 else 'failed'
            
        except Exception as e:
            logger.error(f"Test suite execution failed: {str(e)}")
            overall_result['status'] = 'error'
            overall_result['error'] = str(e)
        
        finally:
            # Cleanup
            self.cleanup_test_data()
        
        overall_result['end_time'] = datetime.now()
        overall_result['total_duration'] = (overall_result['end_time'] - overall_result['start_time']).total_seconds()
        
        return overall_result
    
    def print_test_results(self, results: Dict[str, Any]):
        """Print formatted test results."""
        print("\n" + "="*80)
        print("COMPREHENSIVE INTEGRATION TEST RESULTS")
        print("="*80)
        
        print(f"Test Suite: {results['test_suite']}")
        print(f"Status: {results['status'].upper()}")
        print(f"Duration: {results['total_duration']:.2f} seconds")
        print(f"Start Time: {results['start_time']}")
        print(f"End Time: {results['end_time']}")
        
        print("\nSUMMARY:")
        summary = results['summary']
        print(f"  Total Tests: {summary['total_tests']}")
        print(f"  Passed: {summary['passed_tests']}")
        print(f"  Failed: {summary['failed_tests']}")
        print(f"  Total Assertions: {summary['total_assertions']}")
        print(f"  Total Errors: {summary['total_errors']}")
        
        print("\nDETAILED RESULTS:")
        for test in results['tests']:
            status_symbol = "✓" if test['status'] == 'passed' else "✗"
            print(f"\n{status_symbol} {test['test_name']} ({test['status'].upper()})")
            print(f"   Duration: {test['duration']:.2f}s")
            print(f"   Assertions: {len(test['assertions'])}")
            
            if test['assertions']:
                for assertion in test['assertions']:
                    print(f"     ✓ {assertion}")
            
            if test['errors']:
                print(f"   Errors: {len(test['errors'])}")
                for error in test['errors']:
                    print(f"     ✗ {error}")
        
        print("\n" + "="*80)


async def main():
    """Main test execution function."""
    runner = IntegrationTestRunner()
    
    try:
        results = await runner.run_all_tests()
        runner.print_test_results(results)
        
        # Exit with appropriate code
        exit_code = 0 if results['status'] == 'passed' else 1
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Test execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())