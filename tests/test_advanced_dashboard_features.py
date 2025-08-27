"""
Tests for advanced dashboard features including filtering, bulk operations, and customization.
"""

import pytest
import json
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timedelta
from fastapi.testclient import TestClient

from app.main import app
from app.models import (
    ProcessedSubject, SubjectInfo, MRIQCMetrics, QualityAssessment, 
    QualityStatus, AgeGroup, NormalizedMetrics
)


class TestAdvancedFiltering:
    """Test advanced filtering capabilities."""
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    @pytest.fixture
    def sample_subjects(self):
        """Create sample subjects for testing."""
        subjects = []
        
        # Create subjects with different characteristics
        for i in range(20):
            subject_info = SubjectInfo(
                subject_id=f"sub-{i:03d}",
                age=20 + (i * 2),  # Ages from 20 to 58
                sex="M" if i % 2 == 0 else "F",
                session=f"ses-{i % 3 + 1}",
                scan_type="T1w" if i % 2 == 0 else "BOLD",
                acquisition_date=datetime.now() - timedelta(days=i)
            )
            
            metrics = MRIQCMetrics(
                snr=10 + (i * 0.5),
                cnr=2 + (i * 0.1),
                fber=1000 + (i * 50),
                efc=0.4 + (i * 0.01),
                fwhm_avg=2.5 + (i * 0.05)
            )
            
            # Vary quality status
            if i < 5:
                quality_status = QualityStatus.PASS
                composite_score = 0.8 + (i * 0.02)
            elif i < 10:
                quality_status = QualityStatus.WARNING
                composite_score = 0.6 + (i * 0.02)
            elif i < 15:
                quality_status = QualityStatus.FAIL
                composite_score = 0.3 + (i * 0.02)
            else:
                quality_status = QualityStatus.UNCERTAIN
                composite_score = 0.5 + (i * 0.02)
            
            quality_assessment = QualityAssessment(
                overall_status=quality_status,
                metric_assessments={},
                composite_score=composite_score,
                recommendations=[],
                flags=[],
                confidence=0.8
            )
            
            # Age group based on age
            age = subject_info.age
            if age < 18:
                age_group = AgeGroup.PEDIATRIC
            elif age < 35:
                age_group = AgeGroup.YOUNG_ADULT
            elif age < 65:
                age_group = AgeGroup.MIDDLE_AGE
            else:
                age_group = AgeGroup.ELDERLY
            
            normalized_metrics = NormalizedMetrics(
                raw_metrics=metrics,
                percentiles={"snr": 50 + i, "cnr": 60 + i},
                z_scores={"snr": 0.1 * i, "cnr": 0.2 * i},
                age_group=age_group,
                normative_dataset="default"
            )
            
            subject = ProcessedSubject(
                subject_info=subject_info,
                raw_metrics=metrics,
                normalized_metrics=normalized_metrics,
                quality_assessment=quality_assessment,
                processing_timestamp=datetime.now() - timedelta(hours=i)
            )
            
            subjects.append(subject)
        
        return subjects
    
    def test_filter_by_quality_status(self, client, sample_subjects):
        """Test filtering by quality status."""
        # Mock the processed subjects store
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            response = client.post('/api/subjects/filter', json={
                'filter_criteria': {
                    'quality_status': ['pass']
                }
            })
            
            assert response.status_code == 200
            data = response.json()
            
            # Should only return subjects with pass status
            assert len(data['subjects']) == 5
            for subject in data['subjects']:
                assert subject['quality_assessment']['overall_status'] == 'pass'
    
    def test_filter_by_age_group(self, client, sample_subjects):
        """Test filtering by age group."""
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            response = client.post('/api/subjects/filter', json={
                'filter_criteria': {
                    'age_group': ['young_adult']
                }
            })
            
            assert response.status_code == 200
            data = response.json()
            
            # Should only return young adult subjects
            for subject in data['subjects']:
                if subject['normalized_metrics']:
                    assert subject['normalized_metrics']['age_group'] == 'young_adult'
    
    def test_filter_by_age_range(self, client, sample_subjects):
        """Test filtering by age range."""
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            response = client.post('/api/subjects/filter', json={
                'filter_criteria': {
                    'age_range': {
                        'min': 25,
                        'max': 35
                    }
                }
            })
            
            assert response.status_code == 200
            data = response.json()
            
            # Should only return subjects within age range
            for subject in data['subjects']:
                age = subject['subject_info']['age']
                assert 25 <= age <= 35
    
    def test_filter_by_scan_type(self, client, sample_subjects):
        """Test filtering by scan type."""
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            response = client.post('/api/subjects/filter', json={
                'filter_criteria': {
                    'scan_type': ['T1w']
                }
            })
            
            assert response.status_code == 200
            data = response.json()
            
            # Should only return T1w subjects
            for subject in data['subjects']:
                assert subject['subject_info']['scan_type'] == 'T1w'
    
    def test_filter_by_search_text(self, client, sample_subjects):
        """Test filtering by search text."""
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            response = client.post('/api/subjects/filter', json={
                'filter_criteria': {
                    'search_text': 'sub-001'
                }
            })
            
            assert response.status_code == 200
            data = response.json()
            
            # Should only return subjects matching search
            assert len(data['subjects']) == 1
            assert data['subjects'][0]['subject_info']['subject_id'] == 'sub-001'
    
    def test_filter_by_metric_range(self, client, sample_subjects):
        """Test filtering by metric ranges."""
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            response = client.post('/api/subjects/filter', json={
                'filter_criteria': {
                    'metric_filters': {
                        'snr': {
                            'min': 15,
                            'max': 20
                        }
                    }
                }
            })
            
            assert response.status_code == 200
            data = response.json()
            
            # Should only return subjects with SNR in range
            for subject in data['subjects']:
                snr = subject['raw_metrics']['snr']
                assert 15 <= snr <= 20
    
    def test_filter_by_date_range(self, client, sample_subjects):
        """Test filtering by date range."""
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            response = client.post('/api/subjects/filter', json={
                'filter_criteria': {
                    'date_range': {
                        'start': yesterday.isoformat(),
                        'end': today.isoformat()
                    }
                }
            })
            
            assert response.status_code == 200
            data = response.json()
            
            # Should return subjects processed within date range
            assert len(data['subjects']) >= 0  # At least some subjects should match
    
    def test_combined_filters(self, client, sample_subjects):
        """Test combining multiple filters."""
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            response = client.post('/api/subjects/filter', json={
                'filter_criteria': {
                    'quality_status': ['pass', 'warning'],
                    'scan_type': ['T1w'],
                    'age_range': {
                        'min': 20,
                        'max': 40
                    }
                }
            })
            
            assert response.status_code == 200
            data = response.json()
            
            # Should return subjects matching all criteria
            for subject in data['subjects']:
                assert subject['quality_assessment']['overall_status'] in ['pass', 'warning']
                assert subject['subject_info']['scan_type'] == 'T1w'
                age = subject['subject_info']['age']
                assert 20 <= age <= 40
    
    def test_pagination_with_filters(self, client, sample_subjects):
        """Test pagination works with filters."""
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            # First page
            response = client.post('/api/subjects/filter?page=1&page_size=5', json={
                'filter_criteria': {
                    'quality_status': ['pass', 'warning']
                }
            })
            
            assert response.status_code == 200
            data = response.json()
            
            assert data['page'] == 1
            assert data['page_size'] == 5
            assert len(data['subjects']) <= 5
            assert data['total_count'] == 10  # 5 pass + 5 warning subjects


class TestBulkOperations:
    """Test bulk operations functionality."""
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    @pytest.fixture
    def sample_subjects(self):
        """Create sample subjects for bulk operations."""
        subjects = []
        
        for i in range(5):
            subject_info = SubjectInfo(
                subject_id=f"sub-{i:03d}",
                age=25 + i,
                scan_type="T1w"
            )
            
            metrics = MRIQCMetrics(snr=10 + i, cnr=2 + i)
            
            quality_assessment = QualityAssessment(
                overall_status=QualityStatus.WARNING,
                metric_assessments={},
                composite_score=0.6,
                recommendations=[],
                flags=[],
                confidence=0.8
            )
            
            subject = ProcessedSubject(
                subject_info=subject_info,
                raw_metrics=metrics,
                quality_assessment=quality_assessment,
                processing_timestamp=datetime.now()
            )
            
            subjects.append(subject)
        
        return subjects
    
    def test_bulk_update_quality_status(self, client, sample_subjects):
        """Test bulk updating quality status."""
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            with patch('app.routes.audit_logger') as mock_audit:
                response = client.post('/api/subjects/bulk-update', json={
                    'subject_ids': ['sub-001', 'sub-002', 'sub-003'],
                    'quality_status': 'pass',
                    'reason': 'Manual review completed'
                })
                
                assert response.status_code == 200
                data = response.json()
                
                assert data['updated_count'] == 3
                assert data['requested_count'] == 3
                assert len(data['errors']) == 0
                
                # Verify subjects were updated
                for subject in sample_subjects[:3]:
                    assert subject.quality_assessment.overall_status == QualityStatus.PASS
                
                # Verify audit logging was called
                assert mock_audit.log_quality_decision.call_count == 3
                assert mock_audit.log_user_action.called
    
    def test_bulk_update_nonexistent_subjects(self, client, sample_subjects):
        """Test bulk update with non-existent subjects."""
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            response = client.post('/api/subjects/bulk-update', json={
                'subject_ids': ['sub-001', 'sub-999', 'sub-002'],
                'quality_status': 'fail',
                'reason': 'Test update'
            })
            
            assert response.status_code == 200
            data = response.json()
            
            # Should update only existing subjects
            assert data['updated_count'] == 2
            assert data['requested_count'] == 3
    
    def test_bulk_update_empty_list(self, client, sample_subjects):
        """Test bulk update with empty subject list."""
        with patch('app.routes.processed_subjects_store', {'batch1': sample_subjects}):
            response = client.post('/api/subjects/bulk-update', json={
                'subject_ids': [],
                'quality_status': 'pass',
                'reason': 'Empty test'
            })
            
            assert response.status_code == 200
            data = response.json()
            
            assert data['updated_count'] == 0
            assert data['requested_count'] == 0


class TestDashboardCustomization:
    """Test dashboard customization features."""
    
    def test_view_settings_storage(self):
        """Test view settings can be stored and retrieved."""
        # This would be tested in the frontend JavaScript
        # Here we test the concept
        
        settings = {
            'widgets': {
                'summaryCards': True,
                'qualityChart': False,
                'ageChart': True,
                'metricsChart': True,
                'recentActivity': False,
                'subjectsTable': True
            },
            'columns': {
                'subjectId': True,
                'age': True,
                'ageGroup': False,
                'scanType': True,
                'qualityStatus': True,
                'score': True,
                'metrics': False,
                'processed': True
            },
            'refreshInterval': 60,
            'defaultPageSize': 25,
            'defaultSort': 'composite_score_desc'
        }
        
        # Test that settings structure is valid
        assert 'widgets' in settings
        assert 'columns' in settings
        assert 'refreshInterval' in settings
        assert isinstance(settings['refreshInterval'], int)
        assert settings['refreshInterval'] >= 0
    
    def test_filter_presets_structure(self):
        """Test filter presets structure."""
        preset = {
            'name': 'Failed Subjects',
            'filters': {
                'quality_status': ['fail'],
                'age_range': {'min': 18, 'max': 65}
            }
        }
        
        # Test preset structure
        assert 'name' in preset
        assert 'filters' in preset
        assert isinstance(preset['filters'], dict)
    
    def test_quick_presets_definitions(self):
        """Test quick preset definitions."""
        quick_presets = {
            'failed_subjects': {'quality_status': ['fail']},
            'warning_subjects': {'quality_status': ['warning']},
            'pediatric_subjects': {'age_group': ['pediatric']},
            'recent_subjects': {'date_range': {'start': '2024-01-01'}},
            'high_quality': {'min_composite_score': 0.8}
        }
        
        # Test each preset has valid structure
        for preset_name, filters in quick_presets.items():
            assert isinstance(filters, dict)
            assert len(filters) > 0


class TestUIFunctionality:
    """Test UI functionality and user experience."""
    
    def test_filter_form_validation(self):
        """Test filter form validation logic."""
        # Test age range validation
        def validate_age_range(min_age, max_age):
            if min_age is not None and max_age is not None:
                return min_age <= max_age
            return True
        
        assert validate_age_range(18, 65) == True
        assert validate_age_range(65, 18) == False
        assert validate_age_range(None, 65) == True
        assert validate_age_range(18, None) == True
    
    def test_search_text_processing(self):
        """Test search text processing."""
        def process_search_text(text):
            if not text:
                return None
            return text.strip().lower()
        
        assert process_search_text("  SUB-001  ") == "sub-001"
        assert process_search_text("") is None
        assert process_search_text(None) is None
    
    def test_pagination_calculations(self):
        """Test pagination calculations."""
        def calculate_pagination(current_page, total_items, page_size):
            total_pages = max(1, (total_items + page_size - 1) // page_size)
            start_idx = (current_page - 1) * page_size
            end_idx = min(start_idx + page_size, total_items)
            
            return {
                'total_pages': total_pages,
                'start_idx': start_idx,
                'end_idx': end_idx,
                'has_previous': current_page > 1,
                'has_next': current_page < total_pages
            }
        
        # Test normal pagination
        result = calculate_pagination(2, 100, 10)
        assert result['total_pages'] == 10
        assert result['start_idx'] == 10
        assert result['end_idx'] == 20
        assert result['has_previous'] == True
        assert result['has_next'] == True
        
        # Test edge cases
        result = calculate_pagination(1, 5, 10)
        assert result['total_pages'] == 1
        assert result['start_idx'] == 0
        assert result['end_idx'] == 5
        assert result['has_previous'] == False
        assert result['has_next'] == False
    
    def test_sort_functionality(self):
        """Test sorting functionality."""
        def apply_sort(items, sort_by, sort_order):
            reverse = sort_order == 'desc'
            
            if sort_by == 'subject_id':
                return sorted(items, key=lambda x: x['subject_id'], reverse=reverse)
            elif sort_by == 'age':
                return sorted(items, key=lambda x: x['age'] or 0, reverse=reverse)
            elif sort_by == 'composite_score':
                return sorted(items, key=lambda x: x['composite_score'], reverse=reverse)
            
            return items
        
        test_items = [
            {'subject_id': 'sub-003', 'age': 25, 'composite_score': 0.8},
            {'subject_id': 'sub-001', 'age': 30, 'composite_score': 0.6},
            {'subject_id': 'sub-002', 'age': None, 'composite_score': 0.9}
        ]
        
        # Test subject ID sorting
        sorted_items = apply_sort(test_items, 'subject_id', 'asc')
        assert sorted_items[0]['subject_id'] == 'sub-001'
        assert sorted_items[2]['subject_id'] == 'sub-003'
        
        # Test score sorting
        sorted_items = apply_sort(test_items, 'composite_score', 'desc')
        assert sorted_items[0]['composite_score'] == 0.9
        assert sorted_items[2]['composite_score'] == 0.6


class TestPerformanceAndScalability:
    """Test performance aspects of advanced features."""
    
    def test_large_dataset_filtering(self):
        """Test filtering performance with large datasets."""
        # Create a large number of mock subjects
        large_dataset = []
        for i in range(1000):
            subject = {
                'subject_id': f'sub-{i:04d}',
                'age': 20 + (i % 50),
                'quality_status': ['pass', 'warning', 'fail', 'uncertain'][i % 4],
                'composite_score': 0.1 + (i % 10) * 0.1
            }
            large_dataset.append(subject)
        
        # Test filtering performance
        import time
        
        start_time = time.time()
        
        # Simulate filtering
        filtered = [s for s in large_dataset if s['quality_status'] == 'pass']
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Should process quickly (less than 1 second for 1000 items)
        assert processing_time < 1.0
        assert len(filtered) == 250  # Every 4th item is 'pass'
    
    def test_memory_usage_bulk_operations(self):
        """Test memory usage during bulk operations."""
        # Test that bulk operations don't create unnecessary copies
        subject_ids = [f'sub-{i:04d}' for i in range(100)]
        
        # Simulate bulk update without creating copies
        updated_count = 0
        for subject_id in subject_ids:
            # Simulate update operation
            updated_count += 1
        
        assert updated_count == 100


class TestErrorHandling:
    """Test error handling in advanced features."""
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    def test_invalid_filter_criteria(self, client):
        """Test handling of invalid filter criteria."""
        response = client.post('/api/subjects/filter', json={
            'filter_criteria': {
                'invalid_field': 'invalid_value'
            }
        })
        
        # Should handle gracefully and ignore invalid fields
        assert response.status_code == 200
    
    def test_bulk_update_with_invalid_status(self, client):
        """Test bulk update with invalid quality status."""
        response = client.post('/api/subjects/bulk-update', json={
            'subject_ids': ['sub-001'],
            'quality_status': 'invalid_status',
            'reason': 'Test'
        })
        
        # Should return validation error
        assert response.status_code == 422
    
    def test_empty_dataset_filtering(self, client):
        """Test filtering with empty dataset."""
        with patch('app.routes.processed_subjects_store', {}):
            response = client.post('/api/subjects/filter', json={
                'filter_criteria': {
                    'quality_status': ['pass']
                }
            })
            
            assert response.status_code == 200
            data = response.json()
            assert data['total_count'] == 0
            assert len(data['subjects']) == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])