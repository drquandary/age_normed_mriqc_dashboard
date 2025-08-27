"""
Tests for normative database functionality.
"""

import pytest
import tempfile
import os
from pathlib import Path

from app.database import NormativeDatabase


class TestNormativeDatabase:
    """Test cases for NormativeDatabase class."""
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        db = NormativeDatabase(db_path)
        yield db
        
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    def test_database_initialization(self, temp_db):
        """Test database initialization creates tables and default data."""
        # Check that age groups were created
        age_groups = temp_db.get_age_groups()
        assert len(age_groups) == 5
        
        # Verify age group names
        group_names = [ag['name'] for ag in age_groups]
        expected_names = ['pediatric', 'adolescent', 'young_adult', 'middle_age', 'elderly']
        assert all(name in group_names for name in expected_names)
    
    def test_age_group_ranges(self, temp_db):
        """Test age group ranges are correct."""
        age_groups = temp_db.get_age_groups()
        
        # Find specific age groups and check ranges
        for ag in age_groups:
            if ag['name'] == 'pediatric':
                assert ag['min_age'] == 6.0
                assert ag['max_age'] == 12.0
            elif ag['name'] == 'young_adult':
                assert ag['min_age'] == 18.0
                assert ag['max_age'] == 35.0
            elif ag['name'] == 'elderly':
                assert ag['min_age'] == 66.0
                assert ag['max_age'] == 100.0
    
    def test_get_age_group_by_age(self, temp_db):
        """Test age group lookup by age."""
        # Test pediatric range
        pediatric = temp_db.get_age_group_by_age(8.5)
        assert pediatric is not None
        assert pediatric['name'] == 'pediatric'
        
        # Test young adult range
        young_adult = temp_db.get_age_group_by_age(25.0)
        assert young_adult is not None
        assert young_adult['name'] == 'young_adult'
        
        # Test elderly range
        elderly = temp_db.get_age_group_by_age(70.0)
        assert elderly is not None
        assert elderly['name'] == 'elderly'
        
        # Test out of range
        out_of_range = temp_db.get_age_group_by_age(150.0)
        assert out_of_range is None
    
    def test_normative_data_exists(self, temp_db):
        """Test that normative data was populated."""
        age_groups = temp_db.get_age_groups()
        pediatric_id = None
        
        for ag in age_groups:
            if ag['name'] == 'pediatric':
                pediatric_id = ag['id']
                break
        
        assert pediatric_id is not None
        
        # Check SNR data exists
        snr_data = temp_db.get_normative_data('snr', pediatric_id)
        assert snr_data is not None
        assert snr_data['mean_value'] > 0
        assert snr_data['std_value'] > 0
        assert snr_data['percentile_5'] is not None
        assert snr_data['percentile_95'] is not None
    
    def test_quality_thresholds_exist(self, temp_db):
        """Test that quality thresholds were populated."""
        age_groups = temp_db.get_age_groups()
        young_adult_id = None
        
        for ag in age_groups:
            if ag['name'] == 'young_adult':
                young_adult_id = ag['id']
                break
        
        assert young_adult_id is not None
        
        # Check SNR thresholds
        snr_thresholds = temp_db.get_quality_thresholds('snr', young_adult_id)
        assert snr_thresholds is not None
        assert snr_thresholds['warning_threshold'] > 0
        assert snr_thresholds['fail_threshold'] > 0
        assert snr_thresholds['direction'] == 'higher_better'
        
        # Check EFC thresholds (lower is better)
        efc_thresholds = temp_db.get_quality_thresholds('efc', young_adult_id)
        assert efc_thresholds is not None
        assert efc_thresholds['direction'] == 'lower_better'
    
    def test_add_custom_normative_data(self, temp_db):
        """Test adding custom normative data."""
        age_groups = temp_db.get_age_groups()
        test_age_group_id = age_groups[0]['id']
        
        # Add custom metric
        percentiles = {'5': 1.0, '25': 2.0, '50': 3.0, '75': 4.0, '95': 5.0}
        temp_db.add_custom_normative_data(
            metric_name='test_metric',
            age_group_id=test_age_group_id,
            mean_value=3.0,
            std_value=1.0,
            percentiles=percentiles,
            sample_size=100,
            dataset_source='test_dataset'
        )
        
        # Verify it was added
        custom_data = temp_db.get_normative_data('test_metric', test_age_group_id)
        assert custom_data is not None
        assert custom_data['mean_value'] == 3.0
        assert custom_data['std_value'] == 1.0
        assert custom_data['percentile_50'] == 3.0
        assert custom_data['dataset_source'] == 'test_dataset'
    
    def test_add_custom_age_group(self, temp_db):
        """Test adding custom age group."""
        # Add custom age group in a range that doesn't overlap with existing groups
        new_id = temp_db.add_custom_age_group(
            name='test_group',
            min_age=101.0,
            max_age=110.0,
            description='Test age group'
        )
        
        assert new_id is not None
        
        # Verify it was added
        test_group = temp_db.get_age_group_by_age(105.0)
        assert test_group is not None
        assert test_group['name'] == 'test_group'
        assert test_group['min_age'] == 101.0
        assert test_group['max_age'] == 110.0
    
    def test_database_constraints(self, temp_db):
        """Test database constraints and unique indexes."""
        age_groups = temp_db.get_age_groups()
        test_age_group_id = age_groups[0]['id']
        
        # Add normative data
        percentiles = {'5': 1.0, '25': 2.0, '50': 3.0, '75': 4.0, '95': 5.0}
        temp_db.add_custom_normative_data(
            metric_name='constraint_test',
            age_group_id=test_age_group_id,
            mean_value=3.0,
            std_value=1.0,
            percentiles=percentiles,
            sample_size=100,
            dataset_source='test1'
        )
        
        # Add same metric again (should replace due to UNIQUE constraint)
        temp_db.add_custom_normative_data(
            metric_name='constraint_test',
            age_group_id=test_age_group_id,
            mean_value=4.0,
            std_value=1.5,
            percentiles=percentiles,
            sample_size=150,
            dataset_source='test2'
        )
        
        # Verify replacement occurred
        data = temp_db.get_normative_data('constraint_test', test_age_group_id)
        assert data['mean_value'] == 4.0
        assert data['dataset_source'] == 'test2'
    
    def test_connection_context_manager(self, temp_db):
        """Test database connection context manager."""
        # Test successful connection
        with temp_db.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM age_groups")
            count = cursor.fetchone()[0]
            assert count > 0
        
        # Connection should be closed after context
        # This is implicit - if connection wasn't closed properly,
        # we might see issues with subsequent operations
        
        # Test another operation works fine
        age_groups = temp_db.get_age_groups()
        assert len(age_groups) > 0
    
    def test_normative_data_completeness(self, temp_db):
        """Test that all age groups have normative data for key metrics."""
        age_groups = temp_db.get_age_groups()
        key_metrics = ['snr', 'cnr', 'fber', 'efc', 'fwhm_avg']
        
        for age_group in age_groups:
            for metric in key_metrics:
                normative_data = temp_db.get_normative_data(metric, age_group['id'])
                assert normative_data is not None, f"Missing {metric} data for {age_group['name']}"
                assert normative_data['mean_value'] > 0
                assert normative_data['std_value'] > 0
    
    def test_quality_thresholds_completeness(self, temp_db):
        """Test that all age groups have quality thresholds for key metrics."""
        age_groups = temp_db.get_age_groups()
        key_metrics = ['snr', 'cnr', 'efc', 'fwhm_avg']
        
        for age_group in age_groups:
            for metric in key_metrics:
                thresholds = temp_db.get_quality_thresholds(metric, age_group['id'])
                assert thresholds is not None, f"Missing {metric} thresholds for {age_group['name']}"
                assert thresholds['warning_threshold'] is not None
                assert thresholds['fail_threshold'] is not None
                assert thresholds['direction'] in ['higher_better', 'lower_better']
    
    def test_percentile_ordering(self, temp_db):
        """Test that percentile values are properly ordered."""
        age_groups = temp_db.get_age_groups()
        
        for age_group in age_groups:
            snr_data = temp_db.get_normative_data('snr', age_group['id'])
            if snr_data:
                p5 = snr_data['percentile_5']
                p25 = snr_data['percentile_25']
                p50 = snr_data['percentile_50']
                p75 = snr_data['percentile_75']
                p95 = snr_data['percentile_95']
                
                # Check ordering
                assert p5 <= p25 <= p50 <= p75 <= p95, f"Percentiles not ordered for {age_group['name']}"