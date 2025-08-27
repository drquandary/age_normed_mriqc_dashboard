"""
Tests for age normalization functionality.
"""

import pytest
import tempfile
import os
import numpy as np
from unittest.mock import patch

from app.age_normalizer import AgeNormalizer
from app.models import AgeGroup, MRIQCMetrics, QualityStatus


class TestAgeNormalizer:
    """Test cases for AgeNormalizer class."""
    
    @pytest.fixture
    def temp_normalizer(self):
        """Create temporary normalizer for testing."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        normalizer = AgeNormalizer(db_path)
        yield normalizer
        
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    def test_get_age_group_valid_ages(self, temp_normalizer):
        """Test age group assignment for valid ages."""
        # Test pediatric
        assert temp_normalizer.get_age_group(8.0) == AgeGroup.PEDIATRIC
        assert temp_normalizer.get_age_group(12.0) == AgeGroup.PEDIATRIC
        
        # Test adolescent
        assert temp_normalizer.get_age_group(15.0) == AgeGroup.ADOLESCENT
        
        # Test young adult
        assert temp_normalizer.get_age_group(25.0) == AgeGroup.YOUNG_ADULT
        assert temp_normalizer.get_age_group(35.0) == AgeGroup.YOUNG_ADULT
        
        # Test middle age
        assert temp_normalizer.get_age_group(45.0) == AgeGroup.MIDDLE_AGE
        
        # Test elderly
        assert temp_normalizer.get_age_group(70.0) == AgeGroup.ELDERLY
    
    def test_get_age_group_invalid_ages(self, temp_normalizer):
        """Test age group assignment for invalid ages."""
        # Test None age
        assert temp_normalizer.get_age_group(None) is None
        
        # Test negative age
        assert temp_normalizer.get_age_group(-5.0) is None
        
        # Test out of range age
        assert temp_normalizer.get_age_group(150.0) is None
    
    def test_get_age_group_caching(self, temp_normalizer):
        """Test that age group lookups are cached."""
        # First lookup
        age_group1 = temp_normalizer.get_age_group(25.0)
        
        # Second lookup should use cache
        age_group2 = temp_normalizer.get_age_group(25.0)
        
        assert age_group1 == age_group2 == AgeGroup.YOUNG_ADULT
        assert 25.0 in temp_normalizer._age_group_cache
    
    def test_calculate_percentile(self, temp_normalizer):
        """Test percentile calculation."""
        # Test with known values
        percentile = temp_normalizer.calculate_percentile(0.0, 0.0, 1.0)  # z=0, should be 50th percentile
        assert abs(percentile - 50.0) < 0.1
        
        # Test with positive z-score
        percentile = temp_normalizer.calculate_percentile(1.0, 0.0, 1.0)  # z=1, should be ~84th percentile
        assert 83 < percentile < 85
        
        # Test with negative z-score
        percentile = temp_normalizer.calculate_percentile(-1.0, 0.0, 1.0)  # z=-1, should be ~16th percentile
        assert 15 < percentile < 17
        
        # Test edge cases
        percentile = temp_normalizer.calculate_percentile(10.0, 0.0, 0.0)  # Invalid std
        assert percentile == 50.0
    
    def test_calculate_z_score(self, temp_normalizer):
        """Test z-score calculation."""
        # Test standard cases
        z_score = temp_normalizer.calculate_z_score(1.0, 0.0, 1.0)
        assert abs(z_score - 1.0) < 0.001
        
        z_score = temp_normalizer.calculate_z_score(-1.0, 0.0, 1.0)
        assert abs(z_score - (-1.0)) < 0.001
        
        z_score = temp_normalizer.calculate_z_score(5.0, 5.0, 2.0)
        assert abs(z_score - 0.0) < 0.001
        
        # Test invalid std
        z_score = temp_normalizer.calculate_z_score(10.0, 0.0, 0.0)
        assert z_score == 0.0
    
    def test_normalize_metrics_valid_case(self, temp_normalizer):
        """Test metric normalization with valid data."""
        metrics = MRIQCMetrics(
            snr=15.0,
            cnr=3.5,
            fber=1500.0,
            efc=0.45,
            fwhm_avg=2.8
        )
        
        normalized = temp_normalizer.normalize_metrics(metrics, 25.0)  # Young adult
        
        assert normalized is not None
        assert normalized.age_group == AgeGroup.YOUNG_ADULT
        assert 'snr' in normalized.percentiles
        assert 'snr' in normalized.z_scores
        assert normalized.raw_metrics == metrics
    
    def test_normalize_metrics_invalid_age(self, temp_normalizer):
        """Test metric normalization with invalid age."""
        metrics = MRIQCMetrics(snr=15.0)
        
        # Test with None age
        normalized = temp_normalizer.normalize_metrics(metrics, None)
        assert normalized is None
        
        # Test with out-of-range age
        normalized = temp_normalizer.normalize_metrics(metrics, 150.0)
        assert normalized is None
    
    def test_normalize_metrics_partial_data(self, temp_normalizer):
        """Test metric normalization with partial metric data."""
        metrics = MRIQCMetrics(
            snr=15.0,
            cnr=None,  # Missing value
            fber=1500.0
        )
        
        normalized = temp_normalizer.normalize_metrics(metrics, 25.0)
        
        assert normalized is not None
        assert 'snr' in normalized.percentiles
        assert 'cnr' not in normalized.percentiles  # Should skip None values
        assert 'fber' in normalized.percentiles
    
    def test_assess_metric_quality_higher_better(self, temp_normalizer):
        """Test quality assessment for 'higher is better' metrics."""
        # Get young adult age group ID
        age_groups = temp_normalizer.db.get_age_groups()
        young_adult_id = None
        for ag in age_groups:
            if ag['name'] == 'young_adult':
                young_adult_id = ag['id']
                break
        
        assert young_adult_id is not None
        
        # Test SNR (higher is better)
        # High value should pass
        status = temp_normalizer.assess_metric_quality('snr', 20.0, young_adult_id)
        assert status == QualityStatus.PASS
        
        # Medium value should be warning
        status = temp_normalizer.assess_metric_quality('snr', 12.0, young_adult_id)
        assert status == QualityStatus.WARNING
        
        # Low value should fail
        status = temp_normalizer.assess_metric_quality('snr', 8.0, young_adult_id)
        assert status == QualityStatus.FAIL
    
    def test_assess_metric_quality_lower_better(self, temp_normalizer):
        """Test quality assessment for 'lower is better' metrics."""
        # Get young adult age group ID
        age_groups = temp_normalizer.db.get_age_groups()
        young_adult_id = None
        for ag in age_groups:
            if ag['name'] == 'young_adult':
                young_adult_id = ag['id']
                break
        
        assert young_adult_id is not None
        
        # Test EFC (lower is better)
        # Low value should pass
        status = temp_normalizer.assess_metric_quality('efc', 0.40, young_adult_id)
        assert status == QualityStatus.PASS
        
        # Medium value should be warning
        status = temp_normalizer.assess_metric_quality('efc', 0.55, young_adult_id)
        assert status == QualityStatus.WARNING
        
        # High value should fail
        status = temp_normalizer.assess_metric_quality('efc', 0.70, young_adult_id)
        assert status == QualityStatus.FAIL
    
    def test_assess_metric_quality_no_thresholds(self, temp_normalizer):
        """Test quality assessment when no thresholds exist."""
        age_groups = temp_normalizer.db.get_age_groups()
        test_age_group_id = age_groups[0]['id']
        
        # Test with non-existent metric
        status = temp_normalizer.assess_metric_quality('nonexistent_metric', 10.0, test_age_group_id)
        assert status == QualityStatus.UNCERTAIN
    
    def test_get_age_group_statistics(self, temp_normalizer):
        """Test retrieval of age group statistics."""
        stats = temp_normalizer.get_age_group_statistics(AgeGroup.YOUNG_ADULT)
        
        assert isinstance(stats, dict)
        assert 'snr' in stats
        assert 'mean' in stats['snr']
        assert 'std' in stats['snr']
        assert 'percentiles' in stats['snr']
        assert 'sample_size' in stats['snr']
        
        # Check percentile structure
        percentiles = stats['snr']['percentiles']
        assert '5' in percentiles
        assert '95' in percentiles
    
    def test_validate_age_coverage(self, temp_normalizer):
        """Test age coverage validation."""
        ages = [8.0, 15.0, 25.0, 45.0, 70.0, 150.0, None, -5.0]
        
        coverage = temp_normalizer.validate_age_coverage(ages)
        
        assert 'covered' in coverage
        assert 'uncovered' in coverage
        assert 'coverage_rate' in coverage
        
        # Should have 5 covered ages (8, 15, 25, 45, 70)
        assert len(coverage['covered']) == 5
        
        # Should have 3 uncovered ages (150, None, -5)
        assert len(coverage['uncovered']) == 3
        
        # Coverage rate should be 5/8 = 0.625
        assert abs(coverage['coverage_rate'] - 0.625) < 0.001
    
    def test_validate_age_coverage_empty_list(self, temp_normalizer):
        """Test age coverage validation with empty list."""
        coverage = temp_normalizer.validate_age_coverage([])
        
        assert coverage['covered'] == []
        assert coverage['uncovered'] == []
        assert coverage['coverage_rate'] == 0.0
    
    def test_get_metric_recommendations_normal(self, temp_normalizer):
        """Test recommendations for normal metrics."""
        from app.models import NormalizedMetrics
        
        # Create normalized metrics with normal values
        metrics = MRIQCMetrics(snr=15.0)
        normalized = NormalizedMetrics(
            raw_metrics=metrics,
            percentiles={'snr': 50.0},  # Normal percentile
            z_scores={'snr': 0.0},      # Normal z-score
            age_group=AgeGroup.YOUNG_ADULT,
            normative_dataset="test"
        )
        
        recommendations = temp_normalizer.get_metric_recommendations(normalized)
        
        assert len(recommendations) == 1
        assert "normal ranges" in recommendations[0].lower()
    
    def test_get_metric_recommendations_concerning(self, temp_normalizer):
        """Test recommendations for concerning metrics."""
        from app.models import NormalizedMetrics
        
        # Create normalized metrics with concerning values
        metrics = MRIQCMetrics(snr=15.0, cnr=3.0)
        normalized = NormalizedMetrics(
            raw_metrics=metrics,
            percentiles={'snr': 2.0, 'cnr': 98.0},  # Extreme percentiles
            z_scores={'snr': -3.0, 'cnr': 2.8},     # Extreme z-scores
            age_group=AgeGroup.YOUNG_ADULT,
            normative_dataset="test"
        )
        
        recommendations = temp_normalizer.get_metric_recommendations(normalized)
        
        # Should have multiple recommendations
        assert len(recommendations) > 1
        
        # Check for specific recommendations
        rec_text = ' '.join(recommendations).lower()
        assert 'below 5th percentile' in rec_text
        assert 'above 95th percentile' in rec_text
        assert 'z-score' in rec_text
    
    def test_get_percentile_from_lookup(self, temp_normalizer):
        """Test percentile calculation using lookup table."""
        # Create test percentile data
        percentile_data = {
            'percentile_5': 10.0,
            'percentile_25': 15.0,
            'percentile_50': 20.0,
            'percentile_75': 25.0,
            'percentile_95': 30.0,
            'mean_value': 20.0,
            'std_value': 5.0
        }
        
        # Test exact percentile values
        assert temp_normalizer.get_percentile_from_lookup(20.0, percentile_data) == 50.0
        assert temp_normalizer.get_percentile_from_lookup(10.0, percentile_data) == 5.0
        assert temp_normalizer.get_percentile_from_lookup(30.0, percentile_data) == 95.0
        
        # Test interpolation
        percentile = temp_normalizer.get_percentile_from_lookup(17.5, percentile_data)
        assert 25 < percentile < 50  # Should be between 25th and 50th percentile
        
        # Test extrapolation
        assert temp_normalizer.get_percentile_from_lookup(5.0, percentile_data) == 5.0  # Below minimum
        assert temp_normalizer.get_percentile_from_lookup(35.0, percentile_data) == 95.0  # Above maximum
    
    def test_get_percentile_from_lookup_incomplete_data(self, temp_normalizer):
        """Test percentile calculation with incomplete lookup data."""
        # Missing some percentile values
        incomplete_data = {
            'percentile_5': 10.0,
            'percentile_25': None,  # Missing
            'percentile_50': 20.0,
            'percentile_75': 25.0,
            'percentile_95': None,  # Missing
            'mean_value': 20.0,
            'std_value': 5.0
        }
        
        # Should fall back to normal distribution calculation
        percentile = temp_normalizer.get_percentile_from_lookup(20.0, incomplete_data)
        assert 45 < percentile < 55  # Should be around 50th percentile
    
    @patch('app.age_normalizer.logger')
    def test_logging_warnings(self, mock_logger, temp_normalizer):
        """Test that appropriate warnings are logged."""
        # Test invalid age
        temp_normalizer.get_age_group(-5.0)
        mock_logger.warning.assert_called()
        
        # Test invalid standard deviation
        temp_normalizer.calculate_percentile(10.0, 5.0, 0.0)
        mock_logger.warning.assert_called()
    
    def test_age_group_boundary_conditions(self, temp_normalizer):
        """Test age group assignment at boundary conditions."""
        # Test exact boundary values
        assert temp_normalizer.get_age_group(6.0) == AgeGroup.PEDIATRIC
        assert temp_normalizer.get_age_group(12.0) == AgeGroup.PEDIATRIC
        assert temp_normalizer.get_age_group(13.0) == AgeGroup.ADOLESCENT
        assert temp_normalizer.get_age_group(17.0) == AgeGroup.ADOLESCENT
        assert temp_normalizer.get_age_group(18.0) == AgeGroup.YOUNG_ADULT
        
        # Test just outside boundaries
        assert temp_normalizer.get_age_group(5.9) is None
        assert temp_normalizer.get_age_group(100.1) is None