"""
Age normalization service for MRIQC quality metrics.
"""

import logging
import numpy as np
from typing import Dict, List, Optional, Tuple
from scipy import stats
from enum import Enum

from .database import NormativeDatabase
from .models import AgeGroup, MRIQCMetrics, NormalizedMetrics, QualityStatus
from .common_utils.logging_config import setup_logging
from .cache_service import cache_service

logger = setup_logging(__name__)


class AgeNormalizer:
    """Handles age group assignment and percentile calculations for MRIQC metrics with caching."""
    
    def __init__(self, db_path: str = "data/normative_data.db"):
        self.db = NormativeDatabase(db_path)
        self._age_group_cache = {}
        self._normative_cache = {}
    
    def get_age_group(self, age: float) -> Optional[AgeGroup]:
        """
        Determine age group for a given age.
        
        Args:
            age: Age in years
            
        Returns:
            AgeGroup enum value or None if age is out of range
        """
        if age is None or age < 0:
            logger.warning(f"Invalid age provided: {age}")
            return None
        
        # Check cache first
        if age in self._age_group_cache:
            return self._age_group_cache[age]
        
        age_group_data = self.db.get_age_group_by_age(age)
        if not age_group_data:
            logger.warning(f"No age group found for age {age}")
            return None
        
        try:
            age_group = AgeGroup(age_group_data['name'])
            self._age_group_cache[age] = age_group
            return age_group
        except ValueError:
            logger.error(f"Invalid age group name: {age_group_data['name']}")
            return None
    
    def normalize_metrics(self, metrics: MRIQCMetrics, age: float) -> Optional[NormalizedMetrics]:
        """
        Normalize MRIQC metrics using age-appropriate normative data with caching.
        
        Args:
            metrics: Raw MRIQC metrics
            age: Subject age in years
            
        Returns:
            NormalizedMetrics with percentiles and z-scores
        """
        # Check cache first
        metrics_hash = cache_service.generate_hash(metrics.model_dump())
        cached_result = cache_service.get_normalized_metrics(metrics_hash, age)
        if cached_result:
            return NormalizedMetrics(**cached_result)
        
        age_group = self.get_age_group(age)
        if not age_group:
            logger.warning(f"Cannot normalize metrics - no age group for age {age}")
            return None
        
        # Get age group ID from database
        age_groups = self.db.get_age_groups()
        age_group_id = None
        for ag in age_groups:
            if ag['name'] == age_group.value:
                age_group_id = ag['id']
                break
        
        if not age_group_id:
            logger.error(f"Age group ID not found for {age_group.value}")
            return None
        
        percentiles = {}
        z_scores = {}
        
        # Process each metric that has a value
        for metric_name, metric_value in metrics.model_dump().items():
            if metric_value is None:
                continue
            
            normative_data = self.db.get_normative_data(metric_name, age_group_id)
            if not normative_data:
                logger.warning(f"No normative data found for {metric_name} in age group {age_group.value}")
                continue
            
            # Calculate percentile
            percentile = self.calculate_percentile(
                metric_value, 
                normative_data['mean_value'], 
                normative_data['std_value']
            )
            percentiles[metric_name] = percentile
            
            # Calculate z-score
            z_score = self.calculate_z_score(
                metric_value,
                normative_data['mean_value'],
                normative_data['std_value']
            )
            z_scores[metric_name] = z_score
        
        return NormalizedMetrics(
            raw_metrics=metrics,
            percentiles=percentiles,
            z_scores=z_scores,
            age_group=age_group,
            normative_dataset="literature_composite"
        )
    
    def calculate_percentile(self, value: float, mean: float, std: float) -> float:
        """
        Calculate percentile rank for a value given normal distribution parameters.
        
        Args:
            value: Observed value
            mean: Population mean
            std: Population standard deviation
            
        Returns:
            Percentile rank (0-100)
        """
        if std <= 0:
            logger.warning(f"Invalid standard deviation: {std}")
            return 50.0  # Return median if std is invalid
        
        z_score = (value - mean) / std
        percentile = stats.norm.cdf(z_score) * 100
        
        # Clamp to valid range
        return max(0.0, min(100.0, percentile))
    
    def calculate_z_score(self, value: float, mean: float, std: float) -> float:
        """
        Calculate z-score for a value.
        
        Args:
            value: Observed value
            mean: Population mean
            std: Population standard deviation
            
        Returns:
            Z-score
        """
        if std <= 0:
            logger.warning(f"Invalid standard deviation: {std}")
            return 0.0
        
        return (value - mean) / std
    
    def get_percentile_from_lookup(self, value: float, percentile_data: Dict[str, float]) -> float:
        """
        Get percentile using lookup table interpolation.
        
        Args:
            value: Observed value
            percentile_data: Dict with percentile keys (5, 25, 50, 75, 95) and values
            
        Returns:
            Interpolated percentile rank
        """
        # Extract percentile points
        percentiles = [5, 25, 50, 75, 95]
        values = []
        
        for p in percentiles:
            key = f'percentile_{p}'
            if key in percentile_data and percentile_data[key] is not None:
                values.append(percentile_data[key])
            else:
                return self.calculate_percentile(value, percentile_data.get('mean_value', 0), 
                                               percentile_data.get('std_value', 1))
        
        if len(values) != len(percentiles):
            logger.warning("Incomplete percentile data, falling back to normal distribution")
            return self.calculate_percentile(value, percentile_data.get('mean_value', 0), 
                                           percentile_data.get('std_value', 1))
        
        # Interpolate percentile
        if value <= values[0]:
            return 5.0
        elif value >= values[-1]:
            return 95.0
        else:
            return np.interp(value, values, percentiles)
    
    def assess_metric_quality(self, metric_name: str, metric_value: float, 
                            age_group_id: int) -> QualityStatus:
        """
        Assess quality status for a single metric.
        
        Args:
            metric_name: Name of the metric
            metric_value: Metric value
            age_group_id: Age group ID
            
        Returns:
            QualityStatus enum
        """
        thresholds = self.db.get_quality_thresholds(metric_name, age_group_id)
        if not thresholds:
            logger.warning(f"No thresholds found for {metric_name} in age group {age_group_id}")
            return QualityStatus.UNCERTAIN
        
        warning_thresh = thresholds['warning_threshold']
        fail_thresh = thresholds['fail_threshold']
        direction = thresholds['direction']
        
        if direction == 'higher_better':
            if metric_value >= warning_thresh:
                return QualityStatus.PASS
            elif metric_value >= fail_thresh:
                return QualityStatus.WARNING
            else:
                return QualityStatus.FAIL
        else:  # lower_better
            if metric_value <= warning_thresh:
                return QualityStatus.PASS
            elif metric_value <= fail_thresh:
                return QualityStatus.WARNING
            else:
                return QualityStatus.FAIL
    
    def get_age_group_statistics(self, age_group: AgeGroup) -> Dict[str, Dict]:
        """
        Get normative statistics for all metrics in an age group.
        
        Args:
            age_group: AgeGroup enum
            
        Returns:
            Dictionary of metric statistics
        """
        # Get age group ID
        age_groups = self.db.get_age_groups()
        age_group_id = None
        for ag in age_groups:
            if ag['name'] == age_group.value:
                age_group_id = ag['id']
                break
        
        if not age_group_id:
            return {}
        
        # Get all normative data for this age group
        with self.db.get_connection() as conn:
            cursor = conn.execute("""
                SELECT metric_name, mean_value, std_value, 
                       percentile_5, percentile_25, percentile_50, 
                       percentile_75, percentile_95, sample_size
                FROM normative_data 
                WHERE age_group_id = ?
            """, (age_group_id,))
            
            statistics = {}
            for row in cursor.fetchall():
                statistics[row['metric_name']] = {
                    'mean': row['mean_value'],
                    'std': row['std_value'],
                    'percentiles': {
                        '5': row['percentile_5'],
                        '25': row['percentile_25'],
                        '50': row['percentile_50'],
                        '75': row['percentile_75'],
                        '95': row['percentile_95']
                    },
                    'sample_size': row['sample_size']
                }
            
            return statistics
    
    def validate_age_coverage(self, ages: List[float]) -> Dict[str, List[float]]:
        """
        Validate age coverage and identify ages without normative data.
        
        Args:
            ages: List of ages to validate
            
        Returns:
            Dictionary with 'covered' and 'uncovered' age lists
        """
        covered = []
        uncovered = []
        
        for age in ages:
            if age is None or age < 0:
                uncovered.append(age)
                continue
                
            age_group = self.get_age_group(age)
            if age_group:
                covered.append(age)
            else:
                uncovered.append(age)
        
        return {
            'covered': covered,
            'uncovered': uncovered,
            'coverage_rate': len(covered) / len(ages) if ages else 0.0
        }
    
    def get_metric_recommendations(self, normalized_metrics: NormalizedMetrics) -> List[str]:
        """
        Generate recommendations based on normalized metrics.
        
        Args:
            normalized_metrics: Normalized metrics data
            
        Returns:
            List of recommendation strings
        """
        recommendations = []
        
        # Check for concerning percentiles
        for metric_name, percentile in normalized_metrics.percentiles.items():
            if percentile < 5:
                recommendations.append(f"{metric_name} is below 5th percentile for age group")
            elif percentile > 95:
                recommendations.append(f"{metric_name} is above 95th percentile for age group")
        
        # Check z-scores
        for metric_name, z_score in normalized_metrics.z_scores.items():
            if abs(z_score) > 2.5:
                recommendations.append(f"{metric_name} z-score ({z_score:.2f}) indicates potential quality issue")
        
        if not recommendations:
            recommendations.append("All metrics within normal ranges for age group")
        
        return recommendations