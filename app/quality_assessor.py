"""
Quality assessment engine for MRIQC metrics with age-appropriate thresholds.

This module implements the QualityAssessor class that evaluates scan quality
using age-specific thresholds and generates comprehensive quality assessments.
"""

import logging
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum

from .models import (
    MRIQCMetrics, NormalizedMetrics, QualityAssessment, QualityStatus,
    AgeGroup, SubjectInfo
)
from .database import NormativeDatabase
from .age_normalizer import AgeNormalizer
from .common_utils.logging_config import setup_logging

logger = setup_logging(__name__)


@dataclass
class ThresholdViolation:
    """Details of a threshold violation."""
    metric_name: str
    value: float
    threshold: float
    threshold_type: str  # 'warning' or 'fail'
    severity: str
    direction: str  # 'higher_better' or 'lower_better'


class QualityAssessor:
    """
    Evaluates scan quality using age-appropriate thresholds.
    
    This class applies age-specific quality thresholds to MRIQC metrics,
    calculates composite quality scores, and generates recommendations.
    """
    
    def __init__(self, db_path: str = "data/normative_data.db"):
        """
        Initialize the quality assessor.
        
        Args:
            db_path: Path to the normative database
        """
        self.db = NormativeDatabase(db_path)
        self.age_normalizer = AgeNormalizer(db_path)
        
        # Metric weights for composite score calculation
        self.metric_weights = {
            'snr': 0.20,
            'cnr': 0.18,
            'fber': 0.15,
            'efc': 0.15,
            'fwhm_avg': 0.12,
            'qi1': 0.10,
            'cjv': 0.10
        }
        
        # Quality score mappings
        self.status_scores = {
            QualityStatus.PASS: 100,
            QualityStatus.WARNING: 70,
            QualityStatus.FAIL: 30,
            QualityStatus.UNCERTAIN: 50
        }
    
    def assess_quality(self, metrics: MRIQCMetrics, subject_info: SubjectInfo) -> QualityAssessment:
        """
        Perform comprehensive quality assessment.
        
        Args:
            metrics: Raw MRIQC metrics
            subject_info: Subject demographic information
            
        Returns:
            QualityAssessment with overall status, scores, and recommendations
        """
        logger.info(f"Assessing quality for subject {subject_info.subject_id}")
        
        # Get age group and normalized metrics if age is available
        age_group = None
        age_group_id = None
        normalized_metrics = None
        
        if subject_info.age is not None:
            age_group = self.age_normalizer.get_age_group(subject_info.age)
            if age_group:
                age_groups = self.db.get_age_groups()
                for ag in age_groups:
                    if ag['name'] == age_group.value:
                        age_group_id = ag['id']
                        break
                
                normalized_metrics = self.age_normalizer.normalize_metrics(
                    metrics, subject_info.age
                )
        
        # Assess individual metrics
        metric_assessments = {}
        threshold_violations = {}
        flags = []
        
        for metric_name, metric_value in metrics.model_dump().items():
            if metric_value is None:
                continue
            
            # Assess metric quality
            status, violation = self._assess_single_metric(
                metric_name, metric_value, age_group_id
            )
            metric_assessments[metric_name] = status
            
            if violation:
                threshold_violations[metric_name] = {
                    'value': violation.value,
                    'threshold': violation.threshold,
                    'severity': violation.severity,
                    'direction': violation.direction
                }
                flags.append(f"{metric_name}_{violation.severity}")
        
        # Calculate composite score
        composite_score = self.calculate_composite_score(metric_assessments, metrics)
        
        # Determine overall status
        overall_status = self._determine_overall_status(metric_assessments, composite_score)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(
            metric_assessments, threshold_violations, normalized_metrics, subject_info
        )
        
        # Calculate confidence
        confidence = self._calculate_confidence(
            metric_assessments, age_group is not None, len(metric_assessments)
        )
        
        return QualityAssessment(
            overall_status=overall_status,
            metric_assessments=metric_assessments,
            composite_score=composite_score,
            recommendations=recommendations,
            flags=flags,
            confidence=confidence,
            threshold_violations=threshold_violations
        )
    
    def _assess_single_metric(self, metric_name: str, metric_value: float, 
                            age_group_id: Optional[int]) -> Tuple[QualityStatus, Optional[ThresholdViolation]]:
        """
        Assess quality status for a single metric.
        
        Args:
            metric_name: Name of the metric
            metric_value: Metric value
            age_group_id: Age group ID (None if age unknown)
            
        Returns:
            Tuple of (QualityStatus, ThresholdViolation or None)
        """
        if age_group_id is None:
            logger.warning(f"No age group available for {metric_name} assessment")
            return QualityStatus.UNCERTAIN, None
        
        # Get thresholds for this metric and age group
        thresholds = self.db.get_quality_thresholds(metric_name, age_group_id)
        if not thresholds:
            logger.warning(f"No thresholds found for {metric_name} in age group {age_group_id}")
            return QualityStatus.UNCERTAIN, None
        
        warning_thresh = thresholds['warning_threshold']
        fail_thresh = thresholds['fail_threshold']
        direction = thresholds['direction']
        
        # Apply thresholds based on direction
        if direction == 'higher_better':
            if metric_value >= warning_thresh:
                return QualityStatus.PASS, None
            elif metric_value >= fail_thresh:
                violation = ThresholdViolation(
                    metric_name=metric_name,
                    value=metric_value,
                    threshold=warning_thresh,
                    threshold_type='warning',
                    severity='warning',
                    direction=direction
                )
                return QualityStatus.WARNING, violation
            else:
                violation = ThresholdViolation(
                    metric_name=metric_name,
                    value=metric_value,
                    threshold=fail_thresh,
                    threshold_type='fail',
                    severity='fail',
                    direction=direction
                )
                return QualityStatus.FAIL, violation
        
        else:  # lower_better
            if metric_value <= warning_thresh:
                return QualityStatus.PASS, None
            elif metric_value <= fail_thresh:
                violation = ThresholdViolation(
                    metric_name=metric_name,
                    value=metric_value,
                    threshold=warning_thresh,
                    threshold_type='warning',
                    severity='warning',
                    direction=direction
                )
                return QualityStatus.WARNING, violation
            else:
                violation = ThresholdViolation(
                    metric_name=metric_name,
                    value=metric_value,
                    threshold=fail_thresh,
                    threshold_type='fail',
                    severity='fail',
                    direction=direction
                )
                return QualityStatus.FAIL, violation
    
    def calculate_composite_score(self, metric_assessments: Dict[str, QualityStatus], 
                                metrics: MRIQCMetrics) -> float:
        """
        Calculate composite quality score.
        
        Args:
            metric_assessments: Quality status for each metric
            metrics: Raw MRIQC metrics
            
        Returns:
            Composite score (0-100)
        """
        if not metric_assessments:
            return 50.0  # Neutral score if no metrics
        
        total_weight = 0.0
        weighted_score = 0.0
        
        for metric_name, status in metric_assessments.items():
            # Get weight for this metric
            weight = self.metric_weights.get(metric_name, 0.05)  # Default small weight
            
            # Get score for this status
            score = self.status_scores[status]
            
            weighted_score += weight * score
            total_weight += weight
        
        # Handle metrics not in weight table
        if total_weight < 1.0:
            remaining_weight = 1.0 - total_weight
            remaining_metrics = [m for m in metric_assessments.keys() 
                               if m not in self.metric_weights]
            
            if remaining_metrics:
                equal_weight = remaining_weight / len(remaining_metrics)
                for metric_name in remaining_metrics:
                    status = metric_assessments[metric_name]
                    score = self.status_scores[status]
                    weighted_score += equal_weight * score
        
        return min(100.0, max(0.0, weighted_score))
    
    def _determine_overall_status(self, metric_assessments: Dict[str, QualityStatus], 
                                composite_score: float) -> QualityStatus:
        """
        Determine overall quality status.
        
        Args:
            metric_assessments: Individual metric assessments
            composite_score: Composite quality score
            
        Returns:
            Overall QualityStatus
        """
        if not metric_assessments:
            return QualityStatus.UNCERTAIN
        
        # Count status occurrences
        status_counts = {status: 0 for status in QualityStatus}
        for status in metric_assessments.values():
            status_counts[status] += 1
        
        total_metrics = len(metric_assessments)
        
        # Decision logic
        fail_rate = status_counts[QualityStatus.FAIL] / total_metrics
        warning_rate = status_counts[QualityStatus.WARNING] / total_metrics
        uncertain_rate = status_counts[QualityStatus.UNCERTAIN] / total_metrics
        
        # If more than 20% of metrics fail, overall fail
        if fail_rate > 0.2:
            return QualityStatus.FAIL
        
        # If any critical metrics fail, overall fail
        critical_metrics = ['snr', 'cnr', 'efc']
        for metric in critical_metrics:
            if (metric in metric_assessments and 
                metric_assessments[metric] == QualityStatus.FAIL):
                return QualityStatus.FAIL
        
        # If more than 40% uncertain, overall uncertain
        if uncertain_rate > 0.4:
            return QualityStatus.UNCERTAIN
        
        # If more than 30% warning or any fail, overall warning
        if warning_rate > 0.3 or fail_rate > 0:
            return QualityStatus.WARNING
        
        # Use composite score as tie-breaker
        if composite_score >= 80:
            return QualityStatus.PASS
        elif composite_score >= 60:
            return QualityStatus.WARNING
        elif composite_score >= 40:
            return QualityStatus.FAIL
        else:
            return QualityStatus.UNCERTAIN
    
    def _generate_recommendations(self, metric_assessments: Dict[str, QualityStatus],
                                threshold_violations: Dict[str, Dict],
                                normalized_metrics: Optional[NormalizedMetrics],
                                subject_info: SubjectInfo) -> List[str]:
        """
        Generate specific recommendations based on assessment results.
        
        Args:
            metric_assessments: Individual metric assessments
            threshold_violations: Details of threshold violations
            normalized_metrics: Age-normalized metrics (if available)
            subject_info: Subject information
            
        Returns:
            List of recommendation strings
        """
        recommendations = []
        
        # Overall assessment recommendations
        fail_count = sum(1 for status in metric_assessments.values() 
                        if status == QualityStatus.FAIL)
        warning_count = sum(1 for status in metric_assessments.values() 
                           if status == QualityStatus.WARNING)
        
        if fail_count == 0 and warning_count == 0:
            recommendations.append("All quality metrics within acceptable ranges")
        elif fail_count > 0:
            recommendations.append(f"EXCLUDE: {fail_count} metric(s) failed quality thresholds")
        elif warning_count > 0:
            recommendations.append(f"REVIEW: {warning_count} metric(s) require manual review")
        
        # Specific metric recommendations
        for metric_name, violation in threshold_violations.items():
            if violation['severity'] == 'fail':
                recommendations.append(
                    f"CRITICAL: {metric_name} = {violation['value']:.2f} "
                    f"(threshold: {violation['threshold']:.2f})"
                )
            elif violation['severity'] == 'warning':
                recommendations.append(
                    f"WARNING: {metric_name} = {violation['value']:.2f} "
                    f"(threshold: {violation['threshold']:.2f})"
                )
        
        # Age-specific recommendations
        if normalized_metrics:
            age_recommendations = self.age_normalizer.get_metric_recommendations(
                normalized_metrics
            )
            recommendations.extend(age_recommendations)
        elif subject_info.age is None:
            recommendations.append(
                "Consider providing age information for more accurate assessment"
            )
        
        # Scan type specific recommendations
        if subject_info.scan_type.value == 'T1w':
            if 'snr' in metric_assessments and metric_assessments['snr'] != QualityStatus.PASS:
                recommendations.append("Consider checking T1w acquisition parameters")
        elif subject_info.scan_type.value == 'BOLD':
            if 'fd_mean' in metric_assessments and metric_assessments['fd_mean'] != QualityStatus.PASS:
                recommendations.append("High motion detected - consider motion correction")
        
        # Remove duplicates while preserving order
        seen = set()
        unique_recommendations = []
        for rec in recommendations:
            if rec not in seen:
                seen.add(rec)
                unique_recommendations.append(rec)
        
        return unique_recommendations
    
    def _calculate_confidence(self, metric_assessments: Dict[str, QualityStatus],
                            has_age_info: bool, num_metrics: int) -> float:
        """
        Calculate confidence in the assessment.
        
        Args:
            metric_assessments: Individual metric assessments
            has_age_info: Whether age information is available
            num_metrics: Number of metrics assessed
            
        Returns:
            Confidence score (0-1)
        """
        base_confidence = 0.5
        
        # Boost confidence with more metrics
        metric_boost = min(0.3, num_metrics * 0.05)
        base_confidence += metric_boost
        
        # Boost confidence with age information
        if has_age_info:
            base_confidence += 0.2
        
        # Reduce confidence with uncertain assessments
        uncertain_count = sum(1 for status in metric_assessments.values() 
                            if status == QualityStatus.UNCERTAIN)
        uncertain_penalty = uncertain_count * 0.1
        base_confidence -= uncertain_penalty
        
        # Boost confidence with consistent assessments
        status_counts = {}
        for status in metric_assessments.values():
            status_counts[status] = status_counts.get(status, 0) + 1
        
        max_count = max(status_counts.values()) if status_counts else 0
        consistency = max_count / len(metric_assessments) if metric_assessments else 0
        base_confidence += consistency * 0.2
        
        return min(1.0, max(0.0, base_confidence))
    
    def apply_thresholds(self, metrics: MRIQCMetrics, age_group_id: int) -> Dict[str, QualityStatus]:
        """
        Apply quality thresholds to all metrics for a specific age group.
        
        Args:
            metrics: MRIQC metrics
            age_group_id: Age group ID
            
        Returns:
            Dictionary mapping metric names to quality status
        """
        results = {}
        
        for metric_name, metric_value in metrics.model_dump().items():
            if metric_value is None:
                continue
            
            status, _ = self._assess_single_metric(metric_name, metric_value, age_group_id)
            results[metric_name] = status
        
        return results
    
    def get_threshold_summary(self, age_group: AgeGroup) -> Dict[str, Dict]:
        """
        Get summary of all thresholds for an age group.
        
        Args:
            age_group: Age group enum
            
        Returns:
            Dictionary of threshold information by metric
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
        
        # Get all thresholds for this age group
        with self.db.get_connection() as conn:
            cursor = conn.execute("""
                SELECT metric_name, warning_threshold, fail_threshold, direction
                FROM quality_thresholds 
                WHERE age_group_id = ?
            """, (age_group_id,))
            
            thresholds = {}
            for row in cursor.fetchall():
                thresholds[row['metric_name']] = {
                    'warning_threshold': row['warning_threshold'],
                    'fail_threshold': row['fail_threshold'],
                    'direction': row['direction']
                }
            
            return thresholds
    
    def validate_thresholds(self, age_group_id: int) -> List[str]:
        """
        Validate threshold consistency for an age group.
        
        Args:
            age_group_id: Age group ID
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        with self.db.get_connection() as conn:
            cursor = conn.execute("""
                SELECT metric_name, warning_threshold, fail_threshold, direction
                FROM quality_thresholds 
                WHERE age_group_id = ?
            """, (age_group_id,))
            
            for row in cursor.fetchall():
                metric = row['metric_name']
                warning = row['warning_threshold']
                fail = row['fail_threshold']
                direction = row['direction']
                
                if warning is None or fail is None:
                    errors.append(f"{metric}: Missing threshold values")
                    continue
                
                if direction == 'higher_better' and warning <= fail:
                    errors.append(
                        f"{metric}: Warning threshold ({warning}) should be > "
                        f"fail threshold ({fail}) for higher_better metric"
                    )
                elif direction == 'lower_better' and warning >= fail:
                    errors.append(
                        f"{metric}: Warning threshold ({warning}) should be < "
                        f"fail threshold ({fail}) for lower_better metric"
                    )
        
        return errors