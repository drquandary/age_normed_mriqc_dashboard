"""
Longitudinal data service for age-normed MRIQC dashboard.

This module provides services for managing longitudinal subjects,
calculating trends, and generating longitudinal reports.
"""

import logging
from typing import List, Dict, Optional, Tuple, Union
from datetime import datetime, timedelta
import numpy as np
from scipy import stats
import pandas as pd

from .models import (
    LongitudinalSubject, TimePoint, LongitudinalTrend, 
    LongitudinalSummary, ProcessedSubject, AgeGroup, QualityStatus
)
from .database import NormativeDatabase
from .age_normalizer import AgeNormalizer
from .common_utils.logging_config import setup_logging

logger = setup_logging(__name__)


class LongitudinalService:
    """Service for managing longitudinal data and trend analysis."""
    
    def __init__(self, db: NormativeDatabase = None, age_normalizer: AgeNormalizer = None):
        self.db = db or NormativeDatabase()
        self.age_normalizer = age_normalizer or AgeNormalizer()
    
    def add_subject_timepoint(self, subject_id: str, processed_subject: ProcessedSubject,
                            session: str = None, days_from_baseline: int = None,
                            study_name: str = None) -> str:
        """
        Add a timepoint for a longitudinal subject.
        
        Args:
            subject_id: Subject identifier
            processed_subject: Complete processed subject data
            session: Session identifier (e.g., 'baseline', 'followup1')
            days_from_baseline: Days elapsed from baseline scan
            study_name: Name of longitudinal study
            
        Returns:
            Timepoint ID
        """
        try:
            # Generate timepoint ID
            timepoint_id = f"{subject_id}_{session}" if session else f"{subject_id}_{datetime.now().strftime('%Y%m%d')}"
            
            # Extract age and scan date from processed subject
            age_at_scan = processed_subject.subject_info.age
            scan_date = processed_subject.subject_info.acquisition_date
            
            # Calculate days from baseline if not provided
            if days_from_baseline is None and scan_date:
                baseline_timepoints = self.db.get_subject_timepoints(subject_id)
                if baseline_timepoints:
                    baseline_dates = [tp for tp in baseline_timepoints if tp.get('scan_date')]
                    if baseline_dates:
                        baseline_date = min(datetime.fromisoformat(tp['scan_date'].replace('Z', '+00:00')) 
                                          for tp in baseline_dates)
                        if isinstance(scan_date, str):
                            scan_date = datetime.fromisoformat(scan_date.replace('Z', '+00:00'))
                        days_from_baseline = (scan_date - baseline_date).days
                else:
                    days_from_baseline = 0  # This is the baseline
            
            # Create or update longitudinal subject
            existing_subject = self.db.get_longitudinal_subject(subject_id)
            if not existing_subject:
                self.db.create_longitudinal_subject(
                    subject_id=subject_id,
                    baseline_age=age_at_scan,
                    sex=processed_subject.subject_info.sex.value if processed_subject.subject_info.sex else None,
                    study_name=study_name
                )
            
            # Add timepoint
            self.db.add_timepoint(
                timepoint_id=timepoint_id,
                subject_id=subject_id,
                session=session,
                age_at_scan=age_at_scan,
                days_from_baseline=days_from_baseline,
                scan_date=scan_date.isoformat() if scan_date else None,
                processed_data=processed_subject.model_dump()
            )
            
            logger.info(f"Added timepoint {timepoint_id} for subject {subject_id}")
            return timepoint_id
            
        except Exception as e:
            logger.error(f"Error adding timepoint for subject {subject_id}: {str(e)}")
            raise
    
    def get_longitudinal_subject(self, subject_id: str) -> Optional[LongitudinalSubject]:
        """
        Get complete longitudinal subject data.
        
        Args:
            subject_id: Subject identifier
            
        Returns:
            LongitudinalSubject object or None if not found
        """
        try:
            # Get subject info
            subject_data = self.db.get_longitudinal_subject(subject_id)
            if not subject_data:
                return None
            
            # Get timepoints
            timepoint_data = self.db.get_subject_timepoints(subject_id)
            if not timepoint_data:
                return None
            
            # Convert timepoints to TimePoint objects
            timepoints = []
            for tp_data in timepoint_data:
                processed_data = tp_data.get('processed_data', {})
                if isinstance(processed_data, str):
                    import json
                    processed_data = json.loads(processed_data)
                
                processed_subject = ProcessedSubject(**processed_data)
                
                timepoint = TimePoint(
                    timepoint_id=tp_data['timepoint_id'],
                    subject_id=subject_id,
                    session=tp_data.get('session'),
                    age_at_scan=tp_data.get('age_at_scan'),
                    days_from_baseline=tp_data.get('days_from_baseline'),
                    scan_date=datetime.fromisoformat(tp_data['scan_date']) if tp_data.get('scan_date') else None,
                    processed_subject=processed_subject
                )
                timepoints.append(timepoint)
            
            # Sort timepoints by days from baseline
            timepoints.sort(key=lambda x: x.days_from_baseline or 0)
            
            return LongitudinalSubject(
                subject_id=subject_id,
                baseline_age=subject_data.get('baseline_age'),
                sex=subject_data.get('sex'),
                timepoints=timepoints,
                study_name=subject_data.get('study_name')
            )
            
        except Exception as e:
            logger.error(f"Error getting longitudinal subject {subject_id}: {str(e)}")
            return None
    
    def calculate_metric_trend(self, subject_id: str, metric_name: str) -> Optional[LongitudinalTrend]:
        """
        Calculate trend for a specific metric across timepoints.
        
        Args:
            subject_id: Subject identifier
            metric_name: Name of the quality metric
            
        Returns:
            LongitudinalTrend object or None if insufficient data
        """
        try:
            longitudinal_subject = self.get_longitudinal_subject(subject_id)
            if not longitudinal_subject or len(longitudinal_subject.timepoints) < 2:
                return None
            
            # Extract metric values and timepoints
            values_over_time = []
            ages = []
            age_groups = []
            quality_statuses = []
            
            for tp in longitudinal_subject.timepoints:
                # Get metric value
                metric_value = getattr(tp.processed_subject.raw_metrics, metric_name, None)
                if metric_value is None:
                    continue
                
                values_over_time.append({
                    'timepoint_id': tp.timepoint_id,
                    'value': metric_value,
                    'days_from_baseline': tp.days_from_baseline or 0,
                    'age_at_scan': tp.age_at_scan,
                    'session': tp.session
                })
                
                ages.append(tp.age_at_scan)
                
                # Track age group changes
                if tp.age_at_scan:
                    age_group = self.age_normalizer.get_age_group(tp.age_at_scan)
                    age_groups.append(age_group.value if age_group else 'unknown')
                
                # Track quality status changes
                quality_assessment = tp.processed_subject.quality_assessment
                metric_status = quality_assessment.metric_assessments.get(metric_name, 'uncertain')
                quality_statuses.append({
                    'timepoint_id': tp.timepoint_id,
                    'status': metric_status,
                    'days_from_baseline': tp.days_from_baseline or 0
                })
            
            if len(values_over_time) < 2:
                return None
            
            # Calculate linear trend
            days = np.array([v['days_from_baseline'] for v in values_over_time])
            values = np.array([v['value'] for v in values_over_time])
            
            # Perform linear regression
            slope, intercept, r_value, p_value, std_err = stats.linregress(days, values)
            r_squared = r_value ** 2
            
            # Determine trend direction
            if p_value < 0.05:  # Significant trend
                if abs(slope) < 0.001:  # Very small slope
                    trend_direction = 'stable'
                elif slope > 0:
                    trend_direction = 'improving' if self._is_higher_better(metric_name) else 'declining'
                else:
                    trend_direction = 'declining' if self._is_higher_better(metric_name) else 'improving'
            else:
                if r_squared < 0.1:  # Low R-squared suggests variability
                    trend_direction = 'variable'
                else:
                    trend_direction = 'stable'
            
            # Detect age group changes
            unique_age_groups = list(set(age_groups))
            age_group_changes = []
            if len(unique_age_groups) > 1:
                age_group_changes = [f"Transitioned from {age_groups[0]} to {age_groups[-1]}"]
            
            # Create trend object
            trend = LongitudinalTrend(
                subject_id=subject_id,
                metric_name=metric_name,
                trend_direction=trend_direction,
                trend_slope=slope,
                trend_r_squared=r_squared,
                trend_p_value=p_value,
                values_over_time=values_over_time,
                age_group_changes=age_group_changes,
                quality_status_changes=quality_statuses
            )
            
            # Store trend in database
            self.db.calculate_and_store_trend(
                subject_id=subject_id,
                metric_name=metric_name,
                trend_direction=trend_direction,
                trend_slope=slope,
                trend_r_squared=r_squared,
                trend_p_value=p_value,
                values_over_time=values_over_time,
                age_group_changes=age_group_changes,
                quality_status_changes=quality_statuses
            )
            
            return trend
            
        except Exception as e:
            logger.error(f"Error calculating trend for {subject_id}, metric {metric_name}: {str(e)}")
            return None
    
    def calculate_all_trends_for_subject(self, subject_id: str) -> List[LongitudinalTrend]:
        """
        Calculate trends for all available metrics for a subject.
        
        Args:
            subject_id: Subject identifier
            
        Returns:
            List of LongitudinalTrend objects
        """
        try:
            longitudinal_subject = self.get_longitudinal_subject(subject_id)
            if not longitudinal_subject:
                return []
            
            # Get all available metrics from first timepoint
            first_timepoint = longitudinal_subject.timepoints[0]
            raw_metrics = first_timepoint.processed_subject.raw_metrics
            
            trends = []
            for metric_name in raw_metrics.model_fields.keys():
                if getattr(raw_metrics, metric_name) is not None:
                    trend = self.calculate_metric_trend(subject_id, metric_name)
                    if trend:
                        trends.append(trend)
            
            return trends
            
        except Exception as e:
            logger.error(f"Error calculating all trends for subject {subject_id}: {str(e)}")
            return []
    
    def get_study_longitudinal_summary(self, study_name: str) -> Optional[LongitudinalSummary]:
        """
        Generate longitudinal summary for a study.
        
        Args:
            study_name: Name of the study
            
        Returns:
            LongitudinalSummary object
        """
        try:
            # Get summary data from database
            summary_data = self.db.get_study_longitudinal_summary(study_name)
            
            basic_stats = summary_data.get('basic_statistics', {})
            followup_stats = summary_data.get('followup_statistics', {})
            age_stats = summary_data.get('age_statistics', {})
            trend_stats = summary_data.get('trend_statistics', {})
            
            # Format timepoints per subject statistics
            timepoints_per_subject = {
                'mean': basic_stats.get('avg_timepoints_per_subject', 0) or 0,
                'min': basic_stats.get('min_timepoints_per_subject', 0) or 0,
                'max': basic_stats.get('max_timepoints_per_subject', 0) or 0
            }
            
            # Format follow-up duration statistics
            follow_up_duration = {
                'mean': followup_stats.get('avg_followup_days', 0) or 0,
                'min': followup_stats.get('min_followup_days', 0) or 0,
                'max': followup_stats.get('max_followup_days', 0) or 0
            }
            
            # Format age progression statistics
            age_progression = {
                'mean_baseline_age': age_stats.get('avg_baseline_age', 0) or 0,
                'mean_final_age': age_stats.get('avg_final_age', 0) or 0,
                'mean_age_change': age_stats.get('avg_age_change', 0) or 0
            }
            
            # Calculate quality stability (simplified)
            quality_stability = {
                'stable_subjects': 0,
                'improving_subjects': 0,
                'declining_subjects': 0,
                'variable_subjects': 0
            }
            
            # Count age group transitions (simplified)
            age_group_transitions = {
                'no_transition': basic_stats.get('total_subjects', 0) or 0,
                'single_transition': 0,
                'multiple_transitions': 0
            }
            
            return LongitudinalSummary(
                study_name=study_name,
                total_subjects=basic_stats.get('total_subjects', 0) or 0,
                total_timepoints=basic_stats.get('total_timepoints', 0) or 0,
                timepoints_per_subject=timepoints_per_subject,
                follow_up_duration=follow_up_duration,
                age_progression=age_progression,
                metric_trends=trend_stats,
                quality_stability=quality_stability,
                age_group_transitions=age_group_transitions
            )
            
        except Exception as e:
            logger.error(f"Error generating longitudinal summary for study {study_name}: {str(e)}")
            return None
    
    def get_subjects_with_longitudinal_data(self, study_name: str = None) -> List[Dict]:
        """
        Get all subjects with longitudinal data.
        
        Args:
            study_name: Optional study name filter
            
        Returns:
            List of subject dictionaries with timepoint counts
        """
        try:
            if study_name:
                subjects = self.db.get_longitudinal_subjects_by_study(study_name)
            else:
                subjects = self.db.get_all_longitudinal_subjects()
            
            # Add timepoint counts
            for subject in subjects:
                timepoints = self.db.get_subject_timepoints(subject['subject_id'])
                subject['timepoint_count'] = len(timepoints)
                
                if timepoints:
                    ages = [tp.get('age_at_scan') for tp in timepoints if tp.get('age_at_scan')]
                    if ages:
                        subject['age_range'] = {'min': min(ages), 'max': max(ages)}
                    
                    days = [tp.get('days_from_baseline') for tp in timepoints if tp.get('days_from_baseline') is not None]
                    if days:
                        subject['follow_up_days'] = max(days) - min(days)
            
            return subjects
            
        except Exception as e:
            logger.error(f"Error getting subjects with longitudinal data: {str(e)}")
            return []
    
    def _is_higher_better(self, metric_name: str) -> bool:
        """
        Determine if higher values are better for a given metric.
        
        Args:
            metric_name: Name of the metric
            
        Returns:
            True if higher is better, False otherwise
        """
        # Metrics where higher values are better
        higher_better_metrics = {
            'snr', 'cnr', 'fber', 'qi1', 'qi2', 'wm2max'
        }
        
        # Metrics where lower values are better
        lower_better_metrics = {
            'efc', 'fwhm_avg', 'fwhm_x', 'fwhm_y', 'fwhm_z', 'cjv',
            'dvars', 'fd_mean', 'fd_num', 'fd_perc', 'outlier_fraction'
        }
        
        if metric_name in higher_better_metrics:
            return True
        elif metric_name in lower_better_metrics:
            return False
        else:
            # Default assumption for unknown metrics
            logger.warning(f"Unknown metric direction for {metric_name}, assuming higher is better")
            return True
    
    def detect_age_group_transitions(self, subject_id: str) -> List[Dict]:
        """
        Detect age group transitions for a subject.
        
        Args:
            subject_id: Subject identifier
            
        Returns:
            List of transition events
        """
        try:
            longitudinal_subject = self.get_longitudinal_subject(subject_id)
            if not longitudinal_subject:
                return []
            
            transitions = []
            previous_age_group = None
            
            for tp in longitudinal_subject.timepoints:
                if tp.age_at_scan:
                    current_age_group = self.age_normalizer.get_age_group(tp.age_at_scan)
                    
                    if previous_age_group and current_age_group != previous_age_group:
                        transitions.append({
                            'timepoint_id': tp.timepoint_id,
                            'from_age_group': previous_age_group.value,
                            'to_age_group': current_age_group.value,
                            'age_at_transition': tp.age_at_scan,
                            'days_from_baseline': tp.days_from_baseline
                        })
                    
                    previous_age_group = current_age_group
            
            return transitions
            
        except Exception as e:
            logger.error(f"Error detecting age group transitions for {subject_id}: {str(e)}")
            return []
    
    def export_longitudinal_data(self, study_name: str = None, format: str = 'csv') -> str:
        """
        Export longitudinal data for analysis.
        
        Args:
            study_name: Optional study name filter
            format: Export format ('csv' or 'json')
            
        Returns:
            Path to exported file
        """
        try:
            subjects = self.get_subjects_with_longitudinal_data(study_name)
            
            if format == 'csv':
                return self._export_longitudinal_csv(subjects, study_name)
            elif format == 'json':
                return self._export_longitudinal_json(subjects, study_name)
            else:
                raise ValueError(f"Unsupported export format: {format}")
                
        except Exception as e:
            logger.error(f"Error exporting longitudinal data: {str(e)}")
            raise
    
    def _export_longitudinal_csv(self, subjects: List[Dict], study_name: str = None) -> str:
        """Export longitudinal data as CSV."""
        import csv
        from pathlib import Path
        
        # Create export directory
        export_dir = Path("data/exports")
        export_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"longitudinal_data_{study_name or 'all'}_{timestamp}.csv"
        filepath = export_dir / filename
        
        # Collect all timepoint data
        rows = []
        for subject_data in subjects:
            subject_id = subject_data['subject_id']
            timepoints = self.db.get_subject_timepoints(subject_id)
            
            for tp in timepoints:
                if tp.get('processed_data'):
                    import json
                    processed_data = json.loads(tp['processed_data'])
                    raw_metrics = processed_data.get('raw_metrics', {})
                    quality_assessment = processed_data.get('quality_assessment', {})
                    
                    row = {
                        'subject_id': subject_id,
                        'timepoint_id': tp['timepoint_id'],
                        'session': tp.get('session', ''),
                        'age_at_scan': tp.get('age_at_scan', ''),
                        'days_from_baseline': tp.get('days_from_baseline', ''),
                        'scan_date': tp.get('scan_date', ''),
                        'overall_quality_status': quality_assessment.get('overall_status', ''),
                        'composite_score': quality_assessment.get('composite_score', ''),
                        **raw_metrics  # Add all raw metrics
                    }
                    rows.append(row)
        
        # Write CSV
        if rows:
            with open(filepath, 'w', newline='') as csvfile:
                fieldnames = rows[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        
        logger.info(f"Exported longitudinal data to {filepath}")
        return str(filepath)
    
    def _export_longitudinal_json(self, subjects: List[Dict], study_name: str = None) -> str:
        """Export longitudinal data as JSON."""
        import json
        from pathlib import Path
        
        # Create export directory
        export_dir = Path("data/exports")
        export_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"longitudinal_data_{study_name or 'all'}_{timestamp}.json"
        filepath = export_dir / filename
        
        # Collect all data
        export_data = {
            'study_name': study_name,
            'export_timestamp': datetime.now().isoformat(),
            'subjects': []
        }
        
        for subject_data in subjects:
            subject_id = subject_data['subject_id']
            longitudinal_subject = self.get_longitudinal_subject(subject_id)
            
            if longitudinal_subject:
                export_data['subjects'].append(longitudinal_subject.model_dump())
        
        # Write JSON
        with open(filepath, 'w') as jsonfile:
            json.dump(export_data, jsonfile, indent=2, default=str)
        
        logger.info(f"Exported longitudinal data to {filepath}")
        return str(filepath)