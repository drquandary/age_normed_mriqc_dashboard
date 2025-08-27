"""
Configuration service for study-specific settings and validation.

This module provides configuration management functionality including
validation of custom age groups and thresholds, and application of
study-specific settings to quality assessment.
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

from .models import StudyConfiguration, QualityThresholds, AgeGroup
from .database import NormativeDatabase
from .common_utils.logging_config import setup_logging

logger = setup_logging(__name__)


class ConfigurationValidationError(Exception):
    """Raised when configuration validation fails."""
    pass


class ConfigurationService:
    """Service for managing study configurations."""
    
    def __init__(self, db_path: str = "data/normative_data.db"):
        self.db = NormativeDatabase(db_path)
    
    def validate_study_configuration(self, config: StudyConfiguration) -> List[str]:
        """
        Validate a study configuration.
        
        Args:
            config: StudyConfiguration to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Validate study name
        if not config.study_name or len(config.study_name.strip()) == 0:
            errors.append("Study name cannot be empty")
        elif len(config.study_name) > 100:
            errors.append("Study name cannot exceed 100 characters")
        
        # Check for duplicate study name
        existing_config = self.db.get_study_configuration(config.study_name)
        if existing_config:
            errors.append(f"Study configuration '{config.study_name}' already exists")
        
        # Validate custom age groups
        if config.custom_age_groups:
            age_group_errors = self._validate_custom_age_groups(config.custom_age_groups)
            errors.extend(age_group_errors)
        
        # Validate custom thresholds
        if config.custom_thresholds:
            threshold_errors = self._validate_custom_thresholds(
                config.custom_thresholds, 
                config.custom_age_groups
            )
            errors.extend(threshold_errors)
        
        # Validate normative dataset
        if not config.normative_dataset:
            errors.append("Normative dataset cannot be empty")
        
        # Validate exclusion criteria
        if config.exclusion_criteria:
            for criterion in config.exclusion_criteria:
                if not isinstance(criterion, str) or len(criterion.strip()) == 0:
                    errors.append("Exclusion criteria must be non-empty strings")
        
        # Validate created_by
        if not config.created_by or len(config.created_by.strip()) == 0:
            errors.append("Created by field cannot be empty")
        
        return errors
    
    def _validate_custom_age_groups(self, age_groups: List[Dict[str, Any]]) -> List[str]:
        """Validate custom age group definitions."""
        errors = []
        group_names = set()
        age_ranges = []
        
        for i, group in enumerate(age_groups):
            group_prefix = f"Age group {i+1}"
            
            # Check required fields
            if 'name' not in group:
                errors.append(f"{group_prefix}: Missing 'name' field")
                continue
            if 'min_age' not in group:
                errors.append(f"{group_prefix}: Missing 'min_age' field")
                continue
            if 'max_age' not in group:
                errors.append(f"{group_prefix}: Missing 'max_age' field")
                continue
            
            name = group['name']
            min_age = group['min_age']
            max_age = group['max_age']
            
            # Validate name
            if not isinstance(name, str) or len(name.strip()) == 0:
                errors.append(f"{group_prefix}: Name must be a non-empty string")
                continue
            
            if name in group_names:
                errors.append(f"{group_prefix}: Duplicate age group name '{name}'")
            group_names.add(name)
            
            # Validate age values
            try:
                min_age = float(min_age)
                max_age = float(max_age)
            except (ValueError, TypeError):
                errors.append(f"{group_prefix}: Age values must be numeric")
                continue
            
            if min_age < 0 or max_age < 0:
                errors.append(f"{group_prefix}: Age values cannot be negative")
            
            if min_age >= max_age:
                errors.append(f"{group_prefix}: min_age must be less than max_age")
            
            if min_age > 120 or max_age > 120:
                errors.append(f"{group_prefix}: Age values seem unrealistic (>120 years)")
            
            age_ranges.append((min_age, max_age, name))
        
        # Check for overlapping age ranges
        age_ranges.sort(key=lambda x: x[0])  # Sort by min_age
        for i in range(len(age_ranges) - 1):
            current_max = age_ranges[i][1]
            next_min = age_ranges[i + 1][0]
            if current_max > next_min:
                errors.append(
                    f"Overlapping age ranges: '{age_ranges[i][2]}' and '{age_ranges[i + 1][2]}'"
                )
        
        return errors
    
    def _validate_custom_thresholds(self, thresholds: List[QualityThresholds], 
                                  custom_age_groups: Optional[List[Dict[str, Any]]]) -> List[str]:
        """Validate custom quality thresholds."""
        errors = []
        
        # Get valid age group names
        valid_age_groups = set()
        if custom_age_groups:
            valid_age_groups.update(group['name'] for group in custom_age_groups if 'name' in group)
        
        # Always include default age groups as they can be referenced
        default_groups = self.db.get_age_groups()
        valid_age_groups.update(group['name'] for group in default_groups)
        
        # Valid metric names (from MRIQC)
        valid_metrics = {
            'snr', 'cnr', 'fber', 'efc', 'fwhm_avg', 'fwhm_x', 'fwhm_y', 'fwhm_z',
            'qi1', 'qi2', 'cjv', 'wm2max', 'dvars', 'fd_mean', 'fd_num', 'fd_perc',
            'gcor', 'gsr_x', 'gsr_y', 'outlier_fraction'
        }
        
        threshold_keys = set()
        
        for i, threshold in enumerate(thresholds):
            threshold_prefix = f"Threshold {i+1}"
            
            # Validate metric name
            if threshold.metric_name not in valid_metrics:
                errors.append(f"{threshold_prefix}: Invalid metric name '{threshold.metric_name}'")
            
            # Validate age group
            age_group_name = threshold.age_group if isinstance(threshold.age_group, str) else threshold.age_group.value
            if age_group_name not in valid_age_groups:
                errors.append(f"{threshold_prefix}: Invalid age group '{age_group_name}'")
            
            # Check for duplicates
            threshold_key = (threshold.metric_name, age_group_name)
            if threshold_key in threshold_keys:
                errors.append(
                    f"{threshold_prefix}: Duplicate threshold for metric '{threshold.metric_name}' "
                    f"and age group '{age_group_name}'"
                )
            threshold_keys.add(threshold_key)
            
            # Validate threshold values
            if threshold.warning_threshold is None or threshold.fail_threshold is None:
                errors.append(f"{threshold_prefix}: Both warning and fail thresholds are required")
                continue
            
            # Validate direction and threshold order
            if threshold.direction == 'higher_better':
                if threshold.warning_threshold <= threshold.fail_threshold:
                    errors.append(
                        f"{threshold_prefix}: For 'higher_better' metrics, "
                        "warning threshold must be greater than fail threshold"
                    )
            elif threshold.direction == 'lower_better':
                if threshold.warning_threshold >= threshold.fail_threshold:
                    errors.append(
                        f"{threshold_prefix}: For 'lower_better' metrics, "
                        "warning threshold must be less than fail threshold"
                    )
            else:
                errors.append(f"{threshold_prefix}: Direction must be 'higher_better' or 'lower_better'")
        
        return errors
    
    def create_study_configuration(self, config: StudyConfiguration) -> Tuple[bool, List[str]]:
        """
        Create a new study configuration.
        
        Args:
            config: StudyConfiguration to create
            
        Returns:
            Tuple of (success, error_messages)
        """
        try:
            # Validate configuration
            validation_errors = self.validate_study_configuration(config)
            if validation_errors:
                return False, validation_errors
            
            # Create base configuration
            config_id = self.db.create_study_configuration(
                study_name=config.study_name,
                normative_dataset=config.normative_dataset,
                exclusion_criteria=config.exclusion_criteria,
                created_by=config.created_by
            )
            
            # Add custom age groups
            if config.custom_age_groups:
                for group in config.custom_age_groups:
                    success = self.db.add_custom_age_group_to_study(
                        study_name=config.study_name,
                        name=group['name'],
                        min_age=float(group['min_age']),
                        max_age=float(group['max_age']),
                        description=group.get('description')
                    )
                    if not success:
                        logger.warning(f"Failed to add custom age group '{group['name']}'")
            
            # Add custom thresholds
            if config.custom_thresholds:
                for threshold in config.custom_thresholds:
                    age_group_name = threshold.age_group if isinstance(threshold.age_group, str) else threshold.age_group.value
                    success = self.db.add_custom_threshold_to_study(
                        study_name=config.study_name,
                        metric_name=threshold.metric_name,
                        age_group_name=age_group_name,
                        warning_threshold=threshold.warning_threshold,
                        fail_threshold=threshold.fail_threshold,
                        direction=threshold.direction
                    )
                    if not success:
                        logger.warning(
                            f"Failed to add custom threshold for {threshold.metric_name} "
                            f"in {age_group_name}"
                        )
            
            logger.info(f"Created study configuration: {config.study_name}")
            return True, []
            
        except Exception as e:
            logger.error(f"Failed to create study configuration: {str(e)}")
            return False, [f"Database error: {str(e)}"]
    
    def get_study_configuration(self, study_name: str) -> Optional[Dict]:
        """Get study configuration by name."""
        return self.db.get_study_configuration(study_name)
    
    def get_all_study_configurations(self) -> List[Dict]:
        """Get all active study configurations."""
        return self.db.get_all_study_configurations()
    
    def update_study_configuration(self, study_name: str, 
                                 normative_dataset: str = None,
                                 exclusion_criteria: List[str] = None) -> bool:
        """Update an existing study configuration."""
        try:
            success = self.db.update_study_configuration(
                study_name=study_name,
                normative_dataset=normative_dataset,
                exclusion_criteria=exclusion_criteria
            )
            if success:
                logger.info(f"Updated study configuration: {study_name}")
            return success
        except Exception as e:
            logger.error(f"Failed to update study configuration {study_name}: {str(e)}")
            return False
    
    def delete_study_configuration(self, study_name: str) -> bool:
        """Delete a study configuration."""
        try:
            success = self.db.delete_study_configuration(study_name)
            if success:
                logger.info(f"Deleted study configuration: {study_name}")
            return success
        except Exception as e:
            logger.error(f"Failed to delete study configuration {study_name}: {str(e)}")
            return False
    
    def get_age_group_for_study(self, study_name: str, age: float) -> Optional[Dict]:
        """
        Get appropriate age group for a subject age in a specific study.
        
        Args:
            study_name: Name of the study configuration
            age: Subject age in years
            
        Returns:
            Age group dictionary or None if no match
        """
        # Get effective age groups for the study
        age_groups = self.db.get_effective_age_groups_for_study(study_name)
        
        # Find matching age group
        for group in age_groups:
            if group['min_age'] <= age <= group['max_age']:
                return group
        
        return None
    
    def get_quality_thresholds_for_study(self, study_name: str, metric_name: str, 
                                       age_group_name: str) -> Optional[Dict]:
        """
        Get quality thresholds for a specific metric and age group in a study.
        
        Args:
            study_name: Name of the study configuration
            metric_name: Name of the quality metric
            age_group_name: Name of the age group
            
        Returns:
            Threshold dictionary or None if not found
        """
        return self.db.get_effective_thresholds_for_study(
            study_name, metric_name, age_group_name
        )
    
    def apply_study_configuration(self, study_name: str, subject_data: Dict) -> Dict:
        """
        Apply study-specific configuration to subject processing.
        
        Args:
            study_name: Name of the study configuration
            subject_data: Subject data dictionary
            
        Returns:
            Updated subject data with study-specific settings applied
        """
        try:
            config = self.get_study_configuration(study_name)
            if not config:
                logger.warning(f"Study configuration '{study_name}' not found")
                return subject_data
            
            # Apply study-specific age group assignment
            if 'age' in subject_data and subject_data['age'] is not None:
                age_group = self.get_age_group_for_study(study_name, subject_data['age'])
                if age_group:
                    subject_data['study_age_group'] = age_group['name']
                    subject_data['study_age_group_range'] = {
                        'min_age': age_group['min_age'],
                        'max_age': age_group['max_age']
                    }
            
            # Add study configuration metadata
            subject_data['study_configuration'] = {
                'study_name': config['study_name'],
                'normative_dataset': config['normative_dataset'],
                'has_custom_age_groups': len(config['custom_age_groups']) > 0,
                'has_custom_thresholds': len(config['custom_thresholds']) > 0
            }
            
            return subject_data
            
        except Exception as e:
            logger.error(f"Failed to apply study configuration {study_name}: {str(e)}")
            return subject_data
    
    def validate_configuration_update(self, study_name: str, updates: Dict) -> List[str]:
        """
        Validate configuration updates before applying them.
        
        Args:
            study_name: Name of existing study configuration
            updates: Dictionary of updates to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Check if study exists
        existing_config = self.get_study_configuration(study_name)
        if not existing_config:
            errors.append(f"Study configuration '{study_name}' not found")
            return errors
        
        # Validate normative dataset
        if 'normative_dataset' in updates:
            if not updates['normative_dataset'] or len(updates['normative_dataset'].strip()) == 0:
                errors.append("Normative dataset cannot be empty")
        
        # Validate exclusion criteria
        if 'exclusion_criteria' in updates:
            criteria = updates['exclusion_criteria']
            if criteria is not None:
                if not isinstance(criteria, list):
                    errors.append("Exclusion criteria must be a list")
                else:
                    for criterion in criteria:
                        if not isinstance(criterion, str) or len(criterion.strip()) == 0:
                            errors.append("Exclusion criteria must be non-empty strings")
        
        return errors
    
    def get_configuration_summary(self, study_name: str) -> Optional[Dict]:
        """
        Get a summary of a study configuration for display purposes.
        
        Args:
            study_name: Name of the study configuration
            
        Returns:
            Configuration summary dictionary
        """
        config = self.get_study_configuration(study_name)
        if not config:
            return None
        
        return {
            'study_name': config['study_name'],
            'normative_dataset': config['normative_dataset'],
            'created_by': config['created_by'],
            'created_at': config['created_at'],
            'updated_at': config['updated_at'],
            'custom_age_groups_count': len(config['custom_age_groups']),
            'custom_thresholds_count': len(config['custom_thresholds']),
            'exclusion_criteria_count': len(config['exclusion_criteria']),
            'has_customizations': (
                len(config['custom_age_groups']) > 0 or 
                len(config['custom_thresholds']) > 0
            )
        }