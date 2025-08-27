"""
Example usage of the configuration service.

This script demonstrates how to create, manage, and apply study configurations
with custom age groups and quality thresholds.
"""

import sys
import os
from pathlib import Path

# Add the app directory to the Python path
sys.path.append(str(Path(__file__).parent.parent))

from app.config_service import ConfigurationService
from app.models import StudyConfiguration, QualityThresholds, AgeGroup


def main():
    """Demonstrate configuration service functionality."""
    
    # Initialize configuration service
    config_service = ConfigurationService("data/example_config.db")
    
    print("=== Configuration Service Example ===\n")
    
    # 1. Create a study configuration with custom age groups and thresholds
    print("1. Creating a pediatric study configuration...")
    
    pediatric_config = StudyConfiguration(
        study_name="Pediatric Development Study",
        normative_dataset="pediatric_norms_v2",
        custom_age_groups=[
            {
                "name": "early_childhood",
                "min_age": 3.0,
                "max_age": 6.0,
                "description": "Early childhood (3-6 years)"
            },
            {
                "name": "school_age",
                "min_age": 6.0,
                "max_age": 12.0,
                "description": "School age children (6-12 years)"
            },
            {
                "name": "adolescent",
                "min_age": 12.0,
                "max_age": 18.0,
                "description": "Adolescents (12-18 years)"
            }
        ],
        custom_thresholds=[
            QualityThresholds(
                metric_name="snr",
                age_group=AgeGroup.PEDIATRIC,
                warning_threshold=12.0,
                fail_threshold=9.0,
                direction="higher_better"
            ),
            QualityThresholds(
                metric_name="efc",
                age_group=AgeGroup.PEDIATRIC,
                warning_threshold=0.55,
                fail_threshold=0.65,
                direction="lower_better"
            )
        ],
        exclusion_criteria=[
            "excessive_motion",
            "scanner_artifacts",
            "incomplete_coverage"
        ],
        created_by="researcher_001"
    )
    
    # Create the configuration
    success, errors = config_service.create_study_configuration(pediatric_config)
    
    if success:
        print("✓ Configuration created successfully!")
    else:
        print("✗ Configuration creation failed:")
        for error in errors:
            print(f"  - {error}")
        return
    
    # 2. Retrieve and display the configuration
    print("\n2. Retrieving configuration details...")
    
    config = config_service.get_study_configuration("Pediatric Development Study")
    if config:
        print(f"Study Name: {config['study_name']}")
        print(f"Normative Dataset: {config['normative_dataset']}")
        print(f"Created By: {config['created_by']}")
        print(f"Custom Age Groups: {len(config['custom_age_groups'])}")
        print(f"Custom Thresholds: {len(config['custom_thresholds'])}")
        print(f"Exclusion Criteria: {config['exclusion_criteria']}")
    
    # 3. Demonstrate age group assignment
    print("\n3. Testing age group assignment...")
    
    test_ages = [4.5, 8.0, 15.0, 25.0]
    for age in test_ages:
        age_group = config_service.get_age_group_for_study(
            "Pediatric Development Study", age
        )
        if age_group:
            print(f"Age {age}: {age_group['name']} ({age_group['min_age']}-{age_group['max_age']})")
        else:
            print(f"Age {age}: No matching age group")
    
    # 4. Demonstrate threshold retrieval
    print("\n4. Testing quality threshold retrieval...")
    
    thresholds = config_service.get_quality_thresholds_for_study(
        "Pediatric Development Study", "snr", "pediatric"
    )
    if thresholds:
        print(f"SNR thresholds for pediatric group:")
        print(f"  Warning: {thresholds['warning_threshold']}")
        print(f"  Fail: {thresholds['fail_threshold']}")
        print(f"  Direction: {thresholds['direction']}")
    
    # 5. Apply configuration to subject data
    print("\n5. Applying configuration to subject data...")
    
    subject_data = {
        'subject_id': 'sub-001',
        'age': 8.0,
        'scan_type': 'T1w',
        'snr': 11.5
    }
    
    updated_data = config_service.apply_study_configuration(
        "Pediatric Development Study", subject_data
    )
    
    print(f"Original data: {subject_data}")
    print(f"Updated data includes:")
    print(f"  Study age group: {updated_data.get('study_age_group')}")
    print(f"  Configuration metadata: {updated_data.get('study_configuration', {}).get('study_name')}")
    
    # 6. Create another configuration for comparison
    print("\n6. Creating an adult study configuration...")
    
    adult_config = StudyConfiguration(
        study_name="Adult Aging Study",
        normative_dataset="adult_aging_norms",
        custom_age_groups=[
            {
                "name": "young_adult",
                "min_age": 18.0,
                "max_age": 40.0,
                "description": "Young adults (18-40 years)"
            },
            {
                "name": "middle_aged",
                "min_age": 40.0,
                "max_age": 65.0,
                "description": "Middle-aged adults (40-65 years)"
            },
            {
                "name": "older_adult",
                "min_age": 65.0,
                "max_age": 90.0,
                "description": "Older adults (65-90 years)"
            }
        ],
        exclusion_criteria=[
            "neurological_disorder",
            "psychiatric_medication"
        ],
        created_by="researcher_002"
    )
    
    success, errors = config_service.create_study_configuration(adult_config)
    if success:
        print("✓ Adult study configuration created!")
    
    # 7. List all configurations
    print("\n7. Listing all study configurations...")
    
    all_configs = config_service.get_all_study_configurations()
    print(f"Total configurations: {len(all_configs)}")
    
    for config in all_configs:
        summary = config_service.get_configuration_summary(config['study_name'])
        if summary:
            print(f"\n{summary['study_name']}:")
            print(f"  Created by: {summary['created_by']}")
            print(f"  Custom age groups: {summary['custom_age_groups_count']}")
            print(f"  Custom thresholds: {summary['custom_thresholds_count']}")
            print(f"  Has customizations: {summary['has_customizations']}")
    
    # 8. Update a configuration
    print("\n8. Updating configuration...")
    
    success = config_service.update_study_configuration(
        "Pediatric Development Study",
        normative_dataset="pediatric_norms_v3",
        exclusion_criteria=["excessive_motion", "scanner_artifacts", "incomplete_coverage", "age_mismatch"]
    )
    
    if success:
        print("✓ Configuration updated successfully!")
        
        # Verify update
        updated_config = config_service.get_study_configuration("Pediatric Development Study")
        print(f"New normative dataset: {updated_config['normative_dataset']}")
        print(f"Updated exclusion criteria: {len(updated_config['exclusion_criteria'])} items")
    
    # 9. Validate a new configuration
    print("\n9. Validating a new configuration...")
    
    test_config = StudyConfiguration(
        study_name="Test Validation Study",
        custom_age_groups=[
            {
                "name": "invalid_group",
                "min_age": 15.0,
                "max_age": 10.0  # Invalid: min > max
            }
        ],
        created_by="test_user"
    )
    
    validation_errors = config_service.validate_study_configuration(test_config)
    if validation_errors:
        print("✗ Configuration validation failed:")
        for error in validation_errors:
            print(f"  - {error}")
    else:
        print("✓ Configuration is valid!")
    
    print("\n=== Example completed ===")
    
    # Cleanup example database
    db_path = Path("data/example_config.db")
    if db_path.exists():
        db_path.unlink()
        print("Example database cleaned up.")


if __name__ == "__main__":
    main()