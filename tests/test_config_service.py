"""
Tests for the configuration service module.

This module tests study configuration creation, validation, and management
functionality including custom age groups and quality thresholds.
"""

import pytest
import tempfile
import os
from datetime import datetime
from pathlib import Path

from app.config_service import ConfigurationService, ConfigurationValidationError
from app.models import StudyConfiguration, QualityThresholds, AgeGroup
from app.database import NormativeDatabase


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    yield db_path
    
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def config_service(temp_db):
    """Create a configuration service with temporary database."""
    return ConfigurationService(temp_db)


@pytest.fixture
def sample_study_config():
    """Create a sample study configuration."""
    return StudyConfiguration(
        study_name="Test Study",
        normative_dataset="test_norms",
        custom_age_groups=[
            {"name": "children", "min_age": 6.0, "max_age": 12.0, "description": "Children"},
            {"name": "teens", "min_age": 13.0, "max_age": 17.0, "description": "Teenagers"}
        ],
        custom_thresholds=[
            QualityThresholds(
                metric_name="snr",
                age_group="children",  # Use custom age group name
                warning_threshold=10.0,
                fail_threshold=8.0,
                direction="higher_better"
            )
        ],
        exclusion_criteria=["excessive_motion", "artifacts"],
        created_by="test_user"
    )


class TestConfigurationValidation:
    """Test configuration validation functionality."""
    
    def test_valid_configuration(self, config_service, sample_study_config):
        """Test validation of a valid configuration."""
        errors = config_service.validate_study_configuration(sample_study_config)
        assert len(errors) == 0
    
    def test_empty_study_name(self, config_service):
        """Test validation fails for empty study name."""
        config = StudyConfiguration(
            study_name="",
            created_by="test_user"
        )
        errors = config_service.validate_study_configuration(config)
        assert any("Study name cannot be empty" in error for error in errors)
    
    def test_long_study_name(self, config_service):
        """Test validation fails for overly long study name."""
        config = StudyConfiguration(
            study_name="x" * 101,  # Exceeds 100 character limit
            created_by="test_user"
        )
        errors = config_service.validate_study_configuration(config)
        assert any("cannot exceed 100 characters" in error for error in errors)
    
    def test_invalid_age_groups(self, config_service):
        """Test validation of invalid custom age groups."""
        config = StudyConfiguration(
            study_name="Test Study",
            custom_age_groups=[
                {"name": "invalid", "min_age": 15.0, "max_age": 10.0},  # min > max
                {"name": "", "min_age": 5.0, "max_age": 10.0},  # empty name
                {"name": "negative", "min_age": -5.0, "max_age": 10.0},  # negative age
            ],
            created_by="test_user"
        )
        errors = config_service.validate_study_configuration(config)
        assert len(errors) >= 3
        assert any("min_age must be less than max_age" in error for error in errors)
        assert any("Name must be a non-empty string" in error for error in errors)
        assert any("Age values cannot be negative" in error for error in errors)
    
    def test_overlapping_age_groups(self, config_service):
        """Test validation of overlapping age groups."""
        config = StudyConfiguration(
            study_name="Test Study",
            custom_age_groups=[
                {"name": "group1", "min_age": 5.0, "max_age": 15.0},
                {"name": "group2", "min_age": 10.0, "max_age": 20.0},  # Overlaps with group1
            ],
            created_by="test_user"
        )
        errors = config_service.validate_study_configuration(config)
        assert any("Overlapping age ranges" in error for error in errors)
    
    def test_duplicate_age_group_names(self, config_service):
        """Test validation of duplicate age group names."""
        config = StudyConfiguration(
            study_name="Test Study",
            custom_age_groups=[
                {"name": "duplicate", "min_age": 5.0, "max_age": 10.0},
                {"name": "duplicate", "min_age": 15.0, "max_age": 20.0},
            ],
            created_by="test_user"
        )
        errors = config_service.validate_study_configuration(config)
        assert any("Duplicate age group name" in error for error in errors)
    
    def test_invalid_thresholds(self, config_service):
        """Test validation of invalid quality thresholds."""
        config = StudyConfiguration(
            study_name="Test Study",
            custom_thresholds=[
                QualityThresholds(
                    metric_name="invalid_metric",
                    age_group=AgeGroup.YOUNG_ADULT,
                    warning_threshold=10.0,
                    fail_threshold=8.0,
                    direction="higher_better"
                ),
                QualityThresholds(
                    metric_name="snr",
                    age_group=AgeGroup.YOUNG_ADULT,
                    warning_threshold=8.0,  # Should be > fail for higher_better
                    fail_threshold=10.0,
                    direction="higher_better"
                )
            ],
            created_by="test_user"
        )
        errors = config_service.validate_study_configuration(config)
        assert any("Invalid metric name" in error for error in errors)
        assert any("warning threshold must be greater than fail threshold" in error for error in errors)
    
    def test_empty_created_by(self, config_service):
        """Test validation fails for empty created_by field."""
        config = StudyConfiguration(
            study_name="Test Study",
            created_by=""
        )
        errors = config_service.validate_study_configuration(config)
        assert any("Created by field cannot be empty" in error for error in errors)


class TestConfigurationCRUD:
    """Test configuration CRUD operations."""
    
    def test_create_configuration(self, config_service, sample_study_config):
        """Test creating a study configuration."""
        success, errors = config_service.create_study_configuration(sample_study_config)
        assert success
        assert len(errors) == 0
        
        # Verify configuration was created
        retrieved = config_service.get_study_configuration("Test Study")
        assert retrieved is not None
        assert retrieved['study_name'] == "Test Study"
        assert retrieved['normative_dataset'] == "test_norms"
        assert len(retrieved['custom_age_groups']) == 2
        assert len(retrieved['custom_thresholds']) == 1
    
    def test_create_duplicate_configuration(self, config_service, sample_study_config):
        """Test creating duplicate configuration fails."""
        # Create first configuration
        success, errors = config_service.create_study_configuration(sample_study_config)
        assert success
        
        # Try to create duplicate
        success, errors = config_service.create_study_configuration(sample_study_config)
        assert not success
        assert any("already exists" in error for error in errors)
    
    def test_get_nonexistent_configuration(self, config_service):
        """Test getting non-existent configuration returns None."""
        result = config_service.get_study_configuration("Nonexistent Study")
        assert result is None
    
    def test_get_all_configurations(self, config_service):
        """Test getting all configurations."""
        # Create multiple configurations
        config1 = StudyConfiguration(
            study_name="Study 1",
            created_by="user1"
        )
        config2 = StudyConfiguration(
            study_name="Study 2",
            created_by="user2"
        )
        
        config_service.create_study_configuration(config1)
        config_service.create_study_configuration(config2)
        
        all_configs = config_service.get_all_study_configurations()
        assert len(all_configs) == 2
        study_names = [config['study_name'] for config in all_configs]
        assert "Study 1" in study_names
        assert "Study 2" in study_names
    
    def test_update_configuration(self, config_service, sample_study_config):
        """Test updating a configuration."""
        # Create configuration
        config_service.create_study_configuration(sample_study_config)
        
        # Update configuration
        success = config_service.update_study_configuration(
            study_name="Test Study",
            normative_dataset="updated_norms",
            exclusion_criteria=["new_criterion"]
        )
        assert success
        
        # Verify update
        updated = config_service.get_study_configuration("Test Study")
        assert updated['normative_dataset'] == "updated_norms"
        assert updated['exclusion_criteria'] == ["new_criterion"]
    
    def test_update_nonexistent_configuration(self, config_service):
        """Test updating non-existent configuration fails."""
        success = config_service.update_study_configuration(
            study_name="Nonexistent",
            normative_dataset="test"
        )
        assert not success
    
    def test_delete_configuration(self, config_service, sample_study_config):
        """Test deleting a configuration."""
        # Create configuration
        config_service.create_study_configuration(sample_study_config)
        
        # Delete configuration
        success = config_service.delete_study_configuration("Test Study")
        assert success
        
        # Verify deletion
        result = config_service.get_study_configuration("Test Study")
        assert result is None
    
    def test_delete_nonexistent_configuration(self, config_service):
        """Test deleting non-existent configuration fails."""
        success = config_service.delete_study_configuration("Nonexistent")
        assert not success


class TestConfigurationApplication:
    """Test configuration application functionality."""
    
    def test_get_age_group_for_study(self, config_service, sample_study_config):
        """Test getting age group for a subject in a study."""
        # Create configuration with custom age groups
        config_service.create_study_configuration(sample_study_config)
        
        # Test age group assignment
        age_group = config_service.get_age_group_for_study("Test Study", 8.0)
        assert age_group is not None
        assert age_group['name'] == "children"
        assert age_group['min_age'] == 6.0
        assert age_group['max_age'] == 12.0
        
        # Test age outside range
        age_group = config_service.get_age_group_for_study("Test Study", 25.0)
        assert age_group is None
    
    def test_get_age_group_default_study(self, config_service):
        """Test getting age group for study without custom groups."""
        # Create configuration without custom age groups
        config = StudyConfiguration(
            study_name="Default Study",
            created_by="test_user"
        )
        config_service.create_study_configuration(config)
        
        # Should use default age groups
        age_group = config_service.get_age_group_for_study("Default Study", 25.0)
        assert age_group is not None
        assert age_group['name'] == "young_adult"
    
    def test_get_quality_thresholds_for_study(self, config_service, sample_study_config):
        """Test getting quality thresholds for a study."""
        # Create configuration with custom thresholds
        config_service.create_study_configuration(sample_study_config)
        
        # Test custom threshold retrieval
        thresholds = config_service.get_quality_thresholds_for_study(
            "Test Study", "snr", "children"
        )
        assert thresholds is not None
        assert thresholds['warning_threshold'] == 10.0
        assert thresholds['fail_threshold'] == 8.0
        assert thresholds['direction'] == "higher_better"
    
    def test_apply_study_configuration(self, config_service, sample_study_config):
        """Test applying study configuration to subject data."""
        # Create configuration
        config_service.create_study_configuration(sample_study_config)
        
        # Test subject data with age in custom age group
        subject_data = {
            'subject_id': 'sub-001',
            'age': 8.0,
            'scan_type': 'T1w'
        }
        
        updated_data = config_service.apply_study_configuration("Test Study", subject_data)
        
        assert 'study_age_group' in updated_data
        assert updated_data['study_age_group'] == 'children'
        assert 'study_configuration' in updated_data
        assert updated_data['study_configuration']['study_name'] == "Test Study"
        assert updated_data['study_configuration']['has_custom_age_groups'] is True
    
    def test_apply_nonexistent_study_configuration(self, config_service):
        """Test applying non-existent study configuration."""
        subject_data = {'subject_id': 'sub-001', 'age': 25.0}
        
        # Should return original data unchanged
        updated_data = config_service.apply_study_configuration("Nonexistent", subject_data)
        assert updated_data == subject_data


class TestConfigurationSummary:
    """Test configuration summary functionality."""
    
    def test_get_configuration_summary(self, config_service, sample_study_config):
        """Test getting configuration summary."""
        # Create configuration
        config_service.create_study_configuration(sample_study_config)
        
        summary = config_service.get_configuration_summary("Test Study")
        assert summary is not None
        assert summary['study_name'] == "Test Study"
        assert summary['custom_age_groups_count'] == 2
        assert summary['custom_thresholds_count'] == 1
        assert summary['exclusion_criteria_count'] == 2
        assert summary['has_customizations'] is True
    
    def test_get_summary_nonexistent_configuration(self, config_service):
        """Test getting summary for non-existent configuration."""
        summary = config_service.get_configuration_summary("Nonexistent")
        assert summary is None


class TestConfigurationUpdateValidation:
    """Test configuration update validation."""
    
    def test_validate_configuration_update(self, config_service, sample_study_config):
        """Test validating configuration updates."""
        # Create configuration
        config_service.create_study_configuration(sample_study_config)
        
        # Test valid update
        errors = config_service.validate_configuration_update(
            "Test Study",
            {"normative_dataset": "new_dataset"}
        )
        assert len(errors) == 0
        
        # Test invalid update
        errors = config_service.validate_configuration_update(
            "Test Study",
            {"normative_dataset": ""}
        )
        assert any("cannot be empty" in error for error in errors)
    
    def test_validate_update_nonexistent_configuration(self, config_service):
        """Test validating update for non-existent configuration."""
        errors = config_service.validate_configuration_update(
            "Nonexistent",
            {"normative_dataset": "test"}
        )
        assert any("not found" in error for error in errors)


class TestDatabaseIntegration:
    """Test database integration for configuration management."""
    
    def test_database_schema_creation(self, temp_db):
        """Test that configuration tables are created properly."""
        db = NormativeDatabase(temp_db)
        
        with db.get_connection() as conn:
            # Check that configuration tables exist
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name LIKE '%configuration%'
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            expected_tables = [
                'study_configurations',
                'custom_age_groups',
                'custom_quality_thresholds'
            ]
            
            for table in expected_tables:
                assert table in tables
    
    def test_custom_age_group_cascade_delete(self, config_service, sample_study_config):
        """Test that custom age groups are deleted when configuration is deleted."""
        # Create configuration
        config_service.create_study_configuration(sample_study_config)
        
        # Verify custom age groups exist
        age_groups = config_service.db.get_custom_age_groups_for_study("Test Study")
        assert len(age_groups) == 2
        
        # Delete configuration
        config_service.delete_study_configuration("Test Study")
        
        # Verify custom age groups are gone
        age_groups = config_service.db.get_custom_age_groups_for_study("Test Study")
        assert len(age_groups) == 0
    
    def test_custom_threshold_cascade_delete(self, config_service, sample_study_config):
        """Test that custom thresholds are deleted when configuration is deleted."""
        # Create configuration
        config_service.create_study_configuration(sample_study_config)
        
        # Verify custom thresholds exist
        thresholds = config_service.db.get_custom_thresholds_for_study("Test Study")
        assert len(thresholds) == 1
        
        # Delete configuration
        config_service.delete_study_configuration("Test Study")
        
        # Verify custom thresholds are gone
        thresholds = config_service.db.get_custom_thresholds_for_study("Test Study")
        assert len(thresholds) == 0


if __name__ == "__main__":
    pytest.main([__file__])