"""
Tests for configuration management API endpoints.

This module tests the REST API endpoints for study configuration
management including CRUD operations and validation.
"""

import pytest
import tempfile
import os
from fastapi.testclient import TestClient

from app.main import app
from app.routes import config_service
from app.models import AgeGroup


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
def client(temp_db):
    """Create test client with temporary database."""
    # Replace the global config service with one using temp database
    global config_service
    from app.config_service import ConfigurationService
    config_service = ConfigurationService(temp_db)
    
    return TestClient(app)


@pytest.fixture
def sample_config_data():
    """Sample configuration data for testing."""
    return {
        "study_name": "Test Study API",
        "normative_dataset": "test_norms",
        "custom_age_groups": [
            {
                "name": "children",
                "min_age": 6.0,
                "max_age": 12.0,
                "description": "Children group"
            }
        ],
        "custom_thresholds": [
            {
                "metric_name": "snr",
                "age_group": "pediatric",
                "warning_threshold": 10.0,
                "fail_threshold": 8.0,
                "direction": "higher_better"
            }
        ],
        "exclusion_criteria": ["excessive_motion"],
        "created_by": "test_user"
    }


class TestConfigurationEndpoints:
    """Test configuration management endpoints."""
    
    def test_create_configuration(self, client, sample_config_data):
        """Test creating a study configuration via API."""
        response = client.post("/api/configurations", json=sample_config_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["study_name"] == "Test Study API"
        assert data["normative_dataset"] == "test_norms"
        assert len(data["custom_age_groups"]) == 1
        assert len(data["custom_thresholds"]) == 1
    
    def test_create_invalid_configuration(self, client):
        """Test creating invalid configuration returns error."""
        invalid_data = {
            "study_name": "",  # Empty name
            "created_by": "test_user"
        }
        
        response = client.post("/api/configurations", json=invalid_data)
        assert response.status_code == 400
        assert "errors" in response.json()
    
    def test_get_all_configurations(self, client, sample_config_data):
        """Test getting all configurations."""
        # Create a configuration first
        client.post("/api/configurations", json=sample_config_data)
        
        response = client.get("/api/configurations")
        assert response.status_code == 200
        
        data = response.json()
        assert "configurations" in data
        assert data["total_count"] == 1
        assert data["configurations"][0]["study_name"] == "Test Study API"
    
    def test_get_specific_configuration(self, client, sample_config_data):
        """Test getting a specific configuration."""
        # Create configuration
        client.post("/api/configurations", json=sample_config_data)
        
        response = client.get("/api/configurations/Test Study API")
        assert response.status_code == 200
        
        data = response.json()
        assert data["study_name"] == "Test Study API"
        assert data["normative_dataset"] == "test_norms"
    
    def test_get_nonexistent_configuration(self, client):
        """Test getting non-existent configuration returns 404."""
        response = client.get("/api/configurations/Nonexistent")
        assert response.status_code == 404   
 
    def test_update_configuration(self, client, sample_config_data):
        """Test updating a configuration."""
        # Create configuration
        client.post("/api/configurations", json=sample_config_data)
        
        # Update configuration
        update_data = {
            "normative_dataset": "updated_norms",
            "exclusion_criteria": ["new_criterion"]
        }
        
        response = client.put("/api/configurations/Test Study API", json=update_data)
        assert response.status_code == 200
        
        data = response.json()
        assert data["normative_dataset"] == "updated_norms"
        assert data["exclusion_criteria"] == ["new_criterion"]
    
    def test_update_nonexistent_configuration(self, client):
        """Test updating non-existent configuration returns 404."""
        update_data = {"normative_dataset": "test"}
        
        response = client.put("/api/configurations/Nonexistent", json=update_data)
        assert response.status_code == 404
    
    def test_delete_configuration(self, client, sample_config_data):
        """Test deleting a configuration."""
        # Create configuration
        client.post("/api/configurations", json=sample_config_data)
        
        # Delete configuration
        response = client.delete("/api/configurations/Test Study API")
        assert response.status_code == 200
        assert "deleted successfully" in response.json()["message"]
        
        # Verify deletion
        response = client.get("/api/configurations/Test Study API")
        assert response.status_code == 404
    
    def test_delete_nonexistent_configuration(self, client):
        """Test deleting non-existent configuration returns 404."""
        response = client.delete("/api/configurations/Nonexistent")
        assert response.status_code == 404
    
    def test_get_study_age_groups(self, client, sample_config_data):
        """Test getting age groups for a study."""
        # Create configuration
        client.post("/api/configurations", json=sample_config_data)
        
        response = client.get("/api/configurations/Test Study API/age-groups")
        assert response.status_code == 200
        
        data = response.json()
        assert data["study_name"] == "Test Study API"
        assert len(data["age_groups"]) == 1
        assert data["is_custom"] is True
        assert data["age_groups"][0]["name"] == "children"
    
    def test_get_study_metric_thresholds(self, client, sample_config_data):
        """Test getting metric thresholds for a study."""
        # Create configuration
        client.post("/api/configurations", json=sample_config_data)
        
        response = client.get("/api/configurations/Test Study API/thresholds/snr")
        assert response.status_code == 200
        
        data = response.json()
        assert data["study_name"] == "Test Study API"
        assert data["metric_name"] == "snr"
        assert "thresholds" in data
    
    def test_validate_configuration(self, client):
        """Test configuration validation endpoint."""
        valid_data = {
            "study_name": "Valid Study",
            "normative_dataset": "test_norms",
            "custom_age_groups": [],
            "custom_thresholds": [],
            "exclusion_criteria": [],
            "created_by": "test_user"
        }
        
        response = client.post("/api/configurations/Valid Study/validate", json=valid_data)
        assert response.status_code == 200
        
        data = response.json()
        assert data["is_valid"] is True
        assert len(data["errors"]) == 0
    
    def test_validate_invalid_configuration(self, client):
        """Test validation of invalid configuration."""
        invalid_data = {
            "study_name": "",  # Empty name
            "normative_dataset": "test_norms",
            "custom_age_groups": [],
            "custom_thresholds": [],
            "exclusion_criteria": [],
            "created_by": "test_user"
        }
        
        response = client.post("/api/configurations/Invalid Study/validate", json=invalid_data)
        assert response.status_code == 200
        
        data = response.json()
        assert data["is_valid"] is False
        assert len(data["errors"]) > 0


class TestConfigurationErrorHandling:
    """Test error handling in configuration endpoints."""
    
    def test_create_duplicate_configuration(self, client, sample_config_data):
        """Test creating duplicate configuration returns error."""
        # Create first configuration
        response = client.post("/api/configurations", json=sample_config_data)
        assert response.status_code == 200
        
        # Try to create duplicate
        response = client.post("/api/configurations", json=sample_config_data)
        assert response.status_code == 400
        assert "errors" in response.json()
    
    def test_invalid_json_request(self, client):
        """Test invalid JSON request returns error."""
        response = client.post("/api/configurations", data="invalid json")
        assert response.status_code == 422  # Unprocessable Entity
    
    def test_missing_required_fields(self, client):
        """Test missing required fields returns validation error."""
        incomplete_data = {
            "normative_dataset": "test_norms"
            # Missing study_name and created_by
        }
        
        response = client.post("/api/configurations", json=incomplete_data)
        assert response.status_code == 422  # Validation error


if __name__ == "__main__":
    pytest.main([__file__])