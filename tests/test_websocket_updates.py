"""
Tests for WebSocket real-time updates functionality.

This module tests the WebSocket endpoints for real-time dashboard
and batch processing updates.
"""

import pytest
import json
import asyncio
from datetime import datetime
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, AsyncMock

from app.main import app
from app.models import ProcessedSubject, SubjectInfo, MRIQCMetrics, QualityAssessment, QualityStatus, ScanType


@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def sample_batch_status():
    """Create sample batch status data."""
    return {
        "batch_id": "test-batch-123",
        "status": "processing",
        "progress": {"completed": 5, "total": 10, "progress_percent": 50.0},
        "subjects_processed": 5,
        "total_subjects": 10,
        "errors": [],
        "created_at": datetime.now(),
        "started_at": datetime.now()
    }


class TestWebSocketDashboard:
    """Test WebSocket dashboard endpoint."""
    
    def test_websocket_dashboard_connection(self, client):
        """Test WebSocket dashboard connection establishment."""
        with client.websocket_connect("/api/ws/dashboard") as websocket:
            # Should receive connection confirmation
            data = websocket.receive_text()
            message = json.loads(data)
            
            assert message["type"] == "connection_established"
            assert "Connected to dashboard updates" in message["message"]
            assert "timestamp" in message
    
    def test_websocket_dashboard_ping_pong(self, client):
        """Test WebSocket ping/pong functionality."""
        with client.websocket_connect("/api/ws/dashboard") as websocket:
            # Skip connection confirmation
            websocket.receive_text()
            
            # Send ping
            ping_message = {"type": "ping"}
            websocket.send_text(json.dumps(ping_message))
            
            # Should receive pong
            data = websocket.receive_text()
            message = json.loads(data)
            
            assert message["type"] == "pong"
            assert "timestamp" in message
    
    def test_websocket_dashboard_batch_subscription(self, client):
        """Test subscribing to batch updates via dashboard WebSocket."""
        with client.websocket_connect("/api/ws/dashboard") as websocket:
            # Skip connection confirmation
            websocket.receive_text()
            
            # Subscribe to batch updates
            subscribe_message = {
                "type": "subscribe_batch",
                "batch_id": "test-batch-123"
            }
            websocket.send_text(json.dumps(subscribe_message))
            
            # Should receive subscription confirmation
            data = websocket.receive_text()
            message = json.loads(data)
            
            assert message["type"] == "subscription_confirmed"
            assert message["batch_id"] == "test-batch-123"
            assert "Subscribed to batch" in message["message"]


class TestWebSocketBatchStatus:
    """Test WebSocket batch status endpoint."""
    
    def test_websocket_batch_connection(self, client):
        """Test WebSocket batch connection."""
        batch_id = "test-batch-123"
        
        with client.websocket_connect(f"/api/ws/batch/{batch_id}") as websocket:
            # Should receive initial status (batch not found in this case)
            data = websocket.receive_text()
            message = json.loads(data)
            
            assert message["type"] == "batch_not_found"
            assert message["batch_id"] == batch_id
    
    def test_websocket_batch_with_existing_status(self, client, sample_batch_status):
        """Test WebSocket batch connection with existing batch status."""
        batch_id = "test-batch-123"
        
        with patch('app.routes.batch_status_store', {batch_id: sample_batch_status}):
            with client.websocket_connect(f"/api/ws/batch/{batch_id}") as websocket:
                # Should receive initial status
                data = websocket.receive_text()
                message = json.loads(data)
                
                assert message["type"] == "initial_status"
                assert message["batch_id"] == batch_id
                assert message["status"] == "processing"
                assert message["progress"]["completed"] == 5
                assert message["total_subjects"] == 10
    
    def test_websocket_batch_ping_pong(self, client):
        """Test WebSocket batch ping/pong functionality."""
        batch_id = "test-batch-123"
        
        with client.websocket_connect(f"/api/ws/batch/{batch_id}") as websocket:
            # Skip initial status message
            websocket.receive_text()
            
            # Send ping
            ping_message = {"type": "ping"}
            websocket.send_text(json.dumps(ping_message))
            
            # Should receive pong with batch_id
            data = websocket.receive_text()
            message = json.loads(data)
            
            assert message["type"] == "pong"
            assert message["batch_id"] == batch_id
            assert "timestamp" in message


class TestWebSocketUpdates:
    """Test WebSocket update broadcasting."""
    
    @pytest.mark.asyncio
    async def test_batch_progress_broadcast(self):
        """Test broadcasting batch progress updates."""
        from app.routes import manager
        
        batch_id = "test-batch-123"
        
        # Mock WebSocket connection
        mock_websocket = AsyncMock()
        manager.batch_subscribers[batch_id] = [mock_websocket]
        
        # Broadcast progress update
        progress_message = json.dumps({
            "type": "batch_progress_update",
            "batch_id": batch_id,
            "progress": {"completed": 7, "total": 10, "progress_percent": 70.0},
            "current_subject": "sub-007"
        })
        
        await manager.broadcast_to_batch(progress_message, batch_id)
        
        # Verify message was sent
        mock_websocket.send_text.assert_called_once_with(progress_message)
    
    @pytest.mark.asyncio
    async def test_batch_completion_broadcast(self):
        """Test broadcasting batch completion updates."""
        from app.routes import manager
        
        batch_id = "test-batch-123"
        
        # Mock WebSocket connection
        mock_websocket = AsyncMock()
        manager.batch_subscribers[batch_id] = [mock_websocket]
        
        # Broadcast completion update
        completion_message = json.dumps({
            "type": "batch_completed",
            "batch_id": batch_id,
            "subjects_processed": 10,
            "errors_count": 0,
            "completion_time": datetime.now().isoformat()
        })
        
        await manager.broadcast_to_batch(completion_message, batch_id)
        
        # Verify message was sent
        mock_websocket.send_text.assert_called_once_with(completion_message)
    
    @pytest.mark.asyncio
    async def test_dashboard_update_broadcast(self):
        """Test broadcasting dashboard updates."""
        from app.routes import manager
        
        # Mock WebSocket connections
        mock_websocket1 = AsyncMock()
        mock_websocket2 = AsyncMock()
        manager.active_connections = [mock_websocket1, mock_websocket2]
        
        # Broadcast dashboard update
        dashboard_message = json.dumps({
            "type": "dashboard_update",
            "message": "New batch completed with 10 subjects"
        })
        
        await manager.broadcast_dashboard_update(dashboard_message)
        
        # Verify message was sent to all connections
        mock_websocket1.send_text.assert_called_once_with(dashboard_message)
        mock_websocket2.send_text.assert_called_once_with(dashboard_message)
    
    @pytest.mark.asyncio
    async def test_error_broadcast(self):
        """Test broadcasting error updates."""
        from app.routes import manager
        
        batch_id = "test-batch-123"
        
        # Mock WebSocket connection
        mock_websocket = AsyncMock()
        manager.batch_subscribers[batch_id] = [mock_websocket]
        
        # Broadcast error update
        error_message = json.dumps({
            "type": "processing_error",
            "batch_id": batch_id,
            "error": {
                "error_type": "validation_error",
                "message": "Invalid metric value",
                "error_code": "VAL_001"
            },
            "subject_id": "sub-005"
        })
        
        await manager.broadcast_to_batch(error_message, batch_id)
        
        # Verify message was sent
        mock_websocket.send_text.assert_called_once_with(error_message)


class TestConnectionManager:
    """Test WebSocket connection manager functionality."""
    
    @pytest.mark.asyncio
    async def test_connection_management(self):
        """Test connection and disconnection management."""
        from app.routes import ConnectionManager
        
        manager = ConnectionManager()
        mock_websocket = AsyncMock()
        
        # Test batch subscription connection
        batch_id = "test-batch"
        await manager.connect(mock_websocket, batch_id)
        assert mock_websocket in manager.active_connections
        assert batch_id in manager.batch_subscribers
        assert mock_websocket in manager.batch_subscribers[batch_id]
        
        # Test disconnection
        manager.disconnect(mock_websocket, batch_id)
        assert mock_websocket not in manager.batch_subscribers[batch_id]
        assert mock_websocket not in manager.active_connections
        
        # Test regular connection without batch
        mock_websocket2 = AsyncMock()
        await manager.connect(mock_websocket2)
        assert mock_websocket2 in manager.active_connections
        
        manager.disconnect(mock_websocket2)
        assert mock_websocket2 not in manager.active_connections
    
    @pytest.mark.asyncio
    async def test_connection_cleanup_on_error(self):
        """Test connection cleanup when WebSocket errors occur."""
        from app.routes import manager
        
        batch_id = "test-batch-123"
        
        # Mock WebSocket that raises exception
        mock_websocket_error = AsyncMock()
        mock_websocket_error.send_text.side_effect = Exception("Connection closed")
        
        # Mock working WebSocket
        mock_websocket_ok = AsyncMock()
        
        manager.batch_subscribers[batch_id] = [mock_websocket_error, mock_websocket_ok]
        
        # Broadcast message
        test_message = "test message"
        await manager.broadcast_to_batch(test_message, batch_id)
        
        # Error connection should be removed, working connection should remain
        assert mock_websocket_error not in manager.batch_subscribers[batch_id]
        assert mock_websocket_ok in manager.batch_subscribers[batch_id]
        
        # Working connection should have received the message
        mock_websocket_ok.send_text.assert_called_once_with(test_message)


class TestRealTimeIntegration:
    """Test integration of real-time updates with processing."""
    
    @pytest.mark.asyncio
    async def test_processing_with_websocket_updates(self):
        """Test that processing sends appropriate WebSocket updates."""
        from app.routes import process_subjects_background, manager
        
        # Create sample subjects
        subjects = []
        for i in range(3):
            subject_info = SubjectInfo(
                subject_id=f"sub-{i:03d}",
                age=25.0,
                scan_type=ScanType.T1W
            )
            
            raw_metrics = MRIQCMetrics(snr=10.0 + i)
            
            quality_assessment = QualityAssessment(
                overall_status=QualityStatus.PASS,
                metric_assessments={"snr": QualityStatus.PASS},
                composite_score=75.0,
                confidence=0.85
            )
            
            subject = ProcessedSubject(
                subject_info=subject_info,
                raw_metrics=raw_metrics,
                quality_assessment=quality_assessment
            )
            
            subjects.append(subject)
        
        batch_id = "test-batch-integration"
        
        # Mock WebSocket connection
        mock_websocket = AsyncMock()
        manager.batch_subscribers[batch_id] = [mock_websocket]
        
        # Mock the stores
        with patch('app.routes.batch_status_store', {}) as mock_batch_store, \
             patch('app.routes.processed_subjects_store', {}) as mock_subjects_store:
            
            # Initialize batch status
            mock_batch_store[batch_id] = {
                'batch_id': batch_id,
                'status': 'pending',
                'progress': {'completed': 0, 'total': len(subjects), 'progress_percent': 0},
                'subjects_processed': 0,
                'total_subjects': len(subjects),
                'errors': [],
                'created_at': datetime.now()
            }
            
            # Run background processing
            await process_subjects_background(subjects, batch_id, apply_quality_assessment=False)
            
            # Verify WebSocket messages were sent
            assert mock_websocket.send_text.call_count >= 2  # At least start and completion messages
            
            # Check that completion message was sent
            calls = mock_websocket.send_text.call_args_list
            completion_calls = [call for call in calls if "batch_completed" in str(call)]
            assert len(completion_calls) > 0


if __name__ == "__main__":
    pytest.main([__file__])