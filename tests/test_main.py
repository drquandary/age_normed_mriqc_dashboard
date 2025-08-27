import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_root():
    response = client.get('/')
    assert response.status_code == 200
    assert 'Age-Normed MRIQC Dashboard' in response.json().get('detail', '')


def test_process_data():
    payload = {'sample_id': 1, 'value': 2.0}
    response = client.post('/api/process', json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data['sample_id'] == 1
    assert data['result'] == 4.0


def test_health():
    response = client.get('/api/health')
    assert response.status_code == 200
    assert response.json().get('status') == 'ok'


def test_process_includes_csv_value_when_present():
    payload = {'sample_id': 1, 'value': 1.0}
    response = client.post('/api/process', json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data.get('csv_value') == 0.5
