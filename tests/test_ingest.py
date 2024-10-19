# test_ingest.py
import os
import pytest
from unittest.mock import patch
from src.ingest.competition_data_ingest import fetch_and_store, create_table_if_not_exists, build_url_and_table

@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv('API_KEY', 'test_api_key')
    monkeypatch.setenv('GCP_PROJECT_ID', 'test_project_id')
    monkeypatch.setenv('BIGQUERY_DATASET', 'test_dataset_id')
    monkeypatch.setenv('DOCKER_GOOGLE_APPLICATION_CREDENTIALS', 'test_credentials')
    monkeypatch.setenv('COMPETITION_IDS', '2021')
    monkeypatch.setenv('API_RESOURCES', 'competitions,matches')

@patch('src.ingest.client')
def test_create_table_if_not_exists(mock_client, mock_env):
    mock_client.get_table.side_effect = Exception("Table not found.")
    create_table_if_not_exists("test_project_id.test_dataset.raw_table")
    mock_client.create_table.assert_called()

@patch('src.ingest.requests.get')
def test_fetch_and_store(mock_get, mock_env):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"key": "value"}
    fetch_and_store("competitions", "2021")
    assert mock_get.called
