import os
import pytest
from unittest.mock import patch
from ingest.competition_data_ingest import fetch_and_store, create_table_if_not_exists, build_url_and_table

@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv('API_KEY', 'test_api_key')
    monkeypatch.setenv('GCP_PROJECT_ID', 'test_project_id')
    monkeypatch.setenv('BIGQUERY_DATASET', 'test_dataset_id')
    monkeypatch.setenv('DOCKER_GOOGLE_APPLICATION_CREDENTIALS', 'test_credentials')
    monkeypatch.setenv('COMPETITION_IDS', '2021')
    monkeypatch.setenv('API_RESOURCES', 'competitions,matches')

# Mock bigquery.Client from the appropriate module
@patch('ingest.competition_data_ingest.bigquery.Client')
def test_create_table_if_not_exists(mock_bigquery_client, mock_env):
    # Mock instance of bigquery.Client
    mock_instance = mock_bigquery_client.return_value
    mock_instance.get_table.side_effect = Exception("Table not found.")
    
    # Call the function
    create_table_if_not_exists("test_project_id.test_dataset.raw_table")
    
    # Assert that create_table was called after the table was not found
    mock_instance.create_table.assert_called()

# Mock requests.get
@patch('ingest.competition_data_ingest.requests.get')
def test_fetch_and_store(mock_get, mock_env):
    # Mock the API response
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"key": "value"}
    
    # Call the function
    fetch_and_store("competitions", "2021")
    
    # Assert that the request was made
    assert mock_get.called
