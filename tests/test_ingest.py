import os
import pytest
from unittest.mock import patch, MagicMock
from google.cloud import bigquery
from ingest.competition_data_ingest import fetch_and_store
import sys

@pytest.fixture
def mock_env(monkeypatch):
    # Set environment variables for testing
    monkeypatch.setenv('API_KEY', 'test_api_key')
    monkeypatch.setenv('GCP_PROJECT_ID', 'test_project_id')
    monkeypatch.setenv('BIGQUERY_DATASET', 'test_dataset_id')
    monkeypatch.setenv('DOCKER_GOOGLE_APPLICATION_CREDENTIALS', 'test_credentials')
    monkeypatch.setenv('COMPETITION_IDS', '2021')
    monkeypatch.setenv('API_RESOURCES', 'competitions,matches')

# Mock requests.get and bigquery.Client to test fetch_and_store
@patch('ingest.competition_data_ingest.requests.get')
@patch('ingest.competition_data_ingest.bigquery.Client')
def test_fetch_and_store(mock_bigquery_client, mock_requests_get, mock_env):
    # Debug: Print the value of API_KEY to ensure it's correctly set
    print(f"API_KEY in test: {os.getenv('API_KEY')}")

    # Mock requests.get to simulate a successful API response
    mock_requests_get.return_value.status_code = 200
    mock_requests_get.return_value.json.return_value = {"key": "value"}

    # Mock bigquery.Client and insert_rows_json
    mock_bigquery_instance = mock_bigquery_client.return_value
    mock_bigquery_instance.insert_rows_json = MagicMock()

    # Call the fetch_and_store function
    fetch_and_store("competitions", "2021")
    
    # Assert that the API was called with the correct headers
    mock_requests_get.assert_called_once_with(
        "https://api.football-data.org/v4/competitions/2021",
        headers={'X-Auth-Token': 'test_api_key'}
    )
    
    # Assert that data was inserted into BigQuery
    mock_bigquery_instance.insert_rows_json.assert_called_once()
