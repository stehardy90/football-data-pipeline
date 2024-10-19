import os
import pytest
from unittest.mock import patch, MagicMock
from ingest.competition_data_ingest import fetch_and_store, create_table_if_not_exists, build_url_and_table
import sys

@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv('API_KEY', 'test_api_key')
    monkeypatch.setenv('GCP_PROJECT_ID', 'test_project_id')
    monkeypatch.setenv('BIGQUERY_DATASET', 'test_dataset_id')
    monkeypatch.setenv('DOCKER_GOOGLE_APPLICATION_CREDENTIALS', 'test_credentials')
    monkeypatch.setenv('COMPETITION_IDS', '2021')
    monkeypatch.setenv('API_RESOURCES', 'competitions,matches')

# Mock bigquery.Client and service_account.Credentials
@patch('ingest.competition_data_ingest.bigquery.Client')
@patch('ingest.competition_data_ingest.service_account.Credentials')
def test_create_table_if_not_exists(mock_credentials, mock_bigquery_client, mock_env):
    # Mock instance of bigquery.Client
    mock_instance = mock_bigquery_client.return_value

    # Force the output to be flushed
    print("Mocking bigquery.Client...", file=sys.stderr, flush=True)

    # Mock get_table to raise an exception to simulate "table not found"
    mock_instance.get_table.side_effect = Exception("Table not found.")
    
    # Mock create_table to prevent real API calls
    mock_instance.create_table = MagicMock()

    # Mock credentials to prevent any real credential issues
    mock_credentials.from_service_account_file.return_value = MagicMock()

    # Add debugging to capture the instance type issues
    print("Attempting to create the table...", file=sys.stderr, flush=True)
    
    # Call the function
    try:
        create_table_if_not_exists("test_project_id.test_dataset.raw_table")
    except Exception as e:
        # Print the exception to better understand the flow
        print(f"Exception during create_table_if_not_exists: {e}", file=sys.stderr, flush=True)
    
    # Debugging statement to confirm the flow reached this point
    print("Checking if create_table was called...", file=sys.stderr, flush=True)

    # Assert that create_table was called after the table was not found
    mock_instance.create_table.assert_called()

# Mock requests.get for the fetch_and_store function
@patch('ingest.competition_data_ingest.requests.get')
def test_fetch_and_store(mock_get, mock_env):
    # Mock the API response
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"key": "value"}
    
    # Call the function
    fetch_and_store("competitions", "2021")
    
    # Assert that the request was made
    assert mock_get.called
