import pytest
import requests
from unittest.mock import patch, MagicMock
import os

# Patch credentials BEFORE the module imports to avoid file access errors
@patch('src.ingest.competition_data_ingest.service_account.Credentials.from_service_account_info', return_value=MagicMock())
@patch('src.ingest.competition_data_ingest.service_account.Credentials.from_service_account_file', return_value=MagicMock())
@patch('os.getenv', side_effect=lambda key, default=None: {
    "API_KEY": "fake_api_key",
    "COMPETITION_IDS": "2021",
    "API_RESOURCES": "matches",
    "DOCKER_GOOGLE_APPLICATION_CREDENTIALS": "mock_credentials"
}.get(key, default))
def test_build_url_and_table(mock_getenv, mock_service_account_info, mock_service_account_file):
    """
    Test the build_url_and_table function constructs the correct API URL
    and corresponding BigQuery table ID.
    """
    from src.ingest.competition_data_ingest import build_url_and_table
    api_url, table_id = build_url_and_table('matches', '2021')
    assert api_url == "https://api.football-data.org/v4/competitions/2021/matches"
    assert table_id == "project.dataset.raw_football_matches"


# Mock the BigQuery client and test table creation
@patch('src.ingest.competition_data_ingest.bigquery.Client')
@patch('os.getenv', side_effect=lambda key, default=None: {
    "API_KEY": "fake_api_key",
    "COMPETITION_IDS": "2021",
    "API_RESOURCES": "matches",
    "DOCKER_GOOGLE_APPLICATION_CREDENTIALS": "mock_credentials"
}.get(key, default))
@patch('src.ingest.competition_data_ingest.service_account.Credentials.from_service_account_file', return_value=MagicMock())
def test_create_table_if_not_exists(mock_getenv, mock_bigquery_client, mock_service_account_file):
    """
    Test the behavior of create_table_if_not_exists, ensuring it correctly checks for
    the table and creates it if it doesn't exist.
    """
    from src.ingest.competition_data_ingest import create_table_if_not_exists
    mock_client_instance = MagicMock()
    mock_bigquery_client.return_value = mock_client_instance

    create_table_if_not_exists("project.dataset.raw_football_matches")
    mock_bigquery_client.return_value.get_table.assert_called_once_with("project.dataset.raw_football_matches")


# Test the API fetching and BigQuery insertion with mock data
@patch('src.ingest.competition_data_ingest.requests.get')
@patch('src.ingest.competition_data_ingest.bigquery.Client')
@patch('src.ingest.competition_data_ingest.service_account.Credentials.from_service_account_file', return_value=MagicMock())
@patch('os.getenv', side_effect=lambda key, default=None: {
    "API_KEY": "fake_api_key",
    "COMPETITION_IDS": "2021",
    "API_RESOURCES": "matches",
    "DOCKER_GOOGLE_APPLICATION_CREDENTIALS": "mock_credentials"
}.get(key, default))
def test_fetch_and_store(mock_getenv, mock_service_account_file, mock_bigquery_client, mock_requests_get):
    """
    Test fetch_and_store function to ensure the API data is correctly fetched and inserted into BigQuery.
    """
    from src.ingest.competition_data_ingest import fetch_and_store

    # Mock API response
    mock_response = MagicMock()
    mock_response.json.return_value = {"count": 380, "matches": [{"id": 123, "status": "SCHEDULED"}]}
    mock_response.raise_for_status = MagicMock()
    mock_requests_get.return_value = mock_response

    # Mock BigQuery client
    mock_client_instance = MagicMock()
    mock_bigquery_client.return_value = mock_client_instance

    # Call the function
    fetch_and_store("matches", "2021")

    # Verify API call
    mock_requests_get.assert_called_once_with("https://api.football-data.org/v4/competitions/2021/matches",
                                              headers={'X-Auth-Token': "fake_api_key"})

    # Verify BigQuery insertion
    mock_client_instance.insert_rows_json.assert_called_once_with("project.dataset.raw_football_matches",
                                                                  [{'endpoint': "https://api.football-data.org/v4/competitions/2021/matches",
                                                                    'raw_json': '{"count":380,"matches":[{"id":123,"status":"SCHEDULED"}]}',
                                                                    'loaded_date': '2024-08-13T19:00:00Z'}])


# Test rate limit retry behavior (429 error)
@patch('src.ingest.competition_data_ingest.requests.get')
@patch('src.ingest.competition_data_ingest.time.sleep', return_value=None)  # Mock time.sleep to avoid delays in testing
@patch('src.ingest.competition_data_ingest.service_account.Credentials.from_service_account_file', return_value=MagicMock())
@patch('os.getenv', side_effect=lambda key, default=None: {
    "API_KEY": "fake_api_key",
    "COMPETITION_IDS": "2021",
    "API_RESOURCES": "matches",
    "DOCKER_GOOGLE_APPLICATION_CREDENTIALS": "mock_credentials"
}.get(key, default))
def test_rate_limit_retry(mock_getenv, mock_service_account_file, mock_sleep, mock_requests_get):
    """
    Test fetch_and_store for retry behavior when hitting a rate limit (HTTP 429).
    """
    from src.ingest.competition_data_ingest import fetch_and_store

    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_requests_get.return_value = mock_response

    # Call fetch_and_store and expect retry behavior
    fetch_and_store("matches", "2021")

    # Ensure that retries occurred
    assert mock_requests_get.call_count > 1
    assert mock_requests_get.call_count <= 5  # Ensure it doesn't retry more than 5 times
    mock_sleep.assert_called()  # Ensure sleep was called for backoff
