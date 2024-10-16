import pytest
import requests
from unittest.mock import patch, MagicMock
from src.ingest.competition_data_ingest import fetch_and_store, build_url_and_table, create_table_if_not_exists
import os

# Mock constants
MOCK_COMPETITION_ID = '2021'
MOCK_RESOURCE = 'matches'
MOCK_API_URL = f"https://api.football-data.org/v4/competitions/{MOCK_COMPETITION_ID}/{MOCK_RESOURCE}"
MOCK_TABLE_ID = 'project.dataset.raw_football_matches'
MOCK_API_KEY = 'fake_api_key'

# Mock data for testing
MOCK_API_RESPONSE = {
    "count": 380,
    "matches": [{"id": 123, "status": "SCHEDULED", "utcDate": "2024-08-13T19:00:00Z"}]
}

MOCK_BIGQUERY_DATA = {
    'endpoint': MOCK_API_URL,
    'raw_json': '{"count":380,"matches":[{"id":123,"status":"SCHEDULED","utcDate":"2024-08-13T19:00:00Z"}]}',
    'loaded_date': '2024-08-13T19:00:00Z'
}

# Patch os.getenv globally for all tests to ensure environment variables are set
@patch('os.getenv', side_effect=lambda key, default=None: {
    "API_KEY": MOCK_API_KEY,
    "COMPETITION_IDS": MOCK_COMPETITION_ID,
    "API_RESOURCES": MOCK_RESOURCE
}.get(key, default))
def test_build_url_and_table(mock_getenv):
    """
    Test if the build_url_and_table function constructs the correct API URL
    and corresponding BigQuery table ID.
    """
    api_url, table_id = build_url_and_table(MOCK_RESOURCE, MOCK_COMPETITION_ID)
    assert api_url == MOCK_API_URL
    assert table_id == MOCK_TABLE_ID

# Mock the BigQuery client and test table creation
@patch('src.ingest.competition_data_ingest.bigquery.Client')
@patch('os.getenv', side_effect=lambda key, default=None: {
    "API_KEY": MOCK_API_KEY,
    "COMPETITION_IDS": MOCK_COMPETITION_ID,
    "API_RESOURCES": MOCK_RESOURCE
}.get(key, default))
def test_create_table_if_not_exists(mock_getenv, mock_bigquery_client):
    """
    Test the behavior of create_table_if_not_exists, ensuring it correctly checks for
    the table and creates it if it doesn't exist.
    """
    mock_client_instance = MagicMock()
    mock_bigquery_client.return_value = mock_client_instance

    create_table_if_not_exists(MOCK_TABLE_ID)
    mock_bigquery_client.return_value.get_table.assert_called_once_with(MOCK_TABLE_ID)

# Test the API fetching and BigQuery insertion with mock data
@patch('src.ingest.competition_data_ingest.requests.get')
@patch('src.ingest.competition_data_ingest.bigquery.Client')
@patch('os.getenv', side_effect=lambda key, default=None: {
    "API_KEY": MOCK_API_KEY,
    "COMPETITION_IDS": MOCK_COMPETITION_ID,
    "API_RESOURCES": MOCK_RESOURCE
}.get(key, default))
def test_fetch_and_store(mock_getenv, mock_bigquery_client, mock_requests_get):
    """
    Test fetch_and_store function to ensure the API data is correctly fetched and inserted into BigQuery.
    """
    # Mock API response
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_API_RESPONSE
    mock_response.raise_for_status = MagicMock()
    mock_requests_get.return_value = mock_response

    # Mock BigQuery client
    mock_client_instance = MagicMock()
    mock_bigquery_client.return_value = mock_client_instance

    # Call the function
    fetch_and_store(MOCK_RESOURCE, MOCK_COMPETITION_ID)

    # Verify API call
    mock_requests_get.assert_called_once_with(MOCK_API_URL, headers={'X-Auth-Token': MOCK_API_KEY})

    # Verify BigQuery insertion
    mock_client_instance.insert_rows_json.assert_called_once_with(MOCK_TABLE_ID, [MOCK_BIGQUERY_DATA])

# Test rate limit retry behavior (429 error)
@patch('src.ingest.competition_data_ingest.requests.get')
@patch('src.ingest.competition_data_ingest.time.sleep', return_value=None)  # Mock time.sleep to avoid delays in testing
@patch('os.getenv', side_effect=lambda key, default=None: {
    "API_KEY": MOCK_API_KEY,
    "COMPETITION_IDS": MOCK_COMPETITION_ID,
    "API_RESOURCES": MOCK_RESOURCE
}.get(key, default))
def test_rate_limit_retry(mock_getenv, mock_sleep, mock_requests_get):
    """
    Test fetch_and_store for retry behavior when hitting a rate limit (HTTP 429).
    """
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_requests_get.return_value = mock_response

    # Call fetch_and_store and expect retry behavior
    fetch_and_store(MOCK_RESOURCE, MOCK_COMPETITION_ID)

    # Ensure that retries occurred
    assert mock_requests_get.call_count > 1
    assert mock_requests_get.call_count <= 5  # Ensure it doesn't retry more than 5 times
    mock_sleep.assert_called()  # Ensure sleep was called for backoff
