import os
import requests
import json
import time
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# Load environment variables
load_dotenv()

API_KEY = os.getenv('API_KEY')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('BIGQUERY_DATASET') 
KEYFILE = os.getenv('DOCKER_GOOGLE_APPLICATION_CREDENTIALS')
BIGQUERY_LOCATION = os.getenv('BIGQUERY_LOCATION', 'EU')
BASE_URL = "https://api.football-data.org/v4"  # API base URL
COMPETITION_IDS = os.getenv('COMPETITION_IDS').split(',')
API_RESOURCES = os.getenv('API_RESOURCES').split(',')

# Check if all required environment variables are set
if not API_KEY:
    raise ValueError("FOOTBALL_API_KEY environment variable not set.")
if not DATASET_ID:
    raise ValueError("DATASET_ID environment variable not set.")
if not PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID environment variable not set.")
if not KEYFILE:
    raise ValueError("DOCKER_GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
if not COMPETITION_IDS:
    raise ValueError("COMPETITION_IDS environment variable not set.")
if not API_RESOURCES:
    raise ValueError("API_RESOURCES environment variable not set.")

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(KEYFILE)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Function to create table if it does not exist
def create_table_if_not_exists(table_id):
    try:
        client.get_table(table_id)
        print(f"Table {table_id} already exists.")
    except Exception as e:
        print(f"Table {table_id} not found. Creating table...")
        schema = [
            bigquery.SchemaField("endpoint", "STRING"),
            bigquery.SchemaField("raw_json", "STRING"),
            bigquery.SchemaField("loaded_date", "TIMESTAMP")
        ]
        table = bigquery.Table(table_id, schema=schema)
        try:
            client.create_table(table)
            print(f"Table {table_id} created successfully.")
            # Add a small delay after table creation to allow for propagation
            time.sleep(2)  # Wait for 2 seconds before inserting data
        except Exception as table_err:
            print(f"Failed to create table {table_id}: {table_err}")

# Function to construct the API URL and corresponding BigQuery table name
def build_url_and_table(resource_name, competition_id=None):
    if resource_name == "competitions":
        api_url = f"{BASE_URL}/competitions/{competition_id}" if competition_id else f"{BASE_URL}/competitions"
        table_id = f"{PROJECT_ID}.{DATASET_ID}.raw_football_competitions"
    else:
        api_url = f"{BASE_URL}/competitions/{competition_id}/{resource_name}"
        table_id = f"{PROJECT_ID}.{DATASET_ID}.raw_football_{resource_name}"
    
    return api_url, table_id

# Function to fetch data from the API and store it in BigQuery
def fetch_and_store(resource_name, competition_id=None):
    api_url, table_id = build_url_and_table(resource_name, competition_id)
    create_table_if_not_exists(table_id)

    headers = {'X-Auth-Token': API_KEY}
    retries = 5
    backoff_factor = 2

    for attempt in range(retries):
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Prepare row data
            loaded_date = datetime.utcnow().isoformat()
            row_data = {
                'endpoint': api_url,
                'raw_json': json.dumps(data),
                'loaded_date': loaded_date
            }

            # Insert into BigQuery
            client.insert_rows_json(table_id, [row_data])
            print(f"Data from {resource_name} for competition ID {competition_id} loaded successfully.")
            break
        except requests.exceptions.RequestException as e:
            if response.status_code == 429:  # Rate limit error
                wait_time = backoff_factor ** attempt  # Exponential backoff
                print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Failed to fetch data from {resource_name} for competition ID {competition_id}: {e}")
                break

# Main function to orchestrate the data ingestion
def main():
    for resource in API_RESOURCES:
        if resource == "competitions":
            # Fetch for all competitions
            for competition_id in COMPETITION_IDS:
                fetch_and_store(resource, competition_id)
                time.sleep(1)  # Avoid rate limits
        else:
            # Fetch for specific competition endpoints (standings, teams)
            for competition_id in COMPETITION_IDS:
                fetch_and_store(resource, competition_id)
                time.sleep(1)  # Avoid rate limits

if __name__ == '__main__':
    main()
