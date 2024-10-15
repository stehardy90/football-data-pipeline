# Football Data Pipeline

This project implements a data pipeline that ingests, processes, and prepares football-related data from the football-data.org API for analysis. The pipeline is designed to transform raw JSON data into structured tables for reporting and analytics, stored in Google BigQuery.

## Architecture

- **Raw Layer (Bronze)**: Ingests raw JSON data from the API and stores it in BigQuery as is. This layer contains the untransformed data for competitions, matches, and players etc.
- **Staging Layer (Silver)**: Cleans and structures the raw data into a more usable format. This includes tables like stg_matches, stg_scorers, and stg_teams, which prepare the data for analysis by handling data cleaning, transformations, and schema standardisation.
- **Aggregate Layer (Gold)**: Contains fact and dimension tables ready for reporting and analysis. In this project, reporting views that summarise or aggregate data, such as goals per game or player rankings, are also included directly within the Gold layer for simplicity. Although a separate analytics layer could be used in larger or more complex projects, this approach keeps the architecture streamlined.

## Technologies Used

- **Python**: For data ingestion scripts.
- **Apache Airflow**: Manages orchestration and scheduling of the pipeline, ensuring automated and reliable data ingestion and transformations. Alerts are configured for task failures, sending notifications via Slack for immediate troubleshooting.
- **dbt**: Used for transforming raw JSON data into clean, structured tables and implementing Type 2 Slowly Changing Dimensions (SCD2) using dbt snapshots. Includes built-in and custom tests to validate data integrity.
- **Google BigQuery**: The data warehouse used for storing raw, staging, and final tables. It supports powerful querying and analytics, especially for large datasets.
- **Docker**: Containerisation is used to standardise the development and deployment environment, ensuring consistent behavior across different environments.

## Key Features

- **Data Integrity with dbt Testing**: Includes both built-in and custom dbt tests to validate data quality. Tests ensure data integrity for key business metrics such as unique player IDs, non-null values for match results, and valid date ranges.
- **Automated Orchestration with Airflow**: Apache Airflow manages the scheduling and orchestration of ingestion, transformation, and testing steps, ensuring a reliable and automated data pipeline.
- **Incremental Loading**: Where appropriate, ingests new data only, reducing reprocessing.
- **Historical Snapshotting**: Uses Slowly Changing Dimensions (SCD Type 2) for tracking changes in evolving data (e.g., players, scorers).
- **Real-time Slack Notifications**: Slack integration alerts on task failures or pipeline errors, allowing for proactive monitoring and troubleshooting.
- **Reporting Views in Gold Layer**: Reporting views, typically housed in a separate analytics layer in larger projects, are incorporated directly into the Gold layer for this project. This simplifies the architecture while still allowing for detailed business insights, such as goals per game or player performance summaries.


## Project Structure

```bash
├── src/
│   ├── dags/                   # Airflow DAGs for orchestration
│   ├── transform/              # dbt models for data transformations
│   └── scripts/                # Custom Python scripts for API data ingestion
├── docker/                     # Docker environment setup
│   └── .env                    # Docker environment variables (excluded)
├── README.md                   # Project documentation
├── .gitignore                  # Ignored files and directories
└── requirements.txt            # Python dependencies
```

## 4. **Setup Instructions**
To get the Football Data Pipeline up and running, follow these instructions to set up your environment, configure the necessary services, and run the pipeline.

###  Clone the Repository
```bash
git clone https://github.com/your_username/football-data-pipeline.git
cd football-data-pipeline
```

###  Set Up Google Cloud and BigQuery
- **Create a Google Cloud Project**: Ensure you have a Google Cloud project with BigQuery enabled.
- **Create a BigQuery Dataset**: Set up the following datasets for the different layers:
  - *Raw (Bronze) Layer*: football_data_bronze
  - *Staging (Silver) Layer*: football_data_silver
  - *Snapshots (SCD2)*: football_data_snapshot
  - *Aggregate (Gold)*: football_data_gold
    
- **Enable API Access**: Ensure that BigQuery API and Cloud Storage API are enabled.

### Configure Google Cloud Credentials
Download the Google Cloud Service Account key JSON file and set the path in your .env file (instructions below).

### Environment Configuration
**Create an .env File**: Copy the .env.example file and configure the necessary variables.
```bash
FOOTBALL_API_KEY=<Your_Football_API_Key>
GCP_PROJECT_ID=<Your_Google_Cloud_Project_ID>
DOCKER_GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keyfile.json"
```

### Set Up Docker
- **Build Docker Containers**: Ensure Docker is installed. Navigate to your docker directory and use the following command to build and set up the containerised environment:
```bash
docker-compose up --build
```
- **Run Airflow**: Access Airflow at http://localhost:8080 after Docker is up and running. Configure Airflow connections for BigQuery and Slack.

### Configure Slack for Alerts
- Create a Slack Webhook URL and configure it in Airflow.
- Add the Slack connection in Airflow and ensure real-time alerts for pipeline failures and errors are working.

### Running the Pipeline
- Access Airflow at http://localhost:8080 after Docker is up and running.
- Set the football_data_dbt_pipeline dag running


