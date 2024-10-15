# Football Data Pipeline

This project is an automated ETL pipeline for ingesting, transforming, and analyzing football data from the football-data.org API. The pipeline uses Airflow for orchestration, dbt for data transformation, and Google BigQuery for data storage. The pipeline processes data related to competitions, matches, standings, and teams, and transforms it for reporting and analytics.

## Technologies Used

- **Python**: For data ingestion scripts.
- **Airflow**: To orchestrate the data pipeline.
- **dbt**: For data transformation and modeling.
- **Google BigQuery**: As the data warehouse for storage.
- **Docker**: For containerized execution of the entire pipeline.
- **Google Cloud**: For cloud infrastructure and storage.



#### 4. **Setup Instructions**
- **Environment Setup**: Provide steps for setting up the environment (e.g., Docker, Python dependencies, etc.).
- **Airflow and dbt Setup**: Instructions on how to set up Airflow and dbt (if not done automatically).

Example:
```markdown
## Setup Instructions

### Prerequisites
- Install [Docker](https://www.docker.com/) and Docker Compose.
- Install [Google Cloud SDK](https://cloud.google.com/sdk).

### Environment Setup
1. Create a `.env` file in the root directory based on `.env.example` (located in the `docker/` directory).
2. Fill in the required values:
   - `PROJECT_BASE_DIR`: Path to the project base directory (e.g., `/opt/airflow/project`).
   - `DBT_PROFILES_DIR`: Path to the dbt profiles directory.
   - `GCP_PROJECT_ID`: Your Google Cloud project ID.
   - `BIGQUERY_DATASET`: Dataset for BigQuery.
   - `GOOGLE_APPLICATION_CREDENTIALS`: Path to your Google Cloud credentials JSON file.

### Running the Project
1. Start the containers:
   ```bash
   docker-compose up --build
   ```
2. Access the Airflow web UI at `http://localhost:8080`.
3. Trigger the Airflow DAG to start the ETL process.






## Usage

### Running the ETL Pipeline
- Access the Airflow UI at `http://localhost:8080`.
- Trigger the `football_data_dbt_pipeline_dag` to start data ingestion and transformation.
