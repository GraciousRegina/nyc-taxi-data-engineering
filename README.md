# NYC Taxi Data Engineering & Analytics Pipeline

An end-to-end data engineering project that ingests, validates, transforms, and orchestrates New York City Yellow Taxi trip data using Python, PostgreSQL, Apache Airflow, dbt, and Docker.

The pipeline is designed for repeatable monthly ingestion, automated data-quality checks, analytical modeling, and recovery from missed or incomplete processing runs.

## Project Highlights

- **2,964,550 taxi trips** ingested for January 2024.
- **Apache Airflow** orchestrates ingestion, validation, dbt transformations, and testing.
- **PostgreSQL** stores raw taxi trips, ingestion metadata, and analytical models.
- **dbt** builds reusable staging, fact, and location-level analytics models.
- **Automated recovery DAG** identifies eligible missing monthly loads and triggers controlled recovery.
- **Idempotent ingestion** prevents unnecessary duplicate processing.
- **Docker Compose** provides a containerized development environment.

## Technology Stack

| Category | Technologies |
|---|---|
| Programming | Python, SQL |
| Orchestration | Apache Airflow |
| Database | PostgreSQL 16 |
| Data Transformation | dbt Core, dbt-postgres |
| Data Processing | pandas, PyArrow |
| Infrastructure | Docker, Docker Compose |
| Testing | pytest, dbt tests |
| Data Source | NYC TLC Yellow Taxi Trip Records |

## Pipeline Architecture

```text
NYC TLC Yellow Taxi Parquet Files
                |
                v
       Python Ingestion
                |
                v
       PostgreSQL Warehouse
       |                  |
       v                  v
  taxi_trips       ingestion_tracking
       |
       v
   Data Validation
       |
       v
   dbt Transformations
       |
       +-- stg_taxi_trips
       |
       +-- fct_daily_taxi_demand
       |
       +-- mart_pickup_locations
       |
       v
   dbt Data Quality Tests
```

Apache Airflow coordinates the workflow and monitors task execution.

A separate recovery DAG checks for eligible missing monthly ingestion runs and can trigger the main pipeline when recovery is needed.

## Data Pipeline

### 1. Data Ingestion

The ingestion module downloads NYC TLC Yellow Taxi Parquet data and loads trip records into PostgreSQL.

Features include:

- Monthly data ingestion
- Local Parquet caching
- Ingestion tracking
- Idempotent processing
- PostgreSQL advisory locking
- Transactional replacement for controlled reprocessing

### 2. Data Warehouse

PostgreSQL stores taxi trip records and ingestion metadata.

Key tables:

- `public.taxi_trips`
- `public.ingestion_tracking`

For January 2024, the warehouse contains **2,964,550 taxi trips**.

### 3. Data Transformation

dbt transforms warehouse data into analytical models.

| Model | Purpose |
|---|---|
| `stg_taxi_trips` | Staging layer for taxi trip records |
| `fct_daily_taxi_demand` | Daily taxi demand analytics |
| `mart_pickup_locations` | Pickup-location-level analytics |

The January 2024 processing run produced 31 daily-demand records and 260 pickup-location records.

### 4. Workflow Orchestration

The main Airflow DAG, `nyc_taxi_pipeline`, executes:

```text
Verify Airflow Environment
          |
          v
Determine Target Month
          |
          v
Ingest Taxi Data
          |
          v
Validate Warehouse
          |
          v
Run dbt Models
          |
          v
Run dbt Tests
```

The main DAG is configured to run monthly on the 15th at 9:00 AM, using the America/New_York timezone.

### 5. Automated Recovery

The `nyc_taxi_recovery` DAG provides recovery logic for missing or incomplete monthly ingestion runs.

It includes:

- Checks for eligible missing months
- Ingestion completion verification
- Protection against overlapping pipeline executions
- A failure cooldown
- PostgreSQL advisory locking
- Controlled triggering of the main DAG

The recovery DAG is configured for daily evaluation at 10:00 AM New York time.

**Operational note:** Both DAGs are intentionally paused in the current development environment. Their schedules are configured, but automatic execution is not enabled.

## Data Quality and Validation

The project uses Python integration tests, warehouse validation, and dbt tests to verify pipeline behavior and analytical data quality.

Validation completed during development:

- 5 Python integration tests passed
- 10 dbt tests passed
- Successful end-to-end Airflow pipeline execution for January 2024

## Repository Structure

```text
nyc-taxi-data-engineering/
|
|-- dags/
|   |-- nyc_taxi_pipeline.py
|   |-- nyc_taxi_recovery.py
|
|-- src/
|   |-- ingestion/
|       |-- ingest_taxi.py
|
|-- sql/
|   |-- 01_create_tables.sql
|   |-- 02_create_ingestion_tracking.sql
|
|-- dbt/
|   |-- taxi_analytics/
|       |-- models/
|
|-- config/
|   |-- profiles.yml
|
|-- tests/
|   |-- test_data_quality.py
|
|-- Dockerfile.airflow
|-- docker-compose.yml
|-- requirements.txt
|-- .gitignore
|-- README.md
```

## Getting Started

### Prerequisites

- Python 3.12
- Docker Desktop with Docker Compose
- Git

### Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/nyc-taxi-data-engineering.git
cd nyc-taxi-data-engineering
```

Replace `YOUR_USERNAME` with the GitHub account where this repository is published.

### Configure the Environment

Create a local `.env` file with the environment variables required by `docker-compose.yml`.

Database credentials must be provided through environment variables rather than committed to Git.

### Start the Services

```bash
docker compose up -d --build
```

The Docker environment contains the PostgreSQL warehouse and Apache Airflow services.

### Run the Pipeline

Once the environment is configured, use the Airflow web interface to inspect the DAGs and trigger the main pipeline for a supported month.

The DAGs are paused by default in the development workflow to prevent unintended automatic ingestion.

### Run Tests

Python tests:

```bash
pytest tests/
```

dbt tests, from the configured dbt environment:

```bash
dbt test
```

The Python integration tests require an accessible test warehouse, and dbt commands require the correct database profile and environment variables.

## Engineering Considerations

The project incorporates several production-oriented data engineering practices:

- **Idempotency:** Avoids reprocessing already completed monthly loads unnecessarily.
- **Concurrency control:** Uses database advisory locks to protect ingestion and recovery operations.
- **Observability:** Tracks ingestion status and validates warehouse contents.
- **Modularity:** Separates ingestion, orchestration, transformations, and tests.
- **Recoverability:** Supports controlled recovery of missing monthly loads.
- **Credential management:** Uses environment variables for database passwords.

## Future Improvements

- Deploy the pipeline to a cloud environment.
- Add centralized monitoring and alerting.
- Implement CI/CD for automated testing.
- Extend the analytics layer with additional months and forecasting models.

## Author

Data Science graduate student focused on data engineering, scalable data systems, and applied analytics.