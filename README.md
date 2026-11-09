
# NYC Taxi Data Engineering & Analytics Pipeline

An end-to-end data engineering project that ingests, validates, stores, and transforms New York City Yellow Taxi trip data using **Python, PostgreSQL, Apache Airflow, dbt, and Docker**.

This project demonstrates practical data engineering skills including ETL pipeline development, workflow orchestration, data warehousing, SQL transformations, automated data quality testing, and recovery workflow design.

## Project Overview

The pipeline processes publicly available NYC Yellow Taxi trip records and transforms raw transportation data into structured analytical datasets.

### Key Results

- **2,964,550 NYC Yellow Taxi records** ingested for January 2024.
- PostgreSQL 16 data warehouse deployed using Docker.
- Monthly data ingestion and processing workflow developed using Apache Airflow.
- Daily recovery workflow designed to identify missing monthly loads.
- **6 of 6 Airflow tasks successfully completed** in the November 6, 2026 pipeline run.
- Three dbt analytical models developed.
- **10 of 10 dbt data quality tests passed**, with zero warnings and errors.
- **5 Python integration tests passed.**
- Ingestion tracking implemented to monitor monthly data loading.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.12 | Data ingestion, processing, and validation |
| Pandas | Data manipulation |
| PyArrow | Parquet file processing |
| PostgreSQL 16 | Data warehousing |
| Apache Airflow 2.10.5 | Workflow orchestration |
| dbt | SQL transformations and data quality testing |
| Docker & Docker Compose | Containerized infrastructure |
| SQL | Analytical queries and data modeling |
| Git & GitHub | Version control |

## Pipeline Architecture

```mermaid
flowchart TD
    A["NYC TLC Yellow Taxi Parquet Files"] --> B["Python Monthly Ingestion"]
    B --> C[("PostgreSQL Warehouse")]
    C --> D["Warehouse Validation"]
    D --> E["dbt Transformations"]
    E --> F["dbt Data Quality Tests"]

    C --> G["taxi_trips"]
    C --> H["ingestion_tracking"]

    E --> I["stg_taxi_trips"]
    E --> J["fct_daily_taxi_demand"]
    E --> K["mart_pickup_locations"]

    L["Airflow Main DAG"] -.-> B
    L -.-> D
    L -.-> E
    L -.-> F

    R["Airflow Recovery DAG"] --> S["Check Missing Monthly Loads"]
    S --> H
    S -->|"Trigger eligible recovery"| L
```

### Pipeline Components

**1. Data Source**

The pipeline uses publicly available NYC Taxi & Limousine Commission (TLC) Yellow Taxi trip data stored in Parquet format.

**2. Python Data Ingestion**

Python scripts process monthly taxi trip datasets and load records into PostgreSQL.

The ingestion layer supports data validation and tracking of loaded datasets.

**3. PostgreSQL Data Warehouse**

PostgreSQL stores the ingested trip data and ingestion metadata.

Primary tables:

- `public.taxi_trips`
- `public.ingestion_tracking`

**4. dbt Transformations**

dbt transforms warehouse data into reusable analytical models for taxi demand and pickup-location analysis.

**5. Data Quality Testing**

Python integration tests and dbt tests validate ingestion and transformation correctness.

**6. Apache Airflow Orchestration**

Airflow provides monthly pipeline execution and daily recovery workflow definitions.

## Data Warehouse

### Main Tables

| Table | Description |
|---|---|
| `public.taxi_trips` | Stores ingested NYC Yellow Taxi trip records |
| `public.ingestion_tracking` | Tracks monthly ingestion activity |

### Ingestion Results

| Metric | Result |
|---|---|
| Dataset | NYC Yellow Taxi |
| Period | January 2024 |
| Records ingested | 2,964,550 |
| Storage | PostgreSQL 16 |
| Source format | Parquet |

## dbt Analytics Models

The project includes three analytical models.

### 1. `stg_taxi_trips`

A staging model that prepares trip-level warehouse data for downstream analysis.

### 2. `fct_daily_taxi_demand`

A fact model designed to support daily taxi demand analysis.

### 3. `mart_pickup_locations`

An analytical model designed to support pickup-location analysis.

These models demonstrate SQL-based transformations and analytical data modeling.

## Apache Airflow Orchestration

The project contains two Airflow DAGs.

### Main Pipeline DAG

**File:** `dags/nyc_taxi_pipeline.py`

Defines the monthly ingestion, validation, and transformation workflow.

The main pipeline contains six tasks:

1. `verify_airflow`
2. `get_target_month`
3. `ingest_taxi_data`
4. `validate_taxi_warehouse`
5. `run_dbt_models`
6. `test_dbt_models`

### Recovery DAG

**File:** `dags/nyc_taxi_recovery.py`

Defines a daily workflow for checking missing monthly loads and coordinating recovery.

Both DAGs are currently paused to prevent unintended scheduled execution.

## Pipeline Execution Screenshots

### Airflow Execution History

The Airflow interface shows the execution history of the NYC Taxi pipeline, including successful pipeline runs.

![Airflow Execution History](screenshots/airflow_dags.png)

### Successful End-to-End Pipeline Execution

On November 6, 2026, the Airflow pipeline completed all six tasks successfully.

The execution graph demonstrates the workflow from environment verification and monthly ingestion through PostgreSQL validation, dbt transformations, and data quality testing.

![Successful Airflow Pipeline](screenshots/airflow_pipeline_success.png)

## Data Quality & Testing

The project includes automated tests to verify pipeline reliability and data integrity.

| Test Category | Result |
|---|---|
| dbt data quality tests | 10 passed |
| Python integration tests | 5 passed |
| Airflow main pipeline | 6 of 6 tasks successful in the selected run |

### dbt Test Execution Results

All **10 automated dbt data quality tests passed successfully**, with zero warnings and zero errors.

The tests validate key data quality constraints, including non-null values and uniqueness across staging and analytical models.

**Latest verified test execution:**

| Metric | Result |
|---|---|
| Tests executed | 10 |
| Tests passed | 10 |
| Warnings | 0 |
| Errors | 0 |
| Skipped | 0 |
| Execution time | 4.22 seconds |
| Status | Completed successfully |

![Successful dbt Tests](screenshots/dbt_tests_passed.png)

### Python Integration Tests

The project also includes Python integration tests covering pipeline functionality.

**Result: 5 integration tests passed.**

## Project Structure

```text
nyc-taxi-data-engineering/
|
|-- dags/
|   |-- nyc_taxi_pipeline.py
|   |-- nyc_taxi_recovery.py
|
|-- dbt/
|   |-- taxi_analytics/
|       |-- models/
|
|-- config/
|   |-- profiles.yml
|
|-- screenshots/
|   |-- airflow_dags.png
|   |-- airflow_pipeline_success.png
|   |-- dbt_tests_passed.png
|
|-- Dockerfile.airflow
|-- README.md
|-- .gitignore
|
|-- Additional ingestion, testing,
|   and Docker configuration files
```


## Getting Started

Follow these steps to set up and run the NYC Taxi Data Engineering & Analytics Pipeline locally.

### Prerequisites

Ensure the following tools are installed:

- Python 3.12
- Docker Desktop with Docker Compose
- Git
- dbt-core and dbt-postgres (for running dbt locally)

PostgreSQL runs inside Docker, so a separate local PostgreSQL installation is not required.

### 1. Clone the Repository

Clone the project from GitHub:

```bash
git clone https://github.com/GraciousRegina/nyc-taxi-data-engineering.git
cd nyc-taxi-data-engineering
```

### 2. Configure Environment Variables

Create a local `.env` file from the example template.

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

On macOS or Linux:

```bash
cp .env.example .env
```

Open `.env` and replace the placeholder passwords with your own secure credentials.

| Variable | Description |
|---|---|
| `POSTGRES_USER` | PostgreSQL warehouse username |
| `POSTGRES_PASSWORD` | PostgreSQL warehouse password |
| `POSTGRES_DB` | PostgreSQL warehouse database |
| `AIRFLOW_DB_PASSWORD` | Airflow metadata database password |
| `AIRFLOW_DB_PASSWORD_ENCODED` | URL-encoded Airflow database password |
| `POSTGRES_HOST` | Host used for local dbt connections |
| `POSTGRES_PORT` | Port used for local dbt connections |

The `AIRFLOW_DB_PASSWORD_ENCODED` value must represent the same password as `AIRFLOW_DB_PASSWORD`, encoded for use in a database URL.

For local dbt execution, use:

```dotenv
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5433
```

For dbt running inside Docker, the PostgreSQL service is available at `postgres:5432`.

**Security:** Never commit `.env` or actual credentials to GitHub. Only `.env.example`, containing placeholders, should be tracked.

### 3. Start the Infrastructure

Ensure Docker Desktop is running.

Start the PostgreSQL and Apache Airflow services:

```bash
docker compose up -d
```

Verify that the containers are running:

```bash
docker compose ps
```

The main services include:

- PostgreSQL warehouse (`postgres`)
- Airflow metadata database (`airflow-postgres`)
- Airflow scheduler (`airflow-scheduler`)
- Airflow webserver (`airflow-webserver`)

The Airflow web interface is available at:

http://127.0.0.1:8080

### 4. Run the Pipeline

The project includes two Apache Airflow DAGs:

- `nyc_taxi_pipeline` — main ingestion and transformation workflow
- `nyc_taxi_recovery` — recovery workflow

List the available DAGs:

```bash
docker compose exec -T airflow-scheduler airflow dags list
```

The DAGs are intentionally kept paused to prevent unintended execution.

To execute the pipeline, first review the configured data source, database credentials, and task dependencies. Then enable or manually trigger the appropriate DAG through the Airflow web interface.

### 5. Run dbt Models and Tests

Navigate to the dbt project:

```bash
cd dbt/taxi_analytics
```

Ensure the PostgreSQL environment variables from your `.env` file are available in the terminal running dbt.

Verify the connection:

```bash
dbt debug --profiles-dir ../../config
```

Run the transformation models:

```bash
dbt run --profiles-dir ../../config
```

Execute the dbt data quality tests:

```bash
dbt test --profiles-dir ../../config
```

The dbt project includes three analytics models:

- `stg_taxi_trips` — cleaned and standardized taxi trip data
- `fct_daily_taxi_demand` — daily trip demand and revenue metrics
- `mart_pickup_locations` — pickup location analytics

### 6. Run Python Integration Tests

Return to the project root:

```bash
cd ../..
```

Install the Python dependencies in your active virtual environment:

```bash
python -m pip install -r requirements.txt
```

Run the integration tests:

```bash
python -m pytest
```

These tests help validate the ingestion and warehouse integration components.

### 7. Verify Pipeline Results

After successful ingestion and transformation, verify the following:

- Taxi trip records are loaded into PostgreSQL.
- Ingestion tracking metadata is recorded.
- Airflow tasks complete successfully.
- dbt analytics models are materialized.
- Data quality tests pass.

The documented reference execution processed **2,964,550 NYC Yellow Taxi trip records** for January 2024.

Execution screenshots and testing evidence are included in the sections above.

### 8. Stop the Infrastructure

To stop the running services without deleting PostgreSQL volumes:

```bash
docker compose down
```

Avoid using `docker compose down -v` unless you intentionally want to remove persistent database volumes and their stored data.

## Engineering Highlights

### Data Ingestion

Processed nearly three million NYC Yellow Taxi records using Python and Parquet-based data processing.

### Data Warehousing

Implemented a PostgreSQL warehouse with ingestion tracking.

### Data Modeling

Built dbt staging and analytical models to support taxi demand and pickup-location analysis.

### Workflow Orchestration

Developed Airflow workflows for monthly processing and daily recovery checks.

### Data Quality

Validated the project with 10 successful dbt tests, 5 successful Python integration tests, and a fully successful six-task Airflow run.

### Containerization

Used Docker and Docker Compose to support a reproducible development environment.

## Future Improvements

- Add incremental ingestion for additional months.
- Expand historical taxi demand analysis.
- Introduce pipeline monitoring and alerting.
- Add an interactive analytics dashboard.
- Optimize large-scale data processing.
- Expand automated data quality checks.

## Data Source

NYC Taxi & Limousine Commission (TLC) Trip Record Data:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Author

**Gracious Regina Zigara**

M.S. Data Science  
Stevens Institute of Technology

GitHub: https://github.com/GraciousRegina
