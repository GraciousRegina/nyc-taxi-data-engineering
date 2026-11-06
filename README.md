
# NYC Taxi Data Engineering & Analytics Pipeline

An end-to-end data engineering project that ingests, validates, stores, and transforms New York City Yellow Taxi trip data using **Python, PostgreSQL, Apache Airflow, dbt, and Docker**.

The project demonstrates practical data engineering skills including ETL pipeline development, workflow orchestration, data warehousing, SQL transformations, automated data quality testing, and recovery workflow design.

## Project Overview

This pipeline processes publicly available NYC Yellow Taxi trip records and transforms raw transportation data into structured analytical datasets.

### Key Results

- **2,964,550 NYC Yellow Taxi trip records** ingested for January 2024.
- PostgreSQL 16 data warehouse deployed using Docker.
- Monthly ingestion and processing workflow developed with Apache Airflow.
- Daily recovery workflow designed to identify missing monthly loads.
- **6 Airflow pipeline tasks successfully completed** in the November 6, 2026 run.
- Three dbt analytical models developed.
- **10 dbt tests passed.**
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

The pipeline uses publicly available NYC Taxi & Limousine Commission Yellow Taxi trip data stored in Parquet format.

**2. Python Ingestion**

Python scripts process monthly taxi trip datasets and load records into PostgreSQL.

The ingestion layer supports data validation and tracking of loaded datasets.

**3. PostgreSQL Data Warehouse**

PostgreSQL stores ingested trip data and ingestion metadata.

Primary tables:

- `public.taxi_trips`
- `public.ingestion_tracking`

**4. dbt Transformations**

dbt transforms warehouse data into reusable analytical models for taxi demand and pickup-location analysis.

**5. Data Quality Testing**

Python integration tests and dbt tests verify ingestion and transformation correctness.

**6. Apache Airflow Orchestration**

Airflow defines monthly pipeline execution and daily recovery workflows.

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

The main pipeline includes six tasks:

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

The project includes automated tests for pipeline reliability.

| Test Category | Result |
|---|---|
| dbt tests | 10 passed |
| Python integration tests | 5 passed |
| Airflow main pipeline | 6 of 6 tasks successful in the selected run |

Testing helps validate the ingestion and transformation components.

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
|
|-- Dockerfile.airflow
|-- README.md
|-- .gitignore
|
|-- Additional ingestion, testing,
|   and Docker configuration files
```

## Getting Started

### Prerequisites

- Python 3.12
- Docker and Docker Compose
- PostgreSQL
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/GraciousRegina/nyc-taxi-data-engineering.git
cd nyc-taxi-data-engineering
```

### 2. Configure Environment Variables

Configure the environment variables required by the project's Docker, PostgreSQL, and Airflow services.

The dbt profile reads the PostgreSQL password from:

```text
POSTGRES_PASSWORD
```

Store credentials locally and do not commit secrets to GitHub.

### 3. Start the Infrastructure

Use the Docker Compose configuration included in the repository to start the required services.

For a standard Docker Compose setup:

```bash
docker compose up -d
```

### 4. Run the Pipeline

Use the ingestion scripts and dbt project included in the repository.

Airflow DAGs can be inspected through the Airflow web interface.

The DAGs are intentionally paused by default. Review configuration and dependencies before enabling scheduled execution.

### 5. Run Tests

Run the project's Python integration tests and dbt tests using the corresponding test commands and project configuration.

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

Validated the project with 10 successful dbt tests and 5 successful Python integration tests.

### Containerization

Used Docker to support a reproducible development environment.

## Future Improvements

- Add incremental ingestion for additional months.
- Expand historical taxi demand analysis.
- Introduce pipeline monitoring and alerting.
- Add a visualization dashboard.
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
