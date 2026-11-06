
import os
from datetime import datetime, timedelta

import pendulum
import psycopg2

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# --------------------------------------------------
# Configuration
# --------------------------------------------------

NEW_YORK_TZ = pendulum.timezone("America/New_York")

DBT_PROJECT_DIR = "/opt/airflow/project/dbt"
DBT_PROFILES_DIR = "/opt/airflow/project/config"

DBT_COMMAND = (
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    "--profile taxi_analytics"
)


# --------------------------------------------------
# Task 1: Verify Airflow
# --------------------------------------------------

def verify_airflow():
    print("NYC Taxi Data Engineering Pipeline")
    print("Airflow is running successfully!")


# --------------------------------------------------
# Task 2: Determine target month
# --------------------------------------------------

def get_target_month(**context):
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    # Manual configuration takes priority.
    if "year" in conf or "month" in conf:
        if "year" not in conf or "month" not in conf:
            raise ValueError(
                "Provide both year and month for a manual override."
            )

        year = int(conf["year"])
        month = int(conf["month"])

        if not 1 <= month <= 12:
            raise ValueError(
                "Month must be between 1 and 12."
            )

        if not 2009 <= year <= 2100:
            raise ValueError(
                "Year must be between 2009 and 2100."
            )

    else:
        # Scheduled DAG runs use the end of their
        # data interval as the reference date.
        if dag_run.run_type == "scheduled":
            reference_date = context["data_interval_end"]

        # Manual runs without an override use
        # their actual start time.
        else:
            reference_date = (
                dag_run.start_date
                or pendulum.now("UTC")
            )

        reference_date = pendulum.instance(
            reference_date
        ).in_timezone(NEW_YORK_TZ)

        target = reference_date.subtract(months=1)

        year = target.year
        month = target.month

    print(
        f"Target ingestion month: {year}-{month:02d}"
    )

    return {
        "year": year,
        "month": month,
    }


# --------------------------------------------------
# Task 3: Validate PostgreSQL warehouse
# --------------------------------------------------

def validate_taxi_warehouse():
    connection = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM public.taxi_trips"
            )
            total_records = cursor.fetchone()[0]

        print(
            f"Total taxi records: {total_records:,}"
        )

        if total_records == 0:
            raise ValueError(
                "Taxi warehouse is empty!"
            )

        print(
            "SUCCESS: Taxi warehouse validation passed!"
        )

    finally:
        connection.close()


# --------------------------------------------------
# Airflow DAG
# --------------------------------------------------

with DAG(
    dag_id="nyc_taxi_pipeline",
    description=(
        "NYC Taxi end-to-end data engineering pipeline"
    ),
    start_date=datetime(
        2026, 1, 1,
        tzinfo=NEW_YORK_TZ
    ),

    # Keep disabled until scheduling tests finish.
    # Planned schedule: 9 AM New York time
    # on the 15th of every month.
    # schedule="0 9 15 * *",
    schedule="0 9 15 * *",
    catchup=False,
    max_active_runs=1,
    tags=[
        "nyc-taxi",
        "data-engineering",
    ],
) as dag:

    # Task 1: Verify Airflow
    verify_task = PythonOperator(
        task_id="verify_airflow",
        python_callable=verify_airflow,
    )

    # Task 2: Select ingestion month
    target_month_task = PythonOperator(
        task_id="get_target_month",
        python_callable=get_target_month,
    )

    # Task 3: Ingest taxi records
    ingest_task = BashOperator(
        task_id="ingest_taxi_data",
        bash_command=(
            "python /opt/airflow/project/src/ingestion/ingest_taxi.py "
            "--year {{ ti.xcom_pull(task_ids='get_target_month')['year'] | int }} "
            "--month {{ ti.xcom_pull(task_ids='get_target_month')['month'] | int }}"
        ),

        # Retry failed ingestion three additional times.
        retries=3,

        # Wait 12 hours between retry attempts.
        retry_delay=timedelta(hours=12),
    )

    # Task 4: Validate warehouse
    validate_task = PythonOperator(
        task_id="validate_taxi_warehouse",
        python_callable=validate_taxi_warehouse,
    )

    # Task 5: Execute dbt transformations
    dbt_run_task = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"dbt run {DBT_COMMAND}",
    )

    # Task 6: Execute dbt data quality tests
    dbt_test_task = BashOperator(
        task_id="test_dbt_models",
        bash_command=f"dbt test {DBT_COMMAND}",
    )

    # --------------------------------------------------
    # Task dependencies
    # --------------------------------------------------

    (
        verify_task
        >> target_month_task
        >> ingest_task
        >> validate_task
        >> dbt_run_task
        >> dbt_test_task
    )
