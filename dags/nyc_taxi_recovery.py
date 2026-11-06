
import os
from datetime import datetime, timedelta

import pendulum
import psycopg2

from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import (
    AirflowSkipException,
    DagRunAlreadyExists,
)
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.settings import Session


NEW_YORK_TZ = pendulum.timezone("America/New_York")
MAIN_DAG_ID = "nyc_taxi_pipeline"

BACKLOG_START_YEAR = 2026
BACKLOG_START_MONTH = 1
PUBLICATION_DAY = 15
RETRY_COOLDOWN_HOURS = 24

# Dedicated recovery coordination lock.
# This is separate from the monthly ingestion locks.
RECOVERY_LOCK_KEY_1 = 202411
RECOVERY_LOCK_KEY_2 = 999998


def get_eligible_months():
    """Return eligible months, oldest first."""
    now = pendulum.now(NEW_YORK_TZ)

    first_month = pendulum.datetime(
        BACKLOG_START_YEAR,
        BACKLOG_START_MONTH,
        1,
        tz=NEW_YORK_TZ,
    )

    if now.day < PUBLICATION_DAY:
        last_month = now.subtract(months=2).start_of("month")
    else:
        last_month = now.subtract(months=1).start_of("month")

    months = []
    current = first_month

    while current <= last_month:
        months.append((current.year, current.month))
        current = current.add(months=1)

    return months


def recover_missing_months(**context):
    """
    Find and trigger one eligible missing month.

    A PostgreSQL advisory lock coordinates recovery decisions.
    Completed data is protected, and failed runs have a
    24-hour cooldown measured from their completion time.
    """
    eligible_months = get_eligible_months()

    if not eligible_months:
        raise AirflowSkipException("No eligible recovery months.")

    connection = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )

    lock_acquired = False

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(%s, %s)",
                (RECOVERY_LOCK_KEY_1, RECOVERY_LOCK_KEY_2),
            )
            lock_acquired = cursor.fetchone()[0]

        # End the implicit transaction, but retain the
        # session-level advisory lock.
        connection.commit()

        if not lock_acquired:
            raise AirflowSkipException(
                "Another recovery process holds the lock."
            )

        session = Session()

        try:
            runs = (
                session.query(DagRun)
                .filter(DagRun.dag_id == MAIN_DAG_ID)
                .order_by(DagRun.execution_date.desc())
                .all()
            )

            if any(
                run.state in ("running", "queued")
                for run in runs
            ):
                raise AirflowSkipException(
                    "Main pipeline already has an active run."
                )

            recent_attempts = {}

            for run in runs:
                conf = run.conf or {}

                if "year" not in conf or "month" not in conf:
                    continue

                try:
                    key = (
                        int(conf["year"]),
                        int(conf["month"]),
                    )
                except (TypeError, ValueError):
                    continue

                if key not in recent_attempts:
                    recent_attempts[key] = run

        finally:
            session.close()

        now_utc = pendulum.now("UTC")

        with connection.cursor() as cursor:
            for year, month in eligible_months:
                month_start = datetime(year, month, 1)

                if month == 12:
                    month_end = datetime(year + 1, 1, 1)
                else:
                    month_end = datetime(year, month + 1, 1)

                cursor.execute(
                    """
                    SELECT status, row_count
                    FROM public.ingestion_tracking
                    WHERE year = %s AND month = %s
                    """,
                    (year, month),
                )
                tracking = cursor.fetchone()

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM public.taxi_trips
                    WHERE pickup_datetime >= %s
                      AND pickup_datetime < %s
                    """,
                    (month_start, month_end),
                )
                actual_rows = cursor.fetchone()[0]

                print(
                    f"Checking {year}-{month:02d}: "
                    f"tracking={tracking}, "
                    f"actual_rows={actual_rows:,}"
                )

                if tracking and tracking[0] == "completed":
                    expected_rows = tracking[1]

                    if (
                        expected_rows is None
                        or actual_rows != expected_rows
                    ):
                        raise RuntimeError(
                            f"Completed-month mismatch for "
                            f"{year}-{month:02d}. "
                            "Manual investigation required."
                        )

                    print(
                        f"Skipping completed month "
                        f"{year}-{month:02d}"
                    )
                    continue

                if actual_rows > 0:
                    raise RuntimeError(
                        f"Incomplete existing data for "
                        f"{year}-{month:02d}. "
                        "Automatic recovery blocked."
                    )

                previous_run = recent_attempts.get(
                    (year, month)
                )

                if previous_run is not None:
                    if previous_run.state == "success":
                        raise RuntimeError(
                            f"Airflow reports success for "
                            f"{year}-{month:02d}, but tracking "
                            "is not completed. "
                            "Manual investigation required."
                        )

                    attempt_time = (
                        previous_run.end_date
                        or previous_run.start_date
                        or previous_run.execution_date
                    )

                    if attempt_time is not None:
                        elapsed = (
                            now_utc
                            - pendulum.instance(attempt_time)
                        )

                        if elapsed < timedelta(
                            hours=RETRY_COOLDOWN_HOURS
                        ):
                            print(
                                f"Cooldown active for "
                                f"{year}-{month:02d}"
                            )
                            continue

                # Use the recovery DAG's own run ID to
                # distinguish attempts across different days.
                recovery_run_id = context["run_id"]

                safe_run_id = recovery_run_id.replace(
                    ":", "_"
                ).replace("+", "_")

                trigger_run_id = (
                    f"recovery__{year}_{month:02d}"
                    f"__{safe_run_id}"
                )

                print(
                    f"Triggering recovery for "
                    f"{year}-{month:02d}"
                )

                try:
                    triggered = trigger_dag(
                        dag_id=MAIN_DAG_ID,
                        run_id=trigger_run_id,
                        conf={
                            "year": year,
                            "month": month,
                        },
                    )

                except DagRunAlreadyExists:
                    raise AirflowSkipException(
                        "This recovery attempt was "
                        "already triggered."
                    )

                print(
                    f"Triggered main pipeline: "
                    f"{triggered.run_id}"
                )

                return {
                    "year": year,
                    "month": month,
                    "trigger_run_id": triggered.run_id,
                }

        raise AirflowSkipException(
            "No recovery candidate currently eligible."
        )

    finally:
        if lock_acquired:
            try:
                connection.rollback()

                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(%s, %s)",
                        (
                            RECOVERY_LOCK_KEY_1,
                            RECOVERY_LOCK_KEY_2,
                        ),
                    )
                    unlocked = cursor.fetchone()[0]

                connection.commit()

                if not unlocked:
                    print(
                        "WARNING: Recovery advisory lock "
                        "was not held at release time."
                    )

            finally:
                connection.close()
        else:
            connection.close()


with DAG(
    dag_id="nyc_taxi_recovery",
    description=(
        "Safely recover missing NYC Taxi months "
        "from January 2026 onward"
    ),
    start_date=datetime(
        2026, 1, 1, tzinfo=NEW_YORK_TZ
    ),

    # Keep automatic scheduling disabled.
    schedule="0 10 * * *",
    catchup=False,
    max_active_runs=1,
    tags=[
        "nyc-taxi",
        "recovery",
        "data-engineering",
    ],
) as dag:

    recovery_task = PythonOperator(
        task_id="recover_missing_months",
        python_callable=recover_missing_months,
    )
