
import argparse
import logging
import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

LOCK_NAMESPACE = 202411

COLUMNS = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "tip_amount": "tip_amount",
    "total_amount": "total_amount",
}



def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER", "taxi_user"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        database=os.getenv("POSTGRES_DB", "taxi_warehouse"),
    )
    return create_engine(url, future=True)



def month_bounds(year, month):
    start = pd.Timestamp(year=year, month=month, day=1)
    end = start + pd.DateOffset(months=1)
    return start.to_pydatetime(), end.to_pydatetime()


def acquire_month_lock(connection, year, month):
    """
    Acquire a session-level PostgreSQL advisory lock.

    The connection must remain open for the entire ingestion.
    """
    lock_key = year * 100 + month

    acquired = connection.execute(
        text(
            "SELECT pg_try_advisory_lock(:namespace, :lock_key)"
        ),
        {
            "namespace": LOCK_NAMESPACE,
            "lock_key": lock_key,
        },
    ).scalar_one()

    # End the implicit transaction while keeping the session lock.
    connection.commit()

    if not acquired:
        raise RuntimeError(
            f"Another ingestion is already running for "
            f"{year}-{month:02d}."
        )

    logger.info(
        "Acquired ingestion lock for %04d-%02d",
        year,
        month,
    )


def release_month_lock(connection, year, month):
    lock_key = year * 100 + month

    released = connection.execute(
        text(
            "SELECT pg_advisory_unlock(:namespace, :lock_key)"
        ),
        {
            "namespace": LOCK_NAMESPACE,
            "lock_key": lock_key,
        },
    ).scalar_one()

    connection.commit()

    if not released:
        logger.warning(
            "Advisory lock was not held for %04d-%02d",
            year,
            month,
        )
    else:
        logger.info(
            "Released ingestion lock for %04d-%02d",
            year,
            month,
        )


def get_month_state(connection, year, month):
    start, end = month_bounds(year, month)

    row_count = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM public.taxi_trips
            WHERE pickup_datetime >= :start
              AND pickup_datetime < :end
        """),
        {"start": start, "end": end},
    ).scalar_one()

    tracking = connection.execute(
        text("""
            SELECT status, row_count
            FROM public.ingestion_tracking
            WHERE year = :year AND month = :month
        """),
        {
            "year": year,
            "month": month,
        },
    ).mappings().first()

    return row_count, tracking


def download_data(year, month):
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    destination = RAW_DIR / filename
    temporary = RAW_DIR / f"{filename}.part"

    if destination.exists():
        logger.info("Using cached file: %s", destination)
        return destination

    url = (
        "https://d37ci6vzurychx.cloudfront.net/"
        f"trip-data/{filename}"
    )

    logger.info("Downloading %s", url)

    try:
        with requests.get(
            url,
            stream=True,
            timeout=120,
        ) as response:
            response.raise_for_status()

            with temporary.open("wb") as file:
                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):
                    if chunk:
                        file.write(chunk)

        temporary.replace(destination)

    finally:
        temporary.unlink(missing_ok=True)

    logger.info("Downloaded %s", destination)
    return destination


def clean_data(df, year, month):
    missing = set(COLUMNS) - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    df = df.rename(
        columns=COLUMNS
    )[list(COLUMNS.values())].copy()

    df["pickup_datetime"] = pd.to_datetime(
        df["pickup_datetime"],
        errors="coerce",
    )

    df["dropoff_datetime"] = pd.to_datetime(
        df["dropoff_datetime"],
        errors="coerce",
    )

    df = df.dropna(
        subset=[
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
        ]
    )

    start = pd.Timestamp(
        year=year,
        month=month,
        day=1,
    )
    end = start + pd.DateOffset(months=1)

    df = df[
        (df["pickup_datetime"] >= start)
        & (df["pickup_datetime"] < end)
        & (df["dropoff_datetime"] >= df["pickup_datetime"])
        & (df["trip_distance"] >= 0)
    ].copy()

    integer_columns = [
        "vendor_id",
        "passenger_count",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
    ]

    for column in integer_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        ).astype("Int64")

    return df


def mark_running(connection, year, month):
    with connection.begin():
        connection.execute(
            text("""
                INSERT INTO public.ingestion_tracking
                    (
                        year,
                        month,
                        status,
                        row_count,
                        started_at,
                        completed_at,
                        updated_at
                    )
                VALUES
                    (
                        :year,
                        :month,
                        'running',
                        0,
                        NOW(),
                        NULL,
                        NOW()
                    )
                ON CONFLICT (year, month)
                DO UPDATE SET
                    status = 'running',
                    row_count = 0,
                    started_at = NOW(),
                    completed_at = NULL,
                    updated_at = NOW()
            """),
            {
                "year": year,
                "month": month,
            },
        )


def mark_failed(connection, year, month):
    with connection.begin():
        connection.execute(
            text("""
                UPDATE public.ingestion_tracking
                SET status = 'failed',
                    updated_at = NOW()
                WHERE year = :year
                  AND month = :month
            """),
            {
                "year": year,
                "month": month,
            },
        )


def load_data(connection, df, year, month):
    start, end = month_bounds(year, month)

    # Replace monthly data and update tracking atomically.
    with connection.begin():
        connection.execute(
            text("""
                DELETE FROM public.taxi_trips
                WHERE pickup_datetime >= :start
                  AND pickup_datetime < :end
            """),
            {
                "start": start,
                "end": end,
            },
        )

        df.to_sql(
            "taxi_trips",
            con=connection,
            schema="public",
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
        )

        connection.execute(
            text("""
                UPDATE public.ingestion_tracking
                SET status = 'completed',
                    row_count = :row_count,
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE year = :year
                  AND month = :month
            """),
            {
                "year": year,
                "month": month,
                "row_count": len(df),
            },
        )

    logger.info(
        "Completed %04d-%02d: %s records",
        year,
        month,
        f"{len(df):,}",
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--year",
        type=int,
        default=2024,
    )
    parser.add_argument(
        "--month",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Intentionally replace existing monthly records",
    )

    args = parser.parse_args()

    if not 1 <= args.month <= 12:
        parser.error("--month must be between 1 and 12")

    engine = get_engine()

    try:
        # Hold one dedicated PostgreSQL connection throughout
        # ingestion so the advisory lock remains active.
        with engine.connect() as connection:
            acquired = False

            try:
                acquire_month_lock(
                    connection,
                    args.year,
                    args.month,
                )
                acquired = True

                existing_rows, tracking = get_month_state(
                    connection,
                    args.year,
                    args.month,
                )

                # SELECT statements started an implicit transaction.
                connection.commit()

                if (
                    tracking
                    and tracking["status"] == "completed"
                ):
                    if (
                        existing_rows == tracking["row_count"]
                        and not args.force
                    ):
                        logger.info(
                            "Skipping %04d-%02d: "
                            "completed with %s records.",
                            args.year,
                            args.month,
                            f"{existing_rows:,}",
                        )
                        return

                    if existing_rows != tracking["row_count"]:
                        raise RuntimeError(
                            "Warehouse row count does not match "
                            "completed ingestion tracking. "
                            "Investigate before rerunning."
                        )

                if existing_rows > 0 and not args.force:
                    raise RuntimeError(
                        f"{existing_rows:,} records already exist "
                        f"for {args.year}-{args.month:02d}, "
                        "but ingestion is not confirmed complete. "
                        "Investigate before using --force."
                    )

                # Download and validate before modifying
                # any warehouse data or tracking status.
                path = download_data(
                    args.year,
                    args.month,
                )

                logger.info("Reading %s", path)

                df = pd.read_parquet(path)

                logger.info(
                    "Raw rows: %s",
                    len(df),
                )

                df = clean_data(
                    df,
                    args.year,
                    args.month,
                )

                logger.info(
                    "Validated rows: %s",
                    len(df),
                )

                if df.empty:
                    raise ValueError(
                        "No valid taxi records for the selected month."
                    )

                mark_running(
                    connection,
                    args.year,
                    args.month,
                )

                try:
                    load_data(
                        connection,
                        df,
                        args.year,
                        args.month,
                    )
                except Exception:
                    logger.exception(
                        "Ingestion failed for %04d-%02d",
                        args.year,
                        args.month,
                    )

                    if connection.in_transaction():
                        connection.rollback()

                    mark_failed(
                        connection,
                        args.year,
                        args.month,
                    )
                    raise

            finally:
                if acquired:
                    if connection.in_transaction():
                        connection.rollback()

                    release_month_lock(
                        connection,
                        args.year,
                        args.month,
                    )

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
