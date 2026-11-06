
import os

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()


@pytest.fixture(scope="module")
def db_connection():
    database_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER", "taxi_user"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host="localhost",
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        database=os.getenv("POSTGRES_DB", "taxi_warehouse"),
    )

    engine = create_engine(database_url)

    with engine.connect() as connection:
        yield connection

    engine.dispose()


def test_table_contains_data(db_connection):
    count = db_connection.execute(
        text("SELECT COUNT(*) FROM public.taxi_trips")
    ).scalar_one()

    assert count > 0, "The taxi_trips table is empty"


def test_trip_ids_are_unique(db_connection):
    result = db_connection.execute(
        text("""
            SELECT COUNT(*) - COUNT(DISTINCT trip_id)
            FROM public.taxi_trips
        """)
    ).scalar_one()

    assert result == 0, f"Found {result} duplicate trip IDs"


def test_pickup_datetime_not_null(db_connection):
    result = db_connection.execute(
        text("""
            SELECT COUNT(*)
            FROM public.taxi_trips
            WHERE pickup_datetime IS NULL
        """)
    ).scalar_one()

    assert result == 0, f"Found {result} missing pickup dates"


def test_trip_distance_nonnegative(db_connection):
    result = db_connection.execute(
        text("""
            SELECT COUNT(*)
            FROM public.taxi_trips
            WHERE trip_distance < 0
        """)
    ).scalar_one()

    assert result == 0, f"Found {result} negative distances"


def test_dropoff_after_pickup(db_connection):
    result = db_connection.execute(
        text("""
            SELECT COUNT(*)
            FROM public.taxi_trips
            WHERE dropoff_datetime < pickup_datetime
        """)
    ).scalar_one()

    assert result == 0, f"Found {result} invalid trip durations"
