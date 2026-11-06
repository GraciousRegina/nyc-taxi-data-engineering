
CREATE TABLE IF NOT EXISTS taxi_trips (
    trip_id BIGSERIAL PRIMARY KEY,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    payment_type INTEGER,
    fare_amount NUMERIC(12, 2),
    tip_amount NUMERIC(12, 2),
    total_amount NUMERIC(12, 2),
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_taxi_pickup_datetime
ON taxi_trips (pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_taxi_pickup_location
ON taxi_trips (pickup_location_id);
