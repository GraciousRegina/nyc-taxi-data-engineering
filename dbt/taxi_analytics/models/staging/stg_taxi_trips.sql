
{{ config(materialized='view') }}

with source_trips as (

    select
        trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_location_id,
        dropoff_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        ingested_at
    from public.taxi_trips

),

cleaned_trips as (

    select
        *,
        pickup_datetime::date as pickup_date,
        extract(hour from pickup_datetime)::integer as pickup_hour,
        extract(epoch from (
            dropoff_datetime - pickup_datetime
        )) / 60.0 as trip_duration_minutes

    from source_trips
    where pickup_datetime is not null
      and dropoff_datetime >= pickup_datetime
      and trip_distance >= 0

)

select * from cleaned_trips
