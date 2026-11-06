
{{ config(materialized='table') }}

select
    pickup_location_id,
    count(*) as total_trips,
    round(sum(total_amount)::numeric, 2) as total_revenue,
    round(avg(total_amount)::numeric, 2) as avg_trip_revenue,
    round(avg(trip_distance)::numeric, 2) as avg_trip_distance_miles,
    round(avg(trip_duration_minutes)::numeric, 2) as avg_trip_duration_minutes,
    count(distinct pickup_date) as active_days

from {{ ref('stg_taxi_trips') }}

where pickup_location_id is not null

group by pickup_location_id
