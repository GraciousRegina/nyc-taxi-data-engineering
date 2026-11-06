
{{ config(materialized='table') }}

select
    pickup_date,
    count(*) as total_trips,
    count(distinct pickup_location_id) as active_pickup_locations,
    round(sum(total_amount)::numeric, 2) as total_revenue,
    round(avg(total_amount)::numeric, 2) as avg_trip_revenue,
    round(avg(trip_distance)::numeric, 2) as avg_trip_distance_miles,
    round(avg(trip_duration_minutes)::numeric, 2) as avg_trip_duration_minutes

from {{ ref('stg_taxi_trips') }}

group by pickup_date
