-- Spark's version of the daily aggregate, staged so the parity test can compare it to dbt's.
select
    cast(pickup_date as date) as pickup_date,
    pickup_borough,
    pickup_zone,
    trips,
    passengers,
    revenue,
    avg_fare,
    avg_distance_mi,
    avg_duration_min,
    avg_tip_pct,
    revenue_per_trip
from {{ source('lakehouse', 'daily_zone_kpis') }}
