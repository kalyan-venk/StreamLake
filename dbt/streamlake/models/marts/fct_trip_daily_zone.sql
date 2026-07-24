-- dbt's independent rebuild of the daily zone aggregate.
--
-- Spark already computes this (gold.daily_zone_kpis). Recomputing it here in warehouse SQL is
-- deliberate: tests/assert_batch_spark_dbt_parity.sql compares the two, so a change to either
-- engine's logic that silently moves a number gets caught by a failing test rather than by
-- someone noticing a chart looks off.
with trips as (

    select * from {{ ref('stg_trips') }}

)

select
    pickup_date,
    pickup_borough,
    pickup_zone,
    count(*)                                              as trips,
    sum(coalesce(passenger_count, 0))                     as passengers,
    round(sum(total_amount), 2)                           as revenue,
    round(avg(fare_amount), 3)                            as avg_fare,
    round(avg(trip_distance_mi), 3)                       as avg_distance_mi,
    round(avg(trip_duration_min), 3)                      as avg_duration_min,
    round(avg(tip_pct), 3)                                as avg_tip_pct,
    round(sum(total_amount) / count(*), 3)                as revenue_per_trip,
    round(100.0 * sum(case when payment_type = 1 then 1 else 0 end) / count(*), 2)
                                                          as card_share_pct
from trips
group by 1, 2, 3
