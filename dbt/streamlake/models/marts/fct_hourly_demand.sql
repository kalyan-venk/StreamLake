-- Demand by borough and hour of day, with each hour's share of the borough's daily volume.
-- The window function is the point: this is the kind of question the warehouse answers better
-- than the lake, which is why the curated layer exists at all.
with trips as (

    select * from {{ ref('stg_trips') }}

),

hourly as (

    select
        pickup_date,
        pickup_hour,
        pickup_borough,
        count(*)                          as trips,
        round(sum(total_amount), 2)       as revenue,
        round(avg(trip_duration_min), 3)  as avg_duration_min,
        round(avg(avg_speed_mph), 3)      as avg_speed_mph
    from trips
    group by 1, 2, 3

)

select
    pickup_date,
    pickup_hour,
    pickup_borough,
    trips,
    revenue,
    avg_duration_min,
    avg_speed_mph,
    round(100.0 * trips / sum(trips) over (partition by pickup_date, pickup_borough), 3)
        as share_of_borough_day_pct
from hourly
