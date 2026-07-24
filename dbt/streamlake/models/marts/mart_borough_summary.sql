-- One row per borough for the whole loaded period: the dashboard's headline table.
with trips as (

    select * from {{ ref('stg_trips') }}

),

by_borough as (

    select
        pickup_borough,
        count(*)                                  as trips,
        count(distinct pickup_zone)               as zones,
        round(sum(total_amount), 2)               as revenue,
        round(avg(total_amount), 3)               as avg_ticket,
        round(avg(trip_distance_mi), 3)           as avg_distance_mi,
        round(avg(trip_duration_min), 3)          as avg_duration_min,
        round(avg(tip_pct), 3)                    as avg_tip_pct,
        min(pickup_ts)                            as first_pickup_ts,
        max(pickup_ts)                            as last_pickup_ts
    from trips
    group by 1

)

select
    *,
    round(100.0 * trips / sum(trips) over (), 3)     as trip_share_pct,
    round(100.0 * revenue / sum(revenue) over (), 3) as revenue_share_pct
from by_borough
