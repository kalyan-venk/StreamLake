-- Cross-engine reconciliation: Spark's gold aggregate vs dbt's warehouse rebuild.
--
-- Both compute trips and revenue per zone per day from the same silver rows, through completely
-- different engines. They must agree. A tolerance of one cent absorbs float rounding; anything
-- larger means one of the two transformations changed and the other did not, which is exactly
-- the drift that would otherwise be discovered by a stakeholder.
with spark_side as (

    select pickup_date, pickup_borough, pickup_zone, trips, revenue
    from {{ ref('stg_spark_daily_zone_kpis') }}

),

dbt_side as (

    select pickup_date, pickup_borough, pickup_zone, trips, revenue
    from {{ ref('fct_trip_daily_zone') }}

)

select
    coalesce(s.pickup_date, d.pickup_date)          as pickup_date,
    coalesce(s.pickup_zone, d.pickup_zone)          as pickup_zone,
    s.trips                                          as spark_trips,
    d.trips                                          as dbt_trips,
    s.revenue                                        as spark_revenue,
    d.revenue                                        as dbt_revenue
from spark_side s
full outer join dbt_side d
    on  s.pickup_date     = d.pickup_date
    and s.pickup_borough  = d.pickup_borough
    and s.pickup_zone     = d.pickup_zone
where s.pickup_date is null
   or d.pickup_date is null
   or s.trips <> d.trips
   or abs(s.revenue - d.revenue) > 0.01
