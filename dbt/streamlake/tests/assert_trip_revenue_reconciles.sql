-- Total revenue must survive aggregation. If the fact table's sum drifts from the trip-level
-- sum, a join fanned out or a group-by dropped rows.
with detail as (

    select round(sum(total_amount), 2) as revenue from {{ ref('stg_trips') }}

),

aggregated as (

    select round(sum(revenue), 2) as revenue from {{ ref('fct_trip_daily_zone') }}

)

select detail.revenue as detail_revenue, aggregated.revenue as aggregated_revenue
from detail, aggregated
where abs(detail.revenue - aggregated.revenue) > 1.00
