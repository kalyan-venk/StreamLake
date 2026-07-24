-- The freshness monitor behind the dashboard's second page.
--
-- A static chart tells you what the data says. This tells you whether the data is still being
-- produced: for every curated table, how recent its newest record is relative to the end of the
-- period the pipeline claims to have loaded, and how many hours of lag that represents. A
-- pipeline that silently stopped three days ago renders a perfectly healthy-looking chart —
-- this is the model that catches it.
{% set period_end = var('period_end') %}

with watermarks as (

    select 'trips'            as table_name, 'silver'  as layer,
           cast(max(pickup_ts) as timestamp) as watermark_ts, count(*) as row_count
    from {{ ref('stg_trips') }}

    union all
    select 'daily_zone_kpis', 'gold',
           cast(max(pickup_date) as timestamp), count(*)
    from {{ ref('stg_spark_daily_zone_kpis') }}

    union all
    select 'hourly_demand', 'gold',
           cast(max(pickup_hour_ts) as timestamp), count(*)
    from {{ ref('stg_spark_hourly_demand') }}

    union all
    select 'payment_mix', 'gold',
           cast(max(pickup_date) as timestamp), count(*)
    from {{ ref('stg_payment_mix') }}

    union all
    select 'fct_trip_daily_zone', 'mart',
           cast(max(pickup_date) as timestamp), count(*)
    from {{ ref('fct_trip_daily_zone') }}

)

select
    table_name,
    layer,
    watermark_ts,
    row_count,
    cast('{{ period_end }}' as timestamp) as period_end_ts,
    round(
        {{ seconds_between("watermark_ts", "cast('" ~ period_end ~ "' as timestamp)") }} / 3600.0,
        2
    ) as lag_hours,
    case
        when {{ seconds_between("watermark_ts", "cast('" ~ period_end ~ "' as timestamp)") }}
             <= 86400 then 'fresh'
        when {{ seconds_between("watermark_ts", "cast('" ~ period_end ~ "' as timestamp)") }}
             <= 259200 then 'stale'
        else 'breached'
    end as freshness_status
from watermarks
