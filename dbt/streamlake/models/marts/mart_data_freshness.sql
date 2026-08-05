-- The freshness monitor behind the dashboard's second page.
--
-- A static chart tells you what the data says. This tells you whether the data is still being
-- produced: for every curated table, how recent its newest record is relative to the end of the
-- period the pipeline claims to have loaded, and how many hours of lag that represents. A
-- pipeline that silently stopped three days ago renders a perfectly healthy-looking chart,
-- this is the model that catches it.
{% set period_end = var('period_end') %}

with watermarks as (

    select 'transactions'          as table_name, 'silver'  as layer,
           cast(max(trans_time) as timestamp) as watermark_ts, count(*) as row_count
    from {{ ref('stg_transactions') }}

    union all
    select 'category_hourly_fraud', 'gold',
           cast(max(trans_hour_ts) as timestamp), count(*)
    from {{ ref('stg_spark_category_hourly_fraud') }}

    union all
    select 'state_hourly_volume', 'gold',
           cast(max(trans_hour_ts) as timestamp), count(*)
    from {{ ref('stg_spark_state_hourly_volume') }}

    union all
    select 'merchant_risk_leaderboard', 'gold',
           cast('{{ period_end }}' as timestamp), count(*)
    from {{ ref('stg_merchant_risk') }}

    union all
    select 'fct_category_hourly_fraud', 'mart',
           cast(max(trans_hour_ts) as timestamp), count(*)
    from {{ ref('fct_category_hourly_fraud') }}

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
