-- dbt's independent rebuild of the category/hour fraud aggregate.
--
-- Spark already computes this (gold.category_hourly_fraud). Recomputing it here in warehouse SQL
-- is deliberate: tests/assert_batch_spark_dbt_parity.sql compares the two, so a change to either
-- engine's logic that silently moves the fraud rate gets caught by a failing test rather than by
-- someone noticing a chart looks off.
with transactions as (

    select * from {{ ref('stg_transactions') }}

),

hourly as (

    select
        date_trunc('hour', trans_time) as trans_hour_ts,
        category,
        count(*)                            as txns,
        sum(is_fraud)                       as fraud_txns,
        round(sum(amt), 2)                  as total_amt,
        round(avg(amt), 3)                  as avg_amt
    from transactions
    group by 1, 2

)

select
    trans_hour_ts,
    category,
    txns,
    fraud_txns,
    total_amt,
    avg_amt,
    round(fraud_txns * 1.0 / txns, 6) as fraud_rate
from hourly
