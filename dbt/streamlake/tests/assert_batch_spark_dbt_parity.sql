-- Cross-engine reconciliation: Spark's gold aggregate vs dbt's warehouse rebuild.
--
-- Both engines compute the same category/hour fraud KPIs from the same silver rows. Every
-- shared column is compared here, so a change to either engine's logic that moves a number
-- surfaces as a failing test rather than as a chart that looks slightly off. Counts must match
-- exactly. The rounded money and rate columns get a small tolerance for last-digit rounding
-- differences between the two engines. A genuine logic divergence is far larger than that
-- tolerance.
with spark_side as (

    select
        trans_hour_ts, category,
        txns, fraud_txns, total_amt, avg_amt, fraud_rate
    from {{ ref('stg_spark_category_hourly_fraud') }}

),

dbt_side as (

    select
        trans_hour_ts, category,
        txns, fraud_txns, total_amt, avg_amt, fraud_rate
    from {{ ref('fct_category_hourly_fraud') }}

)

select
    coalesce(s.trans_hour_ts, d.trans_hour_ts) as trans_hour_ts,
    coalesce(s.category, d.category)           as category,
    s.txns   as spark_txns,   d.txns   as dbt_txns,
    s.total_amt as spark_total_amt, d.total_amt as dbt_total_amt
from spark_side s
full outer join dbt_side d
    on  s.trans_hour_ts = d.trans_hour_ts
    and s.category      = d.category
where s.trans_hour_ts is null
   or d.trans_hour_ts is null
   or s.txns <> d.txns
   or s.fraud_txns <> d.fraud_txns
   or abs(s.total_amt  - d.total_amt)  > 0.01
   or abs(s.avg_amt    - d.avg_amt)    > 0.005
   or abs(s.fraud_rate - d.fraud_rate) > 0.0005
