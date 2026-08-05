-- Spark's version of the category/hour fraud aggregate, staged so the parity test can compare
-- it to dbt's own rebuild.
select
    trans_hour_ts,
    category,
    txns,
    fraud_txns,
    total_amt,
    avg_amt,
    fraud_rate
from {{ source('lakehouse', 'category_hourly_fraud') }}
