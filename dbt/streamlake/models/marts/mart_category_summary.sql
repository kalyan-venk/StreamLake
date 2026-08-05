-- One row per merchant category for the whole loaded period: the dashboard's headline table.
with transactions as (

    select * from {{ ref('stg_transactions') }}

),

by_category as (

    select
        category,
        count(*)                                  as txns,
        sum(is_fraud)                             as fraud_txns,
        round(sum(amt), 2)                        as total_amt,
        round(avg(amt), 3)                        as avg_amt,
        min(trans_time)                           as first_trans_time,
        max(trans_time)                           as last_trans_time
    from transactions
    group by 1

)

select
    *,
    round(fraud_txns * 1.0 / txns, 6)                as fraud_rate,
    round(100.0 * txns / sum(txns) over (), 3)       as txn_share_pct,
    round(100.0 * total_amt / sum(total_amt) over (), 3) as amt_share_pct
from by_category
