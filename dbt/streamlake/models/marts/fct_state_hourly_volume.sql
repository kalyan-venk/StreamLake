-- Transaction volume by cardholder state and hour, with each hour's share of the state's daily
-- volume. The window function is the point: this is the kind of question the warehouse answers
-- better than the lake, which is why the curated layer exists at all.
with transactions as (

    select * from {{ ref('stg_transactions') }}

),

hourly as (

    select
        trans_date,
        trans_hour,
        state,
        count(*)                     as txns,
        round(sum(amt), 2)           as total_amt,
        round(avg(amt), 3)           as avg_amt
    from transactions
    group by 1, 2, 3

)

select
    trans_date,
    trans_hour,
    state,
    txns,
    total_amt,
    avg_amt,
    round(100.0 * txns / sum(txns) over (partition by trans_date, state), 3)
        as share_of_state_day_pct
from hourly
