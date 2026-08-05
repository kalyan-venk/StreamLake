select
    trans_hour_ts,
    state,
    txns,
    total_amt,
    avg_amt
from {{ source('lakehouse', 'state_hourly_volume') }}
