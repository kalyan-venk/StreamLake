select
    cast(pickup_date as date) as pickup_date,
    payment_type,
    payment_type_desc,
    trips,
    revenue,
    avg_tip_pct,
    trip_share
from {{ source('lakehouse', 'payment_mix') }}
