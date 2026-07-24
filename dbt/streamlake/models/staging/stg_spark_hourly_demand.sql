select
    pickup_hour_ts,
    pickup_borough,
    trips,
    revenue,
    avg_duration_min
from {{ source('lakehouse', 'hourly_demand') }}
