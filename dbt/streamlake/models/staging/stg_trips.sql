-- Staging does renaming, typing, and nothing else. No business logic lives here, so when a
-- mart looks wrong there is exactly one place it can have gone wrong.
with source as (

    select * from {{ source('lakehouse', 'trips') }}

),

renamed as (

    select
        trip_id,
        vendor_id,
        pickup_ts,
        dropoff_ts,
        cast(pickup_date as date)                as pickup_date,
        pickup_hour,
        passenger_count,
        trip_distance_mi,
        trip_duration_min,
        avg_speed_mph,
        pu_location_id,
        do_location_id,
        pickup_borough,
        pickup_zone,
        dropoff_borough,
        dropoff_zone,
        payment_type,
        payment_type_desc,
        fare_amount,
        tip_amount,
        tip_pct,
        tolls_amount,
        congestion_surcharge,
        airport_fee,
        total_amount,
        batch_id,
        ingested_at
    from source

)

select * from renamed
