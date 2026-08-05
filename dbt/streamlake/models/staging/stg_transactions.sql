-- Staging does renaming and nothing else. No business logic lives here, so when a mart looks
-- wrong there is exactly one place it can have gone wrong.
with source as (

    select * from {{ source('lakehouse', 'transactions') }}

),

renamed as (

    select
        trans_num,
        trans_time,
        cast(trans_date as date)         as trans_date,
        trans_hour,
        cc_num_last4,
        cc_num_hash,
        merchant,
        category,
        channel,
        amt,
        gender,
        city,
        state,
        zip,
        city_pop,
        job,
        cardholder_age,
        merch_lat,
        merch_long,
        distance_km,
        is_fraud,
        merch_zipcode,
        batch_id,
        ingested_at
    from source

)

select * from renamed
