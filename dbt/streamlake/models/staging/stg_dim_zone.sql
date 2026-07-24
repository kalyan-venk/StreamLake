with source as (

    select * from {{ source('lakehouse', 'dim_zone') }}

)

select
    cast(location_id as integer) as location_id,
    borough,
    zone                          as zone_name,
    service_zone
from source
