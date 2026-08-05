with source as (

    select * from {{ source('lakehouse', 'dim_category') }}

)

select
    category,
    channel
from source
