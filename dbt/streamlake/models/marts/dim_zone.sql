-- The conformed zone dimension. Surrogate-free on purpose: TLC's location_id is stable, public,
-- and already used by every downstream consumer, so inventing a new key would only add a join.
select
    location_id,
    borough,
    zone_name,
    service_zone,
    case
        when borough = 'EWR' then 'Airport'
        when service_zone = 'Airports' then 'Airport'
        when borough = 'Manhattan' then 'Core'
        else 'Outer'
    end as zone_class
from {{ ref('stg_dim_zone') }}
