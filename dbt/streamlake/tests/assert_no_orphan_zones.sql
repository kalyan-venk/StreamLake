-- Referential integrity in a warehouse with no foreign keys: every pickup location on a trip
-- must exist in the zone dimension, or the dashboard's borough totals quietly lose trips.
select t.pu_location_id, count(*) as trips
from {{ ref('stg_trips') }} t
left join {{ ref('dim_zone') }} z
    on t.pu_location_id = z.location_id
where z.location_id is null
group by 1
