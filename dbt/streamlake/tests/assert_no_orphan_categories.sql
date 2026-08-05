-- Referential integrity in a warehouse with no foreign keys: every category on a transaction
-- must exist in the category dimension, or the dashboard's category totals quietly lose rows.
select t.category, count(*) as txns
from {{ ref('stg_transactions') }} t
left join {{ ref('dim_category') }} c
    on t.category = c.category
where c.category is null
group by 1
