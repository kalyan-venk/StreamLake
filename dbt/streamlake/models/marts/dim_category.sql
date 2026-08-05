-- The conformed category dimension. Surrogate-free on purpose: category is already the join key
-- every downstream consumer uses, so inventing a new key would only add a join.
select
    category,
    channel,
    case
        when channel = 'online' then 'card_not_present'
        when channel = 'in_person' then 'card_present'
        else 'general'
    end as category_tier
from {{ ref('stg_dim_category') }}
