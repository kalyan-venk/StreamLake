-- Total transaction amount must survive aggregation. If the fact table's sum drifts from the
-- transaction-level sum, a join fanned out or a group-by dropped rows.
with detail as (

    select round(sum(amt), 2) as total_amt from {{ ref('stg_transactions') }}

),

aggregated as (

    select round(sum(total_amt), 2) as total_amt from {{ ref('fct_category_hourly_fraud') }}

)

select detail.total_amt as detail_amt, aggregated.total_amt as aggregated_amt
from detail, aggregated
where abs(detail.total_amt - aggregated.total_amt) > 1.00
