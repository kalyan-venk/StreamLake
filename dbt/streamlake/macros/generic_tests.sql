{#
    Three generic tests the project leans on. dbt_utils has equivalents, but keeping them local
    means `dbt build` works from a clean checkout with no `dbt deps` step and no package
    version pinned to someone else's release cadence.
#}

{% test unique_combination(model, columns) %}
    -- Fails with one row per duplicated key combination.
    select
        {{ columns | join(', ') }},
        count(*) as occurrences
    from {{ model }}
    group by {{ range(1, columns | length + 1) | join(', ') }}
    having count(*) > 1
{% endtest %}


{% test positive_values(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null and {{ column_name }} <= 0
{% endtest %}


{% test percentage_range(model, column_name) %}
    -- A share column outside 0-100 means the denominator in a window function is wrong.
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and ({{ column_name }} < 0 or {{ column_name }} > 100)
{% endtest %}


{% test fraction_range(model, column_name) %}
    -- A ratio column (fraud_txns / txns) outside 0-1 means the numerator exceeded the
    -- denominator, which for a rate that is a subset count of the same total should never happen.
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and ({{ column_name }} < 0 or {{ column_name }} > 1)
{% endtest %}
