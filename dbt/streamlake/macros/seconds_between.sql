{#
    DuckDB and Snowflake spell timestamp difference differently. Adapter dispatch keeps the
    models identical across both targets instead of forking the SQL, which is the whole point
    of running the warehouse layer through dbt rather than raw scripts.
#}

{% macro seconds_between(start_ts, end_ts) -%}
    {{ return(adapter.dispatch('seconds_between', 'streamlake')(start_ts, end_ts)) }}
{%- endmacro %}

{% macro default__seconds_between(start_ts, end_ts) -%}
    date_diff('second', {{ start_ts }}, {{ end_ts }})
{%- endmacro %}

{% macro snowflake__seconds_between(start_ts, end_ts) -%}
    datediff(second, {{ start_ts }}, {{ end_ts }})
{%- endmacro %}
