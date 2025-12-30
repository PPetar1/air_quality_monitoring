{{ config(materialized='incremental', alias='parameter', unique_key='id', schema='gold') }}

select
    p.id,
    p.name,
    p.units,
    p.description,
    coalesce(p.display_name, p.name) as display_name,
    current_timestamp as dbt_load_timestamp

from {{ ref('silver_parameter') }} as p
where
    p.id in (
        select distinct m.parameter_id from {{ ref('silver_measurement') }} as m
    )
    {% if is_incremental() %}

        and
        p.dbt_load_timestamp
        > (
            select coalesce(max(dbt_load_timestamp), '1900-01-01')
            from {{ this }}
        )

    {% endif %}
