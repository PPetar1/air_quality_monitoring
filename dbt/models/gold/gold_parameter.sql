{{ config(materialized='incremental', alias='parameter', unique_key='id', schema='gold') }}

select
    id,
    name,
    units,
    description,
    coalesce(description, display_name, name) as display_name,
    current_timestamp as dbt_load_timestamp

from {{ ref('silver_parameter') }}
where
    id in (
        select distinct parameter_id from {{ ref('silver_measurement') }}
    )
    {% if is_incremental() %}

        and
        dbt_load_timestamp
        > (
            select coalesce(max(dbt_load_timestamp), '1900-01-01')
            from {{ this }}
        )

    {% endif %}
