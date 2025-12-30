{{ config(materialized='incremental', alias='country', unique_key='id', schema='gold') }}

select
    id,
    code,
    name,
    current_timestamp as dbt_load_timestamp

from {{ ref('silver_country') }}
where
    id in (132, 110, 103, 80, 75, 65, 131, 62, 74, 97, 104)
    {% if is_incremental() %}

        and
        dbt_load_timestamp
        > (
            select coalesce(max(dbt_load_timestamp), '1900-01-01')
            from {{ this }}
        )

    {% endif %}
