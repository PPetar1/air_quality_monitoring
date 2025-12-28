{{ config(materialized='incremental', alias='country', unique_key='id', schema='gold') }}

select
    id,
    code,
    name,
    current_timestamp as dbt_load_timestamp

from {{ ref('silver_country') }}

{% if is_incremental() %}

    where
        dbt_load_timestamp
        > (
            select coalesce(max(dbt_load_timestamp), '1900-01-01')
            from {{ this }}
        )

{% endif %}
