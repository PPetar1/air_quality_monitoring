{{ config(materialized='incremental', alias='location', unique_key='id', schema='gold') }}

select
    id,
    name,
    instruments,
    owner_name,
    provider_name,
    coordinates_latitude,
    coordinates_longitude,
    datetime_first_utc,
    datetime_last_utc,
    min(id) over (partition by coalesce(locality, name)) as locality_id,
    current_timestamp as dbt_load_timestamp

from {{ ref('silver_location') }}

{% if is_incremental() %}

    where
        dbt_load_timestamp
        > (
            select coalesce(max(dbt_load_timestamp), '1900-01-01')
            from {{ this }}
        )

{% endif %}
