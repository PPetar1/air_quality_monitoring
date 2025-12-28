{{ config(materialized='incremental', alias='location', unique_key='id', schema='silver') }}

select
    id,
    name,
    locality,
    instruments,
    country_id,
    owner_name,
    provider_name,
    coordinates_latitude,
    coordinates_longitude,
    datetime_first_utc,
    datetime_last_utc,
    current_timestamp as dbt_load_timestamp

from {{ ref('bronze_location') }}

{% if is_incremental() %}

    where
        dbt_load_timestamp
        > (
            select coalesce(max(dbt_load_timestamp), '1900-01-01')
            from {{ this }}
        )

{% endif %}
