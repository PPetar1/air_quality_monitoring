{{ config(materialized='incremental', alias='locality', unique_key='id', schema='gold') }}

select
    l.country_id,
    min(l.id) as id,
    coalesce(l.locality, l.name) as name,
    concat(coalesce(l.locality, l.name), ', ', c.code) as display_name,
    avg(l.coordinates_latitude) as coordinates_latitude,
    avg(l.coordinates_longitude) as coordinates_longitude,
    min(l.datetime_first_utc) as datetime_first_utc,
    max(l.datetime_last_utc) as datetime_last_utc,
    current_timestamp as dbt_load_timestamp

from {{ ref('silver_location') }} as l
left join {{ ref('silver_country') }} as c on l.country_id = c.id

{% if is_incremental() %}

    where
        l.dbt_load_timestamp
        > (
            select coalesce(max(dbt_load_timestamp), '1900-01-01')
            from {{ this }}
        )

{% endif %}

group by coalesce(l.locality, l.name), l.country_id, c.code
