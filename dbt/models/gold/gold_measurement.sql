{{ config(materialized='incremental', alias='measurement', schema='gold') }}

select distinct
    value,
    period_interval,
    period_datetime_from_utc,
    period_datetime_to_utc,
    parameter_id,
    sensor_id,
    current_timestamp as dbt_load_timestamp

from {{ ref('silver_measurement') }}

{% if is_incremental() %}

    where
        dbt_load_timestamp
        = (
            select coalesce(max(dbt_load_timestamp), '1900-01-01')
            from {{ this }}
        )

{% endif %}
