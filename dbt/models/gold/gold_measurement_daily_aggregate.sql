{{ config(materialized='incremental', alias='measurement_daily_aggregate', schema='gold') }}

with measurement_cte as (
    select
        sensor_id,
        cast(period_datetime_from_utc as date) as measurement_date,
        period_interval,
        parameter_id,
        value
    from {{ ref('silver_measurement') }}
    {% if is_incremental() %}

        where
            dbt_load_timestamp
            > (
                select coalesce(max(dbt_load_timestamp), '1900-01-01')
                from {{ this }}
            )

    {% endif %}
)

select
    m.sensor_id,
    m.measurement_date as date,
    m.period_interval,
    m.parameter_id,
    isodow(m.measurement_date) as day_of_week,
    avg(m.value) as daily_average,
    count(*) as number_of_measurements,
    current_timestamp as dbt_load_timestamp
from measurement_cte as m
group by m.sensor_id, m.measurement_date, m.period_interval, m.parameter_id
