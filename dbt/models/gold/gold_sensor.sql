{{ config(materialized='incremental', alias='sensor', unique_key='id', schema='gold') }}

select 
	id,
	name,
	parameter_id,
	datetime_first_utc,
	datetime_last_utc,
	latest_datetime_utc,
	latest_value,
	summary_min,
	summary_q02,
	summary_q25,
	summary_median,
	summary_q75,
	summary_q98,
	summary_max,
	summary_avg,
	summary_sd,
	location_id,
	current_timestamp as dbt_load_timestamp

from {{ ref('silver_sensor') }}

{% if is_incremental() %}

where dbt_load_timestamp = (select coalesce(max(dbt_load_timestamp), '1900-01-01') from {{ this }})

{% endif %}
