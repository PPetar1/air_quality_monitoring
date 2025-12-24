{{ config(materialized='incremental', alias='location', unique_key='id', schema='gold') }}

select 
	id,
	name,
	locality,
	timezone,
	is_mobile,
	is_monitor,
	instruments,
	sensors, 
	bounds,
	distance,
	country_id,
	country_code,
	country_name,
	owner_id,
	owner_name,
	provider_id,
	provider_name,
	coordinates_latitude,
	coordinates_longitude,
	datetime_first_utc,
	datetime_first_local,
	datetime_last_utc,
	datetime_last_local,
	current_timestamp as dbt_load_timestamp

from {{ ref('silver_location') }}

{% if is_incremental() %}

where dbt_load_timestamp >= (select coalesce(max(dbt_load_timestamp), '1900-01-01') from {{ this }})

{% endif %}
