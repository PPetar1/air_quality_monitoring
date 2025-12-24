{{ config(materialized='incremental', alias='sensor', unique_key='id', schema='silver') }}

select 
	id,
	name,
	parameter_id,
	parameter_name,
	parameter_units,
	parameter_display_name,
	datetime_first_utc,
	datetime_first_local,
	datetime_last_utc,
	datetime_last_local,
	coverage_expected_count,
	coverage_expected_interval,
	coverage_observed_count,
	coverage_observed_interval,
	coverage_percent_complete,
	coverage_percent_coverage,
	coverage_datetime_from_utc,
	coverage_datetime_from_local,
	coverage_datetime_to_utc,
	coverage_datetime_to_local,
	latest_datetime_utc,
	latest_datetime_local,
	latest_value,
	latest_coordinates_latitude,
	latest_coordinates_longitude,
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

from {{ ref('bronze_sensor') }}

{% if is_incremental() %}

where dbt_load_timestamp = (select coalesce(max(dbt_load_timestamp), '1900-01-01') from {{ this }})

{% endif %}
