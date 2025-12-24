{{ config(materialized='incremental', alias='measurement', schema='gold') }}

select distinct
	value,
	coordinates,
	summary,
	period_label,
	period_interval,
	period_datetime_from_utc,
	period_datetime_from_local,
	period_datetime_to_utc,
	period_datetime_to_local,
	parameter_id,
	parameter_name,
	parameter_units,
	parameter_display_name,
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
	sensor_id,
	current_timestamp as dbt_load_timestamp

from {{ ref('silver_measurement') }}

{% if is_incremental() %}

where dbt_load_timestamp = (select coalesce(max(dbt_load_timestamp), '1900-01-01') from {{ this }})

{% endif %}
