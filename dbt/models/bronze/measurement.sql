{{ config(materialized='incremental', schema='bronze') }}

select distinct
	value,
	coordinates,
	summary,
	"period.label" as period_label,
	"period.interval" as period_interval,
	"period.datetime_from.utc" as period_datetime_from_utc,
	"period.datetime_from.local" as period_datetime_from_local,
	"period.datetime_to.utc" as period_datetime_to_utc,
	"period.datetime_to.local" as period_datetime_to_local,
	"parameter.id" as parameter_id,
	"parameter.name" as parameter_name,
	"parameter.units" as parameter_units,
	"parameter.display_name" as parameter_display_name,
	"coverage.expected_count" as coverage_expected_count,
	"coverage.expected_interval" as coverage_expected_interval,
	"coverage.observed_count" as coverage_observed_count,
	"coverage.observed_interval" as coverage_observed_interval,
	"coverage.percent_complete" as coverage_percent_complete,
	"coverage.percent_coverage" as coverage_percent_coverage,
	"coverage.datetime_from.utc" as coverage_datetime_from_utc,
	"coverage.datetime_from.local" as coverage_datetime_from_local,
	"coverage.datetime_to.utc" as coverage_datetime_to_utc,
	"coverage.datetime_to.local" as coverage_datetime_to_local,
	sensor_id,
	extraction_timestamp,
	current_timestamp as dbt_load_timestamp

from read_parquet('../data/raw/measurement/new/*.parquet')

{% if is_incremental() %}

where extraction_timestamp >= (select coalesce(max(extraction_timestamp), '1900-01-01') from {{ this }})

{% endif %}
