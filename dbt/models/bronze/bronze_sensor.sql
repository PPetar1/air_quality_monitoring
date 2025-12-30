{{ config(materialized='incremental', alias='sensor', unique_key='id', schema='bronze') }}

{% if not is_incremental() %}

select distinct 
	id,
	name,
	"parameter.id" as parameter_id,
	"parameter.name" as parameter_name,
	"parameter.units" as parameter_units,
	"parameter.display_name" as parameter_display_name,
	"datetime_first.utc" as datetime_first_utc,
	"datetime_first.local" as datetime_first_local,
	"datetime_last.utc" as datetime_last_utc,
	"datetime_last.local" as datetime_last_local,
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
	"latest.datetime.utc" as latest_datetime_utc,
	"latest.datetime.local" as latest_datetime_local,
	"latest.value" as latest_value,
	"latest.coordinates.latitude" as latest_coordinates_latitude,
	"latest.coordinates.longitude" as latest_coordinates_longitude,
	"summary.min" as summary_min,
	"summary.q02" as summary_q02,
	"summary.q25" as summary_q25,
	"summary.median" as summary_median,
	"summary.q75" as summary_q75,
	"summary.q98" as summary_q98,
	"summary.max" as summary_max,
	"summary.avg" as summary_avg,
	"summary.sd" as summary_sd,
	location_id,
	current_timestamp as dbt_load_timestamp

from read_parquet('../data/raw/sensor/**/*.parquet', union_by_name=true) 
where (id, extraction_timestamp) in (select id, max(extraction_timestamp) from read_parquet('../data/raw/sensor/**/*.parquet', union_by_name=true) group by id)

{% elif files_exist('../data/raw/sensor/new/*.parquet') %}

    select
        id,
        name,
        "parameter.id" as parameter_id,
        "parameter.name" as parameter_name,
        "parameter.units" as parameter_units,
        "parameter.display_name" as parameter_display_name,
        "datetime_first.utc" as datetime_first_utc,
        "datetime_first.local" as datetime_first_local,
        "datetime_last.utc" as datetime_last_utc,
        "datetime_last.local" as datetime_last_local,
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
        "latest.datetime.utc" as latest_datetime_utc,
        "latest.datetime.local" as latest_datetime_local,
        "latest.value" as latest_value,
        "latest.coordinates.latitude" as latest_coordinates_latitude,
        "latest.coordinates.longitude" as latest_coordinates_longitude,
        "summary.min" as summary_min,
        "summary.q02" as summary_q02,
        "summary.q25" as summary_q25,
        "summary.median" as summary_median,
        "summary.q75" as summary_q75,
        "summary.q98" as summary_q98,
        "summary.max" as summary_max,
        "summary.avg" as summary_avg,
        "summary.sd" as summary_sd,
        location_id,
        current_timestamp as dbt_load_timestamp

    from read_parquet('../data/raw/sensor/new/*.parquet', union_by_name = true)
    where
        extraction_timestamp
        > (
            select coalesce(max(dbt_load_timestamp), '1900-01-01')
            from {{ this }}
        )

{% else %}

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

    from {{ this }}
    where 1 = 0

{% endif %}
