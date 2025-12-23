{{ config(materialized='incremental', unique_key='id', schema='bronze') }}

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
	"country.id" as country_id,
	"country.code" as country_code,
	"country.name" as country_name,
	"owner.id" as owner_id,
	"owner.name" as owner_name,
	"provider.id" as provider_id,
	"provider.name" as provider_name,
	"coordinates.latitude" as coordinates_latitude,
	"coordinates.longitude" as coordinates_longitude,
	"datetime_first.utc" as datetime_first_utc,
	"datetime_first.local" as datetime_first_local,
	"datetime_last.utc" as datetime_last_utc,
	"datetime_last.local" as datetime_last_local,
	extraction_timestamp,
	current_timestamp as dbt_load_timestamp

from read_parquet('../data/raw/location/new/*.parquet')

{% if is_incremental() %}

where extraction_timestamp >= (select coalesce(max(extraction_timestamp), '1900-01-01') from {{ this }})

{% endif %}
