{{ config(materialized='incremental', alias='location', unique_key='id', schema='bronze') }}

{% if not is_incremental() %}

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
	current_timestamp as dbt_load_timestamp

from '../data/raw/location/**/*.parquet'

{% elif files_exist('../data/raw/location/new/*.parquet') %}

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
        current_timestamp as dbt_load_timestamp

    from '../data/raw/location/new/*.parquet'
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

    from {{ this }}
    where 1 = 0

{% endif %}
