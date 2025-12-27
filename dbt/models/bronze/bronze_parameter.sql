{{ config(materialized='incremental', alias='parameter', unique_key='id', schema='bronze') }}

{% if not is_incremental() %}

select 
	id,
	name,
	units,
	display_name,
	description,
	current_timestamp as dbt_load_timestamp

from '../data/raw/parameter/**/*.parquet'

{% else %}

select 
	id,
	name,
	units,
	display_name,
	description,
	current_timestamp as dbt_load_timestamp

from '../data/raw/parameter/new/*.parquet'
where extraction_timestamp >= (select coalesce(max(dbt_load_timestamp), '1900-01-01') from {{ this }})

{% endif %}
