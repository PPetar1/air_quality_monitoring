{{ config(materialized='incremental', unique_key='id', schema='bronze') }}

select 
	id,
	name,
	units,
	display_name,
	description,
	extraction_timestamp,
	current_timestamp as dbt_load_timestamp

from read_parquet('../data/raw/parameter/new/*.parquet')

{% if is_incremental() %}

where extraction_timestamp >= (select coalesce(max(extraction_timestamp), '1900-01-01') from {{ this }})

{% endif %}
