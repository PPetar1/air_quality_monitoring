{{ config(materialized='incremental', alias='country', unique_key='id', schema='bronze') }}

{% if not is_incremental() %}

select 
	id,
        code,
 	name,
	datetime_first,
	datetime_last,
       	parameters,
	current_timestamp as dbt_load_timestamp

from '../data/raw/country/**/*.parquet'

{% else %}

select 
	id,
        code,
 	name,
	datetime_first,
	datetime_last,
       	parameters,
	current_timestamp as dbt_load_timestamp

from '../data/raw/country/new/*.parquet'
where extraction_timestamp >= (select coalesce(max(dbt_load_timestamp), '1900-01-01') from {{ this }})

{% endif %}
